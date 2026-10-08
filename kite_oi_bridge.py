"""
OI Bias Monitor — Kite WebSocket Bridge (Railway / PostgreSQL edition)
=======================================================================
Supports Nifty and Sensex. Configured via environment variables.

Railway setup
─────────────
1.  Add a Postgres plugin → DATABASE_URL is injected automatically.
2.  Set env vars:
        KITE_API_KEY
        KITE_ACCESS_TOKEN
        INDEX           nifty  (default) | sensex
3.  Deploy from GitHub — Railway will run:  python kite_oi_bridge.py

Index-specific behaviour
────────────────────────
  INDEX=nifty   → table oi_history_nifty,   instrument name NIFTY,  exchange NSE, strike step 50
  INDEX=sensex  → table oi_history_sensex,  instrument name SENSEX, exchange BSE, strike step 100

Dashboard tabs served: Option Chain & OI Moves · Selected Strike OI · Straddle · OpenHighLow

Endpoints:
    GET  /                       → serves the dashboard HTML
    GET  /oi                     → snapshot for ALL strikes (OI, OI change)
    GET  /ltp                    → live LTP + BS delta/gamma/GEX per strike, GEX summary
                                   (net GEX, gamma flip, call/put wall), settlement VWAP,
                                   futures order flow, OpenHighLow snapshot
    GET  /oi/history             → 1-min rows for today or ?date=YYYY-MM-DD
    GET  /oi/dates               → dates that have history
    GET  /strikes                → strike list
    GET  /oi/openhighlow         → OpenHighLow tab snapshot (also piggybacked on /ltp)
    POST /oi/openhighlow/reseed  → re-pull today's Open/High/Low from the exchange
    GET  /oi/openhighlow/debug   → explain why a strike is / isn't on the OHL lists
    GET  /health                 → status + OHLC buffer
    POST /reset-csv              → wipe today's rows, start fresh
"""

import collections
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta, time as dtime

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def ts_to_ist(ts): return datetime.fromtimestamp(ts, tz=IST)

# ── MARKET HOURS ──────────────────────────────────────────────────────────────
# NSE cash/F&O: Monday–Friday, 09:15–15:29 IST (15:30 is close, so last
# valid candle starts at 15:29 and completes at 15:30).
MARKET_OPEN_HM  = (9, 15)
MARKET_CLOSE_HM = (15, 30)   # exclusive upper bound

def is_market_open(dt=None):
    """True if dt (defaults to now IST) is within Mon–Fri 09:15–15:29 IST."""
    if dt is None:
        dt = now_ist()
    if dt.weekday() >= 5:          # Sat=5, Sun=6
        return False
    mins = dt.hour * 60 + dt.minute
    open_mins  = MARKET_OPEN_HM[0]  * 60 + MARKET_OPEN_HM[1]
    close_mins = MARKET_CLOSE_HM[0] * 60 + MARKET_CLOSE_HM[1]
    return open_mins <= mins < close_mins

import math
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from kiteconnect import KiteTicker, KiteConnect


# ── BLACK-SCHOLES IV ──────────────────────────────────────────────────────────

def _norm_cdf(x):
    """Standard normal CDF via math.erfc (no scipy needed)."""
    return 0.5 * math.erfc(-x / math.sqrt(2))

def _bs_price(S, K, T, r, sigma, opt_type):
    """Black-Scholes option price. opt_type: 'CE' or 'PE'."""
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if opt_type == "CE" else (K - S))
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if opt_type == "CE":
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)

def _bs_delta(S, K, T, r, sigma, opt_type):
    """Black-Scholes delta (always returned as a positive value 0–1)."""
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    raw = _norm_cdf(d1)
    return raw if opt_type == "CE" else abs(raw - 1)

def compute_iv(S, K, T, r, market_price, opt_type, tol=1e-5, max_iter=100):
    """
    Implied volatility via bisection.
    Returns IV as a decimal (0.20 = 20%) or None if no solution found.
    """
    if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return None
    intrinsic = max(0.0, (S - K) if opt_type == "CE" else (K - S))
    if market_price <= intrinsic + 0.01:
        return None
    lo, hi = 0.001, 5.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        price = _bs_price(S, K, T, r, mid, opt_type)
        if abs(price - market_price) < tol:
            return mid
        if price < market_price:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2

# ── OPTION DELTA (Option Chain "Delta" column) ────────────────────────────────
RISK_FREE = 0.065


def _years_to_expiry(expiry_str):
    """Time to expiry in years, measured to 15:30 IST on expiry day rather than
    in whole days, so delta stays sensible on expiry day itself.
    Floored at 15 minutes so it never collapses to zero right at the close."""
    if not expiry_str:
        return None
    try:
        from datetime import date as _date
        d = _date.fromisoformat(str(expiry_str)[:10])
    except Exception:
        return None
    exp_dt = datetime(d.year, d.month, d.day,
                      MARKET_CLOSE_HM[0], MARKET_CLOSE_HM[1], tzinfo=IST)
    secs = (exp_dt - now_ist()).total_seconds()
    return max(secs, 15 * 60) / (365.0 * 24 * 3600)


def _bs_gamma(S, K, T, r, sigma):
    """Black-Scholes gamma (identical for CE and PE)."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    return math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi) / (S * sigma * math.sqrt(T))


def option_greeks(S, K, T, ltp, opt_type):
    """(delta, gamma, iv) from the IV implied by the option's own LTP.
    delta is signed (CE 0..+1, PE -1..0); iv is a decimal (0.13 = 13%).
    Returns (None, None, None) when there is nothing to price."""
    if not S or not K or not T or not ltp or ltp <= 0:
        return None, None, None
    iv = compute_iv(S, K, T, RISK_FREE, ltp, opt_type)
    if iv is None:
        # Premium at/below intrinsic → no time value left: deep ITM, delta ±1, no gamma.
        intrinsic = (S - K) if opt_type == "CE" else (K - S)
        if intrinsic > 0:
            return (1.0 if opt_type == "CE" else -1.0), 0.0, None
        return None, None, None
    d = _bs_delta(S, K, T, RISK_FREE, iv, opt_type)   # always positive
    return (round(d if opt_type == "CE" else -d, 3),
            _bs_gamma(S, K, T, RISK_FREE, iv), iv)


# ── GAMMA EXPOSURE (GEX) ──────────────────────────────────────────────────────
# GEX per leg = gamma × OI × spot² × 1%   → ₹ change in dealer delta for a 1% move.
# Sign convention (the standard "dealer GEX" one): dealers are assumed net LONG
# the calls customers sold them and net SHORT the puts customers bought, so
# call GEX counts positive and put GEX negative.
#   Net GEX > 0 → dealers hedge against the move (buy dips / sell rips): pinning,
#                 mean-reversion, lower realised vol.
#   Net GEX < 0 → dealers hedge with the move: moves get amplified.
# Gamma flip = spot level where total net GEX crosses zero (each strike's IV held
# fixed, spot shifted). Totals only cover the tracked strike window
# (OI_NUM_STRIKES) — widen it for a fuller picture.
# Kite reports option OI in units (quantity), so no lot-size multiplier is
# needed; set GEX_OI_MULTIPLIER if your feed ever reports OI in lots.
GEX_OI_MULTIPLIER = float(os.environ.get("GEX_OI_MULTIPLIER", 1))


def _leg_gex(spot, gamma, oi, opt_type):
    """Signed ₹ GEX for one leg per 1% spot move."""
    if gamma is None or not oi or not spot:
        return 0.0
    sign = 1.0 if opt_type == "CE" else -1.0
    return sign * gamma * oi * GEX_OI_MULTIPLIER * spot * spot * 0.01


def _gex_summary(spot, T, legs, strike_step):
    """legs: list of dicts {strike, type, iv, oi, gex}. Returns the summary block."""
    if not spot or not legs:
        return None
    total = sum(l["gex"] for l in legs)

    by_strike = {}
    for l in legs:
        by_strike[l["strike"]] = by_strike.get(l["strike"], 0.0) + l["gex"]
    ce = [l for l in legs if l["type"] == "CE" and l["gex"] > 0]
    pe = [l for l in legs if l["type"] == "PE" and l["gex"] < 0]
    call_wall = max(ce, key=lambda l: l["gex"])["strike"] if ce else None
    put_wall  = min(pe, key=lambda l: l["gex"])["strike"] if pe else None

    # Gamma flip: re-price total GEX across hypothetical spot levels spanning the
    # tracked strike window, IV per leg held constant; take the zero crossing
    # nearest to current spot (linear interpolation between grid points).
    flip = None
    priced = [l for l in legs if l["iv"] and l["oi"]]
    strikes = sorted(by_strike)
    if T and priced and len(strikes) >= 2:
        lo, hi = strikes[0], strikes[-1]
        step = max(strike_step / 10.0, 1.0)
        def total_at(x):
            return sum(_leg_gex(x, _bs_gamma(x, l["strike"], T, RISK_FREE, l["iv"]),
                                l["oi"], l["type"]) for l in priced)
        prev_x, prev_v = lo, total_at(lo)
        x = lo + step
        best = None
        while x <= hi + 1e-9:
            v = total_at(x)
            if prev_v == 0 or (prev_v < 0) != (v < 0):
                cross = prev_x if prev_v == 0 else prev_x + (x - prev_x) * (-prev_v) / (v - prev_v)
                if best is None or abs(cross - spot) < abs(best - spot):
                    best = cross
            prev_x, prev_v = x, v
            x += step
        flip = round(best, 1) if best is not None else None

    return {
        "total":       round(total),            # ₹ per 1% move
        "regime":      "positive" if total >= 0 else "negative",
        "flip":        flip,
        "call_wall":   call_wall,
        "put_wall":    put_wall,
        "per_strike":  {str(k): round(v) for k, v in by_strike.items()},
        "window":      [strikes[0], strikes[-1]] if strikes else None,
    }

# ── CONFIG ────────────────────────────────────────────────────────────────────

API_KEY      = os.environ["KITE_API_KEY"]
ACCESS_TOKEN = os.environ["KITE_ACCESS_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

FLASK_PORT        = int(os.environ.get("PORT", 5000))   # Railway sets PORT
OI_HISTORY_MAXLEN = 500

# ── INDEX SELECTION ───────────────────────────────────────────────────────────
# Set INDEX=nifty (default) or INDEX=sensex in your Railway environment.
_INDEX_RAW = os.environ.get("INDEX", "nifty").strip().lower()
if _INDEX_RAW not in ("nifty", "sensex"):
    raise ValueError(f"INDEX env var must be 'nifty' or 'sensex', got: {_INDEX_RAW!r}")

if _INDEX_RAW == "sensex":
    INDEX_NAME      = "sensex"
    INDEX_LABEL     = "Sensex"
    SPOT_TOKEN      = 265     # BSE SENSEX instrument token
    SPOT_QUOTE_KEY  = "BSE:SENSEX"
    INSTRUMENT_NAME = "SENSEX"    # matches kite.instruments "name" field
    EXCHANGE        = "BFO"       # Bombay F&O exchange
    STRIKE_STEP     = 100
    ROLL_THRESHOLD  = 200
    DB_TABLE        = "oi_history_sensex"
else:
    INDEX_NAME      = "nifty"
    INDEX_LABEL     = "Nifty 50"
    SPOT_TOKEN      = 256265  # NSE NIFTY 50 instrument token
    SPOT_QUOTE_KEY  = "NSE:NIFTY 50"
    INSTRUMENT_NAME = "NIFTY"
    EXCHANGE        = "NFO"
    STRIKE_STEP     = 50
    ROLL_THRESHOLD  = 100
    DB_TABLE        = "oi_history_nifty"

# OI_NUM_STRIKES: total strikes to track for OI (must be odd so ATM sits in the middle).
# Default 11 = ATM ±5 strikes. Set e.g. OI_NUM_STRIKES=21 for ATM ±10 strikes.
NUM_STRIKES    = int(os.environ.get("OI_NUM_STRIKES", 11))
if NUM_STRIKES % 2 == 0:
    NUM_STRIKES += 1          # force odd so ATM is centred
    print(f"Warning: OI_NUM_STRIKES must be odd — bumped to {NUM_STRIKES}")


# ── APP ───────────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder="static")
CORS(app)

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)


# ── DATABASE ──────────────────────────────────────────────────────────────────

def get_db():
    """Return a new psycopg2 connection. Call .close() when done."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    """
    Create the index-specific oi_history table if it doesn't exist.
    Uses a JSONB 'data' column so the schema is always flexible —
    no ALTER TABLE needed when the strike window changes.
    Table name is DB_TABLE: oi_history_nifty or oi_history_sensex.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {DB_TABLE} (
                    id          SERIAL PRIMARY KEY,
                    ts          DOUBLE PRECISION NOT NULL,
                    time_label  TEXT,
                    session_id  INTEGER,
                    trade_date  DATE DEFAULT CURRENT_DATE,
                    data        JSONB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_date
                    ON {DB_TABLE} (trade_date);
                CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_date_time
                    ON {DB_TABLE} (trade_date, ts);
                -- OpenHighLow latch-state persistence: one JSONB blob per
                -- (day, index) holding the whole day_oh dict, so latched /
                -- filled flags survive redeploys and restarts.
                CREATE TABLE IF NOT EXISTS ohl_day_state (
                    trade_date  DATE NOT NULL,
                    index_name  TEXT NOT NULL,
                    updated_ts  DOUBLE PRECISION,
                    data        JSONB NOT NULL,
                    PRIMARY KEY (trade_date, index_name)
                );
            """)
        conn.commit()
    print(f"DB initialised — table {DB_TABLE} ready.")


def db_write_row(row: dict):
    """Insert one 1-min completed candle row into the index-specific table."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {DB_TABLE} (ts, time_label, session_id, trade_date, data)
                VALUES (%s, %s, %s, CURRENT_DATE, %s)
                """,
                (
                    row.get("ts"),
                    row.get("time_label"),
                    row.get("session_id"),
                    json.dumps(row),
                ),
            )
        conn.commit()


def db_read_today() -> list:
    """Return all 1-min rows for today as a list of dicts."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT data FROM {DB_TABLE} WHERE trade_date = CURRENT_DATE ORDER BY ts ASC"
            )
            rows = [dict(r["data"]) for r in cur.fetchall()]
    return rows


def db_reset_today():
    """Delete all rows for today from the index-specific table."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {DB_TABLE} WHERE trade_date = CURRENT_DATE")
        conn.commit()


def db_read_by_date(date_str: str) -> list:
    """Return all 1-min rows for a specific trade_date (YYYY-MM-DD string)."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT data FROM {DB_TABLE} WHERE trade_date = %s ORDER BY ts ASC",
                (date_str,),
            )
            rows = [dict(r["data"]) for r in cur.fetchall()]
    return rows


def db_list_dates() -> list:
    """Return sorted list of distinct trade_dates that have data (YYYY-MM-DD strings)."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT trade_date FROM {DB_TABLE} ORDER BY trade_date DESC"
            )
            return [str(r[0]) for r in cur.fetchall()]


# ── STATE ─────────────────────────────────────────────────────────────────────

state = {
    "spot":           None,
    "last_anchored":  None,
    "atm_strike":     None,
    "strikes":        [],
    "bear_strike":    None,
    "bull_strike":    None,
    "session_id":     0,
    "tokens":         {},
    "oi":             {},
    "oi_baseline":    {},
    "oi_prev_snap":   {},
    "roll_log":       [],
    "expiry_date":    None,   # front-month expiry as YYYY-MM-DD string
    "fut_token":      None,   # front-month Nifty Futures instrument token
    "fut_symbol":     None,   # front-month Futures tradingsymbol (for ohlc() seeding)
    "fut_ltp":        None,   # latest futures LTP (updated by on_ticks)
}

minute_buffer = {
    "start_ts":   None,
    "start_snap": None,
    "spot_open":  None,
    "spot_high":  None,
    "spot_low":   None,
    "spot_close": None,
    "ltp_ohlc":   {},
}

state_lock = threading.Lock()
oi_history = collections.deque(maxlen=OI_HISTORY_MAXLEN)   # in-memory fallback

# ── SETTLEMENT VWAP (projected close) ────────────────────────────────────────
# NSE computes each constituent's close as its VWAP over 15:00–15:30, then
# recomputes the index — the expiry settlement price. We track three estimates:
#
#  1. spot_twap  — time-weighted average of Nifty spot (1 sample/sec), same as
#                  before. Proxy because spot has no volume.
#  2. fut_vwap   — true volume-weighted average of Nifty Futures LTP over the
#                  window. Futures have real volume and closely track spot minus
#                  basis. We subtract the live basis to convert back to spot.
#  3. synth_spot — put-call parity synthetic: CE_ltp - PE_ltp + ATM_strike.
#                  Time-averaged over the 15:00–15:30 window. Requires only the
#                  options we're already tracking — no extra token needed.
#
# All three are exposed in _settlement_vwap_payload() and shown in the OC ticker.
settlement_vwap = {
    "date":           None,
    # --- spot TWAP (existing) ---
    "sum":            0.0,
    "n":              0,
    "value":          None,
    "last_sec":       None,
    # --- futures true VWAP (every tick counted, no throttle) ---
    "fut_vol_sum":    0.0,   # Σ (price × traded_qty in window)
    "fut_vol_n":      0.0,   # Σ traded_qty in window
    "fut_vwap":       None,  # running VWAP value
    # --- synth spot VWAP (ATM CE - PE + K, volume-weighted by CE+PE qty) ---
    "synth_sum":      0.0,
    "synth_n":        0.0,
    "synth_value":    None,
    "synth_last_sec": None,
    # --- basis TWAP over 15:00–15:30 (fut_ltp − spot, 1 sample/sec) ---
    # Used to convert fut_vwap → spot_equiv with the same window average,
    # so numerator and denominator are both averaged over identical intervals.
    "basis_sum":      0.0,
    "basis_n":        0,
    "basis_twap":     None,  # window-average basis; None until window starts
    "basis_last_sec": None,
}

# Live basis tracker: futures LTP − spot, kept as a short rolling buffer so we
# can report the median basis without being spiked by a single bad tick.
_basis_samples = collections.deque(maxlen=60)   # last ~60 ticks ≈ ~1 min

SETTLE_WIN_START = dtime(15, 0, 0)
SETTLE_WIN_END   = dtime(15, 30, 0)

def _reset_settlement_vwap_if_new_day(today):
    if settlement_vwap["date"] != today:
        settlement_vwap.update({
            "date": today,
            "sum": 0.0, "n": 0, "value": None, "last_sec": None,
            "fut_vol_sum": 0.0, "fut_vol_n": 0.0, "fut_vwap": None,
            "synth_sum": 0.0, "synth_n": 0.0, "synth_value": None, "synth_last_sec": None,
            "basis_sum": 0.0, "basis_n": 0, "basis_twap": None, "basis_last_sec": None,
        })


def _update_settlement_vwap(spot):
    """Accumulate spot into the 15:00–15:30 IST time-weighted average (spot TWAP).
    Caller must hold state_lock. Samples at most once per second."""
    if spot is None:
        return
    now = now_ist()
    _reset_settlement_vwap_if_new_day(now.strftime("%Y-%m-%d"))
    t = now.time()
    if t < SETTLE_WIN_START or t > SETTLE_WIN_END:
        return
    sec_key = now.strftime("%H:%M:%S")
    if sec_key == settlement_vwap["last_sec"]:
        return
    settlement_vwap["last_sec"] = sec_key
    settlement_vwap["sum"] += float(spot)
    settlement_vwap["n"]   += 1
    settlement_vwap["value"] = settlement_vwap["sum"] / settlement_vwap["n"]


def _update_basis(fut_ltp, spot):
    """Track live basis (fut − spot) in a rolling 60-sample buffer.
    Caller must hold state_lock."""
    if fut_ltp and spot and spot > 0:
        _basis_samples.append(fut_ltp - spot)
        _update_basis_twap(fut_ltp, spot)


def _update_basis_twap(fut_ltp, spot):
    """Accumulate (fut_ltp − spot) into a TWAP over the 15:00–15:30 window,
    sampled at most once per second.  This window-average basis is then used
    to convert fut_vwap → spot_equiv so both are averaged over the exact same
    interval — eliminating the error from using a pre-window rolling median.
    Caller must hold state_lock."""
    if not (fut_ltp and spot and spot > 0):
        return
    now = now_ist()
    _reset_settlement_vwap_if_new_day(now.strftime("%Y-%m-%d"))
    t = now.time()
    if t < SETTLE_WIN_START or t > SETTLE_WIN_END:
        return
    sec_key = now.strftime("%H:%M:%S")
    if sec_key == settlement_vwap["basis_last_sec"]:
        return
    settlement_vwap["basis_last_sec"] = sec_key
    settlement_vwap["basis_sum"] += fut_ltp - spot
    settlement_vwap["basis_n"]   += 1
    settlement_vwap["basis_twap"] = settlement_vwap["basis_sum"] / settlement_vwap["basis_n"]


def _median_basis():
    """Median of recent basis samples, or None if not enough data."""
    s = sorted(_basis_samples)
    n = len(s)
    if n == 0:
        return None
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _update_fut_vwap(fut_ltp, traded_qty):
    """Accumulate futures tick into volume-weighted average during 15:00–15:30.
    Uses traded_qty (the tick's last_traded_quantity) as the volume weight.
    Every distinct tick is counted — no 1-sec throttle — so true VWAP is
    preserved across all traded lots in the settlement window.
    Falls back to equal weighting (qty=1) if qty is zero.
    Caller must hold state_lock."""
    if fut_ltp is None or fut_ltp <= 0:
        return
    now = now_ist()
    _reset_settlement_vwap_if_new_day(now.strftime("%Y-%m-%d"))
    t = now.time()
    if t < SETTLE_WIN_START or t > SETTLE_WIN_END:
        return
    qty = max(1, traded_qty or 1)
    settlement_vwap["fut_vol_sum"] += fut_ltp * qty
    settlement_vwap["fut_vol_n"]   += qty
    settlement_vwap["fut_vwap"] = settlement_vwap["fut_vol_sum"] / settlement_vwap["fut_vol_n"]


def _update_synth_vwap(ce_ltp, pe_ltp, atm_strike, ce_qty=1, pe_qty=1):
    """Accumulate synthetic spot = CE − PE + K into a volume-weighted average
    during 15:00–15:30.  ce_qty / pe_qty are the last_traded_quantity values
    from the most recent CE/PE ticks; their average is used as the weight so
    ticks with real traded volume are emphasised over quiet ticks.
    Caller must hold state_lock."""
    if ce_ltp is None or pe_ltp is None or atm_strike is None:
        return
    if ce_ltp <= 0 or pe_ltp <= 0:
        return
    now = now_ist()
    _reset_settlement_vwap_if_new_day(now.strftime("%Y-%m-%d"))
    t = now.time()
    if t < SETTLE_WIN_START or t > SETTLE_WIN_END:
        return
    sec_key = now.strftime("%H:%M:%S")
    if sec_key == settlement_vwap["synth_last_sec"]:
        return
    settlement_vwap["synth_last_sec"] = sec_key
    synth  = ce_ltp - pe_ltp + atm_strike
    weight = max(1, ((ce_qty or 1) + (pe_qty or 1)) / 2)
    settlement_vwap["synth_sum"] += synth * weight
    settlement_vwap["synth_n"]   += weight
    settlement_vwap["synth_value"] = settlement_vwap["synth_sum"] / settlement_vwap["synth_n"]


def _composite_settle(fut_spot, synth, spot_twap):
    """Weighted composite settlement estimate.
    Weights: Fut VWAP/live 50% + Synth VWAP/live 35% + Spot TWAP/live 15%.
    If a component is missing, remaining weights are renormalised so the
    estimate degrades gracefully rather than returning None."""
    components = []
    if fut_spot  is not None: components.append((fut_spot,  0.50))
    if synth     is not None: components.append((synth,     0.35))
    if spot_twap is not None: components.append((spot_twap, 0.15))
    if not components:
        return None
    total_w = sum(w for _, w in components)
    value   = sum(v * w for v, w in components) / total_w
    return round(value, 2)


def _settlement_vwap_payload():
    """Browser-friendly snapshot of all three settlement estimates.
    Caller must hold state_lock."""
    t     = now_ist().time()
    in_w  = SETTLE_WIN_START <= t <= SETTLE_WIN_END
    done  = t > SETTLE_WIN_END

    # Basis selection:
    #   During/after window → use basis_twap (same 15:00–15:30 average as
    #     fut_vwap, so numerator and denominator span identical intervals).
    #   Before window       → fall back to rolling median basis (best available).
    basis_twap   = settlement_vwap["basis_twap"]   # None until window starts
    median_basis = _median_basis()
    basis        = basis_twap if basis_twap is not None else median_basis

    # Futures VWAP → subtract window-average basis to get implied spot
    fut_vwap_raw   = settlement_vwap["fut_vwap"]
    fut_spot_equiv = round(fut_vwap_raw - basis, 2) if (fut_vwap_raw and basis is not None) else None

    # Live futures spot equivalent — visible all day, not just in window
    # Before window: uses median_basis (best available live estimate)
    # During/after:  uses basis_twap so the live display is also window-consistent
    live_basis    = basis  # same selection logic applies
    fut_ltp_live  = state.get("fut_ltp")
    fut_live_spot = round(fut_ltp_live - live_basis, 2) if (fut_ltp_live and live_basis is not None) else None

    # Live synth spot (CE - PE + K) — computed from latest ATM LTPs, all day
    atm = state.get("atm_strike")
    live_synth = None
    if atm:
        ce_rk = f"s{int(atm)}_ce"
        pe_rk = f"s{int(atm)}_pe"
        ce_tok = next((t2 for t2, m in state["tokens"].items() if m["role_key"] == ce_rk), None)
        pe_tok = next((t2 for t2, m in state["tokens"].items() if m["role_key"] == pe_rk), None)
        ce_ltp = state["oi"].get(ce_tok, {}).get("ltp") if ce_tok else None
        pe_ltp = state["oi"].get(pe_tok, {}).get("ltp") if pe_tok else None
        if ce_ltp and pe_ltp and ce_ltp > 0 and pe_ltp > 0:
            live_synth = round(ce_ltp - pe_ltp + atm, 2)

    return {
        # --- spot TWAP (existing field name preserved for frontend compat) ---
        "value":          round(settlement_vwap["value"], 2) if settlement_vwap["value"] else None,
        "n":              settlement_vwap["n"],
        "active":         in_w,
        "done":           done and settlement_vwap["value"] is not None,
        # --- futures VWAP (window average, only populated 15:00–15:30) ---
        "fut_vwap_raw":   round(fut_vwap_raw,   2) if fut_vwap_raw   else None,
        "fut_spot":       fut_spot_equiv,
        "fut_n":          settlement_vwap["fut_vol_n"],
        # --- live futures spot (visible all day) ---
        "fut_live_spot":  fut_live_spot,
        "fut_ltp":        round(fut_ltp_live, 2) if fut_ltp_live else None,
        # --- synth spot window average (only populated 15:00–15:30) ---
        "synth_value":    round(settlement_vwap["synth_value"], 2) if settlement_vwap["synth_value"] else None,
        "synth_n":        settlement_vwap["synth_n"],
        # --- live synth spot (visible all day) ---
        "live_synth":     live_synth,
        # --- basis info (window TWAP preferred; median shown before window) ---
        "basis":          round(basis, 2) if basis is not None else None,
        "basis_twap":     round(basis_twap, 2) if basis_twap is not None else None,
        "basis_n":        settlement_vwap["basis_n"],
        "basis_samples":  len(_basis_samples),
        # --- composite weighted estimate (headline Proj. Settle) ---
        # Weights: Fut VWAP 50% (true volume, most reliable) +
        #          Synth VWAP 35% (arb-enforced, volume-weighted) +
        #          Spot TWAP 15% (no volume, proxy only)
        # Falls back gracefully if any component is missing.
        "composite":      _composite_settle(fut_spot_equiv, settlement_vwap.get("synth_value"), settlement_vwap.get("value")),
        "composite_live": _composite_settle(fut_live_spot,  live_synth,                         state.get("spot")),
    }

# ── ORDER FLOW IMBALANCE (Nifty Futures) ──────────────────────────────────────
# Kite's MODE_FULL tick for the front-month future carries buy_quantity /
# sell_quantity — the AGGREGATE pending order qty across the whole exchange
# order book for that contract (not just the visible top-5 depth) — plus a
# 5-level depth snapshot. We track both:
#   - aggregate imbalance = (buy_qty - sell_qty) / (buy_qty + sell_qty)
#   - top-5 depth imbalance = same ratio using only the visible 5 levels
# and keep a rolling ~10-min history so the frontend can chart a trend into
# the close, not just show a single snapshot — useful for the last-10-min
# closing move since order flow tends to build directionally before a strong
# move rather than jumping there instantly.
#
# NOTE: this is standard Kite Connect quote/tick data (buy_quantity /
# sell_quantity / depth), NOT the new SEBI Closing Auction Session (CAS)
# fields (Reference Price / Indicative Close / Total Imbalance Qty). Those
# CAS-specific fields are confirmed live on Kite's own web UI depth panel as
# of Aug 2026 but I could not confirm they're exposed via the Kite Connect
# API yet — verify live before wiring a CAS-specific panel to real fields.
ORDER_FLOW_HISTORY_SECONDS = 600  # 10 minutes

order_flow = {
    "date":            None,
    "buy_qty":         None,   # latest aggregate buy_quantity (whole book)
    "sell_qty":        None,   # latest aggregate sell_quantity (whole book)
    "depth_buy_qty":   None,   # Σ qty across visible top-5 buy levels
    "depth_sell_qty":  None,   # Σ qty across visible top-5 sell levels
    "history":         collections.deque(maxlen=1200),  # (epoch_s, agg_ratio, depth_ratio, buy_qty, sell_qty)
    "last_sample_sec": None,
}


def _reset_order_flow_if_new_day(today):
    if order_flow["date"] != today:
        order_flow.update({
            "date": today, "buy_qty": None, "sell_qty": None,
            "depth_buy_qty": None, "depth_sell_qty": None,
            "last_sample_sec": None,
        })
        order_flow["history"].clear()


def _imbalance_ratio(buy, sell):
    """(buy - sell) / (buy + sell) → -1 (all sell) .. +1 (all buy). None if no data."""
    total = (buy or 0) + (sell or 0)
    if total <= 0:
        return None
    return round(((buy or 0) - (sell or 0)) / total, 4)


def _update_order_flow(tick):
    """Called from on_ticks for every front-month FUT tick (MODE_FULL carries
    buy_quantity/sell_quantity/depth even on ticks where price didn't change).
    state_lock must be held by the caller."""
    now = now_ist()
    _reset_order_flow_if_new_day(now.strftime("%Y-%m-%d"))

    buy_qty  = tick.get("buy_quantity")
    sell_qty = tick.get("sell_quantity")
    depth    = tick.get("depth", {}) or {}
    buy_levels  = depth.get("buy",  []) or []
    sell_levels = depth.get("sell", []) or []
    depth_buy_qty  = sum(lvl.get("quantity", 0) for lvl in buy_levels)
    depth_sell_qty = sum(lvl.get("quantity", 0) for lvl in sell_levels)

    order_flow["buy_qty"]        = buy_qty
    order_flow["sell_qty"]       = sell_qty
    order_flow["depth_buy_qty"]  = depth_buy_qty
    order_flow["depth_sell_qty"] = depth_sell_qty

    # Sample into history at most once/sec — ticks can arrive several
    # times/sec and we only need enough resolution for a 10-min sparkline.
    sec_key = int(time.time())
    if sec_key == order_flow["last_sample_sec"]:
        return
    order_flow["last_sample_sec"] = sec_key

    agg_ratio   = _imbalance_ratio(buy_qty, sell_qty)
    depth_ratio = _imbalance_ratio(depth_buy_qty, depth_sell_qty)
    order_flow["history"].append((sec_key, agg_ratio, depth_ratio, buy_qty, sell_qty))

    cutoff = sec_key - ORDER_FLOW_HISTORY_SECONDS
    while order_flow["history"] and order_flow["history"][0][0] < cutoff:
        order_flow["history"].popleft()


def _order_flow_payload():
    """Browser-friendly snapshot. Caller must hold state_lock."""
    hist = list(order_flow["history"])
    agg_ratio   = _imbalance_ratio(order_flow["buy_qty"], order_flow["sell_qty"])
    depth_ratio = _imbalance_ratio(order_flow["depth_buy_qty"], order_flow["depth_sell_qty"])

    # Trend: current ratio vs ~3 min ago — is buy/sell pressure building or fading?
    trend_3m = None
    if hist and agg_ratio is not None:
        target = hist[-1][0] - 180
        past = next((r for (t, r, *_rest) in hist if t >= target and r is not None), None)
        if past is not None:
            trend_3m = round(agg_ratio - past, 4)

    return {
        "buy_qty":         order_flow["buy_qty"],
        "sell_qty":        order_flow["sell_qty"],
        "imbalance":       agg_ratio,     # -1..+1, whole-book buy_qty vs sell_qty
        "depth_buy_qty":   order_flow["depth_buy_qty"],
        "depth_sell_qty":  order_flow["depth_sell_qty"],
        "depth_imbalance": depth_ratio,   # -1..+1, top-5 visible levels only
        "trend_3m":        trend_3m,      # +ve = buy pressure building, -ve = sell pressure building
        "market_open":     is_market_open(),
        "history": [
            {"t": t, "r": r, "dr": dr}
            for (t, r, dr, *_rest) in hist
        ],
    }


# ── INSTRUMENT HELPERS ────────────────────────────────────────────────────────

def round_to_nearest_strike(price):
    return round(price / STRIKE_STEP) * STRIKE_STEP


def compute_strikes_window(atm):
    half = NUM_STRIKES // 2
    return [atm + (i - half) * STRIKE_STEP for i in range(NUM_STRIKES)]


def compute_bear_bull(spot):
    base = round_to_nearest_strike(spot)
    return base - 100, base + 100


def get_instruments_for_strikes(strikes):
    instruments = kite.instruments(EXCHANGE)
    index_opts = [
        i for i in instruments
        if i["name"] == INSTRUMENT_NAME and i["instrument_type"] in ("CE", "PE")
    ]
    expiries = sorted(set(i["expiry"] for i in index_opts))
    front = expiries[0]
    strike_set = set(strikes)
    result = []
    for i in index_opts:
        if i["expiry"] != front or i["strike"] not in strike_set:
            continue
        k = str(int(i["strike"]))
        t = i["instrument_type"].lower()
        i["role_key"] = f"s{k}_{t}"
        result.append(i)
    found = {i["strike"] for i in result}
    missing = strike_set - found
    if missing:
        print(f"  Warning: strikes not found in {EXCHANGE}: {sorted(missing)}")
    return result


def get_futures_token():
    """Find the front-month Nifty/Sensex Futures instrument token.
    Returns (token, tradingsymbol) or (None, None) on failure."""
    try:
        instruments = kite.instruments(EXCHANGE)
        futs = [
            i for i in instruments
            if i["name"] == INSTRUMENT_NAME and i["instrument_type"] == "FUT"
        ]
        if not futs:
            print(f"[Futures] No FUT instruments found for {INSTRUMENT_NAME} on {EXCHANGE}")
            return None, None
        futs.sort(key=lambda i: i["expiry"])
        front = futs[0]
        print(f"[Futures] Front-month FUT: {front['tradingsymbol']}  token={front['instrument_token']}  expiry={front['expiry']}")
        return front["instrument_token"], front["tradingsymbol"]
    except Exception as e:
        print(f"[Futures] Failed to fetch FUT instrument: {e}")
        return None, None


def get_live_spot():
    quote = kite.quote(SPOT_QUOTE_KEY)
    return quote[SPOT_QUOTE_KEY]["last_price"]


# ── SNAPSHOT ──────────────────────────────────────────────────────────────────

def _current_snap(ts):
    snap = {"ts": ts, "spot": state["spot"]}
    for token, meta in state["tokens"].items():
        entry = state["oi"].get(token, {})
        rk = meta["role_key"]
        snap[rk + "_oi"]       = entry.get("oi", 0)
        snap[rk + "_ltp"]      = entry.get("ltp", 0)
        snap[rk + "_baseline"] = state["oi_baseline"].get(token, 0)
    return snap


# ── SPOT / LTP OHLC ───────────────────────────────────────────────────────────

def _update_spot_ohlc(spot):
    if spot is None:
        return
    if minute_buffer["spot_open"] is None:
        minute_buffer["spot_open"]  = spot
        minute_buffer["spot_high"]  = spot
        minute_buffer["spot_low"]   = spot
    else:
        if spot > minute_buffer["spot_high"]:
            minute_buffer["spot_high"] = spot
        if spot < minute_buffer["spot_low"]:
            minute_buffer["spot_low"] = spot
    minute_buffer["spot_close"] = spot


def _update_ltp_ohlc(token, ltp):
    if ltp is None or ltp == 0:
        return
    buf = minute_buffer["ltp_ohlc"]
    if token not in buf:
        buf[token] = {"open": ltp, "high": ltp, "low": ltp, "close": ltp}
    else:
        if ltp > buf[token]["high"]:
            buf[token]["high"] = ltp
        if ltp < buf[token]["low"]:
            buf[token]["low"] = ltp
        buf[token]["close"] = ltp


# ── DAY-LEVEL OPEN/HIGH/LOW TRACKER (OpenHighLow tab) ─────────────────────────
# Classic intraday "OHL scanner" logic, applied per option leg (CE/PE) and the
# front-month future — with LATCHED semantics:
#   Open = Low   → the leg moved UP off its opening print while never trading
#                  below it. Once that happens the condition is LATCHED for the
#                  rest of the day: a later tick at/below the open does NOT
#                  remove the strike from the list — it flips its status from
#                  Pending to Filled (the open level got retested/broken).
#   Open = High  → mirror image: leg moved DOWN off the open while never
#                  trading above it; a later tick at/above the open = Filled.
# A leg that breaks its open on the very first move (before ever moving away
# in the qualifying direction) never latches and never appears — it was never
# a genuine OHL candidate.
# "Open" is seeded from the EXCHANGE's own OHLC for each leg via _seed_day_oh()
# (called once at startup) — NOT from whatever tick happens to arrive first.
# That matters because this bridge can be (re)started mid-session; without the
# exchange seed, "Open" would silently become "whatever the price was when the
# process happened to restart," which is wrong and is what produced the
# Open == LTP / all-PENDING table when the feature was first deployed mid-day.
# The first-tick capture below only kicks in as a fallback if the seed call
# fails (e.g. API hiccup) for a given leg.
# A leg "fills" once price moves away from the open and then comes back to
# exactly retest that same level — the classic OHL retest entry trigger.
day_oh      = {}                     # token -> {open, high, low, moved_dn, moved_up, filled_oh, filled_ol}
day_oh_meta = {"date": None, "seeded": False}

# Prices from the REST ohlc() seed and from the binary websocket ticks are two
# independent float sources. Exact `==` between them is fragile, so all
# "Open equals High/Low" checks use a half-tick tolerance instead.
OHL_EPS = 0.02   # NSE/BSE option tick size is 0.05 — anything within 0.02 is "equal"

def _feq(a, b):
    return abs(a - b) <= OHL_EPS


def _reset_day_oh_if_new_day():
    today = now_ist().strftime("%Y-%m-%d")
    if day_oh_meta["date"] != today:
        day_oh.clear()
        day_oh_meta["date"]   = today
        day_oh_meta["seeded"] = False   # force a fresh exchange seed for the new session


def _seed_day_oh():
    """Seed day_oh with the exchange-reported day Open/High/Low for every
    tracked leg + the front-month future, via one kite.ohlc() REST call.
    Safe to re-run any time (e.g. after a reconnect) so 'Open' is always
    the real 09:15 print — correct even if the bridge restarts at, say, 11:30.

    IMPORTANT: before 09:15 IST kite.ohlc() still returns the PREVIOUS
    session's OHLC. Seeding from that poisons every Open with yesterday's
    value and silently kills the whole scanner for the day, so we refuse
    to seed outside market hours and let the auto-reseed loop do it right
    after the open instead.
    Does its own locking; do NOT call while already holding state_lock."""
    if not is_market_open():
        print("[OpenHighLow] Seed skipped — market not open yet (pre-open ohlc() "
              "would return YESTERDAY's values). Auto-reseed will run just after 09:15 IST.")
        return

    with state_lock:
        tokens     = dict(state["tokens"])
        fut_token  = state.get("fut_token")
        fut_symbol = state.get("fut_symbol")
    if not tokens and not fut_token:
        return

    key_to_token = {f"{EXCHANGE}:{meta['tradingsymbol']}": token for token, meta in tokens.items()}
    if fut_token and fut_symbol:
        key_to_token[f"{EXCHANGE}:{fut_symbol}"] = fut_token
    try:
        quotes = kite.ohlc(list(key_to_token.keys()))
    except Exception as e:
        print(f"[OpenHighLow] kite.ohlc() seed failed ({e}) — will retry via auto-reseed loop.")
        return

    seeded = 0
    with state_lock:
        _reset_day_oh_if_new_day()
        for key, token in key_to_token.items():
            q = quotes.get(key)
            if not q:
                continue
            ohlc = q.get("ohlc", {})
            o = ohlc.get("open")
            if not o:
                continue
            h = ohlc.get("high") or o
            l = ohlc.get("low") or o

            e = day_oh.get(token)
            if e and _feq(e.get("open", 0), o):
                # ── MERGE: same open → the accumulated latch/fill flags are
                # valid; never destroy them. Fold in the exchange extremes
                # (they may reveal moves that happened while we were down).
                held_low  = _feq(e["low"],  o)   # was still holding open when last seen
                held_high = _feq(e["high"], o)
                moved_up  = e["moved_up"] or h > o + OHL_EPS
                moved_dn  = e["moved_dn"] or l < o - OHL_EPS
                # Late latch: it held its open as long as we watched it, and
                # the exchange proves it moved away in the qualifying
                # direction → register it (inclusive by design).
                if not e["latched_ol"] and held_low and moved_up:
                    e["latched_ol"] = True
                if not e["latched_oh"] and held_high and moved_dn:
                    e["latched_oh"] = True
                e["moved_up"], e["moved_dn"] = moved_up, moved_dn
                e["high"] = max(e["high"], h)
                e["low"]  = min(e["low"],  l)
                # A latched leg whose open is broken per exchange → Filled.
                if e["latched_ol"] and e["low"] < o - OHL_EPS:
                    e["filled_ol"] = True
                if e["latched_oh"] and e["high"] > o + OHL_EPS:
                    e["filled_oh"] = True
            else:
                # ── REPLACE: no prior entry, or the stored open disagrees
                # with the exchange (stale/poisoned) — rebuild from scratch.
                moved_dn = l < o - OHL_EPS
                moved_up = h > o + OHL_EPS
                day_oh[token] = {
                    "open": o, "high": h, "low": l,
                    "moved_dn": moved_dn, "moved_up": moved_up,
                    "latched_ol": moved_up and _feq(l, o),
                    "latched_oh": moved_dn and _feq(h, o),
                    "filled_oh": False, "filled_ol": False,
                }
            seeded += 1
        if seeded > 0:
            day_oh_meta["seeded"] = True
    print(f"[OpenHighLow] Seeded day Open/High/Low for {seeded}/{len(key_to_token)} instruments from the exchange.")
    _db_save_day_oh()


def _auto_reseed_day_oh_loop():
    """Background loop that guarantees a valid same-day exchange seed:
    - process started pre-market  → seeds shortly after 09:15
    - process running overnight   → new-day reset clears the flag, re-seeds next open
    - startup seed failed (API hiccup) → keeps retrying every 20s until it works."""
    while True:
        try:
            now = now_ist()
            past_open_grace = (now.hour * 60 + now.minute) * 60 + now.second >= \
                              (MARKET_OPEN_HM[0] * 60 + MARKET_OPEN_HM[1]) * 60 + 20
            if is_market_open() and past_open_grace and not day_oh_meta.get("seeded"):
                _seed_day_oh()
        except Exception as e:
            print(f"[OpenHighLow] auto-reseed loop error: {e}")
        time.sleep(20)


# ── OHL latch-state persistence ──────────────────────────────────────────────
# Latched / Filled flags are derived from the OBSERVED tick sequence, so they
# can't be reconstructed from the exchange's day OHLC after a restart (open,
# high, low alone can't tell "moved up first, broke later → Filled" apart from
# "broke immediately → never a candidate"). Persisting day_oh to Postgres and
# restoring it at startup is what keeps registered strikes on the list across
# redeploys.

def _db_save_day_oh():
    """Upsert today's full day_oh dict as one JSONB blob. Cheap (one row)."""
    with state_lock:
        if not day_oh:
            return
        payload = {str(tok): dict(d) for tok, d in day_oh.items()}
        date_str = day_oh_meta.get("date")
    if not date_str:
        return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ohl_day_state (trade_date, index_name, updated_ts, data)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (trade_date, index_name)
                    DO UPDATE SET updated_ts = EXCLUDED.updated_ts, data = EXCLUDED.data
                    """,
                    (date_str, DB_TABLE, time.time(), psycopg2.extras.Json(payload)),
                )
            conn.commit()
    except Exception as e:
        print(f"[OpenHighLow] persist failed: {e}")


def _db_load_day_oh():
    """Restore today's day_oh from Postgres (if present). Run once at startup,
    BEFORE _seed_day_oh(), so the seed merges into restored latch state
    instead of starting blind."""
    today = now_ist().strftime("%Y-%m-%d")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT data FROM ohl_day_state WHERE trade_date = %s AND index_name = %s",
                    (today, DB_TABLE),
                )
                row = cur.fetchone()
    except Exception as e:
        print(f"[OpenHighLow] restore failed ({e}) — starting with empty latch state.")
        return
    if not row or not row[0]:
        print("[OpenHighLow] No saved latch state for today — fresh start.")
        return
    restored = 0
    with state_lock:
        day_oh_meta["date"] = today
        for tok_str, d in row[0].items():
            try:
                # Tolerate entries saved by older code without latch keys
                d.setdefault("latched_ol", False)
                d.setdefault("latched_oh", False)
                day_oh[int(tok_str)] = d
                restored += 1
            except (ValueError, TypeError):
                continue
    print(f"[OpenHighLow] Restored latch state for {restored} instruments from DB.")


def _day_oh_persist_loop():
    """Save the latch state every 30s during market hours (and once shortly
    after close, so the final Filled/Pending picture is kept)."""
    last_saved_after_close = None
    while True:
        try:
            if is_market_open():
                _db_save_day_oh()
                last_saved_after_close = None
            elif last_saved_after_close != now_ist().strftime("%Y-%m-%d"):
                _db_save_day_oh()
                last_saved_after_close = now_ist().strftime("%Y-%m-%d")
        except Exception as e:
            print(f"[OpenHighLow] persist loop error: {e}")
        time.sleep(30)


def _update_day_oh(token, ltp):
    """Update the day Open/High/Low + fill state for one instrument
    (option leg or the front-month future). Caller must hold state_lock."""
    if ltp is None or ltp <= 0:
        return
    # Kite pushes a cached snapshot tick immediately on subscribe (last_price =
    # previous close) and can stream indicative prices pre-open. Letting those
    # through poisons the first-tick fallback "Open" and the running high/low,
    # so day-level OHL only ever consumes ticks inside the trading session.
    if not is_market_open():
        return
    _reset_day_oh_if_new_day()
    d = day_oh.get(token)
    if d is None:
        # Fallback only — normally _seed_day_oh() has already populated this
        # instrument with the real exchange Open before any ticks arrive.
        day_oh[token] = {
            "open": ltp, "high": ltp, "low": ltp,
            "moved_dn": False, "moved_up": False,
            "latched_ol": False, "latched_oh": False,
            "filled_oh": False, "filled_ol": False,
        }
        return

    op = d["open"]

    # Latch checks run BEFORE updating high/low: on the very tick that breaks
    # the open, the opposite extreme still equals the open, so a leg that held
    # its open until now latches on this tick and is immediately marked Filled
    # — it stays on the list instead of vanishing.

    # ── Open = Low side ──
    if ltp > op + OHL_EPS:
        d["moved_up"] = True
        if not d["latched_ol"] and _feq(d["low"], op):
            d["latched_ol"] = True          # moved up while holding the open → registered
    elif d["moved_up"]:                      # ltp is back at/below the open
        if not d["latched_ol"] and _feq(d["low"], op):
            d["latched_ol"] = True          # held the open right up to this tick
        if d["latched_ol"] and not d["filled_ol"]:
            d["filled_ol"] = True           # open retested/broken → Filled (leg stays listed)

    # ── Open = High side ──
    if ltp < op - OHL_EPS:
        d["moved_dn"] = True
        if not d["latched_oh"] and _feq(d["high"], op):
            d["latched_oh"] = True          # moved down while holding the open → registered
    elif d["moved_dn"]:                      # ltp is back at/above the open
        if not d["latched_oh"] and _feq(d["high"], op):
            d["latched_oh"] = True          # held the open right up to this tick
        if d["latched_oh"] and not d["filled_oh"]:
            d["filled_oh"] = True           # open retested/broken → Filled (leg stays listed)

    # Now update running high/low for the day.
    if ltp > d["high"]:
        d["high"] = ltp
    if ltp < d["low"]:
        d["low"] = ltp


def _open_high_low_snapshot_unlocked():
    """Build the OpenHighLow tab payload — one row per strike, CE and PE side
    by side. A strike only appears in a list if at least one leg currently
    satisfies that condition. Caller must hold state_lock."""
    by_strike = {}
    for token, meta in state["tokens"].items():
        by_strike.setdefault(meta["strike"], {})[meta["instrument_type"].lower()] = token

    def leg_info(token):
        if token is None:
            return None
        d = day_oh.get(token)
        if not d:
            return None
        snap = state["oi"].get(token, {})
        return {
            "open":      round(d["open"], 2),
            "high":      round(d["high"], 2),
            "low":       round(d["low"], 2),
            "ltp":       snap.get("ltp", 0),
            # Latched → stays listed all day even if the open later breaks;
            # the _feq fallback keeps freshly-opened legs (still sitting on
            # their open, not yet latched) visible too.
            "is_oh":     d.get("latched_oh", False) or _feq(d["high"], d["open"]),
            "is_ol":     d.get("latched_ol", False) or _feq(d["low"],  d["open"]),
            "filled_oh": d["filled_oh"],
            "filled_ol": d["filled_ol"],
        }

    # ── Active-month future OHL (same scanner logic, one instrument) ──
    future = None
    fut_token = state.get("fut_token")
    if fut_token:
        d = day_oh.get(fut_token)
        if d:
            future = {
                "symbol":    state.get("fut_symbol") or "FUT",
                "open":      round(d["open"], 2),
                "high":      round(d["high"], 2),
                "low":       round(d["low"], 2),
                "ltp":       state.get("fut_ltp") or 0,
                "is_oh":     d.get("latched_oh", False) or _feq(d["high"], d["open"]),
                "is_ol":     d.get("latched_ol", False) or _feq(d["low"],  d["open"]),
                "filled_oh": d["filled_oh"],
                "filled_ol": d["filled_ol"],
            }

    open_high, open_low = [], []
    for strike in sorted(by_strike):
        toks = by_strike[strike]
        ce = leg_info(toks.get("ce"))
        pe = leg_info(toks.get("pe"))

        if (ce and ce["is_oh"]) or (pe and pe["is_oh"]):
            open_high.append({
                "strike": strike,
                "ce": {"open": ce["open"], "ltp": ce["ltp"],
                       "status": "filled" if ce["filled_oh"] else "pending"}
                      if (ce and ce["is_oh"]) else None,
                "pe": {"open": pe["open"], "ltp": pe["ltp"],
                       "status": "filled" if pe["filled_oh"] else "pending"}
                      if (pe and pe["is_oh"]) else None,
            })

        if (ce and ce["is_ol"]) or (pe and pe["is_ol"]):
            open_low.append({
                "strike": strike,
                "ce": {"open": ce["open"], "ltp": ce["ltp"],
                       "status": "filled" if ce["filled_ol"] else "pending"}
                      if (ce and ce["is_ol"]) else None,
                "pe": {"open": pe["open"], "ltp": pe["ltp"],
                       "status": "filled" if pe["filled_ol"] else "pending"}
                      if (pe and pe["is_ol"]) else None,
            })

    return {
        "as_of":     now_ist().strftime("%H:%M:%S"),
        "seeded":    day_oh_meta.get("seeded", False),
        "future":    future,
        "open_high": open_high,
        "open_low":  open_low,
    }


# ── 1-MINUTE AGGREGATOR ───────────────────────────────────────────────────────

def _append_history():
    now          = time.time()
    current_snap = _current_snap(now)

    if minute_buffer["start_ts"] is None:
        minute_buffer["start_ts"]   = now
        minute_buffer["start_snap"] = current_snap
        return

    if now - minute_buffer["start_ts"] < 60:
        return

    start = minute_buffer["start_snap"]
    close = current_snap

    # ── Market-hours guard ────────────────────────────────────────────
    # The candle's identity is its START timestamp. Only write it if that
    # start time falls within official market hours. This prevents after-
    # hours ticks (exchange still streams stale data after 15:30) from
    # creating flat/no-move candles in the DB.
    candle_start_ist = ts_to_ist(minute_buffer["start_ts"])
    if not is_market_open(candle_start_ist):
        # Reset buffer so next market-open starts clean
        minute_buffer["start_ts"]   = None
        minute_buffer["start_snap"] = None
        minute_buffer["spot_open"]  = None
        minute_buffer["spot_high"]  = None
        minute_buffer["spot_low"]   = None
        minute_buffer["spot_close"] = None
        minute_buffer["ltp_ohlc"]   = {}
        return
    # ─────────────────────────────────────────────────────────────────

    row = {
        "ts":          minute_buffer["start_ts"],
        "time_label":  ts_to_ist(minute_buffer["start_ts"]).strftime("%H:%M"),
        "session_id":  state["session_id"],
        "bear_strike": state["bear_strike"],
        "bull_strike": state["bull_strike"],
        "spot_open":   round(minute_buffer["spot_open"])  if minute_buffer["spot_open"]  else 0,
        "spot_high":   round(minute_buffer["spot_high"])  if minute_buffer["spot_high"]  else 0,
        "spot_low":    round(minute_buffer["spot_low"])   if minute_buffer["spot_low"]   else 0,
        "spot_close":  round(minute_buffer["spot_close"]) if minute_buffer["spot_close"] else 0,
    }

    for token, meta in state["tokens"].items():
        rk = meta["role_key"]
        close_ltp = close.get(rk + "_ltp", 0)
        ohlc = minute_buffer["ltp_ohlc"].get(token, {})
        row[rk + "_oi"]        = close.get(rk + "_oi", 0)
        row[rk + "_ltp"]       = close_ltp
        row[rk + "_ltp_open"]  = ohlc.get("open",  close_ltp)
        row[rk + "_ltp_high"]  = ohlc.get("high",  close_ltp)
        row[rk + "_ltp_low"]   = ohlc.get("low",   close_ltp)
        row[rk + "_ltp_close"] = ohlc.get("close", close_ltp)
        row[rk + "_baseline"]  = close.get(rk + "_baseline", 0)
        row[rk + "_delta"]     = close.get(rk + "_oi", 0) - start.get(rk + "_oi", 0)

    # Write to PostgreSQL (non-blocking — do it in a thread to avoid holding state_lock)
    threading.Thread(target=db_write_row, args=(row,), daemon=True).start()
    oi_history.append(row)

    print(
        f"[{row['time_label']}] "
        f"O={row['spot_open']} H={row['spot_high']} L={row['spot_low']} C={row['spot_close']}  "
        f"bear={row['bear_strike']} bull={row['bull_strike']} session={row['session_id']}"
    )

    minute_buffer["start_ts"]   = now
    minute_buffer["start_snap"] = current_snap
    minute_buffer["spot_open"]  = minute_buffer["spot_close"]
    minute_buffer["spot_high"]  = minute_buffer["spot_close"]
    minute_buffer["spot_low"]   = minute_buffer["spot_close"]
    new_ltp_ohlc = {}
    for token in minute_buffer["ltp_ohlc"]:
        last_close = minute_buffer["ltp_ohlc"][token]["close"]
        new_ltp_ohlc[token] = {"open": last_close, "high": last_close, "low": last_close, "close": last_close}
    minute_buffer["ltp_ohlc"] = new_ltp_ohlc


# ── TICKER ────────────────────────────────────────────────────────────────────

ticker_instance = None


def build_ticker():
    global ticker_instance
    if ticker_instance:
        try:
            ticker_instance.close()
        except Exception:
            pass
        time.sleep(1)

    ticker_instance = KiteTicker(API_KEY, ACCESS_TOKEN)

    def on_ticks(ws, ticks):
        with state_lock:
            for tick in ticks:
                token = tick["instrument_token"]

                if token == SPOT_TOKEN:
                    new_spot = tick.get("last_price", state["spot"])
                    state["spot"] = new_spot
                    _update_spot_ohlc(new_spot)
                    _update_settlement_vwap(new_spot)
                    _check_roll(new_spot)
                    # Refresh basis whenever spot updates
                    if state["fut_ltp"]:
                        _update_basis(state["fut_ltp"], new_spot)
                    continue

                if token == state["fut_token"]:
                    fut_ltp = tick.get("last_price", 0)
                    qty     = tick.get("last_traded_quantity", 0) or tick.get("volume_traded", 0) or 1
                    if fut_ltp and fut_ltp > 0:
                        state["fut_ltp"] = fut_ltp
                        _update_fut_vwap(fut_ltp, qty)
                        _update_day_oh(token, fut_ltp)   # active-month future OHL scan
                        if state["spot"]:
                            _update_basis(fut_ltp, state["spot"])
                    # buy_quantity/sell_quantity/depth can update independently
                    # of price, so this runs on every FUT tick, not just
                    # price-changing ones.
                    _update_order_flow(tick)
                    continue

                if token not in state["tokens"]:
                    continue

                current_oi  = tick.get("oi", 0)
                current_ltp = tick.get("last_price", 0)
                current_qty = tick.get("last_traded_quantity", 0) or tick.get("volume_traded", 0) or 1

                # Day OHL scanner must see every traded tick — deep OTM legs can
                # briefly report oi=0 early in the session, and skipping those
                # ticks here used to blind the OpenHighLow tracker to them.
                _update_day_oh(token, current_ltp)

                if current_oi == 0:
                    continue

                if token not in state["oi_baseline"]:
                    state["oi_baseline"][token] = current_oi
                    state["oi_prev_snap"][token] = current_oi
                    rk = state["tokens"][token]["role_key"]
                    print(f"[{now_ist().strftime('%H:%M:%S')}] Baseline — {rk}: OI={current_oi:,}")

                depth_raw   = tick.get("depth", {})
                buy_levels  = depth_raw.get("buy",  [])
                sell_levels = depth_raw.get("sell", [])
                state["oi"][token] = {
                    "oi":   current_oi,
                    "ltp":  current_ltp,
                    "qty":  current_qty,
                    "buy_qty":  tick.get("buy_quantity"),   # whole-book aggregate (Total row in Kite depth)
                    "sell_qty": tick.get("sell_quantity"),
                    "depth": {"buy": buy_levels, "sell": sell_levels},
                }
                _update_ltp_ohlc(token, current_ltp)

            # ── Synthetic spot from ATM CE − PE + K ─────────────────────────
            # Computed once per tick batch (after all ticks processed) using
            # the latest ATM strike and its CE/PE LTPs from state["oi"].
            # CE/PE traded quantities are passed so the synth VWAP is volume-
            # weighted (busy ticks count more than quiet ones).
            atm = state["atm_strike"]
            if atm:
                ce_rk = f"s{int(atm)}_ce"
                pe_rk = f"s{int(atm)}_pe"
                ce_tok = next((t for t, m in state["tokens"].items() if m["role_key"] == ce_rk), None)
                pe_tok = next((t for t, m in state["tokens"].items() if m["role_key"] == pe_rk), None)
                ce_data = state["oi"].get(ce_tok, {}) if ce_tok else {}
                pe_data = state["oi"].get(pe_tok, {}) if pe_tok else {}
                ce_ltp  = ce_data.get("ltp")
                pe_ltp  = pe_data.get("ltp")
                ce_qty  = ce_data.get("qty", 1)
                pe_qty  = pe_data.get("qty", 1)
                _update_synth_vwap(ce_ltp, pe_ltp, atm, ce_qty, pe_qty)

            _append_history()

    def on_connect(ws, response):
        with state_lock:
            all_tokens = [SPOT_TOKEN] + list(state["tokens"].keys())
            fut_token  = state["fut_token"]
        if fut_token:
            all_tokens.append(fut_token)
        ws.subscribe(all_tokens)
        ws.set_mode(ws.MODE_FULL, all_tokens)
        n_opts = len(all_tokens) - 1 - (1 if fut_token else 0)
        print(f"[{now_ist().strftime('%H:%M:%S')}] Subscribed: {INDEX_LABEL} spot + {n_opts} option tokens" +
              (f" + futures" if fut_token else ""))

    def on_error(ws, code, reason):
        print(f"Ticker error {code}: {reason}")

    def on_close(ws, code, reason):
        print(f"Ticker closed: {reason}.")
        if not is_market_open():
            print(f"[{now_ist().strftime('%H:%M:%S')}] Market closed — not reconnecting.")
            return
        print("Reconnecting in 5s...")
        time.sleep(5)
        build_ticker()

    ticker_instance.on_ticks   = on_ticks
    ticker_instance.on_connect = on_connect
    ticker_instance.on_error   = on_error
    ticker_instance.on_close   = on_close
    ticker_instance.connect(threaded=True)


# ── STRIKE ROLL ───────────────────────────────────────────────────────────────

def _check_roll(new_spot):
    if state["last_anchored"] is None or new_spot is None:
        return
    if abs(new_spot - state["last_anchored"]) < ROLL_THRESHOLD:
        return

    bear, bull = compute_bear_bull(new_spot)
    min_s = min(state["strikes"])
    max_s = max(state["strikes"])
    bear  = max(min_s, min(bear, max_s))
    bull  = max(min_s, min(bull, max_s))

    if bear >= bull:
        strikes_sorted = sorted(state["strikes"])
        below = [s for s in strikes_sorted if s <= new_spot]
        above = [s for s in strikes_sorted if s > new_spot]
        bear  = below[-1] if below else strikes_sorted[0]
        bull  = above[0]  if above else strikes_sorted[-1]
        if bear == bull and len(strikes_sorted) > 1:
            idx  = strikes_sorted.index(bear)
            bear = strikes_sorted[max(0, idx - 1)]
            bull = strikes_sorted[min(len(strikes_sorted) - 1, idx + 1)]

    if bear == state["bear_strike"] and bull == state["bull_strike"]:
        state["last_anchored"] = new_spot
        return

    prev_bear = state["bear_strike"]
    prev_bull = state["bull_strike"]

    state["session_id"]   += 1
    state["bear_strike"]   = bear
    state["bull_strike"]   = bull
    state["last_anchored"] = new_spot

    state["roll_log"].append({
        "time":       now_ist().strftime("%H:%M:%S"),
        "session_id": state["session_id"],
        "from_spot":  round(new_spot),
        "from_bear":  prev_bear,
        "from_bull":  prev_bull,
        "to_bear":    bear,
        "to_bull":    bull,
    })

    print(
        f"[{now_ist().strftime('%H:%M:%S')}] "
        f"Roll → Bear {prev_bear}→{bear}  Bull {prev_bull}→{bull}  "
        f"Session {state['session_id']}"
    )


# ── STARTUP ───────────────────────────────────────────────────────────────────

def initialise(seed_spot):
    atm     = round_to_nearest_strike(seed_spot)
    strikes = compute_strikes_window(atm)
    bear, bull = compute_bear_bull(seed_spot)
    bear = max(min(strikes), min(bear, max(strikes)))
    bull = max(min(strikes), min(bull, max(strikes)))

    if bear >= bull:
        strikes_sorted = sorted(strikes)
        below = [s for s in strikes_sorted if s <= seed_spot]
        above = [s for s in strikes_sorted if s > seed_spot]
        bear  = below[-1] if below else strikes_sorted[0]
        bull  = above[0]  if above else strikes_sorted[-1]

    print(f"\nATM: {atm}  |  Window: {strikes[0]} – {strikes[-1]}")
    print(f"Initial bear: {bear}  bull: {bull}")

    instruments = get_instruments_for_strikes(strikes)
    print(f"\nInstruments found: {len(instruments)} (expected {len(strikes) * 2})")
    for i in instruments:
        print(f"  {i['role_key']:15s}: {i['tradingsymbol']:25s}  token={i['instrument_token']}  expiry={i['expiry']}")

    token_map = {i["instrument_token"]: i for i in instruments}

    # Derive expiry_date from any instrument (all share the same front expiry)
    front_expiry = None
    if instruments:
        raw_exp = instruments[0].get("expiry")
        if raw_exp:
            front_expiry = str(raw_exp)[:10]   # normalise to YYYY-MM-DD

    with state_lock:
        state["atm_strike"]    = atm
        state["strikes"]       = strikes
        state["bear_strike"]   = bear
        state["bull_strike"]   = bull
        state["last_anchored"] = seed_spot
        state["tokens"]        = token_map
        if front_expiry:
            state["expiry_date"] = front_expiry

    print("\nFetching front-month Futures token...")
    fut_token, fut_sym = get_futures_token()
    with state_lock:
        state["fut_token"]  = fut_token
        state["fut_symbol"] = fut_sym
    if fut_token:
        print(f"[Futures] Subscribed: {fut_sym} (token {fut_token})")
    else:
        print("[Futures] No FUT token — futures VWAP estimate will be unavailable.")

    print("\nRestoring OpenHighLow latch state from DB (survives redeploys)...")
    _db_load_day_oh()

    print("Seeding OpenHighLow day levels from exchange OHLC...")
    _seed_day_oh()
    # Guarantees a valid same-day seed even if the bridge started pre-market,
    # runs overnight into a new session, or the startup seed call failed.
    threading.Thread(target=_auto_reseed_day_oh_loop, daemon=True).start()
    threading.Thread(target=_day_oh_persist_loop,     daemon=True).start()

    build_ticker()


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────

@app.route("/")
def serve_dashboard():
    """Serve the dashboard HTML — the public URL entry point."""
    return send_from_directory("static", "index.html")


@app.route("/oi")
def get_oi():
    with state_lock:
        result = {
            "spot":        round(state["spot"]) if state["spot"] else None,
            "session_id":  state["session_id"],
            "atm_strike":  state["atm_strike"],
            "bear_strike": state["bear_strike"],
            "bull_strike": state["bull_strike"],
            "strikes":     state["strikes"],
            "roll_log":    list(state["roll_log"]),
            "as_of":       now_ist().strftime("%H:%M:%S"),
            "expiry_date": state["expiry_date"],
            "options":     {},
        }
        for token, meta in state["tokens"].items():
            snap       = state["oi"].get(token, {})
            current_oi = snap.get("oi", 0)
            baseline   = state["oi_baseline"].get(token, current_oi)
            prev_snap  = state["oi_prev_snap"].get(token, current_oi)
            rk         = meta["role_key"]
            result["options"][rk] = {
                "strike":                    meta["strike"],
                "type":                      meta["instrument_type"],
                "symbol":                    meta["tradingsymbol"],
                "ltp":                       snap.get("ltp", 0),
                "oi":                        current_oi,
                "oi_change_total":           current_oi - baseline,
                "oi_change_since_last_poll": current_oi - prev_snap,
                "baseline_oi":               baseline,
            }
            state["oi_prev_snap"][token] = current_oi
    return jsonify(result)


@app.route("/ltp")
def get_ltp():
    with state_lock:
        spot = state["spot"]
        T    = _years_to_expiry(state["expiry_date"])
        result = {
            "spot":    spot,
            "fut_ltp": state["fut_ltp"],
            "expiry_date": state["expiry_date"],
            "as_of":   now_ist().strftime("%H:%M:%S"),
            "settlement_vwap": _settlement_vwap_payload(),
            "order_flow": _order_flow_payload(),
            "options": {},
        }
        legs = []
        for token, meta in state["tokens"].items():
            snap = state["oi"].get(token, {})
            ltp  = snap.get("ltp", 0)
            oi   = snap.get("oi", 0)
            K    = float(meta["strike"])
            otype = meta["instrument_type"]
            delta, gamma, iv = option_greeks(spot, K, T, ltp, otype)
            gex = _leg_gex(spot, gamma, oi, otype)
            legs.append({"strike": int(K), "type": otype, "iv": iv, "oi": oi, "gex": gex})
            result["options"][meta["role_key"]] = {
                "strike": meta["strike"],
                "type":   otype,
                "symbol": meta["tradingsymbol"],
                "ltp":    ltp,
                "delta":  delta,
                "gamma":  round(gamma, 7) if gamma is not None else None,
                "iv":     round(iv * 100, 2) if iv else None,
                "gex":    round(gex),                 # ₹ per 1% move, signed
                "buy_qty":  snap.get("buy_qty"),
                "sell_qty": snap.get("sell_qty"),
            }
        result["gex"] = _gex_summary(spot, T, legs, STRIKE_STEP)
        # Piggyback the OpenHighLow snapshot on the existing 1-second LTP poll
        # so that tab gets tick-by-tick refresh for free, with no extra timer.
        result["open_high_low"] = _open_high_low_snapshot_unlocked()
    return jsonify(result)


@app.route("/oi/openhighlow")
def get_open_high_low():
    """Standalone snapshot — same data as the open_high_low key inside /ltp.
    Used for the initial render when the OpenHighLow tab is opened."""
    with state_lock:
        return jsonify(_open_high_low_snapshot_unlocked())


@app.route("/oi/openhighlow/reseed", methods=["POST"])
def reseed_open_high_low():
    """Re-pull today's Open/High/Low from the exchange for every tracked leg.
    Use this to fix a wrong 'Open' without restarting the whole bridge — e.g.
    if the bridge was (re)deployed mid-session before this seed existed, or
    after any temporary kite.ohlc() failure at startup."""
    try:
        _seed_day_oh()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    with state_lock:
        snap = _open_high_low_snapshot_unlocked()
    return jsonify({"status": "ok", "as_of": snap["as_of"]})


@app.route("/oi/openhighlow/debug")
def openhighlow_debug():
    """Explain exactly why a given strike is / is not on the OHL lists.
    Usage: /oi/openhighlow/debug?strike=24000
    For each leg (CE/PE) it returns:
      - tracked: is the token subscribed at all (strike inside the window)?
      - internal: the bridge's own day_oh entry (open/high/low + flags)
      - exchange: a LIVE kite.ohlc() pull for the same leg
      - verdict: 'not tracked' / 'ohl seed missing' / diff between the two views
    If internal.low < internal.open but exchange.low == exchange.open, a tick
    the exchange later cancelled/adjusted (or a bad tick) contaminated the
    internal low — hit /oi/openhighlow/reseed to resync from the exchange."""
    strike = request.args.get("strike", type=float)
    if strike is None:
        return jsonify({"status": "error", "message": "pass ?strike=24000"}), 400

    with state_lock:
        window   = sorted(state["strikes"]) if state["strikes"] else []
        legs = {}
        for token, meta in state["tokens"].items():
            if float(meta["strike"]) == strike:
                legs[meta["instrument_type"].upper()] = {
                    "token": token, "tradingsymbol": meta["tradingsymbol"],
                }
        internal = {}
        for side, leg in legs.items():
            d = day_oh.get(leg["token"])
            internal[side] = dict(d) if d else None
        seeded = day_oh_meta.get("seeded", False)

    if not legs:
        return jsonify({
            "status": "ok", "strike": strike, "tracked": False,
            "verdict": f"Strike {strike:g} is NOT in the subscribed window "
                       f"({window[0]:g}–{window[-1]:g})" if window else "No window initialised",
            "window": window,
        })

    # Live exchange view for the same legs (single REST call)
    keys = {f"{EXCHANGE}:{leg['tradingsymbol']}": side for side, leg in legs.items()}
    exchange = {}
    try:
        quotes = kite.ohlc(list(keys.keys()))
        for key, side in keys.items():
            q = quotes.get(key) or {}
            exchange[side] = {"ohlc": q.get("ohlc"), "last_price": q.get("last_price")}
    except Exception as e:
        exchange = {"error": str(e)}

    out = {"status": "ok", "strike": strike, "tracked": True, "seeded": seeded,
           "window": [window[0], window[-1]] if window else None, "legs": {}}
    for side, leg in legs.items():
        d  = internal.get(side)
        ex = exchange.get(side) if isinstance(exchange, dict) else None
        verdict = []
        if d is None:
            verdict.append("no internal day_oh entry — leg never seeded and no tick seen yet")
        else:
            if d.get("latched_ol"):
                verdict.append("OPEN = LOW latched ✓ — stays listed all day"
                               + (" (Filled: open was retested/broken)" if d["filled_ol"] else " (Pending)"))
            elif _feq(d["low"], d["open"]):
                verdict.append("currently holding Open = Low (not yet latched — no up-move seen)")
            else:
                verdict.append(f"never latched Open=Low: low {d['low']} broke open {d['open']} "
                               f"before any qualifying up-move")
            if d.get("latched_oh"):
                verdict.append("OPEN = HIGH latched ✓ — stays listed all day"
                               + (" (Filled: open was retested/broken)" if d["filled_oh"] else " (Pending)"))
            elif _feq(d["high"], d["open"]):
                verdict.append("currently holding Open = High (not yet latched — no down-move seen)")
        if ex and ex.get("ohlc") and d:
            eo, el, eh = ex["ohlc"].get("open"), ex["ohlc"].get("low"), ex["ohlc"].get("high")
            if eo is not None and abs(eo - d["open"]) > OHL_EPS:
                verdict.append(f"MISMATCH: exchange open {eo} vs internal open {d['open']} → stale seed, hit /oi/openhighlow/reseed")
            if el is not None and abs(el - d["low"]) > OHL_EPS:
                verdict.append(f"MISMATCH: exchange low {el} vs internal low {d['low']} → bad/contaminating tick, hit /oi/openhighlow/reseed")
        out["legs"][side] = {"tradingsymbol": leg["tradingsymbol"],
                             "internal": d, "exchange": ex, "verdict": verdict}
    return jsonify(out)


@app.route("/strikes")
def get_strikes():
    with state_lock:
        return jsonify({
            "atm_strike":  state["atm_strike"],
            "bear_strike": state["bear_strike"],
            "bull_strike": state["bull_strike"],
            "strikes":     state["strikes"],
            "session_id":  state["session_id"],
            "expiry_date": state["expiry_date"],
        })


@app.route("/oi/history")
def get_history():
    """
    Returns ALL 1-min rows for a given date (or today if no date param).
    Query param: ?date=YYYY-MM-DD
    Reads from PostgreSQL — falls back to in-memory deque for today if DB unavailable.
    """
    date_param = request.args.get("date", "").strip()
    if date_param:
        # Validate format
        try:
            from datetime import date as _date
            _date.fromisoformat(date_param)   # raises ValueError if invalid
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        try:
            rows = db_read_by_date(date_param)
            return jsonify(rows)
        except Exception as e:
            print(f"DB read error for date {date_param}: {e}")
            return jsonify([])
    else:
        try:
            rows = db_read_today()
            return jsonify(rows)
        except Exception as e:
            print(f"DB read error, falling back to memory: {e}")
            return jsonify(list(oi_history))


@app.route("/oi/dates")
def get_dates():
    """
    Returns list of distinct trade dates that have data, newest first.
    Response: { "dates": ["2025-04-28", "2025-04-25", ...] }
    """
    try:
        dates = db_list_dates()
        return jsonify({"dates": dates})
    except Exception as e:
        print(f"DB list dates error: {e}")
        return jsonify({"dates": []})



@app.route("/health")
def health():
    with state_lock:
        return jsonify({
            "status":           "ok",
            "spot":             state["spot"],
            "session_id":       state["session_id"],
            "atm_strike":       state["atm_strike"],
            "bear_strike":      state["bear_strike"],
            "bull_strike":      state["bull_strike"],
            "strikes":          state["strikes"],
            "tokens_tracked":   len(state["tokens"]),
            "history_rows":     len(oi_history),
            "roll_count":       len(state["roll_log"]),
            "spot_ohlc_buffer": {
                "open":  minute_buffer["spot_open"],
                "high":  minute_buffer["spot_high"],
                "low":   minute_buffer["spot_low"],
                "close": minute_buffer["spot_close"],
            },
        })


@app.route("/reset-csv", methods=["POST"])
def reset_csv():
    """Wipe today's rows from DB + in-memory buffer. Call each morning."""
    try:
        db_reset_today()
    except Exception as e:
        print(f"DB reset error: {e}")
    oi_history.clear()
    with state_lock:
        state["session_id"]            = 0
        minute_buffer["start_ts"]      = None
        minute_buffer["start_snap"]    = None
        minute_buffer["spot_open"]     = None
        minute_buffer["spot_high"]     = None
        minute_buffer["spot_low"]      = None
        minute_buffer["spot_close"]    = None
        minute_buffer["ltp_ohlc"]      = {}
    print(f"[{now_ist().strftime('%H:%M:%S')}] DB reset. Session 0.")
    return jsonify({"status": "ok", "message": "Today's rows cleared. Session reset to 0."})


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print(f"{INDEX_LABEL} OI Bias Monitor — Bridge (Railway / PostgreSQL edition)")
    print(f"Index: {INDEX_LABEL}  |  Table: {DB_TABLE}  |  Exchange: {EXCHANGE}")
    print("=" * 60)

    print("\nInitialising database...")
    init_db()

    print(f"\nFetching live {INDEX_LABEL} spot...")
    seed_spot = get_live_spot()
    print(f"Live spot: {seed_spot:,.2f}")

    half = NUM_STRIKES // 2
    print(f"\nInitialising {NUM_STRIKES}-strike window (ATM ±{half} strikes)...")
    initialise(seed_spot)

    # ── Scheduled market-close shutdown ───────────────────────────────
    # At 15:30 IST the ticker is stopped so no after-hours ticks arrive.
    # Flask keeps running so the dashboard stays readable after close.
    def _schedule_market_close():
        now = now_ist()
        close_today = now.replace(
            hour=MARKET_CLOSE_HM[0], minute=MARKET_CLOSE_HM[1],
            second=0, microsecond=0
        )
        wait = (close_today - now).total_seconds()
        if wait <= 0:
            return   # already past close today — nothing to schedule
        print(f"[{now.strftime('%H:%M:%S')}] Ticker auto-stop scheduled at "
              f"{MARKET_CLOSE_HM[0]:02d}:{MARKET_CLOSE_HM[1]:02d} IST "
              f"({int(wait//60)}m away).")
        time.sleep(wait)
        global ticker_instance
        if ticker_instance:
            try:
                ticker_instance.close()
            except Exception:
                pass
        print(f"[{now_ist().strftime('%H:%M:%S')}] Market closed — ticker stopped. "
              f"Dashboard remains live for review.")

    threading.Thread(target=_schedule_market_close, daemon=True).start()
    # ─────────────────────────────────────────────────────────────────

    print(f"\nFlask listening on port {FLASK_PORT}")
    print(f"  GET  /                serves dashboard HTML")
    print(f"  GET  /oi              all strikes snapshot")
    print(f"  GET  /ltp             latest LTP snapshot")
    print(f"  GET  /oi/live-candle  latest forming candle snapshot")
    print(f"  GET  /                serves dashboard HTML")
    print(f"  GET  /oi              all strikes snapshot")
    print(f"  GET  /ltp             latest LTP + delta/gamma/GEX snapshot")
    print(f"  GET  /strikes         strike list")
    print(f"  GET  /oi/history      1-min rows + spot OHLC (today, or ?date=YYYY-MM-DD)")
    print(f"  GET  /oi/dates        dates that have history")
    print(f"  GET  /oi/openhighlow  OpenHighLow tab snapshot (CE/PE Open=High & Open=Low)")
    print(f"  POST /oi/openhighlow/reseed  re-pull today's Open/High/Low from the exchange")
    print(f"  GET  /oi/openhighlow/debug   ?strike=N — why a strike is / isn't listed")
    print(f"  GET  /health          status + OHLC buffer")
    print(f"  POST /reset-csv       wipe today's rows")
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)

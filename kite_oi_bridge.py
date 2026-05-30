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

Endpoints (unchanged):
    GET  /                 → serves the dashboard HTML
    GET  /oi               → snapshot for ALL strikes + active bear/bull
    GET  /ltp              → live LTP for all tokens
    GET  /oi/history       → 1-min rows for today (all strikes + OHLC)
    GET  /oi/live-candle   → currently forming 1-min candle
    GET  /strikes          → strike list
    POST /depth/watch/start            → create depth watcher (strike, type, seconds up to 600)
    GET  /depth/watch/stream/<id>      → SSE: live tick-by-tick depth events
    POST /depth/watch/stop/<id>        → cancel early
    GET  /depth/watch/result/<id>      → full result after completion
    GET  /health           → status + OHLC buffer
    POST /reset-csv        → wipe today's rows, start fresh
"""

import collections
import json
import os
import threading
import queue
import time
from datetime import datetime, timezone, timedelta

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
from flask import Flask, jsonify, request, send_from_directory, Response
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

def compute_iv_snapshot():
    """
    Compute IV and delta for every tracked strike using current LTPs.
    Returns dict keyed by role_key: {strike, type, ltp, iv (%), delta}.
    """
    RISK_FREE = 0.065   # approximate RBI repo rate annualised

    with state_lock:
        spot       = state["spot"]
        expiry_str = state["expiry_date"]
        tokens     = dict(state["tokens"])
        oi_snap    = dict(state["oi"])

    if not spot or not expiry_str:
        return {}

    try:
        from datetime import date as _date
        today      = now_ist().date()
        expiry     = _date.fromisoformat(expiry_str[:10])
        days_left  = max(0, (expiry - today).days)
    except Exception:
        return {}

    T = max(days_left / 365.0, 1 / 365.0)   # floor at 1 day to avoid T=0 on expiry day

    result = {}
    for token, meta in tokens.items():
        ltp = oi_snap.get(token, {}).get("ltp", 0)
        if not ltp or ltp <= 0:
            continue
        K        = float(meta["strike"])
        opt_type = meta["instrument_type"]   # "CE" or "PE"
        iv = compute_iv(float(spot), K, T, RISK_FREE, float(ltp), opt_type)
        if iv is None:
            continue
        delta = _bs_delta(float(spot), K, T, RISK_FREE, iv, opt_type)
        result[meta["role_key"]] = {
            "strike": int(K),
            "type":   opt_type,
            "ltp":    round(ltp, 2),
            "iv":     round(iv * 100, 2),
            "delta":  round(delta, 3),
        }
    return result

def _find_by_delta(iv_map, target_delta, opt_type, tolerance=0.06):
    """Return the iv_map entry closest to target_delta for a given opt_type."""
    candidates = [
        v for v in iv_map.values()
        if v["type"] == opt_type and abs(v["delta"] - target_delta) <= tolerance
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda x: abs(x["delta"] - target_delta))


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

# OI_NUM_STRIKES: total strikes to track (must be odd so ATM sits in the middle).
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

# ── BROWSER LIVE STREAM (Server-Sent Events) ─────────────────────────────────
# The Kite WebSocket already gives tick-by-tick updates to this backend.
# These queues let Flask push the latest snapshot to all connected browsers
# immediately, instead of waiting for /ltp or /oi/live-candle polling.
stream_subscribers = []
stream_lock = threading.Lock()

def _make_live_payload_unlocked(elapsed_override=None):
    """Build one browser-friendly payload. Caller must hold state_lock."""
    now_ts = time.time()
    payload = {
        "type": "tick",
        "as_of": now_ist().strftime("%H:%M:%S"),
        "spot": state["spot"],
        "session_id": state["session_id"],
        "atm_strike": state["atm_strike"],
        "bear_strike": state["bear_strike"],
        "bull_strike": state["bull_strike"],
        "expiry_date": state["expiry_date"],
        "options": {},
        "live_candle": {"available": False},
    }

    for token, meta in state["tokens"].items():
        snap = state["oi"].get(token, {})
        payload["options"][meta["role_key"]] = {
            "strike": meta["strike"],
            "type": meta["instrument_type"],
            "symbol": meta["tradingsymbol"],
            "ltp": snap.get("ltp", 0),
            "oi": snap.get("oi", 0),
        }

    if minute_buffer["start_ts"] is not None and minute_buffer["start_snap"] is not None:
        start = minute_buffer["start_snap"]
        elapsed = elapsed_override if elapsed_override is not None else round(now_ts - minute_buffer["start_ts"])
        live = {
            "available": True,
            "elapsed_sec": elapsed,
            "time_label": ts_to_ist(minute_buffer["start_ts"]).strftime("%H:%M") + "*",
            "spot_open": round(minute_buffer["spot_open"]) if minute_buffer["spot_open"] else 0,
            "spot_high": round(minute_buffer["spot_high"]) if minute_buffer["spot_high"] else 0,
            "spot_low": round(minute_buffer["spot_low"]) if minute_buffer["spot_low"] else 0,
            "spot_close": round(minute_buffer["spot_close"]) if minute_buffer["spot_close"] else 0,
            "bear_strike": state["bear_strike"],
            "bull_strike": state["bull_strike"],
            "deltas": {},
            "ltp_ohlc": {},
        }
        for token, meta in state["tokens"].items():
            rk = meta["role_key"]
            current_oi = state["oi"].get(token, {}).get("oi", 0)
            start_oi = start.get(rk + "_oi", current_oi)
            live["deltas"][rk] = {"oi": current_oi, "delta": current_oi - start_oi}
            ohlc = minute_buffer["ltp_ohlc"].get(token, {})
            if ohlc:
                live["ltp_ohlc"][rk] = ohlc
        payload["live_candle"] = live
    return payload

def _broadcast_live_payload(payload):
    line = f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"
    stale = []
    with stream_lock:
        for q in list(stream_subscribers):
            try:
                if q.full():
                    try:
                        q.get_nowait()
                    except Exception:
                        pass
                q.put_nowait(line)
            except Exception:
                stale.append(q)
        for q in stale:
            if q in stream_subscribers:
                stream_subscribers.remove(q)

def _broadcast_live_from_state_unlocked():
    _broadcast_live_payload(_make_live_payload_unlocked())


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
                    _check_roll(new_spot)
                    continue

                if token not in state["tokens"]:
                    continue

                current_oi  = tick.get("oi", 0)
                current_ltp = tick.get("last_price", 0)

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
                    "depth": {"buy": buy_levels, "sell": sell_levels},
                }
                _update_ltp_ohlc(token, current_ltp)

            _append_history()
            _broadcast_live_from_state_unlocked()

    def on_connect(ws, response):
        with state_lock:
            all_tokens = [SPOT_TOKEN] + list(state["tokens"].keys())
        ws.subscribe(all_tokens)
        ws.set_mode(ws.MODE_FULL, all_tokens)
        print(f"[{now_ist().strftime('%H:%M:%S')}] Subscribed: {INDEX_LABEL} spot + {len(state['tokens'])} option tokens")

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
        result = {
            "spot":    state["spot"],
            "as_of":   now_ist().strftime("%H:%M:%S"),
            "options": {},
        }
        for token, meta in state["tokens"].items():
            snap = state["oi"].get(token, {})
            result["options"][meta["role_key"]] = {
                "strike": meta["strike"],
                "type":   meta["instrument_type"],
                "symbol": meta["tradingsymbol"],
                "ltp":    snap.get("ltp", 0),
            }
    return jsonify(result)


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


@app.route("/oi/live-candle")
def live_candle():
    with state_lock:
        if minute_buffer["start_ts"] is None or minute_buffer["start_snap"] is None:
            return jsonify({"available": False})

        now          = time.time()
        current_snap = _current_snap(now)
        start        = minute_buffer["start_snap"]
        elapsed      = round(now - minute_buffer["start_ts"])

        result = {
            "available":   True,
            "elapsed_sec": elapsed,
            "time_label":  ts_to_ist(minute_buffer["start_ts"]).strftime("%H:%M") + "*",
            "spot_open":   round(minute_buffer["spot_open"])  if minute_buffer["spot_open"]  else 0,
            "spot_high":   round(minute_buffer["spot_high"])  if minute_buffer["spot_high"]  else 0,
            "spot_low":    round(minute_buffer["spot_low"])   if minute_buffer["spot_low"]   else 0,
            "spot_close":  round(minute_buffer["spot_close"]) if minute_buffer["spot_close"] else 0,
            "bear_strike": state["bear_strike"],
            "bull_strike": state["bull_strike"],
            "deltas":      {},
        }

        for token, meta in state["tokens"].items():
            rk         = meta["role_key"]
            current_oi = state["oi"].get(token, {}).get("oi", 0)
            start_oi   = start.get(rk + "_oi", current_oi)
            result["deltas"][rk] = {
                "oi":    current_oi,
                "delta": current_oi - start_oi,
            }

    return jsonify(result)



@app.route("/stream")
def stream_ticks():
    """Push tick-by-tick snapshots to the browser using Server-Sent Events."""
    q = queue.Queue(maxsize=10)
    with stream_lock:
        stream_subscribers.append(q)

    def gen():
        try:
            # Send an initial snapshot immediately so the chart does not wait for the next tick.
            with state_lock:
                initial = _make_live_payload_unlocked(elapsed_override=0)
            yield f"data: {json.dumps(initial, separators=(',', ':'))}\n\n"
            while True:
                try:
                    yield q.get(timeout=15)
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with stream_lock:
                if q in stream_subscribers:
                    stream_subscribers.remove(q)

    return Response(gen(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })

# ── DEPTH WATCHER — background tasks + SSE streaming ─────────────────────────
#
# Architecture:
#   POST /depth/watch/start              → creates a watcher, returns watch_id
#   GET  /depth/watch/stream/<watch_id>  → SSE: one event/sec while running,
#                                          then a final "done" event with summary
#   POST /depth/watch/stop/<watch_id>    → cancel early
#   GET  /depth/watch/result/<watch_id>  → full result after completion
#
# This avoids the 120-s HTTP timeout: the SSE connection is a long-lived
# streaming response (same pattern as /stream), not a blocking request.
# Duration up to 600 s (10 min). Watchers auto-expire after 30 min.

_dw_watchers = {}          # watch_id → watcher dict
_dw_lock     = threading.Lock()


def _dw_interpret(opt_type, oi_delta, avg_imb, samples_n):
    if oi_delta < -5000 and avg_imb > 0.1:
        return (f"{opt_type} shorts covering via limit buys — OI fell {oi_delta:,} "
                f"over {samples_n}s, bid side heavier. Piggyback on big_bid_prices.")
    if oi_delta < -5000 and avg_imb < -0.1:
        return (f"{opt_type} OI falling but ask side heavy — likely market-order "
                f"covering or mixed signals. Check LTP direction.")
    if oi_delta < -1000 and abs(avg_imb) < 0.1:
        return (f"{opt_type} OI declining ({oi_delta:,}), depth balanced — "
                f"moderate unwinding, no strong limit-order signal.")
    if oi_delta > 5000 and avg_imb < -0.1:
        return (f"Fresh {opt_type} shorts being added — OI rising +{oi_delta:,}, "
                f"ask side heavy. Sellers entering via limit orders.")
    if oi_delta > 5000 and avg_imb > 0.1:
        return (f"{opt_type} OI rising +{oi_delta:,} with bid-heavy book — "
                f"possible long buildup or other-side short covering.")
    return (f"No dominant signal yet. OI delta={oi_delta:,}, "
            f"avg imbalance={avg_imb:+.3f} over {samples_n}s.")


def _dw_build_summary(opt_type, samples, bid_tracker, ask_tracker, big):
    if not samples:
        return {}
    oi_start  = samples[0]["oi"]
    oi_end    = samples[-1]["oi"]
    ltp_start = samples[0]["ltp"]
    ltp_end   = samples[-1]["ltp"]
    imb_vals  = [s["imbalance"] for s in samples]
    avg_imb   = round(sum(imb_vals) / len(imb_vals), 3)
    oi_delta  = oi_end - oi_start

    big_bids = sorted(
        [{"price": p, "peak_qty": v["peak"], "seen_count": v["count"]}
         for p, v in bid_tracker.items() if v["peak"] >= big],
        key=lambda x: -x["peak_qty"]
    )
    big_asks = sorted(
        [{"price": p, "peak_qty": v["peak"], "seen_count": v["count"]}
         for p, v in ask_tracker.items() if v["peak"] >= big],
        key=lambda x: -x["peak_qty"]
    )
    return {
        "oi_start":       oi_start,
        "oi_end":         oi_end,
        "oi_delta":       oi_delta,
        "ltp_start":      ltp_start,
        "ltp_end":        ltp_end,
        "avg_imbalance":  avg_imb,
        "min_imbalance":  min(imb_vals),
        "max_imbalance":  max(imb_vals),
        "big_bid_prices": big_bids,
        "big_ask_prices": big_asks,
        "interpretation": _dw_interpret(opt_type, oi_delta, avg_imb, len(samples)),
    }


def _dw_worker(watch_id):
    """Background thread: samples depth every second, pushes to per-watcher queue."""
    with _dw_lock:
        w = _dw_watchers.get(watch_id)
    if not w:
        return

    token    = w["token"]
    opt_type = w["opt_type"]
    duration = w["duration"]
    big      = w["big"]
    q        = w["queue"]

    samples     = []
    bid_tracker = {}
    ask_tracker = {}

    for i in range(duration):
        # Check for stop signal
        with _dw_lock:
            if _dw_watchers.get(watch_id, {}).get("stopped"):
                break

        snap        = state["oi"].get(token, {})
        depth       = snap.get("depth", {})
        buy_levels  = depth.get("buy",  [])
        sell_levels = depth.get("sell", [])

        total_bid = sum(l.get("quantity", 0) for l in buy_levels)
        total_ask = sum(l.get("quantity", 0) for l in sell_levels)
        denom     = total_bid + total_ask

        for lv in buy_levels:
            p, qty = lv.get("price", 0), lv.get("quantity", 0)
            if p:
                if p not in bid_tracker:
                    bid_tracker[p] = {"peak": qty, "count": 1}
                else:
                    bid_tracker[p]["peak"]   = max(bid_tracker[p]["peak"], qty)
                    bid_tracker[p]["count"] += 1

        for lv in sell_levels:
            p, qty = lv.get("price", 0), lv.get("quantity", 0)
            if p:
                if p not in ask_tracker:
                    ask_tracker[p] = {"peak": qty, "count": 1}
                else:
                    ask_tracker[p]["peak"]   = max(ask_tracker[p]["peak"], qty)
                    ask_tracker[p]["count"] += 1

        sample = {
            "t":          now_ist().strftime("%H:%M:%S"),
            "seq":        i + 1,
            "ltp":        snap.get("ltp", 0),
            "oi":         snap.get("oi", 0),
            "bid_qty":    total_bid,
            "ask_qty":    total_ask,
            "imbalance":  round((total_bid - total_ask) / denom, 3) if denom else 0,
            "best_bid":   buy_levels[0].get("price", 0)  if buy_levels  else 0,
            "best_ask":   sell_levels[0].get("price", 0) if sell_levels else 0,
            "bid_levels": buy_levels,
            "ask_levels": sell_levels,
        }
        samples.append(sample)

        # Rolling summary pushed with every tick so UI updates live
        rolling = _dw_build_summary(opt_type, samples, bid_tracker, ask_tracker, big)

        event = {
            "type":       "tick",
            "seq":        i + 1,
            "remaining":  duration - i - 1,
            "sample":     sample,
            "rolling":    rolling,
        }
        try:
            q.put_nowait(json.dumps(event))
        except Exception:
            pass

        time.sleep(1)

    # Final summary
    final_summary = _dw_build_summary(opt_type, samples, bid_tracker, ask_tracker, big)
    done_event = {
        "type":    "done",
        "samples": samples,
        "summary": final_summary,
    }
    try:
        q.put_nowait(json.dumps(done_event))
    except Exception:
        pass

    # Store result for /result endpoint
    with _dw_lock:
        if watch_id in _dw_watchers:
            _dw_watchers[watch_id]["result"]   = done_event
            _dw_watchers[watch_id]["finished"] = True


@app.route("/depth/watch/start", methods=["POST"])
def dw_start():
    """
    Create a depth watcher background task.

    JSON body (or query params):
        strike     int    e.g. 24000
        type       CE|PE
        seconds    int    1–600  (up to 10 minutes)
        threshold  int    big-order cutoff in lots, default 500

    Returns: { watch_id, strike, type, symbol, duration }
    """
    data = request.get_json(silent=True) or {}

    def _p(key, default):
        return data.get(key) or request.args.get(key, default)

    try:
        strike   = int(_p("strike", 0))
        opt_type = str(_p("type", "CE")).upper()
        duration = min(max(int(_p("seconds", 30)), 1), 600)
        big      = max(1, int(_p("threshold", 500)))
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid params"}), 400

    if opt_type not in ("CE", "PE"):
        return jsonify({"error": "type must be CE or PE"}), 400

    role_key = f"s{strike}_{opt_type.lower()}"
    with state_lock:
        token  = next((t for t, m in state["tokens"].items()
                       if m["role_key"] == role_key), None)
        symbol = state["tokens"][token]["tradingsymbol"] if token else None

    if token is None:
        tracked = sorted(f"{m['strike']}{m['instrument_type']}"
                         for m in state["tokens"].values())
        return jsonify({"error": f"{role_key} not tracked. Available: {tracked}"}), 404

    import uuid
    watch_id = uuid.uuid4().hex[:8]

    watcher = {
        "watch_id":  watch_id,
        "strike":    strike,
        "opt_type":  opt_type,
        "symbol":    symbol,
        "duration":  duration,
        "big":       big,
        "token":     token,
        "queue":     __import__("queue").Queue(maxsize=700),
        "stopped":   False,
        "finished":  False,
        "result":    None,
        "created_at": time.time(),
    }

    with _dw_lock:
        # Prune stale watchers (> 30 min old)
        cutoff = time.time() - 1800
        stale  = [k for k, v in _dw_watchers.items() if v["created_at"] < cutoff]
        for k in stale:
            del _dw_watchers[k]
        _dw_watchers[watch_id] = watcher

    threading.Thread(target=_dw_worker, args=(watch_id,), daemon=True).start()
    print(f"[{now_ist().strftime('%H:%M:%S')}] Depth watcher {watch_id} started: "
          f"{strike}{opt_type} {duration}s threshold={big}")

    return jsonify({
        "watch_id": watch_id,
        "strike":   strike,
        "type":     opt_type,
        "symbol":   symbol,
        "duration": duration,
    })


@app.route("/depth/watch/stream/<watch_id>")
def dw_stream(watch_id):
    """SSE stream for a running depth watcher. Sends one event/sec."""
    with _dw_lock:
        w = _dw_watchers.get(watch_id)
    if not w:
        return Response("data: {\"error\":\"watch_id not found\"}\n\n",
                        mimetype="text/event-stream", status=404)

    q = w["queue"]

    def gen():
        while True:
            try:
                msg = q.get(timeout=20)
                yield f"data: {msg}\n\n"
                parsed = json.loads(msg)
                if parsed.get("type") == "done":
                    break
            except __import__("queue").Empty:
                yield ": keepalive\n\n"
                # If worker finished but queue is empty, close
                with _dw_lock:
                    if _dw_watchers.get(watch_id, {}).get("finished"):
                        break

    return Response(gen(), mimetype="text/event-stream", headers={
        "Cache-Control":    "no-cache",
        "X-Accel-Buffering": "no",
    })


@app.route("/depth/watch/stop/<watch_id>", methods=["POST"])
def dw_stop(watch_id):
    """Signal a running watcher to stop after the current second."""
    with _dw_lock:
        w = _dw_watchers.get(watch_id)
    if not w:
        return jsonify({"error": "watch_id not found"}), 404
    w["stopped"] = True
    return jsonify({"status": "stop signal sent", "watch_id": watch_id})


@app.route("/depth/watch/result/<watch_id>")
def dw_result(watch_id):
    """Return the full result of a completed watcher."""
    with _dw_lock:
        w = _dw_watchers.get(watch_id)
    if not w:
        return jsonify({"error": "watch_id not found"}), 404
    if not w["finished"]:
        return jsonify({"status": "still_running",
                        "watch_id": watch_id}), 202
    return jsonify(w["result"])


@app.route("/iv")
def get_iv():
    """
    Live IV snapshot: per-strike IV + delta, delta-based risk reversals
    at 25D / 15D / 10D, and a directional signal.
    """
    iv_map = compute_iv_snapshot()

    with state_lock:
        spot       = state["spot"]
        expiry_str = state["expiry_date"]
        spot_open  = minute_buffer.get("spot_open")
        spot_close = minute_buffer.get("spot_close")

    if not iv_map or not spot:
        return jsonify({"available": False, "reason": "IV data not ready — waiting for LTPs"})

    dte = None
    if expiry_str:
        try:
            from datetime import date as _date
            dte = max(0, (_date.fromisoformat(expiry_str[:10]) - now_ist().date()).days)
        except Exception:
            pass

    # 3-minute price change — look back 3 completed 1-min rows in oi_history.
    # Falls back to current candle open->close if fewer than 3 rows exist yet.
    price_chg = 0.0
    price_window = "3m"
    try:
        history_rows = list(oi_history)
        if len(history_rows) >= 3:
            ref = history_rows[-3]
            ref_spot = ref.get("spot_close") or ref.get("spot")
            if ref_spot and ref_spot > 0:
                price_chg = round((spot - ref_spot) / ref_spot * 100, 3)
        elif len(history_rows) >= 1:
            ref = history_rows[0]
            ref_spot = ref.get("spot_close") or ref.get("spot")
            if ref_spot and ref_spot > 0:
                price_chg = round((spot - ref_spot) / ref_spot * 100, 3)
                price_window = f"{len(history_rows)}m"
        elif spot_open and spot_open > 0:
            price_chg = round(((spot_close or spot) - spot_open) / spot_open * 100, 3)
            price_window = "1m"
    except Exception:
        pass

    DELTA_LEVELS = [("25", 0.25), ("15", 0.15), ("10", 0.10)]
    skew = {}
    rr   = {}

    for label, target in DELTA_LEVELS:
        put_entry  = _find_by_delta(iv_map, target, "PE")
        call_entry = _find_by_delta(iv_map, target, "CE")
        skew[label] = {
            "put_iv":     put_entry["iv"]      if put_entry  else None,
            "call_iv":    call_entry["iv"]     if call_entry else None,
            "put_delta":  put_entry["delta"]   if put_entry  else None,
            "call_delta": call_entry["delta"]  if call_entry else None,
            "put_strike": put_entry["strike"]  if put_entry  else None,
            "call_strike":call_entry["strike"] if call_entry else None,
            "put_ltp":    put_entry["ltp"]     if put_entry  else None,
            "call_ltp":   call_entry["ltp"]    if call_entry else None,
        }
        if put_entry and call_entry:
            rr[label] = round(put_entry["iv"] - call_entry["iv"], 2)
        else:
            rr[label] = None

    valid_rrs = [v for v in rr.values() if v is not None]
    avg_rr    = round(sum(valid_rrs) / len(valid_rrs), 2) if valid_rrs else None

    # Tighter thresholds — valid for 3-min window (sustained move, not a spike)
    signal = "neutral"
    if avg_rr is not None:
        if   price_chg >  0.3 and avg_rr <  2: signal = "strong_bullish"
        elif price_chg >  0.3 and avg_rr >= 2: signal = "weak_bullish"
        elif price_chg < -0.3 and avg_rr >  5: signal = "strong_bearish"
        elif price_chg < -0.3 and avg_rr <= 5: signal = "short_covering"
        elif abs(price_chg) <= 0.3 and avg_rr > 6: signal = "tail_hedge"

    return jsonify({
        "available":      True,
        "as_of":          now_ist().strftime("%H:%M:%S"),
        "spot":           round(spot) if spot else None,
        "expiry_date":    expiry_str,
        "dte":            dte,
        "price_chg_pct":  price_chg,
        "price_window":   price_window,
        "iv_map":         iv_map,
        "skew":           skew,
        "risk_reversal":  rr,
        "avg_rr":         avg_rr,
        "signal":         signal,
    })


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
    print(f"  GET  /stream          browser SSE stream for tick-by-tick chart updates")
    print(f"  GET  /strikes         strike list")
    print(f"  GET  /oi/history      1-min rows + spot OHLC (today, or ?date=YYYY-MM-DD)")
    print(f"  POST /depth/watch/start   create depth watcher (?strike=N&type=CE|PE&seconds=N&threshold=N)")
    print(f"  GET  /depth/watch/stream/<id>  SSE live depth events")
    print(f"  POST /depth/watch/stop/<id>    cancel watcher early")
    print(f"  GET  /depth/watch/result/<id>  full result after done")
    print(f"  GET  /health          status + OHLC buffer")
    print(f"  POST /reset-csv       wipe today's rows")
    print("\nPress Ctrl+C to stop.\n")

    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)

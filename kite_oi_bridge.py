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
    GET  /oi/openhighlow   → OpenHighLow tab snapshot (also piggybacked on /ltp)
    POST /oi/openhighlow/reseed → re-pull today's Open/High/Low from the exchange
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

def _get_candle_close_ltps(tokens, n_candles):
    """
    Return {role_key: [ltp_close_c1, ltp_close_c2, ...]} for the last
    n_candles completed 1-min rows. Falls back to live LTP when a row
    doesn't have a close for that strike.
    """
    rows = list(oi_history)[-n_candles:] if len(oi_history) >= 1 else []
    result = {meta["role_key"]: [] for meta in tokens.values()}
    for row in rows:
        for token, meta in tokens.items():
            rk  = meta["role_key"]
            ltp = (row.get(rk + "_ltp_close") or
                   row.get(rk + "_ltp") or 0)
            if ltp > 0:
                result[rk].append(float(ltp))
    return result


def _get_candle_close_ivs(tokens, n_candles):
    """
    Return {role_key: [iv_c1, iv_c2, ...]} from pre-computed iv_close
    values already stored in oi_history rows.  When iv_close is present
    we skip re-running Black-Scholes entirely — much faster.
    Returns None if the rows don't have iv_close yet (first run or
    rows written before this feature was deployed).
    """
    rows = list(oi_history)[-n_candles:] if len(oi_history) >= 1 else []
    if not rows:
        return None
    # Check whether the newest row actually has iv_close data
    sample_rk = next(iter(tokens.values()))["role_key"] if tokens else None
    if sample_rk and rows[-1].get(sample_rk + "_iv_close") is None:
        return None   # rows pre-date this feature — fall back to LTP path
    result = {meta["role_key"]: [] for meta in tokens.values()}
    for row in rows:
        for token, meta in tokens.items():
            rk = meta["role_key"]
            iv = row.get(rk + "_iv_close")
            if iv is not None:
                result[rk].append(float(iv))
    return result


def compute_iv_snapshot():
    """
    Compute IV and delta for every tracked strike.

    Method (controlled by IV_CANDLE_AVG env var, default 3):
      1. Pull LTP closes from the last IV_CANDLE_AVG completed 1-min candles.
      2. Compute Black-Scholes IV for each candle-close LTP.
      3. Average the IVs — smooths out spike candles.
      4. Fall back to live LTP when fewer than IV_CANDLE_AVG candles exist.

    Returns dict keyed by role_key:
      {strike, type, ltp (latest), iv (% avg), iv_samples (list), delta}
    """
    RISK_FREE = 0.065

    with state_lock:
        spot       = state["spot"]
        expiry_str = state["expiry_date"]
        tokens     = dict(state["tokens"])
        oi_snap    = dict(state["oi"])

    if not spot or not expiry_str:
        return {}

    try:
        from datetime import date as _date
        today     = now_ist().date()
        expiry    = _date.fromisoformat(expiry_str[:10])
        days_left = max(0, (expiry - today).days)
    except Exception:
        return {}

    T = max(days_left / 365.0, 1 / 365.0)
    S = float(spot)

    # Try to use pre-computed iv_close values from oi_history rows first.
    # This avoids re-running Black-Scholes on data we already computed.
    # Falls back to LTP-based computation for rows that pre-date this feature.
    candle_ivs  = _get_candle_close_ivs(tokens, IV_CANDLE_AVG)
    candle_ltps = None if candle_ivs is not None else _get_candle_close_ltps(tokens, IV_CANDLE_AVG)

    result = {}
    for token, meta in tokens.items():
        rk       = meta["role_key"]
        K        = float(meta["strike"])
        opt_type = meta["instrument_type"]
        live_ltp = float(oi_snap.get(token, {}).get("ltp", 0) or 0)

        if candle_ivs is not None:
            # Fast path: iv_close already stored — just average them
            iv_samples = candle_ivs.get(rk, [])
            if not iv_samples:
                # Strike not in stored IVs (e.g. just rolled) — compute from live LTP
                iv_live = compute_iv(S, K, T, RISK_FREE, live_ltp, opt_type) if live_ltp > 0 else None
                if iv_live is None:
                    continue
                iv_samples = [round(iv_live * 100, 2)]
        else:
            # Slow path: compute IV from LTP candle closes (legacy rows)
            ltp_list = (candle_ltps or {}).get(rk, [])
            if not ltp_list:
                ltp_list = [live_ltp] if live_ltp > 0 else []
            if not ltp_list:
                continue
            iv_samples = []
            for ltp in ltp_list:
                iv = compute_iv(S, K, T, RISK_FREE, ltp, opt_type)
                if iv is not None:
                    iv_samples.append(round(iv * 100, 2))

        if not iv_samples:
            continue

        avg_iv = round(sum(iv_samples) / len(iv_samples), 2)
        delta  = _bs_delta(S, K, T, RISK_FREE, avg_iv / 100, opt_type)

        result[rk] = {
            "strike":     int(K),
            "type":       opt_type,
            "ltp":        round(live_ltp, 2),
            "iv":         avg_iv,
            "iv_samples": iv_samples,
            "iv_n":       len(iv_samples),
            "delta":      round(delta, 3),
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


def _target_strike_for_delta(S, T, r, atm_iv, target_delta, opt_type):
    """
    Invert Black-Scholes delta to find the strike that has a given delta,
    using ATM IV as the vol seed.

    For a CE:  delta = N(d1)  →  d1 = N_inv(delta)
    For a PE:  delta = N(d1) - 1  →  d1 = N_inv(delta + 1)  [since we store |delta|]

    d1 = (ln(S/K) + (r + 0.5σ²)T) / (σ√T)
    Solving for K:
        K = S * exp((r + 0.5σ²)T - d1 * σ√T)
    """
    import math

    sigma = atm_iv / 100.0
    if sigma <= 0 or T <= 0:
        return None

    # Map our always-positive delta convention back to N(d1)
    # CE: delta = N(d1)              → d1 = N_inv(target_delta)
    # PE: |delta| = 1 - N(d1)        → N(d1) = 1 - target_delta → d1 = N_inv(1 - target_delta)
    nd1 = target_delta if opt_type == "CE" else (1.0 - target_delta)

    # Clamp to avoid math domain errors at extreme deltas
    nd1 = max(1e-6, min(1 - 1e-6, nd1))

    # Inverse normal CDF via rational approximation (Beasley-Springer-Moro)
    def _norm_inv(p):
        a = [0, -3.969683028665376e+01,  2.209460984245205e+02,
             -2.759285104469687e+02,  1.383577518672690e+02,
             -3.066479806614716e+01,  2.506628277459239e+00]
        b = [0, -5.447609879822406e+01,  1.615858368580409e+02,
             -1.556989798598866e+02,  6.680131188771972e+01,
             -1.328068155288572e+01]
        c = [0, -7.784894002430293e-03, -3.223964580411365e-01,
             -2.400758277161838e+00, -2.549732539343734e+00,
              4.374664141464968e+00,  2.938163982698783e+00]
        d = [0,  7.784695709041462e-03,  3.224671290700398e-01,
              2.445134137142996e+00,  3.754408661907416e+00]
        p_low, p_high = 0.02425, 1 - 0.02425
        if p < p_low:
            q = math.sqrt(-2 * math.log(p))
            return (((((c[1]*q+c[2])*q+c[3])*q+c[4])*q+c[5])*q+c[6]) / \
                   ((((d[1]*q+d[2])*q+d[3])*q+d[4])*q+1)
        elif p <= p_high:
            q = p - 0.5
            r2 = q * q
            return (((((a[1]*r2+a[2])*r2+a[3])*r2+a[4])*r2+a[5])*r2+a[6])*q / \
                   (((((b[1]*r2+b[2])*r2+b[3])*r2+b[4])*r2+b[5])*r2+1)
        else:
            q = math.sqrt(-2 * math.log(1 - p))
            return -(((((c[1]*q+c[2])*q+c[3])*q+c[4])*q+c[5])*q+c[6]) / \
                    ((((d[1]*q+d[2])*q+d[3])*q+d[4])*q+1)

    d1 = _norm_inv(nd1)
    K = S * math.exp((r + 0.5 * sigma ** 2) * T - d1 * sigma * math.sqrt(T))
    return K


def compute_skew_by_delta(S, T, r, iv_map):
    """
    Compute IV skew at fixed delta targets (10/15/25/50/70) re-anchored to
    current spot every call.

    Steps for each delta level:
      1. Use ATM IV as vol seed to back-solve the theoretical strike for that delta.
      2. Snap to the nearest tracked strike (CE or PE independently).
      3. Look up that strike's actual IV from iv_map.
      4. Return the real IV at the real strike — not a delta-searched approximation.

    This means that as spot moves, the strikes selected automatically shift
    to the new delta-equivalent strikes. No tolerance fudging required.
    """
    DELTA_TARGETS = [
        ("10", 0.10),
        ("15", 0.15),
        ("25", 0.25),
        ("50", 0.50),
        ("70", 0.70),
    ]

    # Build lookup: (strike, opt_type) → iv_map entry
    strike_map = {}
    for entry in iv_map.values():
        strike_map[(entry["strike"], entry["type"])] = entry

    if not strike_map:
        return {}, {}

    # Get all available strikes per type
    ce_strikes = sorted(set(k for (k, t) in strike_map if t == "CE"))
    pe_strikes = sorted(set(k for (k, t) in strike_map if t == "PE"))

    if not ce_strikes or not pe_strikes:
        return {}, {}

    # Seed vol: use ATM CE IV (strike closest to spot)
    atm_k = min(ce_strikes, key=lambda k: abs(k - S))
    atm_entry = strike_map.get((atm_k, "CE"))
    atm_iv = atm_entry["iv"] if atm_entry else None

    # Fall back to median CE IV if ATM lookup fails
    if atm_iv is None:
        ce_ivs = [strike_map[(k, "CE")]["iv"] for k in ce_strikes if (k, "CE") in strike_map]
        atm_iv = sorted(ce_ivs)[len(ce_ivs) // 2] if ce_ivs else 15.0

    def snap(theoretical_k, available_strikes):
        """Snap theoretical strike to nearest available tracked strike."""
        if theoretical_k is None:
            return None
        return min(available_strikes, key=lambda k: abs(k - theoretical_k))

    skew = {}
    rr   = {}

    for label, target_delta in DELTA_TARGETS:
        # For OTM convention:
        #   CE with target_delta → OTM call (strike > spot for delta < 0.5)
        #   PE with target_delta → OTM put  (strike < spot for delta < 0.5)
        # The inversion handles both OTM and ITM naturally via the delta formula.

        ce_theoretical = _target_strike_for_delta(S, T, r, atm_iv, target_delta, "CE")
        pe_theoretical = _target_strike_for_delta(S, T, r, atm_iv, target_delta, "PE")

        ce_k = snap(ce_theoretical, ce_strikes)
        pe_k = snap(pe_theoretical, pe_strikes)

        ce_entry = strike_map.get((ce_k, "CE")) if ce_k is not None else None
        pe_entry = strike_map.get((pe_k, "PE")) if pe_k is not None else None

        skew[label] = {
            "put_iv":      pe_entry["iv"]    if pe_entry else None,
            "call_iv":     ce_entry["iv"]    if ce_entry else None,
            "put_delta":   pe_entry["delta"] if pe_entry else None,
            "call_delta":  ce_entry["delta"] if ce_entry else None,
            "put_strike":  pe_k,
            "call_strike": ce_k,
            "put_ltp":     pe_entry["ltp"]   if pe_entry else None,
            "call_ltp":    ce_entry["ltp"]   if ce_entry else None,
            "put_iv_n":    pe_entry["iv_n"]  if pe_entry else None,
            "call_iv_n":   ce_entry["iv_n"]  if ce_entry else None,
            "atm_iv_seed": round(atm_iv, 2),          # useful for debugging
            "ce_theoretical_k": round(ce_theoretical) if ce_theoretical else None,
            "pe_theoretical_k": round(pe_theoretical) if pe_theoretical else None,
        }

        if pe_entry and ce_entry:
            rr[label] = round(pe_entry["iv"] - ce_entry["iv"], 2)
        else:
            rr[label] = None

    return skew, rr


def _compute_iv_for_row(row: dict, tokens: dict, spot, expiry_str: str) -> dict:
    """
    Compute candle-close IV for every strike in `tokens` using the
    ltp_close values already stored in `row`, then derive the 5 RR levels.

    Returns a flat dict of fields to merge into the candle row:
        {rk}_iv_close   — IV % at candle-close LTP   (e.g. s24300_ce_iv_close)
        {rk}_delta      — BS delta at candle close    (overwrites OI delta field
                          name clash: stored as {rk}_bs_delta instead)
        iv_rr_10, iv_rr_15, iv_rr_25, iv_rr_50, iv_rr_70  — risk reversals
        iv_atm_ce, iv_atm_pe  — ATM call/put IV for quick reference
        iv_avg_rr       — average of OTM RRs (10/15/25)
    """
    RISK_FREE = 0.065
    result    = {}

    if not spot or not expiry_str:
        return result

    try:
        from datetime import date as _date
        today     = now_ist().date()
        expiry    = _date.fromisoformat(expiry_str[:10])
        days_left = max(0, (expiry - today).days)
    except Exception:
        return result

    T = max(days_left / 365.0, 1 / 365.0)
    S = float(spot)

    # Build iv_map from candle-close LTPs stored in row
    iv_map = {}
    for token, meta in tokens.items():
        rk       = meta["role_key"]
        K        = float(meta["strike"])
        opt_type = meta["instrument_type"]
        ltp      = float(row.get(rk + "_ltp_close") or row.get(rk + "_ltp") or 0)
        if ltp <= 0:
            continue
        iv = compute_iv(S, K, T, RISK_FREE, ltp, opt_type)
        if iv is None:
            continue
        delta = _bs_delta(S, K, T, RISK_FREE, iv, opt_type)
        iv_pct = round(iv * 100, 2)
        result[rk + "_iv_close"] = iv_pct
        result[rk + "_bs_delta"] = round(delta, 3)
        iv_map[rk] = {
            "strike": int(K),
            "type":   opt_type,
            "ltp":    ltp,
            "iv":     iv_pct,
            "iv_n":   1,
            "delta":  round(delta, 3),
        }

    if not iv_map:
        return result

    # Derive RR at all 5 delta levels using spot-driven strike selection.
    # Same logic as the live /iv endpoint — strikes are picked by inverting
    # Black-Scholes delta so they track spot as it moves, not the fixed window.
    skew_detail_full, rr_detail = compute_skew_by_delta(S, T, 0.065, iv_map)

    rrs = {}
    for label in ("10", "15", "25", "50", "70"):
        rr_val = rr_detail.get(label)
        result[f"iv_rr_{label}"] = rr_val
        if rr_val is not None:
            rrs[label] = rr_val
            sd = skew_detail_full.get(label, {})
            result[f"iv_rr_{label}_put_k"]  = sd.get("put_strike")
            result[f"iv_rr_{label}_call_k"] = sd.get("call_strike")

    # ATM IV convenience fields — snap to strike nearest spot
    atm_k_ce  = min((e for e in iv_map.values() if e["type"] == "CE"),
                    key=lambda e: abs(e["strike"] - S), default=None)
    atm_k_pe  = min((e for e in iv_map.values() if e["type"] == "PE"),
                    key=lambda e: abs(e["strike"] - S), default=None)
    result["iv_atm_ce"] = atm_k_ce["iv"] if atm_k_ce else None
    result["iv_atm_pe"] = atm_k_pe["iv"] if atm_k_pe else None

    # avg_rr from OTM levels only
    otm_rrs = [rrs[k] for k in ("10", "15", "25") if k in rrs and rrs[k] is not None]
    result["iv_avg_rr"] = round(sum(otm_rrs) / len(otm_rrs), 2) if otm_rrs else None

    return result


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

# IV_NUM_STRIKES: how many strikes around ATM to use for IV/skew computation.
# Must be >= NUM_STRIKES to be meaningful; default 21 = ATM ±10 strikes.
# A wider window ensures 70Δ (ITM) strikes are always in range.
# Set IV_NUM_STRIKES=31 for ATM ±15 if you want deeper ITM coverage.
# NOTE: IV strikes are computed from the SAME WebSocket tokens as OI.
#       If IV_NUM_STRIKES > NUM_STRIKES, the extra strikes are fetched via
#       kite.quote() at IV computation time (one REST call per /iv request).
#       Keep IV_NUM_STRIKES <= NUM_STRIKES to avoid REST calls entirely.
IV_NUM_STRIKES = int(os.environ.get("IV_NUM_STRIKES", max(21, NUM_STRIKES)))
if IV_NUM_STRIKES % 2 == 0:
    IV_NUM_STRIKES += 1
    print(f"Warning: IV_NUM_STRIKES must be odd — bumped to {IV_NUM_STRIKES}")
IV_HALF = IV_NUM_STRIKES // 2
print(f"IV window: ATM ±{IV_HALF} strikes ({IV_NUM_STRIKES} total)")

# IV_CANDLE_AVG: number of completed 1-min candles to average IV over.
# Default 3 — smooths spike candles. Set IV_CANDLE_AVG=1 for raw candle-close.
IV_CANDLE_AVG  = max(1, int(os.environ.get("IV_CANDLE_AVG", 3)))


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
                -- IV-specific indexes for fast backtesting queries
                -- e.g. SELECT * WHERE iv_rr_25 > 5 AND trade_date = '2025-05-01'
                CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_iv_rr25
                    ON {DB_TABLE} ((data->>'iv_rr_25'));
                CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_iv_avg_rr
                    ON {DB_TABLE} ((data->>'iv_avg_rr'));
                CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_date_time
                    ON {DB_TABLE} (trade_date, ts);
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
        "fut_ltp": state["fut_ltp"],
        "session_id": state["session_id"],
        "atm_strike": state["atm_strike"],
        "bear_strike": state["bear_strike"],
        "bull_strike": state["bull_strike"],
        "expiry_date": state["expiry_date"],
        "settlement_vwap": _settlement_vwap_payload(),
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
# Classic intraday "OHL scanner" logic, applied per option leg (CE/PE):
#   Open = High  → premium has not traded above its opening print all day
#                  (every subsequent tick has been <= open) — bearish-for-that-
#                  leg signal, sellers in control from the first tick.
#   Open = Low   → premium has not traded below its opening print all day
#                  (every subsequent tick has been >= open) — bullish-for-that-
#                  leg signal, buyers in control from the first tick.
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
day_oh_meta = {"date": None}


def _reset_day_oh_if_new_day():
    today = now_ist().strftime("%Y-%m-%d")
    if day_oh_meta["date"] != today:
        day_oh.clear()
        day_oh_meta["date"] = today


def _seed_day_oh():
    """Seed day_oh with the exchange-reported day Open/High/Low for every
    tracked leg, via one kite.ohlc() REST call. Run this once at startup (and
    it's safe to re-run any time, e.g. after a reconnect) so 'Open' is always
    the real 09:15 print — correct even if the bridge restarts at, say, 11:30.
    Does its own locking; do NOT call while already holding state_lock."""
    with state_lock:
        tokens = dict(state["tokens"])
    if not tokens:
        return

    key_to_token = {f"{EXCHANGE}:{meta['tradingsymbol']}": token for token, meta in tokens.items()}
    try:
        quotes = kite.ohlc(list(key_to_token.keys()))
    except Exception as e:
        print(f"[OpenHighLow] kite.ohlc() seed failed ({e}) — will fall back to first-tick capture per leg.")
        return

    _reset_day_oh_if_new_day()
    seeded = 0
    with state_lock:
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
            day_oh[token] = {
                "open": o, "high": h, "low": l,
                "moved_dn": l < o, "moved_up": h > o,
                "filled_oh": False, "filled_ol": False,
            }
            seeded += 1
    print(f"[OpenHighLow] Seeded day Open/High/Low for {seeded}/{len(tokens)} legs from the exchange.")


def _update_day_oh(token, ltp):
    """Update the day Open/High/Low + fill state for one option leg.
    Caller must hold state_lock."""
    if ltp is None or ltp <= 0:
        return
    _reset_day_oh_if_new_day()
    d = day_oh.get(token)
    if d is None:
        # Fallback only — normally _seed_day_oh() has already populated this
        # leg with the real exchange Open before any ticks arrive.
        day_oh[token] = {
            "open": ltp, "high": ltp, "low": ltp,
            "moved_dn": False, "moved_up": False,
            "filled_oh": False, "filled_ol": False,
        }
        return

    op = d["open"]

    # Evaluate fill conditions BEFORE updating high/low — otherwise a tick that
    # simultaneously sets a new high AND crosses the open level would update the
    # high first, breaking the d["high"] == op guard before we can check it.

    # Open = High: leg dipped below open at some point, now LTP has climbed
    # back to or beyond the open level — mark filled.
    if ltp < op:
        d["moved_dn"] = True
    elif ltp >= op and d["moved_dn"] and d["high"] == op and not d["filled_oh"]:
        d["filled_oh"] = True       # price touched/crossed back up to the Open=High level

    # Open = Low: leg rose above open at some point, now LTP has fallen
    # back to or beyond the open level — mark filled.
    if ltp > op:
        d["moved_up"] = True
    elif ltp <= op and d["moved_up"] and d["low"] == op and not d["filled_ol"]:
        d["filled_ol"] = True       # price touched/crossed back down to the Open=Low level

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
            "is_oh":     d["high"] == d["open"],
            "is_ol":     d["low"]  == d["open"],
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

    # ── IV at candle close ───────────────────────────────────────────
    # Compute Black-Scholes IV for every strike using the candle-close LTPs
    # we just stored in `row`.  The result is merged into the same row so
    # it lands in the JSONB blob alongside OI/LTP data — no schema change.
    # Fields added: {rk}_iv_close, {rk}_bs_delta, iv_rr_10/15/25/50/70,
    #               iv_rr_{n}_put_k, iv_rr_{n}_call_k, iv_atm_ce/pe, iv_avg_rr
    try:
        iv_fields = _compute_iv_for_row(
            row,
            dict(state["tokens"]),
            state["spot"],
            state["expiry_date"],
        )
        row.update(iv_fields)
        iv_summary = (
            f"RR25={iv_fields.get('iv_rr_25','?')} "
            f"RR10={iv_fields.get('iv_rr_10','?')} "
            f"ATM_CE={iv_fields.get('iv_atm_ce','?')} "
            f"ATM_PE={iv_fields.get('iv_atm_pe','?')}"
        )
        print(f"[{row['time_label']}] IV close: {iv_summary}")
    except Exception as _iv_err:
        print(f"[{row['time_label']}] IV compute error (non-fatal): {_iv_err}")
    # ─────────────────────────────────────────────────────────────────

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
                        if state["spot"]:
                            _update_basis(fut_ltp, state["spot"])
                    continue

                if token not in state["tokens"]:
                    continue

                current_oi  = tick.get("oi", 0)
                current_ltp = tick.get("last_price", 0)
                current_qty = tick.get("last_traded_quantity", 0) or tick.get("volume_traded", 0) or 1

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
                    "depth": {"buy": buy_levels, "sell": sell_levels},
                }
                _update_ltp_ohlc(token, current_ltp)
                _update_day_oh(token, current_ltp)  # first-tick fallback handled inside

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
            _broadcast_live_from_state_unlocked()

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
        state["fut_token"] = fut_token
    if fut_token:
        print(f"[Futures] Subscribed: {fut_sym} (token {fut_token})")
    else:
        print("[Futures] No FUT token — futures VWAP estimate will be unavailable.")

    print("\nSeeding OpenHighLow day levels from exchange OHLC...")
    _seed_day_oh()

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
            "fut_ltp": state["fut_ltp"],
            "as_of":   now_ist().strftime("%H:%M:%S"),
            "settlement_vwap": _settlement_vwap_payload(),
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

    # Spot-driven delta skew: for every tick, back-solve the strike that
    # corresponds to each delta target given current spot + ATM IV.
    # This means the strikes update automatically as spot moves — no fixed
    # strike window, no tolerance hunting, no stale delta labels.
    try:
        from datetime import date as _date
        _dte_days = max(0, (_date.fromisoformat(expiry_str[:10]) - now_ist().date()).days)
    except Exception:
        _dte_days = 0
    _T_iv = max(_dte_days / 365.0, 1 / 365.0)

    skew, rr = compute_skew_by_delta(float(spot), _T_iv, 0.065, iv_map)

    # avg_rr uses only OTM levels (10/15/25) for signal — ATM/ITM have different dynamics
    otm_rrs   = [rr[k] for k in ("10", "15", "25") if rr.get(k) is not None]
    avg_rr    = round(sum(otm_rrs) / len(otm_rrs), 2) if otm_rrs else None
    # full avg across all levels for reference
    all_rrs   = [v for v in rr.values() if v is not None]
    avg_rr_all = round(sum(all_rrs) / len(all_rrs), 2) if all_rrs else None

    # Signal — 3-min price + skew combined.
    #
    # RR convention: RR = Put IV - Call IV
    #   Positive RR -> puts more expensive -> bearish/hedge demand (normal equity skew)
    #   Negative RR -> calls more expensive -> bullish chase / upside demand
    #
    # Previous logic only handled positive RR and had a dead zone for price moves
    # between 0.05-0.3% where has_price=True but no condition fired -> always neutral.
    # Fixed: thresholds lowered to 0.1%, negative RR handled explicitly.
    signal = "neutral"
    signal_basis = "price+skew"

    if avg_rr is not None:
        has_price = abs(price_chg) > 0.05   # at least a marginal move

        if has_price:
            if   price_chg >  0.3 and avg_rr <  -1: signal = "strong_bullish"   # strong up + calls clearly pricier
            elif price_chg >  0.3 and avg_rr >= -1: signal = "weak_bullish"      # strong up + skew mixed
            elif price_chg >  0.05 and avg_rr < -2: signal = "weak_bullish"      # any up move + calls clearly pricier
            elif price_chg < -0.3 and avg_rr >   5: signal = "strong_bearish"    # strong down + puts very expensive
            elif price_chg < -0.3 and avg_rr <=  5: signal = "short_covering"    # strong down + puts mild -> covering not panic
            elif price_chg < -0.1 and avg_rr >   3: signal = "weak_bearish"      # mild down + put premium building
            elif avg_rr >  6:                        signal = "tail_hedge"        # heavy put buying regardless of price move
        else:
            # Skew-only: price flat, read structure of vol surface alone
            signal_basis = "skew_only"
            rr10 = rr.get("10")
            rr25 = rr.get("25")
            if rr10 is not None and rr25 is not None:
                divergence = rr10 - rr25   # positive = tail risk steepening
                if   divergence > 4 and avg_rr >   5: signal = "tail_hedge"
                elif avg_rr < -3:                      signal = "strong_bullish"  # calls very expensive vs puts
                elif avg_rr < -1:                      signal = "weak_bullish"    # calls moderately expensive
                elif avg_rr >   7:                     signal = "strong_bearish"
                elif avg_rr >   4:                     signal = "short_covering"

    return jsonify({
        "available":      True,
        "as_of":          now_ist().strftime("%H:%M:%S"),
        "spot":           round(spot) if spot else None,
        "expiry_date":    expiry_str,
        "dte":            dte,
        "price_chg_pct":  price_chg,
        "price_window":   price_window,
        "iv_candle_avg":  IV_CANDLE_AVG,
        "iv_map":         iv_map,
        "skew":           skew,
        "risk_reversal":  rr,
        "avg_rr":         avg_rr,          # OTM only (10/15/25) — used for signal
        "avg_rr_all":     avg_rr_all,      # all 5 levels for reference
        "signal":         signal,
        "signal_basis":   signal_basis,
    })


@app.route("/iv/history")
def get_iv_history():
    """
    Return per-minute IV snapshots for a given date, extracted from the
    stored JSONB rows.  Each row already has iv_rr_*, iv_atm_*, iv_avg_rr
    and per-strike iv_close fields written at candle-close time.

    Query params:
        date=YYYY-MM-DD   (default: today)
        strikes=1         include per-strike iv_close fields (default: 0)

    Response: list of dicts, one per minute, newest-last:
    [
      {
        "time_label": "09:15",
        "ts": 1234567890.0,
        "spot_close": 24312,
        "iv_rr_10": 3.2,  "iv_rr_15": 2.8,  "iv_rr_25": 2.1,
        "iv_rr_50": -0.4, "iv_rr_70": -1.1,
        "iv_atm_ce": 17.4, "iv_atm_pe": 18.1,
        "iv_avg_rr": 2.7,
        # if strikes=1:
        "s24300_ce_iv_close": 17.4,
        "s24300_pe_iv_close": 18.1,
        ...
      },
      ...
    ]
    """
    date_param    = request.args.get("date", "").strip()
    include_strikes = request.args.get("strikes", "0").strip() == "1"

    IV_FIELDS = (
        "time_label", "ts", "spot_close", "spot_open", "spot_high", "spot_low",
        "iv_rr_10", "iv_rr_15", "iv_rr_25", "iv_rr_50", "iv_rr_70",
        "iv_rr_10_put_k", "iv_rr_10_call_k",
        "iv_rr_15_put_k", "iv_rr_15_call_k",
        "iv_rr_25_put_k", "iv_rr_25_call_k",
        "iv_rr_50_put_k", "iv_rr_50_call_k",
        "iv_rr_70_put_k", "iv_rr_70_call_k",
        "iv_atm_ce", "iv_atm_pe", "iv_avg_rr",
    )

    try:
        if date_param:
            from datetime import date as _date
            _date.fromisoformat(date_param)
            rows = db_read_by_date(date_param)
        else:
            rows = db_read_today()
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    out = []
    for row in rows:
        rec = {k: row.get(k) for k in IV_FIELDS}
        if include_strikes:
            for k, v in row.items():
                if k.endswith("_iv_close") or k.endswith("_bs_delta"):
                    rec[k] = v
        out.append(rec)

    return jsonify(out)


@app.route("/iv/dates")
def get_iv_dates():
    """
    Return dates that have at least one row with iv_rr_25 populated.
    Useful for the frontend date picker to show which days have IV data.
    """
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT trade_date
                    FROM {DB_TABLE}
                    WHERE (data->>'iv_rr_25') IS NOT NULL
                    ORDER BY trade_date DESC
                    """
                )
                dates = [str(r[0]) for r in cur.fetchall()]
        return jsonify({"dates": dates})
    except Exception as e:
        return jsonify({"dates": [], "error": str(e)})


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
    print(f"  GET  /oi/openhighlow  OpenHighLow tab snapshot (CE/PE Open=High & Open=Low)")
    print(f"  POST /oi/openhighlow/reseed  re-pull today's Open/High/Low from the exchange")
    print(f"  GET  /oi/history      1-min rows + spot OHLC (today, or ?date=YYYY-MM-DD)")
    print(f"  GET  /iv/history      per-minute IV + RR values (today, or ?date=YYYY-MM-DD&strikes=1)")
    print(f"  GET  /iv/dates        dates that have IV data")
    print(f"  POST /depth/watch/start   create depth watcher (?strike=N&type=CE|PE&seconds=N&threshold=N)")
    print(f"  GET  /depth/watch/stream/<id>  SSE live depth events")
    print(f"  POST /depth/watch/stop/<id>    cancel watcher early")
    print(f"  GET  /depth/watch/result/<id>  full result after done")
    print(f"  GET  /health          status + OHLC buffer")
    print(f"  POST /reset-csv       wipe today's rows")
    print("\nPress Ctrl+C to stop.\n")

    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)

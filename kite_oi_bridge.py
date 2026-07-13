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


# ── ITM IV RATE-OF-CHANGE TRACKER ────────────────────────────────────────────
# Fresh IV engine (replaces the old skew/RR signal):
#   • Tracks TWO strikes: 2-strike ITM CALL (ATM − 2×step) and
#     2-strike ITM PUT (ATM + 2×step).  Nifty example: spot 24000 →
#     CE 23900, PE 24100.
#   • On EVERY option tick, Black-Scholes IV is computed from tick LTP and
#     the per-tick rate of change (RoC = iv_now − iv_prev, in IV points)
#     is recorded.
#   • Every 5 minutes the window closes: avg tick RoC, net ΔIV and RoC/min
#     are computed per side, the spot move is measured, and the window is
#     classified into one of 8 IV scenarios. A summary is PRINTED to the
#     console and appended to history (served by /iv).
#   • When spot moves ITM_ROLL_POINTS (default 2×STRIKE_STEP = 100 for
#     Nifty) from the anchor, strikes re-anchor to the next 2-ITM pair.
#     The per-tick baseline resets so the strike switch doesn't pollute RoC.
#
# All _itm_* functions are called with state_lock ALREADY HELD (from
# on_ticks) unless noted. itm_iv is guarded by state_lock.

ITM_WINDOW_SEC  = int(os.environ.get("ITM_WINDOW_SEC", 300))      # 5-min window
ITM_ROLL_POINTS = float(os.environ.get("ITM_ROLL_POINTS", 2 * STRIKE_STEP))
ITM_OFFSET      = 2 * STRIKE_STEP                                  # "2 strikes ITM"
ITM_IV_TH       = float(os.environ.get("ITM_IV_TH",   0.15))       # IV pts: rising/falling threshold over window
ITM_SPOT_TH     = float(os.environ.get("ITM_SPOT_TH", 0.08))       # % spot move: up/down threshold over window

ITM_SCENARIOS = {
    1: {"label": "Genuine fear — sell-off confirmed",
        "desc":  "Spot falling + PE IV rising while CE IV is not. Real protective demand — trend continuation likely (SELL-side conviction)."},
    2: {"label": "Suspect fall — TRAP / reversal risk",
        "desc":  "Spot falling but PE IV flat or falling. Nobody paying up for protection — market doesn't believe the move. Watch for reversal."},
    3: {"label": "Real momentum rally",
        "desc":  "Spot rising + CE IV rising. Upside being chased with premium — genuine directional demand. Very sharp CE IV spikes near range highs can mark exhaustion."},
    4: {"label": "IV-crush relief rally",
        "desc":  "Spot rising while both IVs deflate. Hedges unwinding, calm continuation — good for sellers, bad for option buyers even with right direction."},
    5: {"label": "Pre-event vol build-up",
        "desc":  "Spot flat but both IVs rising. Market loading energy for a move without picking direction — breakout pending, premiums inflating."},
    6: {"label": "Theta-crush rangebound",
        "desc":  "Spot flat and both IVs bleeding. Sellers harvesting decay — no directional signal, short-premium regime."},
    7: {"label": "Regime-change danger — both tails bid",
        "desc":  "Spot falling and BOTH CE and PE IV rising. Whole distribution being repriced, not just one tail — genuine uncertainty, high danger."},
    8: {"label": "Distrusted bounce — hedges held",
        "desc":  "Spot rising but PE IV rising/elevated too. Smart money keeps protection on through the bounce — retest of lows often follows."},
    0: {"label": "Neutral / mixed",
        "desc":  "No clear combination of spot direction and IV behaviour in this window."},
}


def _itm_blank_side():
    return {
        "strike":    None,
        "token":     None,
        "ltp":       None,
        "prev_iv":   None,     # last tick IV (baseline for next RoC)
        "iv_first":  None,     # first IV of current 5-min window
        "iv_live":   None,     # latest IV
        "roc_sum":   0.0,      # Σ per-tick RoC (IV points)
        "roc_last":  None,     # most recent per-tick RoC
        "ticks":     0,        # ticks with a valid RoC this window
    }


itm_iv = {
    "anchor_spot":     None,
    "ce":              _itm_blank_side(),
    "pe":              _itm_blank_side(),
    "window_start":    None,   # datetime (IST)
    "window_spot_open": None,
    "rolls":           [],     # strike rolls inside the current window
    "windows":         collections.deque(maxlen=150),   # completed 5-min summaries
}


def _itm_token_for(strike, opt_type):
    """Find the websocket token for strike+type. state_lock must be held."""
    rk = f"s{int(strike)}_{opt_type.lower()}"
    for t, m in state["tokens"].items():
        if m["role_key"] == rk:
            return t
    return None


def _itm_T():
    """Year-fraction to expiry from state. state_lock must be held."""
    expiry_str = state.get("expiry_date")
    if not expiry_str:
        return None
    try:
        from datetime import date as _date
        dte = max(0, (_date.fromisoformat(expiry_str[:10]) - now_ist().date()).days)
        return max(dte / 365.0, 1 / 365.0)
    except Exception:
        return None


def _itm_anchor(spot, reason="init"):
    """(Re)select the 2-ITM strikes around current spot. state_lock held."""
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP
    ce_k = atm - ITM_OFFSET        # ITM call = strike below spot
    pe_k = atm + ITM_OFFSET        # ITM put  = strike above spot
    old_ce = itm_iv["ce"]["strike"]
    old_pe = itm_iv["pe"]["strike"]

    for side, k in (("ce", ce_k), ("pe", pe_k)):
        s = _itm_blank_side()
        s["strike"] = k
        s["token"]  = _itm_token_for(k, side.upper())
        itm_iv[side] = s

    itm_iv["anchor_spot"] = spot
    if reason == "roll":
        itm_iv["rolls"].append({
            "time":  now_ist().strftime("%H:%M:%S"),
            "spot":  round(spot, 2),
            "from":  {"ce": old_ce, "pe": old_pe},
            "to":    {"ce": ce_k,   "pe": pe_k},
        })
    print(f"[{now_ist().strftime('%H:%M:%S')}] ITM-IV strikes {reason}: "
          f"spot={spot:.1f} ATM={atm} → CE {ce_k} / PE {pe_k}")


def _itm_on_spot(spot):
    """Called from on_ticks on every spot tick. state_lock held."""
    if not spot:
        return
    if itm_iv["anchor_spot"] is None:
        _itm_anchor(spot, "init")
        itm_iv["window_start"]     = now_ist()
        itm_iv["window_spot_open"] = spot
        return
    if abs(spot - itm_iv["anchor_spot"]) >= ITM_ROLL_POINTS:
        _itm_anchor(spot, "roll")
    _itm_maybe_close_window(spot)


def _itm_on_option_tick(token, ltp):
    """Called from on_ticks for every option tick. state_lock held."""
    if not ltp or ltp <= 0:
        return
    side = None
    if token == itm_iv["ce"]["token"]:
        side = "ce"
    elif token == itm_iv["pe"]["token"]:
        side = "pe"
    if side is None:
        return

    spot = state.get("spot")
    T    = _itm_T()
    if not spot or T is None:
        return

    s  = itm_iv[side]
    iv = compute_iv(float(spot), float(s["strike"]), T, 0.065, float(ltp),
                    side.upper())
    if iv is None:
        return
    iv_pct = round(iv * 100, 4)

    s["ltp"]     = float(ltp)
    s["iv_live"] = iv_pct
    if s["iv_first"] is None:
        s["iv_first"] = iv_pct
    if s["prev_iv"] is not None:
        roc = round(iv_pct - s["prev_iv"], 4)      # IV points per tick
        s["roc_last"] = roc
        s["roc_sum"] += roc
        s["ticks"]   += 1
    s["prev_iv"] = iv_pct


def _itm_side_summary(side_key):
    """Snapshot metrics for one side of the current window."""
    s = itm_iv[side_key]
    d_iv = (round(s["iv_live"] - s["iv_first"], 4)
            if (s["iv_live"] is not None and s["iv_first"] is not None) else None)
    avg_roc = round(s["roc_sum"] / s["ticks"], 5) if s["ticks"] else None
    return {
        "strike":       s["strike"],
        "ltp":          s["ltp"],
        "iv_first":     s["iv_first"],
        "iv_live":      s["iv_live"],
        "d_iv":         d_iv,
        "avg_tick_roc": avg_roc,
        "roc_last":     s["roc_last"],
        "ticks":        s["ticks"],
    }


def _itm_classify(spot_chg_pct, ce_d_iv, pe_d_iv):
    """Map (spot direction × CE/PE IV direction) to one of the 8 scenarios."""
    if spot_chg_pct is None or ce_d_iv is None or pe_d_iv is None:
        return 0
    spot_dir = "up" if spot_chg_pct > ITM_SPOT_TH else ("down" if spot_chg_pct < -ITM_SPOT_TH else "flat")
    ce_dir   = "up" if ce_d_iv >  ITM_IV_TH else ("down" if ce_d_iv < -ITM_IV_TH else "flat")
    pe_dir   = "up" if pe_d_iv >  ITM_IV_TH else ("down" if pe_d_iv < -ITM_IV_TH else "flat")

    if spot_dir == "down":
        if ce_dir == "up" and pe_dir == "up":  return 7   # both tails bid
        if pe_dir == "up":                     return 1   # genuine fear
        return 2                                          # PE flat/down → trap
    if spot_dir == "up":
        if pe_dir == "up":                     return 8   # distrusted bounce
        if ce_dir == "up":                     return 3   # real momentum
        if ce_dir == "down" and pe_dir == "down": return 4  # IV crush rally
        return 0
    # spot flat
    if ce_dir == "up" and pe_dir == "up":      return 5   # pre-event build-up
    if ce_dir == "down" and pe_dir == "down":  return 6   # theta crush
    return 0


def _itm_maybe_close_window(spot):
    """Close + summarise the 5-min window when elapsed. state_lock held."""
    ws = itm_iv["window_start"]
    if ws is None:
        return
    elapsed = (now_ist() - ws).total_seconds()
    if elapsed < ITM_WINDOW_SEC:
        return

    spot_open = itm_iv["window_spot_open"]
    spot_chg_pct = (round((spot - spot_open) / spot_open * 100, 3)
                    if spot_open else None)
    minutes = elapsed / 60.0

    summary = {
        "start":        ws.strftime("%H:%M:%S"),
        "end":          now_ist().strftime("%H:%M:%S"),
        "spot_open":    round(spot_open, 2) if spot_open else None,
        "spot_close":   round(spot, 2),
        "spot_chg_pct": spot_chg_pct,
        "rolls":        list(itm_iv["rolls"]),
    }
    for side in ("ce", "pe"):
        sm = _itm_side_summary(side)
        sm["roc_per_min"] = (round(sm["d_iv"] / minutes, 4)
                             if sm["d_iv"] is not None and minutes > 0 else None)
        summary[side] = sm

    sc_id = _itm_classify(spot_chg_pct,
                          summary["ce"]["d_iv"], summary["pe"]["d_iv"])
    summary["scenario"] = {"id": sc_id, **ITM_SCENARIOS[sc_id]}
    itm_iv["windows"].append(summary)

    # ── the every-5-minute PRINT ──
    def _fmt(sm, tag):
        if sm["iv_first"] is None:
            return f"  {tag:5s} s{sm['strike']}: no ticks this window"
        return (f"  {tag:5s} s{sm['strike']}: IV {sm['iv_first']:.2f} → {sm['iv_live']:.2f}"
                f"  ΔIV={sm['d_iv']:+.2f}"
                f"  avgRoC={sm['avg_tick_roc'] if sm['avg_tick_roc'] is not None else 0:+.5f}/tick"
                f"  ({sm['roc_per_min'] if sm['roc_per_min'] is not None else 0:+.3f} IVpts/min, {sm['ticks']} ticks)")
    print("─" * 66)
    print(f"[{summary['end']}] ITM IV 5-min window {summary['start']}–{summary['end']}")
    print(_fmt(summary["ce"], "CALL"))
    print(_fmt(summary["pe"], "PUT"))
    print(f"  Spot: {summary['spot_open']} → {summary['spot_close']} "
          f"({spot_chg_pct:+.2f}%)" if spot_chg_pct is not None else "  Spot: n/a")
    print(f"  Scenario {sc_id} — {ITM_SCENARIOS[sc_id]['label']}")
    print("─" * 66)

    # reset window (keep strikes + prev_iv so tick RoC continues seamlessly)
    for side in ("ce", "pe"):
        s = itm_iv[side]
        s["iv_first"] = s["iv_live"]
        s["roc_sum"]  = 0.0
        s["roc_last"] = None
        s["ticks"]    = 0
    itm_iv["window_start"]     = now_ist()
    itm_iv["window_spot_open"] = spot
    itm_iv["rolls"]            = []


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
                    _itm_on_spot(new_spot)
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
                    "depth": {"buy": buy_levels, "sell": sell_levels},
                }
                _update_ltp_ohlc(token, current_ltp)
                _itm_on_option_tick(token, current_ltp)

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
    ITM IV rate-of-change tracker (fresh implementation).

    Tracks the 2-strike ITM CALL (ATM − 2×step) and 2-strike ITM PUT
    (ATM + 2×step). IV is computed on every tick; the per-tick RoC is
    averaged over a 5-minute window, classified into one of 8 scenarios,
    printed to the console and returned here.
    """
    with state_lock:
        spot       = state["spot"]
        expiry_str = state["expiry_date"]

        if not spot or itm_iv["anchor_spot"] is None:
            return jsonify({"available": False,
                            "reason": "ITM IV tracker not ready — waiting for ticks"})

        # Safety: also close the window from here in case ticks pause.
        _itm_maybe_close_window(spot)

        ws = itm_iv["window_start"]
        elapsed = (now_ist() - ws).total_seconds() if ws else 0
        spot_open = itm_iv["window_spot_open"]
        live_window = {
            "start":        ws.strftime("%H:%M:%S") if ws else None,
            "elapsed_s":    int(elapsed),
            "remaining_s":  max(0, ITM_WINDOW_SEC - int(elapsed)),
            "spot_open":    round(spot_open, 2) if spot_open else None,
            "spot_chg_pct": (round((spot - spot_open) / spot_open * 100, 3)
                             if spot_open else None),
            "ce":           _itm_side_summary("ce"),
            "pe":           _itm_side_summary("pe"),
            "rolls":        list(itm_iv["rolls"]),
        }
        completed = list(itm_iv["windows"])
        anchor    = itm_iv["anchor_spot"]

    dte = None
    if expiry_str:
        try:
            from datetime import date as _date
            dte = max(0, (_date.fromisoformat(expiry_str[:10]) - now_ist().date()).days)
        except Exception:
            pass

    return jsonify({
        "available":    True,
        "as_of":        now_ist().strftime("%H:%M:%S"),
        "spot":         round(spot, 2),
        "expiry_date":  expiry_str,
        "dte":          dte,
        "anchor_spot":  round(anchor, 2) if anchor else None,
        "roll_points":  ITM_ROLL_POINTS,
        "strike_step":  STRIKE_STEP,
        "window_sec":   ITM_WINDOW_SEC,
        "iv_th":        ITM_IV_TH,
        "spot_th":      ITM_SPOT_TH,
        "window":       live_window,
        "windows":      completed,          # oldest → newest
        "scenarios":    ITM_SCENARIOS,
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

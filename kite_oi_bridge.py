"""
Nifty + Sensex OI Bias Monitor — Railway Bridge
================================================
Tracks BOTH Nifty (NSE) and Sensex (BSE) simultaneously.
Each index has its own state, minute buffer, and DB table:
    nifty_oi_history   — Nifty 1-min candle rows
    sensex_oi_history  — Sensex 1-min candle rows

Railway setup
─────────────
Environment variables required:
    KITE_API_KEY       your Zerodha API key
    KITE_ACCESS_TOKEN  daily access token (update each morning)
    DATABASE_URL       auto-set by Railway Postgres plugin
    PORT               auto-set by Railway

Endpoints:
    GET  /                        serves dashboard HTML
    GET  /oi?index=nifty          live snapshot  (index = nifty | sensex)
    GET  /ltp?index=nifty         live LTP
    GET  /oi/history?index=nifty  1-min rows for today
    GET  /oi/live-candle?index=nifty  forming candle
    GET  /strikes?index=nifty     strike list
    GET  /health                  both indices status
    POST /reset-csv?index=nifty   wipe today's rows for one index
    POST /reset-csv?index=all     wipe today's rows for both indices
"""

import collections
import json
import os
import threading
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from kiteconnect import KiteTicker, KiteConnect


# ── CONFIG ────────────────────────────────────────────────────────────────────

API_KEY      = os.environ["KITE_API_KEY"]
ACCESS_TOKEN = os.environ["KITE_ACCESS_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
FLASK_PORT   = int(os.environ.get("PORT", 5000))

# Nifty
NIFTY_SPOT_TOKEN  = 256265
NIFTY_SPOT_SYMBOL = "NSE:NIFTY 50"
NIFTY_EXCHANGE    = "NFO"
NIFTY_NAME        = "NIFTY"
NIFTY_STRIKE_STEP = 50
NIFTY_ROLL_THR    = 100        # bear/bull roll threshold (points)
NIFTY_DB_TABLE    = "nifty_oi_history"

# Sensex
SENSEX_SPOT_TOKEN  = 265
SENSEX_SPOT_SYMBOL = "BSE:SENSEX"
SENSEX_EXCHANGE    = "BFO"
SENSEX_NAME        = "SENSEX"
SENSEX_STRIKE_STEP = 100
SENSEX_ROLL_THR    = 200
SENSEX_DB_TABLE    = "sensex_oi_history"

NUM_STRIKES       = 11          # ATM ± 5 for each index
OI_HISTORY_MAXLEN = 500


# ── APP ───────────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder="static")
CORS(app)

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)


# ── DATABASE ──────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    """Create both OI history tables if they don't exist."""
    with get_db() as conn:
        with conn.cursor() as cur:
            for table in (NIFTY_DB_TABLE, SENSEX_DB_TABLE):
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id          SERIAL PRIMARY KEY,
                        ts          DOUBLE PRECISION NOT NULL,
                        time_label  TEXT,
                        session_id  INTEGER,
                        trade_date  DATE DEFAULT CURRENT_DATE,
                        data        JSONB NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_{table}_date
                        ON {table} (trade_date);
                """)
        conn.commit()
    print(f"DB initialised — tables {NIFTY_DB_TABLE}, {SENSEX_DB_TABLE} ready.")


def db_write_row(table: str, row: dict):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {table} (ts, time_label, session_id, trade_date, data)
                    VALUES (%s, %s, %s, CURRENT_DATE, %s)
                    """,
                    (row.get("ts"), row.get("time_label"), row.get("session_id"), json.dumps(row)),
                )
            conn.commit()
    except Exception as e:
        print(f"DB write error ({table}): {e}")


def db_read_today(table: str) -> list:
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"SELECT data FROM {table} WHERE trade_date = CURRENT_DATE ORDER BY ts ASC"
                )
                return [dict(r["data"]) for r in cur.fetchall()]
    except Exception as e:
        print(f"DB read error ({table}): {e}")
        return []


def db_reset_today(table: str):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table} WHERE trade_date = CURRENT_DATE")
            conn.commit()
    except Exception as e:
        print(f"DB reset error ({table}): {e}")


# ── PER-INDEX STATE FACTORY ───────────────────────────────────────────────────

def make_state():
    return {
        "spot":          None,
        "last_anchored": None,
        "atm_strike":    None,
        "strikes":       [],
        "bear_strike":   None,
        "bull_strike":   None,
        "session_id":    0,
        "tokens":        {},   # instrument_token -> instrument dict
        "oi":            {},   # instrument_token -> {"oi": int, "ltp": float}
        "oi_baseline":   {},   # instrument_token -> baseline OI at session start
        "oi_prev_snap":  {},   # instrument_token -> OI at last /oi poll
        "roll_log":      [],
    }


def make_buffer():
    return {
        "start_ts":   None,
        "start_snap": None,
        "spot_open":  None,
        "spot_high":  None,
        "spot_low":   None,
        "spot_close": None,
        "ltp_ohlc":   {},   # token -> {"open","high","low","close"}
    }


# ── GLOBAL STATE ──────────────────────────────────────────────────────────────

nifty_state  = make_state()
sensex_state = make_state()

nifty_buf    = make_buffer()
sensex_buf   = make_buffer()

# in-memory fallback deques (used if DB is temporarily unavailable)
nifty_mem    = collections.deque(maxlen=OI_HISTORY_MAXLEN)
sensex_mem   = collections.deque(maxlen=OI_HISTORY_MAXLEN)

state_lock   = threading.Lock()

# quick lookup: spot token → (state, buffer, db_table, mem_deque, cfg)
INDEX_CFG = {
    NIFTY_SPOT_TOKEN: {
        "state": nifty_state, "buf": nifty_buf,
        "table": NIFTY_DB_TABLE, "mem": nifty_mem,
        "strike_step": NIFTY_STRIKE_STEP, "roll_thr": NIFTY_ROLL_THR,
        "name": "nifty",
    },
    SENSEX_SPOT_TOKEN: {
        "state": sensex_state, "buf": sensex_buf,
        "table": SENSEX_DB_TABLE, "mem": sensex_mem,
        "strike_step": SENSEX_STRIKE_STEP, "roll_thr": SENSEX_ROLL_THR,
        "name": "sensex",
    },
}

# reverse lookup: option token → spot token (populated during initialise)
option_to_spot = {}


# ── INSTRUMENT HELPERS ────────────────────────────────────────────────────────

def round_to_nearest(price, step):
    return round(price / step) * step


def compute_strikes_window(atm, step):
    half = NUM_STRIKES // 2
    return [atm + (i - half) * step for i in range(NUM_STRIKES)]


def compute_bear_bull(spot, step):
    base = round_to_nearest(spot, step)
    return base - 2 * step, base + 2 * step


def get_instruments_for_strikes(strikes, exchange, name):
    instruments = kite.instruments(exchange)
    opts = [
        i for i in instruments
        if i["name"] == name and i["instrument_type"] in ("CE", "PE")
    ]
    expiries   = sorted(set(i["expiry"] for i in opts))
    front      = expiries[0]
    strike_set = set(strikes)
    result     = []
    for i in opts:
        if i["expiry"] != front or i["strike"] not in strike_set:
            continue
        k = str(int(i["strike"]))
        t = i["instrument_type"].lower()
        i["role_key"] = f"s{k}_{t}"
        result.append(i)
    found   = {i["strike"] for i in result}
    missing = strike_set - found
    if missing:
        print(f"  [{name}] Warning: strikes not in {exchange}: {sorted(missing)}")
    return result


def get_live_spot(symbol):
    q = kite.quote(symbol)
    return q[symbol]["last_price"]


# ── SNAPSHOT ──────────────────────────────────────────────────────────────────

def _current_snap(st, ts):
    snap = {"ts": ts, "spot": st["spot"]}
    for token, meta in st["tokens"].items():
        entry = st["oi"].get(token, {})
        rk    = meta["role_key"]
        snap[rk + "_oi"]       = entry.get("oi", 0)
        snap[rk + "_ltp"]      = entry.get("ltp", 0)
        snap[rk + "_baseline"] = st["oi_baseline"].get(token, 0)
    return snap


# ── SPOT / LTP OHLC ───────────────────────────────────────────────────────────

def _update_spot_ohlc(buf, spot):
    if spot is None:
        return
    if buf["spot_open"] is None:
        buf["spot_open"] = buf["spot_high"] = buf["spot_low"] = spot
    else:
        if spot > buf["spot_high"]: buf["spot_high"] = spot
        if spot < buf["spot_low"]:  buf["spot_low"]  = spot
    buf["spot_close"] = spot


def _update_ltp_ohlc(buf, token, ltp):
    if not ltp:
        return
    b = buf["ltp_ohlc"]
    if token not in b:
        b[token] = {"open": ltp, "high": ltp, "low": ltp, "close": ltp}
    else:
        if ltp > b[token]["high"]: b[token]["high"] = ltp
        if ltp < b[token]["low"]:  b[token]["low"]  = ltp
        b[token]["close"] = ltp


# ── 1-MINUTE AGGREGATOR ───────────────────────────────────────────────────────

def _append_history(st, buf, table, mem):
    now          = time.time()
    current_snap = _current_snap(st, now)

    if buf["start_ts"] is None:
        buf["start_ts"]   = now
        buf["start_snap"] = current_snap
        return

    if now - buf["start_ts"] < 60:
        return

    start = buf["start_snap"]
    close = current_snap

    row = {
        "ts":          buf["start_ts"],
        "time_label":  datetime.fromtimestamp(buf["start_ts"]).strftime("%H:%M"),
        "session_id":  st["session_id"],
        "bear_strike": st["bear_strike"],
        "bull_strike": st["bull_strike"],
        "spot_open":   round(buf["spot_open"])  if buf["spot_open"]  else 0,
        "spot_high":   round(buf["spot_high"])  if buf["spot_high"]  else 0,
        "spot_low":    round(buf["spot_low"])   if buf["spot_low"]   else 0,
        "spot_close":  round(buf["spot_close"]) if buf["spot_close"] else 0,
    }

    for token, meta in st["tokens"].items():
        rk        = meta["role_key"]
        close_ltp = close.get(rk + "_ltp", 0)
        ohlc      = buf["ltp_ohlc"].get(token, {})
        row[rk + "_oi"]        = close.get(rk + "_oi", 0)
        row[rk + "_ltp"]       = close_ltp
        row[rk + "_ltp_open"]  = ohlc.get("open",  close_ltp)
        row[rk + "_ltp_high"]  = ohlc.get("high",  close_ltp)
        row[rk + "_ltp_low"]   = ohlc.get("low",   close_ltp)
        row[rk + "_ltp_close"] = ohlc.get("close", close_ltp)
        row[rk + "_baseline"]  = close.get(rk + "_baseline", 0)
        row[rk + "_delta"]     = close.get(rk + "_oi", 0) - start.get(rk + "_oi", 0)

    # non-blocking DB write
    threading.Thread(target=db_write_row, args=(table, row), daemon=True).start()
    mem.append(row)

    print(
        f"[{table[:5].upper()} {row['time_label']}] "
        f"O={row['spot_open']} H={row['spot_high']} L={row['spot_low']} C={row['spot_close']}  "
        f"bear={row['bear_strike']} bull={row['bull_strike']} session={row['session_id']}"
    )

    # roll buffer forward
    buf["start_ts"]   = now
    buf["start_snap"] = current_snap
    buf["spot_open"]  = buf["spot_close"]
    buf["spot_high"]  = buf["spot_close"]
    buf["spot_low"]   = buf["spot_close"]
    new_ohlc = {}
    for token in buf["ltp_ohlc"]:
        lc = buf["ltp_ohlc"][token]["close"]
        new_ohlc[token] = {"open": lc, "high": lc, "low": lc, "close": lc}
    buf["ltp_ohlc"] = new_ohlc


# ── STRIKE ROLL ───────────────────────────────────────────────────────────────

def _check_roll(st, new_spot, strike_step, roll_thr):
    if st["last_anchored"] is None or new_spot is None:
        return
    if abs(new_spot - st["last_anchored"]) < roll_thr:
        return

    bear, bull   = compute_bear_bull(new_spot, strike_step)
    min_s, max_s = min(st["strikes"]), max(st["strikes"])
    bear = max(min_s, min(bear, max_s))
    bull = max(min_s, min(bull, max_s))

    if bear >= bull:
        ss    = sorted(st["strikes"])
        below = [s for s in ss if s <= new_spot]
        above = [s for s in ss if s >  new_spot]
        bear  = below[-1] if below else ss[0]
        bull  = above[0]  if above else ss[-1]
        if bear == bull and len(ss) > 1:
            idx  = ss.index(bear)
            bear = ss[max(0, idx - 1)]
            bull = ss[min(len(ss) - 1, idx + 1)]

    if bear == st["bear_strike"] and bull == st["bull_strike"]:
        st["last_anchored"] = new_spot
        return

    prev_bear, prev_bull = st["bear_strike"], st["bull_strike"]
    st["session_id"]   += 1
    st["bear_strike"]   = bear
    st["bull_strike"]   = bull
    st["last_anchored"] = new_spot
    st["roll_log"].append({
        "time":       datetime.now().strftime("%H:%M:%S"),
        "session_id": st["session_id"],
        "from_spot":  round(new_spot),
        "from_bear":  prev_bear,
        "from_bull":  prev_bull,
        "to_bear":    bear,
        "to_bull":    bull,
    })
    print(
        f"Roll → Bear {prev_bear}→{bear}  Bull {prev_bull}→{bull}  "
        f"Session {st['session_id']}"
    )


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

                # ── Spot index ticks ──────────────────────────────────────
                if token in INDEX_CFG:
                    cfg      = INDEX_CFG[token]
                    st       = cfg["state"]
                    buf      = cfg["buf"]
                    new_spot = tick.get("last_price", st["spot"])
                    st["spot"] = new_spot
                    _update_spot_ohlc(buf, new_spot)
                    _check_roll(st, new_spot, cfg["strike_step"], cfg["roll_thr"])
                    continue

                # ── Option ticks ──────────────────────────────────────────
                spot_token = option_to_spot.get(token)
                if spot_token is None:
                    continue

                cfg        = INDEX_CFG[spot_token]
                st         = cfg["state"]
                buf        = cfg["buf"]
                current_oi = tick.get("oi", 0)
                current_ltp= tick.get("last_price", 0)

                if current_oi == 0:
                    continue

                if token not in st["oi_baseline"]:
                    st["oi_baseline"][token] = current_oi
                    st["oi_prev_snap"][token]= current_oi
                    rk = st["tokens"][token]["role_key"]
                    print(f"[{cfg['name'].upper()} baseline] {rk}: OI={current_oi:,}")

                st["oi"][token] = {"oi": current_oi, "ltp": current_ltp}
                _update_ltp_ohlc(buf, token, current_ltp)

            # ── Candle aggregation for each index ─────────────────────────
            for cfg in INDEX_CFG.values():
                _append_history(cfg["state"], cfg["buf"], cfg["table"], cfg["mem"])

    def on_connect(ws, response):
        with state_lock:
            all_tokens = (
                list(INDEX_CFG.keys())                    # both spot tokens
                + list(nifty_state["tokens"].keys())      # nifty options
                + list(sensex_state["tokens"].keys())     # sensex options
            )
        ws.subscribe(all_tokens)
        ws.set_mode(ws.MODE_FULL, all_tokens)
        print(
            f"Subscribed: {len(all_tokens)} tokens total "
            f"(2 spot + {len(nifty_state['tokens'])} nifty + {len(sensex_state['tokens'])} sensex)"
        )

    def on_error(ws, code, reason):
        print(f"Ticker error {code}: {reason}")

    def on_close(ws, code, reason):
        print(f"Ticker closed: {reason}. Reconnecting in 5s...")
        time.sleep(5)
        build_ticker()

    ticker_instance.on_ticks   = on_ticks
    ticker_instance.on_connect = on_connect
    ticker_instance.on_error   = on_error
    ticker_instance.on_close   = on_close
    ticker_instance.connect(threaded=True)


# ── STARTUP ───────────────────────────────────────────────────────────────────

def initialise_index(spot_token, spot_symbol, exchange, name, strike_step, roll_thr, st):
    """Fetch live spot, compute window, find instruments, populate state."""
    print(f"\n{'─'*50}")
    print(f"Initialising {name} ...")
    seed_spot = get_live_spot(spot_symbol)
    print(f"  Live spot: {seed_spot:,.2f}")

    atm     = round_to_nearest(seed_spot, strike_step)
    strikes = compute_strikes_window(atm, strike_step)
    bear, bull = compute_bear_bull(seed_spot, strike_step)
    bear = max(min(strikes), min(bear, max(strikes)))
    bull = max(min(strikes), min(bull, max(strikes)))

    if bear >= bull:
        ss    = sorted(strikes)
        below = [s for s in ss if s <= seed_spot]
        above = [s for s in ss if s >  seed_spot]
        bear  = below[-1] if below else ss[0]
        bull  = above[0]  if above else ss[-1]

    print(f"  ATM: {atm}  |  Window: {strikes[0]} – {strikes[-1]}")
    print(f"  Initial bear: {bear}  bull: {bull}")

    instruments = get_instruments_for_strikes(strikes, exchange, name)
    print(f"  Instruments found: {len(instruments)} (expected {len(strikes)*2})")
    for i in instruments:
        print(f"    {i['role_key']:15s}: {i['tradingsymbol']:25s}  token={i['instrument_token']}  expiry={i['expiry']}")

    token_map = {i["instrument_token"]: i for i in instruments}

    with state_lock:
        st["atm_strike"]    = atm
        st["strikes"]       = strikes
        st["bear_strike"]   = bear
        st["bull_strike"]   = bull
        st["last_anchored"] = seed_spot
        st["tokens"]        = token_map

        # register option → spot_token mapping for tick routing
        for tok in token_map:
            option_to_spot[tok] = spot_token


def initialise_all():
    initialise_index(
        NIFTY_SPOT_TOKEN, NIFTY_SPOT_SYMBOL,
        NIFTY_EXCHANGE, NIFTY_NAME,
        NIFTY_STRIKE_STEP, NIFTY_ROLL_THR,
        nifty_state,
    )
    initialise_index(
        SENSEX_SPOT_TOKEN, SENSEX_SPOT_SYMBOL,
        SENSEX_EXCHANGE, SENSEX_NAME,
        SENSEX_STRIKE_STEP, SENSEX_ROLL_THR,
        sensex_state,
    )
    build_ticker()


# ── HELPERS FOR ENDPOINTS ─────────────────────────────────────────────────────

def _resolve_index(req):
    """Return (state, buf, table, mem) for ?index= param. Defaults to nifty."""
    idx = req.args.get("index", "nifty").lower()
    if idx == "sensex":
        return sensex_state, sensex_buf, SENSEX_DB_TABLE, sensex_mem
    return nifty_state, nifty_buf, NIFTY_DB_TABLE, nifty_mem


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────

@app.route("/")
def serve_dashboard():
    return send_from_directory("static", "index.html")


@app.route("/oi")
def get_oi():
    st, buf, table, mem = _resolve_index(request)
    with state_lock:
        result = {
            "spot":        round(st["spot"]) if st["spot"] else None,
            "session_id":  st["session_id"],
            "atm_strike":  st["atm_strike"],
            "bear_strike": st["bear_strike"],
            "bull_strike": st["bull_strike"],
            "strikes":     st["strikes"],
            "roll_log":    list(st["roll_log"]),
            "as_of":       datetime.now().strftime("%H:%M:%S"),
            "options":     {},
        }
        for token, meta in st["tokens"].items():
            snap       = st["oi"].get(token, {})
            current_oi = snap.get("oi", 0)
            baseline   = st["oi_baseline"].get(token, current_oi)
            prev_snap  = st["oi_prev_snap"].get(token, current_oi)
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
            st["oi_prev_snap"][token] = current_oi
    return jsonify(result)


@app.route("/ltp")
def get_ltp():
    st, buf, table, mem = _resolve_index(request)
    with state_lock:
        result = {
            "spot":    st["spot"],
            "as_of":   datetime.now().strftime("%H:%M:%S"),
            "options": {},
        }
        for token, meta in st["tokens"].items():
            snap = st["oi"].get(token, {})
            result["options"][meta["role_key"]] = {
                "strike": meta["strike"],
                "type":   meta["instrument_type"],
                "symbol": meta["tradingsymbol"],
                "ltp":    snap.get("ltp", 0),
            }
    return jsonify(result)


@app.route("/strikes")
def get_strikes():
    st, buf, table, mem = _resolve_index(request)
    with state_lock:
        return jsonify({
            "atm_strike":  st["atm_strike"],
            "bear_strike": st["bear_strike"],
            "bull_strike": st["bull_strike"],
            "strikes":     st["strikes"],
            "session_id":  st["session_id"],
        })


@app.route("/oi/history")
def get_history():
    st, buf, table, mem = _resolve_index(request)
    rows = db_read_today(table)
    if not rows:
        rows = list(mem)   # fallback to in-memory
    return jsonify(rows)


@app.route("/oi/live-candle")
def live_candle():
    st, buf, table, mem = _resolve_index(request)
    with state_lock:
        if buf["start_ts"] is None or buf["start_snap"] is None:
            return jsonify({"available": False})

        now          = time.time()
        current_snap = _current_snap(st, now)
        start        = buf["start_snap"]
        elapsed      = round(now - buf["start_ts"])

        result = {
            "available":   True,
            "elapsed_sec": elapsed,
            "time_label":  datetime.fromtimestamp(buf["start_ts"]).strftime("%H:%M") + "*",
            "spot_open":   round(buf["spot_open"])  if buf["spot_open"]  else 0,
            "spot_high":   round(buf["spot_high"])  if buf["spot_high"]  else 0,
            "spot_low":    round(buf["spot_low"])   if buf["spot_low"]   else 0,
            "spot_close":  round(buf["spot_close"]) if buf["spot_close"] else 0,
            "bear_strike": st["bear_strike"],
            "bull_strike": st["bull_strike"],
            "deltas":      {},
        }
        for token, meta in st["tokens"].items():
            rk         = meta["role_key"]
            current_oi = st["oi"].get(token, {}).get("oi", 0)
            start_oi   = start.get(rk + "_oi", current_oi)
            result["deltas"][rk] = {"oi": current_oi, "delta": current_oi - start_oi}

    return jsonify(result)


@app.route("/health")
def health():
    with state_lock:
        def _snap(st, buf):
            return {
                "spot":         st["spot"],
                "session_id":   st["session_id"],
                "atm_strike":   st["atm_strike"],
                "bear_strike":  st["bear_strike"],
                "bull_strike":  st["bull_strike"],
                "strikes":      st["strikes"],
                "tokens":       len(st["tokens"]),
                "roll_count":   len(st["roll_log"]),
                "spot_ohlc": {
                    "open":  buf["spot_open"],
                    "high":  buf["spot_high"],
                    "low":   buf["spot_low"],
                    "close": buf["spot_close"],
                },
            }
        return jsonify({
            "status":  "ok",
            "nifty":   _snap(nifty_state,  nifty_buf),
            "sensex":  _snap(sensex_state, sensex_buf),
        })


@app.route("/reset-csv", methods=["POST"])
def reset_csv():
    idx = request.args.get("index", "nifty").lower()

    def _reset(st, buf, table, mem):
        db_reset_today(table)
        mem.clear()
        with state_lock:
            st["session_id"]  = 0
            buf["start_ts"]   = None
            buf["start_snap"] = None
            buf["spot_open"]  = None
            buf["spot_high"]  = None
            buf["spot_low"]   = None
            buf["spot_close"] = None
            buf["ltp_ohlc"]   = {}
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {table} reset.")

    if idx == "all":
        _reset(nifty_state,  nifty_buf,  NIFTY_DB_TABLE,  nifty_mem)
        _reset(sensex_state, sensex_buf, SENSEX_DB_TABLE, sensex_mem)
        msg = "Both nifty_oi_history and sensex_oi_history cleared."
    elif idx == "sensex":
        _reset(sensex_state, sensex_buf, SENSEX_DB_TABLE, sensex_mem)
        msg = "sensex_oi_history cleared."
    else:
        _reset(nifty_state, nifty_buf, NIFTY_DB_TABLE, nifty_mem)
        msg = "nifty_oi_history cleared."

    return jsonify({"status": "ok", "message": msg})


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("Nifty + Sensex OI Monitor — Railway Bridge")
    print("=" * 60)

    print("\nInitialising database tables...")
    init_db()

    print("\nInitialising strike windows + WebSocket...")
    initialise_all()

    print(f"\nFlask listening on 0.0.0.0:{FLASK_PORT}")
    print(f"  GET  /                         dashboard HTML")
    print(f"  GET  /oi?index=nifty|sensex    live OI snapshot")
    print(f"  GET  /ltp?index=nifty|sensex   live LTP")
    print(f"  GET  /oi/history?index=...     1-min rows today")
    print(f"  GET  /oi/live-candle?index=... forming candle")
    print(f"  GET  /strikes?index=...        strike list")
    print(f"  GET  /health                   both indices status")
    print(f"  POST /reset-csv?index=nifty|sensex|all")
    print("\nPress Ctrl+C to stop.\n")

    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)

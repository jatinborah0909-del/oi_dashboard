# Nifty + Sensex OI Bias Monitor — Railway Deployment

## File structure

```
your-repo/
├── kite_oi_bridge.py      Flask app + Kite WebSocket bridge
├── requirements.txt
├── Procfile
├── railway.json
└── static/
    └── index.html         Dashboard (Nifty / Sensex switcher at top)
```

---

## Database tables

| Table               | Contents                        |
|---------------------|---------------------------------|
| `nifty_oi_history`  | Nifty 1-min candle rows (JSONB) |
| `sensex_oi_history` | Sensex 1-min candle rows (JSONB)|

Both tables are created automatically on first startup.
Each row stores the full candle (all 11 strikes × CE+PE) in a JSONB `data` column.

---

## One-time Railway setup (5 minutes)

1. Push this folder to a GitHub repo.
2. Railway → **New Project** → **Deploy from GitHub repo** → select your repo.
3. Railway → **+ New** → **Database** → **Add PostgreSQL**.
   Railway auto-sets `DATABASE_URL` — do NOT set it manually.
4. Railway → your service → **Variables** → add:

   | Variable            | Value                   |
   |---------------------|-------------------------|
   | `KITE_API_KEY`      | your Zerodha API key    |
   | `KITE_ACCESS_TOKEN` | today's access token    |

5. Railway → **Settings** → **Domains** → **Generate Domain**.
   Your public URL is live. Open it — the dashboard loads.

---

## Daily routine (2 minutes each morning)

1. Log in to Zerodha developer console → copy today's `access_token`.
2. Railway → your service → **Variables** → update `KITE_ACCESS_TOKEN`.
3. Railway auto-redeploys in ~30 seconds.
4. Open your Railway URL → dashboard is live for the day.
5. Click **Reset CSV** button on the dashboard to clear yesterday's data.

Close your laptop. The server keeps running all day.

---

## Using the dashboard

The top status bar now has an **Index** switcher:

```
Interval:  1m  3m  5m  10m  15m  |  Index:  Nifty  Sensex
```

Click **Nifty** or **Sensex** to switch. All charts, OI panels, and history
update instantly. Each index has completely independent data.

---

## API endpoints

All endpoints accept an optional `?index=nifty` or `?index=sensex` parameter.
Default is `nifty` if omitted.

| Endpoint                          | Description                        |
|-----------------------------------|------------------------------------|
| `GET /`                           | Dashboard HTML                     |
| `GET /oi?index=nifty`             | Live OI snapshot                   |
| `GET /ltp?index=sensex`           | Live LTP for all tokens            |
| `GET /oi/history?index=nifty`     | Today's 1-min rows from DB         |
| `GET /oi/live-candle?index=nifty` | Currently forming candle           |
| `GET /strikes?index=sensex`       | Strike list                        |
| `GET /health`                     | Both indices status                |
| `POST /reset-csv?index=nifty`     | Wipe today's Nifty rows            |
| `POST /reset-csv?index=sensex`    | Wipe today's Sensex rows           |
| `POST /reset-csv?index=all`       | Wipe both                          |

---

## Useful DB queries (Railway → Postgres → Query tab)

```sql
-- Today's Nifty candles
SELECT time_label, data->>'spot_close', data->>'bear_strike'
FROM nifty_oi_history
WHERE trade_date = CURRENT_DATE ORDER BY ts;

-- Today's Sensex candles
SELECT time_label, data->>'spot_close', data->>'bear_strike'
FROM sensex_oi_history
WHERE trade_date = CURRENT_DATE ORDER BY ts;

-- Row counts per day
SELECT trade_date, COUNT(*) FROM nifty_oi_history GROUP BY trade_date ORDER BY 1;
SELECT trade_date, COUNT(*) FROM sensex_oi_history GROUP BY trade_date ORDER BY 1;
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Red dot / "Bridge offline" | Check Railway logs. Most likely `KITE_ACCESS_TOKEN` expired — update it. |
| No Sensex data | BSE options trade on BFO exchange. Verify your Zerodha account has BFO segment enabled. |
| Missing strikes | Some far OTM strikes may not exist on a given expiry. Check Railway logs for "Warning: strikes not found". |
| Service crashes on startup | Check logs — usually a wrong API key or network issue reaching Zerodha. |

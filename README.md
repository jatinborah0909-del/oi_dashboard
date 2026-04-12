# Nifty OI Bias Monitor — Railway Deployment

## File structure

```
your-repo/
├── kite_oi_bridge.py   ← Flask app + Kite WebSocket bridge (PostgreSQL edition)
├── requirements.txt
├── Procfile
├── railway.json
└── static/
    └── index.html      ← Dashboard (served at your public URL)
```

---

## One-time Railway setup (5 minutes)

### 1. Push to GitHub
Create a new GitHub repo and push all these files.

### 2. Create Railway project
1. Go to https://railway.app → **New Project** → **Deploy from GitHub repo**
2. Select your repo.

### 3. Add PostgreSQL
In your Railway project → **+ New** → **Database** → **Add PostgreSQL**.
Railway automatically sets `DATABASE_URL` in your service's environment.

### 4. Set environment variables
In your Railway service → **Variables** tab → add:

| Variable            | Value                        |
|---------------------|------------------------------|
| `KITE_API_KEY`      | your Zerodha API key         |
| `KITE_ACCESS_TOKEN` | your daily access token      |

> `PORT` and `DATABASE_URL` are injected by Railway automatically — do NOT set them.

### 5. Deploy
Railway builds and deploys automatically on every push.
Click **Settings → Domains → Generate Domain** to get your public URL.

---

## Daily usage

| Task                        | How                                              |
|-----------------------------|--------------------------------------------------|
| Morning start               | Just open the dashboard URL — app is always running |
| New access token each day   | Update `KITE_ACCESS_TOKEN` in Railway Variables → Railway redeploys in ~30s |
| Reset data each morning     | Click **Reset CSV** button on the dashboard (calls `POST /reset-csv`) |
| View logs                   | Railway → your service → **Deployments** → **View Logs** |

---

## What changed from the local version

| | Local | Railway |
|---|---|---|
| Data storage | `oi_history.csv` file | PostgreSQL table `oi_history` |
| How HTML is served | Open file directly in browser | Flask serves it at `/` |
| API base URL in HTML | `http://localhost:5000` | `''` (same origin) |
| Credentials | Hard-coded in `.py` | Environment variables |
| Port | 5000 fixed | `PORT` env var (set by Railway) |

All dashboard behaviour, polling intervals, charts, and logic are **identical**.

---

## Troubleshooting

**Bridge offline / red dot**
- Check Railway logs for errors.
- Most common cause: `KITE_ACCESS_TOKEN` expired. Update it in Variables.

**No history data after restart**
- Data is in PostgreSQL and persists across restarts. Today's rows load automatically.

**"relation oi_history does not exist"**
- The table is created automatically on first start via `init_db()`.

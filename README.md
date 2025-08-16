# monitor

Utility scripts for monitoring financial indicators and sending Telegram alerts.

## Scripts

- `monitor_brent.py` – Brent crude oil price watcher.
- `monitor_cn10y.py` – China 10-year government bond yield watcher. Fetches
  data via [Akshare](https://akshare.akfamily.xyz/) and reads notification
  credentials from the following environment variables:
  - `TELEGRAM_BOT_TOKEN`
  - `TELEGRAM_CHAT_ID`
  - `THRESHOLD` (optional; default `1.85`)

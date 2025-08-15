# monitor

Utility scripts for monitoring financial indicators and sending Telegram alerts.

## Scripts

- `monitor_brent.py` – Brent crude oil price watcher.
- `monitor_cn10y.py` – China 10-year government bond yield watcher. Uses the
  TradingEconomics API and reads the key from the `TE_API_KEY` environment
  variable (falls back to public `guest:guest` credentials).

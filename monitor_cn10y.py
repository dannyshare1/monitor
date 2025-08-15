import os
import sys
from datetime import datetime
import requests

THRESHOLD = float(os.getenv("THRESHOLD", "1.85"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DEFAULT_API_URLS = [
    # TradingEconomics current endpoint (2024-2025)
    "https://api.tradingeconomics.com/bond/yield/country/china?maturity=10y&c=guest:guest",
    # Legacy endpoint kept for backwards compatibility
    "https://api.tradingeconomics.com/bonds/cn-10y?c=guest:guest",
]

# Custom API endpoint can be supplied via environment variable. If provided it will
# override the default list above. This allows the script to be flexible if the
# upstream service changes again or if a user wants to point to a different data
# source.
API_URLS = [os.getenv("API_URL")] if os.getenv("API_URL") else DEFAULT_API_URLS


def fail(msg: str, code: int = 1) -> None:
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(code)


def fetch_yield() -> tuple[float, datetime.date]:
    """Fetch the latest CN 10Y yield from the first working API endpoint.

    Multiple endpoints are tried in order. The first successful response (HTTP
    200 and valid JSON body) is used. If all endpoints fail an error is raised.
    """
    last_error = None
    for url in API_URLS:
        try:
            r = requests.get(url, timeout=20)
        except Exception as e:  # network errors
            last_error = e
            continue
        if r.status_code != 200:
            last_error = RuntimeError(f"HTTP {r.status_code}: {r.text}")
            continue
        try:
            data = r.json()
        except Exception as e:
            last_error = e
            continue
        if isinstance(data, list) and data:
            data = data[0]
        break
    else:
        fail(f"API 请求失败：{last_error}")
    yld = None
    for key in (
        "yield",
        "Yield",
        "close",
        "Close",
        "last",
        "Last",
        "value",
        "Value",
        "LatestValue",
        "latestValue",
    ):
        if key in data:
            try:
                yld = float(data[key])
                break
            except (TypeError, ValueError):
                pass
    if yld is None:
        fail(f"未从 API 返回数据中解析到利率字段：{data}")
    date_str = data.get("Date") or data.get("date") or data.get("datetime")
    try:
        day = datetime.fromisoformat(date_str).date() if date_str else datetime.utcnow().date()
    except Exception:
        day = datetime.utcnow().date()
    return yld, day


def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        fail("未配置 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID。")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=20)
    if r.status_code != 200:
        fail(f"Telegram 推送失败：HTTP {r.status_code} - {r.text}")
    print("[OK] 已推送至 Telegram。")


def main() -> None:
    print("[INFO] 获取中国10年期国债收益率……")
    yld, day = fetch_yield()
    print(f"[INFO] 最近收盘（{day}）：{yld:.2f}%；阈值：{THRESHOLD:.2f}%")
    if yld >= THRESHOLD:
        msg = (
            "🇨🇳 *China 10Y Government Bond*\n"
            f"{day}: 收盘利率 {yld:.2f}% ≥ 阈值 {THRESHOLD:.2f}%"
        )
        send_telegram(msg)
    else:
        print("[INFO] 暂不触发。")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        fail(str(e))

import os
import sys
from datetime import datetime, timedelta, date
import requests
import tushare as ts

THRESHOLD = float(os.getenv("THRESHOLD", "1.85"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")

def fail(msg: str, code: int = 1) -> None:
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(code)

if not TUSHARE_TOKEN:
    fail("未配置 TUSHARE_TOKEN")
PRO = ts.pro_api(TUSHARE_TOKEN)


def fetch_yield() -> tuple[float, date]:
    """从 Tushare 获取最新的中国10年期国债收益率。"""
    today = datetime.utcnow().strftime("%Y%m%d")
    try:
        df = PRO.bond_yield(trade_date=today)
        if df.empty:
            start = (datetime.utcnow() - timedelta(days=7)).strftime("%Y%m%d")
            df = PRO.bond_yield(start_date=start, end_date=today)
    except Exception as e:
        raise RuntimeError(f"Tushare 请求失败：{e}")
    if df.empty:
        raise RuntimeError("Tushare 未返回数据")
    row = df.sort_values(by=df.columns[0]).iloc[-1]
    date_str = str(row.get("date") or row.get("trade_date") or row.get("cal_date"))
    try:
        day = datetime.strptime(date_str, "%Y%m%d").date()
    except Exception:
        day = datetime.utcnow().date()
    yld = None
    for key in ("yield", "yld", "close", "rate", "value"):
        if key in row:
            try:
                yld = float(row[key])
                break
            except Exception:
                pass
    if yld is None:
        for col in row.index:
            if col in ("date", "trade_date", "cal_date"):
                continue
            try:
                yld = float(row[col])
                break
            except Exception:
                continue
    if yld is None:
        raise RuntimeError(f"未从返回数据中解析到利率字段：{row.to_dict()}")
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

import os
import sys
from datetime import datetime, date
import requests
import pandas as pd
import akshare as ak

THRESHOLD = float(os.getenv("THRESHOLD", "1.85"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def fail(msg: str, code: int = 1) -> None:
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(code)

def fetch_yield() -> tuple[float, date]:
    """从 Akshare 获取最新的中国10年期国债收益率。"""
    try:
        df = ak.bond_china_yield()
    except Exception as e:
        raise RuntimeError(f"Akshare 请求失败：{e}")
    if df.empty:
        raise RuntimeError("Akshare 未返回数据")
    target = df[df.apply(lambda r: r.astype(str).str.contains("国债收益率:10年").any(), axis=1)]
    if target.empty:
        raise RuntimeError("未找到国债收益率:10年")
    row = target.iloc[0]
    day = None
    for key in ("日期", "最新日期", "date"):
        if key in row.index:
            try:
                day = pd.to_datetime(row[key]).date()
                break
            except Exception:
                pass
    if day is None:
        day = datetime.utcnow().date()
    yld = None
    for key in ("最新值", "收益率", "利率", "value", "close"):
        if key in row.index:
            try:
                yld = float(row[key])
                break
            except Exception:
                pass
    if yld is None:
        for col, val in row.items():
            if "日" in col or "名" in col or "指" in col:
                continue
            try:
                yld = float(val)
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

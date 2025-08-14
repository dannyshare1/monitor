import os
import sys
import time
import json
from datetime import datetime
import pandas as pd
import requests

# yfinance 作为数据源（Yahoo Finance：BZ=F -> Brent Crude Futures）
import yfinance as yf

SYMBOL = os.getenv("SYMBOL", "BZ=F").strip()
THRESHOLD = float(os.getenv("THRESHOLD", "70"))
CONSECUTIVE_DAYS = int(os.getenv("CONSECUTIVE_DAYS", "5"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def fail(msg: str, code: int = 1):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)

def fetch_daily(symbol: str, lookback_days: int = 30) -> pd.DataFrame:
    """
    拉取最近 N 个自然日的日线，返回按日期升序的 DataFrame（仅含交易日）。
    """
    df = yf.download(
        symbol,
        period=f"{lookback_days}d",
        interval="1d",
        auto_adjust=False,
        progress=False,
    )
    if df is None or df.empty:
        fail("无法获取行情数据（返回为空）。")
    df = df.dropna(subset=["Close"]).copy()
    # 统一为本地 naive 日期索引
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df.sort_index(inplace=True)
    return df

def is_consecutive_bdays(idx: pd.DatetimeIndex) -> bool:
    if len(idx) <= 1:
        return True
    # 生成业务日范围并比较长度与元素一致性
    rng = pd.bdate_range(idx[0], idx[-1])
    return len(rng) == len(idx) and all(a == b for a, b in zip(idx, rng))

def sequence_just_turned_true(df: pd.DataFrame, threshold: float, k: int) -> tuple[bool, pd.DataFrame]:
    """
    判断“最近 k 个交易日收盘都 > threshold 且为连续交易日”，并且
    在此之前一天没有满足（避免重复提醒）。
    返回 (should_alert, last_k_df)
    """
    if len(df) < k:
        return False, df.tail(k)

    last_k = df.tail(k)
    # 条件 A：k 个连续交易日
    if not is_consecutive_bdays(last_k.index):
        return False, last_k

    # 条件 B：这 k 天收盘都 > 阈值
    if not (last_k["Close"] > threshold).all():
        return False, last_k

    # 反复提醒抑制：检查“再往前一天”是否也 > 阈值并且形成 k+1 连续交易日
    # 若 last_(k+1) 也是连续且全部 > 阈值，则说明此前已经满足过，不再提醒。
    if len(df) >= k + 1:
        last_kp1 = df.tail(k + 1)
        if is_consecutive_bdays(last_kp1.index) and (last_kp1["Close"] > threshold).all():
            return False, last_k

    return True, last_k

def fmt_usd(x: float) -> str:
    return f"${x:,.2f}"

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

def main():
    print(f"[INFO] 获取 {SYMBOL} 日线数据……")
    df = fetch_daily(SYMBOL, lookback_days=40)

    should_alert, last_k = sequence_just_turned_true(df, THRESHOLD, CONSECUTIVE_DAYS)

    print("[DEBUG] 最近收盘：")
    print(last_k[["Close"]])

    if not should_alert:
        last_close = df["Close"].iloc[-1]
        last_date = df.index[-1].date()
        print(f"[INFO] 暂不触发。最近收盘（{last_date}）：{fmt_usd(last_close)}；阈值：{fmt_usd(THRESHOLD)}。")
        return

    # 构造消息
    start_date = last_k.index[0].date()
    end_date = last_k.index[-1].date()
    closes_text = "\n".join(
        f"- {d.date()}: {fmt_usd(c)}" for d, c in zip(last_k.index, last_k["Close"].tolist())
    )

    msg = (
        f"🛢 *Brent Watcher*\n"
        f"连续 *{CONSECUTIVE_DAYS}* 个交易日收盘价 > *{fmt_usd(THRESHOLD)}*（{start_date} → {end_date}）：\n"
        f"{closes_text}\n\n"
        f"标的：`{SYMBOL}`（Yahoo Finance）"
    )
    send_telegram(msg)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        fail(str(e))

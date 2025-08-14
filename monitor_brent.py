import os
import sys
from datetime import datetime
import pandas as pd
import requests
import yfinance as yf

SYMBOL = os.getenv("SYMBOL", "BZ=F").strip()
THRESHOLD = float(os.getenv("THRESHOLD", "70"))
CONSECUTIVE_DAYS = int(os.getenv("CONSECUTIVE_DAYS", "5"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def fail(msg: str, code: int = 1):
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(code)

def _normalize_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    兼容 yfinance 可能返回的几种结构：
    1) 单层列：['Open','High','Low','Close','Adj Close','Volume']
    2) 多层列（ticker, field）：[(symbol,'Open'), ...]
    3) 缺少 Close 但有 Adj Close -> 用 Adj Close 作为 Close
    """
    if df is None or df.empty:
        return df

    # 多层列（常见于 group_by='ticker' 或异常返回）
    if isinstance(df.columns, pd.MultiIndex):
        cols_lower = [c[1].lower() for c in df.columns]
        # 优先尝试 (symbol, 'Close')
        if (symbol, 'Close') in df.columns:
            out = df[(symbol, 'Close')].rename('Close').to_frame()
        elif (symbol, 'Adj Close') in df.columns:
            out = df[(symbol, 'Adj Close')].rename('Close').to_frame()
        else:
            # 退一步：只要某层是 Close/Adj Close 也行（如果只有一个ticker）
            try:
                if 'Close' in [c[1] for c in df.columns]:
                    out = df.xs('Close', axis=1, level=1, drop_level=False)
                    # 若仍多列，尽量选第一列
                    if isinstance(out, pd.DataFrame) and out.shape[1] >= 1:
                        out = out.iloc[:, 0].rename('Close').to_frame()
                else:
                    out = df.xs('Adj Close', axis=1, level=1, drop_level=False)
                    if isinstance(out, pd.DataFrame) and out.shape[1] >= 1:
                        out = out.iloc[:, 0].rename('Close').to_frame()
            except Exception:
                out = pd.DataFrame()
    else:
        # 单层列
        if 'Close' in df.columns:
            out = df[['Close']].copy()
        elif 'Adj Close' in df.columns:
            out = df[['Adj Close']].rename(columns={'Adj Close': 'Close'}).copy()
        else:
            out = pd.DataFrame()

    if out is None or out.empty:
        print(f"[DEBUG] 原始列名：{list(df.columns)}")
        return pd.DataFrame()

    out.index = pd.to_datetime(out.index).tz_localize(None)
    out = out.dropna(subset=['Close']).sort_index()
    return out

def fetch_daily(symbol: str, lookback_days: int = 30) -> pd.DataFrame:
    """
    拉取最近 N 天的日线，返回只含 Close 的 DataFrame（列名为 'Close'）。
    如遇空/异常结构，自动重试更长周期。
    """
    for period in (f"{lookback_days}d", "60d", "3mo"):
        df_raw = yf.download(
            symbol,
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
        )
        df = _normalize_ohlcv(df_raw, symbol)
        if df is not None and not df.empty:
            return df
        print(f"[WARN] 数据结构异常或为空，period={period}，原始列：{list(getattr(df_raw, 'columns', []))}")
    fail("无法获取有效的日线数据（多次重试仍失败）。")

def is_consecutive_bdays(idx: pd.DatetimeIndex) -> bool:
    if len(idx) <= 1:
        return True
    rng = pd.bdate_range(idx[0], idx[-1])
    return len(rng) == len(idx) and all(a == b for a, b in zip(idx, rng))

def sequence_just_turned_true(df: pd.DataFrame, threshold: float, k: int):
    """
    判断最近 k 个交易日收盘都 > threshold 且是连续交易日，
    且前一日不满足（避免重复推送）。
    """
    if len(df) < k:
        return False, df.tail(k)

    last_k = df.tail(k)
    if not is_consecutive_bdays(last_k.index):
        return False, last_k

    if not (last_k["Close"] > threshold).all():
        return False, last_k

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
    print(last_k.tail(CONSECUTIVE_DAYS))

    if not should_alert:
        last_close = df["Close"].iloc[-1]
        last_date = df.index[-1].date()
        print(f"[INFO] 暂不触发。最近收盘（{last_date}）：{fmt_usd(last_close)}；阈值：{fmt_usd(THRESHOLD)}。")
        return

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

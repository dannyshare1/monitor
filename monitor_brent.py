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

FIELDS = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}

def fail(msg: str, code: int = 1):
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(code)

def _to_close_df_singlelevel(df: pd.DataFrame) -> pd.DataFrame:
    """将常见的单层列 DataFrame 规范成仅含 'Close' 列。"""
    if df is None or df.empty:
        return pd.DataFrame()
    cols = set(df.columns.astype(str))
    if "Close" in cols:
        out = df[["Close"]].copy()
    elif "Adj Close" in cols:
        out = df[["Adj Close"]].rename(columns={"Adj Close": "Close"}).copy()
    else:
        print(f"[DEBUG] 单层列中未发现 Close/Adj Close，列：{list(df.columns)}")
        return pd.DataFrame()
    out.index = pd.to_datetime(out.index).tz_localize(None)
    out = out.dropna(subset=["Close"]).sort_index()
    return out

def _to_close_df_multilevel(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    将多层列 DataFrame 规范成仅含 'Close' 列，兼容两种顺序：
    1) (字段, 代码) 例如 ('Close', 'BZ=F')
    2) (代码, 字段) 例如 ('BZ=F', 'Close')
    """
    if df is None or df.empty or not isinstance(df.columns, pd.MultiIndex):
        return pd.DataFrame()

    cols = list(df.columns)
    # 尝试判断是哪种顺序
    is_field_first = all(isinstance(c, tuple) and c[0] in FIELDS for c in cols)
    is_ticker_first = all(isinstance(c, tuple) and c[1] in FIELDS for c in cols)

    out = pd.DataFrame()

    try:
        if is_field_first:
            # 优先 ('Close', symbol)，否则 ('Adj Close', symbol)
            if ("Close", symbol) in df.columns:
                out = df[("Close", symbol)].rename("Close").to_frame()
            elif ("Adj Close", symbol) in df.columns:
                out = df[("Adj Close", symbol)].rename("Close").to_frame()
        elif is_ticker_first:
            if (symbol, "Close") in df.columns:
                out = df[(symbol, "Close")].rename("Close").to_frame()
            elif (symbol, "Adj Close") in df.columns:
                out = df[(symbol, "Adj Close")].rename("Close").to_frame()
        else:
            # 结构混乱时，尝试用 xs 选到 Close/Adj Close 任一，再择一列
            for level in (0, 1):
                try:
                    if "Close" in df.columns.get_level_values(level):
                        tmp = df.xs("Close", axis=1, level=level, drop_level=False)
                        if isinstance(tmp, pd.DataFrame) and tmp.shape[1] >= 1:
                            out = tmp.iloc[:, 0].rename("Close").to_frame()
                            break
                    if "Adj Close" in df.columns.get_level_values(level):
                        tmp = df.xs("Adj Close", axis=1, level=level, drop_level=False)
                        if isinstance(tmp, pd.DataFrame) and tmp.shape[1] >= 1:
                            out = tmp.iloc[:, 0].rename("Close").to_frame()
                            break
                except Exception:
                    pass
    except Exception as e:
        print(f"[DEBUG] 多层列解析异常：{e}")

    if out is None or out.empty:
        print(f"[DEBUG] 多层列未解析成功，样例列：{cols[:6]}")
        return pd.DataFrame()

    out.index = pd.to_datetime(out.index).tz_localize(None)
    out = out.dropna(subset=["Close"]).sort_index()
    return out

def fetch_daily(symbol: str, lookback_days: int = 40) -> pd.DataFrame:
    """
    优先用 Ticker().history()（通常单层列），失败再退回 download() 并兼容多层列两种顺序。
    返回仅含 'Close' 的 DataFrame。
    """
    # 1) Ticker().history()
    for period in (f"{lookback_days}d", "60d", "3mo"):
        try:
            t = yf.Ticker(symbol)
            df_raw = t.history(period=period, interval="1d", auto_adjust=False)
            df = _to_close_df_singlelevel(df_raw)
            if df is not None and not df.empty:
                return df
            print(f"[WARN] history() 空或缺列，period={period}，列：{list(getattr(df_raw, 'columns', []))}")
        except Exception as e:
            print(f"[WARN] history() 异常 period={period}：{e}")

    # 2) download() 兼容多层列
    for period in (f"{lookback_days}d", "60d", "3mo"):
        df_raw = yf.download(
            symbol,
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
        )
        if df_raw is None or df_raw.empty:
            print(f"[WARN] download() 返回空，period={period}")
            continue

        if isinstance(df_raw.columns, pd.MultiIndex):
            df = _to_close_df_multilevel(df_raw, symbol)
        else:
            df = _to_close_df_singlelevel(df_raw)

        if df is not None and not df.empty:
            return df

        print(f"[WARN] download() 结构异常，period={period}，原始列：{list(df_raw.columns)}")

    fail("无法获取有效的日线数据（多次重试仍失败）。")

def is_consecutive_bdays(idx: pd.DatetimeIndex) -> bool:
    if len(idx) <= 1:
        return True
    rng = pd.bdate_range(idx[0], idx[-1])
    return len(rng) == len(idx) and all(a == b for a, b in zip(idx, rng))

def sequence_just_turned_true(df: pd.DataFrame, threshold: float, k: int):
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

    print("[DEBUG] 最近收盘（尾部）：")
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

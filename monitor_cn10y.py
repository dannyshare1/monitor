#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
monitor_cn10y.py — China 10Y Govt Bond Yield (daily, free sources)

Tries multiple free methods in order and uses the first one that succeeds:
  1) AkShare: bond_zh_us_rate() -> '中国国债收益率10年'
  2) AkShare: bond_china_yield() -> row containing '国债收益率:10年' or '10年'
  3) TradingEconomics API (guest or key): several endpoints
  4) Investing.com historical page: table scrape

Keeps original threshold and Telegram push logic.
"""

import os
import sys
from datetime import datetime, date
from typing import Optional, Tuple, List

# Light deps first (requests, pandas are common). AkShare is optional but recommended.
import requests
import pandas as pd

# Try to import akshare lazily; allow script to continue if it's missing.
try:
    import akshare as ak
except Exception:
    ak = None

THRESHOLD = float(os.getenv("THRESHOLD", "1.85"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Optional TradingEconomics key. If not set, we try guest:guest.
TRADINGECONOMICS_KEY = os.getenv("TRADINGECONOMICS_KEY", "").strip()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


class YieldFetchError(Exception):
    pass


def _to_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        # Remove possible percent signs and commas
        s = str(x).replace("%", "").replace(",", "").strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def _to_date(x) -> Optional[date]:
    if x is None:
        return None
    try:
        return pd.to_datetime(x).date()
    except Exception:
        return None


def try_akshare_zh_us() -> Tuple[float, date, str]:
    """AkShare: 中美国债收益率（历史），取最后一行的'中国国债收益率10年'."""
    if ak is None:
        raise YieldFetchError("AkShare 未安装")
    try:
        df = ak.bond_zh_us_rate()
    except Exception as e:
        raise YieldFetchError(f"AkShare bond_zh_us_rate() 失败: {e}")
    if df is None or df.empty:
        raise YieldFetchError("AkShare bond_zh_us_rate() 返回空")

    # 找到日期列
    date_col = None
    for c in df.columns:
        if any(k in str(c) for k in ["日期", "date", "Date"]):
            date_col = c
            break
    if date_col is None:
        # 尝试将索引作为日期
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index": "日期"})
            date_col = "日期"
        else:
            raise YieldFetchError("未找到日期列")

    # 找到中国10Y收益率列
    candidates = [
        c
        for c in df.columns
        if ("中国" in str(c) and "10" in str(c) and ("收益" in str(c) or "利率" in str(c)))
    ]
    if not candidates:
        candidates = [c for c in df.columns if ("中国" in str(c) and "10年" in str(c))]
    if not candidates:
        # 兜底：包含"10"和"收益率"
        candidates = [c for c in df.columns if ("10" in str(c) and ("收益" in str(c) or "利率" in str(c)))]
    if not candidates:
        raise YieldFetchError(f"未找到'中国10年期收益率'列，列名有：{list(df.columns)}")

    ycol = candidates[0]
    df = df.sort_values(by=date_col).dropna(subset=[ycol])
    if df.empty:
        raise YieldFetchError("数据列为空")

    last_row = df.iloc[-1]
    yld = _to_float(last_row[ycol])
    day = _to_date(last_row[date_col])
    if yld is None or day is None:
        raise YieldFetchError(f"解析失败：{last_row.to_dict()}")
    return yld, day, "AkShare: bond_zh_us_rate"


def try_akshare_yield_curve() -> Tuple[float, date, str]:
    """AkShare: 中国国债收益率曲线，找10年期最新值。"""
    if ak is None:
        raise YieldFetchError("AkShare 未安装")
    try:
        df = ak.bond_china_yield()
    except Exception as e:
        raise YieldFetchError(f"AkShare bond_china_yield() 失败: {e}")
    if df is None or df.empty:
        raise YieldFetchError("AkShare bond_china_yield() 返回空")

    # 找到代表期限的列名
    term_cols = [c for c in df.columns if any(k in str(c) for k in ["期限", "名义", "指标", "名称"])]
    if not term_cols:
        term_cols = list(df.columns)
    # 查找含有 "10年" 或 "国债收益率:10年" 的行
    mask = pd.Series([False] * len(df))
    for c in term_cols:
        mask = mask | df[c].astype(str).str.contains("10年", na=False) | df[c].astype(str).str.contains(
            "国债收益率:10年", na=False
        )
    sub = df[mask]
    if sub.empty:
        raise YieldFetchError("未匹配到 10 年期行")

    row = sub.iloc[0]
    # 尝试识别日期列和值列
    date_candidates = [c for c in df.columns if any(k in str(c) for k in ["日期", "date", "最新日期"])]
    day = None
    for c in date_candidates:
        day = _to_date(row.get(c))
        if day is not None:
            break
    if day is None:
        day = date.today()

    # 可能的利率列名
    value_candidates = [c for c in df.columns if any(k in str(c) for k in ["最新值", "收益率", "利率", "value", "close"])]
    yld = None
    for c in value_candidates:
        yld = _to_float(row.get(c))
        if yld is not None:
            break
    if yld is None:
        # 兜底：在所有列里找第一个能转成float的值
        for c in df.columns:
            yld = _to_float(row.get(c))
            if yld is not None:
                break
    if yld is None:
        raise YieldFetchError(f"无法解析 10 年期收益率：{row.to_dict()}")

    return yld, day, "AkShare: bond_china_yield"


def try_tradingeconomics() -> Tuple[float, date, str]:
    """TradingEconomics API: 尝试多个端点。优先使用 KEY，否则 guest:guest。"""
    base_cred = TRADINGECONOMICS_KEY if TRADINGECONOMICS_KEY else "guest:guest"
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    endpoints: List[str] = [
        # 1) Markets bond last value (china:10y)
        f"https://api.tradingeconomics.com/markets/bond/china:10y?c={base_cred}&format=json",
        # 2) Historical by indicator & country (Government Bond 10Y)
        f"https://api.tradingeconomics.com/historical/country/China?indicator=Government%20Bond%2010Y&c={base_cred}&format=json",
        # 3) Historical by symbol (china:10y) last N days
        f"https://api.tradingeconomics.com/historical/markets/bond/china:10y?c={base_cred}&format=json",
    ]
    last_exc = None
    for url in endpoints:
        try:
            r = session.get(url, timeout=20)
            if r.status_code != 200:
                last_exc = YieldFetchError(f"HTTP {r.status_code} {r.text[:120]}")
                continue
            data = r.json()
            if not data:
                last_exc = YieldFetchError("空JSON")
                continue

            # Case A: markets/bond returns a dict or list with fields like 'Close', 'Date'
            if isinstance(data, dict):
                data = [data]
            # Try parse the most recent record
            # Sort by date if field exists
            def parse_one(rec):
                # Try common keys
                for dc in ("Date", "date", "Datetime", "timestamp"):
                    d = _to_date(rec.get(dc))
                    if d:
                        break
                else:
                    d = date.today()
                for kc in ("Close", "close", "Value", "value", "Price", "price", "LatestValue"):
                    v = _to_float(rec.get(kc))
                    if v is not None:
                        return v, d
                return None

            # If historical list sort by date
            best = None
            # Try to sort by any date-like key
            def get_dt(rec):
                for dc in ("Date", "date", "Datetime", "timestamp"):
                    try:
                        return pd.to_datetime(rec.get(dc))
                    except Exception:
                        pass
                return pd.NaT
            try:
                data_sorted = sorted(data, key=get_dt)
            except Exception:
                data_sorted = data

            for rec in reversed(data_sorted):
                out = parse_one(rec)
                if out:
                    yld, day = out
                    # TradingEconomics yields are typically in percent already
                    return yld, day, f"TradingEconomics: {url.split('/api.tradingeconomics.com/')[-1].split('?')[0]}"
            last_exc = YieldFetchError("无法解析字段")
        except Exception as e:
            last_exc = e
            continue
    raise YieldFetchError(f"TradingEconomics 失败：{last_exc}")


def try_investing_com() -> Tuple[float, date, str]:
    """Investing.com 历史数据页抓取（中文站）。"""
    url = "https://cn.investing.com/rates-bonds/china-10-year-bond-yield-historical-data"
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.status_code != 200:
            raise YieldFetchError(f"HTTP {r.status_code}")
        # 直接用 pandas 解析所有表格，选包含日期和收盘列的最大表
        tables = pd.read_html(r.text)
        if not tables:
            raise YieldFetchError("页面没有表格")
        # 选择包含“日期”且包含“收盘”或“收盘价”列的表
        cand = []
        for t in tables:
            cols = [str(c) for c in t.columns]
            if any("日期" in c for c in cols) and any(("收盘" in c) or ("收盘价" in c) for c in cols):
                cand.append(t)
        if not cand:
            # 退一步，选行列最多的一张
            cand = [max(tables, key=lambda x: x.shape[0] * x.shape[1])]

        df = cand[0].copy()
        # 标准化列名
        df.columns = [str(c).strip() for c in df.columns]
        # 定位日期列和收盘列
        date_col = next((c for c in df.columns if "日期" in c), df.columns[0])
        close_col = next((c for c in df.columns if ("收盘" in c) or ("收盘价" in c)), df.columns[-1])

        # 去除无效行
        df = df.dropna(subset=[date_col, close_col])
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col]).sort_values(by=date_col)
        last = df.iloc[-1]
        day = last[date_col].date()
        yld = _to_float(last[close_col])
        if yld is None:
            raise YieldFetchError(f"无法解析收盘列：{close_col}")
        return yld, day, "Investing.com 抓取"
    except ValueError as ve:
        # read_html 失败时的常见异常
        raise YieldFetchError(f"解析表格失败：{ve}")
    except Exception as e:
        raise YieldFetchError(f"Investing 请求失败：{e}")


def fetch_yield_multi() -> Tuple[float, date, str]:
    """Try all methods in order; return the first success."""
    methods = [
        try_akshare_zh_us,
        try_akshare_yield_curve,
        try_tradingeconomics,
        try_investing_com,
    ]
    errors = []
    for func in methods:
        name = func.__name__
        try:
            yld, day, source = func()
            print(f"[OK] {source} 成功：{day} -> {yld:.4f}%")
            return yld, day, source
        except Exception as e:
            err = f"{name} 失败：{e}"
            errors.append(err)
            print("[WARN]", err, file=sys.stderr)
            continue
    raise YieldFetchError("全部方法失败：\n" + "\n".join(errors))


def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        # 不再抛错，以便本地测试无推送也能成功
        print("[INFO] 未配置 Telegram，跳过推送。")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code != 200:
            print(f"[WARN] Telegram 推送失败：HTTP {r.status_code} - {r.text}", file=sys.stderr)
        else:
            print("[OK] 已推送至 Telegram。")
    except Exception as e:
        print(f"[WARN] Telegram 推送异常：{e}", file=sys.stderr)


def main() -> None:
    print("[INFO] 获取中国10年期国债收盘收益率（多源尝试）……")
    yld, day, source = fetch_yield_multi()
    print(f"[INFO] 最近收盘（{day}）：{yld:.3f}%；阈值：{THRESHOLD:.2f}% | 来源：{source}")

    if yld >= THRESHOLD:
        msg = (
            "🇨🇳 *China 10Y Government Bond*\n"
            f"{day}: 收盘利率 {yld:.3f}% ≥ 阈值 {THRESHOLD:.2f}%\n"
            f"来源：{source}"
        )
        send_telegram(msg)
    else:
        print("[INFO] 未达阈值，暂不推送。")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Error:", e, file=sys.stderr)
        sys.exit(2)


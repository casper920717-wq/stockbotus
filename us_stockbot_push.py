#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
us_stockbot_push_signals.py
- 美股 MA10/MA20「跨日突破」訊號推播（僅文字訊號，不顯示價格）
- 條件：
  1) 昨收 < MA20_昨、今價 > MA20_今 → 「向上突破，買進」
  2) 昨收 > MA20_昨、今價 < MA20_今 → 「向下突破，賣出」
  3) 昨收 < MA10_昨、今價 > MA10_今 → 「向上突破，買進」
  4) 昨收 > MA10_昨、今價 < MA10_今 → 「向下突破，賣出」
- 僅使用 yfinance；台灣時間 21:30–01:00 內執行；LINE Notify 推播
- 訊息只顯示代號與信號，不顯示任何價格與均線值
"""

import os
import sys
import time
from datetime import datetime, time as dtime

import pytz
import requests
import yfinance as yf
import pandas as pd

# === 使用者設定 ===
CODES = ["MSFT","NVDA","SATS","TTD","HIMS","PLTR","AVGO","MP","NB","LAC",
         "MU","GOOG","TSM","VRT","UUUU","HOND","ALAB","AMPX","FLNC","EOSE",
         "EME","NEE"]

TZ_TAIPEI = pytz.timezone("Asia/Taipei")
MARKET_START = dtime(21, 30, 0)   # 台灣時間
MARKET_END   = dtime(3, 0, 0)     # 跨日結束
ALLOW_OUTSIDE_WINDOW = False

LINE_NOTIFY_TOKEN = os.getenv("LINE_NOTIFY_TOKEN", "").strip()
LINE_NOTIFY_API = "https://notify-api.line.me/api/notify"

BATCH_SIZE = 20

# 狀態檔：避免同一天、同一檔、同一訊號重複推播
STATE_FILE = os.getenv("STATE_FILE", "us_stock_signals_state.json")


# === 小工具 ===
def now_taipei():
    return datetime.now(TZ_TAIPEI)


def within_session(now=None):
    if now is None:
        now = now_taipei()
    t = now.time()
    if MARKET_END < MARKET_START:
        return (t >= MARKET_START) or (t <= MARKET_END)
    return MARKET_START <= t <= MARKET_END

# 👇 在這裡插入美股開盤判斷函式
TZ_NY = pytz.timezone("America/New_York")
MARKET_START_US = dtime(9, 30, 0)
MARKET_END_US   = dtime(16, 0, 0)

def within_us_session(now: datetime | None = None) -> bool:
    """判斷是否在美股開盤時間（紐約時間 9:30–16:00）"""
    if now is None:
        now = datetime.now(TZ_NY)
    t = now.time()
    return MARKET_START_US <= t <= MARKET_END_US


# === LINE Messaging API 設定 ===
LINE_CHANNEL_TOKEN = os.getenv("LINE_CHANNEL_TOKEN", "").strip()
LINE_TO = os.getenv("LINE_TO", "").strip()

def send_line(message: str) -> bool:
    """用 Messaging API 推播訊息"""
    if not LINE_CHANNEL_TOKEN or not LINE_TO:
        print("[WARN] LINE Messaging API 環境變數未設定，略過推播。")
        return False

    try:
        resp = requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LINE_CHANNEL_TOKEN}",
            },
            json={
                "to": LINE_TO,
                "messages": [{"type": "text", "text": message}],
            },
            timeout=15,
        )
        if resp.status_code == 200:
            print("[INFO] LINE 推播成功。")
            return True
        else:
            print(f"[ERROR] LINE 推播失敗 {resp.status_code}: {resp.text}")
            return False
    except Exception as e:
        print(f"[ERROR] LINE 推播錯誤: {e}")
        return False

def get_latest_and_prevclose(ticker: str):
    tkr = yf.Ticker(ticker)
    latest = None
    prev_close = None

    # 快速資訊
    try:
        fi = tkr.fast_info
        latest = getattr(fi, "last_price", None)
        prev_close = getattr(fi, "previous_close", None)
    except Exception:
        pass

    # 補救：history()
    if latest is None or pd.isna(latest):
        try:
            h1m = tkr.history(period="5d", interval="1m")
            if not h1m.empty:
                latest = float(h1m["Close"].dropna().iloc[-1])
        except Exception:
            pass

    if prev_close is None or pd.isna(prev_close):
        try:
            h1d = tkr.history(period="5d", interval="1d")
            c = h1d["Close"].dropna()
            if len(c) >= 2:
                prev_close = float(c.iloc[-2])
            elif len(c) >= 1:
                prev_close = float(c.iloc[-1])
        except Exception:
            pass

    return latest, prev_close


def get_ma_yday_today(ticker: str, window: int, latest_price: float):
    """
    回傳 (MA_yday, MA_today)，
    - MA_yday：用日線收盤，截止到昨收（不含今日）取 window 天平均
    - MA_today：以「最後 window-1 天的收盤 + 今日最新價」組合計算
    若資料不足回傳 (None, None)
    """
    try:
        tkr = yf.Ticker(ticker)
        hist = tkr.history(period="120d", interval="1d")  # 充足緩衝
        closes = hist["Close"].dropna()
        if len(closes) < window:
            return None, None
        # 昨日 MA：最後 window 天（不含今日），對應昨日收盤
        ma_yday = float(closes.tail(window).mean())
        # 今日 MA：若無最新價，則無法計算
        if latest_price is None or pd.isna(latest_price):
            return ma_yday, None
        # 今日 MA 使用 window-1 個最近的日收盤（包含昨收），加上今日最新價
        tail_n_1 = closes.tail(window - 1)
        if len(tail_n_1) < window - 1:
            return ma_yday, None
        ma_today = float((tail_n_1.sum() + float(latest_price)) / window)
        return ma_yday, ma_today
    except Exception:
        return None, None


# === 狀態 ===
def load_state():
    try:
        import json, os
        if not os.path.exists(STATE_FILE):
            return {}
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict):
    try:
        import json, os
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        print("[WARN] 無法寫入狀態：", e)


def build_signals_for(code: str, today_date_str: str):
    latest, prev_close = get_latest_and_prevclose(code)

    # MA20
    ma20_y, ma20_t = get_ma_yday_today(code, 20, latest)
    # MA10
    ma10_y, ma10_t = get_ma_yday_today(code, 10, latest)

    signals = []

    # 只有在四個量都可用時才判斷（避免假訊號）
    if (prev_close is not None and not pd.isna(prev_close)) and \
       (ma20_y is not None and not pd.isna(ma20_y)) and \
       (ma20_t is not None and not pd.isna(ma20_t)) and \
       (latest is not None and not pd.isna(latest)):
        if (prev_close < ma20_y) and (latest > ma20_t):
            signals.append(("MA20", "UP"))
        elif (prev_close > ma20_y) and (latest < ma20_t):
            signals.append(("MA20", "DOWN"))

    if (prev_close is not None and not pd.isna(prev_close)) and \
       (ma10_y is not None and not pd.isna(ma10_y)) and \
       (ma10_t is not None and not pd.isna(ma10_t)) and \
       (latest is not None and not pd.isna(latest)):
        if (prev_close < ma10_y) and (latest > ma10_t):
            signals.append(("MA10", "UP"))
        elif (prev_close > ma10_y) and (latest < ma10_t):
            signals.append(("MA10", "DOWN"))

    # 組訊息（不含價格）
    out_lines = []
    for ma, direction in signals:
        if direction == "UP":
            out_lines.append(f"{code}｜{ma} 向上突破，買進")
        else:
            out_lines.append(f"{code}｜{ma} 向下突破，賣出")

    return out_lines


def de_dupe_signals(state: dict, today: str, code: str, lines: list):
    """
    避免同一日、同一檔、同一訊號重複通知。
    state 結構：{ code: { 'date': 'YYYY-MM-DD', 'sent': { 'MA10_UP': True, ... } } }
    """
    if not lines:
        return []

    node = state.get(code, {})
    last_date = node.get("date")
    sent = node.get("sent", {}) if isinstance(node.get("sent"), dict) else {}

    fresh = []
    for line in lines:
        key = None
        if "MA10 向上突破" in line:
            key = "MA10_UP"
        elif "MA10 向下突破" in line:
            key = "MA10_DOWN"
        elif "MA20 向上突破" in line:
            key = "MA20_UP"
        elif "MA20 向下突破" in line:
            key = "MA20_DOWN"

        if key is None:
            continue

        # 如果是不同日期，清空既有 sent
        if last_date != today:
            sent = {}

        if not sent.get(key, False):
            fresh.append(line)
            sent[key] = True

    # 回寫
    if fresh:
        state[code] = {"date": today, "sent": sent}
    elif last_date != today:
        # 明確寫入今日（就算沒觸發）避免後續邏輯不一致
        state[code] = {"date": today, "sent": sent}

    return fresh


def format_message(all_lines: list, run_dt=None):
    if not all_lines:
        return []

    if run_dt is None:
        run_dt = now_taipei()
    ts_str = run_dt.strftime("%Y-%m-%d %H:%M:%S %Z")

    header = f"【MA10/MA20 跨日突破訊號】{ts_str}"
    lines = [header] + all_lines

    msg = "\n".join(lines)
    if len(msg) <= 900:
        return [msg]

    # 避免 LINE 長度上限，切段
    out = []
    buf = header
    for line in all_lines:
        if len(buf) + 1 + len(line) > 900:
            out.append(buf)
            buf = header + "\n" + line
        else:
            buf = buf + "\n" + line
    if buf:
        out.append(buf)
    return out


def main():
    now = now_taipei()
    if not within_session(now) and not ALLOW_OUTSIDE_WINDOW:
        print("[INFO] 現在不在交易時段（台灣時間 21:30–01:00），結束。")
        return

    state = load_state()
    today = now.strftime("%Y-%m-%d")

    all_signal_lines = []
    codes = list(CODES)
    for i in range(0, len(codes), BATCH_SIZE):
        batch = codes[i:i+BATCH_SIZE]
        for code in batch:
            lines = build_signals_for(code, today)
            # 去重：避免同日同訊號重複
            lines = de_dupe_signals(state, today, code, lines)
            all_signal_lines.extend(lines)
            time.sleep(0.2)

    # 沒有任何訊號就不推播
    messages = format_message(all_signal_lines, run_dt=now)
    for m in messages:
        print(m)
        send_line(m)
        time.sleep(1.0)

    save_state(state)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[FATAL]", e)
        sys.exit(1)

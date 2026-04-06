from pybit.unified_trading import HTTP
from dotenv import load_dotenv
import os
import time
import math
import traceback
from datetime import datetime
import pandas as pd
import requests

# =========================================================
# 환경
# =========================================================
load_dotenv()

DEMO_MODE = True
CATEGORY = "linear"

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT"
]

HIGHER_INTERVAL = "60"
ENTRY_INTERVAL = "5"

# =========================================================
# 전략 파라미터 (튜닝 핵심)
# =========================================================
ADX_MIN = 16
EMA_SPREAD_ATR_MIN = 0.06

ATR_PERIOD = 14
ADX_PERIOD = 14

EMA_FAST = 9
EMA_SLOW = 21

HTF_EMA_FAST = 50
HTF_EMA_SLOW = 200

RISK_PER_TRADE = 0.01

# 익절 구조
PARTIAL_R = 1.3
TRAILING_R = 1.8

# =========================================================
# 텔레그램
# =========================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram(msg):
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
    except:
        pass

# =========================================================
# API
# =========================================================
session = HTTP(
    testnet=DEMO_MODE,
    api_key=os.getenv("BYBIT_API_KEY"),
    api_secret=os.getenv("BYBIT_API_SECRET"),
)

# =========================================================
# 데이터
# =========================================================
def get_kline(symbol, interval, limit=200):
    data = session.get_kline(
        category=CATEGORY,
        symbol=symbol,
        interval=interval,
        limit=limit
    )["result"]["list"]

    df = pd.DataFrame(data, columns=[
        "time","open","high","low","close","volume","turnover"
    ])

    df = df.astype(float)
    df = df[::-1]
    return df

# =========================================================
# 지표
# =========================================================
def add_indicators(df):
    df["ema_fast"] = df["close"].ewm(span=EMA_FAST).mean()
    df["ema_slow"] = df["close"].ewm(span=EMA_SLOW).mean()

    df["tr"] = df["high"] - df["low"]
    df["atr"] = df["tr"].rolling(ATR_PERIOD).mean()
    df["adx"] = df["tr"].rolling(ADX_PERIOD).mean()

    return df

# =========================================================
# 추세
# =========================================================
def get_trend(df):
    ema_fast = df["close"].ewm(span=HTF_EMA_FAST).mean()
    ema_slow = df["close"].ewm(span=HTF_EMA_SLOW).mean()

    if ema_fast.iloc[-1] > ema_slow.iloc[-1]:
        return "LONG"
    elif ema_fast.iloc[-1] < ema_slow.iloc[-1]:
        return "SHORT"
    return "NONE"

# =========================================================
# 상태
# =========================================================
positions = {}

# =========================================================
# 주문
# =========================================================
def place_order(symbol, side, qty):
    session.place_order(
        category=CATEGORY,
        symbol=symbol,
        side=side,
        orderType="Market",
        qty=qty
    )

# =========================================================
# 메인
# =========================================================
def run():
    send_telegram("봇 시작됨")

    while True:
        try:
            for symbol in SYMBOLS:

                # 이미 포지션 있으면 관리
                if symbol in positions:
                    manage_position(symbol)
                    continue

                # 추세
                htf = get_kline(symbol, HIGHER_INTERVAL)
                trend = get_trend(htf)

                if trend == "NONE":
                    continue

                df = get_kline(symbol, ENTRY_INTERVAL)
                df = add_indicators(df)

                last = df.iloc[-1]

                atr = last["atr"]
                if atr == 0 or math.isnan(atr):
                    continue

                ema_spread = abs(last["ema_fast"] - last["ema_slow"])

                # 진입 조건
                if last["adx"] < ADX_MIN:
                    continue

                if ema_spread < atr * EMA_SPREAD_ATR_MIN:
                    continue

                # 진입
                if trend == "LONG" and last["ema_fast"] > last["ema_slow"]:
                    enter(symbol, "Buy", last["close"], atr)

                elif trend == "SHORT" and last["ema_fast"] < last["ema_slow"]:
                    enter(symbol, "Sell", last["close"], atr)

            time.sleep(20)

        except Exception as e:
            send_telegram(f"에러: {e}")
            traceback.print_exc()
            time.sleep(10)

# =========================================================
# 진입
# =========================================================
def enter(symbol, side, price, atr):

    qty = 0.01

    stop = price - atr if side == "Buy" else price + atr

    positions[symbol] = {
        "side": side,
        "entry": price,
        "stop": stop,
        "atr": atr,
        "partial": False,
        "trail": False,
        "qty": qty
    }

    place_order(symbol, side, qty)
    send_telegram(f"{symbol} 진입 {side}")

# =========================================================
# 포지션 관리
# =========================================================
def manage_position(symbol):

    pos = positions[symbol]

    ticker = session.get_tickers(category=CATEGORY, symbol=symbol)
    price = float(ticker["result"]["list"][0]["lastPrice"])

    entry = pos["entry"]
    atr = pos["atr"]
    side = pos["side"]

    # 수익 R 계산
    if side == "Buy":
        r = (price - entry) / atr
    else:
        r = (entry - price) / atr

    # 손절
    if side == "Buy" and price <= pos["stop"]:
        exit_all(symbol)
        return

    if side == "Sell" and price >= pos["stop"]:
        exit_all(symbol)
        return

    # 부분익절
    if not pos["partial"] and r >= PARTIAL_R:
        close_partial(symbol, pos["qty"] * 0.5)
        pos["partial"] = True
        pos["stop"] = entry
        send_telegram(f"{symbol} 부분익절")

    # 트레일링
    if r >= TRAILING_R:
        pos["trail"] = True

    if pos["trail"]:
        if side == "Buy":
            pos["stop"] = max(pos["stop"], price - atr * 0.8)
        else:
            pos["stop"] = min(pos["stop"], price + atr * 0.8)

# =========================================================
# 청산
# =========================================================
def exit_all(symbol):
    pos = positions[symbol]
    side = "Sell" if pos["side"] == "Buy" else "Buy"

    place_order(symbol, side, pos["qty"])
    send_telegram(f"{symbol} 전체청산")

    del positions[symbol]

def close_partial(symbol, qty):
    pos = positions[symbol]
    side = "Sell" if pos["side"] == "Buy" else "Buy"

    place_order(symbol, side, qty)

# =========================================================
# 실행
# =========================================================
if __name__ == "__main__":
    run()
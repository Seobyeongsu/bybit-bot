from pybit.unified_trading import HTTP
from dotenv import load_dotenv
import os
import time
import math
import traceback
import requests
from datetime import datetime
import pandas as pd

# =========================
# 환경 설정
# =========================
load_dotenv()

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

session = HTTP(
    api_key=API_KEY,
    api_secret=API_SECRET,
    testnet=True
)

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT",
    "SOLUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT"
]

# =========================
# 전략 파라미터 (중간버전)
# =========================
ADX_MIN = 18
RISK_PER_TRADE = 0.01

# =========================
# 텔레그램
# =========================
def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}
        )
    except:
        pass

# =========================
# 데이터 가져오기
# =========================
def get_kline(symbol, interval, limit=200):
    try:
        res = session.get_kline(
            category="linear",
            symbol=symbol,
            interval=interval,
            limit=limit
        )
        df = pd.DataFrame(res["result"]["list"])
        df.columns = ["time","open","high","low","close","volume","turnover"]
        df = df.astype(float)
        return df[::-1]
    except:
        return None

# =========================
# 지표 계산
# =========================
def add_indicators(df):
    df["ema9"] = df["close"].ewm(span=9).mean()
    df["ema21"] = df["close"].ewm(span=21).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()
    df["ema200"] = df["close"].ewm(span=200).mean()

    df["tr"] = df["high"] - df["low"]
    df["atr"] = df["tr"].rolling(14).mean()

    df["adx"] = df["tr"].rolling(14).mean()  # 간단 버전

    return df

# =========================
# 진입 신호 (A 전략)
# =========================
def get_entry_signal(df):
    try:
        row = df.iloc[-1]

        # 추세 필터
        if not (row["ema50"] > row["ema200"]):
            return None

        # ADX 필터
        if row["adx"] < ADX_MIN:
            return None

        # 눌림 + 돌파
        if row["close"] > row["ema9"] and row["close"] > row["ema21"]:
            return "LONG"

        return None
    except:
        return None

# =========================
# 주문
# =========================
def place_order(symbol, side):
    try:
        balance = 100  # 테스트용

        price = float(session.get_tickers(category="linear", symbol=symbol)["result"]["list"][0]["lastPrice"])
        qty = round((balance * RISK_PER_TRADE) / price, 3)

        session.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty
        )

        send_telegram(f"{symbol} 진입 {side}")

    except Exception as e:
        send_telegram(f"주문 에러: {e}")

# =========================
# 메인 루프
# =========================
def main():
    send_telegram("봇 시작")

    while True:
        try:
            for symbol in SYMBOLS:
                df = get_kline(symbol, "15")

                if df is None or len(df) < 50:
                    continue

                df = add_indicators(df)

                signal = get_entry_signal(df)

                if signal:
                    place_order(symbol, signal)

            time.sleep(60)

        except Exception as e:
            send_telegram(f"에러 발생: {traceback.format_exc()}")
            time.sleep(10)

if __name__ == "__main__":
    main()
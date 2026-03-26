from pybit.unified_trading import HTTP
from dotenv import load_dotenv
import os
import time
import math
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd


# =========================================================
# 환경 설정
# =========================================================
load_dotenv()

DEMO_MODE = True
CATEGORY = "linear"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "DOGEUSDT", "SOLUSDT"]

# 타임프레임
HIGHER_INTERVAL = "60"   # 1시간
ENTRY_INTERVAL = "15"    # 15분

# 상위 TF 지표
HTF_EMA_FAST = 50
HTF_EMA_SLOW = 200

# 진입 TF 지표
ETF_EMA_FAST = 9
ETF_EMA_SLOW = 21
ADX_PERIOD = 14
ATR_PERIOD = 14

# 진입 조건
ADX_MIN = 18
ADX_STRONG = 25
B_PULLBACK_VALID_BARS = 5

# 리스크
RISK_PER_TRADE = 0.01
STOP_ATR_MULTIPLIER = 1.5

# 부분익절
PARTIAL_TP_R_MULTIPLIER = 1.5     # 1:1.5
PARTIAL_CLOSE_RATIO = 0.5         # 50%

# 트레일링
TRAIL_ACTIVATE_ATR = 1.5
TRAIL_ATR_MULTIPLIER = 1.2

# 노출 제한
MAX_POSITION_RATIO = 0.40
MAX_TOTAL_EXPOSURE = 0.50

# 재진입 제한
REENTRY_BLOCK_BARS = 3
STRONG_TREND_REENTRY_BARS = 2

# 루프
LOOP_SLEEP_SEC = 30

# 로그 파일
BASE_DIR = Path(__file__).resolve().parent
TRADE_LOG = BASE_DIR / "trade_log.csv"
STATUS_LOG = BASE_DIR / "status_log.csv"
BALANCE_LOG = BASE_DIR / "balance_log.csv"
ERROR_LOG = BASE_DIR / "error_log.csv"

# API 세션
session = HTTP(
    demo=DEMO_MODE,
    api_key=os.getenv("BYBIT_API_KEY"),
    api_secret=os.getenv("BYBIT_API_SECRET")
)


# =========================================================
# 런타임 상태
# =========================================================
state = {
    symbol: {
        # 포지션 상태
        "position_side": "NONE",      # NONE / LONG / SHORT
        "position_qty": 0.0,
        "position_entry_price": 0.0,

        # 진입/청산 시각
        "last_entry_time": None,
        "last_exit_time": None,
        "last_entry_bar_time": None,
        "last_exit_bar_time": None,

        # 손실 제어
        "loss_streak": {"LONG": 0, "SHORT": 0},
        "skip_next_entry": {"LONG": 0, "SHORT": 0},

        # 트레일링/손절
        "highest_price": None,
        "lowest_price": None,
        "entry_stop_price": None,
        "initial_stop_distance": None,
        "trail_active": False,
        "trail_price": None,

        # 부분익절
        "partial_tp_price": None,
        "partial_exit_done": False,

        # 최근 진입 정보
        "recent_entry_type": "",      # A / B
        "recent_exit_reason": "",
        "entry_adx": None,
        "entry_atr": None,

        # B급 눌림 상태
        "pullback_active": False,
        "pullback_direction": "",     # LONG / SHORT
        "pullback_ref_high": None,
        "pullback_ref_low": None,
        "pullback_start_bar": None,
        "b_used_in_trend": False,

        # 상위 추세 추적
        "last_higher_trend": "NONE",
    }
    for symbol in SYMBOLS
}


# =========================================================
# 공용 유틸
# =========================================================
def now_kst_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_float(value, default=0.0):
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def interval_to_minutes(interval: str) -> int:
    mapping = {
        "1": 1, "3": 3, "5": 5, "15": 15, "30": 30,
        "60": 60, "120": 120, "240": 240, "D": 1440
    }
    return mapping.get(str(interval), 15)


def bars_diff(start_bar, end_bar, interval_minutes: int) -> int:
    if start_bar is None or end_bar is None:
        return 9999
    diff_minutes = (end_bar - start_bar).total_seconds() / 60
    return int(diff_minutes // interval_minutes)


def append_csv_row(file_path: Path, row: dict):
    df = pd.DataFrame([row])
    header = not file_path.exists()
    df.to_csv(file_path, mode="a", header=header, index=False, encoding="utf-8-sig")


def log_error(symbol: str, func_name: str, error_text: str, order_failed=False, api_message=""):
    row = {
        "time": now_kst_str(),
        "symbol": symbol,
        "function": func_name,
        "error": error_text,
        "order_failed": order_failed,
        "api_message": api_message,
    }
    append_csv_row(ERROR_LOG, row)


def log_status(symbol: str, row_data: dict):
    row = {"time": now_kst_str(), "symbol": symbol, **row_data}
    append_csv_row(STATUS_LOG, row)


def log_balance(balance_info: dict):
    row = {
        "time": now_kst_str(),
        "total_balance": balance_info["total_balance"],
        "available_balance": balance_info["available_balance"],
        "total_equity": balance_info["total_equity"],
        "unrealised_pnl": balance_info["unrealised_pnl"],
        "cum_realised_pnl": balance_info["cum_realised_pnl"],
    }
    append_csv_row(BALANCE_LOG, row)


def log_trade(
    symbol: str,
    position_direction: str,
    action: str,               # ENTRY / EXIT / PARTIAL_EXIT
    entry_type: str,
    entry_price: float,
    exit_price: float,
    qty: float,
    pnl_usdt: float,
    pnl_pct: float,
    exit_reason: str,
    entry_adx: float,
    entry_atr: float,
):
    row = {
        "time": now_kst_str(),
        "symbol": symbol,
        "position_direction": position_direction,
        "action": action,
        "entry_type": entry_type,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "qty": qty,
        "pnl_usdt": pnl_usdt,
        "pnl_pct": pnl_pct,
        "exit_reason": exit_reason,
        "entry_adx": entry_adx,
        "entry_atr": entry_atr,
    }
    append_csv_row(TRADE_LOG, row)


def decimals_from_step(step: float) -> int:
    s = f"{step:.18f}".rstrip("0")
    if "." in s:
        return len(s.split(".")[1])
    return 0


def normalize_qty(qty: float, step: float, min_qty: float) -> float:
    if qty <= 0 or step <= 0:
        return 0.0

    decimals = decimals_from_step(step)
    qty = math.floor(qty / step) * step
    qty = round(qty, decimals)

    if qty < min_qty:
        return 0.0

    return qty


def reset_position_state(symbol: str):
    state[symbol]["position_side"] = "NONE"
    state[symbol]["position_qty"] = 0.0
    state[symbol]["position_entry_price"] = 0.0

    state[symbol]["highest_price"] = None
    state[symbol]["lowest_price"] = None
    state[symbol]["entry_stop_price"] = None
    state[symbol]["initial_stop_distance"] = None
    state[symbol]["trail_active"] = False
    state[symbol]["trail_price"] = None

    state[symbol]["partial_tp_price"] = None
    state[symbol]["partial_exit_done"] = False

    state[symbol]["recent_entry_type"] = ""
    state[symbol]["entry_adx"] = None
    state[symbol]["entry_atr"] = None


def reset_pullback_state(symbol: str):
    state[symbol]["pullback_active"] = False
    state[symbol]["pullback_direction"] = ""
    state[symbol]["pullback_ref_high"] = None
    state[symbol]["pullback_ref_low"] = None
    state[symbol]["pullback_start_bar"] = None


# =========================================================
# Bybit API 헬퍼
# =========================================================
def get_wallet_info():
    result = session.get_wallet_balance(accountType="UNIFIED")
    data = result["result"]["list"][0]

    total_equity = safe_float(data.get("totalEquity"))
    available_balance = safe_float(data.get("totalAvailableBalance"))
    total_wallet_balance = safe_float(data.get("totalWalletBalance"))
    total_perp_upl = safe_float(data.get("totalPerpUPL"))

    cum_realised_pnl = 0.0
    for coin_info in data.get("coin", []):
        cum_realised_pnl += safe_float(coin_info.get("cumRealisedPnl"))

    return {
        "total_balance": total_wallet_balance,
        "available_balance": available_balance,
        "total_equity": total_equity,
        "unrealised_pnl": total_perp_upl,
        "cum_realised_pnl": cum_realised_pnl,
    }


def get_ticker_price(symbol: str) -> float:
    result = session.get_tickers(category=CATEGORY, symbol=symbol)
    return safe_float(result["result"]["list"][0]["lastPrice"])


def get_instrument_info(symbol: str):
    result = session.get_instruments_info(category=CATEGORY, symbol=symbol)
    info = result["result"]["list"][0]

    qty_step = safe_float(info["lotSizeFilter"]["qtyStep"])
    min_order_qty = safe_float(info["lotSizeFilter"]["minOrderQty"])

    return {
        "qty_step": qty_step,
        "min_order_qty": min_order_qty,
    }


def get_position_from_exchange(symbol: str):
    result = session.get_positions(category=CATEGORY, symbol=symbol)
    pos_list = result["result"]["list"]

    if not pos_list:
        return None

    pos = pos_list[0]
    size = safe_float(pos.get("size"))
    if size == 0:
        return None

    side = pos.get("side")
    avg_price = safe_float(pos.get("avgPrice"))

    return {
        "side": side,   # Buy / Sell
        "size": size,
        "avg_price": avg_price,
    }


def set_leverage(symbol: str, leverage: str = "3"):
    try:
        session.set_leverage(
            category=CATEGORY,
            symbol=symbol,
            buyLeverage=leverage,
            sellLeverage=leverage
        )
        print(f"[{now_kst_str()}] {symbol} leverage set: {leverage}")
    except Exception as e:
        print(f"[{now_kst_str()}] {symbol} leverage skip: {e}")


def place_market_entry(symbol: str, side: str, qty: float):
    return session.place_order(
        category=CATEGORY,
        symbol=symbol,
        side=side,
        orderType="Market",
        qty=str(qty),
        timeInForce="IOC"
    )


def place_market_close(symbol: str, close_side: str, qty: float):
    return session.place_order(
        category=CATEGORY,
        symbol=symbol,
        side=close_side,
        orderType="Market",
        qty=str(qty),
        reduceOnly=True,
        timeInForce="IOC"
    )


# =========================================================
# Kline / 지표
# =========================================================
def get_klines(symbol: str, interval: str, limit: int = 400) -> pd.DataFrame:
    result = session.get_kline(
        category=CATEGORY,
        symbol=symbol,
        interval=interval,
        limit=limit
    )

    rows = result["result"]["list"]
    df = pd.DataFrame(rows, columns=[
        "startTime", "open", "high", "low", "close", "volume", "turnover"
    ])

    df = df.sort_values("startTime").reset_index(drop=True)

    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        df[col] = df[col].astype(float)

    df["startTime"] = pd.to_datetime(df["startTime"].astype("int64"), unit="ms")
    return df


def add_ema(df: pd.DataFrame, period: int, col_name: str):
    df[col_name] = df["close"].ewm(span=period, adjust=False).mean()


def add_atr(df: pd.DataFrame, period: int = 14):
    prev_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(period).mean()


def add_adx(df: pd.DataFrame, period: int = 14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    up_move = high.diff()
    down_move = low.shift(1) - low

    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)) * 100

    df["adx"] = dx.rolling(period).mean()


# =========================================================
# 상위 TF / 진입 TF
# =========================================================
def get_higher_tf_trend(symbol: str):
    df = get_klines(symbol, HIGHER_INTERVAL, 350)
    add_ema(df, HTF_EMA_FAST, "ema50")
    add_ema(df, HTF_EMA_SLOW, "ema200")

    closed = df.iloc[:-1].copy().reset_index(drop=True)
    if len(closed) < HTF_EMA_SLOW + 5:
        return "NONE"

    last = closed.iloc[-1]
    ema50 = safe_float(last["ema50"])
    ema200 = safe_float(last["ema200"])

    if ema50 > ema200:
        return "UP"
    if ema50 < ema200:
        return "DOWN"
    return "NONE"


def get_entry_tf_data(symbol: str):
    df = get_klines(symbol, ENTRY_INTERVAL, 350)

    add_ema(df, ETF_EMA_FAST, "ema9")
    add_ema(df, ETF_EMA_SLOW, "ema21")
    add_adx(df, ADX_PERIOD)
    add_atr(df, ATR_PERIOD)

    closed = df.iloc[:-1].copy().reset_index(drop=True)
    if len(closed) < 50:
        return None

    current = closed.iloc[-1]
    prev1 = closed.iloc[-2]
    prev2 = closed.iloc[-3]

    return {
        "df": closed,
        "current": current,
        "prev1": prev1,
        "prev2": prev2,
        "bar_time": current["startTime"],
    }


# =========================================================
# 신호 로직
# =========================================================
def adx_is_improving(prev2, prev1, current):
    up_count = 0
    if safe_float(prev1["adx"]) > safe_float(prev2["adx"]):
        up_count += 1
    if safe_float(current["adx"]) > safe_float(prev1["adx"]):
        up_count += 1
    return up_count >= 1


def prepare_pullback_state(symbol: str, direction: str, current_bar_time, ref_high: float, ref_low: float):
    if state[symbol]["b_used_in_trend"]:
        return

    state[symbol]["pullback_active"] = True
    state[symbol]["pullback_direction"] = direction
    state[symbol]["pullback_start_bar"] = current_bar_time
    state[symbol]["pullback_ref_high"] = ref_high
    state[symbol]["pullback_ref_low"] = ref_low


def update_pullback_state(symbol: str, signal: dict):
    bar_time = signal["bar_time"]
    interval_minutes = interval_to_minutes(ENTRY_INTERVAL)

    if state[symbol]["pullback_active"]:
        elapsed = bars_diff(
            state[symbol]["pullback_start_bar"],
            bar_time,
            interval_minutes
        )
        if elapsed > B_PULLBACK_VALID_BARS:
            reset_pullback_state(symbol)

    # 롱: 추세 중 EMA21 터치형 눌림
    if (
        not state[symbol]["pullback_active"]
        and not state[symbol]["b_used_in_trend"]
        and signal["ema9"] > signal["ema21"]
        and signal["adx_ok"]
        and safe_float(signal["current_low"]) <= safe_float(signal["ema21"]) <= safe_float(signal["current_high"])
    ):
        prepare_pullback_state(
            symbol=symbol,
            direction="LONG",
            current_bar_time=bar_time,
            ref_high=safe_float(signal["current_high"]),
            ref_low=safe_float(signal["current_low"]),
        )

    # 숏: 추세 중 EMA21 터치형 눌림
    if (
        not state[symbol]["pullback_active"]
        and not state[symbol]["b_used_in_trend"]
        and signal["ema9"] < signal["ema21"]
        and signal["adx_ok"]
        and safe_float(signal["current_low"]) <= safe_float(signal["ema21"]) <= safe_float(signal["current_high"])
    ):
        prepare_pullback_state(
            symbol=symbol,
            direction="SHORT",
            current_bar_time=bar_time,
            ref_high=safe_float(signal["current_high"]),
            ref_low=safe_float(signal["current_low"]),
        )


def get_entry_signal(symbol: str):
    data = get_entry_tf_data(symbol)
    if data is None:
        return None

    c = data["current"]
    p1 = data["prev1"]
    p2 = data["prev2"]

    current_price = safe_float(c["close"])
    current_high = safe_float(c["high"])
    current_low = safe_float(c["low"])
    atr = safe_float(c["atr"])
    adx = safe_float(c["adx"])
    ema9 = safe_float(c["ema9"])
    ema21 = safe_float(c["ema21"])
    bar_time = data["bar_time"]

    bullish_cross = (
        (safe_float(p1["ema9"]) <= safe_float(p1["ema21"]))
        and (ema9 > ema21)
        and (current_price > ema21)
    )
    bearish_cross = (
        (safe_float(p1["ema9"]) >= safe_float(p1["ema21"]))
        and (ema9 < ema21)
        and (current_price < ema21)
    )

    adx_ok = adx >= ADX_MIN
    adx_strong = adx >= ADX_STRONG
    adx_improving = adx_is_improving(p2, p1, c)

    signal = {
        "bar_time": bar_time,
        "current_price": current_price,
        "current_high": current_high,
        "current_low": current_low,
        "ema9": ema9,
        "ema21": ema21,
        "adx": adx,
        "atr": atr,
        "adx_ok": adx_ok,
        "adx_strong": adx_strong,
        "adx_improving": adx_improving,

        "long_A": bullish_cross,
        "short_A": bearish_cross,

        "prev1_high": safe_float(p1["high"]),
        "prev1_low": safe_float(p1["low"]),

        "reverse_cross_for_long_exit": bearish_cross,
        "reverse_cross_for_short_exit": bullish_cross,
    }

    return signal


def get_B_signal(symbol: str, signal: dict):
    if not state[symbol]["pullback_active"]:
        return False, ""

    direction = state[symbol]["pullback_direction"]
    elapsed = bars_diff(
        state[symbol]["pullback_start_bar"],
        signal["bar_time"],
        interval_to_minutes(ENTRY_INTERVAL)
    )
    if elapsed > B_PULLBACK_VALID_BARS:
        reset_pullback_state(symbol)
        return False, ""

    # EMA21 눌림 후, 눌림 캔들 고가/저가 돌파 시 즉시 진입
    if direction == "LONG":
        ref_high = state[symbol]["pullback_ref_high"]
        if signal["current_price"] > ref_high and signal["ema9"] > signal["ema21"]:
            return True, "LONG"

    if direction == "SHORT":
        ref_low = state[symbol]["pullback_ref_low"]
        if signal["current_price"] < ref_low and signal["ema9"] < signal["ema21"]:
            return True, "SHORT"

    return False, ""


# =========================================================
# 리스크 / 노출 / 재진입
# =========================================================
def estimate_total_exposure_ratio(total_equity: float) -> float:
    if total_equity <= 0:
        return 0.0

    total_notional = 0.0
    for symbol in SYMBOLS:
        pos = get_position_from_exchange(symbol)
        if pos:
            price = get_ticker_price(symbol)
            total_notional += pos["size"] * price

    return total_notional / total_equity


def bars_since_last_exit(symbol: str, current_bar_time) -> int:
    last_exit_bar = state[symbol]["last_exit_bar_time"]
    if last_exit_bar is None:
        return 9999
    return bars_diff(
        last_exit_bar,
        current_bar_time,
        interval_to_minutes(ENTRY_INTERVAL)
    )


def reentry_block_active(symbol: str, current_bar_time, adx: float) -> bool:
    elapsed_bars = bars_since_last_exit(symbol, current_bar_time)
    required_bars = STRONG_TREND_REENTRY_BARS if adx >= ADX_STRONG else REENTRY_BLOCK_BARS
    return elapsed_bars < required_bars


def compute_position_qty(symbol: str, equity: float, entry_price: float, atr: float):
    info = get_instrument_info(symbol)

    if atr <= 0 or entry_price <= 0 or equity <= 0:
        return 0.0, 0.0, 0.0

    stop_distance = atr * STOP_ATR_MULTIPLIER
    if stop_distance <= 0:
        return 0.0, 0.0, 0.0

    risk_amount = equity * RISK_PER_TRADE
    raw_qty = risk_amount / stop_distance
    raw_notional = raw_qty * entry_price

    max_notional = equity * MAX_POSITION_RATIO
    clamped_notional = min(raw_notional, max_notional)

    final_qty = clamped_notional / entry_price
    final_qty = normalize_qty(
        final_qty,
        info["qty_step"],
        info["min_order_qty"]
    )

    if final_qty == 0:
        return 0.0, stop_distance, clamped_notional

    return final_qty, stop_distance, final_qty * entry_price


def update_loss_streak_and_skip(symbol: str, direction: str, pnl_usdt: float):
    if pnl_usdt < 0:
        state[symbol]["loss_streak"][direction] += 1
    else:
        state[symbol]["loss_streak"][direction] = 0

    if state[symbol]["loss_streak"][direction] >= 2:
        state[symbol]["skip_next_entry"][direction] = 1
        state[symbol]["loss_streak"][direction] = 0


def consume_skip_if_needed(symbol: str, direction: str) -> bool:
    if state[symbol]["skip_next_entry"][direction] > 0:
        state[symbol]["skip_next_entry"][direction] -= 1
        return True
    return False


# =========================================================
# 상태 동기화
# =========================================================
def sync_state_with_exchange(symbol: str):
    pos = get_position_from_exchange(symbol)

    if pos is None:
        if state[symbol]["position_side"] != "NONE":
            reset_position_state(symbol)
        return

    side = "LONG" if pos["side"] == "Buy" else "SHORT"
    state[symbol]["position_side"] = side
    state[symbol]["position_qty"] = pos["size"]
    state[symbol]["position_entry_price"] = pos["avg_price"]

    last_price = get_ticker_price(symbol)

    if side == "LONG":
        if state[symbol]["highest_price"] is None:
            state[symbol]["highest_price"] = last_price
        state[symbol]["lowest_price"] = None
    else:
        if state[symbol]["lowest_price"] is None:
            state[symbol]["lowest_price"] = last_price
        state[symbol]["highest_price"] = None


# =========================================================
# 진입 / 청산 실행
# =========================================================
def open_position(symbol: str, direction: str, entry_type: str, signal: dict, wallet_info: dict):
    equity = wallet_info["total_equity"]
    current_price = signal["current_price"]
    atr = signal["atr"]
    adx = signal["adx"]
    bar_time = signal["bar_time"]

    qty, stop_distance, notional = compute_position_qty(symbol, equity, current_price, atr)
    if qty <= 0:
        return False, "qty=0"

    current_exposure = estimate_total_exposure_ratio(equity)
    additional_exposure = notional / equity if equity > 0 else 0.0
    if current_exposure + additional_exposure > MAX_TOTAL_EXPOSURE:
        return False, "max_total_exposure"

    if consume_skip_if_needed(symbol, direction):
        return False, "skip_after_2_losses"

    info = get_instrument_info(symbol)
    qty = normalize_qty(
        qty,
        info["qty_step"],
        info["min_order_qty"]
    )
    if qty == 0:
        return False, "qty_zero_after_normalize"

    side = "Buy" if direction == "LONG" else "Sell"
    result = place_market_entry(symbol, side, qty)

    time.sleep(0.5)
    pos = get_position_from_exchange(symbol)
    if pos:
        actual_entry_price = pos["avg_price"]
        actual_qty = pos["size"]
    else:
        actual_entry_price = get_ticker_price(symbol)
        actual_qty = qty

    state[symbol]["position_side"] = direction
    state[symbol]["position_qty"] = actual_qty
    state[symbol]["position_entry_price"] = actual_entry_price
    state[symbol]["last_entry_time"] = datetime.now()
    state[symbol]["last_entry_bar_time"] = bar_time
    state[symbol]["recent_entry_type"] = entry_type
    state[symbol]["entry_adx"] = adx
    state[symbol]["entry_atr"] = atr
    state[symbol]["trail_active"] = False
    state[symbol]["trail_price"] = None
    state[symbol]["recent_exit_reason"] = ""

    state[symbol]["initial_stop_distance"] = stop_distance
    state[symbol]["partial_exit_done"] = False

    if direction == "LONG":
        state[symbol]["entry_stop_price"] = actual_entry_price - stop_distance
        state[symbol]["partial_tp_price"] = actual_entry_price + (stop_distance * PARTIAL_TP_R_MULTIPLIER)
        state[symbol]["highest_price"] = actual_entry_price
        state[symbol]["lowest_price"] = None
    else:
        state[symbol]["entry_stop_price"] = actual_entry_price + stop_distance
        state[symbol]["partial_tp_price"] = actual_entry_price - (stop_distance * PARTIAL_TP_R_MULTIPLIER)
        state[symbol]["lowest_price"] = actual_entry_price
        state[symbol]["highest_price"] = None

    if entry_type == "B":
        state[symbol]["b_used_in_trend"] = True
        reset_pullback_state(symbol)

    log_trade(
        symbol=symbol,
        position_direction=direction,
        action="ENTRY",
        entry_type=entry_type,
        entry_price=actual_entry_price,
        exit_price=0.0,
        qty=actual_qty,
        pnl_usdt=0.0,
        pnl_pct=0.0,
        exit_reason="",
        entry_adx=adx,
        entry_atr=atr,
    )

    print(f"[{now_kst_str()}] {symbol} {direction} ENTRY | type={entry_type} | qty={actual_qty} | price={actual_entry_price}")
    print(result)
    return True, "ok"


def close_partial_position(symbol: str, reason: str):
    direction = state[symbol]["position_side"]
    entry_price = state[symbol]["position_entry_price"]
    current_qty = state[symbol]["position_qty"]

    if direction == "NONE" or current_qty <= 0 or entry_price <= 0:
        return False

    info = get_instrument_info(symbol)

    partial_qty = current_qty * PARTIAL_CLOSE_RATIO
    partial_qty = normalize_qty(
        partial_qty,
        info["qty_step"],
        info["min_order_qty"]
    )

    if partial_qty == 0:
        print(f"[{now_kst_str()}] {symbol} PARTIAL SKIP | qty too small")
        return False

    close_side = "Sell" if direction == "LONG" else "Buy"
    result = place_market_close(symbol, close_side, partial_qty)

    time.sleep(0.7)

    remaining_pos = get_position_from_exchange(symbol)
    exit_price = get_ticker_price(symbol)

    if direction == "LONG":
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        pnl_usdt = (exit_price - entry_price) * partial_qty
    else:
        pnl_pct = ((entry_price - exit_price) / entry_price) * 100
        pnl_usdt = (entry_price - exit_price) * partial_qty

    log_trade(
        symbol=symbol,
        position_direction=direction,
        action="PARTIAL_EXIT",
        entry_type=state[symbol]["recent_entry_type"],
        entry_price=entry_price,
        exit_price=exit_price,
        qty=partial_qty,
        pnl_usdt=pnl_usdt,
        pnl_pct=pnl_pct,
        exit_reason=reason,
        entry_adx=state[symbol]["entry_adx"],
        entry_atr=state[symbol]["entry_atr"],
    )

    if remaining_pos is None:
        # 부분청산했는데 전부 닫혀버린 경우
        update_loss_streak_and_skip(symbol, direction, pnl_usdt)
        reset_position_state(symbol)
        state[symbol]["last_exit_time"] = datetime.now()
        state[symbol]["recent_exit_reason"] = reason
        print(f"[{now_kst_str()}] {symbol} PARTIAL EXIT -> FULL CLOSED")
        print(result)
        return True

    # 남은 포지션 상태 갱신
    state[symbol]["position_qty"] = remaining_pos["size"]
    state[symbol]["position_entry_price"] = remaining_pos["avg_price"]
    state[symbol]["partial_exit_done"] = True

    # 본절 이동
    if direction == "LONG":
        state[symbol]["entry_stop_price"] = state[symbol]["position_entry_price"]
    else:
        state[symbol]["entry_stop_price"] = state[symbol]["position_entry_price"]

    print(f"[{now_kst_str()}] {symbol} PARTIAL EXIT | qty={partial_qty} | exit={exit_price} | remaining={remaining_pos['size']}")
    print(result)
    return True


def close_position(symbol: str, reason: str, exit_bar_time=None):
    direction = state[symbol]["position_side"]
    qty = state[symbol]["position_qty"]
    entry_price = state[symbol]["position_entry_price"]

    if direction == "NONE" or qty <= 0 or entry_price <= 0:
        return False

    info = get_instrument_info(symbol)
    qty = normalize_qty(
        qty,
        info["qty_step"],
        info["min_order_qty"]
    )

    if qty == 0:
        print(f"[{now_kst_str()}] {symbol} CLOSE SKIP | qty too small after normalize")
        return False

    close_side = "Sell" if direction == "LONG" else "Buy"
    result = place_market_close(symbol, close_side, qty)

    time.sleep(0.7)

    remaining_pos = get_position_from_exchange(symbol)
    if remaining_pos is not None:
        print(f"[{now_kst_str()}] {symbol} CLOSE NOT FULLY FILLED")
        return False

    exit_price = get_ticker_price(symbol)

    if direction == "LONG":
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        pnl_usdt = (exit_price - entry_price) * qty
    else:
        pnl_pct = ((entry_price - exit_price) / entry_price) * 100
        pnl_usdt = (entry_price - exit_price) * qty

    log_trade(
        symbol=symbol,
        position_direction=direction,
        action="EXIT",
        entry_type=state[symbol]["recent_entry_type"],
        entry_price=entry_price,
        exit_price=exit_price,
        qty=qty,
        pnl_usdt=pnl_usdt,
        pnl_pct=pnl_pct,
        exit_reason=reason,
        entry_adx=state[symbol]["entry_adx"],
        entry_atr=state[symbol]["entry_atr"],
    )

    update_loss_streak_and_skip(symbol, direction, pnl_usdt)

    reset_position_state(symbol)
    state[symbol]["last_exit_time"] = datetime.now()
    state[symbol]["last_exit_bar_time"] = exit_bar_time
    state[symbol]["recent_exit_reason"] = reason

    print(f"[{now_kst_str()}] {symbol} {direction} EXIT | reason={reason} | pnl={pnl_usdt:.4f} USDT ({pnl_pct:.4f}%)")
    print(result)
    return True


# =========================================================
# 트레일링 / 익절 / 손절
# =========================================================
def partial_tp_hit(symbol: str, current_price: float) -> bool:
    if state[symbol]["partial_exit_done"]:
        return False

    target = state[symbol]["partial_tp_price"]
    if target is None:
        return False

    if state[symbol]["position_side"] == "LONG":
        return current_price >= target

    if state[symbol]["position_side"] == "SHORT":
        return current_price <= target

    return False


def update_trailing(symbol: str, current_price: float, atr: float):
    direction = state[symbol]["position_side"]
    entry_price = state[symbol]["position_entry_price"]

    if direction == "NONE" or atr <= 0 or entry_price <= 0:
        return

    if direction == "LONG":
        if state[symbol]["highest_price"] is None or current_price > state[symbol]["highest_price"]:
            state[symbol]["highest_price"] = current_price

        if not state[symbol]["trail_active"]:
            if current_price >= entry_price + (atr * TRAIL_ACTIVATE_ATR):
                state[symbol]["trail_active"] = True
                candidate = state[symbol]["highest_price"] - (atr * TRAIL_ATR_MULTIPLIER)
                state[symbol]["trail_price"] = max(entry_price, candidate)
        else:
            candidate = state[symbol]["highest_price"] - (atr * TRAIL_ATR_MULTIPLIER)
            state[symbol]["trail_price"] = max(entry_price, candidate)

    elif direction == "SHORT":
        if state[symbol]["lowest_price"] is None or current_price < state[symbol]["lowest_price"]:
            state[symbol]["lowest_price"] = current_price

        if not state[symbol]["trail_active"]:
            if current_price <= entry_price - (atr * TRAIL_ACTIVATE_ATR):
                state[symbol]["trail_active"] = True
                candidate = state[symbol]["lowest_price"] + (atr * TRAIL_ATR_MULTIPLIER)
                state[symbol]["trail_price"] = min(entry_price, candidate)
        else:
            candidate = state[symbol]["lowest_price"] + (atr * TRAIL_ATR_MULTIPLIER)
            state[symbol]["trail_price"] = min(entry_price, candidate)


def trailing_hit(symbol: str, current_price: float) -> bool:
    if not state[symbol]["trail_active"] or state[symbol]["trail_price"] is None:
        return False

    if state[symbol]["position_side"] == "LONG":
        return current_price <= state[symbol]["trail_price"]

    if state[symbol]["position_side"] == "SHORT":
        return current_price >= state[symbol]["trail_price"]

    return False


def stop_hit(symbol: str, current_price: float) -> bool:
    stop_price = state[symbol]["entry_stop_price"]
    if stop_price is None:
        return False

    if state[symbol]["position_side"] == "LONG":
        return current_price <= stop_price

    if state[symbol]["position_side"] == "SHORT":
        return current_price >= stop_price

    return False


# =========================================================
# 추세 변화 처리
# =========================================================
def update_trend_context(symbol: str, higher_trend: str):
    prev_trend = state[symbol]["last_higher_trend"]

    if prev_trend != higher_trend:
        state[symbol]["b_used_in_trend"] = False
        reset_pullback_state(symbol)

    state[symbol]["last_higher_trend"] = higher_trend


# =========================================================
# 메인 심볼 처리
# =========================================================
def process_symbol(symbol: str, wallet_info: dict):
    func_name = "process_symbol"
    try:
        sync_state_with_exchange(symbol)

        higher_trend = get_higher_tf_trend(symbol)
        update_trend_context(symbol, higher_trend)

        signal = get_entry_signal(symbol)
        if signal is None:
            return

        update_pullback_state(symbol, signal)

        current_price = get_ticker_price(symbol)
        position_state = state[symbol]["position_side"]

        if position_state in ("LONG", "SHORT"):
            update_trailing(symbol, current_price, signal["atr"])

            # 1순위 손절
            if stop_hit(symbol, current_price):
                log_status(symbol, {
                    "current_price": current_price,
                    "ema9": signal["ema9"],
                    "ema21": signal["ema21"],
                    "adx": signal["adx"],
                    "adx_improving": signal["adx_improving"],
                    "atr": signal["atr"],
                    "higher_tf_trend": higher_trend,
                    "signal_state": "HOLD",
                    "position_state": position_state,
                    "partial_exit_done": state[symbol]["partial_exit_done"],
                    "entry_stop_price": state[symbol]["entry_stop_price"],
                })
                close_position(symbol, "손절", exit_bar_time=signal["bar_time"])
                return

            # 2순위 1차 익절
            if partial_tp_hit(symbol, current_price):
                ok = close_partial_position(symbol, "1차익절")
                if ok:
                    return

            # 3순위 트레일링
            if trailing_hit(symbol, current_price):
                log_status(symbol, {
                    "current_price": current_price,
                    "ema9": signal["ema9"],
                    "ema21": signal["ema21"],
                    "adx": signal["adx"],
                    "adx_improving": signal["adx_improving"],
                    "atr": signal["atr"],
                    "higher_tf_trend": higher_trend,
                    "signal_state": "HOLD",
                    "position_state": position_state,
                    "partial_exit_done": state[symbol]["partial_exit_done"],
                    "entry_stop_price": state[symbol]["entry_stop_price"],
                })
                close_position(symbol, "트레일링", exit_bar_time=signal["bar_time"])
                return

            # 4순위 추세이탈
            if position_state == "LONG" and signal["reverse_cross_for_long_exit"]:
                log_status(symbol, {
                    "current_price": current_price,
                    "ema9": signal["ema9"],
                    "ema21": signal["ema21"],
                    "adx": signal["adx"],
                    "adx_improving": signal["adx_improving"],
                    "atr": signal["atr"],
                    "higher_tf_trend": higher_trend,
                    "signal_state": "HOLD",
                    "position_state": position_state,
                    "partial_exit_done": state[symbol]["partial_exit_done"],
                    "entry_stop_price": state[symbol]["entry_stop_price"],
                })
                close_position(symbol, "추세이탈", exit_bar_time=signal["bar_time"])
                return

            if position_state == "SHORT" and signal["reverse_cross_for_short_exit"]:
                log_status(symbol, {
                    "current_price": current_price,
                    "ema9": signal["ema9"],
                    "ema21": signal["ema21"],
                    "adx": signal["adx"],
                    "adx_improving": signal["adx_improving"],
                    "atr": signal["atr"],
                    "higher_tf_trend": higher_trend,
                    "signal_state": "HOLD",
                    "position_state": position_state,
                    "partial_exit_done": state[symbol]["partial_exit_done"],
                    "entry_stop_price": state[symbol]["entry_stop_price"],
                })
                close_position(symbol, "추세이탈", exit_bar_time=signal["bar_time"])
                return

            log_status(symbol, {
                "current_price": current_price,
                "ema9": signal["ema9"],
                "ema21": signal["ema21"],
                "adx": signal["adx"],
                "adx_improving": signal["adx_improving"],
                "atr": signal["atr"],
                "higher_tf_trend": higher_trend,
                "signal_state": "HOLD",
                "position_state": position_state,
                "partial_exit_done": state[symbol]["partial_exit_done"],
                "entry_stop_price": state[symbol]["entry_stop_price"],
            })
            print(f"[{now_kst_str()}] {symbol} HOLD | pos={position_state} | price={current_price}")
            return

        # 포지션 없음
        if higher_trend == "NONE":
            log_status(symbol, {
                "current_price": current_price,
                "ema9": signal["ema9"],
                "ema21": signal["ema21"],
                "adx": signal["adx"],
                "adx_improving": signal["adx_improving"],
                "atr": signal["atr"],
                "higher_tf_trend": higher_trend,
                "signal_state": "NO TRADE",
                "position_state": "NONE",
                "pullback_active": state[symbol]["pullback_active"],
                "pullback_direction": state[symbol]["pullback_direction"],
            })
            print(f"[{now_kst_str()}] {symbol} NO TRADE | higher trend NONE")
            return

        if not signal["adx_ok"]:
            log_status(symbol, {
                "current_price": current_price,
                "ema9": signal["ema9"],
                "ema21": signal["ema21"],
                "adx": signal["adx"],
                "adx_improving": signal["adx_improving"],
                "atr": signal["atr"],
                "higher_tf_trend": higher_trend,
                "signal_state": "NO TRADE",
                "position_state": "NONE",
                "pullback_active": state[symbol]["pullback_active"],
                "pullback_direction": state[symbol]["pullback_direction"],
            })
            print(f"[{now_kst_str()}] {symbol} NO TRADE | ADX low")
            return

        if reentry_block_active(symbol, signal["bar_time"], signal["adx"]):
            log_status(symbol, {
                "current_price": current_price,
                "ema9": signal["ema9"],
                "ema21": signal["ema21"],
                "adx": signal["adx"],
                "adx_improving": signal["adx_improving"],
                "atr": signal["atr"],
                "higher_tf_trend": higher_trend,
                "signal_state": "REENTRY BLOCK",
                "position_state": "NONE",
                "pullback_active": state[symbol]["pullback_active"],
                "pullback_direction": state[symbol]["pullback_direction"],
            })
            print(f"[{now_kst_str()}] {symbol} REENTRY BLOCK")
            return

        long_ready = False
        short_ready = False
        entry_type = ""

        # A급 우선
        if higher_trend == "UP" and signal["long_A"]:
            long_ready = True
            entry_type = "A"

        elif higher_trend == "DOWN" and signal["short_A"]:
            short_ready = True
            entry_type = "A"

        else:
            # B급(EMA21 터치 후 반전 돌파)
            b_ok, b_dir = get_B_signal(symbol, signal)
            if b_ok and higher_trend == "UP" and b_dir == "LONG":
                long_ready = True
                entry_type = "B"
            elif b_ok and higher_trend == "DOWN" and b_dir == "SHORT":
                short_ready = True
                entry_type = "B"

        signal_state = "NO TRADE"
        if long_ready:
            signal_state = "LONG READY"
        elif short_ready:
            signal_state = "SHORT READY"

        log_status(symbol, {
            "current_price": current_price,
            "ema9": signal["ema9"],
            "ema21": signal["ema21"],
            "adx": signal["adx"],
            "adx_improving": signal["adx_improving"],
            "atr": signal["atr"],
            "higher_tf_trend": higher_trend,
            "signal_state": signal_state,
            "position_state": "NONE",
            "pullback_active": state[symbol]["pullback_active"],
            "pullback_direction": state[symbol]["pullback_direction"],
        })

        if long_ready:
            ok, reason = open_position(symbol, "LONG", entry_type, signal, wallet_info)
            print(f"[{now_kst_str()}] {symbol} LONG READY | type={entry_type} | result={ok} | reason={reason}")
            return

        if short_ready:
            ok, reason = open_position(symbol, "SHORT", entry_type, signal, wallet_info)
            print(f"[{now_kst_str()}] {symbol} SHORT READY | type={entry_type} | result={ok} | reason={reason}")
            return

        print(f"[{now_kst_str()}] {symbol} NO TRADE | trend={higher_trend} | adx={signal['adx']:.2f}")

    except Exception as e:
        log_error(
            symbol=symbol,
            func_name=func_name,
            error_text=f"{e}\n{traceback.format_exc()}",
            order_failed=False,
            api_message=""
        )
        print(f"[{now_kst_str()}] ERROR {symbol}: {e}")


# =========================================================
# 시작 시 상태 정리
# =========================================================
def bootstrap_state():
    for symbol in SYMBOLS:
        try:
            set_leverage(symbol, "3")
            sync_state_with_exchange(symbol)

            pos = get_position_from_exchange(symbol)
            if pos:
                if pos["side"] == "Buy":
                    state[symbol]["position_side"] = "LONG"
                    state[symbol]["highest_price"] = get_ticker_price(symbol)
                    state[symbol]["lowest_price"] = None
                else:
                    state[symbol]["position_side"] = "SHORT"
                    state[symbol]["lowest_price"] = get_ticker_price(symbol)
                    state[symbol]["highest_price"] = None

                state[symbol]["position_qty"] = pos["size"]
                state[symbol]["position_entry_price"] = pos["avg_price"]

        except Exception as e:
            log_error(
                symbol=symbol,
                func_name="bootstrap_state",
                error_text=f"{e}\n{traceback.format_exc()}",
                order_failed=False,
                api_message=""
            )


# =========================================================
# main
# =========================================================
def main():
    print(f"[{now_kst_str()}] === Bybit Auto Bot data-collection final start ===")
    bootstrap_state()

    while True:
        try:
            wallet_info = get_wallet_info()
            log_balance(wallet_info)

            print(
                f"[{now_kst_str()}] BALANCE | "
                f"equity={wallet_info['total_equity']:.2f} | "
                f"available={wallet_info['available_balance']:.2f} | "
                f"upl={wallet_info['unrealised_pnl']:.4f} | "
                f"realised={wallet_info['cum_realised_pnl']:.4f}"
            )

            for symbol in SYMBOLS:
                process_symbol(symbol, wallet_info)

        except Exception as e:
            log_error(
                symbol="SYSTEM",
                func_name="main_loop",
                error_text=f"{e}\n{traceback.format_exc()}",
                order_failed=False,
                api_message=""
            )
            print(f"[{now_kst_str()}] SYSTEM ERROR: {e}")

        print(f"[{now_kst_str()}] ----- sleep {LOOP_SLEEP_SEC}s -----")
        time.sleep(LOOP_SLEEP_SEC)


if __name__ == "__main__":
    main()
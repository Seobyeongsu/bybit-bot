from flask import Flask, render_template, send_file, request
import os
import csv
import subprocess

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TRADE_LOG_FILES = {
    "demo": os.path.join(BASE_DIR, "trade_log_demo.csv"),
    "live": os.path.join(BASE_DIR, "trade_log_live.csv"),
}

BOT_LOG_FILES = {
    "demo": os.path.join(BASE_DIR, "run_demo.log"),
    "live": os.path.join(BASE_DIR, "run_live.log"),
}

STATUS_LOG_FILES = {
    "demo": os.path.join(BASE_DIR, "status_log_demo.csv"),
    "live": os.path.join(BASE_DIR, "status_log_live.csv"),
}

BALANCE_LOG_FILES = {
    "demo": os.path.join(BASE_DIR, "balance_log_demo.csv"),
    "live": os.path.join(BASE_DIR, "balance_log_live.csv"),
}


def get_mode():
    mode = request.args.get("mode", "demo").strip().lower()
    return mode if mode in ("demo", "live") else "demo"


def get_current_files():
    mode = get_mode()
    return {
        "mode": mode,
        "trade_log": TRADE_LOG_FILES[mode],
        "bot_log": BOT_LOG_FILES[mode],
        "status_log": STATUS_LOG_FILES[mode],
        "balance_log": BALANCE_LOG_FILES[mode],
    }


def get_bot_status(mode):
    try:
        target = "main_demo.py" if mode == "demo" else "main_live.py"
        result = subprocess.run(
            ["pgrep", "-f", target],
            capture_output=True,
            text=True,
        )
        return "ON" if result.returncode == 0 else "OFF"
    except Exception:
        return "OFF"


def load_trade_data(file_path):
    trades = []

    if not os.path.exists(file_path):
        return trades

    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)

            for row in reader:
                if not row or len(row) < 14:
                    continue

                trade = {
                    "time": row[0].strip(),
                    "symbol": row[1].strip(),
                    "side": row[2].strip(),
                    "action": row[3].strip(),
                    "entry_type": row[4].strip(),
                    "entry_price": row[5].strip(),
                    "exit_price": row[6].strip(),
                    "qty": row[7].strip(),
                    "pnl_usdt": row[8].strip(),
                    "pnl_pct": row[9].strip(),
                    "exit_reason": row[10].strip(),
                    "entry_adx": row[11].strip(),
                    "entry_atr": row[12].strip(),
                    "entry_vol_ratio": row[13].strip(),
                }
                trades.append(trade)

        print(f"[{get_mode()}] 로드된 거래 수: {len(trades)}")

    except Exception as e:
        print("파일 읽기 오류:", e)

    return trades


def get_trade_summary(trades):
    exit_trades = []

    for t in trades:
        action = t.get("action", "").strip().upper()
        if action == "EXIT":
            exit_trades.append(t)

    total_trades = len(exit_trades)
    win_trades = 0
    total_pnl = 0.0

    for t in exit_trades:
        try:
            pnl = float(t.get("pnl_usdt", 0))
        except Exception:
            pnl = 0.0

        total_pnl += pnl

        if pnl > 0:
            win_trades += 1

    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
    recent_exits = list(reversed(exit_trades))[:10]

    return {
        "total_trades": total_trades,
        "win_trades": win_trades,
        "loss_trades": total_trades - win_trades,
        "win_rate": round(win_rate, 2),
        "total_pnl": round(total_pnl, 2),
        "recent_exits": recent_exits,
    }


def get_strategy_stats(trades):
    result = {
        "A": {"total": 0, "win": 0, "pnl": 0.0},
        "B": {"total": 0, "win": 0, "pnl": 0.0},
        "A1": {"total": 0, "win": 0, "pnl": 0.0},
        "A2": {"total": 0, "win": 0, "pnl": 0.0},
        "B1": {"total": 0, "win": 0, "pnl": 0.0},
        "B2": {"total": 0, "win": 0, "pnl": 0.0},
    }

    for t in trades:
        if t.get("action", "").strip().upper() != "EXIT":
            continue

        strategy = t.get("entry_type", "").strip().upper()

        if strategy not in result:
            result[strategy] = {"total": 0, "win": 0, "pnl": 0.0}

        try:
            pnl = float(t.get("pnl_usdt", 0))
        except Exception:
            pnl = 0.0

        result[strategy]["total"] += 1
        result[strategy]["pnl"] += pnl

        if pnl > 0:
            result[strategy]["win"] += 1

    for key in result:
        total = result[key]["total"]
        win = result[key]["win"]
        win_rate = (win / total * 100) if total > 0 else 0

        result[key]["win_rate"] = round(win_rate, 2)
        result[key]["pnl"] = round(result[key]["pnl"], 2)

    return result


@app.route("/")
def dashboard():
    files = get_current_files()
    trades = load_trade_data(files["trade_log"])
    summary = get_trade_summary(trades)
    strategy_stats = get_strategy_stats(trades)
    bot_status = get_bot_status(files["mode"])

    return render_template(
        "dashboard.html",
        summary=summary,
        strategy_stats=strategy_stats,
        bot_status=bot_status,
        mode=files["mode"],
    )


@app.route("/download/trade-log")
def download_trade_log():
    files = get_current_files()
    if os.path.exists(files["trade_log"]):
        return send_file(files["trade_log"], as_attachment=True)
    return f"{files['mode']} 거래로그 파일이 없습니다.", 404


@app.route("/download/bot-log")
def download_bot_log():
    files = get_current_files()
    if os.path.exists(files["bot_log"]):
        return send_file(files["bot_log"], as_attachment=True)
    return f"{files['mode']} 봇로그 파일이 없습니다.", 404


@app.route("/download/status-log")
def download_status_log():
    files = get_current_files()
    if os.path.exists(files["status_log"]):
        return send_file(files["status_log"], as_attachment=True)
    return f"{files['mode']} 상태로그 파일이 없습니다.", 404


@app.route("/download/balance-log")
def download_balance_log():
    files = get_current_files()
    if os.path.exists(files["balance_log"]):
        return send_file(files["balance_log"], as_attachment=True)
    return f"{files['mode']} 잔고로그 파일이 없습니다.", 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

from flask import Flask, render_template, send_file
import csv
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TRADE_LOG_FILE = os.path.join(BASE_DIR, "trade_log.csv")
BOT_LOG_FILE = os.path.join(BASE_DIR, "bot.log")
STATUS_LOG_FILE = os.path.join(BASE_DIR, "status_log.csv")
BALANCE_LOG_FILE = os.path.join(BASE_DIR, "balance_log.csv")


def load_trade_data():
    trades = []

    try:
        with open(TRADE_LOG_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                trades.append(row)

        print(f"로드된 거래 수: {len(trades)}")

    except Exception as e:
        print("파일 읽기 오류:", e)

    return trades


def get_trade_summary(trades):
    exit_trades = []

    for t in trades:
        action = t.get("action", "").strip()
        if action == "EXIT":
            exit_trades.append(t)

    total_trades = len(exit_trades)
    win_trades = 0
    total_pnl = 0.0

    for t in exit_trades:
        try:
            pnl = float(t.get("pnl_usdt", 0))
        except:
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
        "recent_exits": recent_exits
    }


def get_strategy_stats(trades):
    result = {
        "A": {"total": 0, "win": 0, "pnl": 0.0},
        "B": {"total": 0, "win": 0, "pnl": 0.0},
    }

    for t in trades:
        if t.get("action", "").strip() != "EXIT":
            continue

        strategy = t.get("entry_type", "").strip()

        if strategy not in result:
            continue

        try:
            pnl = float(t.get("pnl_usdt", 0))
        except:
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
    trades = load_trade_data()
    summary = get_trade_summary(trades)
    strategy_stats = get_strategy_stats(trades)

    return render_template(
        "dashboard.html",
        summary=summary,
        strategy_stats=strategy_stats
    )


@app.route("/download/trade-log")
def download_trade_log():
    if os.path.exists(TRADE_LOG_FILE):
        return send_file(TRADE_LOG_FILE, as_attachment=True)
    return "trade_log.csv 파일이 없습니다.", 404


@app.route("/download/bot-log")
def download_bot_log():
    if os.path.exists(BOT_LOG_FILE):
        return send_file(BOT_LOG_FILE, as_attachment=True)
    return "bot.log 파일이 없습니다.", 404


@app.route("/download/status-log")
def download_status_log():
    if os.path.exists(STATUS_LOG_FILE):
        return send_file(STATUS_LOG_FILE, as_attachment=True)
    return "status_log.csv 파일이 없습니다.", 404


@app.route("/download/balance-log")
def download_balance_log():
    if os.path.exists(BALANCE_LOG_FILE):
        return send_file(BALANCE_LOG_FILE, as_attachment=True)
    return "balance_log.csv 파일이 없습니다.", 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
"""
xyz:SILVER ポジションランキング & リアルタイム約定モニター v2

データ収集:
  1. リーダーボードAPIから上位トレーダーのアドレスを取得
  2. recentTrades / userFillsByTime で約定からアドレス収集
  3. WebSocketでリアルタイム約定を監視 → 新アドレス追加
  4. 収集したアドレスをJSONファイルに永続保存

ポジション取得:
  - batchClearinghouseStates で一括取得（1リクエストで複数アドレス）
  - アドレス数に応じて更新間隔を自動調整
"""

import asyncio
import json
import time
import os
from datetime import datetime
from pathlib import Path

try:
    import websockets
except ImportError:
    print("pip install websockets aiohttp")
    exit(1)

try:
    import aiohttp
except ImportError:
    print("pip install websockets aiohttp")
    exit(1)

COIN = "xyz:SILVER"
DEX = "xyz"
WS_URL = "wss://api.hyperliquid.xyz/ws"
API_URL = "https://api.hyperliquid.xyz/info"
LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
DATA_FILE = Path(__file__).parent / "silver_addresses.json"

BATCH_SIZE = 20  # batchClearinghouseStates の1回あたりのアドレス数

known_addresses = set()
positions = {}
trade_count = 0
api_calls = 0  # API呼び出し数カウント


def get_position_ttl():
    """アドレス数に応じて更新間隔を自動調整"""
    n = len(known_addresses)
    if n <= 100:
        return 60
    elif n <= 300:
        return 120
    elif n <= 600:
        return 180
    else:
        return max(300, n // 2)


# ===== データ永続化 =====

def save_addresses():
    data = {
        "updated": datetime.now().isoformat(),
        "count": len(known_addresses),
        "addresses": sorted(list(known_addresses)),
    }
    DATA_FILE.write_text(json.dumps(data, indent=2))


def load_addresses():
    if not DATA_FILE.exists():
        return 0
    try:
        data = json.loads(DATA_FILE.read_text())
        addrs = data.get("addresses", [])
        known_addresses.update(addrs)
        return len(addrs)
    except Exception:
        return 0


# ===== アドレス収集 =====

async def fetch_leaderboard(session):
    """リーダーボードAPIからトップトレーダーのアドレスを取得"""
    global api_calls
    collected = 0
    try:
        async with session.get(LEADERBOARD_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            api_calls += 1
            if resp.status == 200:
                data = await resp.json()
                rows = data.get("leaderboardRows", [])
                for row in rows:
                    addr = row.get("ethAddress", "")
                    if addr and len(addr) > 10:
                        known_addresses.add(addr)
                        collected += 1
    except Exception:
        pass
    return collected


async def fetch_recent_trades(session):
    """REST APIで直近の約定を取得しアドレスを収集"""
    global api_calls
    collected = 0

    # recentTrades
    try:
        payload = {"type": "recentTrades", "coin": COIN}
        async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            api_calls += 1
            if resp.status == 200:
                trades = await resp.json()
                if isinstance(trades, list):
                    for t in trades:
                        for u in t.get("users", []):
                            if u and u != "?" and len(u) > 10:
                                known_addresses.add(u)
                                collected += 1
    except Exception:
        pass

    # userFillsByTime で既知アドレスの取引相手を発見
    try:
        for addr in list(known_addresses)[:30]:
            payload = {
                "type": "userFillsByTime",
                "user": addr,
                "startTime": int((time.time() - 86400 * 3) * 1000),  # 3日前
                "endTime": int(time.time() * 1000),
            }
            async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                api_calls += 1
                if resp.status == 200:
                    fills = await resp.json()
                    if isinstance(fills, list):
                        for f in fills:
                            if f.get("coin") == COIN:
                                for u in f.get("users", []):
                                    if u and u != "?" and len(u) > 10:
                                        known_addresses.add(u)
                                        collected += 1
            await asyncio.sleep(0.15)
    except Exception:
        pass

    return collected


# ===== ポジション取得 (batch API) =====

async def batch_fetch_positions(session, addrs):
    """batchClearinghouseStates で一括取得"""
    global api_calls
    results = {}

    for i in range(0, len(addrs), BATCH_SIZE):
        batch = addrs[i:i + BATCH_SIZE]
        try:
            payload = {
                "type": "batchClearinghouseStates",
                "users": batch,
                "dex": DEX,
            }
            async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                api_calls += 1
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        for addr, state in zip(batch, data):
                            pos_data = {"size": 0, "entry_px": 0, "upnl": 0, "liq_px": 0, "leverage": {}}
                            if state and "assetPositions" in state:
                                for p in state["assetPositions"]:
                                    if p.get("position", {}).get("coin") == COIN:
                                        pos_data = {
                                            "size": float(p["position"].get("szi", 0)),
                                            "entry_px": float(p["position"].get("entryPx", "0")),
                                            "upnl": float(p["position"].get("unrealizedPnl", "0")),
                                            "liq_px": float(p["position"].get("liquidationPx", "0") or "0"),
                                            "leverage": p["position"].get("leverage", {}),
                                        }
                                        break
                            results[addr] = pos_data
        except Exception:
            # バッチ失敗時は個別にフォールバック
            for addr in batch:
                try:
                    payload = {"type": "clearinghouseState", "user": addr, "dex": DEX}
                    async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        api_calls += 1
                        if resp.status == 200:
                            state = await resp.json()
                            pos_data = {"size": 0, "entry_px": 0, "upnl": 0, "liq_px": 0, "leverage": {}}
                            if state and "assetPositions" in state:
                                for p in state["assetPositions"]:
                                    if p.get("position", {}).get("coin") == COIN:
                                        pos_data = {
                                            "size": float(p["position"].get("szi", 0)),
                                            "entry_px": float(p["position"].get("entryPx", "0")),
                                            "upnl": float(p["position"].get("unrealizedPnl", "0")),
                                            "liq_px": float(p["position"].get("liquidationPx", "0") or "0"),
                                            "leverage": p["position"].get("leverage", {}),
                                        }
                                        break
                            results[addr] = pos_data
                except Exception:
                    pass

        if i + BATCH_SIZE < len(addrs):
            await asyncio.sleep(0.3)

    return results


async def update_all_positions(session):
    """全アドレスのポジションを更新"""
    ttl = get_position_ttl()
    addrs = [a for a in known_addresses
             if a not in positions or time.time() - positions.get(a, {}).get("updated", 0) >= ttl]

    if not addrs:
        return

    results = await batch_fetch_positions(session, addrs)
    now = time.time()
    for addr, data in results.items():
        positions[addr] = {**data, "updated": now}


# ===== 表示 =====

def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def short_addr(addr):
    if not addr or len(addr) < 12:
        return addr or "?"
    return addr[:6] + "..." + addr[-4:]


def render(last_trades):
    clear_screen()

    now = datetime.now().strftime("%H:%M:%S")
    active = sum(1 for d in positions.values() if d["size"] != 0)
    ttl = get_position_ttl()
    est_rpm = len(known_addresses) * 60 // max(ttl, 1) // max(BATCH_SIZE, 1)

    print(f"\033[96m{'='*84}\033[0m")
    print(f"\033[96m  xyz:SILVER Position Ranking & Trade Monitor  \033[90mv2 (batch API)\033[0m")
    print(f"\033[90m  {now}  |  Addr: {len(known_addresses)}  |  "
          f"Active: {active}  |  Trades: {trade_count}  |  "
          f"API calls: {api_calls}  |  ~{est_rpm} req/min\033[0m")
    print(f"\033[96m{'='*84}\033[0m")

    # ランキング
    ranked = [(a, d) for a, d in positions.items() if d["size"] != 0]
    ranked.sort(key=lambda x: abs(x[1]["size"]), reverse=True)

    longs = [r for r in ranked if r[1]["size"] > 0]
    shorts = [r for r in ranked if r[1]["size"] < 0]
    total_long = sum(d["size"] for _, d in longs)
    total_short = sum(abs(d["size"]) for _, d in shorts)
    total = total_long + total_short

    print()
    print(f"\033[93m  ── Position Ranking {'─'*58}\033[0m")
    if total > 0:
        bar_len = 40
        long_bar = int(total_long / total * bar_len)
        short_bar = bar_len - long_bar
        bar = f"\033[92m{'█' * long_bar}\033[91m{'█' * short_bar}\033[0m"
        print(f"  {bar}  \033[92mL:{len(longs)}({total_long:+.1f})\033[0m  "
              f"\033[91mS:{len(shorts)}({-total_short:.1f})\033[0m  "
              f"[{total_long/total*100:.0f}% / {total_short/total*100:.0f}%]")
    print()
    print(f"\033[90m  {'#':>3}  {'Address':<15} {'Notional':>12} {'Position':>12} {'Side':>6} "
          f"{'Entry':>10} {'uPnL':>12} {'Lev':>5}\033[0m")
    print(f"\033[90m  {'─'*84}\033[0m")

    if not ranked:
        print(f"\033[90m  Collecting addresses...\033[0m")
    else:
        for i, (addr, data) in enumerate(ranked[:25], 1):
            size = data["size"]
            col = "\033[92m" if size > 0 else "\033[91m"
            side = "LONG" if size > 0 else "SHORT"
            entry = data["entry_px"]
            entry_s = f"${entry:.3f}" if entry > 0 else "-"
            notional = abs(size * entry) if entry > 0 else 0
            notional_s = f"${notional:,.0f}" if notional > 0 else "-"
            upnl = data["upnl"]
            upnl_col = "\033[92m" if upnl >= 0 else "\033[91m"

            lev_info = data.get("leverage", {})
            lev = ""
            if isinstance(lev_info, dict) and lev_info.get("value"):
                lev = f"{lev_info['value']}x"

            print(f"  {i:>3}  {short_addr(addr):<15} "
                  f"{col}{notional_s:>12}\033[0m "
                  f"{col}{size:>+12.4f}\033[0m "
                  f"{col}{side:>6}\033[0m "
                  f"{entry_s:>10} "
                  f"{upnl_col}{'$'+f'{upnl:+.2f}':>12}\033[0m "
                  f"{lev:>5}")

    zero = sum(1 for d in positions.values() if d["size"] == 0)
    pending = len(known_addresses) - len(positions)
    print(f"\033[90m\n  (0 pos: {zero} | pending: {pending})\033[0m")

    # 約定
    print()
    print(f"\033[93m  ── Recent Trades {'─'*62}\033[0m")
    print(f"\033[90m  {'Time':<10} {'Price':>10} {'Size':>10} {'Value':>10} "
          f"{'Side':>5}  {'Buyer':<13} {'Seller':<13}\033[0m")
    print(f"\033[90m  {'─'*80}\033[0m")

    if not last_trades:
        print(f"\033[90m  Waiting for trades...\033[0m")
    else:
        for t in last_trades[-12:]:
            ts = datetime.fromtimestamp(t["time"] / 1000).strftime("%H:%M:%S")
            val = t["price"] * t["size"]
            is_buy = t["side"] == "A"
            col = "\033[92m" if is_buy else "\033[91m"
            side = "BUY" if is_buy else "SELL"
            val_s = f"${val:,.2f}" if val < 1000 else f"${val/1000:.1f}K"

            print(f"  {ts:<10} "
                  f"{'$'+f'{t['price']:.3f}':>10} "
                  f"{t['size']:>10.4f} "
                  f"{val_s:>10} "
                  f"{col}{side:>5}\033[0m  "
                  f"{short_addr(t['buyer']):<13} "
                  f"{short_addr(t['seller']):<13}")

    print(f"\n\033[90m  Ctrl+C to exit  |  TTL: {ttl}s  |  Batch: {BATCH_SIZE}/req  |  Auto-save 30s\033[0m")


# ===== メイン =====

async def main():
    global trade_count

    last_trades = []

    # 保存済みアドレス読み込み
    loaded = load_addresses()
    print(f"\033[96m  Loaded {loaded} saved addresses from {DATA_FILE.name}\033[0m")

    async with aiohttp.ClientSession() as session:
        # リーダーボードからアドレス収集
        print(f"\033[90m  Fetching leaderboard...\033[0m")
        lb_count = await fetch_leaderboard(session)
        print(f"\033[96m  Leaderboard: {lb_count} addresses\033[0m")

        # 過去の約定からアドレス収集
        print(f"\033[90m  Fetching recent trades...\033[0m")
        rt_count = await fetch_recent_trades(session)
        print(f"\033[96m  Recent trades: {rt_count} addresses\033[0m")
        print(f"\033[96m  Total known: {len(known_addresses)} addresses\033[0m")

        # 初回ポジション取得
        print(f"\033[90m  Fetching positions (batch API)...\033[0m")
        await update_all_positions(session)
        save_addresses()
        active = sum(1 for d in positions.values() if d["size"] != 0)
        print(f"\033[92m  Ready! {active} active positions found. Connecting...\033[0m")

        last_save = time.time()

        while True:
            try:
                async with websockets.connect(WS_URL) as ws_conn:
                    await ws_conn.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": COIN}
                    }))

                    render(last_trades)
                    last_update = time.time()

                    async for msg in ws_conn:
                        data = json.loads(msg)
                        if data.get("channel") != "trades":
                            continue

                        for trade in data.get("data", []):
                            trade_count += 1
                            users = trade.get("users", ["?", "?"])
                            buyer, seller = users[0], users[1]

                            known_addresses.add(buyer)
                            known_addresses.add(seller)

                            last_trades.append({
                                "time": trade["time"],
                                "price": float(trade["px"]),
                                "size": float(trade["sz"]),
                                "side": trade["side"],
                                "buyer": buyer,
                                "seller": seller,
                            })

                        if len(last_trades) > 100:
                            last_trades = last_trades[-100:]

                        now = time.time()

                        if now - last_update >= 5:
                            await update_all_positions(session)
                            render(last_trades)
                            last_update = now

                        if now - last_save >= 30:
                            save_addresses()
                            last_save = now

            except (websockets.exceptions.ConnectionClosed, ConnectionError, OSError):
                print("\n\033[91m  Connection lost. Reconnecting in 3s...\033[0m")
                save_addresses()
                await asyncio.sleep(3)
            except KeyboardInterrupt:
                save_addresses()
                print(f"\n\033[93m  Saved {len(known_addresses)} addresses. Stopped.\033[0m")
                return


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        save_addresses()
        print(f"\n\033[93m  Saved {len(known_addresses)} addresses. Stopped.\033[0m")

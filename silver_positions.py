"""
xyz:SILVER ポジションランキング & リアルタイム約定モニター

- 起動時にREST APIで過去の約定を取得 → アドレス収集
- WebSocketでリアルタイム約定を監視 → アドレス追加
- 収集したアドレスをJSONファイルに永続保存
- 各アドレスのポジションを取得してランキング表示
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
DATA_FILE = Path(__file__).parent / "silver_addresses.json"

known_addresses = set()
positions = {}
trade_count = 0
POSITION_TTL = 60


# ===== データ永続化 =====

def save_addresses():
    """収集済みアドレスをJSONに保存"""
    data = {
        "updated": datetime.now().isoformat(),
        "count": len(known_addresses),
        "addresses": sorted(list(known_addresses)),
    }
    DATA_FILE.write_text(json.dumps(data, indent=2))


def load_addresses():
    """保存済みアドレスを読み込み"""
    if not DATA_FILE.exists():
        return 0
    try:
        data = json.loads(DATA_FILE.read_text())
        addrs = data.get("addresses", [])
        known_addresses.update(addrs)
        return len(addrs)
    except Exception:
        return 0


# ===== REST APIで過去の約定を取得 =====

async def fetch_recent_trades(session):
    """REST APIで直近の約定を取得しアドレスを収集"""
    collected = 0
    try:
        # userFills は使えないが、wsTradesのスナップショットを取得する方法として
        # フロントエンドが使う内部APIを試す
        # 公式: POST /info { "type": "recentTrades", "coin": "xyz:SILVER" }
        # ※ builder-deployed perpでは "coin" にフルネーム必要
        payload = {"type": "recentTrades", "coin": COIN}
        async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                trades = await resp.json()
                if isinstance(trades, list):
                    for t in trades:
                        users = t.get("users", [])
                        for u in users:
                            if u and u != "?" and len(u) > 10:
                                known_addresses.add(u)
                                collected += 1
    except Exception as e:
        pass

    # userFillsByTime で既知アドレスの取引相手を発見
    # (既にアドレスがあれば、その取引相手も収集)
    try:
        for addr in list(known_addresses)[:20]:  # 最初の20件
            payload = {
                "type": "userFillsByTime",
                "user": addr,
                "startTime": int((time.time() - 86400) * 1000),  # 24h前
                "endTime": int(time.time() * 1000),
            }
            async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    fills = await resp.json()
                    if isinstance(fills, list):
                        for f in fills:
                            if f.get("coin") == COIN:
                                users = f.get("users", [])
                                for u in users:
                                    if u and u != "?" and len(u) > 10:
                                        known_addresses.add(u)
                                        collected += 1
            await asyncio.sleep(0.2)  # レートリミット配慮
    except Exception:
        pass

    return collected


# ===== ポジション取得 =====

async def fetch_position(session, address):
    try:
        payload = {"type": "clearinghouseState", "user": address, "dex": DEX}
        async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data and "assetPositions" in data:
                for p in data["assetPositions"]:
                    if p.get("position", {}).get("coin") == COIN:
                        return {
                            "size": float(p["position"].get("szi", 0)),
                            "entry_px": float(p["position"].get("entryPx", "0")),
                            "upnl": float(p["position"].get("unrealizedPnl", "0")),
                            "liq_px": float(p["position"].get("liquidationPx", "0") or "0"),
                            "leverage": p["position"].get("leverage", {}),
                        }
            return {"size": 0, "entry_px": 0, "upnl": 0, "liq_px": 0, "leverage": {}}
    except Exception:
        return None


async def update_all_positions(session):
    """全アドレスのポジションを一括更新（並列, 10件ずつ）"""
    addrs = [a for a in known_addresses
             if a not in positions or time.time() - positions.get(a, {}).get("updated", 0) >= POSITION_TTL]

    if not addrs:
        return

    # 10件ずつバッチ処理（APIレートリミット配慮）
    for i in range(0, len(addrs), 10):
        batch = addrs[i:i+10]
        tasks = [fetch_position(session, a) for a in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for addr, result in zip(batch, results):
            if isinstance(result, Exception) or result is None:
                continue
            positions[addr] = {**result, "updated": time.time()}

        if i + 10 < len(addrs):
            await asyncio.sleep(0.5)


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
    print(f"\033[96m{'='*84}\033[0m")
    print(f"\033[96m  xyz:SILVER Position Ranking & Trade Monitor\033[0m")
    print(f"\033[90m  {now}  |  Addresses: {len(known_addresses)}  |  "
          f"Positions: {sum(1 for d in positions.values() if d['size'] != 0)}  |  "
          f"Trades: {trade_count}  |  "
          f"Saved: {DATA_FILE.name}\033[0m")
    print(f"\033[96m{'='*84}\033[0m")

    # ランキング
    ranked = [(a, d) for a, d in positions.items() if d["size"] != 0]
    ranked.sort(key=lambda x: abs(x[1]["size"]), reverse=True)

    longs = [r for r in ranked if r[1]["size"] > 0]
    shorts = [r for r in ranked if r[1]["size"] < 0]
    total_long = sum(d["size"] for _, d in longs)
    total_short = sum(abs(d["size"]) for _, d in shorts)

    print()
    print(f"\033[93m  ── Position Ranking ({'─'*56})\033[0m")
    print(f"\033[90m  LONG: \033[92m{len(longs)} ({total_long:+.2f})\033[90m  |  "
          f"SHORT: \033[91m{len(shorts)} ({-total_short:.2f})\033[90m  |  "
          f"Ratio: \033[0m{total_long/(total_long+total_short)*100:.0f}%L / {total_short/(total_long+total_short)*100:.0f}%S"
          if (total_long + total_short) > 0 else "")
    print()
    print(f"\033[90m  {'#':>3}  {'Address':<15} {'Position':>12} {'Side':>6} "
          f"{'Entry':>10} {'uPnL':>12} {'Lev':>5}\033[0m")
    print(f"\033[90m  {'─'*75}\033[0m")

    if not ranked:
        print(f"\033[90m  Collecting addresses... (takes ~30s after start)\033[0m")
    else:
        for i, (addr, data) in enumerate(ranked[:25], 1):
            size = data["size"]
            col = "\033[92m" if size > 0 else "\033[91m"
            side = "LONG" if size > 0 else "SHORT"
            entry = f"${data['entry_px']:.3f}" if data["entry_px"] > 0 else "-"
            upnl = data["upnl"]
            upnl_col = "\033[92m" if upnl >= 0 else "\033[91m"

            lev_info = data.get("leverage", {})
            lev = ""
            if isinstance(lev_info, dict) and lev_info.get("value"):
                lev = f"{lev_info['value']}x"

            print(f"  {i:>3}  {short_addr(addr):<15} "
                  f"{col}{size:>+12.4f}\033[0m "
                  f"{col}{side:>6}\033[0m "
                  f"{entry:>10} "
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
    print(f"\033[90m  {'─'*75}\033[0m")

    if not last_trades:
        print(f"\033[90m  Waiting for trades...\033[0m")
    else:
        for t in last_trades[-15:]:
            ts = datetime.fromtimestamp(t["time"] / 1000).strftime("%H:%M:%S")
            val = t["price"] * t["size"]
            is_buy = t["side"] == "A"
            col = "\033[92m" if is_buy else "\033[91m"
            side = "BUY" if is_buy else "SELL"
            val_s = f"${val:.2f}" if val < 1000 else f"${val/1000:.1f}K"

            print(f"  {ts:<10} "
                  f"{'$'+f'{t['price']:.3f}':>10} "
                  f"{t['size']:>10.4f} "
                  f"{val_s:>10} "
                  f"{col}{side:>5}\033[0m  "
                  f"{short_addr(t['buyer']):<13} "
                  f"{short_addr(t['seller']):<13}")

    print(f"\n\033[90m  Ctrl+C to exit  |  Auto-save every 30s  |  Refresh every 5s\033[0m")


# ===== メイン =====

async def main():
    global trade_count

    last_trades = []

    # 保存済みアドレス読み込み
    loaded = load_addresses()
    print(f"\033[96m  Loaded {loaded} saved addresses from {DATA_FILE.name}\033[0m")

    async with aiohttp.ClientSession() as session:
        # 起動時に過去の約定からアドレス収集
        print(f"\033[90m  Fetching recent trades to collect addresses...\033[0m")
        collected = await fetch_recent_trades(session)
        print(f"\033[96m  Collected {collected} addresses from recent trades\033[0m")
        print(f"\033[96m  Total known: {len(known_addresses)} addresses\033[0m")

        # 初回ポジション取得
        print(f"\033[90m  Fetching positions for all addresses...\033[0m")
        await update_all_positions(session)
        save_addresses()
        print(f"\033[92m  Ready! Connecting to WebSocket...\033[0m")

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

                        trades = data.get("data", [])
                        new_addr = False
                        for trade in trades:
                            trade_count += 1
                            users = trade.get("users", ["?", "?"])
                            buyer, seller = users[0], users[1]

                            if buyer not in known_addresses:
                                known_addresses.add(buyer)
                                new_addr = True
                            if seller not in known_addresses:
                                known_addresses.add(seller)
                                new_addr = True

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

                        # 5秒ごとに画面更新 + ポジション更新
                        if now - last_update >= 5:
                            await update_all_positions(session)
                            render(last_trades)
                            last_update = now

                        # 30秒ごとに保存
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

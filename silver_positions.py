"""
xyz:SILVER ポジションランキング & リアルタイム約定モニター
- 約定から自動的にアドレスを収集
- 各アドレスのポジションを取得してランキング表示
- 定期的にランキング更新
"""

import asyncio
import json
import time
import os
from datetime import datetime

try:
    import websockets
except ImportError:
    print("websockets をインストールしてください: pip install websockets")
    exit(1)

try:
    import aiohttp
except ImportError:
    print("aiohttp をインストールしてください: pip install aiohttp")
    exit(1)

COIN = "xyz:SILVER"
DEX = "xyz"
WS_URL = "wss://api.hyperliquid.xyz/ws"
API_URL = "https://api.hyperliquid.xyz/info"

# 収集したアドレスとポジション
known_addresses = set()
positions = {}  # address -> { position, last_updated }
trade_count = 0
POSITION_TTL = 60  # 60秒キャッシュ


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


async def fetch_position(session, address):
    """アドレスのxyz:SILVERポジションを取得"""
    try:
        payload = {"type": "clearinghouseState", "user": address, "dex": DEX}
        async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data and "assetPositions" in data:
                for p in data["assetPositions"]:
                    if p.get("position", {}).get("coin") == COIN:
                        szi = float(p["position"].get("szi", 0))
                        entry_px = p["position"].get("entryPx", "0")
                        unrealized_pnl = p["position"].get("unrealizedPnl", "0")
                        return {
                            "size": szi,
                            "entry_px": float(entry_px),
                            "upnl": float(unrealized_pnl),
                        }
            return {"size": 0, "entry_px": 0, "upnl": 0}
    except Exception as e:
        return None


async def update_all_positions(session):
    """全アドレスのポジションを更新"""
    tasks = []
    addrs = list(known_addresses)
    for addr in addrs:
        cached = positions.get(addr)
        if cached and time.time() - cached.get("updated", 0) < POSITION_TTL:
            continue
        tasks.append((addr, fetch_position(session, addr)))

    if not tasks:
        return

    results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)
    for i, (addr, _) in enumerate(tasks):
        result = results[i]
        if isinstance(result, Exception) or result is None:
            continue
        positions[addr] = {
            "size": result["size"],
            "entry_px": result["entry_px"],
            "upnl": result["upnl"],
            "updated": time.time(),
        }


def get_ranking():
    """ポジションランキングを取得"""
    ranked = []
    for addr, data in positions.items():
        if data["size"] != 0:
            ranked.append((addr, data))

    # ポジション量の絶対値でソート
    ranked.sort(key=lambda x: abs(x[1]["size"]), reverse=True)
    return ranked


def short_addr(addr):
    if not addr or len(addr) < 12:
        return addr or "?"
    return addr[:6] + "..." + addr[-4:]


def render(last_trades):
    """画面描画"""
    clear_screen()

    now = datetime.now().strftime("%H:%M:%S")
    print(f"\033[96m{'='*80}\033[0m")
    print(f"\033[96m  xyz:SILVER Position Ranking & Trade Monitor\033[0m")
    print(f"\033[90m  {now}  |  Addresses: {len(known_addresses)}  |  Trades: {trade_count}\033[0m")
    print(f"\033[96m{'='*80}\033[0m")

    # ポジションランキング
    ranking = get_ranking()
    print()
    print(f"\033[93m  ── Position Ranking (Top 20) {'─'*47}\033[0m")
    print(f"\033[90m  {'#':>3}  {'Address':<15} {'Position':>12} {'Side':>6} {'Entry':>10} {'uPnL':>12}\033[0m")
    print(f"\033[90m  {'─'*70}\033[0m")

    if not ranking:
        print(f"\033[90m  Collecting addresses from trades...\033[0m")
    else:
        for i, (addr, data) in enumerate(ranking[:20], 1):
            size = data["size"]
            if size > 0:
                side_str = f"\033[92m{'LONG':>6}\033[0m"
                size_str = f"\033[92m{size:>+12.4f}\033[0m"
            else:
                side_str = f"\033[91m{'SHORT':>6}\033[0m"
                size_str = f"\033[91m{size:>+12.4f}\033[0m"

            entry = f"${data['entry_px']:.3f}" if data["entry_px"] > 0 else "-"
            upnl = data["upnl"]
            if upnl >= 0:
                upnl_str = f"\033[92m${upnl:>+11.2f}\033[0m"
            else:
                upnl_str = f"\033[91m${upnl:>+11.2f}\033[0m"

            print(f"  {i:>3}  {short_addr(addr):<15} {size_str} {side_str} {entry:>10} {upnl_str}")

    # ゼロポジション数
    zero_count = sum(1 for a, d in positions.items() if d["size"] == 0)
    if zero_count > 0:
        print(f"\033[90m\n  ({zero_count} addresses with 0 position)\033[0m")

    # 最新約定
    print()
    print(f"\033[93m  ── Recent Trades {'─'*58}\033[0m")
    print(f"\033[90m  {'Time':<12} {'Price':>10} {'Size':>10} {'Value':>10} {'Side':>6}  {'Buyer':<13} {'Seller':<13}\033[0m")
    print(f"\033[90m  {'─'*70}\033[0m")

    if not last_trades:
        print(f"\033[90m  Waiting for trades...\033[0m")
    else:
        for t in last_trades[-15:]:
            time_str = datetime.fromtimestamp(t["time"] / 1000).strftime("%H:%M:%S")
            price = t["price"]
            size = t["size"]
            value = price * size
            is_buy = t["side"] == "A"

            if is_buy:
                side_str = f"\033[92m{'BUY':>6}\033[0m"
            else:
                side_str = f"\033[91m{'SELL':>6}\033[0m"

            val_str = f"${value:.2f}" if value < 1000 else f"${value/1000:.1f}K"

            print(
                f"  {time_str:<12} "
                f"{'$'+f'{price:.3f}':>10} "
                f"{size:>10.4f} "
                f"{val_str:>10} "
                f"{side_str}  "
                f"{short_addr(t['buyer']):<13} "
                f"{short_addr(t['seller']):<13}"
            )

    print(f"\n\033[90m  Ctrl+C to exit  |  Positions refresh every {POSITION_TTL}s\033[0m")


async def main():
    global trade_count

    last_trades = []

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(WS_URL) as ws_conn:
                    # Subscribe
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
                        for trade in trades:
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

                            # 最新100件だけ保持
                            if len(last_trades) > 100:
                                last_trades = last_trades[-100:]

                        # 定期的にポジション更新 & 画面更新
                        now = time.time()
                        if now - last_update >= 5:
                            await update_all_positions(session)
                            render(last_trades)
                            last_update = now

            except (websockets.exceptions.ConnectionClosed, ConnectionError):
                print("\n\033[91m  Connection lost. Reconnecting in 3s...\033[0m")
                await asyncio.sleep(3)
            except KeyboardInterrupt:
                print("\n\033[93m  Stopped.\033[0m")
                return


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\033[93m  Stopped.\033[0m")

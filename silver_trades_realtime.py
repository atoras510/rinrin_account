#!/usr/bin/env python3
"""
Hyperliquid Silver (SILVER) リアルタイム約定履歴取得

WebSocketを使用してSILVERの約定履歴をリアルタイムで取得し、
buyer/sellerのアドレスを含めて表示します。
"""

import asyncio
import json
import signal
import sys
from datetime import datetime
from typing import Any

import websockets


# Hyperliquid WebSocket URL
MAINNET_WS_URL = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

# 使用するネットワーク
WS_URL = MAINNET_WS_URL

# 購読する銘柄（Builder-deployed perp）
COIN = "xyz:SILVER"


class SilverTradesMonitor:
    """Silverの約定履歴をリアルタイムで監視するクラス"""

    def __init__(self, ws_url: str = WS_URL, coin: str = COIN):
        self.ws_url = ws_url
        self.coin = coin
        self.ws = None
        self.running = False
        self.trade_count = 0

    async def connect(self) -> None:
        """WebSocket接続を確立"""
        print(f"Connecting to {self.ws_url}...")
        self.ws = await websockets.connect(self.ws_url)
        print("Connected!")

    async def subscribe_trades(self) -> None:
        """tradesチャンネルを購読"""
        subscription = {
            "method": "subscribe",
            "subscription": {
                "type": "trades",
                "coin": self.coin
            }
        }
        await self.ws.send(json.dumps(subscription))
        print(f"Subscribed to {self.coin} trades")

    def format_trade(self, trade: dict[str, Any]) -> str:
        """約定データをフォーマット"""
        # タイムスタンプをdatetimeに変換
        timestamp = datetime.fromtimestamp(trade["time"] / 1000)
        time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # サイドの表示
        side = "BUY " if trade["side"] == "A" else "SELL"

        # buyer/sellerアドレス
        users = trade.get("users", ["unknown", "unknown"])
        buyer = users[0] if len(users) > 0 else "unknown"
        seller = users[1] if len(users) > 1 else "unknown"

        # アドレスを短縮表示（先頭6文字...末尾4文字）
        def shorten_addr(addr: str) -> str:
            if addr and len(addr) > 12:
                return f"{addr[:6]}...{addr[-4:]}"
            return addr

        return (
            f"[{time_str}] {self.coin} {side} | "
            f"Price: ${float(trade['px']):,.4f} | "
            f"Size: {float(trade['sz']):,.4f} | "
            f"Buyer: {shorten_addr(buyer)} | "
            f"Seller: {shorten_addr(seller)} | "
            f"TxHash: {trade.get('hash', 'N/A')[:16]}..."
        )

    def print_trade_detail(self, trade: dict[str, Any]) -> None:
        """約定の詳細情報を出力"""
        self.trade_count += 1
        users = trade.get("users", ["unknown", "unknown"])

        print("\n" + "=" * 80)
        print(f"Trade #{self.trade_count}")
        print("-" * 80)
        print(f"  Coin      : {trade.get('coin', self.coin)}")
        print(f"  Side      : {'BUY (Taker bought)' if trade['side'] == 'A' else 'SELL (Taker sold)'}")
        print(f"  Price     : ${float(trade['px']):,.4f}")
        print(f"  Size      : {float(trade['sz']):,.4f}")
        print(f"  Value     : ${float(trade['px']) * float(trade['sz']):,.2f}")
        print(f"  Time      : {datetime.fromtimestamp(trade['time'] / 1000)}")
        print(f"  Trade ID  : {trade.get('tid', 'N/A')}")
        print(f"  Tx Hash   : {trade.get('hash', 'N/A')}")
        print("-" * 40)
        print(f"  Buyer     : {users[0] if len(users) > 0 else 'unknown'}")
        print(f"  Seller    : {users[1] if len(users) > 1 else 'unknown'}")
        print("=" * 80)

    async def handle_message(self, message: str) -> None:
        """受信メッセージを処理"""
        try:
            data = json.loads(message)
            channel = data.get("channel")

            if channel == "subscriptionResponse":
                print(f"Subscription confirmed: {data.get('data', {}).get('subscription', {})}")

            elif channel == "trades":
                trades = data.get("data", [])
                for trade in trades:
                    # 簡易表示
                    print(self.format_trade(trade))
                    # 詳細表示（コメントアウトを外すと詳細表示）
                    # self.print_trade_detail(trade)

            elif channel == "error":
                print(f"Error: {data}")

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")

    async def run(self) -> None:
        """メインループを実行"""
        self.running = True

        try:
            await self.connect()
            await self.subscribe_trades()

            print("\n" + "=" * 80)
            print(f"Monitoring {self.coin} trades in real-time...")
            print("Press Ctrl+C to stop")
            print("=" * 80 + "\n")

            while self.running:
                try:
                    message = await asyncio.wait_for(
                        self.ws.recv(),
                        timeout=30.0  # 30秒タイムアウト（ハートビート用）
                    )
                    await self.handle_message(message)
                except asyncio.TimeoutError:
                    # 接続維持のためpingを送信
                    await self.ws.ping()

        except websockets.ConnectionClosed as e:
            print(f"Connection closed: {e}")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await self.close()

    async def close(self) -> None:
        """接続を閉じる"""
        self.running = False
        if self.ws:
            await self.ws.close()
            print("\nConnection closed.")


async def main():
    """エントリーポイント"""
    monitor = SilverTradesMonitor(
        ws_url=MAINNET_WS_URL,
        coin=COIN
    )

    # Ctrl+Cハンドリング
    loop = asyncio.get_event_loop()

    def signal_handler():
        print("\nShutting down...")
        monitor.running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await monitor.run()


if __name__ == "__main__":
    print("=" * 80)
    print("Hyperliquid Silver Trades Real-time Monitor")
    print("=" * 80)
    asyncio.run(main())

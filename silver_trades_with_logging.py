#!/usr/bin/env python3
"""
Hyperliquid Silver リアルタイム約定履歴取得（CSV保存機能付き）

WebSocketを使用してSILVERの約定履歴をリアルタイムで取得し、
buyer/sellerのアドレスを含めてCSVファイルに保存します。
"""

import asyncio
import csv
import json
import os
import signal
from datetime import datetime
from pathlib import Path
from typing import Any

import websockets


# Hyperliquid WebSocket URL
MAINNET_WS_URL = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

# 設定
WS_URL = MAINNET_WS_URL
COIN = "xyz:SILVER"  # Builder-deployed perp銘柄
OUTPUT_DIR = Path("./trade_logs")
DETAILED_OUTPUT = True  # Trueで詳細表示


class SilverTradesLogger:
    """Silverの約定履歴をリアルタイムで監視・保存するクラス"""

    def __init__(
        self,
        ws_url: str = WS_URL,
        coin: str = COIN,
        output_dir: Path = OUTPUT_DIR,
        detailed_output: bool = DETAILED_OUTPUT
    ):
        self.ws_url = ws_url
        self.coin = coin
        self.output_dir = output_dir
        self.detailed_output = detailed_output
        self.ws = None
        self.running = False
        self.trade_count = 0
        self.csv_file = None
        self.csv_writer = None
        self.file_handle = None

    def setup_csv(self) -> None:
        """CSV出力ファイルをセットアップ"""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.output_dir / f"{self.coin}_trades_{timestamp}.csv"

        self.file_handle = open(filename, "w", newline="", encoding="utf-8")
        self.csv_writer = csv.writer(self.file_handle)

        # ヘッダー行
        self.csv_writer.writerow([
            "timestamp",
            "datetime",
            "coin",
            "side",
            "price",
            "size",
            "value_usd",
            "buyer_address",
            "seller_address",
            "trade_id",
            "tx_hash"
        ])

        print(f"CSV logging to: {filename}")

    async def connect(self) -> None:
        """WebSocket接続を確立"""
        print(f"Connecting to {self.ws_url}...")
        self.ws = await websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=10
        )
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

    def process_trade(self, trade: dict[str, Any]) -> dict[str, Any]:
        """約定データを処理して辞書形式で返す"""
        timestamp_ms = trade.get("time", 0)
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000)

        users = trade.get("users", ["", ""])
        buyer = users[0] if len(users) > 0 else ""
        seller = users[1] if len(users) > 1 else ""

        price = float(trade.get("px", 0))
        size = float(trade.get("sz", 0))

        return {
            "timestamp_ms": timestamp_ms,
            "datetime": timestamp,
            "coin": trade.get("coin", self.coin),
            "side": "BUY" if trade.get("side") == "A" else "SELL",
            "side_raw": trade.get("side"),
            "price": price,
            "size": size,
            "value_usd": price * size,
            "buyer": buyer,
            "seller": seller,
            "trade_id": trade.get("tid", ""),
            "tx_hash": trade.get("hash", "")
        }

    def log_to_csv(self, trade_data: dict[str, Any]) -> None:
        """約定データをCSVに書き込み"""
        if self.csv_writer:
            self.csv_writer.writerow([
                trade_data["timestamp_ms"],
                trade_data["datetime"].isoformat(),
                trade_data["coin"],
                trade_data["side"],
                trade_data["price"],
                trade_data["size"],
                trade_data["value_usd"],
                trade_data["buyer"],
                trade_data["seller"],
                trade_data["trade_id"],
                trade_data["tx_hash"]
            ])
            self.file_handle.flush()  # 即時書き込み

    def print_trade(self, trade_data: dict[str, Any]) -> None:
        """約定データを表示"""
        self.trade_count += 1

        if self.detailed_output:
            self._print_detailed(trade_data)
        else:
            self._print_compact(trade_data)

    def _print_compact(self, td: dict[str, Any]) -> None:
        """コンパクト表示"""
        time_str = td["datetime"].strftime("%H:%M:%S.%f")[:-3]

        def shorten(addr: str) -> str:
            return f"{addr[:6]}...{addr[-4:]}" if len(addr) > 12 else addr

        print(
            f"[{time_str}] {td['coin']} {td['side']:4} | "
            f"${td['price']:,.4f} x {td['size']:,.4f} = ${td['value_usd']:,.2f} | "
            f"B:{shorten(td['buyer'])} S:{shorten(td['seller'])}"
        )

    def _print_detailed(self, td: dict[str, Any]) -> None:
        """詳細表示"""
        print("\n" + "=" * 90)
        print(f"Trade #{self.trade_count}")
        print("-" * 90)
        print(f"  Time       : {td['datetime']} ({td['timestamp_ms']})")
        print(f"  Coin       : {td['coin']}")
        print(f"  Side       : {td['side']} (Taker {'bought' if td['side'] == 'BUY' else 'sold'})")
        print(f"  Price      : ${td['price']:,.6f}")
        print(f"  Size       : {td['size']:,.6f}")
        print(f"  Value      : ${td['value_usd']:,.2f}")
        print("-" * 45)
        print(f"  Buyer      : {td['buyer']}")
        print(f"  Seller     : {td['seller']}")
        print("-" * 45)
        print(f"  Trade ID   : {td['trade_id']}")
        print(f"  Tx Hash    : {td['tx_hash']}")
        print("=" * 90)

    async def handle_message(self, message: str) -> None:
        """受信メッセージを処理"""
        try:
            data = json.loads(message)
            channel = data.get("channel")

            if channel == "subscriptionResponse":
                sub_data = data.get("data", {})
                print(f"Subscription confirmed: {sub_data}")

                # エラーチェック
                if "error" in str(sub_data).lower():
                    print(f"\n⚠️  Warning: Subscription may have failed!")
                    print("   Please verify the coin name using check_available_coins.py")

            elif channel == "trades":
                trades = data.get("data", [])
                for trade in trades:
                    trade_data = self.process_trade(trade)
                    self.print_trade(trade_data)
                    self.log_to_csv(trade_data)

            elif channel == "error":
                print(f"\n❌ Error from server: {data}")

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")

    async def run(self) -> None:
        """メインループを実行"""
        self.running = True
        self.setup_csv()

        try:
            await self.connect()
            await self.subscribe_trades()

            print("\n" + "=" * 90)
            print(f"📊 Monitoring {self.coin} trades in real-time...")
            print(f"📁 Logging to: {self.output_dir}")
            print("Press Ctrl+C to stop")
            print("=" * 90 + "\n")

            while self.running:
                try:
                    message = await asyncio.wait_for(
                        self.ws.recv(),
                        timeout=30.0
                    )
                    await self.handle_message(message)
                except asyncio.TimeoutError:
                    # 接続維持
                    try:
                        await self.ws.ping()
                    except Exception:
                        print("Connection lost, reconnecting...")
                        await self.reconnect()

        except websockets.ConnectionClosed as e:
            print(f"Connection closed: {e}")
            if self.running:
                await self.reconnect()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await self.close()

    async def reconnect(self) -> None:
        """再接続を試行"""
        print("Attempting to reconnect...")
        await asyncio.sleep(2)
        try:
            await self.connect()
            await self.subscribe_trades()
            print("Reconnected successfully!")
        except Exception as e:
            print(f"Reconnection failed: {e}")

    async def close(self) -> None:
        """接続とファイルを閉じる"""
        self.running = False

        if self.ws:
            await self.ws.close()

        if self.file_handle:
            self.file_handle.close()

        print(f"\n✅ Closed. Total trades logged: {self.trade_count}")


async def main():
    """エントリーポイント"""
    # 設定
    coin = os.environ.get("HYPERLIQUID_COIN", COIN)
    detailed = os.environ.get("DETAILED_OUTPUT", "true").lower() == "true"

    logger = SilverTradesLogger(
        ws_url=MAINNET_WS_URL,
        coin=coin,
        output_dir=OUTPUT_DIR,
        detailed_output=detailed
    )

    # シグナルハンドリング
    loop = asyncio.get_event_loop()

    def signal_handler():
        print("\n\n⏹️  Shutting down...")
        logger.running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await logger.run()


if __name__ == "__main__":
    print("=" * 90)
    print("Hyperliquid Trades Real-time Monitor & Logger")
    print("=" * 90)
    print(f"\nCoin: {COIN}")
    print(f"Output: {OUTPUT_DIR}")
    print("\nTip: Run 'python check_available_coins.py' to verify available coins")
    print()

    asyncio.run(main())

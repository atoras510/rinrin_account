# Hyperliquid Silver Trades Monitor

Hyperliquidの約定履歴をリアルタイムで取得し、buyer/sellerのアドレスを含めて表示・保存するツール。

## セットアップ

```bash
pip install -r requirements.txt
```

## 使い方

### 1. 利用可能な銘柄を確認

```bash
python check_available_coins.py
```

SILVERやその他の銘柄が存在するか確認できます。

### 2. リアルタイム約定モニター（シンプル版）

```bash
python silver_trades_realtime.py
```

### 3. リアルタイム約定モニター（CSV保存付き）

```bash
python silver_trades_with_logging.py
```

約定履歴は `trade_logs/` ディレクトリにCSV形式で保存されます。

### 環境変数で銘柄を変更

```bash
HYPERLIQUID_COIN=BTC python silver_trades_with_logging.py
```

## 出力例

```
[14:32:15.123] SILVER BUY  | $32.4500 x 10.0000 = $324.50 | B:0x1234...5678 S:0xabcd...ef01
```

### 詳細表示

```
================================================================================
Trade #1
--------------------------------------------------------------------------------
  Time       : 2024-01-15 14:32:15.123000 (1705312335123)
  Coin       : SILVER
  Side       : BUY (Taker bought)
  Price      : $32.450000
  Size       : 10.000000
  Value      : $324.50
---------------------------------------------
  Buyer      : 0x1234567890abcdef1234567890abcdef12345678
  Seller     : 0xabcdef1234567890abcdef1234567890abcdef01
---------------------------------------------
  Trade ID   : 12345678901234567
  Tx Hash    : 0xabcdef...
================================================================================
```

## CSV出力フォーマット

| カラム | 説明 |
|--------|------|
| timestamp | UNIXタイムスタンプ（ミリ秒） |
| datetime | ISO形式日時 |
| coin | 銘柄名 |
| side | BUY/SELL |
| price | 約定価格 |
| size | 約定数量 |
| value_usd | 約定金額（USD） |
| buyer_address | 買い手アドレス |
| seller_address | 売り手アドレス |
| trade_id | トレードID |
| tx_hash | トランザクションハッシュ |

## API仕様

- WebSocket URL: `wss://api.hyperliquid.xyz/ws`
- tradesサブスクリプションの`users`フィールドに`[buyer, seller]`が含まれる

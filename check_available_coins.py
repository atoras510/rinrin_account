#!/usr/bin/env python3
"""
Hyperliquidで利用可能な銘柄一覧を取得するユーティリティ

SILVERが存在するか確認し、正確な銘柄名を特定します。
"""

import requests

MAINNET_API_URL = "https://api.hyperliquid.xyz"


def get_perp_meta() -> dict:
    """パーペチュアル市場のメタデータを取得"""
    response = requests.post(
        f"{MAINNET_API_URL}/info",
        json={"type": "meta"},
        headers={"Content-Type": "application/json"}
    )
    return response.json()


def get_spot_meta() -> dict:
    """スポット市場のメタデータを取得"""
    response = requests.post(
        f"{MAINNET_API_URL}/info",
        json={"type": "spotMeta"},
        headers={"Content-Type": "application/json"}
    )
    return response.json()


def search_coin(keyword: str) -> None:
    """キーワードで銘柄を検索"""
    keyword_lower = keyword.lower()

    print(f"\nSearching for '{keyword}'...\n")
    print("=" * 60)

    # パーペチュアル市場を検索
    print("\n[Perpetual Markets]")
    print("-" * 60)
    perp_meta = get_perp_meta()
    perp_found = []

    for idx, asset in enumerate(perp_meta.get("universe", [])):
        name = asset.get("name", "")
        if keyword_lower in name.lower():
            perp_found.append({
                "index": idx,
                "name": name,
                "szDecimals": asset.get("szDecimals"),
                "maxLeverage": asset.get("maxLeverage")
            })

    if perp_found:
        for coin in perp_found:
            print(f"  [{coin['index']:3d}] {coin['name']:<12} | "
                  f"szDecimals: {coin['szDecimals']} | "
                  f"maxLeverage: {coin['maxLeverage']}x")
    else:
        print("  No matching perpetuals found.")

    # スポット市場を検索
    print("\n[Spot Markets]")
    print("-" * 60)
    spot_meta = get_spot_meta()
    spot_found = []

    for token in spot_meta.get("tokens", []):
        name = token.get("name", "")
        if keyword_lower in name.lower():
            spot_found.append({
                "index": token.get("index"),
                "name": name,
                "szDecimals": token.get("szDecimals"),
                "weiDecimals": token.get("weiDecimals")
            })

    if spot_found:
        for token in spot_found:
            print(f"  [{token['index']:5d}] {token['name']:<12} | "
                  f"szDecimals: {token['szDecimals']} | "
                  f"weiDecimals: {token['weiDecimals']}")
    else:
        print("  No matching spot tokens found.")

    print("\n" + "=" * 60)


def list_all_perp_coins() -> list[str]:
    """全パーペチュアル銘柄をリスト"""
    perp_meta = get_perp_meta()
    coins = []

    print("\n[All Perpetual Markets]")
    print("=" * 80)

    for idx, asset in enumerate(perp_meta.get("universe", [])):
        name = asset.get("name", "")
        coins.append(name)
        print(f"  [{idx:3d}] {name:<12} | "
              f"szDecimals: {asset.get('szDecimals')} | "
              f"maxLeverage: {asset.get('maxLeverage')}x")

    print(f"\nTotal: {len(coins)} perpetual markets")
    return coins


def main():
    print("=" * 60)
    print("Hyperliquid Available Coins Checker")
    print("=" * 60)

    # SILVERを検索
    search_coin("SILVER")

    # 関連する可能性のある銘柄も検索
    search_coin("XAG")  # 銀の通貨コード
    search_coin("METAL")
    search_coin("GOLD")  # 参考：金があれば銀もある可能性

    # インタラクティブ検索
    print("\n" + "=" * 60)
    print("Interactive Search (type 'list' to see all, 'quit' to exit)")
    print("=" * 60)

    while True:
        query = input("\nEnter coin name to search: ").strip()
        if query.lower() == "quit":
            break
        elif query.lower() == "list":
            list_all_perp_coins()
        elif query:
            search_coin(query)


if __name__ == "__main__":
    main()

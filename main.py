import asyncio
import websockets
import json
import os
import sqlite3
import requests
from telegram import Bot
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def init_db():
    conn = sqlite3.connect('whales.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS whales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT NOT NULL,
        pair TEXT NOT NULL,
        token0_symbol TEXT,
        token1_symbol TEXT,
        amount_usd REAL,
        token_amount REAL,
        entry_time TEXT,
        last_notified TEXT,
        tracked INTEGER DEFAULT 1
    )''')
    conn.commit()
    conn.close()

def check_notification_cooldown(address, pair):
    conn = sqlite3.connect('whales.db')
    c = conn.cursor()
    c.execute('SELECT last_notified FROM whales WHERE address = ? AND pair = ?', (address, pair))
    last_notified = c.fetchone()
    conn.close()
    if last_notified and (datetime.now() - datetime.fromisoformat(last_notified[0])).seconds < 3600:
        return False
    return True

async def check_liquidity(pair_contract):
    url = "https://graphql.bitquery.io/"
    query = """query ($pair_contract: String!) {
        EVM(network: bsc) {
            Pools(where: {Pair: {SmartContract: {is: $pair_contract}}}) {
                Liquidity { Value }
            }
        }
    }"""
    headers = {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}
    variables = {"pair_contract": pair_contract}
    try:
        response = requests.post(url, json={"query": query, "variables": variables}, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data['data']['EVM']['Pools'][0]['Liquidity']['Value'] if data['data']['EVM']['Pools'] else 0
    except Exception as e:
        print(f"Liquidity check error: {e}")
        return 0

async def bitquery_websocket():
    uri = "wss://streaming.bitquery.io/graphql"
    headers = {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}
    try:
        async with websockets.connect(uri, extra_headers=headers) as ws:
            query = """subscription {
                EVM(network: bsc) {
                    DEXTrades(where: {Trade: {Buy: {AmountInUSD: {gt: 100000}}}}) {
                        Transaction { Hash }
                        Trade {
                            Buyer { Address }
                            AmountInUSD
                            Pair { SmartContract Token0 { Symbol } Token1 { Symbol } }
                            Amount
                        }
                    }
                }
            }"""
            await ws.send(json.dumps({"type": "start", "id": "1", "query": query}))
            async for message in ws:
                data = json.loads(message)
                if data.get('type') == 'data' and data.get('payload', {}).get('data'):
                    trade = data['payload']['data']['EVM']['DEXTrades'][0]
                    liquidity = await check_liquidity(trade['Pair']['SmartContract'])
                    if 50000 <= liquidity <= 200000:
                        pair = f"{trade['Pair']['Token0']['Symbol']}/{trade['Pair']['Token1']['Symbol']}"
                        if check_notification_cooldown(trade['Buyer']['Address'], pair):
                            await send_entry_alert(trade)
                            await save_whale_address(trade)
    except Exception as e:
        print(f"WebSocket error: {e}")
        await asyncio.sleep(60)
        await bitquery_websocket()  # Retry

async def send_entry_alert(trade):
    bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
    pair = f"{trade['Pair']['Token0']['Symbol']}/{trade['Pair']['Token1']['Symbol']}"
    message = f"🐳 BALİNA GİRİŞİ! {trade['Buyer']['Address']} {pair} havuzunda {trade['AmountInUSD']}$ aldı! 🚀"
    await bot.send_message(chat_id=os.getenv('TELEGRAM_CHAT_ID'), text=message)

async def save_whale_address(trade):
    conn = sqlite3.connect('whales.db')
    c = conn.cursor()
    pair = f"{trade['Pair']['Token0']['Symbol']}/{trade['Pair']['Token1']['Symbol']}"
    c.execute('''INSERT INTO whales (address, pair, token0_symbol, token1_symbol, amount_usd, token_amount, entry_time, last_notified, tracked)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (trade['Buyer']['Address'], pair,
               trade['Pair']['Token0']['Symbol'], trade['Pair']['Token1']['Symbol'],
               trade['AmountInUSD'], trade['Amount'],
               datetime.now().isoformat(), datetime.now().isoformat(), 1))
    conn.commit()
    conn.close()

async def check_exit(whale_address, entry_amount, pair):
    uri = "wss://streaming.bitquery.io/graphql"
    headers = {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}
    try:
        async with websockets.connect(uri, extra_headers=headers) as ws:
            query = """subscription ($whale_address: String!) {
                EVM(network: bsc) {
                    DEXTrades(where: {Trade: {Sell: {Seller: {Address: {is: $whale_address}}}}}) {
                        Transaction { Hash }
                        Trade {
                            Seller { Address }
                            AmountInUSD
                            Pair { SmartContract Token0 { Symbol } Token1 { Symbol } }
                            Amount
                        }
                    }
                }
            }"""
            variables = {"whale_address": whale_address}
            await ws.send(json.dumps({"type": "start", "id": "2", "query": query, "variables": variables}))
            async for message in ws:
                data = json.loads(message)
                if data.get('type') == 'data' and data.get('payload', {}).get('data'):
                    trade = data['payload']['data']['EVM']['DEXTrades'][0]
                    if trade['Amount'] >= entry_amount * 0.1 and check_notification_cooldown(trade['Seller']['Address'], pair):
                        await send_exit_alert(trade, entry_amount)
                        update_whale_status(trade['Seller']['Address'])
                        break
    except Exception as e:
        print(f"Exit WebSocket error: {e}")
        await asyncio.sleep(60)
        await check_exit(whale_address, entry_amount, pair)  # Retry

async def send_exit_alert(trade, entry_amount):
    bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
    pair = f"{trade['Pair']['Token0']['Symbol']}/{trade['Pair']['Token1']['Symbol']}"
    sell_amount = trade['Amount']
    remaining = await get_wallet_balance(trade['Seller']['Address'], trade['Pair']['SmartContract'])
    message = f"🚨 BALİNA SATIŞI! {trade['Seller']['Address']} {pair} havuzunda {trade['AmountInUSD']}$ sattı ({sell_amount} token), elinde {remaining}$ kaldı! 🏃‍♂️"
    await bot.send_message(chat_id=os.getenv('TELEGRAM_CHAT_ID'), text=message)

async def get_wallet_balance(address, pair_contract):
    url = "https://graphql.bitquery.io/"
    query = """query ($address: String!, $pair_contract: String!) {
        EVM(network: bsc) {
            Balances(where: {Address: {is: $address}, Currency: {SmartContract: {is: $pair_contract}}}) {
                Balance
            }
        }
    }"""
    headers = {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}
    variables = {"address": address, "pair_contract": pair_contract}
    try:
        response = requests.post(url, json={"query": query, "variables": variables}, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data['data']['EVM']['Balances'][0]['Balance'] if data['data']['EVM']['Balances'] else 0
    except Exception as e:
        print(f"Balance check error: {e}")
        return 0

def update_whale_status(address):
    conn = sqlite3.connect('whales.db')
    c = conn.cursor()
    c.execute('UPDATE whales SET tracked = 0 WHERE address = ?', (address,))
    conn.commit()
    conn.close()

def backup_db():
    conn = sqlite3.connect('whales.db')
    c = conn.cursor()
    c.execute('SELECT * FROM whales')
    rows = c.fetchall()
    with open('whales_backup.json', 'w') as f:
        json.dump(rows, f)
    conn.close()

async def monitor_new_pools():
    uri = "wss://streaming.bitquery.io/graphql"
    headers = {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}
    try:
        async with websockets.connect(uri, extra_headers=headers) as ws:
            query = """subscription {
                EVM(network: bsc) {
                    PairCreated {
                        Pair { SmartContract Token0 { Symbol } Token1 { Symbol } }
                    }
                }
            }"""
            await ws.send(json.dumps({"type": "start", "id": "3", "query": query}))
            async for message in ws:
                data = json.loads(message)
                if data.get('type') == 'data' and data.get('payload', {}).get('data'):
                    pair = data['payload']['data']['EVM']['PairCreated'][0]['Pair']
                    pair_address = pair['SmartContract']
                    pair_name = f"{pair['Token0']['Symbol']}/{pair['Token1']['Symbol']}"
                    liquidity = await check_liquidity(pair_address)
                    if 50000 <= liquidity <= 200000:
                        message = f"🆕 YENİ HAVUZ! {pair_name} oluşturuldu, likidite: {liquidity}$ 🚀"
                        bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
                        await bot.send_message(chat_id=os.getenv('TELEGRAM_CHAT_ID'), text=message)
    except Exception as e:
        print(f"New pools WebSocket error: {e}")
        await asyncio.sleep(60)
        await monitor_new_pools()  # Retry

async def monitor_whales():
    init_db()
    asyncio.create_task(bitquery_websocket())
    asyncio.create_task(monitor_new_pools())
    while True:
        conn = sqlite3.connect('whales.db')
        c = conn.cursor()
        c.execute('SELECT address, token_amount, pair FROM whales WHERE tracked = 1')
        whales = c.fetchall()
        conn.close()
        for whale_address, token_amount, pair in whales:
            await check_exit(whale_address, token_amount, pair)
        backup_db()
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(monitor_whales())

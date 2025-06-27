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
    headers = {
        "Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}",
        "Content-Type": "application/json"
    }
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            async with websockets.connect(uri, extra_headers=headers, subprotocols=["graphql-ws"]) as ws:
                await ws.send(json.dumps({
                    "type": "connection_init",
                    "payload": {"headers": {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}}
                }))
                init_response = await ws.recv()
                print(f"WebSocket init response: {init_response}")
                
                query = """subscription {
                    EVM(network: bsc) {
                        DEXTrades(where: {Trade: {Buy: {AmountInUSD: {gt: "100000"}}}}) {
                            Transaction { Hash }
                            Trade {
                                Buy {
                                    Address
                                    AmountInUSD
                                    Currency { SmartContract Symbol }
                                    BaseCurrency { Symbol }
                                }
                                Amount
                            }
                        }
                    }
                }"""
                await ws.send(json.dumps({
                    "id": "1",
                    "type": "start",
                    "payload": {"query": query}
                }))
                async for message in ws:
                    data = json.loads(message)
                    print(f"WebSocket message: {data}")
                    if data.get('type') == 'data' and data.get('payload', {}).get('data'):
                        trade = data['payload']['data']['EVM']['DEXTrades'][0]
                        liquidity = await check_liquidity(trade['Trade']['Buy']['Currency']['SmartContract'])
                        if 50000 <= liquidity <= 200000:
                            pair = f"{trade['Trade']['Buy']['Currency']['Symbol']}/{trade['Trade']['Buy']['BaseCurrency']['Symbol']}"
                            if check_notification_cooldown(trade['Trade']['Buy']['Address'], pair):
                                await send_entry_alert(trade)
                                await save_whale_address(trade)
        except Exception as e:
            print(f"WebSocket error: {e}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying in 60 seconds... ({retry_count}/{max_retries})")
                await asyncio.sleep(60)
            else:
                print("Max retries reached, stopping WebSocket.")
                break

async def send_entry_alert(trade):
    try:
        bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
        pair = f"{trade['Trade']['Buy']['Currency']['Symbol']}/{trade['Trade']['Buy']['BaseCurrency']['Symbol']}"
        message = f"🐳 BALİNA GİRİŞİ! {trade['Trade']['Buy']['Address']} {pair} havuzunda {trade['Trade']['Buy']['AmountInUSD']}$ aldı! 🚀"
        await bot.send_message(chat_id=os.getenv('TELEGRAM_CHAT_ID'), text=message)
    except Exception as e:
        print(f"Telegram send error: {e}")

async def save_whale_address(trade):
    try:
        conn = sqlite3.connect('whales.db')
        c = conn.cursor()
        pair = f"{trade['Trade']['Buy']['Currency']['Symbol']}/{trade['Trade']['Buy']['BaseCurrency']['Symbol']}"
        c.execute('''INSERT INTO whales (address, pair, token0_symbol, token1_symbol, amount_usd, token_amount, entry_time, last_notified, tracked)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (trade['Trade']['Buy']['Address'], pair,
                   trade['Trade']['Buy']['Currency']['Symbol'], trade['Trade']['Buy']['BaseCurrency']['Symbol'],
                   trade['Trade']['Buy']['AmountInUSD'], trade['Trade']['Amount'],
                   datetime.now().isoformat(), datetime.now().isoformat(), 1))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Database save error: {e}")

async def check_exit(whale_address, entry_amount, pair):
    uri = "wss://streaming.bitquery.io/graphql"
    headers = {
        "Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}",
        "Content-Type": "application/json"
    }
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            async with websockets.connect(uri, extra_headers=headers, subprotocols=["graphql-ws"]) as ws:
                await ws.send(json.dumps({
                    "type": "connection_init",
                    "payload": {"headers": {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}}
                }))
                init_response = await ws.recv()
                print(f"Exit WebSocket init response: {init_response}")
                
                query = """subscription ($whale_address: String!) {
                    EVM(network: bsc) {
                        DEXTrades(where: {Trade: {Sell: {Address: {is: $whale_address}}}}) {
                            Transaction { Hash }
                            Trade {
                                Sell {
                                    Address
                                    AmountInUSD
                                    Currency { SmartContract Symbol }
                                    BaseCurrency { Symbol }
                                }
                                Amount
                            }
                        }
                    }
                }"""
                await ws.send(json.dumps({
                    "id": "2",
                    "type": "start",
                    "payload": {"query": query, "variables": {"whale_address": whale_address}}
                }))
                async for message in ws:
                    data = json.loads(message)
                    print(f"Exit WebSocket message: {data}")
                    if data.get('type') == 'data' and data.get('payload', {}).get('data'):
                        trade = data['payload']['data']['EVM']['DEXTrades'][0]
                        if trade['Trade']['Amount'] >= entry_amount * 0.1 and check_notification_cooldown(trade['Trade']['Sell']['Address'], pair):
                            await send_exit_alert(trade, entry_amount)
                            update_whale_status(trade['Trade']['Sell']['Address'])
                            break
        except Exception as e:
            print(f"Exit WebSocket error: {e}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying exit check in 60 seconds... ({retry_count}/{max_retries})")
                await asyncio.sleep(60)
            else:
                print("Max retries reached for exit check.")
                break

async def send_exit_alert(trade, entry_amount):
    try:
        bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
        pair = f"{trade['Trade']['Sell']['Currency']['Symbol']}/{trade['Trade']['Sell']['BaseCurrency']['Symbol']}"
        sell_amount = trade['Trade']['Amount']
        remaining = await get_wallet_balance(trade['Trade']['Sell']['Address'], trade['Trade']['Sell']['Currency']['SmartContract'])
        message = f"🚨 BALİNA SATIŞI! {trade['Trade']['Sell']['Address']} {pair} havuzunda {trade['Trade']['Sell']['AmountInUSD']}$ sattı ({sell_amount} token), elinde {remaining}$ kaldı! 🏃‍♂️"
        await bot.send_message(chat_id=os.getenv('TELEGRAM_CHAT_ID'), text=message)
    except Exception as e:
        print(f"Telegram exit send error: {e}")

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
    try:
        conn = sqlite3.connect('whales.db')
        c = conn.cursor()
        c.execute('UPDATE whales SET tracked = 0 WHERE address = ?', (address,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Database update error: {e}")

def backup_db():
    try:
        conn = sqlite3.connect('whales.db')
        c = conn.cursor()
        c.execute('SELECT * FROM whales')
        rows = c.fetchall()
        with open('whales_backup.json', 'w') as f:
            json.dump(rows, f)
        conn.close()
    except Exception as e:
        print(f"Database backup error: {e}")

async def monitor_new_pools():
    uri = "wss://streaming.bitquery.io/graphql"
    headers = {
        "Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}",
        "Content-Type": "application/json"
    }
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            async with websockets.connect(uri, extra_headers=headers, subprotocols=["graphql-ws"]) as ws:
                await ws.send(json.dumps({
                    "type": "connection_init",
                    "payload": {"headers": {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}}
                }))
                init_response = await ws.recv()
                print(f"New pools WebSocket init response: {init_response}")
                
                query = """subscription {
                    EVM(network: bsc) {
                        DEXPoolCreated {
                            Pool { SmartContract Token0 { Symbol } Token1 { Symbol } }
                        }
                    }
                }"""
                await ws.send(json.dumps({
                    "id": "3",
                    "type": "start",
                    "payload": {"query": query}
                }))
                async for message in ws:
                    data = json.loads(message)
                    print(f"New pools WebSocket message: {data}")
                    if data.get('type') == 'data' and data.get('payload', {}).get('data'):
                        pair = data['payload']['data']['EVM']['DEXPoolCreated'][0]['Pool']
                        pair_address = pair['SmartContract']
                        pair_name = f"{pair['Token0']['Symbol']}/{pair['Token1']['Symbol']}"
                        liquidity = await check_liquidity(pair_address)
                        if 50000 <= liquidity <= 200000:
                            message = f"🆕 YENİ HAVUZ! {pair_name} oluşturuldu, likidite: {liquidity}$ 🚀"
                            bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
                            await bot.send_message(chat_id=os.getenv('TELEGRAM_CHAT_ID'), text=message)
        except Exception as e:
            print(f"New pools WebSocket error: {e}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying new pools in 60 seconds... ({retry_count}/{max_retries})")
                await asyncio.sleep(60)
            else:
                print("Max retries reached for new pools.")
                break

async def monitor_whales():
    init_db()
    asyncio.create_task(bitquery_websocket())
    asyncio.create_task(monitor_new_pools())
    while True:
        try:
            conn = sqlite3.connect('whales.db')
            c = conn.cursor()
            c.execute('SELECT address, token_amount, pair FROM whales WHERE tracked = 1')
            whales = c.fetchall()
            conn.close()
            for whale_address, token_amount, pair in whales:
                await check_exit(whale_address, token_amount, pair)
            backup_db()
        except Exception as e:
            print(f"Monitor whales error: {e}")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(monitor_whales())

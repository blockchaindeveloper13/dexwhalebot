import asyncio
import websockets
import json
import os
import sqlite3
import requests
from telegram.ext import Application, CommandHandler
from telegram import Update
from telegram.error import NetworkError
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
            Pools(where: {SmartContract: {is: $pair_contract}}) {
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
        if 'data' not in data or not data['data']['EVM']['Pools']:
            print(f"No liquidity data for pair: {pair_contract}")
            return 0
        return float(data['data']['EVM']['Pools'][0]['Liquidity']['Value'])
    except Exception as e:
        print(f"Liquidity check error: {e}")
        return 0

async def check_new_pools(application):
    url = "https://graphql.bitquery.io/"
    query = """query {
        EVM(network: bsc) {
            Pools(first: 10, orderBy: {descending: Block_Time}) {
                SmartContract
                Token0 { Symbol }
                Token1 { Symbol }
                Liquidity { Value }
            }
        }
    }"""
    headers = {"Authorization": f"Bearer {os.getenv('BITQUERY_TOKEN')}"}
    try:
        response = requests.post(url, json={"query": query}, headers=headers)
        response.raise_for_status()
        data = response.json()
        if 'data' not in data or not data['data']['EVM']['Pools']:
            print("No pool data found")
            return
        pools = data['data']['EVM']['Pools']
        for pool in pools:
            liquidity = float(pool['Liquidity']['Value'])
            if liquidity >= 100000:  # 100K $ ve üstü
                pair_address = pool['SmartContract']
                pair_name = f"{pool['Token0']['Symbol']}/{pool['Token1']['Symbol']}"
                message = f"🆕 YENİ HAVUZ! {pair_name} oluşturuldu, likidite: {liquidity}$ 🚀"
                await application.bot.send_message(chat_id=os.getenv('TELEGRAM_GROUP'), text=message)
    except Exception as e:
        print(f"New pools check error: {e}")

async def bitquery_websocket(application):
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
                                    Buyer
                                    AmountInUSD
                                    Amount
                                    Currency { SmartContract Symbol }
                                    Dex {
                                        Pair {
                                            SmartContract
                                            Token0 { Symbol }
                                            Token1 { Symbol }
                                        }
                                    }
                                }
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
                        liquidity = await check_liquidity(trade['Trade']['Buy']['Dex']['Pair']['SmartContract'])
                        if liquidity >= 100000:  # 100K $ ve üstü
                            pair = f"{trade['Trade']['Buy']['Dex']['Pair']['Token0']['Symbol']}/{trade['Trade']['Buy']['Dex']['Pair']['Token1']['Symbol']}"
                            if check_notification_cooldown(trade['Trade']['Buy']['Buyer'], pair):
                                await send_entry_alert(trade, application)
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

async def send_entry_alert(trade, application):
    try:
        pair = f"{trade['Trade']['Buy']['Dex']['Pair']['Token0']['Symbol']}/{trade['Trade']['Buy']['Dex']['Pair']['Token1']['Symbol']}"
        message = f"🐳 BALİNA GİRİŞİ! {trade['Trade']['Buy']['Buyer']} {pair} havuzunda {trade['Trade']['Buy']['AmountInUSD']}$ aldı! 🚀"
        await application.bot.send_message(chat_id=os.getenv('TELEGRAM_GROUP'), text=message)
    except Exception as e:
        print(f"Telegram send error: {e}")

async def save_whale_address(trade):
    try:
        conn = sqlite3.connect('whales.db')
        c = conn.cursor()
        pair = f"{trade['Trade']['Buy']['Dex']['Pair']['Token0']['Symbol']}/{trade['Trade']['Buy']['Dex']['Pair']['Token1']['Symbol']}"
        c.execute('''INSERT INTO whales (address, pair, token0_symbol, token1_symbol, amount_usd, token_amount, entry_time, last_notified, tracked)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (trade['Trade']['Buy']['Buyer'], pair,
                   trade['Trade']['Buy']['Dex']['Pair']['Token0']['Symbol'], trade['Trade']['Buy']['Dex']['Pair']['Token1']['Symbol'],
                   trade['Trade']['Buy']['AmountInUSD'], trade['Trade']['Buy']['Amount'],
                   datetime.now().isoformat(), datetime.now().isoformat(), 1))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Database save error: {e}")

async def check_exit(whale_address, entry_amount, pair, application):
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
                        DEXTrades(where: {Trade: {Sell: {Seller: {is: $whale_address}}}}) {
                            Transaction { Hash }
                            Trade {
                                Sell {
                                    Seller
                                    AmountInUSD
                                    Amount
                                    Currency { SmartContract Symbol }
                                    Dex {
                                        Pair {
                                            SmartContract
                                            Token0 { Symbol }
                                            Token1 { Symbol }
                                        }
                                    }
                                }
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
                        if trade['Trade']['Sell']['Amount'] >= entry_amount * 0.1 and check_notification_cooldown(trade['Trade']['Sell']['Seller'], pair):
                            await send_exit_alert(trade, entry_amount, application)
                            update_whale_status(trade['Trade']['Sell']['Seller'])
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

async def send_exit_alert(trade, entry_amount, application):
    try:
        pair = f"{trade['Trade']['Sell']['Dex']['Pair']['Token0']['Symbol']}/{trade['Trade']['Sell']['Dex']['Pair']['Token1']['Symbol']}"
        sell_amount = trade['Trade']['Sell']['Amount']
        remaining = await get_wallet_balance(trade['Trade']['Sell']['Seller'], trade['Trade']['Sell']['Currency']['SmartContract'])
        message = f"🚨 BALİNA SATIŞI! {trade['Trade']['Sell']['Seller']} {pair} havuzunda {trade['Trade']['Sell']['AmountInUSD']}$ sattı ({sell_amount} token), elinde {remaining}$ kaldı! 🏃‍♂️"
        await application.bot.send_message(chat_id=os.getenv('TELEGRAM_GROUP'), text=message)
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
        return float(data['data']['EVM']['Balances'][0]['Balance']) if data['data']['EVM']['Balances'] else 0
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

async def start(update: Update, context):
    await update.message.reply_text("Whale Alert Bot aktif! Balina hareketleri ve yeni havuzlar izleniyor. 🐳")

async def send_test_messages(application):
    try:
        # Test mesajı: Sadece gruba
        await application.bot.send_message(
            chat_id=os.getenv('TELEGRAM_GROUP'),
            text="🔔 TEST: Bot başarıyla deploy edildi! @dexwhale_group için hazır. 🚀"
        )
        print("Test message sent successfully to group")
    except Exception as e:
        print(f"Error sending test message to group: {e}")

async def monitor_whales():
    # Log environment variables for debugging
    print(f"TELEGRAM_BOT_ID: {os.getenv('TELEGRAM_BOT_ID')}")
    print(f"TELEGRAM_GROUP: {os.getenv('TELEGRAM_GROUP')}")
    print(f"BITQUERY_TOKEN: {os.getenv('BITQUERY_TOKEN')}")
    print(f"ENVIRONMENT: {os.getenv('ENVIRONMENT')}")

    # Check if TELEGRAM_BOT_ID is set
    if not os.getenv('TELEGRAM_BOT_ID'):
        print("ERROR: TELEGRAM_BOT_ID is not set!")
        return

    application = Application.builder().token(os.getenv('TELEGRAM_BOT_ID')).build()
    application.add_handler(CommandHandler("start", start))
    
    # Send test message on deploy
    await send_test_messages(application)
    
    # Start polling with retry
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            await application.initialize()
            await application.start()
            await application.updater.start_polling(timeout=90)
            break
        except NetworkError as e:
            print(f"Telegram polling error: {e}")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Retrying polling in 60 seconds... ({retry_count}/{max_retries})")
                await asyncio.sleep(60)
            else:
                print("Max retries reached for Telegram polling.")
                return
    
    # Start monitoring tasks
    asyncio.create_task(bitquery_websocket(application))
    while True:
        try:
            conn = sqlite3.connect('whales.db')
            c = conn.cursor()
            c.execute('SELECT address, token_amount, pair FROM whales WHERE tracked = 1')
            whales = c.fetchall()
            conn.close()
            for whale_address, token_amount, pair in whales:
                await check_exit(whale_address, token_amount, pair, application)
            await check_new_pools(application)
            backup_db()
        except Exception as e:
            print(f"Monitor whales error: {e}")
        await asyncio.sleep(300)

if __name__ == "__main__":
    init_db()
    asyncio.run(monitor_whales())

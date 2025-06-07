import asyncio
import json
import websockets
import firebase_admin
from firebase_admin import credentials, db
import os
import time

# --- Configuration ---
FIREBASE_URL = "https://vix25-486b9-default-rtdb.firebaseio.com"
SYMBOL = "R_150_1S"
MAX_TICKS = 950

# --- Firebase Initialization ---
if not firebase_admin._apps:
    if os.getenv("FIREBASE_CREDENTIALS"):
        # Running on Railway with env variable
        cred_data = json.loads(os.getenv("FIREBASE_CREDENTIALS"))
        cred = credentials.Certificate(cred_data)
    else:
        # Local run with firebase.json file
        cred = credentials.Certificate("firebase.json")

    firebase_admin.initialize_app(cred, {
        'databaseURL': FIREBASE_URL
    })

# --- WebSocket Ticks Fetching ---
async def connect_and_stream():
    uri = "wss://ws.derivws.com/websockets/v3?app_id=1089"

    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps({
            "ticks": SYMBOL,
            "subscribe": 1
        }))

        while True:
            response = await websocket.recv()
            data = json.loads(response)
            tick = data.get("tick")
            if tick:
                await store_tick(tick)

# --- Store Tick in Firebase and Prune ---
async def store_tick(tick):
    ref = db.reference(f'/ticks/{SYMBOL}')
    all_ticks = ref.get() or {}

    key = str(tick["epoch"])
    all_ticks[key] = {
        "epoch": tick["epoch"],
        "quote": tick["quote"],
        "symbol": tick["symbol"]
    }

    # Keep only latest 950 entries
    sorted_keys = sorted(all_ticks.keys(), reverse=True)[:MAX_TICKS]
    pruned_ticks = {k: all_ticks[k] for k in sorted_keys}
    ref.set(pruned_ticks)

    print(f'Stored tick: {tick["epoch"]} -> {tick["quote"]}')

# --- Auto-Restart Loop ---
def run_forever():
    while True:
        try:
            asyncio.run(connect_and_stream())
        except Exception as e:
            print("WebSocket error, retrying in 3 seconds:", e)
            time.sleep(3)

if __name__ == "__main__":
    run_forever()

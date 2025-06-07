import web
import json
import asyncio
import websockets
import threading
import time
from datetime import datetime, timedelta
import requests
from collections import deque
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
FIREBASE_URL = "https://vix25-486b9-default-rtdb.firebaseio.com"
MAX_TICKS = 950
DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"

class TickProcessor:
    def __init__(self):
        self.ticks_buffer = deque(maxlen=MAX_TICKS)
        self.is_connected = False
        self.last_update = None
        self.websocket = None
        
    async def connect_deriv(self):
        """Connect to Deriv WebSocket and subscribe to Volatility 150(1s)"""
        try:
            self.websocket = await websockets.connect(DERIV_WS_URL)
            logger.info("Connected to Deriv WebSocket")
            
            # Subscribe to Volatility 150(1s) ticks
            subscribe_msg = {
                "ticks": "R_50",  # Volatility 150(1s) symbol
                "subscribe": 1
            }
            await self.websocket.send(json.dumps(subscribe_msg))
            logger.info("Subscribed to Volatility 150(1s) ticks")
            
            self.is_connected = True
            
            # Listen for ticks
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    if 'tick' in data:
                        await self.process_tick(data['tick'])
                except Exception as e:
                    logger.error(f"Error processing tick: {e}")
                    
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            self.is_connected = False
            
    async def process_tick(self, tick_data):
        """Process incoming tick and update Firebase"""
        try:
            tick = {
                'timestamp': tick_data['epoch'],
                'price': float(tick_data['quote']),
                'datetime': datetime.fromtimestamp(tick_data['epoch']).isoformat()
            }
            
            # Add to buffer
            self.ticks_buffer.append(tick)
            self.last_update = datetime.now()
            
            # Update Firebase
            await self.update_firebase()
            
        except Exception as e:
            logger.error(f"Error processing tick: {e}")
            
    async def update_firebase(self):
        """Update Firebase with latest ticks and candles"""
        try:
            # Convert deque to list for Firebase
            ticks_list = list(self.ticks_buffer)
            
            # Update ticks
            response = requests.put(
                f"{FIREBASE_URL}/ticks.json", 
                json=ticks_list[-MAX_TICKS:]
            )
            
            if response.status_code == 200:
                # Generate and update candles
                await self.update_candles(ticks_list)
            else:
                logger.error(f"Firebase update failed: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Firebase update error: {e}")
            
    async def update_candles(self, ticks):
        """Generate 1min and 5min candles from ticks"""
        try:
            if len(ticks) < 2:
                return
                
            # Generate 1min candles
            min1_candles = self.generate_candles(ticks, 60)  # 1 minute
            min5_candles = self.generate_candles(ticks, 300)  # 5 minutes
            
            # Update Firebase with candles
            requests.put(f"{FIREBASE_URL}/candles_1min.json", json=min1_candles[-100:])
            requests.put(f"{FIREBASE_URL}/candles_5min.json", json=min5_candles[-50:])
            
        except Exception as e:
            logger.error(f"Candle generation error: {e}")
            
    def generate_candles(self, ticks, interval_seconds):
        """Generate candles from ticks for given interval"""
        if not ticks:
            return []
            
        candles = []
        current_candle = None
        
        for tick in ticks:
            tick_time = tick['timestamp']
            candle_start = (tick_time // interval_seconds) * interval_seconds
            
            if current_candle is None or current_candle['timestamp'] != candle_start:
                # Start new candle
                if current_candle:
                    candles.append(current_candle)
                    
                current_candle = {
                    'timestamp': candle_start,
                    'datetime': datetime.fromtimestamp(candle_start).isoformat(),
                    'open': tick['price'],
                    'high': tick['price'],
                    'low': tick['price'],
                    'close': tick['price'],
                    'volume': 1
                }
            else:
                # Update existing candle
                current_candle['high'] = max(current_candle['high'], tick['price'])
                current_candle['low'] = min(current_candle['low'], tick['price'])
                current_candle['close'] = tick['price']
                current_candle['volume'] += 1
                
        if current_candle:
            candles.append(current_candle)
            
        return candles

# Global tick processor
processor = TickProcessor()

# Web.py URLs
urls = (
    '/', 'Index',
    '/status', 'Status'
)

class Index:
    def GET(self):
        return """
<!DOCTYPE html>
<html>
<head>
    <title>Deriv to Firebase - Volatility 150(1s)</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 40px; 
            background: #f5f5f5;
        }
        .container { 
            max-width: 800px; 
            margin: 0 auto; 
            background: white; 
            padding: 30px; 
            border-radius: 10px; 
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .status { 
            padding: 15px; 
            border-radius: 5px; 
            margin: 20px 0; 
            font-size: 18px;
            display: flex;
            align-items: center;
        }
        .connected { 
            background: #d4edda; 
            color: #155724; 
            border: 1px solid #c3e6cb;
        }
        .disconnected { 
            background: #f8d7da; 
            color: #721c24; 
            border: 1px solid #f5c6cb;
        }
        .icon { 
            font-size: 24px; 
            margin-right: 10px; 
        }
        .stats { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px; 
            margin: 20px 0; 
        }
        .stat-box { 
            background: #f8f9fa; 
            padding: 15px; 
            border-radius: 5px; 
            text-align: center;
        }
        .stat-number { 
            font-size: 24px; 
            font-weight: bold; 
            color: #007bff; 
        }
        .refresh-btn { 
            background: #007bff; 
            color: white; 
            border: none; 
            padding: 10px 20px; 
            border-radius: 5px; 
            cursor: pointer; 
            font-size: 16px;
        }
        .refresh-btn:hover { 
            background: #0056b3; 
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Deriv to Firebase - Volatility 150(1s)</h1>
        
        <div id="status" class="status disconnected">
            <span class="icon">❌</span>
            <span>Checking connection...</span>
        </div>
        
        <div class="stats">
            <div class="stat-box">
                <div class="stat-number" id="tick-count">-</div>
                <div>Ticks Stored</div>
            </div>
            <div class="stat-box">
                <div class="stat-number" id="last-update">-</div>
                <div>Last Update</div>
            </div>
            <div class="stat-box">
                <div class="stat-number" id="candles-1m">-</div>
                <div>1min Candles</div>
            </div>
            <div class="stat-box">
                <div class="stat-number" id="candles-5m">-</div>
                <div>5min Candles</div>
            </div>
        </div>
        
        <button class="refresh-btn" onclick="updateStatus()">🔄 Refresh Status</button>
        
        <h3>Firebase Database URLs:</h3>
        <ul>
            <li><a href="https://vix25-486b9-default-rtdb.firebaseio.com/ticks.json" target="_blank">Raw Ticks</a></li>
            <li><a href="https://vix25-486b9-default-rtdb.firebaseio.com/candles_1min.json" target="_blank">1min Candles</a></li>
            <li><a href="https://vix25-486b9-default-rtdb.firebaseio.com/candles_5min.json" target="_blank">5min Candles</a></li>
        </ul>
    </div>

    <script>
        function updateStatus() {
            fetch('/status')
                .then(response => response.json())
                .then(data => {
                    const statusDiv = document.getElementById('status');
                    const tickCount = document.getElementById('tick-count');
                    const lastUpdate = document.getElementById('last-update');
                    const candles1m = document.getElementById('candles-1m');
                    const candles5m = document.getElementById('candles-5m');
                    
                    if (data.connected) {
                        statusDiv.className = 'status connected';
                        statusDiv.innerHTML = '<span class="icon">✅</span><span>Connected & Streaming</span>';
                    } else {
                        statusDiv.className = 'status disconnected';
                        statusDiv.innerHTML = '<span class="icon">❌</span><span>Disconnected</span>';
                    }
                    
                    tickCount.textContent = data.tick_count || '-';
                    lastUpdate.textContent = data.last_update || '-';
                    candles1m.textContent = data.candles_1m || '-';
                    candles5m.textContent = data.candles_5m || '-';
                })
                .catch(error => {
                    document.getElementById('status').innerHTML = '<span class="icon">❌</span><span>Error fetching status</span>';
                });
        }
        
        // Update status every 5 seconds
        setInterval(updateStatus, 5000);
        
        // Initial status update
        updateStatus();
    </script>
</body>
</html>
        """

class Status:
    def GET(self):
        web.header('Content-Type', 'application/json')
        
        try:
            # Get Firebase data to show stats
            ticks_response = requests.get(f"{FIREBASE_URL}/ticks.json", timeout=5)
            candles_1m_response = requests.get(f"{FIREBASE_URL}/candles_1min.json", timeout=5)
            candles_5m_response = requests.get(f"{FIREBASE_URL}/candles_5min.json", timeout=5)
            
            ticks_count = len(ticks_response.json()) if ticks_response.status_code == 200 and ticks_response.json() else 0
            candles_1m_count = len(candles_1m_response.json()) if candles_1m_response.status_code == 200 and candles_1m_response.json() else 0
            candles_5m_count = len(candles_5m_response.json()) if candles_5m_response.status_code == 200 and candles_5m_response.json() else 0
            
            return json.dumps({
                'connected': processor.is_connected,
                'tick_count': ticks_count,
                'last_update': processor.last_update.strftime('%H:%M:%S') if processor.last_update else None,
                'candles_1m': candles_1m_count,
                'candles_5m': candles_5m_count
            })
        except Exception as e:
            logger.error(f"Status error: {e}")
            return json.dumps({
                'connected': False,
                'error': str(e)
            })

def run_websocket():
    """Run WebSocket connection in separate thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    while True:
        try:
            loop.run_until_complete(processor.connect_deriv())
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            processor.is_connected = False
            time.sleep(5)  # Wait before reconnecting

if __name__ == "__main__":
    # Start WebSocket in background thread
    ws_thread = threading.Thread(target=run_websocket, daemon=True)
    ws_thread.start()
    
    # Start web application
    app = web.application(urls, globals())
    print("🚀 Starting Deriv to Firebase application...")
    print("📊 Volatility 150(1s) ticks will be stored in Firebase")
    print("🌐 Web interface available at: http://localhost:8080")
    app.run()

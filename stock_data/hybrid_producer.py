import json
import time
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime

# 1. Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = "stock_stream"

# The list of companies you want to track live
tickers = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"] 

print("🚀 Starting Hybrid Stream: Fetching LIVE data from Yahoo Finance API...")

while True:
    for symbol in tickers:
        try:
            # Fetch the very latest market data (1-minute intervals)
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1d", interval="1m")
            
            if not hist.empty:
                latest = hist.iloc[-1] # Grab the absolute latest minute
                
                # Format exactly like our PySpark schema expects
                message = {
                    "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Open": float(latest["Open"]),
                    "High": float(latest["High"]),
                    "Low": float(latest["Low"]),
                    "Close": float(latest["Close"]),
                    "Adj Close": float(latest["Close"]), # 1m intervals don't always use Adj Close, so we map Close
                    "Volume": float(latest["Volume"]),
                    "Symbol": symbol
                }
                
                # Push to Kafka
                producer.send(topic_name, value=message)
                print(f"📡 [LIVE] Streamed {symbol}: {message['Close']}")
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")
            
    print("⏳ Waiting 60 seconds for the next live market update...")
    time.sleep(60)
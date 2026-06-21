import os
import json
import time
import pandas as pd
from kafka import KafkaProducer

# 1. Initialize the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = "stock_stream"
stocks_dir = "data"

print("⏳ Gathering and merging all stock files...")

# 2. Read and Merge All CSVs
all_dataframes = []

# Loop through every file in the stocks folder
for filename in os.listdir(stocks_dir):
    if filename.endswith(".csv"):
        file_path = os.path.join(stocks_dir, filename)
        
        # Extract symbol from filename (e.g., 'AAPL.csv' -> 'AAPL')
        symbol = filename.replace(".csv", "")
        
        # Read the CSV using Pandas
        df = pd.read_csv(file_path)
        
        # Attach the exact Stock Symbol to every row
        df["Symbol"] = symbol
        
        all_dataframes.append(df)

# 3. Combine and Sort Chronologically
print("🔄 Combining and sorting data chronologically by Date...")
merged_df = pd.concat(all_dataframes, ignore_index=True)

# Sort by Date so the stream flows correctly through time
if "Date" in merged_df.columns:
    merged_df = merged_df.sort_values(by="Date")

# Replace any Pandas NaN (empty) values with Python None so JSON doesn't crash
merged_df = merged_df.where(pd.notnull(merged_df), None)

total_rows = len(merged_df)
print(f"🚀 Launching stream to topic: {topic_name}!")
print(f"📊 Total combined rows to stream: {total_rows}")
print("-" * 50)

# 4. Stream to Kafka
for index, row in merged_df.iterrows():
    # Convert the row to a dictionary
    message = row.to_dict()
    
    # Send to Kafka
    producer.send(topic_name, value=message)
    
    # Print a progress indicator every 500 rows so you know it's working
    if index % 500 == 0:
        print(f"Streaming {message['Symbol']} data from {message['Date']}...")
        
    # Add a tiny delay to simulate a live stream and protect the Kafka broker
    time.sleep(0.02) 

print("✅ All historical market data has been successfully streamed!")
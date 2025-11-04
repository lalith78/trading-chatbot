from nsepython import nsefetch
import pandas as pd
import time
from datetime import datetime
import os

# Folder to store CSVs
CSV_FOLDER = "nse_live_data"
os.makedirs(CSV_FOLDER, exist_ok=True)

last_prices = {}

# Function to fetch NIFTY 50 live data    
def fetch_nifty50():
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    data = nsefetch(url)  # Fetch JSON data directly
    stocks = data["data"]
    df = pd.DataFrame(stocks)
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

# Save only changed rows
def save_changes(df):
    global last_prices
    changed_rows = []

    for _, row in df.iterrows():
        symbol = row["symbol"]
        price = row["lastPrice"]
        if symbol not in last_prices or last_prices[symbol] != price:
            changed_rows.append(row)
            last_prices[symbol] = price

    if changed_rows:
        df_changes = pd.DataFrame(changed_rows)
        # Create a unique filename using timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(CSV_FOLDER, f"nse_data_{timestamp}.csv")
        df_changes.to_csv(filename, index=False)
        print(f"✅ Saved {len(df_changes)} changed rows to {filename}")
    else:
        print("ℹ No price changes detected this interval.")

# Run bot every 5 minutes
print("🚀 NSE Live Data Bot started (every 5 minutes)")

try:
    while True:
        df = fetch_nifty50()
        save_changes(df)
        time.sleep(300)  # 5 minutes
except KeyboardInterrupt:
    print("🛑 Bot stopped manually")

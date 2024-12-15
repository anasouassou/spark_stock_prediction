import requests
import json

# API URL template
base_url = "https://datasets-server.huggingface.co/rows?dataset=Ammok%2Fapple_stock_price_from_1980-2021&config=default&split=train&offset={offset}&length=100"

all_data = []  # To store all fetched data
offset = 0  # Starting offset
chunk_size = 100  # Number of rows per request

while True:
    # Construct the URL with the current offset
    url = base_url.format(offset=offset)
    
    # Send GET request
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch data at offset {offset}. Status code: {response.status_code}")
        break

    # Parse the JSON response
    data = response.json()
    
    # Extract rows
    rows = data.get("rows", [])
    
    if not rows:
        print("No more data to fetch.")
        break  # Exit the loop if there are no more rows
    
    # Append the rows to the main dataset
    all_data.extend([row["row"] for row in rows])
    
    # Increment the offset for the next chunk
    offset += chunk_size
    print(f"Fetched {len(rows)} rows. Total rows so far: {len(all_data)}")

# Save all the data to a JSON file
with open("apple_stock_data_full.json", "w") as f:
    json.dump(all_data, f)

print(f"Total rows fetched: {len(all_data)}")

#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "azure-cosmos"]
# ///

import csv
import io
import requests
from azure.cosmos import CosmosClient, PartitionKey

# Google Sheet config
SPREADSHEET_ID = "your-spreadsheet-id-here"  # the long string in the sheet URL
SHEET_NAME = "Sheet1"  # or whatever the tab is called
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"

# Cosmos DB config
COSMOS_ENDPOINT = "https://your-account.documents.azure.com:443/"
COSMOS_KEY = "your-cosmos-key"
DATABASE_NAME = "airisks"
CONTAINER_NAME = "risks"

def fetch_sheet_data(url):
    response = requests.get(url)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text))
    return list(reader)

def main():
    # Fetch the sheet
    print("Fetching MIT data from Google Sheets...")
    rows = fetch_sheet_data(SHEET_URL)
    print(f"Got {len(rows)} rows")

    # Connect to Cosmos
    client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    database = client.create_database_if_not_exists(DATABASE_NAME)
    container = database.create_container_if_not_exists(
        id=CONTAINER_NAME,
        partition_key=PartitionKey(path="/source"),
        offer_throughput=400  # minimal RUs, increase if needed
    )

    # Load each row as a document
    for i, row in enumerate(rows):
        doc = {
            "id": f"mit-risk-{i:04d}",
            "source": "MIT",
            **row  # spread all the CSV columns as document fields
        }
        container.upsert_item(doc)
        
        if (i + 1) % 100 == 0:
            print(f"Loaded {i + 1} documents...")

    print(f"Done. Loaded {len(rows)} risks into Cosmos DB.")

if __name__ == "__main__":
    main()
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "azure-cosmos", "azure-identity"]
# ///

import csv
import io
import time
import requests
from urllib.parse import quote
from azure.identity import DefaultAzureCredential
from azure.cosmos import CosmosClient, PartitionKey
from azure.core.exceptions import ServiceResponseTimeoutError

# Google Sheet config
SPREADSHEET_ID = "15LeHcpeuZC9txkvcaMoh3sUhkMvdMMry69xxXL46DT0"  # direct link to the MIT spreadsheet
SHEET_NAME = "AI Risk Database v4"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&sheet={quote(SHEET_NAME)}"

# Cosmos DB config
COSMOS_ENDPOINT = "https://airisks.documents.azure.com:443/"
DATABASE_NAME = "airisks"
CONTAINER_NAME = "mit_risks"

def fetch_sheet_data(url, skip_rows=1):
    """
    Fetch and parse CSV data where headers might not be in the first row.
    
    Args:
        url: The URL to fetch CSV data from
        header_row_index: The 0-based index of the row containing headers (default: 2 for row 3)
    """
    response = requests.get(url)
    response.raise_for_status()

    headings = [
        "title",
        "quickRef",
        "evId",
        "paperId",
        "catId",
        "subCatId",
        "addEvId",
        "categoryLevel",
        "riskCategory",
        "riskSubcategory",
        "description",
        "additionalEvidence",
        "pDef",
        "pAddEv",
        "entity",
        "intent",
        "timing",
        "domain",
        "subDomain",
    ]

    # Use csv.reader to properly handle quoted strings with embedded newlines
    reader = csv.reader(io.StringIO(response.text),strict=True)
    all_rows = list(reader)
    
    # Convert data rows (after header row) to dictionaries
    data_rows = []
    for i,row in enumerate(all_rows[skip_rows:]):
        if row:  # Skip empty rows
            row_dict = dict(zip(headings, row))
            assert len(row) == len(headings), f"Row {i} has {len(row)} columns but expected {len(headings)}... {row_dict}"
            data_rows.append({"id": f"mit.{row_dict['evId']}"} | row_dict | {"source": "MIT_Risk_Repository"})
    
    return data_rows

def upsert_with_retry(container, item, max_retries=5, initial_delay=1):
    """
    Upsert an item with exponential backoff retry for timeout errors.
    
    Args:
        container: The Cosmos DB container
        item: The item to upsert
        max_retries: Maximum number of retry attempts (default: 5)
        initial_delay: Initial delay in seconds before first retry (default: 1)
    """
    for attempt in range(max_retries):
        try:
            container.upsert_item(item)
            return
        except ServiceResponseTimeoutError as e:
            if attempt == max_retries - 1:
                # Last attempt, re-raise the exception
                print(f"Failed to upsert item {item.get('id', 'unknown')} after {max_retries} attempts")
                raise
            
            # Calculate exponential backoff delay
            delay = initial_delay * (2 ** attempt)
            print(f"Timeout on attempt {attempt + 1}/{max_retries} for item {item.get('id', 'unknown')}, retrying in {delay}s...")
            time.sleep(delay)

def main():
    # Fetch the sheet
    print("Fetching MIT data from Google Sheets...")
    rows = fetch_sheet_data(SHEET_URL)
    print(f"Got {len(rows)} rows")

    # Connect to Cosmos

    credential = DefaultAzureCredential()
    client = CosmosClient(COSMOS_ENDPOINT, credential=credential)
    database = client.create_database_if_not_exists(DATABASE_NAME)
    container = database.create_container_if_not_exists(
        id=CONTAINER_NAME,
        partition_key=PartitionKey(path="/source"),
        offer_throughput=400  # minimal RUs, increase if needed
    )

    # Load each row as a document
    for i, row in enumerate(rows):
        upsert_with_retry(container, row)
        
        if (i + 1) % 100 == 0:
            print(f"Loaded {i + 1} documents...")

    print(f"Done. Loaded {len(rows)} risks into Cosmos DB.")

if __name__ == "__main__":
    main()
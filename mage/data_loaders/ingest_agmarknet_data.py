import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta

from utils.helper_functions import get_prj_data_dir, get_previous_week_window


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


MARKET_METADATA_URL = "https://api.agmarknet.gov.in/v1/market-district-state"
DAILY_REPORT_URL = "https://api.agmarknet.gov.in/v1/prices-and-arrivals/market-report/daily"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://agmarknet.gov.in",
    "Referer": "https://agmarknet.gov.in/",
}

# Fetches the state and market IDs needed for the daily report payload
def fetch_market_metadata():
    print("Fetching market metadata...")
    r = requests.get(MARKET_METADATA_URL, headers=HEADERS, timeout=120)
    r.raise_for_status()
    data = r.json()
    
    state_ids = sorted(list(set(item["state_id"] for item in data)))
    market_ids = sorted(list(set(item["market_id"] for item in data)))
    return state_ids, market_ids

def fetch_for_date(target_date, state_ids, market_ids, max_retries=3):
    payload = {
        "date": target_date.strftime("%Y-%m-%d"),
        "State": state_ids,
        "stateIds": state_ids,
        "marketIds": market_ids,
        "includeExcel": False,
        "title": (
            "Market-wise, Commodity-wise Daily Report on "
            f"{target_date.strftime('%d/%m/%Y')}"
        ),
    }

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(DAILY_REPORT_URL, json=payload, headers=HEADERS, timeout=180)
            r.raise_for_status()
            resp = r.json()
            if resp.get("success"):
                return resp.get("states", [])
        except Exception as e:
            print(f"Attempt {attempt} failed for {target_date}: {e}")
            if attempt < max_retries:
                time.sleep(5 * attempt)
    return None

def process_daily_json(states_json, current_date):
    rows = []
    for state in states_json:
        state_id, state_name = state["stateId"], state["stateName"]
        for market in state.get("markets", []):
            market_id, market_name = market["marketId"], market["marketName"]
            for commodity in market.get("commodities", []):
                # Handle NIL transactions
                if "commodityId" not in commodity:           
                    rows.append(
                        {
                            "data_date": current_date.strftime("%Y-%m-%d"),
                            "state_id": state_id,
                            "state_name": state_name,
                            "market_id": market_id,
                            "market_name": market_name,
                            "commodity_id": None,
                            "commodity_name": commodity.get(
                                "commodityName"
                            ),
                            "arrivals": 0,
                            "unit_of_arrivals": None,
                            "variety": None,
                            "grade": None,
                            "min_price": None,
                            "max_price": None,
                            "modal_price": None,
                            "unit_of_price": None,
                        }
                    )
                    continue

                for rec in commodity.get("data", []):
                    rows.append(
                        {
                            "data_date": current_date.strftime("%Y-%m-%d"),
                            "state_id": state_id,
                            "state_name": state_name,
                            "market_id": market_id,
                            "market_name": market_name,
                            "commodity_id": commodity.get(
                                "commodityId"
                            ),
                            "commodity_name": commodity.get(
                                "commodityName"
                            ),
                            "arrivals": rec.get("arrivals"),
                            "unit_of_arrivals": rec.get(
                                "unitOfArrivals"
                            ),
                            "variety": rec.get("variety"),
                            "grade": rec.get("grade"),
                            "min_price": rec.get("minimumPrice"),
                            "max_price": rec.get("maximumPrice"),
                            "modal_price": rec.get("modalPrice"),
                            "unit_of_price": rec.get("unitOfPrice"),
                        }
                    )
    return rows

@data_loader
def load_data(*args, **kwargs):
    execution_date = kwargs.get('execution_date')
    
    if execution_date is None:
        execution_date = datetime.now()

    data_dir = get_prj_data_dir()
    raw_dir = data_dir / 'raw'
    print(f"Raw data directory: {raw_dir}")

    state_ids, market_ids = fetch_market_metadata()
    start_date, end_date = get_previous_week_window(execution_date)

    current_date = start_date
    all_dfs = []

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        file_path = raw_dir / f"agmarknet_{date_str}.csv"

        if file_path.exists():
            print(f"File for {date_str} exists. Loading from local..")
            day_df = pd.read_csv(file_path)
            all_dfs.append(day_df)
        else:
            print(f"Scraping {date_str}....")
            day_json = fetch_for_date(current_date, state_ids, market_ids)
        
            if day_json:
                rows = process_daily_json(day_json, current_date)
                day_df = pd.DataFrame(rows)

                day_df.to_csv(file_path, index=False)
                all_dfs.append(day_df)
                print(f"Csv for {date_str} saved to {file_path}")
                time.sleep(1)
            else:
                day_df = pd.DataFrame()
                print(f"Data is empty for {date_str}")

        current_date += timedelta(days=1)

    print("All csv's are combined to one df and passed to next block (transformer)")
    return pd.concat(all_dfs, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
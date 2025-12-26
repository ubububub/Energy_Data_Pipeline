import os
import schedule
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv("RTE_CLIENT_ID")
CLIENT_SECRET = os.getenv("RTE_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("Missing RTE_CLIENT_ID or RTE_CLIENT_SECRET in .env")

# Constants
DATA_URL = "https://digital.iservices.rte-france.com/open_api/generation_forecast/v2/forecasts"
TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"

START_DATE = "2024-01-01T00:00:00+01:00"
END_DATE = "2024-10-01T00:00:00+02:00"
DELTA_DAYS = 15

RTE_TZ = timezone("Europe/Paris")

# Authentication
def get_token():
    response = requests.post(TOKEN_URL, auth=(CLIENT_ID, CLIENT_SECRET))
    if response.ok:
        return response.json()["access_token"]
    raise Exception(f"Auth failed: {response.status_code}, {response.text}")

# API calls
def get_interval_data(access_token, start_date, end_date, production_type, forecast_type):
    response = requests.get(
        DATA_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "start_date": start_date,
            "end_date": end_date,
            "production_type": production_type,
            "type": forecast_type,
        }
    )
    if response.ok:
        return response.json().get("forecasts", [])
    raise Exception(f"Fetch failed: {response.status_code}, {response.text}")

# DST handling
def handle_dst(start_time, end_time):
    start_time = start_time.astimezone(RTE_TZ)
    end_time = end_time.astimezone(RTE_TZ)
    end_time += start_time.dst() - end_time.dst()
    return start_time, end_time

# Fetch full period
def get_all_data(access_token, start_date, end_date, production_type, forecast_type):
    all_data = []
    interval_start = datetime.fromisoformat(start_date)
    end_date = datetime.fromisoformat(end_date)

    while interval_start < end_date:
        interval_end = min(interval_start + timedelta(days=DELTA_DAYS), end_date)
        interval_start, interval_end = handle_dst(interval_start, interval_end)

        data = get_interval_data(
            access_token,
            interval_start.isoformat(),
            interval_end.isoformat(),
            production_type,
            forecast_type
        )

        all_data.extend(data)
        interval_start = interval_end

    return all_data

# Save CSV
def save_data(data, base_filename):
    records = []

    for forecast in data:
        for value in forecast["values"]:
            records.append({
                "start_date": value["start_date"],
                "end_date": value["end_date"],
                "production_type": forecast["production_type"],
                "forecast_type": forecast["type"],
                "generation_value": value["value"],
            })

    df = pd.DataFrame(records).sort_values("start_date")
    timestamp = datetime.now(RTE_TZ).strftime("%Y%m%d_%H%M%S")
    filename = f"{base_filename}_{timestamp}.csv"

    df.to_csv(filename, index=False)
    print(f"Saved {filename}")

def run_pipeline():
    print(f"Job started at {datetime.now(RTE_TZ)}")

    token = get_token()

    wind_data = get_all_data(token, START_DATE, END_DATE, "WIND_ONSHORE", "CURRENT")
    solar_data = get_all_data(token, START_DATE, END_DATE, "SOLAR", "CURRENT")

    save_data(wind_data, "electricity_generation_wind")
    save_data(solar_data, "electricity_generation_solar")

    print("Job completed\n")

# Scheduler setup
if __name__ == "__main__":
    # Run immediately once
    run_pipeline()

    # Then every 3 hours
    schedule.every(3).hours.do(run_pipeline)

    print("Scheduler started (every 3 hours)")

    while True:
        schedule.run_pending()
        time.sleep(30)

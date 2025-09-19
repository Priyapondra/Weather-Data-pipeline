# flows/Whether_flow.py
import yaml
from pathlib import Path
from prefect import flow, task
from src.extract import fetch_hourly_weather
from src.transform import weather_json_to_df
from src.load import get_engine, upsert_dim_city, bulk_upsert_weather

@task
def load_cities_config():
    return yaml.safe_load(Path("configs/cities.yaml").read_text())["cities"]

@task
def fetch_transform(city):
    raw = fetch_hourly_weather(city["lat"], city["lon"])
    return weather_json_to_df(raw, city["city_id"])

# ⬇️ plain functions (no @task) so we don't pass engine into tasks
def ensure_city(engine, c):
    city_id = upsert_dim_city(engine, c["name"], c["lat"], c["lon"], c.get("country"))
    return {**c, "city_id": city_id}

def load_weather(engine, df):
    bulk_upsert_weather(engine, df)

@flow(name="weather-pipeline")
def main():
    engine = get_engine()

    cities = load_cities_config()         # returns list directly in Prefect 3
    enriched = [ensure_city(engine, c) for c in cities]
    frames = [fetch_transform(c) for c in enriched]
    for df in frames:
        load_weather(engine, df)

if __name__ == "__main__":
    main()

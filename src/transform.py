import pandas as pd

def weather_json_to_df(payload, city_id):
    h = payload["hourly"]
    df = pd.DataFrame({
        "ts": pd.to_datetime(h["time"]),
        "temperature_c": h.get("temperature_2m"),
        "precipitation_mm": h.get("precipitation"),
        "windspeed_kmh": h.get("wind_speed_10m")
    })
    df["city_id"] = city_id
    # basic data hygiene
    df = df.drop_duplicates(subset=["city_id", "ts"]).sort_values("ts")
    return df[["city_id", "ts", "temperature_c", "windspeed_kmh", "precipitation_mm"]]

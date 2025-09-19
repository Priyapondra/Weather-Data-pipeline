import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    )
    return create_engine(url, pool_pre_ping=True)

def upsert_dim_city(engine, name, lat, lon, country):
    with engine.begin() as conn:
        res = conn.execute(
            text("""
            INSERT INTO weather.dim_city (city_name, latitude, longitude, country)
            VALUES (:name, :lat, :lon, :country)
            ON CONFLICT (city_name) DO UPDATE SET
              latitude = EXCLUDED.latitude,
              longitude = EXCLUDED.longitude,
              country = EXCLUDED.country
            RETURNING city_id;
            """),
            {"name": name, "lat": lat, "lon": lon, "country": country}
        )
        return res.scalar()

def bulk_upsert_weather(engine, df):
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TEMP TABLE tmp_weather (
              city_id INT,
              ts TIMESTAMP WITHOUT TIME ZONE,
              temperature_c NUMERIC,
              windspeed_kmh NUMERIC,
              precipitation_mm NUMERIC
            ) ON COMMIT DROP;
        """))
        # ⬇️ use the SQLAlchemy connection directly
        df.to_sql("tmp_weather", con=conn, index=False, if_exists="append", method="multi")
        conn.execute(text("""
            INSERT INTO weather.fact_weather_hourly (city_id, ts, temperature_c, windspeed_kmh, precipitation_mm)
            SELECT city_id, ts, temperature_c, windspeed_kmh, precipitation_mm
            FROM tmp_weather
            ON CONFLICT (city_id, ts) DO UPDATE SET
              temperature_c = EXCLUDED.temperature_c,
              windspeed_kmh = EXCLUDED.windspeed_kmh,
              precipitation_mm = EXCLUDED.precipitation_mm;
        """))

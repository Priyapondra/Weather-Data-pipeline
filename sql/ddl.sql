-- sql/ddl.sql
CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.dim_city (
  city_id SERIAL PRIMARY KEY,
  city_name TEXT NOT NULL,
  latitude NUMERIC NOT NULL,
  longitude NUMERIC NOT NULL,
  country TEXT
);

CREATE TABLE IF NOT EXISTS weather.fact_weather_hourly (
  city_id INT REFERENCES weather.dim_city(city_id),
  ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  temperature_c NUMERIC,
  windspeed_kmh NUMERIC,
  precipitation_mm NUMERIC,
  PRIMARY KEY (city_id, ts)
);

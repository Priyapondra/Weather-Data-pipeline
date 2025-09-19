# 🌦️ Real-Time Weather ETL Pipeline

An end-to-end **ETL pipeline** that ingests real-time weather data from the [Open-Meteo API](https://open-meteo.com/), transforms it with **Pandas**, and loads it into a **PostgreSQL warehouse** running in Docker. The workflow is orchestrated with **Prefect**, designed with a dimensional schema for analytics, and supports daily automated runs.

---

## 🚀 Features
- Extracts hourly weather data (temperature, precipitation, windspeed) for multiple cities.
- Transforms raw JSON responses into structured DataFrames.
- Loads data into **PostgreSQL** using SQLAlchemy with **idempotent upserts**.
- Orchestrated with **Prefect** for scheduling and observability.
- Runs locally with **Docker Compose** for quick setup.
- Provides SQL queries for weather insights (daily averages, rainiest hours).

---

## 🛠️ Tech Stack
- **Python** (Pandas, SQLAlchemy, Requests, PyYAML, Prefect)
- **PostgreSQL** (via Docker)
- **Docker Compose**
- **Open-Meteo API** (no API key required)

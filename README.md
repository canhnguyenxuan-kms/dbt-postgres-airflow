# dbt-postgres-airflow

## Overview

`dbt-postgres-airflow` is a data engineering project that orchestrates the ingestion, transformation, and loading of weather data using Airflow, dbt, and a PostgreSQL database. The project fetches weather data from the WeatherStack API, stores it in a PostgreSQL database, and transforms it using dbt models. Airflow is used to schedule and manage the workflow.

---

## Architecture

- **Airflow**: Orchestrates the workflow, schedules tasks, and manages dependencies.
- **Python**: Fetches weather data from the WeatherStack API and loads it into PostgreSQL.
- **PostgreSQL**: Stores raw and transformed weather data.
- **dbt**: Transforms and models the data in PostgreSQL.
- **WeatherStack API**: Provides current weather data for multiple cities.

---

## Directory Structure

```
dbt-postgres-airflow/
├── airflow/
│   ├── dags/
│   │   └── dbt-orchesstrator.py
│   └── utilities/
│       └── helper_function.py
├── dbt/
│   └── canh_dbt_proj/
│       ├── models/
│       │   └── mart/
│       │       └── weather_del_ins.sql
│       └── target/
│           └── manifest.json
├── requirements.txt
└── README.md
```

---

## Airflow DAGs

### `dbt_orchestrator`

- **Purpose**: Orchestrates the end-to-end pipeline: fetches weather data, loads it into PostgreSQL, and runs dbt models.
- **Key Tasks**:
  - `ingest_weather_data`: PythonOperator that runs `main()` from `helper_function.py` to fetch and load weather data.
  - Dynamically generated dbt model tasks: BashOperators that run dbt models based on the manifest.
- **Dependencies**: Each dbt model task depends on the successful completion of `ingest_weather_data` and on upstream dbt model dependencies.

---


## Python Utilities & Testing

### `helper_function.py`

- **fetch_data_from_api(city)**: Fetches weather data for a given city from the WeatherStack API.
- **connect_to_postgres()**: Connects to the local PostgreSQL database.
- **create_schema_table(conn)**: Creates the `weather` schema and `weather_report` table if they do not exist.
- **insert_data_into_table(conn, data)**: Inserts weather data into the `weather_report` table.
- **main()**: Orchestrates the fetching and loading of weather data for a list of cities.

### `test_helper_function.py`

- Location: `airflow/utilities/test_helper_function.py`
- Contains unit tests for all main functions in `helper_function.py` using Python's `unittest` and `unittest.mock` libraries.
- To run the tests:
   ```bash
   python -m unittest airflow/utilities/test_helper_function.py
   ```

---

## dbt Models

- **Incremental Model**: `weather_del_ins.sql` is an incremental model that deduplicates weather data by `id` and uses a custom pre-hook for delete/insert logic.
- **Manifest**: The dbt manifest is used by Airflow to dynamically generate tasks for each model.

---

## Setup & Usage

### Prerequisites

- Python 3.x
- PostgreSQL
- Airflow
- dbt
- WeatherStack API key

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/dbt-postgres-airflow.git
   cd dbt-postgres-airflow
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up your `.env` file with your WeatherStack API key:
   ```
   WEATHERSTACK_API_KEY=your_api_key_here
   ```

4. Configure Airflow and initialize the database:
   ```bash
   airflow db init
   ```

5. Start Airflow webserver and scheduler:
   ```bash
   airflow webserver
   airflow scheduler
   ```

6. Trigger the DAG from the Airflow UI or CLI.

---

## Notes

- The project uses a 10-second delay between API requests to avoid rate limiting.
- All database credentials and API keys should be managed securely (e.g., with environment variables or Airflow connections).
- The dbt project is expected to be compiled before running the Airflow DAG.

---

## Troubleshooting

- **Too Many Requests**: If you hit API rate limits, increase the delay between requests or upgrade your API plan.
- **Database Connection Errors**: Ensure PostgreSQL is running and credentials are correct.
- **Airflow Task Failures**: Check Airflow logs for detailed error messages.

---

## License

MIT License (or your chosen license)

---


## Authors

- Canh Nguyen Xuan


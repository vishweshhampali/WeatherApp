from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import duckdb
import json
import os
import shutil


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15, 1, 15),  # Start at 3:00 PM UTC today
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data',
    schedule_interval='0 */1 * * *',  # Runs every 15 minutes
    catchup=False  # Ensures no DAG runs are backfilled before 9:15 AM today
)

# Function to extract weather data from Open-Meteo API and save to JSON
def extract_weather_data(**kwargs):
    """
    Extracts weather data from Open-Meteo API for Central London and saves it to a JSON file.
    """

    def fetch_weather_data(latitude, longitude):
        """
        Fetching weather data from Open-Meteo API based on the given latitude and longitude.
        """
        url = 'https://api.open-meteo.com/v1/forecast'
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'current_weather': 'true',
            'hourly': 'temperature_2m,weathercode',  # Adding forecasted weather data for future use
            'timezone': 'Europe/London',
            'forecast_days': 1
        }

        response = requests.get(url, params=params)

        # Checking if the request was successful
        if response.status_code == 200:
            weather_data = response.json()
            return weather_data
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None

    def save_to_json(data, filename):
        """
        Save the weather data to a JSON file with the given filename in the 'data' directory.
        """
        # Create the 'data' directory if it doesn't exist
        data_dir = '/opt/airflow/data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Defining the full path for the file
        filepath = os.path.join(data_dir, filename)

        # Saving the JSON data to the file
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)

        print(f"Weather data saved to {filepath}")

    # Weather data for Central London (51.5074° N, 0.1278° W)
    latitude = 51.5074
    longitude = -0.1278
    weather_data = fetch_weather_data(latitude, longitude)

    if weather_data:
        # Creating a filename using the current timestamp
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{current_time}_weather.json"

        # Saving the weather data to a JSON file
        save_to_json(weather_data, filename)

# Function to create DuckDB tables and insert WMO weather codes
def create_weather_tables(**kwargs):
    # Ensure the directory for the DuckDB database exists
    db_dir = '/opt/airflow/db'  # This should be the path inside the Docker container
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    # Path to the DuckDB database
    db_path = os.path.join(db_dir, "weather_data.db")

    # Connect to DuckDB (this will create the database if it doesn't exist)
    con = duckdb.connect(db_path)

    # Create sequences and tables
    con.execute('''
        CREATE SEQUENCE IF NOT EXISTS seq_location_id START 1;
        CREATE SEQUENCE IF NOT EXISTS seq_weather_id START 1;
        CREATE SEQUENCE IF NOT EXISTS seq_api_call_id START 1;

        CREATE TABLE IF NOT EXISTS location (
            location_id INTEGER PRIMARY KEY,
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            elevation DOUBLE,
            UNIQUE(latitude, longitude)
        );

        CREATE TABLE IF NOT EXISTS weather_condition (
            weather_code INTEGER PRIMARY KEY,
            description VARCHAR(255)
        );

        CREATE TABLE IF NOT EXISTS weather_fact (
            weather_id INTEGER PRIMARY KEY,
            api_call_id INTEGER NOT NULL,
            location_id INTEGER NOT NULL,
            record_creation_time TIMESTAMP NOT NULL,
            time TIMESTAMP NOT NULL,
            temperature_celsius DOUBLE,
            windspeed_kmh DOUBLE,
            wind_direction_deg INTEGER,
            is_day BOOLEAN,
            weather_code INTEGER,
            is_forecast BOOLEAN,
            FOREIGN KEY (location_id) REFERENCES location(location_id),
            FOREIGN KEY (weather_code) REFERENCES weather_condition(weather_code)
        );
    ''')
    # Commit the table creation
    con.commit()

    print("Sequences and tables created successfully in DuckDB!")

    # WMO Weather Codes and Descriptions
    wmo_weather_codes = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Drizzle: Light intensity",
        53: "Drizzle: Moderate intensity",
        55: "Drizzle: Dense intensity",
        56: "Freezing Drizzle: Light intensity",
        57: "Freezing Drizzle: Dense intensity",
        61: "Rain: Slight intensity",
        63: "Rain: Moderate intensity",
        65: "Rain: Heavy intensity",
        66: "Freezing Rain: Light intensity",
        67: "Freezing Rain: Heavy intensity",
        71: "Snow fall: Slight intensity",
        73: "Snow fall: Moderate intensity",
        75: "Snow fall: Heavy intensity",
        77: "Snow grains",
        80: "Rain showers: Slight intensity",
        81: "Rain showers: Moderate intensity",
        82: "Rain showers: Violent intensity",
        85: "Snow showers: Slight intensity",
        86: "Snow showers: Heavy intensity",
        95: "Thunderstorm: Slight or moderate",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail"
    }

    # Insert the WMO weather codes into the weather_condition table if not already present
    for code, description in wmo_weather_codes.items():
        result = con.execute("SELECT weather_code FROM weather_condition WHERE weather_code=?", (code,)).fetchone()
        if result is None:
            con.execute("INSERT INTO weather_condition (weather_code, description) VALUES (?, ?)", (code, description))

    # Commit after inserting the WMO weather codes
    con.commit()
    print("WMO weather codes inserted into weather_condition table successfully!")


def process_weather_files():
    """
    Processes all JSON weather data files in the 'data' directory, inserts the data into DuckDB,
    and moves the processed files to the 'backup' directory.
    """
    # Define directories
    data_dir = '/opt/airflow/data'
    backup_dir = '/opt/airflow/backup'

    # Ensure the backup directory exists, if not, create it
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    # Ensure the directory for the DuckDB database exists
    db_dir = '/opt/airflow/db'  # This should be the path inside the Docker container
    db_path = os.path.join(db_dir, "weather_data.db")

    # Connect to DuckDB (this will create the database if it doesn't exist)
    con = duckdb.connect(db_path)

    # Get the next `api_call_id` for this run
    api_call_id = con.execute("SELECT nextval('seq_api_call_id')").fetchone()[0]
    print(f"API call ID for this run: {api_call_id}")

    # List all files in the data directory
    files = os.listdir(data_dir)

    # Filter for JSON files
    json_files = [f for f in files if f.endswith('.json')]

    # Process each JSON file
    for json_file in json_files:
        json_file_path = os.path.join(data_dir, json_file)

        # Load the JSON data from the file
        with open(json_file_path, 'r') as f:
            weather_data = json.load(f)

        # Extract location data from the JSON file
        latitude = weather_data["latitude"]
        longitude = weather_data["longitude"]
        elevation = weather_data["elevation"]

        # Check if the location already exists
        location_id = con.execute("SELECT location_id FROM location WHERE latitude=? AND longitude=?",
                                  (latitude, longitude)).fetchone()

        if location_id is None:
            # Insert new location and fetch the new location_id
            con.execute("INSERT INTO location VALUES (nextval('seq_location_id'), ?, ?, ?)",
                        (latitude, longitude, elevation))
            location_id = con.execute("SELECT location_id FROM location WHERE latitude=? AND longitude=?",
                                      (latitude, longitude)).fetchone()[0]
        else:
            location_id = location_id[0]

        # Extract current weather data from the JSON
        current_weather = weather_data["current_weather"]
        record_creation_time = datetime.now()
        current_time = datetime.strptime(current_weather["time"], "%Y-%m-%dT%H:%M")
        temperature = current_weather["temperature"]
        windspeed = current_weather["windspeed"]
        wind_direction = current_weather["winddirection"]
        is_day = current_weather["is_day"]
        weather_code = current_weather["weathercode"]

        # Insert current weather into weather_fact
        con.execute('''
            INSERT INTO weather_fact (weather_id, api_call_id, location_id, record_creation_time, time, temperature_celsius, windspeed_kmh, wind_direction_deg, is_day, weather_code, is_forecast)
            VALUES (nextval('seq_weather_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
        api_call_id, location_id, record_creation_time, current_time, temperature, windspeed, wind_direction, is_day, weather_code,
        False))

        # Insert hourly forecast data
        hourly_data = weather_data["hourly"]
        hourly_times = hourly_data["time"]
        hourly_temperatures = hourly_data["temperature_2m"]
        hourly_weathercodes = hourly_data["weathercode"]

        for i in range(len(hourly_times)):
            forecast_time = datetime.strptime(hourly_times[i], "%Y-%m-%dT%H:%M")

            # Compare with current time (with minute, second, and microsecond set to 0)
            if forecast_time > current_time.replace(minute=0, second=0, microsecond=0):
                forecast_temperature = hourly_temperatures[i]
                forecast_weather_code = hourly_weathercodes[i]

                # Insert hourly forecast into weather_fact
                con.execute('''
                    INSERT INTO weather_fact (weather_id, api_call_id, location_id, record_creation_time, time, temperature_celsius, windspeed_kmh, wind_direction_deg, is_day, weather_code, is_forecast)
                    VALUES (nextval('seq_weather_id'), ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                ''', (
                api_call_id, location_id, record_creation_time, forecast_time, forecast_temperature, forecast_weather_code, True))

        con.commit()
        print(f"Data from {json_file} inserted successfully!")

        # Move the processed file to the backup directory
        shutil.move(json_file_path, os.path.join(backup_dir, json_file))
        print(f"{json_file} moved to backup directory.")


def export_to_parquet():
    db_dir = '/opt/airflow/db'
    parquet_dir = '/opt/airflow/parquets'

    if not os.path.exists(parquet_dir):
        os.makedirs(parquet_dir)

    db_path = os.path.join(db_dir, "weather_data.db")
    con = duckdb.connect(db_path)

    # Export each table to a Parquet file
    tables = ['location', 'weather_condition', 'weather_fact']
    for table in tables:
        parquet_path = os.path.join(parquet_dir, f"{table}.parquet")
        con.execute(f"COPY {table} TO '{parquet_path}' (FORMAT 'parquet')")
        print(f"Exported {table} to {parquet_path}")


# Define the tasks

create_tables_task = PythonOperator(
    task_id='create_weather_tables',
    python_callable=create_weather_tables,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_weather_data',
    provide_context=True,
    python_callable=extract_weather_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='process_weather_files',
    provide_context=True,
    python_callable=process_weather_files,
    dag=dag,
)

export_parquet_task = PythonOperator(
    task_id='export_to_parquet',
    python_callable=export_to_parquet,
    dag=dag,
)

# Set task dependencies
create_tables_task >> extract_task >> load_task >> export_parquet_task


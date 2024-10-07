# Weather Dashboard and Data Pipeline using Apache Airflow, DuckDB, and Power BI

This project demonstrates how to build an end-to-end weather data pipeline using the Open-Meteo API, Apache Airflow, DuckDB, and Power BI. The pipeline automates data extraction, transformation, storage, and export to Parquet files for visualization.

## Features
- Extracts weather data from the Open-Meteo API (current and forecast).
- Stores data in DuckDB for efficient querying.
- Automates data ingestion and storage using Apache Airflow.
- Exports weather data to Parquet files for Power BI.
- Processes historical weather data and maintains a backup.

## Technologies Used
- **Apache Airflow:** For automating and scheduling the data pipeline.
- **DuckDB:** An embedded SQL database used to store weather data.
- **Power BI:** For visualizing the weather data from the exported Parquet files.
- **Python:** Core logic for API calls, data extraction, and loading.
- **Docker:** To containerize and run Airflow and DuckDB.

## Setup Instructions

### Prerequisites
- **Docker:** Make sure Docker is installed. You can download it from [here](https://www.docker.com/products/docker-desktop).
- **Git:** Ensure Git is installed on your machine.

### Step 1: Clone the Repository
```bash
git clone https://github.com/yourusername/weather-data-pipeline.git
cd weather-data-pipeline
```
### Step 2: Set up Docker and Airflow
- Navigaye to the docker/ folder
- The docker-compose.yaml file will spin up Airflow and DuckDB with the necessary configurations.
- Run the following command to start the Airflow services:
  ```bash
  docker-compose up
  ```
- Access Airflow's UI by going to http://localhost:8080 in your browser. The default credentials are airflow/airflow.

### Step 3: Load the DAG
- The DAG file is located in the airflow_dag/ folder.
- Airflow will automatically detect and load the DAG from the airflow_dag/ directory.
- Navigate to the Airflow UI, activate the DAG, and trigger it manually to start the pipeline.

### Step 4: Check Data Insertion
- The raw weather data is saved as JSON files in the data/ directory.
- DuckDB database files are stored in the db/ folder.
- Processed files are backed up in the backup/ directory.

### Step 5: Export Parquet Files
- The Parquet files will be exported to the parquets/ directory.
- You can connect Power BI to the Parquet files for data visualization.

## How it works
- Extract: Weather data is extracted from Open-Meteo’s API for a specific location (Central London).
- Load & Process: The data is processed and backed up, then inserted into the DuckDB tables.
- Export: The tables are exported as Parquet files and are ready for use in Power BI.

## Contributions
Contributions are welcome! Please fork the repository and submit a pull request with your improvements or feature additions.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Authors
[Vishwesh Hampali](https://github.com/vishweshhampali)

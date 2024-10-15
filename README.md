# Weather Dashboard and Data Pipeline using Apache Airflow, DuckDB, and Power BI

This project demonstrates an end-to-end weather data pipeline, built using the Open-Meteo API, Apache Airflow, DuckDB, and Power BI. The pipeline automates weather data's extraction, transformation, storage, and export for visualization in Power BI.

## Features
- Weather Data Extraction: Fetches current and forecasted weather data from the Open-Meteo API.
- Storage with DuckDB: Efficient querying and storage of weather data.
- Automated Workflow with Apache Airflow: Scheduling weather data ingestion and processing.
- Data Export: Weather data is exported as Parquet files for Power BI integration.
- Historical Data Processing: Maintains backups of processed weather data.

## Technologies Used
- **Apache Airflow:** Automates and schedules data pipeline tasks.
- **DuckDB:** Lightweight SQL database for weather data storage.
- **Power BI:** Visualizes weather data from exported Parquet files.
- **Python:** Core logic for data extraction, transformation, and loading.
- **Docker:** Containerizes and manages Airflow and DuckDB.

## Project Workflow
- **Extract:** Weather data is retrieved from the Open-Meteo API for Central London.
- **Load & Process:** Data is processed, backed up, and inserted into DuckDB tables.
- **Export:** Data is exported as Parquet files, which are then visualized in Power BI.

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
- Navigate to the docker/ folder.
- The docker-compose.yaml file sets up Airflow and DuckDB with the necessary configurations.
- Run the following command to start the Airflow services:
  ```bash
  docker-compose up
  ```
- Once started, access Airflow’s UI by going to http://localhost:8080 in your browser. The default credentials are airflow/airflow.

### Step 3: Load the DAG
- The DAG file is located in the airflow_dag/ folder.
- Airflow will automatically detect and load the DAG from the airflow_dag/ directory.
- Open the Airflow UI, activate the DAG, and trigger it manually to start the data pipeline.

### Step 4: Verify Data Insertion
- Extracted weather data is saved as JSON files in the data/ directory.
- The DuckDB database files are stored in the db/ folder.
- Processed files are backed up in the backup/ directory.

### Step 5: Export Parquet Files
- Parquet files will be exported to the parquets/ directory.
- Use these files to build data visualizations in Power BI.

## File Overview
- DAG File: The ETL pipeline logic is defined in the weather_etl_DAG.py file (weather_etl_DAG).
- Docker Configuration: Both the docker-compose.yaml and the Dockerfile (weather_etl_DAG) configure the containerized environment for the Airflow services.

## Power BI Visualization
The exported Parquet files can be loaded into Power BI to generate weather visualizations. Here’s an example of the Power BI dashboard generated:

## Contributions
Contributions are welcome! Fork the repository, make improvements or add features, and submit a pull request.

## License
This project is licensed under the MIT License - see the LICENSE file for more details.

## Authors
[Vishwesh Hampali](https://github.com/vishweshhampali)

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

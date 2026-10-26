# Real-Time Weather Streaming Data Dashboard

## Project Overview

This project implements a real-time weather data streaming dashboard using WeatherAPI, Apache Kafka, Apache Spark Structured Streaming, MongoDB, and Streamlit.

The application retrieves current weather information for Quezon City, Philippines, publishes the data to Kafka, processes streaming messages using Spark, and stores historical weather records in MongoDB. A Streamlit dashboard provides real-time visualization and historical data analysis.

## Features

- **Real-time data collection:** Retrieves current weather information from WeatherAPI.
- **Kafka messaging:** Publishes weather records to the `streaming-data` topic.
- **Stream processing:** Uses Spark Structured Streaming to parse and process incoming Kafka messages.
- **Historical storage:** Uses MongoDB to store and retrieve weather records.
- **Interactive dashboard:** Displays temperature readings, tables, and charts.
- **Historical analysis:** Supports time-range filtering and hourly, daily, and weekly aggregation.
- **Automatic refresh:** Updates the dashboard at a configurable interval.
- **Error handling:** Handles API request failures, invalid records, and connection errors in the relevant components.

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Application development |
| WeatherAPI | Current weather data source |
| Apache Kafka | Message streaming |
| Apache Spark Structured Streaming | Stream processing |
| MongoDB | Historical data storage |
| Streamlit | Dashboard interface |
| Plotly | Data visualization |
| Docker Compose | Containerized services |

## System Architecture

The project uses two data paths after Kafka receives the weather messages:

**Real-time dashboard**

WeatherAPI → Python Producer → Kafka (`streaming-data`) → Streamlit Kafka Consumer → Real-time View

**Historical data**

WeatherAPI → Python Producer → Kafka (`streaming-data`) → MongoDB Consumer → MongoDB → Streamlit Historical View

**Spark processing**

Kafka (`streaming-data`) → Spark Structured Streaming → Console Output

The Spark processor runs separately from the MongoDB consumer. In the current implementation, Spark displays processed messages in the console, while the separate consumer saves the records to MongoDB.

## Project Files

- `app.py` — Streamlit dashboard, Kafka consumption, historical queries, charts, and aggregation.
- `producer.py` — Retrieves weather data from WeatherAPI and sends messages to Kafka.
- `spark_processor.py` — Processes Kafka messages using Spark Structured Streaming.
- `mongo_consumer.py` — Consumes Kafka messages and stores records in MongoDB.
- `docker-compose.yml` — Defines the Kafka, Spark, and MongoDB services.
- `requirements.txt` — Lists Python dependencies.
- `.env` — Stores the WeatherAPI key. This file must remain private.

## Prerequisites

Install or prepare the following:

- Python 3.10 or another compatible Python version.
- Docker Desktop.
- Git.
- A WeatherAPI account and API key.
- The project's Python dependencies.

The project has been tested in a Windows environment using PowerShell, Docker, and Visual Studio Code.

## Installation and Configuration

### 1. Clone the repository

Clone your fork of the professor's repository:

```bash
git clone YOUR_FORK_REPOSITORY_URL
cd big-data-streamlit-template
```

Replace `YOUR_FORK_REPOSITORY_URL` with the URL of your actual fork.

### 2. Create and activate a virtual environment

In Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks script activation, use the execution-policy instructions appropriate for your local environment.

### 3. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 4. Configure the WeatherAPI key

Create a `.env` file in the project root directory:

```dotenv
WEATHER_API_KEY=YOUR_WEATHERAPI_KEY
```

Replace the placeholder with your own API key. Do not commit or submit this file publicly.

### 5. Start the Docker services

If the project contains the provided Docker Compose configuration, run:

```powershell
docker compose up -d
```

Check the running containers:

```powershell
docker ps
```

The project uses the following local service addresses:

- Kafka: `localhost:9092`
- Spark Master UI: `http://localhost:8080`
- Spark Worker UI: `http://localhost:8081`
- MongoDB: `mongodb://localhost:27017/`
- Streamlit Dashboard: `http://localhost:8501`

## Running the Application

Keep the required services running while testing the application.

### 1. Start the MongoDB consumer

Open a terminal in the project directory and run:

```powershell
python mongo_consumer.py
```

This process reads weather messages from Kafka and saves valid records to MongoDB.

### 2. Start the Spark processor

The Spark processor must be available inside the Spark container at `/tmp/spark_processor.py`, as configured in the current development setup.

Run the following command when the file has been copied into the container and the Docker services are running:

```powershell
docker exec weather-spark /opt/spark/bin/spark-submit --master spark://spark:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /tmp/spark_processor.py
```

The Spark application reads the `streaming-data` topic and displays processed records in the console. Keep this process running during testing.

### 3. Start the weather producer

Open another terminal and run:

```powershell
python producer.py
```

The producer retrieves current weather data and publishes messages to Kafka. By default, it sends approximately one message every 15 seconds.

To run a limited test:

```powershell
python producer.py --duration 60
```

This runs the producer for approximately 60 seconds, subject to API request and processing time.

### 4. Launch the Streamlit dashboard

Open another terminal and run:

```powershell
python -m streamlit run app.py
```

Open the local address displayed in the terminal, normally:

`http://localhost:8501`

## Dashboard Usage

### Real-Time Streaming View

The real-time view consumes available Kafka messages and displays weather readings through metrics, a line chart, and a raw-data table.

### Historical Data View

The historical view retrieves records from MongoDB. Users can select a time range, filter the metric type, and choose hourly, daily, or weekly aggregation.

The number of available historical records depends on the messages collected and stored during the producer and consumer runs.

### Data Export

The dashboard has been used to download CSV exports of the displayed data. Excel workbook export (`.xlsx`) should only be considered available if the corresponding download feature has been implemented and tested.

## Testing and Verification

The following checks were performed during development:

- Kafka, Spark Master, Spark Worker, and MongoDB containers were running.
- The Kafka producer successfully sent four weather messages during a 60-second test.
- Spark Master displayed the `WeatherKafkaStreamProcessor` application in the RUNNING state.
- MongoDB contained four weather records during the initial historical-data test.
- The dashboard displayed the historical records and temperature trend.
- Hourly, daily, and weekly aggregation options were tested.
- Three CSV exports were downloaded from the dashboard.

These results demonstrate the main components of the prototype. Further testing with more records is recommended to evaluate longer streaming sessions and aggregation behavior.

## Troubleshooting

**Kafka connection error**

- Confirm that the Kafka container is running.
- Check that the dashboard and producer use `localhost:9092`.
- Verify that the Kafka topic is named `streaming-data`.

**No historical records appear**

- Confirm that MongoDB is running.
- Make sure `mongo_consumer.py` is running and receiving messages.
- Check that the selected time range includes the timestamps of stored records.

**Spark application fails to start**

- Confirm that the Spark Master and Worker containers are running.
- Verify that `/tmp/spark_processor.py` exists inside the Spark container.
- Check that the Spark Kafka connector package can be downloaded or is already cached.

**WeatherAPI request fails**

- Check the internet connection.
- Verify that `WEATHER_API_KEY` is correctly configured in `.env`.
- Confirm that the API key is valid and that the API request is permitted.

## Limitations and Future Improvements

The current prototype focuses on current weather data and a limited number of historical records. Spark currently outputs processed messages to the console, while MongoDB storage is handled by a separate consumer.

Future improvements may include connecting Spark's processed output directly to a storage layer, testing with a larger dataset, expanding historical visualizations, adding verified Excel or PDF export features, and improving monitoring and recovery for service failures.

## Conclusion

The project demonstrates a streaming data application that combines an external weather API, Kafka messaging, Spark Structured Streaming, MongoDB storage, and an interactive Streamlit dashboard. It provides practical experience in data collection, stream processing, historical storage, and visualization.

The implementation successfully demonstrated message transmission, Spark application execution, historical record retrieval, and dashboard aggregation. Further testing and integration can improve the application's reliability and extend its capabilities.

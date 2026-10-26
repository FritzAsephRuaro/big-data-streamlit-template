"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    """Configure Kafka, historical storage, and dashboard controls."""
    st.sidebar.title("Dashboard Controls")
    st.sidebar.subheader("Data Source Configuration")

    kafka_broker = st.sidebar.text_input(
        "Kafka Broker",
        value="localhost:9092",
        help="Kafka broker address. Use localhost:9092 when Streamlit runs on your computer.",
    )
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic",
        value="streaming-data",
        help="Kafka topic containing weather records.",
    )

    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["MongoDB", "HDFS"],
        index=0,
        help="Historical weather records are currently queried from MongoDB.",
    )

    return {
        "kafka_broker": kafka_broker.strip(),
        "kafka_topic": kafka_topic.strip(),
        "storage_type": storage_type,
    }

def generate_sample_data():
    """Return an empty, correctly structured frame when no live records exist.

    The dashboard intentionally avoids fabricated sample readings so charts
    represent actual data from Kafka or MongoDB only.
    """
    return pd.DataFrame(
        columns=["timestamp", "value", "metric_type", "sensor_id"]
    )

def consume_kafka_data(config):
    """Read weather records from the Kafka topic for the dashboard."""
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")

    consumer = None

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[kafka_broker],
            group_id=None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=3000,
        )

        # Subscribe and initialize the topic partition assignment.
        consumer.subscribe([kafka_topic])
        consumer.poll(timeout_ms=1000)

        # Read existing records as well as newly arriving records.
        consumer.seek_to_beginning()

        messages = []
        start_time = time.time()

        while time.time() - start_time < 5 and len(messages) < 100:
            batches = consumer.poll(timeout_ms=500)

            for batch in batches.values():
                for message in batch:
                    data = message.value

                    if not isinstance(data, dict):
                        continue

                    required = [
                        "timestamp", "value", "metric_type", "sensor_id"
                    ]
                    if not all(key in data for key in required):
                        continue

                    try:
                        timestamp_str = str(data["timestamp"])
                        if timestamp_str.endswith("Z"):
                            timestamp_str = timestamp_str[:-1] + "+00:00"

                        timestamp = datetime.fromisoformat(timestamp_str)
                        if timestamp.tzinfo is not None:
                            timestamp = timestamp.astimezone().replace(tzinfo=None)

                        messages.append({
                            "timestamp": timestamp,
                            "value": float(data["value"]),
                            "metric_type": data["metric_type"],
                            "sensor_id": data["sensor_id"],
                        })
                    except (ValueError, TypeError):
                        continue

        if not messages:
            return pd.DataFrame(
                columns=["timestamp", "value", "metric_type", "sensor_id"]
            )

        return (
            pd.DataFrame(messages)
            .drop_duplicates()
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    except Exception as e:
        st.error(f"Kafka consumer error: {e}")
        return None

    finally:
        if consumer is not None:
            consumer.close()



def query_historical_data(time_range="1h", metrics=None):
    """
    Retrieve historical weather records from MongoDB
    based on the selected time range and metric types.
    """
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    empty_columns = [
        "timestamp",
        "value",
        "metric_type",
        "sensor_id"
    ]

    client = None

    try:
        client = MongoClient(
            "mongodb://localhost:27017/",
            serverSelectionTimeoutMS=3000
        )

        # Check the MongoDB connection
        client.admin.command("ping")

        # Select the database and collection
        db = client["weather_dashboard"]
        collection = db["weather_records"]

        # Set the selected historical time range
        now = datetime.now().astimezone()

        range_map = {
            "1h": timedelta(hours=1),
            "24h": timedelta(hours=24),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30)
        }

        start_time = now - range_map.get(
            time_range,
            timedelta(hours=1)
        )

        query = {
            "timestamp": {
                "$gte": start_time
            }
        }

        # Filter by selected metric types
        if metrics and "all" not in metrics:
            query["metric_type"] = {
                "$in": metrics
            }

        # Retrieve records in chronological order
        records = list(
            collection.find(
                query,
                {"_id": 0}
            ).sort("timestamp", 1)
        )

        if not records:
            return pd.DataFrame(columns=empty_columns)

        df = pd.DataFrame(records)

        # Convert fields to usable data types
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
            utc=True
        )

        df["value"] = pd.to_numeric(
            df["value"],
            errors="coerce"
        )

        df = df.dropna(
            subset=["timestamp", "value"]
        )

        return df

    except (PyMongoError, ValueError, TypeError, KeyError) as e:
        st.error(f"MongoDB historical query failed: {e}")
        return pd.DataFrame(columns=empty_columns)

    finally:
        if client is not None:
            client.close()



def display_real_time_view(config, refresh_interval):
    """Display live weather records consumed from Kafka."""
    st.header("📈 Real-time Streaming Dashboard")

    refresh_state = st.session_state.refresh_state
    status = "🟢 Enabled" if refresh_state["auto_refresh"] else "🔴 Disabled"
    st.info(f"**Auto-refresh:** {status} — updates every {refresh_interval} seconds")

    with st.spinner("Fetching real-time data from Kafka..."):
        real_time_data = consume_kafka_data(config)

    if real_time_data is None:
        st.error(
            "Could not read from Kafka. Check that Kafka is running and that "
            "the broker and topic settings in the sidebar are correct."
        )
        return

    st.success("Kafka query completed.")

    if real_time_data.empty:
        st.warning(
            "No valid weather messages were found in this Kafka topic. "
            "Run the producer and then refresh the dashboard."
        )
        return

    real_time_data = real_time_data.copy()
    real_time_data["timestamp"] = pd.to_datetime(
        real_time_data["timestamp"], errors="coerce"
    )
    real_time_data["value"] = pd.to_numeric(
        real_time_data["value"], errors="coerce"
    )
    real_time_data = (
        real_time_data.dropna(subset=["timestamp", "value"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    if real_time_data.empty:
        st.warning("Kafka messages were received, but none had valid timestamps and numeric values.")
        return

    st.subheader("📊 Live Data Metrics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Records Received", len(real_time_data))
    with col2:
        st.metric("Latest Temperature", f"{real_time_data['value'].iloc[-1]:.2f} °C")
    with col3:
        first_time = real_time_data["timestamp"].min().strftime("%H:%M:%S")
        last_time = real_time_data["timestamp"].max().strftime("%H:%M:%S")
        st.metric("Data Range", f"{first_time} – {last_time}")

    st.subheader("📈 Real-time Temperature Trend")
    fig = px.line(
        real_time_data,
        x="timestamp",
        y="value",
        color="sensor_id" if real_time_data["sensor_id"].nunique() > 1 else None,
        title=f"WeatherAPI Temperature Readings ({len(real_time_data)} records)",
        labels={"value": "Temperature (°C)", "timestamp": "Time", "sensor_id": "Source"},
        markers=True,
        template="plotly_white",
    )
    fig.update_layout(xaxis_title="Time", yaxis_title="Temperature (°C)", hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("📋 View Raw Kafka Data"):
        st.dataframe(
            real_time_data.sort_values("timestamp", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

def display_historical_view(config):
    """Display historical weather records from MongoDB."""
    st.header("📊 Historical Data Analysis")

    if config.get("storage_type", "MongoDB") == "HDFS":
        st.warning(
            "HDFS is not connected in this project configuration. "
            "Select MongoDB in the sidebar to view the historical records currently stored by the consumer."
        )
        return

    with st.expander("ℹ️ Implementation Guide"):
        st.write(
            "Historical weather records are retrieved from MongoDB. "
            "Use the filters to explore the selected time period "
            "and view temperature trends."
        )

    # Interactive controls
    st.subheader("Data Filters")
    col1, col2, col3 = st.columns(3)

    with col1:
        time_range = st.selectbox(
            "Time Range",
            ["1h", "24h", "7d", "30d"],
            help="Select the historical time period."
        )

    with col2:
        metric_type = st.selectbox(
            "Metric Type",
            ["temperature", "humidity", "all"],
            help="Temperature is stored as value; humidity is a separate field."
        )

    with col3:
        aggregation = st.selectbox(
            "Aggregation",
            ["raw", "hourly", "daily", "weekly"],
            help="Choose how to group historical readings."
        )

    # Retrieve the historical records from MongoDB
    metrics = (
        ["temperature"]
        if metric_type == "temperature"
        else None
    )

    historical_data = query_historical_data(time_range, metrics)

    if historical_data is None or historical_data.empty:
        st.warning(
            "No historical records were found for this time range. "
            "Make sure the Kafka-to-MongoDB consumer is running "
            "and that MongoDB contains recent weather records."
        )
        return

    historical_data = historical_data.copy()

    historical_data["timestamp"] = pd.to_datetime(
        historical_data["timestamp"],
        errors="coerce",
        utc=True
    )
    historical_data["value"] = pd.to_numeric(
        historical_data["value"],
        errors="coerce"
    )

    historical_data = historical_data.dropna(
        subset=["timestamp", "value"]
    )

    if historical_data.empty:
        st.warning("No valid numeric weather readings are available.")
        return

    # Select the metric to display.
    # The current producer publishes temperature as its value.
    if metric_type == "humidity":
        if "humidity" not in historical_data.columns:
            st.warning("Humidity values are not available in these records.")
            return

        historical_data["value"] = pd.to_numeric(
            historical_data["humidity"],
            errors="coerce"
        )
        chart_label = "Humidity (%)"

    else:
        historical_data = historical_data[
            historical_data["metric_type"] == "temperature"
        ]
        chart_label = "Temperature (°C)"

    historical_data = historical_data.dropna(
        subset=["timestamp", "value"]
    )

    if historical_data.empty:
        st.warning("No records are available for the selected metric.")
        return

    # Apply aggregation if selected.
    chart_data = historical_data.sort_values("timestamp").copy()

    if aggregation != "raw":
        frequency_map = {
            "hourly": "h",
            "daily": "D",
            "weekly": "W"
        }

        frequency = frequency_map[aggregation]

        chart_data = (
            chart_data
            .set_index("timestamp")
            .resample(frequency)["value"]
            .mean()
            .dropna()
            .reset_index()
        )

    # Display the historical records
    st.subheader("Historical Data Table")

    display_columns = [
        column for column in [
            "timestamp",
            "value",
            "metric_type",
            "sensor_id",
            "location",
            "condition",
            "humidity",
            "wind_kph",
            "feelslike_c"
        ]
        if column in historical_data.columns
    ]

    st.dataframe(
        historical_data[display_columns].sort_values(
            "timestamp",
            ascending=False
        ),
        width="stretch",
        hide_index=True
    )

    # Display historical trends
    st.subheader("Historical Trends")

    if aggregation == "raw":
        fig = px.line(
            chart_data,
            x="timestamp",
            y="value",
            title=f"Historical {metric_type.title()} Readings",
            labels={
                "timestamp": "Time",
                "value": chart_label
            },
            markers=True,
            template="plotly_white"
        )
    else:
        fig = px.line(
            chart_data,
            x="timestamp",
            y="value",
            title=f"{aggregation.title()} Average {metric_type.title()}",
            labels={
                "timestamp": "Time",
                "value": chart_label
            },
            markers=True,
            template="plotly_white"
        )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, width="stretch")

    # Summary statistics
    st.subheader("Data Summary")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Records", len(historical_data))

    with col2:
        st.metric(
            "Average Reading",
            f"{historical_data['value'].mean():.2f}"
        )

    with col3:
        variability = historical_data["value"].std()
        st.metric(
            "Standard Deviation",
            f"{variability:.2f}" if pd.notna(variability) else "N/A"
        )

    start_date = historical_data["timestamp"].min()
    end_date = historical_data["timestamp"].max()

    st.caption(
        f"Data period: {start_date.strftime('%Y-%m-%d %H:%M UTC')} "
        f"to {end_date.strftime('%Y-%m-%d %H:%M UTC')}"
    )

def main():
    """Configure and run the streaming dashboard."""
    st.title("🚀 Streaming Data Dashboard")

    with st.expander("📋 Project Instructions"):
        st.markdown(
            """
            **Big Data Streaming Dashboard**

            - **Real-time data:** consume weather messages from Kafka.
            - **Historical data:** query saved records from MongoDB.
            - **Visualizations:** explore temperature trends and summary statistics.
            - **Error handling:** show connection or data-validation messages when a source is unavailable.
            """
        )

    if "refresh_state" not in st.session_state:
        st.session_state.refresh_state = {
            "last_refresh": datetime.now(),
            "auto_refresh": True,
        }

    config = setup_sidebar()

    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state["auto_refresh"] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state["auto_refresh"],
        help="Automatically refresh the dashboard.",
    )

    # Always define the interval, even when auto-refresh is disabled.
    refresh_interval = 15
    if st.session_state.refresh_state["auto_refresh"]:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15,
            help="Set how often the dashboard refreshes.",
        )
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

    if st.sidebar.button("🔄 Manual Refresh"):
        st.rerun()

    st.session_state.refresh_state["last_refresh"] = datetime.now()
    st.sidebar.markdown("---")
    st.sidebar.metric(
        "Last Refresh",
        st.session_state.refresh_state["last_refresh"].strftime("%H:%M:%S"),
    )

    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    with tab1:
        display_real_time_view(config, refresh_interval)
    with tab2:
        display_historical_view(config)


if __name__ == "__main__":
    main()
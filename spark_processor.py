
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructType,
    StructField,
)


def main():
    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:29092",
    )
    kafka_topic = os.getenv("KAFKA_TOPIC", "streaming-data")

    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("metric_type", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("condition", StringType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("wind_kph", DoubleType(), True),
        StructField("feelslike_c", DoubleType(), True),
    ])

    spark = (
        SparkSession.builder
        .appName("WeatherKafkaStreamProcessor")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    weather = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("weather"))
        .select("weather.*")
        .withColumn(
            "event_time",
            to_timestamp(col("timestamp"))
        )
        .filter(
            col("value").isNotNull()
            & col("metric_type").isNotNull()
        )
    )

    query = (
        weather.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "/tmp/weather-spark-checkpoint")
        .start()
    )

    print(f"Spark is processing Kafka topic: {kafka_topic}")
    query.awaitTermination()


if __name__ == "__main__":
    main()

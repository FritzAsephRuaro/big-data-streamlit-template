
"""
Read weather messages from Kafka and save them to MongoDB.
"""

import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "streaming-data"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "weather_dashboard"
MONGO_COLLECTION = "weather_records"


def main():
    mongo_client = None
    consumer = None

    try:
        # Connect to MongoDB
        mongo_client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000
        )
        mongo_client.admin.command("ping")

        collection = mongo_client[MONGO_DATABASE][MONGO_COLLECTION]

        # Create an index to help historical time-range queries
        collection.create_index("timestamp")

        # Connect to Kafka
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda message: json.loads(
                message.decode("utf-8")
            )
        )

        print("Connected to Kafka and MongoDB.")
        print(f"Reading topic: {KAFKA_TOPIC}")
        print(f"Saving to: {MONGO_DATABASE}.{MONGO_COLLECTION}")
        print("Press Ctrl+C to stop.")

        for message in consumer:
            record = message.value

            # Ensure each record has the fields the dashboard needs
            required_fields = [
                "timestamp",
                "value",
                "metric_type",
                "sensor_id"
            ]

            if not all(field in record for field in required_fields):
                print("Skipped a message with missing required fields.")
                continue

            # Convert ISO timestamp strings to datetime objects
            timestamp = datetime.fromisoformat(
                record["timestamp"].replace("Z", "+00:00")
            )

            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            record["timestamp"] = timestamp

            # Avoid duplicate records when Kafka replays a message
            record_id = f"{message.topic}-{message.partition}-{message.offset}"

            collection.update_one(
                {"_id": record_id},
                {"$setOnInsert": record},
                upsert=True
            )

            print(
                f"Saved {record['metric_type']} reading: "
                f"{record['value']} at {record['timestamp']}"
            )

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    except Exception as error:
        print(f"Consumer error: {error}")

    finally:
        if consumer is not None:
            consumer.close()

        if mongo_client is not None:
            mongo_client.close()


if __name__ == "__main__":
    main()

import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from sqlalchemy import text

from ingestion.db_config import get_engine


TOPIC = "orders"
BOOTSTRAP_SERVERS = ["localhost:9092"]

engine = get_engine()


def save_to_bronze(event, topic, partition, offset):
    query = text("""
        INSERT INTO bronze.kafka_orders (
            event_id,
            event_type,
            topic,
            partition_id,
            kafka_offset,
            payload,
            ingested_at
        )
        VALUES (
            :event_id,
            :event_type,
            :topic,
            :partition_id,
            :kafka_offset,
            CAST(:payload AS JSONB),
            :ingested_at
        )
        ON CONFLICT (topic, partition_id, kafka_offset)
        DO NOTHING
    """)

    values = {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "topic": topic,
        "partition_id": partition,
        "kafka_offset": offset,
        "payload": json.dumps(event),
        "ingested_at": datetime.now(timezone.utc)
    }

    with engine.begin() as connection:
        connection.execute(query, values)


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="datapulse-bronze-consumer",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        )
    )

    print("DataPulse Bronze consumer started.")

    try:
        for message in consumer:
            event = message.value

            try:
                save_to_bronze(
                    event,
                    message.topic,
                    message.partition,
                    message.offset
                )

                # Commit Kafka offset only after database persistence.
                consumer.commit()

                print(
                    f"Saved event {event.get('event_id')} "
                    f"| partition={message.partition} "
                    f"| offset={message.offset}"
                )

            except Exception as error:
                print(f"Error processing event: {error}")
                raise

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    finally:
        consumer.close()
        engine.dispose()


if __name__ == "__main__":
    main()
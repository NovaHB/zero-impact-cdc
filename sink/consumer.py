import json
import duckdb
from confluent_kafka import Consumer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "cdc.public.users"
GROUP_ID = "identity-sink-business-key-v1"
DB_PATH = "identity.duckdb"


def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def create_database():
    conn = duckdb.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER,
            name VARCHAR,
            nin VARCHAR UNIQUE,
            status VARCHAR
        )
    """)

    return conn


def upsert_user(conn, row):
    nin = row.get("nin")

    if not nin:
        raise ValueError(
            f"Cannot upsert user {row.get('id')}: NIN is required"
        )

    # NIN is the business key.
    # A new source ID with an existing NIN updates the existing person.
    conn.execute("""
        INSERT INTO users (
            id,
            name,
            nin,
            status
        )
        VALUES (?, ?, ?, ?)

        ON CONFLICT (nin)
        DO UPDATE SET
            id = excluded.id,
            name = excluded.name,
            status = excluded.status
    """, [
        row.get("id"),
        row.get("name"),
        row.get("nin"),
        row.get("status"),
    ])


def delete_user(conn, row):
    nin = row.get("nin")

    if nin:
        conn.execute(
            "DELETE FROM users WHERE nin = ?",
            [nin],
        )
    elif row.get("id") is not None:
        conn.execute(
            "DELETE FROM users WHERE id = ?",
            [row["id"]],
        )


def apply_event(conn, event):
    payload = event["payload"]

    operation = payload["op"]
    before = payload.get("before")
    after = payload.get("after")

    if operation in ("r", "c", "u"):
        if after:
            upsert_user(conn, after)
            print(
                f"{operation.upper()} → UPSERT "
                f"id={after.get('id')} nin={after.get('nin')}"
            )

    elif operation == "d":
        if before:
            delete_user(conn, before)
            print(
                f"D → DELETE "
                f"id={before.get('id')} nin={before.get('nin')}"
            )

    else:
        print(f"Unknown operation: {operation}")


def main():
    consumer = create_consumer()
    conn = create_database()

    consumer.subscribe([KAFKA_TOPIC])

    print("CDC sink started...")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"DuckDB: {DB_PATH}")
    print(f"Consumer group: {GROUP_ID}")
    print("Business key: nin")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            # Debezium tombstone after DELETE.
            if message.value() is None:
                print("T → TOMBSTONE ignored")
                consumer.commit(message)
                continue

            try:
                event = json.loads(
                    message.value().decode("utf-8")
                )

                conn.execute("BEGIN TRANSACTION")

                apply_event(conn, event)

                conn.execute("COMMIT")

                # Only acknowledge Kafka after DuckDB succeeds.
                consumer.commit(message)

            except Exception as error:
                print(f"Processing error: {error}")

                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass

    except KeyboardInterrupt:
        print("\nStopping CDC sink...")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
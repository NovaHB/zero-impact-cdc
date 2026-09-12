# Zero-Impact CDC

A practical Change Data Capture pipeline that captures PostgreSQL changes with Debezium, streams them through Kafka, and applies them to DuckDB.

## Architecture

```text
PostgreSQL
    ↓
Debezium
    ↓
Kafka
    ↓
Python CDC Consumer
    ↓
DuckDB
```

## Stack

* PostgreSQL 16
* Debezium 2.7.3.Final
* Apache Kafka 3.7
* Python
* confluent-kafka
* DuckDB
* Docker Compose

## What this project demonstrates

* WAL-based PostgreSQL CDC
* Debezium and Kafka event streaming
* INSERT, UPDATE and DELETE propagation
* Snapshot and tombstone handling
* Transactional DuckDB writes
* Business-key idempotency
* Kafka replay resilience
* Schema evolution and drift detection

## Idempotency

The sink uses `nin` as the business key.

Multiple PostgreSQL records with the same NIN resolve to one logical record in DuckDB.

```text
PostgreSQL

id=100, nin=NIN-001
id=200, nin=NIN-001
id=300, nin=NIN-001

        ↓

DuckDB

id=300, nin=NIN-001
```

The latest event wins.

The pipeline was also tested by replaying the Kafka topic from the beginning without creating duplicate NINs.

## Schema Evolution

The schema-evolution consumer handles changes to the PostgreSQL source schema without silently breaking the sink.

* **New columns:** automatically added
* **Removed columns:** detected while sink columns are retained
* **Renames:** detected as potential renames
* **Compatible type changes:** supported, e.g. `INTEGER → BIGINT`
* **Unsafe type changes:** rejected with transaction rollback

Schema fingerprints and drift events are stored in DuckDB for tracking.

## Reliability

DuckDB changes are committed before the Kafka offset is acknowledged.

If processing fails:

```text
DuckDB transaction → ROLLBACK
Kafka offset       → NOT COMMITTED
```

This prevents failed events from being acknowledged as successfully processed.

## Running

Start the infrastructure:

```powershell
docker compose up -d
```

Run the standard CDC consumer:

```powershell
cd sink
python consumer.py
```

Run the schema-evolution consumer:

```powershell
python consumer_schema_evolution.py
```

## Repository Structure

```text
zero-impact-cdc/
├── docker-compose.yml
├── README.md
├── .gitignore
└── sink/
    ├── check_duckdb.py
    ├── consumer.py
    └── consumer_schema_evolution.py
```

The DuckDB database and Python virtual environment are excluded from Git.


# Zero-Impact CDC

A practical Change Data Capture pipeline that captures PostgreSQL changes with Debezium, streams them through Kafka, and applies them to DuckDB.

## Architecture

PostgreSQL
? Debezium
? Kafka
? Python CDC Consumer
? DuckDB

## Stack

- PostgreSQL 16
- Debezium 2.7.3.Final
- Apache Kafka 3.7
- Python
- confluent-kafka
- DuckDB
- Docker Compose

## What this project demonstrates

- PostgreSQL logical replication and WAL-based CDC
- Debezium change events
- Kafka event streaming
- INSERT, UPDATE and DELETE propagation
- Snapshot events
- Debezium tombstone handling
- Transactional DuckDB writes
- Kafka offset acknowledgement after successful sink writes
- Business-key idempotency
- Replay resilience

## Idempotency

The DuckDB sink uses `nin` as the business key.

This means multiple source events with different PostgreSQL IDs but the same NIN resolve to one logical person.

Example:

    PostgreSQL
    id=100, nin=NIN-001
    id=200, nin=NIN-001
    id=300, nin=NIN-001

    DuckDB
    id=300, nin=NIN-001

The latest event updates the existing logical record instead of creating another row.

The pipeline was also tested by replaying the Kafka topic from the beginning. The DuckDB dataset remained deduplicated by NIN.

## CDC event handling

Debezium operations are handled as follows:

- `r` ? snapshot / upsert
- `c` ? insert / upsert
- `u` ? update / upsert
- `d` ? delete
- tombstone ? ignored

DuckDB changes are committed before the Kafka offset is acknowledged.

## Running the project

Start the infrastructure:

    docker compose up -d

Then start the Python sink:

    cd sink
    python consumer.py

Changes made in PostgreSQL flow through Debezium and Kafka before being applied to DuckDB.

## Repository structure

    zero-impact-cdc/
    +-- docker-compose.yml
    +-- README.md
    +-- .gitignore
    +-- sink/
        +-- consumer.py

The DuckDB database is generated locally and is intentionally excluded from Git.

The Python virtual environment is also excluded from Git.

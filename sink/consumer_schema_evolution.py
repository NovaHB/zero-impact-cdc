
import json
import hashlib
from datetime import date, timedelta

import duckdb
from confluent_kafka import Consumer, KafkaException


# ============================================================
# CONFIG
# ============================================================

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "cdc.public.users"
GROUP_ID = "identity-sink-schema-evolution-v1"

DUCKDB_PATH = "identity.duckdb"


# ============================================================
# KAFKA
# ============================================================

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([TOPIC])


# ============================================================
# DUCKDB
# ============================================================

conn = duckdb.connect(DUCKDB_PATH)

conn.execute("""
CREATE TABLE IF NOT EXISTS schema_registry (
    fingerprint VARCHAR PRIMARY KEY,
    columns_json VARCHAR,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS schema_drift_log (
    id BIGINT PRIMARY KEY,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fingerprint VARCHAR,
    drift_type VARCHAR,
    column_name VARCHAR,
    old_type VARCHAR,
    new_type VARCHAR,
    details VARCHAR
)
""")

conn.execute("""
CREATE SEQUENCE IF NOT EXISTS schema_drift_seq
START 1
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER,
    name VARCHAR,
    nin VARCHAR UNIQUE,
    status VARCHAR
)
""")

conn.commit()


# ============================================================
# IDENTIFIER / NAME HELPERS
# ============================================================

def quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


def normalize_name(name):
    return (
        name.lower()
        .replace("_", "")
        .replace("-", "")
        .replace(" ", "")
    )


# ============================================================
# DEBEZIUM TYPE MAPPING
# ============================================================

def map_debezium_type(field):
    """
    Convert a Debezium schema field into a DuckDB type.
    """

    field_type = field.get("type")
    logical_name = field.get("name")

    # PostgreSQL DATE
    if logical_name == "io.debezium.time.Date":
        return "DATE"

    # Debezium timestamps
    if logical_name in {
        "io.debezium.time.Timestamp",
        "io.debezium.time.MicroTimestamp",
        "io.debezium.time.NanoTimestamp",
        "io.debezium.time.ZonedTimestamp",
    }:
        return "TIMESTAMP"

    # Debezium time
    if logical_name in {
        "io.debezium.time.Time",
        "io.debezium.time.MicroTime",
        "io.debezium.time.NanoTime",
    }:
        return "TIME"

    # Decimal
    if logical_name == "org.apache.kafka.connect.data.Decimal":
        return "DECIMAL(38,18)"

    if field_type == "int8":
        return "TINYINT"

    if field_type == "int16":
        return "SMALLINT"

    if field_type == "int32":
        return "INTEGER"

    if field_type == "int64":
        return "BIGINT"

    if field_type == "float32":
        return "REAL"

    if field_type == "float64":
        return "DOUBLE"

    if field_type == "boolean":
        return "BOOLEAN"

    if field_type == "string":
        return "VARCHAR"

    if field_type == "bytes":
        return "BLOB"

    # Keep arbitrary nested structures as JSON.
    if field_type in {"struct", "map", "array"}:
        return "VARCHAR"

    return "VARCHAR"


# ============================================================
# DEBEZIUM ROW SCHEMA
# ============================================================

def get_row_schema(event):
    """
    Find the actual PostgreSQL row schema inside Debezium.

    Debezium's outer schema looks conceptually like:

        schema
        └── fields
            ├── before
            │   └── fields
            │       ├── id
            │       ├── name
            │       ├── nin
            │       └── status
            │
            ├── after
            │   └── fields
            │       ├── id
            │       ├── name
            │       ├── nin
            │       └── status
            │
            ├── source
            ├── op
            └── ts_ms

    We ONLY want the fields inside before/after.
    """

    outer_schema = event.get("schema", {})

    for field in outer_schema.get("fields", []):

        field_name = field.get("field")

        if field_name not in {"after", "before"}:
            continue

        # IMPORTANT:
        #
        # The nested row definition is represented by the
        # 'fields' property of the after/before field itself.
        #
        # Do NOT use field.get("schema").
        nested_fields = field.get("fields")

        if isinstance(nested_fields, list):
            return field

    return None


def extract_row_fields(row_schema):
    """
    Extract only actual source-table columns.

    Returns:

        {
            "id": {
                "duckdb_type": "INTEGER",
                "debezium_type": "int32",
                "logical_type": None
            }
        }
    """

    result = {}

    for field in row_schema.get("fields", []):

        column_name = field.get("field")

        if not column_name:
            continue

        result[column_name] = {
            "duckdb_type": map_debezium_type(field),
            "debezium_type": field.get("type"),
            "logical_type": field.get("name"),
        }

    return result


# ============================================================
# SCHEMA FINGERPRINT
# ============================================================

def schema_fingerprint(columns):
    """
    Give every observed source schema a deterministic fingerprint.

    This lets us distinguish:

        V1
        V2
        V3

    during Kafka replay.

    An old V1 event arriving after V2 is not treated as a
    brand-new schema change.
    """

    normalized = []

    for name in sorted(columns):

        info = columns[name]

        normalized.append({
            "name": name,
            "duckdb_type": info["duckdb_type"],
            "debezium_type": info["debezium_type"],
            "logical_type": info["logical_type"],
        })

    payload = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":")
    )

    return hashlib.sha256(
        payload.encode()
    ).hexdigest()[:16]


# ============================================================
# DUCKDB SCHEMA
# ============================================================

def get_sink_columns():

    rows = conn.execute(
        "DESCRIBE users"
    ).fetchall()

    return {
        row[0]: row[1]
        for row in rows
    }


# ============================================================
# TYPE COMPATIBILITY
# ============================================================

def type_rank(dtype):

    dtype = dtype.upper()

    return {
        "TINYINT": 1,
        "SMALLINT": 2,
        "INTEGER": 3,
        "BIGINT": 4,
        "REAL": 5,
        "DOUBLE": 6,
    }.get(dtype)


def compatible_type_change(old_type, new_type):

    old_type = old_type.upper()
    new_type = new_type.upper()

    if old_type == new_type:
        return True

    old_rank = type_rank(old_type)
    new_rank = type_rank(new_type)

    # Numeric widening.
    if old_rank is not None and new_rank is not None:
        return new_rank >= old_rank

    return False


# ============================================================
# DRIFT LOGGING
# ============================================================

def log_drift(
    fingerprint,
    drift_type,
    column_name=None,
    old_type=None,
    new_type=None,
    details=None,
):

    drift_id = conn.execute(
        "SELECT nextval('schema_drift_seq')"
    ).fetchone()[0]

    conn.execute("""
        INSERT INTO schema_drift_log (
            id,
            fingerprint,
            drift_type,
            column_name,
            old_type,
            new_type,
            details
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        drift_id,
        fingerprint,
        drift_type,
        column_name,
        old_type,
        new_type,
        details,
    ])


# ============================================================
# SCHEMA REGISTRY
# ============================================================

def register_schema(fingerprint, columns):

    existing = conn.execute("""
        SELECT 1
        FROM schema_registry
        WHERE fingerprint = ?
    """, [fingerprint]).fetchone()

    columns_json = json.dumps(
        columns,
        sort_keys=True,
        default=str
    )

    if existing:

        conn.execute("""
            UPDATE schema_registry
            SET last_seen = CURRENT_TIMESTAMP
            WHERE fingerprint = ?
        """, [fingerprint])

        return False

    conn.execute("""
        INSERT INTO schema_registry (
            fingerprint,
            columns_json
        )
        VALUES (?, ?)
    """, [
        fingerprint,
        columns_json,
    ])

    return True


# ============================================================
# SCHEMA EVOLUTION
# ============================================================

def adapt_schema(source_columns, fingerprint):

    sink_columns = get_sink_columns()

    source_names = set(source_columns.keys())
    sink_names = set(sink_columns.keys())

    added = source_names - sink_names
    missing_from_source = sink_names - source_names

    # --------------------------------------------------------
    # 1. NEW COLUMNS
    # --------------------------------------------------------

    for column_name in sorted(added):

        duckdb_type = source_columns[
            column_name
        ]["duckdb_type"]

        conn.execute(
            f"""
            ALTER TABLE users
            ADD COLUMN {quote_identifier(column_name)}
            {duckdb_type}
            """
        )

        log_drift(
            fingerprint=fingerprint,
            drift_type="COLUMN_ADDED",
            column_name=column_name,
            new_type=duckdb_type,
            details=(
                f"New source column detected: "
                f"{column_name}"
            ),
        )

        print(
            f"SCHEMA ADAPTED → added column "
            f"{column_name} ({duckdb_type})"
        )

    # Refresh sink schema after additions.
    sink_columns = get_sink_columns()

    # --------------------------------------------------------
    # 2. TYPE CHANGES
    # --------------------------------------------------------

    for column_name in sorted(
        source_names.intersection(
            sink_columns.keys()
        )
    ):

        source_type = source_columns[
            column_name
        ]["duckdb_type"]

        sink_type = sink_columns[
            column_name
        ]

        if source_type.upper() == sink_type.upper():
            continue

        if compatible_type_change(
            sink_type,
            source_type
        ):

            try:

                conn.execute(
                    f"""
                    ALTER TABLE users
                    ALTER COLUMN {quote_identifier(column_name)}
                    SET DATA TYPE {source_type}
                    """
                )

                log_drift(
                    fingerprint=fingerprint,
                    drift_type="TYPE_CHANGE_COMPATIBLE",
                    column_name=column_name,
                    old_type=sink_type,
                    new_type=source_type,
                    details=(
                        f"Safe widening: "
                        f"{sink_type} -> {source_type}"
                    ),
                )

                print(
                    f"SCHEMA ADAPTED → type widened "
                    f"{column_name}: "
                    f"{sink_type} → {source_type}"
                )

            except Exception as exc:

                raise ValueError(
                    f"Failed to apply compatible type "
                    f"change for {column_name}: "
                    f"{sink_type} -> {source_type}. "
                    f"Reason: {exc}"
                )

        else:

            log_drift(
                fingerprint=fingerprint,
                drift_type="TYPE_CHANGE_INCOMPATIBLE",
                column_name=column_name,
                old_type=sink_type,
                new_type=source_type,
                details=(
                    f"Unsafe schema type change detected: "
                    f"{sink_type} -> {source_type}"
                ),
            )

            raise ValueError(
                f"Unsafe schema type change detected "
                f"for {column_name}: "
                f"{sink_type} -> {source_type}"
            )

    # --------------------------------------------------------
    # 3. REMOVED COLUMNS
    # --------------------------------------------------------

    for column_name in sorted(
        missing_from_source
    ):

        log_drift(
            fingerprint=fingerprint,
            drift_type="COLUMN_MISSING_FROM_SOURCE_SCHEMA",
            column_name=column_name,
            old_type=sink_columns[
                column_name
            ],
            details=(
                f"Column exists in sink but is absent "
                f"from this observed source schema: "
                f"{column_name}"
            ),
        )

        print(
            f"SCHEMA DRIFT → source schema missing "
            f"column {column_name} "
            f"(sink retained)"
        )

    # --------------------------------------------------------
    # 4. POTENTIAL RENAMES
    # --------------------------------------------------------

    added_normalized = {
        normalize_name(name): name
        for name in added
    }

    removed_normalized = {
        normalize_name(name): name
        for name in missing_from_source
    }

    for removed_key, removed_name in (
        removed_normalized.items()
    ):

        for added_key, added_name in (
            added_normalized.items()
        ):

            if (
                removed_key in added_key
                or added_key in removed_key
            ):

                log_drift(
                    fingerprint=fingerprint,
                    drift_type="POTENTIAL_RENAME",
                    column_name=added_name,
                    old_type=sink_columns.get(
                        removed_name
                    ),
                    new_type=source_columns[
                        added_name
                    ]["duckdb_type"],
                    details=(
                        f"Possible rename detected: "
                        f"{removed_name} -> "
                        f"{added_name}. "
                        f"Semantic confirmation required."
                    ),
                )

                print(
                    f"SCHEMA WARNING → potential rename "
                    f"{removed_name} → "
                    f"{added_name}"
                )


# ============================================================
# VALUE CONVERSION
# ============================================================

def convert_value(field, value):

    if value is None:
        return None

    logical_type = field.get(
        "logical_type"
    )

    # Debezium DATE:
    #
    # integer = number of days since
    # 1970-01-01
    if (
        logical_type
        == "io.debezium.time.Date"
        and isinstance(value, int)
    ):

        return (
            date(1970, 1, 1)
            + timedelta(days=value)
        )

    return value


def serialize_complex_value(value):

    if isinstance(value, (dict, list)):
        return json.dumps(
            value,
            default=str
        )

    return value


# ============================================================
# BUILD ROW
# ============================================================

def build_row(event, source_columns):

    payload = event.get(
        "payload",
        {}
    )

    row = payload.get("after")

    if row is None:
        row = payload.get("before")

    if row is None:
        return None

    result = {}

    for column_name, field in (
        source_columns.items()
    ):

        value = row.get(
            column_name
        )

        value = convert_value(
            field,
            value
        )

        value = serialize_complex_value(
            value
        )

        result[column_name] = value

    return result


# ============================================================
# UPSERT
# ============================================================

def upsert_row(row):

    if row is None:
        return

    if "nin" not in row:
        raise ValueError(
            "Source row does not contain "
            "required business key: nin"
        )

    columns = list(row.keys())

    column_sql = ", ".join(
        quote_identifier(column)
        for column in columns
    )

    placeholders = ", ".join(
        "?"
        for _ in columns
    )

    update_columns = [
        column
        for column in columns
        if column != "nin"
    ]

    update_sql = ", ".join(
        f"{quote_identifier(column)} = "
        f"excluded.{quote_identifier(column)}"
        for column in update_columns
    )

    values = [
        row[column]
        for column in columns
    ]

    sql = f"""
        INSERT INTO users (
            {column_sql}
        )
        VALUES (
            {placeholders}
        )
        ON CONFLICT (nin)
        DO UPDATE SET
            {update_sql}
    """

    conn.execute(
        sql,
        values
    )


# ============================================================
# DELETE
# ============================================================

def delete_row(row):

    if row is None:
        return

    if row.get("nin") is not None:

        conn.execute(
            f"""
            DELETE FROM users
            WHERE {quote_identifier("nin")} = ?
            """,
            [row["nin"]]
        )

        return

    if row.get("id") is not None:

        conn.execute(
            f"""
            DELETE FROM users
            WHERE {quote_identifier("id")} = ?
            """,
            [row["id"]]
        )


# ============================================================
# MAIN LOOP
# ============================================================

print()
print("==============================================")
print(" ZERO-IMPACT CDC")
print(" Schema Evolution Consumer")
print("==============================================")
print(f"Topic : {TOPIC}")
print(f"Group : {GROUP_ID}")
print(f"Sink  : {DUCKDB_PATH}")
print()
print("Waiting for CDC events...")
print()


try:

    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            raise KafkaException(
                message.error()
            )

        # ----------------------------------------------------
        # TOMBSTONE
        # ----------------------------------------------------

        if message.value() is None:

            print(
                f"T → TOMBSTONE ignored "
                f"(partition="
                f"{message.partition()}, "
                f"offset="
                f"{message.offset()})"
            )

            consumer.commit(
                message
            )

            continue

        # ----------------------------------------------------
        # PARSE EVENT
        # ----------------------------------------------------

        try:

            event = json.loads(
                message.value().decode(
                    "utf-8"
                )
            )

        except Exception as exc:

            print()
            print("MESSAGE PARSE ERROR")
            print(exc)
            print(
                "Kafka offset NOT committed."
            )
            print()

            continue

        payload = event.get(
            "payload",
            {}
        )

        operation = payload.get(
            "op"
        )

        # ----------------------------------------------------
        # FIND ACTUAL SOURCE ROW SCHEMA
        # ----------------------------------------------------

        row_schema = get_row_schema(
            event
        )

        if row_schema is None:

            print()
            print("SCHEMA ERROR")
            print(
                "Could not locate nested "
                "Debezium after/before row schema."
            )
            print(
                "Kafka offset NOT committed."
            )
            print()

            continue

        source_columns = extract_row_fields(
            row_schema
        )

        if not source_columns:

            print()
            print("SCHEMA ERROR")
            print(
                "No source row fields detected."
            )
            print(
                "Kafka offset NOT committed."
            )
            print()

            continue

        # ----------------------------------------------------
        # FINGERPRINT
        # ----------------------------------------------------

        fingerprint = schema_fingerprint(
            source_columns
        )

        # ----------------------------------------------------
        # PROCESS TRANSACTIONALLY
        # ----------------------------------------------------

        try:

            conn.execute(
                "BEGIN"
            )

            # Register this schema.
            is_new_schema = register_schema(
                fingerprint,
                source_columns
            )

            # Only perform structural drift analysis
            # the first time this schema fingerprint
            # is encountered.
            #
            # This prevents replaying:
            #
            # V1 -> V2 -> V1
            #
            # from falsely creating endless drift logs.
            if is_new_schema:

                adapt_schema(
                    source_columns,
                    fingerprint
                )

                print(
                    f"SCHEMA OBSERVED → "
                    f"{fingerprint} | "
                    f"{', '.join(sorted(source_columns.keys()))}"
                )

            # ------------------------------------------------
            # APPLY CDC EVENT
            # ------------------------------------------------

            if operation in {
                "c",
                "r",
                "u"
            }:

                row = build_row(
                    event,
                    source_columns
                )

                upsert_row(
                    row
                )

                print(
                    f"{operation.upper()} → "
                    f"UPSERT "
                    f"nin={row.get('nin')} "
                    f"schema={fingerprint}"
                )

            elif operation == "d":

                row = build_row(
                    event,
                    source_columns
                )

                delete_row(
                    row
                )

                print(
                    f"D → DELETE "
                    f"nin="
                    f"{row.get('nin') if row else None} "
                    f"schema={fingerprint}"
                )

            else:

                print(
                    f"UNKNOWN OPERATION → "
                    f"{operation}"
                )

            # ------------------------------------------------
            # DUCKDB FIRST
            # ------------------------------------------------

            conn.commit()

            # ------------------------------------------------
            # KAFKA SECOND
            # ------------------------------------------------

            consumer.commit(
                message
            )

        except Exception as exc:

            try:
                conn.rollback()
            except Exception:
                pass

            print()
            print("PROCESSING ERROR")
            print(
                type(exc).__name__
                + ": "
                + str(exc)
            )
            print(
                "DuckDB transaction rolled back."
            )
            print(
                "Kafka offset NOT committed."
            )
            print()

            continue


except KeyboardInterrupt:

    print()
    print(
        "Stopping schema evolution consumer..."
    )


finally:

    consumer.close()
    conn.close()

    print(
        "Consumer stopped."
    )

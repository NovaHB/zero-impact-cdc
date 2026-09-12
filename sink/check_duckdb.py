
import duckdb


conn = duckdb.connect("identity.duckdb")


print("DuckDB schema:")
for row in conn.execute("DESCRIBE users").fetchall():
    print(row)


print("\nRows found:")

rows = conn.execute(
    """
    SELECT *
    FROM users
    ORDER BY id
    """
).fetchall()


for row in rows:
    print(row)


print(f"\nRow count: {len(rows)}")


print("\nSchema registry:")
for row in conn.execute(
    """
    SELECT
        fingerprint,
        columns_json,
        first_seen,
        last_seen
    FROM schema_registry
    ORDER BY first_seen
    """
).fetchall():
    print(row)


print("\nSchema drift log:")
for row in conn.execute(
    """
    SELECT
        id,
        detected_at,
        fingerprint,
        drift_type,
        column_name,
        old_type,
        new_type,
        details
    FROM schema_drift_log
    ORDER BY id
    """
).fetchall():
    print(row)


conn.close()


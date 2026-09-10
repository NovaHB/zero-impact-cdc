import duckdb


conn = duckdb.connect("identity.duckdb")

rows = conn.execute(
    """
    SELECT id, name, nin, status
    FROM users
    WHERE name = 'IDEMPOTENCY TEST'
    """
).fetchall()

print("Rows found:")
for row in rows:
    print(row)

print(f"\nRow count: {len(rows)}")

conn.close()
import duckdb

con = duckdb.connect("data/duckdb/crypto.duckdb")

print("\nLatest candles:\n")
print(
    con.execute("""
        SELECT *
        FROM main_marts.fct_candles_1m
        ORDER BY minute_bucket DESC
        LIMIT 5
    """).fetchdf()
)

print("\nLatest orderflow:\n")
print(
    con.execute("""
        SELECT *
        FROM main_marts.fct_orderflow_1m
        ORDER BY minute_bucket DESC
        LIMIT 5
    """).fetchdf()
)

print("\nLatest trades_1m:\n")
print(
    con.execute("""
        SELECT *
        FROM main_marts.fct_trades_1m
        ORDER BY minute_bucket DESC
        LIMIT 5
    """).fetchdf()
)

con.close()
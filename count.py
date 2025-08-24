#!/usr/bin/env python3

import duckdb
import multiprocessing

def main():
    # 1) In‐memory DuckDB, use all cores
    con = duckdb.connect(database=':memory:')
    con.execute(f"PRAGMA threads={multiprocessing.cpu_count()}")

    # 2) Read Parquet and classify into detailed buckets
    parquet_file = 'Bitcoin_addresses_LATEST.parquet'
    con.execute(f"""
    CREATE OR REPLACE VIEW classified AS
    SELECT
      address,
      CASE
        -- Legacy P2PKH
        WHEN address LIKE '1%'
             AND REGEXP_MATCHES(
               address,
               '^[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]{{26,35}}$'
             ) THEN 'P2PKH'

        -- Compatibility P2SH
        WHEN address LIKE '3%'
             AND REGEXP_MATCHES(
               address,
               '^[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]{{26,35}}$'
             ) THEN 'P2SH'

        -- Native SegWit v0: P2WPKH (20-byte → 42 chars)
        WHEN LOWER(address) LIKE 'bc1q%'
             AND LENGTH(address) = 42
             AND REGEXP_MATCHES(
               address,
               '^bc1q[qpzry9x8gf2tvdw0s3jn54khce6mua7l]+$'
             ) THEN 'P2WPKH (Bech32 v0)'

        -- Native SegWit v0: P2WSH (32-byte → 62 chars)
        WHEN LOWER(address) LIKE 'bc1q%'
             AND LENGTH(address) = 62
             AND REGEXP_MATCHES(
               address,
               '^bc1q[qpzry9x8gf2tvdw0s3jn54khce6mua7l]+$'
             ) THEN 'P2WSH (Bech32 v0)'

        -- Taproot (Bech32m v1)
        WHEN LOWER(address) LIKE 'bc1p%'
             AND REGEXP_MATCHES(
               address,
               '^bc1p[qpzry9x8gf2tvdw0s3jn54khce6mua7l]+$'
             ) THEN 'P2TR (Bech32m v1)'

        -- Future SegWit (Bech32m v2+)
        WHEN LOWER(address) LIKE 'bc1[2-9m-z]%'
             AND REGEXP_MATCHES(
               address,
               '^bc1[2-9m-z][qpzry9x8gf2tvdw0s3jn54khce6mua7l]+$'
             ) THEN 'Future SegWit (Bech32m v2+)'

        -- Anything else
        ELSE 'Other'
      END AS type
    FROM read_parquet('{parquet_file}')
    """)

    # 3) Summarize counts by subtype
    df = con.execute("""
      SELECT type, COUNT(*) AS count
      FROM classified
      GROUP BY type
      ORDER BY count DESC
    """).df()
    print(df)

    # 4) Export all “Other” for inspection
    con.execute("""
      COPY (
        SELECT address
        FROM classified
        WHERE type = 'Other'
      )
      TO 'other_addresses.csv'
      WITH (HEADER TRUE, DELIMITER ',')
    """)
    print("Exported all ‘Other’ addresses → other_addresses.csv")

if __name__ == '__main__':
    main()


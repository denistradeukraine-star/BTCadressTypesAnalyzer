Bitcoin Address Classifier

This Python script is a tool for analyzing and classifying a large dataset of Bitcoin addresses. It leverages the power of DuckDB to perform fast, in-memory queries on a Parquet file, categorizing addresses into different types based on their format and structure.
🚀 Overview

The script reads a .parquet file containing Bitcoin addresses and runs a single, multi-core SQL query to classify each address. It then generates a summary of the counts for each address type and exports any addresses that don't match a known format into a separate CSV file for manual review.



# BTCadressTypesAnalyzer

🛠️ Prerequisites

    Python 3.6+

    The duckdb Python library. You can install it using pip install duckdb.

    A Parquet file named Bitcoin_addresses_LATEST.parquet in the same directory as the script. This file is assumed to have a column named address.

💡 How It Works

The script operates in four main steps:

    DuckDB In-Memory Connection: It initializes a connection to an in-memory DuckDB database. By using PRAGMA threads=..., it configures DuckDB to use all available CPU cores, enabling highly parallelized and efficient processing of the data.

    Address Classification: The core of the script is a single SQL query that creates a temporary view called classified. This query uses a CASE statement with REGEXP_MATCHES to check each address against a series of patterns, categorizing them into these buckets:

        P2PKH: Legacy addresses starting with 1.

        P2SH: Compatibility addresses starting with 3.

        P2WPKH (Bech32 v0): Native SegWit addresses starting with bc1q and having a specific length.

        P2WSH (Bech32 v0): Native SegWit addresses starting with bc1q with a longer, specific length.

        P2TR (Bech32m v1): Taproot addresses starting with bc1p.

        Future SegWit (Bech32m v2+): Addresses that follow the Bech32m format but with a new version number (e.g., bc1...).

        Other: Anything that does not match the above patterns.

    Summarize Results: After the classification, a second query groups the data by the new type column and counts the number of addresses in each category. The final result, which is a summary of all address types and their counts, is printed directly to the console.

    Export Unclassified Addresses: To ensure no addresses are lost and to provide a basis for further analysis, the script exports all addresses classified as 'Other' into a CSV file named other_addresses.csv. This file can be used to manually inspect any non-standard or new address types.

➡️ Usage

To run the script, simply ensure you have the prerequisites and the Parquet file in the same directory, then execute the file from your terminal:

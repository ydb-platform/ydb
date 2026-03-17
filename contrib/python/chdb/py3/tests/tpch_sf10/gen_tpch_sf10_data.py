#!/usr/bin/env python3

import os
import sys
import pyarrow.parquet as pq
import gdown

current_dir = os.path.dirname(os.path.abspath(__file__))

# Set the path to the TPCH data directory
sf10_data_dir = os.path.join(current_dir, "../data/tpch-sf10/")
raw_data_dir = os.path.join(current_dir, "../data/tpch-sf10-raw/")
data_dir = sf10_data_dir


def convert_column_names_to_lowercase(input_file, output_file):
    # Read the input Parquet file
    table = pq.read_table(input_file)

    # Get the schema of the table
    original_columns = table.column_names

    # Convert the column names to lowercase
    lowercase_columns = [column.lower() for column in original_columns]

    # Create a new table with the updated schema
    new_table = table.rename_columns(lowercase_columns)
    print("New schema for the table:")
    print(new_table.schema)

    # Write the new table to the output Parquet file
    pq.write_table(new_table, output_file)


# Check if data_dir exists
if not os.path.exists(raw_data_dir):
    os.makedirs(raw_data_dir)

# Download the data files
# gdown --folder https://drive.google.com/drive/folders/1mZC3NuPBZC4mjP3_kH18c9fLrv8ME7RU
gdown.download_folder(
    url="https://drive.google.com/drive/folders/1mZC3NuPBZC4mjP3_kH18c9fLrv8ME7RU",
    output=os.path.dirname(raw_data_dir),
    verify=True,
)

if not os.path.exists(data_dir):
    os.makedirs(data_dir)

for tbl in [
    "lineitem",
    "customer",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]:
    if not os.path.exists(data_dir + tbl + ".parquet"):
        convert_column_names_to_lowercase(
            raw_data_dir + tbl + ".parquet", data_dir + tbl + ".parquet"
        )

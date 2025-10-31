#!/usr/bin/env python3

#--query_path .github/scripts/analytics/data_mart_queries/perfomance_olap_mart.sql --table_path perfomance/olap/fast_results --store_type column --partition_keys Run_start_timestamp --primary_keys Db Suite Test Branch Run_start_timestamp --ttl_min 43200 --ttl_key Run_start_timestamp
import argparse
import ydb
import os
import time
from ydb_wrapper import YDBWrapper

# Get repository path
dir = os.path.dirname(__file__)
repo_path = os.path.abspath(f"{dir}/../../../")

def get_data_from_query_with_metadata(ydb_wrapper, query):
    """Get data from query using ydb_wrapper and extract column metadata"""
    # Use the new method that returns both data and column metadata in one call
    results, column_types = ydb_wrapper.execute_scan_query_with_metadata(query)
    return results, column_types

def ydb_type_to_str(ydb_type, store_type = 'ROW'):
    # Converts YDB type to string representation for table creation
    is_optional = False
    if ydb_type.HasField('optional_type'):
        is_optional = True
        base_type = ydb_type.optional_type.item
    else:
        base_type = ydb_type

    for type in ydb.PrimitiveType:
        if type.proto.type_id == base_type.type_id:
            break
    if is_optional:
        result_type = ydb.OptionalType(type)
        name = result_type._repr
    else:
        result_type = type
        name = result_type.name

    if name.upper() == 'BOOL' and store_type.upper() == 'COLUMN':
        if is_optional:
            result_type = ydb.OptionalType(ydb.PrimitiveType.Uint8)
        else:
            result_type = ydb.PrimitiveType.Uint8
        name = 'Uint8'
    return result_type, name

def create_table(ydb_wrapper, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key):
    """Create table using ydb_wrapper based on the structure of the provided column types."""
    if not column_types:
        raise ValueError("No column types to create table from.")

    columns_sql = []
    for column_name, column_ydb_type in column_types:
        column_type_obj, column_type_str = ydb_type_to_str(column_ydb_type, store_type.upper())
        if column_name in primary_keys:
            columns_sql.append(f"`{column_name}` {column_type_str.replace('?','')} NOT NULL")
        else:
            columns_sql.append(f"`{column_name}` {column_type_str.replace('?','')}")

    partition_keys_sql = ", ".join([f"`{key}`" for key in partition_keys])
    primary_keys_sql = ", ".join([f"`{key}`" for key in primary_keys])
    
    # Добавляем TTL только если оба аргумента заданы
    ttl_clause = ""
    if ttl_min and ttl_key:
        ttl_clause = f' TTL = Interval("PT{ttl_min}M") ON {ttl_key}'

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        {', '.join(columns_sql)},
        PRIMARY KEY ({primary_keys_sql})
    )
    PARTITION BY HASH({partition_keys_sql})
    WITH (
       {"STORE = COLUMN" if store_type.upper() == 'COLUMN' else ''}
        {',' if store_type and  ttl_clause else ''}
        {ttl_clause}
    )
    """

    ydb_wrapper.create_table(table_path, create_table_sql)
def create_table_if_not_exists(ydb_wrapper, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key):
    """Create table if it does not already exist, using ydb_wrapper."""
    # For now, we'll always try to create the table
    # In a more sophisticated implementation, we could check if table exists first
    create_table(ydb_wrapper, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key)

def parse_args():
    parser = argparse.ArgumentParser(description="YDB Table Manager")
    parser.add_argument("--table_path", required=True, help="Table path and name")
    parser.add_argument("--query_path", required=True, help="Path to the SQL query file")
    parser.add_argument("--store_type", choices=["column", "row"], required=True, help="Table store type (column or row)")
    parser.add_argument("--partition_keys", nargs="+", required=True, help="List of partition keys")
    parser.add_argument("--primary_keys", nargs="+", required=True, help="List of primary keys")
    parser.add_argument("--ttl_min", type=int, help="TTL in minutes")
    parser.add_argument("--ttl_key", help="TTL key column name")
    
    return parser.parse_args()

def main():
    args = parse_args()

    table_path = args.table_path
    batch_size = 1000

    # Initialize YDB wrapper with context manager for automatic cleanup
    with YDBWrapper() as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return 1

        # Read SQL query from file
        sql_query_path = os.path.join(repo_path, args.query_path)
        print(f'Query found: {sql_query_path}')
        with open(sql_query_path, 'r') as file:
            sql_query = file.read()

        # Run query to get sample data and column types
        results, column_types = get_data_from_query_with_metadata(ydb_wrapper, sql_query)
        if not results:
            print("No data to create table from.")
            return

        create_table_if_not_exists(
            ydb_wrapper, table_path, column_types, args.store_type,
            args.partition_keys, args.primary_keys, args.ttl_min, args.ttl_key
        )

        print(f'Preparing to upsert: {len(results)} rows')
        
        # Подготавливаем column_types_map один раз
        column_types_map = ydb.BulkUpsertColumns()
        for column_name, column_ydb_type in column_types:
            column_type_obj, column_type_str = ydb_type_to_str(column_ydb_type, args.store_type.upper())
            column_types_map.add_column(column_name, column_type_obj)
        
        ydb_wrapper.bulk_upsert_batches(table_path, results, column_types_map, batch_size)
        
        print('Data uploaded')


if __name__ == "__main__":
    main()


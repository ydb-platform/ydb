#!/usr/bin/env python3

#--query_path .github/scripts/analytics/data_mart_queries/perfomance_olap_mart.sql --table_path perfomance/olap/fast_results --store_type column --partition_keys Run_start_timestamp --primary_keys Db Suite Test Branch Run_start_timestamp --ttl_min 43200 --ttl_key Run_start_timestamp
import argparse
import ydb
import configparser
import os
import time

# Load configuration
dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)
repo_path = os.path.abspath(f"{dir}/../../../")

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

def get_data_from_query_with_metadata(driver, query):
    results = []
    scan_query = ydb.ScanQuery(query, {})
    it = driver.table_client.scan_query(scan_query)
    print(f"Executing query")
    start_time = time.time()
    column_types = None
    while True:
        try:
            result = next(it)
            if column_types is None:
                column_types = [(col.name, col.type) for col in result.result_set.columns]
            
            results.extend(result.result_set.rows)
        
        except StopIteration:
            break

    end_time = time.time()
    print(f'Captured {len(results)} rows, duration: {end_time - start_time}s')
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

def create_table(session, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key):
    """Create table based on the structure of the provided column types."""
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

    print(f"Creating table with query: {create_table_sql}")
    session.execute_scheme(create_table_sql)
def create_table_if_not_exists(session, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key):
    """Create table if it does not already exist, based on column types."""
    try:
        session.describe_table(table_path)
        print(f"Table '{table_path}' already exists.")
    except ydb.Error:
        print(f"Table '{table_path}' does not exist. Creating table...")
        create_table(session, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key)

def bulk_upsert(table_client, table_path, rows, column_types,store_type='ROW'):
    print(f"> Bulk upsert into: {table_path}")

    column_types_map = ydb.BulkUpsertColumns()
    for column_name, column_ydb_type in column_types:
        column_type_obj, column_type_str = ydb_type_to_str(column_ydb_type, store_type.upper())
        column_types_map.add_column(column_name, column_type_obj)

    table_client.bulk_upsert(table_path, rows, column_types_map)

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

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1
    else:
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    table_path = args.table_path
    batch_size = 50000

    # Read SQL query from file
    sql_query_path = os.path.join(repo_path, args.query_path)
    print(f'Query found: {sql_query_path}')
    with open(sql_query_path, 'r') as file:
        sql_query = file.read()

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        with ydb.SessionPool(driver) as pool:
            # Run query to get sample data and column types
            results, column_types = get_data_from_query_with_metadata(driver, sql_query)
            if not results:
                print("No data to create table from.")
                return

            # Create table if not exists based on sample column types
            pool.retry_operation_sync(
                lambda session: create_table_if_not_exists(
                    session, f'{DATABASE_PATH}/{table_path}', column_types, args.store_type,
                    args.partition_keys, args.primary_keys, args.ttl_min, args.ttl_key
                )
            )

            print(f'Preparing to upsert: {len(results)} rows')
            for start in range(0, len(results), batch_size):
                batch_rows = results[start:start + batch_size]
                print(f'Upserting: {start}-{start + len(batch_rows)}/{len(results)} rows')
                bulk_upsert(driver.table_client, f'{DATABASE_PATH}/{table_path}', batch_rows, column_types, args.store_type)
            print('Data uploaded')


if __name__ == "__main__":
    main()


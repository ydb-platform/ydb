#!/usr/bin/env python3

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

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

def parse_args():
    parser = argparse.ArgumentParser(description="Delete old records from YDB table")
    parser.add_argument("--table-path", required=True, help="Table path and name")
    parser.add_argument("--timestamp-field", required=True, help="Name of the timestamp field")
    parser.add_argument("--delete-interval", required=True, help="Interval to delete records older than, in ISO 8601 format (https://en.wikipedia.org/wiki/ISO_8601#Durations) without 'P'")
    
    return parser.parse_args()

def delete_old_records(session, full_table_path, timestamp_field, delete_interval):
    """Delete records older than the specified interval."""
    # First, count the number of records that will be deleted
    count_query = f"""
    SELECT COUNT(*) as count
    FROM `{full_table_path}`
    WHERE `{timestamp_field}` < CurrentUtcDate() - Interval("P{delete_interval}")
    """
    
    print(f"Counting records to delete...")
    result_sets = session.transaction().execute(count_query)
    row_count = result_sets[0].rows[0].count
    
    if row_count == 0:
        print("No records to delete.")
        return 0
    
    print(f"Found {row_count} records older than {delete_interval}.")
    
    # Now perform the delete operation
    delete_query = f"""
    DELETE FROM `{full_table_path}`
    WHERE `{timestamp_field}` < CurrentUtcDate() - Interval("P{delete_interval}")
    """
    
    print(f"Executing DELETE query: {delete_query}")
    start_time = time.time()
    session.transaction().execute(delete_query, commit_tx=True)
    end_time = time.time()
    
    print(f"Deleted {row_count} records in {end_time - start_time:.2f} seconds.")
    return row_count

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
    full_table_path = f'{DATABASE_PATH}/{table_path}'
    timestamp_field = args.timestamp_field
    delete_interval = args.delete_interval

    print(f"Connecting to YDB to delete records from {full_table_path}")
    print(f"Will delete records where {timestamp_field} < CurrentUtcDate() - Interval(\"P{delete_interval}\")")

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        # Wait until driver is ready
        driver.wait(timeout=10, fail_fast=True)
        
        with ydb.SessionPool(driver) as pool:
            try:
                def transaction_delete(session):
                    return delete_old_records(session, full_table_path, timestamp_field, delete_interval)
                
                deleted_count = pool.retry_operation_sync(transaction_delete)
                
                print(f"Successfully deleted old records from {full_table_path}")
                print(f"Total records deleted: {deleted_count}")
                return 0
            except ydb.Error as e:
                print(f"Error deleting records: {e}")
                return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)

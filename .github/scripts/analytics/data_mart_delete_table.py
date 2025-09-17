#!/usr/bin/env python3

import argparse
import ydb
import configparser
import os

# Load configuration
dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

def parse_args():
    parser = argparse.ArgumentParser(description="Delete a YDB table")
    parser.add_argument("--table_path", required=True, help="Table path and name to delete")
    
    return parser.parse_args()

def check_table_exists(session, table_path):
    """Check if table exists"""
    try:
        session.describe_table(table_path)
        return True
    except ydb.SchemeError:
        return False

def delete_table(session, table_path):
    """Delete the specified table."""
    try:
        session.drop_table(table_path)
        print(f"Table '{table_path}' successfully deleted.")
        return True
    except ydb.Error as e:
        print(f"Error deleting table: {e}")
        return False

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

    print(f"Connecting to YDB to delete table {full_table_path}")

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        # Wait until driver is ready
        driver.wait(timeout=10, fail_fast=True)
        
        with ydb.SessionPool(driver) as pool:
            # Проверяем существование таблицы
            def check_and_delete(session):
                exists = check_table_exists(session, full_table_path)
                if exists:
                    return delete_table(session, full_table_path)
                else:
                    print(f"Table '{full_table_path}' does not exist.")
                    return False
            
            result = pool.retry_operation_sync(check_and_delete)
            
            if result:
                print(f"Table {full_table_path} has been deleted successfully.")
                return 0
            else:
                print(f"No table was deleted.")
                return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)

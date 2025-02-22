#!/usr/bin/env python3

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


def drop_table(session, table_path):
    print(f"> Dropping table if exists: '{table_path}'")
    session.execute_scheme(f"DROP TABLE IF EXISTS `{table_path}`;")


def create_test_history_fast_table(session, table_path):
    print(f"> Creating table: '{table_path}'")
    session.execute_scheme(f"""
        CREATE TABLE `{table_path}` (
            `build_type` Utf8 NOT NULL,
            `job_name` Utf8 NOT NULL,
            `job_id` Uint64,
            `commit` Utf8,
            `branch` Utf8 NOT NULL,
            `pull` Utf8,
            `run_timestamp` Timestamp NOT NULL,
            `test_id` Utf8 NOT NULL,
            `suite_folder` Utf8,
            `test_name` Utf8,
            `full_name` Utf8 NOT NULL,
            `duration` Double,
            `status` Utf8,
            `status_description` Utf8,
            `owners` Utf8,
            PRIMARY KEY (`full_name`, `run_timestamp`, `job_name`, `branch`, `build_type`, test_id)
        )
        PARTITION BY HASH(run_timestamp)
        WITH (
        STORE = COLUMN,
        TTL = Interval("P7D") ON run_timestamp
        )
    """)


def bulk_upsert(table_client, table_path, rows):
    print(f"> Bulk upsert into: {table_path}")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("job_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("job_id", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("commit", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("pull", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("run_timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("test_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("duration", ydb.OptionalType(ydb.PrimitiveType.Double))
        .add_column("status", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("status_description", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


def get_missed_data_for_upload(driver):
    results = []
    query = f"""
        SELECT 
        build_type, 
        job_name, 
        job_id, 
        commit, 
        branch, 
        pull, 
        all_data.run_timestamp as run_timestamp, 
        test_id, 
        suite_folder, 
        test_name,
        cast(suite_folder || '/' || test_name as UTF8)  as full_name, 
        duration,
        status,
        status_description,
        owners
    FROM `test_results/test_runs_column`  as all_data
    LEFT JOIN (
        select distinct run_timestamp  from `test_results/analytics/test_history_fast`
    ) as fast_data_missed
    ON all_data.run_timestamp = fast_data_missed.run_timestamp
    WHERE
        all_data.run_timestamp >= CurrentUtcDate() - 6*Interval("P1D") AND
        fast_data_missed.run_timestamp is NULL
    """

    scan_query = ydb.ScanQuery(query, {})
    it = driver.table_client.scan_query(scan_query)
    print(f'missed data capturing')
    while True:
        try:
            result = next(it)
            results.extend(result.result_set.rows)
        except StopIteration:
            break
    return results


def main():

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        return 1
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    table_path = "test_results/analytics/test_history_fast"
    batch_size = 50000

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        with ydb.SessionPool(driver) as pool:
            prepared_for_upload_rows = get_missed_data_for_upload(driver)
            print(f'Preparing to upsert: {len(prepared_for_upload_rows)} rows')
            if prepared_for_upload_rows:
                for start in range(0, len(prepared_for_upload_rows), batch_size):
                    batch_rows_for_upload = prepared_for_upload_rows[start:start + batch_size]
                    print(f'upserting: {start}-{start + len(batch_rows_for_upload)}/{len(prepared_for_upload_rows)} rows')
                    bulk_upsert(driver.table_client, f'{DATABASE_PATH}/{table_path}', batch_rows_for_upload)
                print('Tests uploaded')
            else:
                print('Nothing to upload')


if __name__ == "__main__":
    main()

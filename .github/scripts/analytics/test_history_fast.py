#!/usr/bin/env python3

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


def check_table_exists(session, table_path):
    """Check if table exists"""
    try:
        session.describe_table(table_path)
        return True
    except ydb.SchemeError as e:
        print(f"Table does not exist, error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error while checking table: {e}")
        return False


def create_test_history_fast_table(session, table_path):
    print(f"> Creating table: '{table_path}'")
    start_time = time.time()
    try:
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
                PRIMARY KEY (`run_timestamp`, `full_name`, `job_name`, `branch`, `build_type`, test_id)
            )
            PARTITION BY HASH(run_timestamp)
            WITH (
            STORE = COLUMN,
            TTL = Interval("P7D") ON run_timestamp
            )
        """)
        elapsed = time.time() - start_time
        print(f"Table '{table_path}' created successfully with 7-day TTL (took {elapsed:.2f}s)")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"Error creating table '{table_path}': {e} (took {elapsed:.2f}s)")
        raise


def bulk_upsert(table_client, table_path, rows):
    print(f"> Bulk upsert into: {table_path}")
    start_time = time.time()
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
    elapsed = time.time() - start_time
    print(f"Bulk upsert completed: {len(rows)} rows (took {elapsed:.2f}s)")


def get_missed_data_for_upload(driver, full_table_path):
    print(f'Fetching missed data')
    start_time = time.time()
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
        select distinct run_timestamp  from `{full_table_path}`
    ) as fast_data_missed
    ON all_data.run_timestamp = fast_data_missed.run_timestamp
    WHERE
        all_data.run_timestamp >= CurrentUtcDate() - 6*Interval("P1D")
        and String::Contains(all_data.test_name, '.flake8')  = FALSE
        and (CASE 
            WHEN String::Contains(all_data.test_name, 'sole chunk') OR String::Contains(all_data.test_name, 'chunk+chunk')  OR String::Contains(all_data.test_name, '] chunk') THEN TRUE
            ELSE FALSE
            END) = FALSE
        and (all_data.branch = 'main' or all_data.branch like 'stable-%')
        and fast_data_missed.run_timestamp is NULL
    """

    scan_query = ydb.ScanQuery(query, {})
    it = driver.table_client.scan_query(scan_query)
    while True:
        try:
            result = next(it)
            results.extend(result.result_set.rows)
        except StopIteration:
            break
    elapsed = time.time() - start_time
    print(f'Total missed data captured: {len(results)} records (took {elapsed:.2f}s)')
    return results


def main():
    print("Starting test_history_fast script")
    script_start_time = time.time()

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
    full_table_path = f'{DATABASE_PATH}/{table_path}'
    batch_size = 1000

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        with ydb.SessionPool(driver) as pool:
            # Проверяем существование таблицы и создаем её если нужно
            def check_and_create_table(session):
                print(f"Checking table: {full_table_path}")
                table_check_start = time.time()
                exists = check_table_exists(session, full_table_path)
                if not exists:
                    print("Creating table...")
                    create_test_history_fast_table(session, full_table_path)
                    # Проверяем, что таблица действительно создалась
                    exists_after_creation = check_table_exists(session, full_table_path)
                    if exists_after_creation:
                        table_check_elapsed = time.time() - table_check_start
                        print(f"Table created and verified successfully (took {table_check_elapsed:.2f}s)")
                    else:
                        print("ERROR: Table was not created successfully!")
                        return False
                    return True
                else:
                    table_check_elapsed = time.time() - table_check_start
                    print(f"Table already exists (took {table_check_elapsed:.2f}s)")
                return exists
            
            pool.retry_operation_sync(check_and_create_table)
            
            # Продолжаем с основной логикой скрипта
            prepared_for_upload_rows = get_missed_data_for_upload(driver, full_table_path)
            print(f'Preparing to upsert: {len(prepared_for_upload_rows)} rows')
            if prepared_for_upload_rows:
                upload_start_time = time.time()
                for start in range(0, len(prepared_for_upload_rows), batch_size):
                    batch_start_time = time.time()
                    batch_rows_for_upload = prepared_for_upload_rows[start:start + batch_size]
                    print(f'upserting: {start}-{start + len(batch_rows_for_upload)}/{len(prepared_for_upload_rows)} rows')
                    bulk_upsert(driver.table_client, full_table_path, batch_rows_for_upload)
                    batch_elapsed = time.time() - batch_start_time
                    print(f'Batch completed (took {batch_elapsed:.2f}s)')
                upload_elapsed = time.time() - upload_start_time
                print(f'All tests uploaded (total upload time: {upload_elapsed:.2f}s)')
            else:
                print('Nothing to upload')
    
    script_elapsed = time.time() - script_start_time
    print(f"Script completed successfully (total time: {script_elapsed:.2f}s)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import ydb
import os
from ydb_wrapper import YDBWrapper


def create_test_history_fast_table(ydb_wrapper, table_path):
    print(f"> Creating table: '{table_path}'")
    create_sql = f"""
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
    """
    ydb_wrapper.create_table(table_path, create_sql)


def get_missed_data_for_upload(ydb_wrapper):
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
        all_data.run_timestamp >= CurrentUtcDate() - 6*Interval("P1D")
        and String::Contains(all_data.test_name, '.flake8')  = FALSE
        and (CASE 
            WHEN String::Contains(all_data.test_name, 'sole chunk') 
                OR String::Contains(all_data.test_name, 'chunk+chunk') 
                OR String::Contains(all_data.test_name, '[chunk]') 
            THEN TRUE
            ELSE FALSE
            END) = FALSE
        and (all_data.branch = 'main' or all_data.branch like 'stable-%' or all_data.branch like 'stream-nb-2%')
        and fast_data_missed.run_timestamp is NULL
    """

    print(f'missed data capturing')
    results = ydb_wrapper.execute_scan_query(query)
    return results


def main():
    # Initialize YDB wrapper with context manager for automatic cleanup
    script_name = os.path.basename(__file__)
    with YDBWrapper(script_name=script_name) as ydb_wrapper:
        
        
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return 1
        
        table_path = "test_results/analytics/test_history_fast"
        full_table_path = f'{ydb_wrapper.database_path}/{table_path}'
        batch_size = 1000

        # Create table if it doesn't exist
        create_test_history_fast_table(ydb_wrapper, full_table_path)
        
        # Get missed data for upload
        prepared_for_upload_rows = get_missed_data_for_upload(ydb_wrapper)
        print(f'Preparing to upsert: {len(prepared_for_upload_rows)} rows')
        
        if prepared_for_upload_rows:
            # Подготавливаем column_types один раз (те же поля, что возвращает get_missed_data_for_upload)
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
            
            # Используем bulk_upsert_batches для агрегированной статистики
            ydb_wrapper.bulk_upsert_batches(full_table_path, prepared_for_upload_rows, column_types, batch_size)
            print('Tests uploaded')
        else:
            print('Nothing to upload')


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import os
import posixpath
import time
import ydb
from collections import Counter
from ydb_wrapper import YDBWrapper


def create_tables(ydb_wrapper, table_path):
    print(f"> create table if not exists:'{table_path}'")
    
    create_sql = f"""
        CREATE table IF NOT EXISTS `{table_path}` (
            `test_name` Utf8 NOT NULL,
            `suite_folder` Utf8 NOT NULL,
            `full_name` Utf8 NOT NULL,
            `run_timestamp_last` Timestamp NOT NULL,
            `owners` Utf8 ,
            PRIMARY KEY (`test_name`, `suite_folder`, `full_name`)
        )
            PARTITION BY HASH(suite_folder,`full_name`)
            WITH (STORE = COLUMN)
        """
    
    ydb_wrapper.create_table(table_path, create_sql)


def main():
    with YDBWrapper() as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return 1
        
        # Get table paths from config
        test_runs_table = ydb_wrapper.get_table_path("test_results")
        table_path = ydb_wrapper.get_table_path("testowners")    

        query_get_owners = f"""
   select 
        DISTINCT test_name, 
        suite_folder, 
        suite_folder || '/' || test_name as full_name, 
        FIRST_VALUE(owners) OVER w AS owners, 
        FIRST_VALUE (run_timestamp) OVER w AS run_timestamp_last 
        FROM 
        `{test_runs_table}` 
    WHERE 
        run_timestamp >= CurrentUtcDate()- Interval("P1D") 
        AND branch = 'main' 
        and job_name in (
            'Nightly-run',
            'Regression-run',
            'Regression-run_Large',
            'Regression-run_Small_and_Medium',
            'Regression-run_compatibility',
            'Regression-whitelist-run',
            'Postcommit_relwithdebinfo', 
            'Postcommit_asan'
        ) 
        WINDOW w AS (
            PARTITION BY test_name, 
            suite_folder 
            ORDER BY 
            run_timestamp DESC
        ) 
        order by 
        run_timestamp_last desc
        
    """
    
        # Execute query using ydb_wrapper
        results = ydb_wrapper.execute_scan_query(query_get_owners)

        print(f'testowners data captured, {len(results)} rows')
        test_list = []
        for row in results:
            test_list.append({
                'suite_folder': row['suite_folder'],
                'test_name': row['test_name'],
                'full_name': row['full_name'],
                'owners': row['owners'],
                'run_timestamp_last': row['run_timestamp_last'],
            })
        
        print('upserting testowners')
        create_tables(ydb_wrapper, table_path)
        
        # Prepare column_types
        column_types = (
            ydb.BulkUpsertColumns()
            .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("run_timestamp_last", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
            .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        )
        
        ydb_wrapper.bulk_upsert_batches(table_path, test_list, column_types, batch_size=1000)

        print('testowners updated')


if __name__ == "__main__":
    main()

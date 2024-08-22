#!/usr/bin/env python3

import argparse
import configparser
import datetime
import os
import posixpath
import traceback
import time
import ydb
from collections import Counter
#from upload_test_results import get_codeowners_for_tests

dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def create_tables(pool,  table_path):
    print(f"> create table if not exists:'{table_path}'")

    def callee(session):
        session.execute_scheme(f"""
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
            """)

    return pool.retry_operation_sync(callee)


def bulk_upsert(table_client, table_path, rows):
    print(f"> bulk upsert: {table_path}")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("run_timestamp_last", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


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
    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        
        # settings, paths, consts
        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
        table_client = ydb.TableClient(driver, tc_settings)
        
        table_path = f'test_results/analytics/testowners'    

        query_get_owners = f"""
       select 
            DISTINCT test_name, 
            suite_folder, 
            suite_folder || '/' || test_name as full_name, 
            FIRST_VALUE(owners) OVER w AS owners, 
            FIRST_VALUE (run_timestamp) OVER w AS run_timestamp_last 
            FROM 
            `test_results/test_runs_column` 
        WHERE 
            run_timestamp >= CurrentUtcDate()- Interval("P10D") 
            AND branch = 'main' 
            and job_name in (
                'Nightly-run', 'Postcommit_relwithdebinfo', 
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
        query = ydb.ScanQuery(query_get_owners, {})
        # start transaction time
        start_time = time.time()
        it = driver.table_client.scan_query(query)
        # end transaction time

        results = []
        test_list = []
        while True:
            try:
                result = next(it)
                results = results + result.result_set.rows
            except StopIteration:
                break
        end_time = time.time()
        print(f'transaction duration: {end_time - start_time}')

        print(f'testowners data captured, {len(results)} rows')
        for row in results:
            test_list.append({
                'suite_folder': row['suite_folder'],
                'test_name': row['test_name'],
                'full_name': row['full_name'],
                'owners': row['owners'],
                'run_timestamp_last': row['run_timestamp_last'],
            })
        print('upserting testowners')
        with ydb.SessionPool(driver) as pool:

            create_tables(pool, table_path)
            full_path = posixpath.join(DATABASE_PATH, table_path)
            bulk_upsert(driver.table_client, full_path,
                        test_list)

        print('testowners updated')


if __name__ == "__main__":
    main()

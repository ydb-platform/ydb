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

dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

build_preset = os.environ.get("build_preset")
branch = os.environ.get("branch_to_compare")

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def create_tables(pool,  table_path):
    print(f"> create table: {table_path}")

    def callee(session):
        session.execute_scheme(f"""
            CREATE table IF NOT EXISTS`{table_path}` (
                branch Utf8 NOT NULL,
                build_type Utf8 NOT NULL,
                commit Utf8 NOT NULL,
                duration Double,
                full_name Utf8 NOT NULL,
                job_id Uint64,
                job_name Utf8,
                log Utf8,
                logsdir Utf8,
                owners Utf8,
                pull Utf8,
                run_timestamp Timestamp NOT NULL,
                status_description Utf8,
                status Utf8 NOT NULL,
                stderr Utf8,
                stdout Utf8,
                suite_folder Utf8 NOT NULL,
                test_id Utf8 NOT NULL,
                test_name Utf8 NOT NULL,
                PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,build_type, status, run_timestamp)
            )
                PARTITION BY HASH(`full_name`, build_type )
                WITH (STORE = COLUMN)
            """)

    return pool.retry_operation_sync(callee)


def bulk_upsert(table_client, table_path, rows):
    print(f"> bulk upsert: {table_path}")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("commit", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("duration", ydb.OptionalType(ydb.PrimitiveType.Double))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("job_id", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("job_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("log", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("logsdir", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("pull", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("run_timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("status", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("status_description", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("stderr", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("stdout", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        
    )
    table_client.bulk_upsert(table_path, rows, column_types)


def main():
   # parser = argparse.ArgumentParser()
   # parser.add_argument('--days-window', default=5, type=int, help='how many days back we collecting history')

   # args, unknown = parser.parse_known_args()
   # history_for_n_day = args.days_window
    
  #  print(f'Getting hostory in window {history_for_n_day} days')
    

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]="/home/kirrysin/fork_2/.github/scripts/my-robot-key.json"
        #return 1
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
        
       #table_path = f'test_results/analytics/flaky_tests_history_{history_for_n_day}_days'
       # default_start_date = datetime.date(2024, 7, 1)
        table_path = 'test_results/test_runs_results_column'
        
        with ydb.SessionPool(driver) as pool:
            create_tables(pool, table_path)
            
      
         # geting last timestamp from runs column
        default_start_date = datetime.datetime(2024, 7, 1)
        last_date_query = f"select max(run_timestamp) as last_run_timestamp from `{table_path}`"
        query = ydb.ScanQuery(last_date_query, {})
        it = table_client.scan_query(query)
        results = []
        start_time = time.time()
        while True:
            try:
                result = next(it)
                results = results + result.result_set.rows
            except StopIteration:
                break
            
        end_time = time.time()
        print(f"transaction 'geting last timestamp from runs column' duration: {end_time - start_time}")

        if results[0] and results[0].get( 'max_date_window', default_start_date) is not None:
            last_date = results[0].get(
                'max_date_window', default_start_date).strftime("%Y-%m-%dT%H:%M:%SZ")

        else:
            last_date = ddefault_start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # geting timestamp list from runs
        last_date_query = f"""select distinct run_timestamp from `test_results/test_runs_results`
        where run_timestamp >=Timestamp('{last_date}')"""
        query = ydb.ScanQuery(last_date_query, {})
        it = table_client.scan_query(query)
        timestamps = []
        start_time = time.time()
        while True:
            try:
                result = next(it)
                timestamps = timestamps + result.result_set.rows
            except StopIteration:
                break
        end_time = time.time()
        print(f"transaction 'geting timestamp list from runs' duration: {end_time - start_time}")

        for ts in timestamps:
            # getting history for dates >= last_date
            query_get_runs = f"""
            select * from `test_results/test_runs_results` 
            where run_timestamp = cast({ts['run_timestamp']} as Timestamp)
            """
            query = ydb.ScanQuery(query_get_runs, {})
            # start transaction time
            start_time = time.time()
            it = driver.table_client.scan_query(query)
            # end transaction time

            results = []
            prepared_for_update_rows = []
            while True:
                try:
                    result = next(it)
                    results = results + result.result_set.rows
                except StopIteration:
                    break
            end_time = time.time()
            print(f'transaction duration: {end_time - start_time}')

            print(f'runs data captured, {len(results)} rows')
            for row in results:
                prepared_for_update_rows.append({
                    'branch': row['branch'],
                    'build_type': row['build_type'], 
                    'commit': row['commit'],
                    'duration': row['duration'],
                    'full_name': f"{row['suite_folder']}/{row['test_name']}",
                    'job_id': row['job_id'],
                    'job_name': row['job_name'], 
                    'log': row['log'],
                    'logsdir': row['logsdir'],
                    'owners': row['owners'],
                    'pull': row['pull'],
                    'run_timestamp': row['run_timestamp'],
                    'status_description': row['status_description'],
                    'status': row['status'],
                    'stderr': row['stderr'],
                    'stdout': row['stdout'],
                    'suite_folder': row['suite_folder'],
                    'test_id': row['test_id'],
                    'test_name': row['test_name'],
                })
            print('upserting runs')
            with ydb.SessionPool(driver) as pool:

                full_path = posixpath.join(DATABASE_PATH, table_path)
                bulk_upsert(driver.table_client, full_path,
                            prepared_for_update_rows)

            print('history updated')


if __name__ == "__main__":
    main()

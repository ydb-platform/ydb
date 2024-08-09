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
            CREATE table `{table_path}` (
                `test_name` Utf8 NOT NULL,
                `suite_folder` Utf8 NOT NULL,
                `full_name` Utf8 NOT NULL,
                `date_window` Date NOT NULL,
                `days_ago_window` Uint64 NOT NULL,
                `history` String,
                `history_class` String,
                `pass_count` Uint64,
                `mute_count` Uint64,
                `fail_count` Uint64,
                `skip_count` Uint64,
                PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,date_window)
            )
                PARTITION BY HASH(`full_name`)
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
        .add_column("date_window", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("days_ago_window", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("history", ydb.OptionalType(ydb.PrimitiveType.String))
        .add_column("history_class", ydb.OptionalType(ydb.PrimitiveType.String))
        .add_column("pass_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("mute_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("fail_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("skip_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days-window', default=5, type=int, help='how many days back we collecting history')

    args, unknown = parser.parse_known_args()
    history_for_n_day = args.days_window
    
    print(f'Getting hostory in window {history_for_n_day} days')
    

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
        
        table_path = f'test_results/analytics/flaky_tests_history_{history_for_n_day}_days'
        default_start_date = datetime.date(2024, 7, 1)
        
        with ydb.SessionPool(driver) as pool:
            create_tables(pool, table_path)
            
        # geting last date from history
        last_date_query = f"select max(date_window) as max_date_window from `{table_path}`"
        query = ydb.ScanQuery(last_date_query, {})
        it = table_client.scan_query(query)
        results = []
        while True:
            try:
                result = next(it)
                results = results + result.result_set.rows
            except StopIteration:
                break
    
        if results[0] and results[0].get( 'max_date_window', default_start_date) is not None:
            last_date = results[0].get(
                'max_date_window', default_start_date).strftime('%Y-%m-%d')
        else:
            last_date = default_start_date.strftime('%Y-%m-%d')
        
        print(f'last hisotry date: {last_date}')
        # getting history for dates >= last_date
        query_get_history = f"""
    select
        full_name,
        date_base,
        history_list,
        dist_hist,
        suite_folder,
        test_name
    from (
        select
            full_name,
            date_base,
            AGG_LIST(status) as history_list ,
            String::JoinFromList( AGG_LIST_DISTINCT(status) ,',') as dist_hist,
            suite_folder,
            test_name
        from (
            select * from (
                select * from (
                    select
                        DISTINCT suite_folder || '/' || test_name as full_name,
                        suite_folder,
                        test_name
                    from  `test_results/test_runs_results`
                    where
                        status in ('failure','mute')
                        and job_name in ('Nightly-run', 'Postcommit_relwithdebinfo')
                        and build_type = 'relwithdebinfo' and
                        run_timestamp >= Date('{last_date}') -{history_for_n_day}*Interval("P1D") 
                ) as tests_with_fails
                cross join (
                    select 
                        DISTINCT DateTime::MakeDate(run_timestamp) as date_base
                    from  `test_results/test_runs_results`
                    where
                        status in ('failure','mute')
                        and job_name in ('Nightly-run', 'Postcommit_relwithdebinfo')
                        and build_type = 'relwithdebinfo'
                        and run_timestamp>= Date('{last_date}')
                    ) as date_list
                ) as test_and_date
            left JOIN (
                select * from (
                    select
                        suite_folder || '/' || test_name as full_name,
                        run_timestamp,
                        status
                        --ROW_NUMBER() OVER (PARTITION BY test_name ORDER BY run_timestamp DESC) AS rn
                    from  `test_results/test_runs_results`
                    where
                        run_timestamp >= Date('{last_date}') -{history_for_n_day}*Interval("P1D") and
                        job_name in ('Nightly-run', 'Postcommit_relwithdebinfo')
                        and build_type = 'relwithdebinfo'
                )
            ) as hist
            ON test_and_date.full_name=hist.full_name
            where
                hist.run_timestamp >= test_and_date.date_base -{history_for_n_day}*Interval("P1D") AND
                hist.run_timestamp <= test_and_date.date_base

        )
        GROUP BY full_name,suite_folder,test_name,date_base
 
    )
        """
        query = ydb.ScanQuery(query_get_history, {})
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

        print(f'history data captured, {len(results)} rows')
        for row in results:
            row['count'] = dict(zip(list(row['history_list']), [list(
                row['history_list']).count(i) for i in list(row['history_list'])]))
            prepared_for_update_rows.append({
                'suite_folder': row['suite_folder'],
                'test_name': row['test_name'],
                'full_name': row['full_name'],
                'date_window': row['date_base'],
                'days_ago_window': history_for_n_day,
                'history': ','.join(row['history_list']).encode('utf8'),
                'history_class': row['dist_hist'],
                'pass_count': row['count'].get('passed', 0),
                'mute_count': row['count'].get('mute', 0),
                'fail_count': row['count'].get('failure', 0),
                'skip_count': row['count'].get('skipped', 0),
            })
        print('upserting history')
        with ydb.SessionPool(driver) as pool:

            create_tables(pool, table_path)
            full_path = posixpath.join(DATABASE_PATH, table_path)
            bulk_upsert(driver.table_client, full_path,
                        prepared_for_update_rows)

        print('history updated')


if __name__ == "__main__":
    main()

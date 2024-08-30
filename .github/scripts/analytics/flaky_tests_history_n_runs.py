#!/usr/bin/env python3

import argparse
import configparser
import datetime
import os
import posixpath
import traceback
import time
import ydb

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
                `build_type` Utf8 NOT NULL,
                `branch` Utf8 NOT NULL,
                `runs_window` Uint64 NOT NULL,
                `history` String,
                `history_class` String,
                `pass_count` Uint64,
                `mute_count` Uint64,
                `fail_count` Uint64,
                `skip_count` Uint64,
                PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,date_window,runs_window,build_type,branch)
            )
                PARTITION BY HASH(`full_name`,build_type,branch)
                WITH (STORE = COLUMN)
            """)

    return pool.retry_operation_sync(callee)


def bulk_upsert(table_client, table_path, rows):
    print(f"> bulk upsert: {table_path}")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("date_window", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("runs_window", ydb.OptionalType(ydb.PrimitiveType.Uint64))
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
    parser.add_argument('--runs', default=10,choices=[10, 25, 50], type=int, help='how many runs back we collecting history')
    parser.add_argument('--build_type',choices=['relwithdebinfo', 'release-asan'], default='relwithdebinfo', type=str, help='build : relwithdebinfo or release-asan')
    parser.add_argument('--branch', default='main',choices=['main'], type=str, help='branch')

    args, unknown = parser.parse_known_args()
    history_for_n_runs = args.runs
    build_type = args.build_type
    branch = args.branch
        
    print(f'Getting hostory in window {history_for_n_runs} runs')
    

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
        
        table_path = f'test_results/analytics/flaky_tests_history_{history_for_n_runs}_runs'
        default_start_date = datetime.date(2024, 7, 19)
        
        with ydb.SessionPool(driver) as pool:
            create_tables(pool, table_path)
            
        # geting last date from history
        last_date_query = f"""select max(date_window) as max_date_window from `{table_path}`
            where build_type = '{build_type}' and branch = '{branch}'"""
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
            last_datetime = results[0].get(
                'max_date_window', default_start_date)
        else:
            last_date = default_start_date.strftime('%Y-%m-%d')
            last_datetime = default_start_date

        print(f'last hisotry date: {last_date}')
        today = datetime.date.today()
        date_list = [today - datetime.timedelta(days=x) for x in range((today - last_datetime).days+1)]
        for date in sorted(date_list):
            query_get_history = f"""
               select
                    full_name,
                    date_base,
                    build_type,
                    branch,
                    history_list,
                    dist_hist,
                    suite_folder,
                    test_name
                from (
                    select
                        full_name,
                        date_base,
                        build_type,
                        branch,
                        AGG_LIST(status) as history_list ,
                        String::JoinFromList( AGG_LIST_DISTINCT(status) ,',') as dist_hist,
                        suite_folder,
                        test_name
                    from (
                        select * from (
                                select t1.test_name, t1.suite_folder, t1.full_name,
                                Date('{date}') as date_base,
                               '{build_type}' as  build_type,
                               '{branch}' as  branch
                                from  `test_results/analytics/testowners` as t1
                            ) as test_and_date
                        left JOIN (
                            select * from (
                                select
                                    suite_folder || '/' || test_name as full_name,
                                    run_timestamp,
                                    status ,
                                    ROW_NUMBER() OVER (PARTITION BY test_name ORDER BY run_timestamp DESC) AS run_number
                                from  `test_results/test_runs_column`
                                where
                                    run_timestamp <= Date('{date}') 
                                    and run_timestamp >= Date('{date}') -14*Interval("P1D") 
                                    and job_name in ('Postcommit_relwithdebinfo','Postcommit_asan')
                                    and build_type = '{build_type}'
                                    and status != 'skipped'
                                    and branch = '{branch}'
                            )
                            where run_number <= {history_for_n_runs}
                        ) as hist
                        ON test_and_date.full_name=hist.full_name
                    )
                    GROUP BY full_name,suite_folder,test_name,date_base,build_type,branch
            
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
                    'build_type': row['build_type'],
                    'branch': row['branch'],
                    'runs_window': history_for_n_runs,
                    'history': ','.join(row['history_list']).encode('utf8'),
                    'history_class': row['dist_hist'],
                    'pass_count': row['count'].get('passed', 0),
                    'mute_count': row['count'].get('mute', 0),
                    'fail_count': row['count'].get('failure', 0),
                    'skip_count': row['count'].get('skipped', 0),
                })
            print(f'upserting history for date {date}')
            with ydb.SessionPool(driver) as pool:

                full_path = posixpath.join(DATABASE_PATH, table_path)
                bulk_upsert(driver.table_client, full_path,
                            prepared_for_update_rows)

            print('flaky history updated')
        print('finished')


if __name__ == "__main__":
    main()

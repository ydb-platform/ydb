#!/usr/bin/env python3

import argparse
import configparser
import datetime
import os
import posixpath
import sys
import traceback
import time
import ydb
from collections import Counter
import pandas as pd


dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)
DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def create_tables(pool, table_path):
    print(f"> create table if not exists:'{table_path}'")

    def callee(session):
        session.execute_scheme(
            f"""
            CREATE table IF NOT EXISTS `{table_path}` (
                `test_name` Utf8 NOT NULL,
                `suite_folder` Utf8 NOT NULL,
                `full_name` Utf8 NOT NULL,
                `date_window` Date NOT NULL,
                `build_type` Utf8 NOT NULL,
                `branch` Utf8 NOT NULL,
                `days_ago_window` Uint64 NOT NULL,
                `history` Utf8,
                `history_class` Utf8,
                `pass_count` Uint64,
                `mute_count` Uint64,
                `fail_count` Uint64,
                `skip_count` Uint64,
                `success_rate` Uint64,
                `summary` Utf8,
                `owner` Utf8,
                `is_muted` Uint32,
                `is_test_chunk` Uint32,
                `state` Utf8,
                `previous_state` Utf8,
                `state_change_date` Date,
                `days_in_state` Uint64,
                PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,date_window, build_type, branch)
            )
                PARTITION BY HASH(build_type,branch)
                WITH (STORE = COLUMN)
            """
        )

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
        .add_column("days_ago_window", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("history", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("history_class", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("pass_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("mute_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("fail_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("skip_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("success_rate", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("summary", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("owner", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("is_muted", ydb.OptionalType(ydb.PrimitiveType.Uint32))
        .add_column("is_test_chunk", ydb.OptionalType(ydb.PrimitiveType.Uint32))
        .add_column("state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("previous_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("state_change_date", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("days_in_state", ydb.OptionalType(ydb.PrimitiveType.Uint64))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--days-window', default=1, type=int, help='how many days back we collecting history')
    parser.add_argument(
        '--build_type',
        choices=['relwithdebinfo', 'release-asan'],
        default='relwithdebinfo',
        type=str,
        help='build : relwithdebinfo or release-asan',
    )
    parser.add_argument('--branch', default='main', choices=['main'], type=str, help='branch')

    args, unknown = parser.parse_known_args()
    history_for_n_day = args.days_window
    build_type = args.build_type
    branch = args.branch

    print(f'Getting aggregated history in window {history_for_n_day} days')

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
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

        # settings, paths, consts
        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=False)
        table_client = ydb.TableClient(driver, tc_settings)

        table_path = f'test_results/analytics/tests_monitor'

        data = {
            'test_name': [],
            'suite_folder': [],
            'full_name': [],
            'date_window': [],
            'build_type': [],
            'branch': [],
            'owners': [],
            'days_ago_window': [],
            'history': [],
            'history_class': [],
            'pass_count': [],
            'mute_count': [],
            'fail_count': [],
            'skip_count': [],
            'is_muted': [],
        }
        base_date = datetime.datetime(1970, 1, 1)

        today = datetime.date.today()
        default_start_date = datetime.date(2024, 8, 1)
        date_list = [today - datetime.timedelta(days=x) for x in range((today - default_start_date).days + 1)]
        for date in sorted(date_list):
            query_get_history = f"""
                SELECT
                    hist.branch AS branch,
                    hist.build_type AS build_type,
                    hist.date_window AS date_window, 
                    hist.days_ago_window AS days_ago_window,
                    hist.fail_count AS fail_count,
                    hist.full_name AS full_name,
                    hist.history AS history,
                    hist.history_class AS history_class,
                    hist.mute_count AS mute_count,
                    COALESCE(owners_t.owners, fallback_t.owners) AS owners,
                    hist.pass_count AS pass_count,
                    COALESCE(owners_t.run_timestamp_last, NULL) AS run_timestamp_last,
                    COALESCE(owners_t.is_muted, NULL) AS is_muted,
                    hist.skip_count AS skip_count,
                    hist.suite_folder AS suite_folder,
                    hist.test_name AS test_name
                
                FROM (
                    SELECT * FROM
                    `test_results/analytics/flaky_tests_window_{history_for_n_day}_days` 
                    WHERE 
                    date_window = Date('{date}')
                    AND build_type = '{build_type}'
                    AND branch = '{branch}'
                    --and full_name in (
                    --'ydb/tests/functional/compatibility/test_compatibility.py.TestCompatibility.test_simple')
                    
                    
                ) AS hist 
                LEFT JOIN (
                    SELECT 
                        test_name,
                        suite_folder,
                        owners,
                        run_timestamp_last,
                        is_muted,
                        date
                    FROM 
                        `test_results/all_tests_with_owner_and_mute`
                    WHERE 
                        branch = '{branch}'
                        AND date = Date('{date}')
                ) AS owners_t
                ON 
                    hist.test_name = owners_t.test_name
                    AND hist.suite_folder = owners_t.suite_folder
                    AND hist.date_window = owners_t.date
                LEFT JOIN (
                    SELECT 
                        test_name,
                        suite_folder,
                        owners
                    FROM 
                        `test_results/analytics/testowners`
                ) AS fallback_t
                ON 
                    hist.test_name = fallback_t.test_name
                    AND hist.suite_folder = fallback_t.suite_folder
                WHERE
                    owners_t.test_name IS NOT NULL OR fallback_t.test_name IS NOT NULL
            
        
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
            print(f'transaction for {date} duration: {end_time - start_time}')
            start_time = time.time()
            for row in results:
                data['test_name'].append(row['test_name'])
                data['suite_folder'].append(row['suite_folder'])
                data['full_name'].append(row['full_name'])
                data['date_window'].append(base_date + datetime.timedelta(days=row['date_window']))
                data['build_type'].append(row['build_type'])
                data['branch'].append(row['branch'])
                data['owners'].append(row['owners'])
                data['days_ago_window'].append(row['days_ago_window'])
                data['history'].append(
                    row['history'].decode('utf-8') if isinstance(row['history'], bytes) else row['history']
                )
                data['history_class'].append(
                    row['history_class'].decode('utf-8')
                    if isinstance(row['history_class'], bytes)
                    else row['history_class']
                )
                data['pass_count'].append(row['pass_count'])
                data['mute_count'].append(row['mute_count'])
                data['fail_count'].append(row['fail_count'])
                data['skip_count'].append(row['skip_count'])
                data['is_muted'].append(row['is_muted'])

        end_time = time.time()
        print(f'Dataframe inited: {end_time - start_time}')
        start_time = time.time()
        df = pd.DataFrame(data)

        df = df.sort_values(by=['full_name', 'date_window'])
        
        end_time = time.time()
        print(f'Dataframe sorting: {end_time - start_time}')
        start_time = time.time()

        def determine_state(row):
            history_class = row['history_class']
            is_muted = row['is_muted']

            if is_muted == 1:
                if 'mute' in history_class:
                    return 'Muted Flaky'
                elif 'pass' in history_class:
                    return 'Muted Stable'
                elif 'skipped' in history_class or not history_class:
                    return 'Skipped'
                else:
                    return history_class
            else:
                if 'failure' in history_class and 'mute' not in history_class:
                    return 'Flaky'
                elif 'mute' in history_class:
                    return 'Muted'
                elif 'skipped' in history_class or not history_class:
                    return 'Skipped'
                elif 'pass' in history_class:
                    return 'Passed'
                else:
                    return history_class

        def calculate_streak_flag(row):
            if row['state'] == row['previous_state']:
                return 0
            else:
                return 1

        def calculate_success_rate(row):
            total_count = row['pass_count'] + row['mute_count'] + row['fail_count']
            if total_count == 0:
                return 0.0
            else:
                return (row['pass_count'] / total_count) * 100

        def calculate_summary(row):
            return (
                'Pass:'
                + str(row['pass_count'])
                + ' Fail:'
                + str(row['fail_count'])
                + ' Mute:'
                + str(row['mute_count'])
                + ' Skip:'
                + str(row['skip_count'])
            )

        def compute_owner(owner):
            if not owner or owner == '':
                return 'Unknown'
            elif ';;' in owner:
                parts = owner.split(';;', 1)
                if 'TEAM' in parts[0]:
                    return parts[0]
                else:
                    return parts[1]
            else:
                return owner

        df['success_rate'] = df.apply(calculate_success_rate, axis=1).astype(int)
        df['summary'] = df.apply(calculate_summary, axis=1)
        df['owner'] = df['owners'].apply(compute_owner)
        df['is_test_chunk'] = df['full_name'].str.contains('chunk chunk|chunk\+chunk', regex=True).astype(int)
        df['is_muted'] = df['is_muted'].fillna(0).astype(int)
        df['success_rate'].astype(int)
        df['state'] = df.apply(determine_state, axis=1)

        # Determ state for prev date and saving change state date
        previous_state_list = []
        state_change_date_list = []

        for name, group in df.groupby('full_name'):
            prev_status = 'no_runs'
            prev_date = datetime.datetime.strptime('01/08/2024', "%d/%m/%Y")
            for index, row in group.iterrows():
                previous_state_list.append(prev_status)
                if row['state'] != prev_status:

                    prev_status = row['state']
                    prev_date = row['date_window']

                state_change_date_list.append(prev_date)

        df['previous_state'] = previous_state_list
        df['state_change_date'] = state_change_date_list
        df['state_change_date'] = df['state_change_date'].dt.date
        df['date_window'] = df['date_window'].dt.date
        df['streak_flag'] = df.apply(calculate_streak_flag, axis=1)
        df['streak_id'] = df.groupby('full_name')['streak_flag'].cumsum()
        df['days_in_state'] = df.groupby(['full_name', 'streak_id']).cumcount() + 1

        result = df[
            [
                'full_name',
                'date_window',
                'suite_folder',
                'test_name',
                'days_ago_window',
                'build_type',
                'branch',
                'history',
                'history_class',
                'pass_count',
                'mute_count',
                'fail_count',
                'skip_count',
                'summary',
                'owner',
                'is_test_chunk',
                'is_muted',
                'state',
                'previous_state',
                'state_change_date',
                'days_in_state',
                'success_rate',
            ]
        ]

        end_time = time.time()
        print(f'Dataframe data processing {end_time - start_time}')
        print(f'Data collected, {len(result)} rows')
        start_time = time.time()
        prepared_for_update_rows = result.to_dict('records')
        end_time = time.time()
        print(f'Data converting to dict for upsert: {end_time - start_time}')

        start_upsert_time = time.time()

        with ydb.SessionPool(driver) as pool:

            create_tables(pool, table_path)
            full_path = posixpath.join(DATABASE_PATH, table_path)

            def chunk_data(data, chunk_size):
                for i in range(0, len(data), chunk_size):
                    yield data[i : i + chunk_size]

            chunk_size = 40000
            total_chunks = len(prepared_for_update_rows) // chunk_size + (
                1 if len(prepared_for_update_rows) % chunk_size != 0 else 0
            )

            for i, chunk in enumerate(chunk_data(prepared_for_update_rows, chunk_size), start=1):
                start_time = time.time()
                print(f"Uploading chunk {i}/{total_chunks}")
                bulk_upsert(driver.table_client, full_path, chunk)
                end_time = time.time()
                print(f'upsert for: {end_time - start_time} ')

        end_time = time.time()
        print(f'monitor data upserted: {end_time - start_upsert_time}')


if __name__ == "__main__":
    main()

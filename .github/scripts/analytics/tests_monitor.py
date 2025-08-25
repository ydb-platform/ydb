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
import pandas as pd
from collections import Counter
from multiprocessing import Pool, cpu_count

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
                `previous_mute_state` Uint32,
                `mute_state_change_date` Date,
                `days_in_mute_state` Uint64,
                `previous_state_filtered` Utf8,
                `state_change_date_filtered` Date,
                `days_in_state_filtered` Uint64,
                `state_filtered` Utf8,
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
        .add_column("previous_mute_state", ydb.OptionalType(ydb.PrimitiveType.Uint32))
        .add_column("days_in_mute_state", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("mute_state_change_date", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("previous_state_filtered", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("state_change_date_filtered", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("days_in_state_filtered", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("state_filtered", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


def process_test_group(name, group, last_day_data, default_start_date):
    state_list_for_filter = ['Muted', 'Muted Flaky', 'Muted Stable', 'Flaky', 'Passed']
    """Processes data for a single test group (by full_name)."""

    previous_state_list = []
    state_change_date_list = []
    days_in_state_list = []

    previous_mute_state_list = []
    mute_state_change_date_list = []
    days_in_mute_state_list = []

    previous_state_filtered_list = []
    state_change_date_filtered_list = []
    days_in_state_filtered_list = []
    state_filtered_list = []

    # Get 'days_in_state' for the last existing day for the current test
    if last_day_data is not None and last_day_data[last_day_data['full_name'] == name].shape[0] > 0:
        prev_state = last_day_data[last_day_data['full_name'] == name]['state'].iloc[0]
        prev_date = last_day_data[last_day_data['full_name'] == name]['state_change_date'].iloc[0]
        current_days_in_state = last_day_data[last_day_data['full_name'] == name]['days_in_state'].iloc[0]
        
        prev_mute_state = last_day_data[last_day_data['full_name'] == name]['is_muted'].iloc[0]
        prev_mute_date = last_day_data[last_day_data['full_name'] == name]['mute_state_change_date'].iloc[0]
        current_days_in_mute_state = last_day_data[last_day_data['full_name'] == name]['days_in_mute_state'].iloc[0]
        
        prev_state_filtered = last_day_data[last_day_data['full_name'] == name]['state_filtered'].iloc[0]
        prev_date_filtered = last_day_data[last_day_data['full_name'] == name]['state_change_date_filtered'].iloc[0]
        current_days_in_state_filtered = last_day_data[last_day_data['full_name'] == name][
            'days_in_state_filtered'
        ].iloc[0]

        saved_prev_state = last_day_data[last_day_data['full_name'] == name]['previous_state'].iloc[0]
        saved_prev_mute_state= last_day_data[last_day_data['full_name'] == name]['previous_mute_state'].iloc[0]
        saved_prev_state_filtered = last_day_data[last_day_data['full_name'] == name]['previous_state_filtered'].iloc[0]
    else:
        prev_state = 'no_runs'
        prev_date = datetime.datetime(default_start_date.year, default_start_date.month, default_start_date.day)
        current_days_in_state = 0
        
        prev_mute_state = 0
        prev_mute_date = datetime.datetime(default_start_date.year, default_start_date.month, default_start_date.day)
        current_days_in_mute_state = 0

        state_filtered = ''
        prev_state_filtered = 'no_runs'
        prev_date_filtered = datetime.datetime(
            default_start_date.year, default_start_date.month, default_start_date.day
        )
        current_days_in_state_filtered = 0
        
        saved_prev_state = prev_state
        saved_prev_mute_state = prev_mute_state
        saved_prev_state_filtered = prev_state_filtered

    for index, row in group.iterrows():
        # Process prev state
        current_days_in_state += 1
        if row['state'] != prev_state:
            saved_prev_state = prev_state
            prev_state = row['state']
            prev_date = row['date_window']
            current_days_in_state = 1
        previous_state_list.append(saved_prev_state)
        state_change_date_list.append(prev_date)
        days_in_state_list.append(current_days_in_state)
        
        # Process prev mute state

        current_days_in_mute_state += 1
        if row['is_muted'] != prev_mute_state:
            saved_prev_mute_state = prev_mute_state
            prev_mute_state = row['is_muted']
            prev_mute_date = row['date_window']
            current_days_in_mute_state = 1

        previous_mute_state_list.append(saved_prev_mute_state)
        mute_state_change_date_list.append(prev_mute_date)
        days_in_mute_state_list.append(current_days_in_mute_state)

        # Process filtered states

        if row['state'] not in state_list_for_filter:
            state_filtered = prev_state_filtered
        else:
            state_filtered = row['state']

        current_days_in_state_filtered += 1
        if state_filtered != prev_state_filtered:
            saved_prev_state_filtered = prev_state_filtered
            prev_state_filtered = state_filtered
            prev_date_filtered = row['date_window']
            current_days_in_state_filtered = 1

        state_filtered_list.append(state_filtered)
        previous_state_filtered_list.append(saved_prev_state_filtered)
        state_change_date_filtered_list.append(prev_date_filtered)
        days_in_state_filtered_list.append(current_days_in_state_filtered)

    return {
        'previous_state': previous_state_list,
        'state_change_date': state_change_date_list,
        'days_in_state': days_in_state_list,
        'previous_mute_state': previous_mute_state_list,
        'mute_state_change_date': mute_state_change_date_list,
        'days_in_mute_state': days_in_mute_state_list,
        'previous_state_filtered': previous_state_filtered_list,
        'state_change_date_filtered': state_change_date_filtered_list,
        'days_in_state_filtered': days_in_state_filtered_list,
        'state_filtered': state_filtered_list,
    }


def determine_state(row):
    history_class = row['history_class']
    is_muted = row['is_muted']

    if is_muted == 1:
        if 'mute' in history_class or 'failure' in history_class:
            return 'Muted Flaky'
        elif 'pass' in history_class and not 'failure' in history_class and not 'mute' in history_class :
            return 'Muted Stable'
        elif 'skipped' in history_class:
            return 'Skipped'
        else:
            return 'no_runs'
    else:
        if 'failure' in history_class and 'mute' not in history_class:
            return 'Flaky'
        elif 'mute' in history_class:
            return 'Muted'
        elif 'pass' in history_class:
            return 'Passed'
        elif 'skipped' in history_class:
            return 'Skipped'
        else:
            return 'no_runs'


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days-window', default=1, type=int, help='how many days back we collecting history')
    parser.add_argument(
        '--build_type',
        choices=['relwithdebinfo', 'release-asan', 'release-tsan', 'release-msan'],
        default='relwithdebinfo',
        type=str,
        help='build type',
    )
    parser.add_argument('--branch', default='main', type=str, help='branch')

    parser.add_argument(
        '--concurent',
        dest='concurrent_mode',
        action='store_true',
        default=True,
        help='Set concurrent mode to true (default).',
    )

    parser.add_argument(
        '--no-concurrent', dest='concurrent_mode', action='store_false', help='Set concurrent mode to false.'
    )

    args, unknown = parser.parse_known_args()
    history_for_n_day = args.days_window
    build_type = args.build_type
    branch = args.branch
    concurrent_mode = args.concurrent_mode

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1
    else:
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=False)
        table_client = ydb.TableClient(driver, tc_settings)
        base_date = datetime.datetime(1970, 1, 1)
        default_start_date = datetime.date(2025, 2, 1)
        today = datetime.date.today()
        table_path = f'test_results/analytics/tests_monitor'

        # Get last existing day
        print("Geting date of last collected monitor data")
        query_last_exist_day = f"""
            SELECT MAX(date_window) AS last_exist_day
            FROM `{table_path}`
            WHERE build_type = '{build_type}'
            AND branch = '{branch}'
        """
        query = ydb.ScanQuery(query_last_exist_day, {})
        it = driver.table_client.scan_query(query)
        last_exist_day = None
        while True:
            try:
                result = next(it)
                last_exist_day = result.result_set.rows[0][
                    'last_exist_day'
                ]  # exclude last day, we want recalculate last day
                break
            except StopIteration:
                break
            except Exception as e:
                print(f"Error during fetching last existing day: {e}")

        last_exist_df = None
        last_day_data = None

        # If no data exists, set last_exist_day to a default start date
        if last_exist_day is None:
            last_exist_day = default_start_date
            last_exist_day_str = last_exist_day.strftime('%Y-%m-%d')
            date_list = [today - datetime.timedelta(days=x) for x in range((today - last_exist_day).days + 1)]
            print(f"Monitor data do not exist - init new monitor collecting from default date {last_exist_day_str}")
        else:
            # Get data from tests_monitor for last existing day
            last_exist_day = (base_date + datetime.timedelta(days=last_exist_day)).date()
            if last_exist_day == today:  # to recalculate data for today
                last_exist_day = last_exist_day - datetime.timedelta(days=1)
            last_exist_day_str = last_exist_day.strftime('%Y-%m-%d')
            print(f"Monitor data exist - geting data for date {last_exist_day_str}")
            date_list = [today - datetime.timedelta(days=x) for x in range((today - last_exist_day).days)]
            query_last_exist_data = f"""
                SELECT *
                FROM `{table_path}`
                WHERE build_type = '{build_type}'
                AND branch = '{branch}'
                AND date_window = Date('{last_exist_day_str}')
            """
            query = ydb.ScanQuery(query_last_exist_data, {})
            it = driver.table_client.scan_query(query)
            last_exist_data = []

            while True:
                try:
                    result = next(it)
                    # Convert each row to a dictionary with consistent keys
                    for row in result.result_set.rows:
                        row_dict = {
                            'test_name': row['test_name'],
                            'suite_folder': row['suite_folder'],
                            'full_name': row['full_name'],
                            'date_window': base_date + datetime.timedelta(days=row['date_window']),
                            'build_type': row['build_type'],
                            'branch': row['branch'],
                            'days_ago_window': row['days_ago_window'],
                            'history': row['history'],
                            'history_class': row['history_class'],
                            'pass_count': row['pass_count'],
                            'mute_count': row['mute_count'],
                            'fail_count': row['fail_count'],
                            'skip_count': row['skip_count'],
                            'success_rate': row['success_rate'],
                            'summary': row['summary'],
                            'owners': row['owner'],
                            'is_muted': row['is_muted'],
                            'is_test_chunk': row['is_test_chunk'],
                            'state': row['state'],
                            'previous_state': row['previous_state'],
                            'state_change_date': base_date + datetime.timedelta(days=row['state_change_date']),
                            'days_in_state': row['days_in_state'],
                            'previous_mute_state': row['previous_mute_state'],
                            'mute_state_change_date': base_date + datetime.timedelta(days=row['mute_state_change_date']),
                            'days_in_mute_state': row['days_in_mute_state'],
                            'previous_state_filtered': row['previous_state_filtered'],
                            'state_change_date_filtered': base_date
                            + datetime.timedelta(days=row['state_change_date_filtered']),
                            'days_in_state_filtered': row['days_in_state_filtered'],
                            'state_filtered': row['state_filtered'],
                        }
                        last_exist_data.append(row_dict)
                except StopIteration:
                    break

            last_exist_df = pd.DataFrame(last_exist_data)

        # Get data from flaky_tests_window table for dates after last existing day
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

        print(f'Getting aggregated history in window {history_for_n_day} days')
        for date in sorted(date_list):
            # Query for data from flaky_tests_window with date_window >= last_existing_day
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
                    owners_t.test_name IS NOT NULL OR fallback_t.test_name IS NOT NULL;
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
            print(f'Captured raw data for {date} duration: {end_time - start_time}')
            start_time = time.time()

            # Check if new data was found
            if results:
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

            else:
                print(
                    f"Warning: No data found in flaky_tests_window for date {date} build_type='{build_type}', branch='{branch}'"
                )

        start_time = time.time()
        df = pd.DataFrame(data)
        # **Concatenate DataFrames**
        if last_exist_df is not None and last_exist_df.shape[0] > 0:
            last_day_data = last_exist_df[
                [
                    'full_name',
                    'days_in_state',
                    'state',
                    'previous_state',
                    'state_change_date',
                    'is_muted',
                    'days_in_mute_state',
                    'previous_mute_state',
                    'mute_state_change_date',
                    'days_in_state_filtered',
                    'state_change_date_filtered',
                    'previous_state_filtered',
                    'state_filtered',
                ]
            ]

        end_time = time.time()
        print(f'Dataframe inited: {end_time - start_time}')
        tart_time = time.time()

        df = df.sort_values(by=['full_name', 'date_window'])

        end_time = time.time()
        print(f'Dataframe sorted: {end_time - start_time}')
        start_time = time.time()

        df['success_rate'] = df.apply(calculate_success_rate, axis=1).astype(int)
        df['summary'] = df.apply(calculate_summary, axis=1)
        df['owner'] = df['owners'].apply(compute_owner)
        df['is_test_chunk'] = df['full_name'].str.contains(']? chunk|sole chunk|chunk chunk|chunk\+chunk', regex=True).astype(int)
        df['is_muted'] = df['is_muted'].fillna(0).astype(int)
        df['success_rate'].astype(int)
        df['state'] = df.apply(determine_state, axis=1)

        end_time = time.time()
        print(f'Computed base params: {end_time - start_time}')
        start_time = time.time()

        if concurrent_mode:
            with Pool(processes=cpu_count()) as pool:
                results = pool.starmap(
                    process_test_group,
                    [(name, group, last_day_data, default_start_date) for name, group in df.groupby('full_name')],
                )
                end_time = time.time()
                print(
                    f'Computed days_in_state, state_change_date, previous_state and other params: {end_time - start_time}'
                )
                start_time = time.time()
                # Apply results to the DataFrame
                for i, (name, group) in enumerate(df.groupby('full_name')):
                    df.loc[group.index, 'previous_state'] = results[i]['previous_state']
                    df.loc[group.index, 'state_change_date'] = results[i]['state_change_date']
                    df.loc[group.index, 'days_in_state'] = results[i]['days_in_state']
                    df.loc[group.index, 'previous_mute_state'] = results[i]['previous_mute_state']
                    df.loc[group.index, 'mute_state_change_date'] = results[i]['mute_state_change_date']
                    df.loc[group.index, 'days_in_mute_state'] = results[i]['days_in_mute_state']
                    df.loc[group.index, 'previous_state_filtered'] = results[i]['previous_state_filtered']
                    df.loc[group.index, 'state_change_date_filtered'] = results[i]['state_change_date_filtered']
                    df.loc[group.index, 'days_in_state_filtered'] = results[i]['days_in_state_filtered']
                    df.loc[group.index, 'state_filtered'] = results[i]['state_filtered']

        else:
            previous_state_list = []
            state_change_date_list = []
            days_in_state_list = []
            previous_mute_state_list = []
            mute_state_change_date_list = []
            days_in_mute_state_list = []
            previous_state_filtered_list = []
            state_change_date_filtered_list = []
            days_in_state_filtered_list = []
            state_filtered_list = []
            for name, group in df.groupby('full_name'):
                result = process_test_group(name, group, last_day_data, default_start_date)
                previous_state_list = previous_state_list + result['previous_state']
                state_change_date_list = state_change_date_list + result['state_change_date']
                days_in_state_list = days_in_state_list + result['days_in_state']
                previous_mute_state_list = previous_mute_state_list + result['previous_mute_state']
                mute_state_change_date_list = mute_state_change_date_list + result['mute_state_change_date']
                days_in_mute_state_list = days_in_mute_state_list + result['days_in_mute_state']
                previous_state_filtered_list = previous_state_filtered_list + result['previous_state_filtered']
                state_change_date_filtered_list = state_change_date_filtered_list + result['state_change_date_filtered']
                days_in_state_filtered_list = days_in_state_filtered_list + result['days_in_state_filtered']
                state_filtered_list = state_filtered_list + result['state_filtered']

            end_time = time.time()
            print(
                f'Computed days_in_state, state_change_date, previous_state and other params: {end_time - start_time}'
            )
            start_time = time.time()
            # Apply results to the DataFrame
            df['previous_state'] = previous_state_list
            df['state_change_date'] = state_change_date_list
            df['days_in_state'] = days_in_state_list
            df['previous_mute_state'] = previous_mute_state_list
            df['mute_state_change_date'] = mute_state_change_date_list
            df['days_in_mute_state'] = days_in_mute_state_list
            df['previous_state_filtered'] = previous_state_filtered_list
            df['state_change_date_filtered'] = state_change_date_filtered_list
            df['days_in_state_filtered'] = days_in_state_filtered_list
            df['state_filtered'] = state_filtered_list

        end_time = time.time()
        print(f'Saving computed result in dataframe: {end_time - start_time}')
        start_time = time.time()

        df['date_window'] = df['date_window'].dt.date
        df['state_change_date'] = df['state_change_date'].dt.date
        df['days_in_state'] = df['days_in_state'].astype(int)
        df['previous_mute_state'] = df['previous_mute_state'].astype(int)
        df['mute_state_change_date'] = df['mute_state_change_date'].dt.date
        df['days_in_mute_state'] = df['days_in_mute_state'].astype(int)
        df['state_change_date_filtered'] = df['state_change_date_filtered'].dt.date
        df['days_in_state_filtered'] = df['days_in_state_filtered'].astype(int)

        end_time = time.time()
        print(f'Converting types of columns: {end_time - start_time}')
        start_time = time.time()

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
                'previous_mute_state',
                'mute_state_change_date',
                'days_in_mute_state',
                'previous_state_filtered',
                'state_change_date_filtered',
                'days_in_state_filtered',
                'state_filtered',
                'success_rate',
            ]
        ]

        end_time = time.time()
        print(f'Dataframe prepared {end_time - start_time}')
        print(f'Data collected, {len(result)} rows')
        start_time = time.time()
        prepared_for_update_rows = result.to_dict('records')
        end_time = time.time()
        print(f'Data converted to dict for upsert: {end_time - start_time}')

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

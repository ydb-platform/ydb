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
                `date_window` Date NOT NULL,
                `build_type` Utf8 NOT NULL,
                `branch` Utf8 NOT NULL,
                `first_run` Timestamp,
                `last_run` Timestamp ,
                `owners` Utf8 ,
                `days_ago_window` Uint64 NOT NULL,
                `history` String,
                `history_class` String,
                `pass_count` Uint64,
                `mute_count` Uint64,
                `fail_count` Uint64,
                `skip_count` Uint64,
                PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,date_window, build_type, branch)
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
        .add_column("days_ago_window", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("history", ydb.OptionalType(ydb.PrimitiveType.String))
        .add_column("history_class", ydb.OptionalType(ydb.PrimitiveType.String))
        .add_column("pass_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("mute_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("fail_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("skip_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("state", ydb.OptionalType(ydb.PrimitiveType.Utf8))        
        .add_column("days_in_status", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("success_rate", ydb.OptionalType(ydb.PrimitiveType.Uint64))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--days-window', default=1, type=int, help='how many days back we collecting history')
    parser.add_argument('--build_type',choices=['relwithdebinfo', 'release-asan'], default='relwithdebinfo', type=str, help='build : relwithdebinfo or release-asan')
    parser.add_argument('--branch', default='main',choices=['main'], type=str, help='branch')

    args, unknown = parser.parse_known_args()
    history_for_n_day = args.days_window
    build_type = args.build_type
    branch = args.branch
    
    print(f'Getting history in window {history_for_n_day} days')
    

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
        
        table_path = f'test_results/analytics/flaky_tests_monitor_window_{history_for_n_day}_days'

        today = datetime.date.today()
 
        query_get_history = f"""
    $status_changes  = (
        SELECT test_name, 
            suite_folder, 
            full_name, 
            date_window, 
            build_type, 
            branch, 
            owners, 
            days_ago_window, 
            history, 
            history_class, 
            pass_count, 
            mute_count, 
            fail_count, 
            skip_count,
            state_detailed,
            LAG(state_detailed) OVER (PARTITION BY build_type,full_name,branch ORDER BY date_window) AS previous_status,

        from(
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
                hist.test_name AS test_name,
                CASE
                    WHEN (String::Contains(hist.history_class, 'failure') AND NOT String::Contains(hist.history_class, 'mute')) THEN 'Flaky'
                    WHEN String::Contains(hist.history_class, 'pass') THEN 'Passed'
                    WHEN (String::Contains(hist.history_class, 'skipped') OR hist.history_class IS NULL OR hist.history_class = '') THEN 'Skipped'
                    ELSE hist.history_class
                END AS state_detailed_basic,
                CASE
                    WHEN owners_t.is_muted = 1 THEN
                        CASE
                            WHEN String::Contains(hist.history_class, 'mute') THEN 'Muted Flaky'
                            WHEN String::Contains(hist.history_class, 'pass') THEN 'Muted Stable'
                            WHEN (String::Contains(hist.history_class, 'skipped') OR hist.history_class IS NULL OR hist.history_class = '') THEN 'Skipped'
                            ELSE hist.history_class
                        END
                    ELSE
                        CASE
                            WHEN (String::Contains(hist.history_class, 'failure') AND NOT String::Contains(hist.history_class, 'mute')) THEN 'Flaky'
                            WHEN String::Contains(hist.history_class, 'mute') THEN 'Muted'
                            WHEN (String::Contains(hist.history_class, 'skipped') OR hist.history_class IS NULL OR hist.history_class = '') THEN 'Skipped'
                            WHEN String::Contains(hist.history_class, 'pass') THEN 'Passed'
                            ELSE hist.history_class
                        END
                END AS state_detailed
            FROM (
                SELECT * FROM
                `test_results/analytics/flaky_tests_window_{history_for_n_day}_days` 
                WHERE 
                --date_window >= Date('{today}') - 30*Interval("P1D")
                build_type = '{build_type}'
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
        )
    );
    $state_streaks = (
        SELECT
            test_name,
            suite_folder,
            full_name,
            date_window,
            build_type,
            branch,
            owners,
            days_ago_window,
            history,
            history_class,
            pass_count,
            mute_count,
            fail_count,
            skip_count,
            state_detailed,
            SUM(CASE WHEN state_detailed = previous_status THEN 0 ELSE 1 END) OVER (PARTITION BY full_name,branch,build_type  ORDER BY date_window ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS streak_id
        FROM
            $status_changes
    );
    SELECT
        test_name,
        suite_folder,
        full_name,
        date_window,
        build_type,
        branch,
        owners,
        days_ago_window,
        history,
        history_class,
        pass_count,
        mute_count,
        fail_count,
        skip_count,
        state_detailed as state,
        ROW_NUMBER() OVER (PARTITION BY build_type,full_name, state_detailed, streak_id ORDER BY date_window)  AS days_in_status
    FROM
        $state_streaks
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

        print(f'monitor data data captured, {len(results)} rows')
        for row in results: 
            prepared_for_update_rows.append({
                'suite_folder': row['suite_folder'],
                'test_name': row['test_name'],
                'full_name': row['full_name'],
                'date_window': row['date_window'],
                'days_ago_window': row['days_ago_window'],
                'build_type': row['build_type'],
                'branch': row['branch'],
                'history': row['history'],
                'history_class': row['history_class'],
                'pass_count': row['pass_count'],
                'mute_count': row['mute_count'],
                'fail_count': row['fail_count'],
                'skip_count': row['skip_count'],
                'state': row['state'],
                'days_in_status': row['days_in_status'],
                'success_rate': 0.0 if row['pass_count'] + row['mute_count'] + row['fail_count'] == 0  else  row['pass_count']/(row['pass_count'] + row['mute_count'] + row['fail_count'])*100
            })
        print(f'upserting monitor data')
        with ydb.SessionPool(driver) as pool:

            create_tables(pool, table_path)
            full_path = posixpath.join(DATABASE_PATH, table_path)
            bulk_upsert(driver.table_client, full_path,
                        prepared_for_update_rows)

        print('monitor data updated')


if __name__ == "__main__":
    main()
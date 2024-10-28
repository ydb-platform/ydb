#!/usr/bin/env python3
import argparse
import configparser
import datetime
import os
import posixpath
import re
import ydb
from get_diff_lines_of_file import get_diff_lines_of_file
from mute_utils import pattern_to_re
from transform_ya_junit import YaMuteCheck

dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
repo_path = f"{dir}/../../../"
muted_ya_path = '.github/config/muted_ya.txt'
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def get_all_tests(job_id=None, branch=None):
    print(f'Getting all tests')

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)

        # settings, paths, consts
        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
        table_client = ydb.TableClient(driver, tc_settings)

        # geting last date from history
        today = datetime.date.today().strftime('%Y-%m-%d')
        if job_id and branch:  # extend all tests from main by new tests from pr

            tests = f"""
            SELECT * FROM (
                SELECT 
                    suite_folder,
                    test_name,
                    full_name
                    from `test_results/analytics/testowners`
                WHERE  
                    run_timestamp_last >= Date('{today}') - 6*Interval("P1D") 
                    and run_timestamp_last <= Date('{today}') + Interval("P1D")
                UNION
                SELECT DISTINCT
                    suite_folder,
                    test_name,
                    suite_folder || '/' || test_name as full_name
                FROM `test_results/test_runs_column`
                WHERE
                    job_id = {job_id} 
                    and branch = '{branch}'
            )
            """
        else:  # only all tests from main
            tests = f"""
            SELECT 
                suite_folder,
                test_name,
                full_name,
                owners,
                run_timestamp_last,
                Date('{today}') as date
            FROM `test_results/analytics/testowners`
            WHERE  
                run_timestamp_last >= Date('{today}') - 6*Interval("P1D") 
                and run_timestamp_last <= Date('{today}') + Interval("P1D")
            """
        query = ydb.ScanQuery(tests, {})
        it = table_client.scan_query(query)
        results = []
        while True:
            try:
                result = next(it)
                results = results + result.result_set.rows
            except StopIteration:
                break
        return results


def create_tables(pool, table_path):
    print(f"> create table if not exists:'{table_path}'")

    def callee(session):
        session.execute_scheme(
            f"""
            CREATE table IF NOT EXISTS `{table_path}` (
                `date` Date NOT NULL,
                `test_name` Utf8 NOT NULL,
                `suite_folder` Utf8 NOT NULL,
                `full_name` Utf8 NOT NULL,
                `run_timestamp_last` Timestamp NOT NULL,
                `owners` Utf8,
                `branch` Utf8 NOT NULL,
                `is_muted` Uint32 ,
                PRIMARY KEY (`date`,branch, `test_name`, `suite_folder`, `full_name`)
            )
                PARTITION BY HASH(date,branch)
                WITH (STORE = COLUMN)
            """
        )

    return pool.retry_operation_sync(callee)


def bulk_upsert(table_client, table_path, rows):
    print(f"> bulk upsert: {table_path}")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("date", ydb.OptionalType(ydb.PrimitiveType.Date))
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("run_timestamp_last", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("is_muted", ydb.OptionalType(ydb.PrimitiveType.Uint32))
    )
    table_client.bulk_upsert(table_path, rows, column_types)


def write_to_file(text, file):
    os.makedirs(os.path.dirname(file), exist_ok=True)
    with open(file, 'w') as f:
        f.writelines(text)


def upload_muted_tests(tests):
    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)

        # settings, paths, consts
        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
        table_client = ydb.TableClient(driver, tc_settings)

        table_path = f'test_results/all_tests_with_owner_and_mute'

        with ydb.SessionPool(driver) as pool:
            create_tables(pool, table_path)
            full_path = posixpath.join(DATABASE_PATH, table_path)
            bulk_upsert(driver.table_client, full_path, tests)


def to_str(data):
    if isinstance(data, str):
        return data
    elif isinstance(data, bytes):
        return data.decode('utf-8')
    else:
        raise ValueError("Unsupported type")


def mute_applier(args):
    output_path = args.output_folder

    all_tests_file = os.path.join(output_path, '1_all_tests.txt')
    all_muted_tests_file = os.path.join(output_path, '1_all_muted_tests.txt')

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    # all muted
    mute_check = YaMuteCheck()
    mute_check.load(muted_ya_path)

    if args.mode == 'upload_muted_tests':
        all_tests = get_all_tests(branch=args.branch)
        for test in all_tests:
            testsuite = to_str(test['suite_folder'])
            testcase = to_str(test['test_name'])
            test['branch'] = 'main'
            test['is_muted'] = int(mute_check(testsuite, testcase))

        upload_muted_tests(all_tests)

    elif args.mode == 'get_mute_diff':
        all_tests = get_all_tests(job_id=args.job_id, branch=args.branch)
        all_tests.sort(key=lambda test: test['full_name'])
        muted_tests = []
        all_tests_names_and_suites = []
        for test in all_tests:
            testsuite = to_str(test['suite_folder'])
            testcase = to_str(test['test_name'])
            all_tests_names_and_suites.append(testsuite + ' ' + testcase + '\n')
            if mute_check(testsuite, testcase):
                muted_tests.append(testsuite + ' ' + testcase + '\n')

        write_to_file(all_tests_names_and_suites, all_tests_file)
        write_to_file(muted_tests, all_muted_tests_file)

        added_mute_lines_file = os.path.join(output_path, '2_added_mute_lines.txt')
        new_muted_tests_file = os.path.join(output_path, '2_new_muted_tests.txt')
        removed_mute_lines_file = os.path.join(output_path, '3_removed_mute_lines.txt')
        unmuted_tests_file = os.path.join(output_path, '3_unmuted_tests.txt')

        added_lines, removed_lines = get_diff_lines_of_file(args.base_sha, args.head_sha, muted_ya_path)

        # checking added lines
        write_to_file('\n'.join(added_lines), added_mute_lines_file)
        mute_check = YaMuteCheck()
        mute_check.load(added_mute_lines_file)
        added_muted_tests = []
        print("New muted tests captured")
        for test in all_tests:
            testsuite = to_str(test['suite_folder'])
            testcase = to_str(test['test_name'])
            if mute_check(testsuite, testcase):
                added_muted_tests.append(testsuite + ' ' + testcase + '\n')

        # checking removed lines
        write_to_file('\n'.join(removed_lines), removed_mute_lines_file)
        mute_check = YaMuteCheck()
        mute_check.load(removed_mute_lines_file)
        removed_muted_tests = []
        print("Unmuted tests captured")
        for test in all_tests:
            testsuite = to_str(test['suite_folder'])
            testcase = to_str(test['test_name'])
            if mute_check(testsuite, testcase):
                removed_muted_tests.append(testsuite + ' ' + testcase + '\n')

        # geting only uniq items in both lists because not uniq items= this tests was muted before
        added_set = set(added_muted_tests)
        removed_set = set(removed_muted_tests)
        added_unique = added_set - removed_set
        removed_unique = removed_set - added_set
        added_muted_tests = list(sorted(added_unique))
        removed_muted_tests = list(sorted(removed_unique))

        write_to_file(added_muted_tests, new_muted_tests_file)
        write_to_file(removed_muted_tests, unmuted_tests_file)

        print(f"All tests have been written to {all_tests_file}.")
        print(f"All mutes tests have been written to {all_muted_tests_file}.")
        print(f"Added lines have been written to {added_mute_lines_file}.")
        print(f"New muted tests have been written to {new_muted_tests_file}.")
        print(f"Removed lines have been written to {removed_mute_lines_file}.")
        print(f"Unmuted tests have been written to {unmuted_tests_file}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate diff files for mute_ya.txt")

    parser.add_argument(
        '--output_folder',
        type=str,
        default=repo_path + '.github/config/mute_info/',
        help=f'The folder to output results. Default is the value of repo_path = {repo_path}.github/config/mute_info/.',
    )

    subparsers = parser.add_subparsers(dest='mode', help="Mode to perform")

    upload_muted_tests_parser = subparsers.add_parser(
        'upload_muted_tests', help='apply mute rules for all tests in main and upload to database'
    )
    upload_muted_tests_parser.add_argument(
        '--branch', required=True, default='main', help='branch for getting all tests'
    )

    get_mute_details_parser = subparsers.add_parser(
        'get_mute_diff',
        help='apply mute rules for all tests in main extended by new tests from pr and collect new muted and unmuted',
    )
    get_mute_details_parser.add_argument('--base_sha', required=True, help='Base sha of PR')
    get_mute_details_parser.add_argument('--head_sha', required=True, help='Head sha of PR')
    get_mute_details_parser.add_argument(
        '--branch',
        required=True,
        help='pass branch to extend list of tests by new tests from this pr (by job-id of PR-check and branch)',
    )
    get_mute_details_parser.add_argument(
        '--job-id',
        required=True,
        help='pass job-id to extend list of tests by new tests from this pr (by job-id of PR-check and branch)',
    )
    args = parser.parse_args()

    mute_applier(args)
#!/usr/bin/env python3
import argparse
import configparser
import datetime
import os
import posixpath
import re
import time
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


def get_all_tests(job_id=None, branch=None, build_type=None):
    print(f'🔍 Getting all tests with parameters:')
    print(f'   - job_id: {job_id}')
    print(f'   - branch: {branch}')
    print(f'   - build_type: {build_type}')

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
        print(f'📅 Using date: {today}')
        
        if job_id and branch:  # extend all tests from main by new tests from pr
            print(f'🔄 Mode: Extend all tests from main by new tests from PR')
            print(f'   - job_id: {job_id}')
            print(f'   - branch: {branch}')

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
        else:  # get all tests with run_timestamp_last from test_runs_column for specific branch
            print(f'🎯 Mode: Get all tests with run_timestamp_last from test_runs_column')
            print(f'   - branch: {branch}')
            print(f'   - build_type: {build_type}')

            tests_query = f"""
            SELECT 
                t.suite_folder as suite_folder,
                t.test_name as test_name,
                t.full_name as full_name,
                t.owners as owners,
                trc.run_timestamp_last as run_timestamp_last,
                Date('{today}') as date
            FROM `test_results/analytics/testowners` t
            INNER JOIN (
                SELECT 
                    suite_folder,
                    test_name,
                    MAX(run_timestamp) as run_timestamp_last
                FROM `test_results/test_runs_column`
                WHERE branch = '{branch}'
                AND build_type = '{build_type}'
                GROUP BY suite_folder, test_name
            ) trc ON t.suite_folder = trc.suite_folder AND t.test_name = trc.test_name
            """
        
        print(f'📝 Executing SQL query:')
        print(tests_query)
        
        query = ydb.ScanQuery(tests_query, {})
        print(f'⏱️  Starting query execution...')
        start_time = time.time()
        it = table_client.scan_query(query)
        
        results = []
        batch_count = 0
        while True:
            try:
                result = next(it)
                batch_count += 1
                batch_size = len(result.result_set.rows) if result.result_set.rows else 0
                results = results + result.result_set.rows
                print(f'📦 Batch {batch_count}: {batch_size} rows')
            except StopIteration:
                break
        
        end_time = time.time()
        print(f'✅ Query completed in {end_time - start_time:.2f} seconds')
        print(f'📊 Total results: {len(results)} tests')
        
        
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
    print(f'📤 Starting bulk upsert to: {table_path}')
    print(f'   - Rows to upsert: {len(rows)}')
    
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
    
    print(f'🔧 Column types configured')
    print(f'⏱️  Executing bulk upsert...')
    
    start_time = time.time()
    table_client.bulk_upsert(table_path, rows, column_types)
    end_time = time.time()
    
    print(f'✅ Bulk upsert completed in {end_time - start_time:.2f} seconds')
    print(f'📊 Successfully upserted {len(rows)} rows')


def write_to_file(text, file):
    os.makedirs(os.path.dirname(file), exist_ok=True)
    with open(file, 'w') as f:
        f.writelines(text)


def upload_muted_tests(tests):
    print(f'💾 Starting upload_muted_tests with {len(tests)} tests')
    
    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        print(f'📡 Connecting to YDB for upload: {DATABASE_ENDPOINT}/{DATABASE_PATH}')
        driver.wait(timeout=10, fail_fast=True)
        print(f'✅ YDB connection established for upload')

        # settings, paths, consts
        tc_settings = ydb.TableClientSettings().with_native_date_in_result_sets(enabled=True)
        table_client = ydb.TableClient(driver, tc_settings)

        table_path = f'test_results/all_tests_with_owner_and_mute'
        print(f'📋 Target table: {table_path}')

        with ydb.SessionPool(driver) as pool:
            print(f'🏗️  Creating table if not exists...')
            create_tables(pool, table_path)
            
            full_path = posixpath.join(DATABASE_PATH, table_path)
            print(f'📤 Starting bulk upsert to: {full_path}')
            print(f'   - Records to upload: {len(tests)}')
            
            start_time = time.time()
            bulk_upsert(driver.table_client, full_path, tests)
            end_time = time.time()
            
            print(f'✅ Bulk upsert completed in {end_time - start_time:.2f} seconds')
            print(f'📊 Successfully uploaded {len(tests)} test records')


def to_str(data):
    if isinstance(data, str):
        return data
    elif isinstance(data, bytes):
        return data.decode('utf-8')
    else:
        raise ValueError("Unsupported type")


def mute_applier(args):
    print(f'🚀 Starting mute_applier with mode: {args.mode}')
    print(f'   - branch: {args.branch}')
    print(f'   - build_type: {args.build_type}')
    print(f'   - output_folder: {args.output_folder}')
    
    output_path = args.output_folder

    all_tests_file = os.path.join(output_path, '1_all_tests.txt')
    all_muted_tests_file = os.path.join(output_path, '1_all_muted_tests.txt')

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("❌ Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1
    else:
        print("✅ YDB credentials found, setting up environment")
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    # all muted
    print(f'📋 Loading mute rules from: {muted_ya_path}')
    mute_check = YaMuteCheck()
    mute_check.load(muted_ya_path)
    print(f'✅ Mute rules loaded successfully')

    if args.mode == 'upload_muted_tests':
        print(f'📊 Mode: upload_muted_tests')
        print(f'   - branch: {args.branch}')
        print(f'   - build_type: {args.build_type}')
        
        all_tests = get_all_tests(branch=args.branch, build_type=args.build_type)
        print(f'📝 Processing {len(all_tests)} tests...')
        
        muted_count = 0
        for i, test in enumerate(all_tests):
            testsuite = to_str(test['suite_folder'])
            testcase = to_str(test['test_name'])
            test['branch'] = args.branch
            is_muted = int(mute_check(testsuite, testcase))
            test['is_muted'] = is_muted
            
            if is_muted:
                muted_count += 1
            
        
        print(f'📊 Processing complete:')
        print(f'   - Total tests: {len(all_tests)}')
        print(f'   - Muted tests: {muted_count}')
        print(f'   - Unmuted tests: {len(all_tests) - muted_count}')
        
        print(f'💾 Uploading to database...')
        upload_muted_tests(all_tests)
        print(f'✅ Upload completed successfully')

    elif args.mode == 'get_mute_diff':
        all_tests = get_all_tests(job_id=args.job_id, branch=args.branch, build_type=args.build_type)
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
    print(f'🚀 Starting get_muted_tests.py script')
    print(f'📅 Current time: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'📋 Muted YA file path: {muted_ya_path}')
    
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
    upload_muted_tests_parser.add_argument(
        '--build_type', required=True, help='build type for filtering tests'
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
    get_mute_details_parser.add_argument(
        '--build_type',
        required=True,
        help='build type for filtering tests',
    )
    args = parser.parse_args()
    
    print(f'📋 Parsed arguments:')
    print(f'   - mode: {args.mode}')
    print(f'   - branch: {getattr(args, "branch", "N/A")}')
    print(f'   - build_type: {getattr(args, "build_type", "N/A")}')
    print(f'   - output_folder: {args.output_folder}')
    if hasattr(args, 'job_id'):
        print(f'   - job_id: {args.job_id}')

    print(f'🎯 Starting mute_applier...')
    mute_applier(args)
    print(f'✅ get_muted_tests.py script completed successfully')
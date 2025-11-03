#!/usr/bin/env python3
import argparse
import datetime
import os
import posixpath
import re
import sys
import time
import ydb
from get_diff_lines_of_file import get_diff_lines_of_file
from mute_utils import pattern_to_re
from transform_ya_junit import YaMuteCheck

# Add analytics directory to path for ydb_wrapper import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
from ydb_wrapper import YDBWrapper

dir = os.path.dirname(__file__)
repo_path = f"{dir}/../../../"
muted_ya_path = '.github/config/muted_ya.txt'


def get_all_tests(job_id=None, branch=None, build_type=None):
    print(f'🔍 Getting all tests with parameters:')
    print(f'   - job_id: {job_id}')
    print(f'   - branch: {branch}')
    print(f'   - build_type: {build_type}')

    script_name = os.path.basename(__file__)
    
    # Initialize YDB wrapper with context manager for automatic cleanup
    with YDBWrapper(script_name=script_name) as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return []

        # geting last date from history
        today = datetime.date.today().strftime('%Y-%m-%d')
        print(f'📅 Using date: {today}')
        
        # Get table paths from config
        test_runs_table = ydb_wrapper.get_table_path("test_results")
        testowners_table = ydb_wrapper.get_table_path("testowners")
        
        if job_id and branch:  # extend all tests from main by new tests from pr
            print(f'🔄 Mode: Extend all tests from main by new tests from PR')
            print(f'   - job_id: {job_id}')
            print(f'   - branch: {branch}')

            tests_query = f"""
        SELECT * FROM (
            SELECT 
                suite_folder,
                test_name,
                full_name
                from `{testowners_table}`
            WHERE  
                run_timestamp_last >= Date('{today}') - 6*Interval("P1D") 
                and run_timestamp_last <= Date('{today}') + Interval("P1D")
            UNION
            SELECT DISTINCT
                suite_folder,
                test_name,
                suite_folder || '/' || test_name as full_name
            FROM `{test_runs_table}`
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
        FROM `{testowners_table}` t
        INNER JOIN (
            SELECT 
                suite_folder,
                test_name,
                MAX(run_timestamp) as run_timestamp_last
            FROM `{test_runs_table}`
            WHERE branch = '{branch}'
            AND build_type = '{build_type}'
            GROUP BY suite_folder, test_name
        ) trc ON t.suite_folder = trc.suite_folder AND t.test_name = trc.test_name
        """
        
        print(f'📝 Executing SQL query:')
        print(tests_query)
        
        print(f'⏱️  Starting query execution...')
        results = ydb_wrapper.execute_scan_query(tests_query)
        
        print(f'✅ Query completed successfully')
        print(f'📊 Total results: {len(results)} tests')
        
        return results


def create_tables(ydb_wrapper, table_path):
    print(f"> create table if not exists:'{table_path}'")

    create_sql = f"""
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
    
    ydb_wrapper.create_table(table_path, create_sql)


def write_to_file(text, file):
    os.makedirs(os.path.dirname(file), exist_ok=True)
    with open(file, 'w') as f:
        f.writelines(text)


def upload_muted_tests(tests):
    print(f'💾 Starting upload_muted_tests with {len(tests)} tests')
    
    script_name = os.path.basename(__file__)
    
    # Initialize YDB wrapper with context manager for automatic cleanup
    with YDBWrapper(script_name=script_name) as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return
        
        # Get table path from config
        table_path = ydb_wrapper.get_table_path("all_tests_with_owner_and_mute")
        print(f'📋 Target table: {table_path}')

        print(f'🏗️  Creating table if not exists...')
        create_tables(ydb_wrapper, table_path)
        
        full_path = f"{ydb_wrapper.database_path}/{table_path}"
        print(f'📤 Starting bulk upsert to: {full_path}')
        print(f'   - Records to upload: {len(tests)}')
        
        # Подготавливаем column_types
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
        
        # Используем bulk_upsert_batches
        ydb_wrapper.bulk_upsert_batches(full_path, tests, column_types, batch_size=1000)
        
        print(f'✅ Bulk upsert completed successfully')
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
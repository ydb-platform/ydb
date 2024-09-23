#!/usr/bin/env python3
import argparse
import configparser
import datetime
import os
import posixpath
import re
import requests
import sys
import ydb
from mute_utils import mute_target, pattern_to_re
from get_file_diff import extract_diff_lines

dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
repo_path = f"{dir}/../../../"
muted_ya_path = '.github/config/muted_ya.txt'
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

class YaMuteCheck:
    def __init__(self):
        self.regexps = set()
        self.regexps = []

    def load(self, fn):
        with open(fn, "r") as fp:
            for line in fp:
                line = line.strip()
                try:
                    testsuite, testcase = line.split(" ", maxsplit=1)
                except ValueError:
                    log_print(f"SKIP INVALID MUTE CONFIG LINE: {line!r}")
                    continue
                self.populate(testsuite, testcase)

    def populate(self, testsuite, testcase):
        check = []

        for p in (pattern_to_re(testsuite), pattern_to_re(testcase)):
            try:
                check.append(re.compile(p))
            except re.error:
                log_print(f"Unable to compile regex {p!r}")
                return

        self.regexps.append(tuple(check))

    def __call__(self, suite_name, test_name):
        for ps, pt in self.regexps:
            if ps.match(suite_name) and pt.match(test_name):
                return True
        return False

def get_all_tests(**kwargs):
    print(f'Getting all tests')
    path_to_save = kwargs.get('path', None)
    job_id = kwargs.get('job_id', None)
    branch = kwargs.get('branch', None)

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
            
        # geting last date from history
        today = datetime.date.today().strftime('%Y-%m-%d')
        if job_id and branch: # extend all tests from main by new tests from pr
            
            tests = f"""
            select * from (
                select 
                    suite_folder,
                    test_name,
                    full_name
                    from  `test_results/analytics/testowners`
                where  
                    run_timestamp_last >= Date('{today}') - 6*Interval("P1D") 
                    and run_timestamp_last <= Date('{today}') + Interval("P1D")
                union
                select distinct
                    suite_folder,
                    test_name,
                    suite_folder || '/' || test_name as full_name
                from  `test_results/test_runs_column`
                where
                    job_id = {job_id} 
                    and branch = '{branch}'
            )
            order by full_name
            """
        else: #only all tests from main
            tests = f"""
            select 
                suite_folder,
                test_name,
                full_name,
                owners,
                run_timestamp_last,
                Date('{today}') as date
            from  `test_results/analytics/testowners`
            where  
                run_timestamp_last >= Date('{today}') - 6*Interval("P1D") 
                and run_timestamp_last <= Date('{today}') + Interval("P1D")
            order by full_name
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
        lines=[]
        if path_to_save :
            for row in results: 
                lines.append(row['suite_folder'] + ' '+ row['test_name'] + '\n')
                
            write_to_file(lines, path_to_save)
        return results


def create_tables(pool,  table_path):
    print(f"> create table if not exists:'{table_path}'")

    def callee(session):
        session.execute_scheme(f"""
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
            """)

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
        
def read_file(path):
    line_list = []
    with open(path, "r") as fp:
        for line in fp:
            line = line.strip()
            try:
                testsuite, testcase = line.split(" ", maxsplit=1)
            except ValueError:
                log_print(f"SKIP INVALID MUTE CONFIG LINE: {line!r}")
                continue
            line_list.append((testsuite,testcase))
            
        return line_list
        

def download_file(url, local_filename):
    """
    Downloads a file from the given URL and saves it locally.

    :param url: URL of the file
    :param local_filename: Name to save the file locally
    :return: Name of the locally saved file
    """
    try:
        # Perform an HTTP GET request
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  # Raise an exception for HTTP errors
            # Open the file for writing in binary mode
            with open(local_filename, 'wb') as file:
                # Save the file in chunks (to avoid loading the entire file into memory)
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        print(f"Downloaded file to {local_filename}")
        return local_filename
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return None



def write_to_file(text, file):
    #os.remove(file)
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
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        
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
    
def get_mute_details(args):
    output_path = args.output_folder
    use_all_tests_s3_file = args.use_all_tests_s3_file


    all_tests_file = os.path.join(output_path, '3_all_tests.txt')
    all_muted_tests_file = os.path.join(output_path, '3_all_muted_tests.txt')
     
    if use_all_tests_s3_file:
        download_file(use_all_tests_s3_file, all_tests_file)
        all_tests = read_file(os.path.join(repo_path,all_tests_file))
    else:
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
        if args.job_id and args.branch:
            all_tests= get_all_tests(path=all_tests_file, job_id=args.job_id, branch=args.branch)
        else:
            all_tests= get_all_tests(path=all_tests_file,branch=args.branch)

    if all_tests == 1:
        return 1
    #all muted
    mute_check = YaMuteCheck()
    mute_check.load(muted_ya_path)
    muted_tests = []
    print("All muted tests captured")

    if args.mode == 'upload_muted_tests':
        for test in all_tests:
            testsuite = to_str(test[0])
            testcase = to_str(test[1])
            test['branch'] = 'main'
            test['is_muted'] = int(mute_check(testsuite, testcase))
                
        upload_muted_tests(all_tests)
    elif args.mode == 'get_mute_details':
        for test in all_tests:
            testsuite = to_str(test[0])
            testcase = to_str(test[1])
            if mute_check(testsuite, testcase):
                muted_tests.append(testsuite + ' ' + testcase + '\n')
   
        write_to_file(muted_tests,os.path.join(repo_path, all_muted_tests_file))
        
        added_lines_file = os.path.join(output_path, '1_added_mute_lines.txt')
        added_lines_file_muted = os.path.join(output_path, '1_new_muted_tests.txt')
        removed_lines_file = os.path.join(output_path, '2_removed_mute_lines.txt')
        removed_lines_file_muted = os.path.join(output_path, '2_unmuted_tests.txt')

        added_texts, removed_texts = extract_diff_lines(muted_ya_path)
        write_to_file('\n'.join(added_texts), added_lines_file)
        write_to_file('\n'.join(removed_texts), removed_lines_file)
        #checking added lines
        mute_check = YaMuteCheck()
        mute_check.load(added_lines_file)
        added_muted_line = read_file(added_lines_file)
        added_muted_tests=[]
        print("New muted tests captured")
        for test in all_tests:
            testsuite = test[0]
            testcase = test[1]
            if mute_check(testsuite, testcase):
                added_muted_tests.append(testsuite + ' '+ testcase + '\n')
                
        #checking removed lines
        mute_check = YaMuteCheck()
        mute_check.load(removed_lines_file)
    
        removed_muted_line = read_file(removed_lines_file)
        removed_muted_tests=[]
        print("Unmuted tests captured")
        for test in all_tests:
            testsuite = test[0]
            testcase = test[1]
            if mute_check(testsuite, testcase):
                removed_muted_tests.append(testsuite + ' '+ testcase + '\n')
        
        # geting only uniq items in both lists because not uniq items= this tests was muted before    
        added_set = set(added_muted_tests)
        removed_set = set(removed_muted_tests)
        added_unique = added_set - removed_set
        removed_unique = removed_set - added_set
        added_muted_tests = list(sorted(added_unique))
        removed_muted_tests = list(sorted(removed_unique))

        write_to_file(added_muted_tests, added_lines_file_muted)
        write_to_file(removed_muted_tests, removed_lines_file_muted)
        
        print(f"All tests have been written to {all_tests_file}.")
        print(f"All mutes tests have been written to {all_muted_tests_file}.")
        print(f"Added lines have been written to {added_lines_file}.")
        print(f"Removed lines have been written to {removed_lines_file}.")
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate diff files for mute_ya.txt")
    
    parser.add_argument('--use_all_tests_s3_file', 
        help='pass s3 file url to use it as all test base')
    parser.add_argument('--output_folder',
        type=str, 
        default=repo_path + '.github/config/mute_info/'  , 
        help=f'The folder to output results. Default is the value of repo_path = {repo_path}.github/config/mute_info/.')
    
    subparsers = parser.add_subparsers(dest='mode', help="Mode to perform")
    
    upload_muted_tests_parser = subparsers.add_parser('upload_muted_tests',
        help='apply mute rules for all tests in main and upload to database')
    upload_muted_tests_parser.add_argument('--branch', 
        required= True,
        default='main',
        help='branch for getting all tests')
    
    get_mute_details_parser = subparsers.add_parser('get_mute_details',
        help='apply mute rules for all tests in main extended by new tests from pr and collect new muted and unmuted')

    get_mute_details_parser.add_argument('--branch', 
        required= True,
        help='pass branch to extend list of tests by new tests from this pr (by job-id of PR-check and branch)')
    get_mute_details_parser.add_argument('--job-id',  
        required= True,
        help='pass job-id to extend list of tests by new tests from this pr (by job-id of PR-check and branch)')    
    args = parser.parse_args()

    get_mute_details(args)
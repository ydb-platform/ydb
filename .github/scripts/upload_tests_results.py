#!/usr/bin/env python3

import argparse
import configparser
import fnmatch
import os
import subprocess
import sys
import time
import ydb
import xml.etree.ElementTree as ET

from codeowners import CodeOwners
from concurrent.futures import ThreadPoolExecutor, as_completed


def parse_junit_xml(test_results_file, build_type, job_name, job_id, commit, branch, pull, run_timestamp):
    tree = ET.parse(test_results_file)
    root = tree.getroot()

    results = []
    for testsuite in root.findall(".//testsuite"):
        suite_folder = testsuite.get("name")

        for testcase in testsuite.findall("testcase"):
            name = testcase.get("name")            
            duration = testcase.get("time")

            status_description = ""
            status = "passed"
            if testcase.find("properties/property/[@name='mute']") is not None:
                status = "mute"
                status_description = testcase.find(
                    "properties/property/[@name='mute']"
                ).get("value")
            elif testcase.find("failure") is not None:
                status = "failure"
                status_description = testcase.find("failure").text
            elif testcase.find("error") is not None:
                status = "error"
                status_description = testcase.find("error").text
            elif testcase.find("skipped") is not None:
                status = "skipped"
                status_description = testcase.find("skipped").text

            results.append(
                {
                    "build_type": build_type,
                    "commit": commit,
                    "branch": branch,
                    "pull": pull,
                    "run_timestamp": run_timestamp,
                    "job_name": job_name,
                    "job_id": job_id,
                    "suite_folder": suite_folder,
                    "test_name": name,
                    "duration": float(duration),
                    "status": status,
                    "status_description": status_description.replace("\r\n", ";;").replace("\n", ";;").replace("\"", "'"),
                    "log": "" if testcase.find("properties/property/[@name='url:log']") is None else testcase.find("properties/property/[@name='url:log']").get('value'),
                    "logsdir": "" if testcase.find("properties/property/[@name='url:logsdir']") is None else testcase.find("properties/property/[@name='url:logsdir']").get('value'),
                    "stderr": "" if testcase.find("properties/property/[@name='url:stderr']") is None else testcase.find("properties/property/[@name='url:stderr']").get('value'),
                    "stdout": "" if testcase.find("properties/property/[@name='url:stdout']") is None else testcase.find("properties/property/[@name='url:stdout']").get('value'),

                }
            )
    return results


def get_codeowners_for_tests(codeowners_file_path, tests_data):
    with open(codeowners_file_path, 'r') as file:
        data = file.read()
        owners_odj = CodeOwners(data)

        tests_data_with_owners = []
        for test in tests_data:
            target_path = f'{test["suite_folder"]}'
            owners = owners_odj.of(target_path) 
            test["owners"] = joined_owners=";;".join([(":".join(x)) for x in owners])
            tests_data_with_owners.append(test)
        return tests_data_with_owners


def create_table(session, sql):

    try:
        session.execute_scheme(sql)
    except ydb.issues.AlreadyExists:
        pass
    except Exception as e:
        print(f"Error creating table: {e}, sql:\n{sql}", file=sys.stderr)
        raise e


def create_tests_table(session, table_path):
    # Creating of new table if not exist yet
    sql = f"""
    --!syntax_v1
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        build_type Utf8,
        job_name Utf8,
        job_id Uint64,
        commit Utf8,
        branch Utf8,
        pull Utf8,
        run_timestamp Timestamp,
        test_id Utf8,
        suite_folder Utf8,
        test_name Utf8,
        duration Double,
        status Utf8,
        status_description Utf8,
        owners Utf8,
        log Utf8,
        logsdir Utf8,
        stderr Utf8,
        stdout Utf8,
        PRIMARY KEY(test_id)
    );
    """
    create_table(session, sql)


def upload_results(pool, sql):
    with pool.checkout() as session:
        try:
            session.transaction().execute(sql, commit_tx=True)
        except Exception as e:
            print(f"Error executing query {e}, sql:\n{sql}", file=sys.stderr)
            raise e


def prepare_and_upload_tests(pool, path, results, batch_size):

    total_records = len(results)
    if total_records == 0:
        print("Error:Object stored test results is empty, check test results artifacts")
        return 1

    with ThreadPoolExecutor(max_workers=10) as executor:

        for start in range(0, total_records, batch_size):
            futures = []
            end = min(start + batch_size, total_records)
            batch = results[start:end]

            sql = f"""--!syntax_v1
            UPSERT INTO `{path}` 
            (build_type,
            job_name,
            job_id,
            commit,
            branch,
            pull, 
            run_timestamp, 
            test_id, 
            suite_folder,
            test_name, 
            duration, 
            status,
            status_description,
            log,
            logsdir,
            stderr,
            stdout,
            owners) VALUES"""
            values = []
            for index, result in enumerate(batch):

                values.append(
                    f"""
                ("{result['build_type']}", 
                "{result['job_name']}", 
                {result['job_id']}, 
                "{result['commit']}",
                "{result['branch']}",
                "{result['pull']}", 
                DateTime::FromSeconds({result['run_timestamp']}),
                "{result['pull']}_{result['run_timestamp']}_{start+index}",
                "{result['suite_folder']}",
                "{result['test_name']}",
                {result['duration']},
                "{result['status']}",
                "{result['status_description']}",
                "{result['log']}",
                "{result['logsdir']}",
                "{result['stderr']}",
                "{result['stdout']}",
                "{result['owners']}")
                """
                )
            sql += ", ".join(values) + ";"

            futures.append(executor.submit(upload_results, pool, sql))

        for future in as_completed(futures):
            future.result()  # Raise exception if occurred


def create_pool(endpoint, database):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.credentials_from_env_variables()
    )

    driver = ydb.Driver(driver_config)
    driver.wait(fail_fast=True, timeout=5)
    return ydb.SessionPool(driver)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--test-results-file', action='store', dest="test_results_file", required=True, help='XML with results of tests')
    parser.add_argument('--build-type', action='store', dest="build_type", required=True, help='build type')
    parser.add_argument('--commit', default='store', dest="commit", required=True, help='commit sha')
    parser.add_argument('--branch', default='store', dest="branch", required=True, help='branch name ')
    parser.add_argument('--pull', action='store', dest="pull",required=True, help='pull number')
    parser.add_argument('--run-timestamp', action='store', dest="run_timestamp",required=True, help='time of test run start')
    parser.add_argument('--job-name', action='store', dest="job_name",required=True, help='job name where launched')
    parser.add_argument('--job-id', action='store', dest="job_id",required=True, help='job id of workflow')

    args = parser.parse_args()

    test_results_file = args.test_results_file
    build_type = args.build_type
    commit = args.commit
    branch = args.branch
    pull = args.pull
    run_timestamp = args.run_timestamp
    job_name = args.job_name
    job_id = args.job_id

    path_in_database = "test_results"
    batch_size_default = 30
    dir = os.path.dirname(__file__)
    git_root = f"{dir}/../.."
    codeowners = f"{git_root}/.github/TESTOWNERS.ini"
    config = configparser.ConfigParser()
    config_file_path = f"{git_root}/.github/config/ydb_qa_db.ini"
    config.read(config_file_path)

    DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
    DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        return 0
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    test_table_name = f"{path_in_database}/test_runs_results"
    # Prepare connection and table
    pool = create_pool(DATABASE_ENDPOINT, DATABASE_PATH)
    with pool.checkout() as session:
        create_tests_table(session, test_table_name)

    # Parse and upload
    results = parse_junit_xml(
        test_results_file, build_type, job_name, job_id, commit, branch, pull, run_timestamp
    )
    result_with_owners = get_codeowners_for_tests(codeowners, results)
    prepare_and_upload_tests(
        pool, test_table_name, results, batch_size=batch_size_default
    )


if __name__ == "__main__":
    main()

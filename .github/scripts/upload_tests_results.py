#!/usr/bin/env python3

import argparse
import configparser
import datetime
import fnmatch
import os
import posixpath
import shlex
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
import ydb


from codeowners import CodeOwners
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal


def create_tables(pool,  table_path):
    print(f"> create table: {table_path}")

    def callee(session):
        session.execute_scheme(f"""
            CREATE table IF NOT EXISTS`{table_path}` (
                build_type Utf8 NOT NULL,
                job_name Utf8,
                job_id Uint64,
                commit Utf8,
                branch Utf8 NOT NULL,
                pull Utf8,
                run_timestamp Timestamp NOT NULL,
                test_id Utf8 NOT NULL,
                suite_folder Utf8 NOT NULL,
                test_name Utf8 NOT NULL,
                duration Double,
                status Utf8 NOT NULL,
                status_description Utf8,
                owners Utf8,
                log Utf8,
                logsdir Utf8,
                stderr Utf8,
                stdout Utf8,
                PRIMARY KEY (`test_name`, `suite_folder`,build_type, branch, status, run_timestamp)
            )
              PARTITION BY HASH(`suite_folder`,test_name, build_type, branch )
                WITH (STORE = COLUMN)
            """)

    return pool.retry_operation_sync(callee)


def bulk_upsert(table_client, table_path, rows):
    print(f"> bulk upsert: {table_path}")
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("commit", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("duration", ydb.OptionalType(ydb.PrimitiveType.Double))
        .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("job_id", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("job_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("log", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("logsdir", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("pull", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("run_timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("status", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("status_description", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("stderr", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("stdout", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))

    )
    table_client.bulk_upsert(table_path, rows, column_types)


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
                    "run_timestamp": int(run_timestamp)*1000000,
                    "job_name": job_name,
                    "job_id": int(job_id),
                    "suite_folder": suite_folder,
                    "test_name": name,
                    "duration": Decimal(duration),
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
            test["owners"] = joined_owners = ";;".join(
                [(":".join(x)) for x in owners])
            tests_data_with_owners.append(test)
        return tests_data_with_owners


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--test-results-file', action='store',
                        required=True, help='XML with results of tests')
    parser.add_argument('--build-type', action='store',
                        required=True, help='build type')
    parser.add_argument('--commit', default='store',
                        dest="commit", required=True, help='commit sha')
    parser.add_argument('--branch', default='store',
                        dest="branch", required=True, help='branch name ')
    parser.add_argument('--pull', action='store', dest="pull",
                        required=True, help='pull number')
    parser.add_argument('--run-timestamp', action='store',
                        dest="run_timestamp", required=True, help='time of test run start')
    parser.add_argument('--job-name', action='store', dest="job_name",
                        required=True, help='job name where launched')
    parser.add_argument('--job-id', action='store', dest="job_id",
                        required=True, help='job id of workflow')

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
    dir = os.path.dirname(__file__)
    git_root = f"{dir}/../.."
    codeowners = f"{git_root}/.github/TESTOWNERS"
    config = configparser.ConfigParser()
    config_file_path = f"{git_root}/.github/config/ydb_qa_db.ini"
    config.read(config_file_path)

    DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
    DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

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
    test_table_name = f"{path_in_database}/test_runs_column"
    full_path = posixpath.join(DATABASE_PATH, test_table_name)

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )

        # Parse and upload
        results = parse_junit_xml(
            test_results_file, build_type, job_name, job_id, commit, branch, pull, run_timestamp
        )
        result_with_owners = get_codeowners_for_tests(codeowners, results)
        prepared_for_update_rows = []
        for index, row in enumerate(result_with_owners):
            prepared_for_update_rows.append({
                'branch': row['branch'],
                'build_type': row['build_type'],
                'commit': row['commit'],
                'duration': row['duration'],
                'job_id': row['job_id'],
                'job_name': row['job_name'],
                'log': row['log'],
                'logsdir': row['logsdir'],
                'owners': row['owners'],
                'pull': row['pull'],
                'run_timestamp': row['run_timestamp'],
                'status_description': row['status_description'],
                'status': row['status'],
                'stderr': row['stderr'],
                'stdout': row['stdout'],
                'suite_folder': row['suite_folder'],
                'test_id': f"{row['pull']}_{row['run_timestamp']}_{index}",
                'test_name': row['test_name'],
            })
        print(f'upserting runs: {len(prepared_for_update_rows)} rows')

        with ydb.SessionPool(driver) as pool:
            create_tables(pool, test_table_name)
            bulk_upsert(driver.table_client, full_path,
                        prepared_for_update_rows)

        print('tests updated')


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import argparse
import os
import posixpath
import sys
import xml.etree.ElementTree as ET
import ydb

from codeowners import CodeOwners
from decimal import Decimal

# Add analytics directory to sys.path for ydb_wrapper import
_analytics_dir = os.path.dirname(os.path.abspath(__file__))
if _analytics_dir not in sys.path:
    sys.path.insert(0, _analytics_dir)
from ydb_wrapper import YDBWrapper

max_characters_for_status_description = int(7340032 / 4)

TABLE_NAME = "test_results/test_runs_column"


def get_table_schema(table_path):
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
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
            PRIMARY KEY (`test_name`, `suite_folder`, build_type, branch, status, run_timestamp)
        )
        PARTITION BY HASH(`suite_folder`, test_name, build_type, branch)
        WITH (STORE = COLUMN)
    """


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
                    "run_timestamp": int(run_timestamp) * 1000000,
                    "job_name": job_name,
                    "job_id": int(job_id),
                    "suite_folder": suite_folder,
                    "test_name": name,
                    "duration": Decimal(duration) if duration else Decimal(0),
                    "status": status,
                    "status_description": (status_description or "").replace("\r\n", ";;").replace("\n", ";;").replace("\"", "'")[:max_characters_for_status_description],
                    "log": "" if testcase.find("properties/property/[@name='url:log']") is None else testcase.find("properties/property/[@name='url:log']").get('value'),
                    "logsdir": "" if testcase.find("properties/property/[@name='url:logsdir']") is None else testcase.find("properties/property/[@name='url:logsdir']").get('value'),
                    "stderr": "" if testcase.find("properties/property/[@name='url:stderr']") is None else testcase.find("properties/property/[@name='url:stderr']").get('value'),
                    "stdout": "" if testcase.find("properties/property/[@name='url:stdout']") is None else testcase.find("properties/property/[@name='url:stdout']").get('value'),
                }
            )
    return results


def sort_codeowners_lines(codeowners_lines):
    def path_specificity(line):
        trimmed_line = line.strip()
        if not trimmed_line or trimmed_line.startswith('#'):
            return -1, -1
        path = trimmed_line.split()[0]
        return len(path.split('/')), len(path)

    return sorted(codeowners_lines, key=path_specificity)


def get_codeowners_for_tests(codeowners_file_path, tests_data):
    with open(codeowners_file_path, 'r') as file:
        data = file.readlines()
        owners_obj = CodeOwners(''.join(sort_codeowners_lines(data)))
        for test in tests_data:
            owners = owners_obj.of(test["suite_folder"])
            test["owners"] = ";;".join([":".join(x) for x in owners])
    return tests_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-results-file', required=True,
                        help='JUnit XML with results of tests')
    parser.add_argument('--build-type', required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--branch', required=True)
    parser.add_argument('--pull', required=True)
    parser.add_argument('--run-timestamp', dest="run_timestamp", required=True)
    parser.add_argument('--job-name', dest="job_name", required=True)
    parser.add_argument('--job-id', dest="job_id", required=True)
    args = parser.parse_args()

    dir_path = os.path.dirname(os.path.abspath(__file__))
    git_root = os.path.normpath(f"{dir_path}/../../..")
    codeowners = f"{git_root}/.github/TESTOWNERS"

    column_types = (
        ydb.BulkUpsertColumns()
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("commit", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("duration", ydb.OptionalType(ydb.PrimitiveType.Double))
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

    try:
        with YDBWrapper() as wrapper:
            if not wrapper.check_credentials():
                print("Warning: YDB credentials not found, skipping upload")
                return 0

            full_table_path = posixpath.join(wrapper.database_path, TABLE_NAME)
            wrapper.create_table(full_table_path, get_table_schema(full_table_path))

            results = parse_junit_xml(
                args.test_results_file, args.build_type, args.job_name, args.job_id,
                args.commit, args.branch, args.pull, args.run_timestamp
            )
            results_with_owners = get_codeowners_for_tests(codeowners, results)

            rows = []
            for index, row in enumerate(results_with_owners):
                rows.append({
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

            if rows:
                print(f'Uploading {len(rows)} test results to {full_table_path}')
                wrapper.bulk_upsert_batches(full_table_path, rows, column_types)
                print('tests uploaded')
            else:
                print('nothing to upload')

    except Exception as e:
        print(f"Warning: Failed to upload test results to YDB: {e}")
        print("This is not a critical error, continuing with CI process...")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())

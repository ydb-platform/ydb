#!/usr/bin/env python3

import argparse
import os
import sys
import xml.etree.ElementTree as ET
import ydb

from codeowners import CodeOwners
from decimal import Decimal
from ydb_wrapper import YDBWrapper

max_characters_for_status_description = int(7340032/4) #workaround for error "cannot split batch in according to limits: there is row with size more then limit (7340032)"

def get_column_types():
    """Получение типов колонок для bulk upsert"""
    return (
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

def get_table_schema(table_path):
    """Получение SQL-схемы для создания таблицы"""
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            -- Primary key columns (в порядке PRIMARY KEY)
            run_timestamp Timestamp NOT NULL,
            build_type Utf8 NOT NULL,
            branch Utf8 NOT NULL,
            test_name Utf8 NOT NULL,
            suite_folder Utf8 NOT NULL,
            status Utf8 NOT NULL,
            
            -- Other required columns
            test_id Utf8 NOT NULL,
            
            -- Optional columns
            job_name Utf8,
            job_id Uint64,
            commit Utf8,
            pull Utf8,
            duration Double,
            status_description Utf8,
            owners Utf8,
            log Utf8,
            logsdir Utf8,
            stderr Utf8,
            stdout Utf8,
            
            -- Primary key definition
            PRIMARY KEY (run_timestamp, build_type, branch, test_name, suite_folder, status)
        )
        PARTITION BY HASH(run_timestamp, build_type, branch, suite_folder)
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
                    "run_timestamp": int(run_timestamp)*1000000,
                    "job_name": job_name,
                    "job_id": int(job_id),
                    "suite_folder": suite_folder,
                    "test_name": name,
                    "duration": Decimal(duration),
                    "status": status,
                    "status_description": status_description.replace("\r\n", ";;").replace("\n", ";;").replace("\"", "'")[:max_characters_for_status_description],
                    "log": "" if testcase.find("properties/property/[@name='url:log']") is None else testcase.find("properties/property/[@name='url:log']").get('value'),
                    "logsdir": "" if testcase.find("properties/property/[@name='url:logsdir']") is None else testcase.find("properties/property/[@name='url:logsdir']").get('value'),
                    "stderr": "" if testcase.find("properties/property/[@name='url:stderr']") is None else testcase.find("properties/property/[@name='url:stderr']").get('value'),
                    "stdout": "" if testcase.find("properties/property/[@name='url:stdout']") is None else testcase.find("properties/property/[@name='url:stdout']").get('value'),

                }
            )
    return results


def sort_codeowners_lines(codeowners_lines):
    def path_specificity(line):
        # removing comments
        trimmed_line = line.strip()
        if not trimmed_line or trimmed_line.startswith('#'):
            return -1, -1
        path = trimmed_line.split()[0]
        return len(path.split('/')), len(path)

    sorted_lines = sorted(codeowners_lines, key=path_specificity)
    return sorted_lines

def get_codeowners_for_tests(codeowners_file_path, tests_data):
    with open(codeowners_file_path, 'r') as file:
        data = file.readlines()
        owners_odj = CodeOwners(''.join(sort_codeowners_lines(data)))
        tests_data_with_owners = []
        for test in tests_data:
            target_path = f'{test["suite_folder"]}'
            owners = owners_odj.of(target_path)
            test["owners"] = ";;".join(
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

    dir_path = os.path.dirname(__file__)
    git_root = f"{dir_path}/../../.."
    codeowners = f"{git_root}/.github/TESTOWNERS"

    try:
        # YDBWrapper сам найдет конфиг по умолчанию и определит имя скрипта
        with YDBWrapper() as wrapper:
            # Проверяем наличие учетных данных
            if not wrapper.check_credentials():
                print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
                return 1
            
            # Получаем относительный путь к таблице через wrapper
            test_table_path = wrapper.get_table_path("test_results")
            
            # Создаем таблицу если не существует (wrapper сам добавит database_path)
            wrapper.create_table(test_table_path, get_table_schema(test_table_path))
            
            # Парсим XML с результатами тестов
            results = parse_junit_xml(
                test_results_file, build_type, job_name, job_id, commit, branch, pull, run_timestamp
            )
            
            # Добавляем информацию о владельцах
            result_with_owners = get_codeowners_for_tests(codeowners, results)
            
            # Подготавливаем данные для загрузки
            prepared_for_upload_rows = []
            for index, row in enumerate(result_with_owners):
                prepared_for_upload_rows.append({
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
            
            # Загружаем данные в YDB (wrapper сам добавит database_path)
            if prepared_for_upload_rows:
                print(f'Uploading {len(prepared_for_upload_rows)} test results')
                wrapper.bulk_upsert_batches(
                    test_table_path, 
                    prepared_for_upload_rows, 
                    get_column_types(),
                    batch_size=1000
                )
                print('Tests uploaded successfully')
            else:
                print('No test results to upload')
                
    except Exception as e:
        print(f"Warning: Failed to upload test results to YDB: {e}")
        print("This is not a critical error, continuing with CI process...")
        return 0
    
    return 0

       


if __name__ == "__main__":
    sys.exit(main())

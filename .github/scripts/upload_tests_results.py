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
from concurrent.futures import ThreadPoolExecutor, as_completed



def get_git_root():
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing git command: {e}", file=sys.stderr)
        return None



def parse_codeowners(codeowners_file):
    # Чтение файла CODEOWNERS
    entries = []

    with open(codeowners_file, "r") as file:
        for line in file:
            if not line.strip() or line.startswith("#"):
                continue
            path, *owners = line.split()
            entries.append((path, owners))

    return entries


def parse_test_name(name):
    rawname = name
    if "[" in name and "]" in name:
        fixture = name[name.index("[") + 1 : name.index("]")]
        name = name[: name.index("[")]
    else:
        fixture = ""

    parts = name.split(".")
    filename = f"{parts[0]}.{parts[1]}" if len(parts) > 1 else ""

    testname = ".".join(parts[2:]) if len(parts) > 1 else ""

    return rawname, filename, testname, fixture


def parse_junit_xml(xml_file, build_type, job_name, job_id, commit, branch, pull, run_time):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    results = []
    for testsuite in root.findall(".//testsuite"):
        suite_name = testsuite.get("name")

        for testcase in testsuite.findall("testcase"):
            name = testcase.get("name")
            rawname, filename, testname, fixture = parse_test_name(name)
            time = testcase.get("time")
            
            status_description = ""
            status = "passed"
            if testcase.find("properties/property/[@name='mute']") is not None:
                status = "mute"
                status_description = testcase.find("properties/property/[@name='mute']").get('value')
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
                    "run_time": run_time,
                    "job_name": job_name,
                    "job_id": job_id,
                    "suite_name": suite_name,
                    "raw_test_name": rawname,
                    "file": filename,
                    "test_name": testname,
                    "fixture": fixture,
                    "time": float(time),
                    "status": status,
                    "status_description": status_description.replace("\r\n", ";;").replace("\n", ";;").replace("\"", "'"),
                    "log": "" if testcase.find("properties/property/[@name='url:log']") is None else testcase.find("properties/property/[@name='url:log']").get('value'),
                    "logsdir": "" if testcase.find("properties/property/[@name='url:logsdir']") is None else testcase.find("properties/property/[@name='url:logsdir']").get('value'),
                    "stderr": "" if testcase.find("properties/property/[@name='url:stderr']") is None else testcase.find("properties/property/[@name='url:stderr']").get('value'),
                    "stdout": "" if testcase.find("properties/property/[@name='url:stdout']") is None else testcase.find("properties/property/[@name='url:stdout']").get('value'),

                }
            )
    return results


def get_codeowners_for_tests(entries, tests_data):
    tests_data_with_owners = []
    for test in tests_data:
        # Функция для поиска владельцев для заданного пути на основе записей из CODEOWNERS.
        owners = []
        target_path = test["suite_name"]
        for path, path_owners in entries:
            path = path + "/" if path[-1] != "/" else path
            path = path + "*" if path[-1] != "*" else path
            if fnmatch.fnmatch(f"/{target_path}", path):
                owners.extend(path_owners)

        test["owners"] = ",".join(owners)
        tests_data_with_owners.append(test)
    return tests_data_with_owners


def upload_codeowners(session, entries):
    # Вставка данных в таблицу
    for entry in entries:
        path, owners = entry
        sql = f"""
        UPSERT INTO codeowners (path, owners) VALUES 
            ("{path}", {str(owners).replace("'", '"')});
        """
        session.transaction().execute(sql, commit_tx=True)


def create_table(session, sql):

    try:
        session.execute_scheme(sql)
    except ydb.issues.AlreadyExists:
        pass
    except Exception as e:
        print(f"Error creating table: {e}, sql:\n{sql}", file=sys.stderr)
        raise e


# Создание таблицы
def create_codeowners_table(session, table_path):
    sql = f"""
    --!syntax_v1
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        path Utf8,
        owners Utf8,
        PRIMARY KEY (path)
    );
    """
    create_table(session, sql)


def create_tests_table(session, table_path):
    # Создание таблицы, если ее еще нет
    sql = f"""
    --!syntax_v1
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        build_type Utf8,
        job_name Utf8,
        job_id Uint64,
        commit Utf8,
        branch Utf8,
        pull Utf8,
        run_time Timestamp,
        test_id Utf8,
        suite_name Utf8,
        raw_test_name Utf8,
        file Utf8,
        test_name Utf8,
        fixture Utf8,
        time Double,
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

    with ThreadPoolExecutor(max_workers=4) as executor:

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
            run_time, 
            test_id, 
            suite_name, 
            raw_test_name, 
            file, 
            test_name, 
            fixture, 
            time, 
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
                DateTime::FromSeconds({result['run_time']}),
                "{result['pull']}_{result['run_time']}_{start+index}",
                "{result['suite_name']}",
                "{result['raw_test_name']}",
                "{result['file']}",
                "{result['test_name']}",
                "{result['fixture']}",
                {result['time']},
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


def create_pool(endpoint, database): #, token):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.credentials_from_env_variables()
        #credentials=ydb.iam.ServiceAccountCredentials.from_file(token),
    )

    driver = ydb.Driver(driver_config)
    driver.wait(fail_fast=True, timeout=5)
    return ydb.SessionPool(driver)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--result-file', action='store', dest="result_file", required=True, help='XML with results of tests')
    parser.add_argument('--build-type', action='store', dest="build_type", required=True, help='build type')
    parser.add_argument('--commit', default='store', dest="commit", required=True, help='commit sha')
    parser.add_argument('--branch', default='store', dest="branch", required=True, help='branch name ')
    parser.add_argument('--pull', action='store', dest="pull",required=True, help='pull number')
    parser.add_argument('--run-time', action='store', dest="run_time",required=True, help='time of test run start')
    parser.add_argument('--job-name', action='store', dest="job_name",required=True, help='job name where launched')
    parser.add_argument('--job-id', action='store', dest="job_id",required=True, help='job id of workflow')

    args = parser.parse_args()

    result_file=args.result_file
    build_type=args.build_type
    commit=args.commit
    branch=args.branch
    pull=args.pull
    run_time=args.run_time
    job_name=args.job_name
    job_id=args.job_id

    path_in_database = "test_results"
    batch_size_default=30
    dir = os.path.dirname(__file__)
    git_root = f"{dir}/../.."
    codeowners = f"{git_root}/.github/CODEOWNERS"
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
  
    test_table_name= f"{path_in_database}/{job_name.lower().replace('-','_')}"
    #Prepare connection and table
    pool = create_pool(DATABASE_ENDPOINT, DATABASE_PATH)#, token)
    with pool.checkout() as session:
        create_tests_table(session, test_table_name)

    # Parse and upload
    results = parse_junit_xml(result_file, build_type, job_name, job_id, commit, branch, pull, run_time)
    result_with_owners = get_codeowners_for_tests(parse_codeowners(codeowners), results)
    prepare_and_upload_tests(pool, test_table_name, results, batch_size=batch_size_default)



if __name__ == "__main__":
    main()
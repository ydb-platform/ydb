#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import os
import sys
import ydb
import time


def parse_test_name(name):
    rawname = name
    if "[" in name and "]" in name:
        fixture = name[name.index('[') + 1: name.index(']')]
        name = name[:name.index('[')]
    else:
        fixture = ""

    parts = name.split(".")
    filename = f"{parts[0]}.{parts[1]}" if len(parts) > 1 else ""

    testname = ".".join(parts[2:]) if len(parts) > 1 else ""

    return rawname, filename, testname, fixture


def parse_junit_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    results = []
    for testsuite in root.findall(".//testsuite"):
        suite_name = testsuite.get("name")

        for testcase in testsuite.findall("testcase"):
            name = testcase.get("name")
            rawname, filename, testname, fixture = parse_test_name(name)
            time = testcase.get("time")

            status = "passed"
            if testcase.find("failure") is not None:
                status = "failure"
            elif testcase.find("error") is not None:
                status = "error"
            elif testcase.find("skipped") is not None:
                status = "skipped"

            results.append({
                "suite_name": suite_name,
                "raw_test_name": rawname,
                "file": filename,
                "test_name": testname,
                "fixture": fixture,
                "time": float(time),
                "status": status
            })
    return results

def parse_codeowners(codeowners_file):
    # Чтение файла CODEOWNERS
    entries = []

    with open(codeowners_file, 'r') as file:
        for line in file:
            if not line.strip():
                continue
            path, *owners = line.split()
            entries.append((path, owners))

    return entries

# Создание таблицы
def create_table_codeowners(session, table_path):
    table= f"{table_path}/codeowners"
    create_table_query = f"""
    CREATE TABLE `{table}` (
        path STRING,
        owners STRING,
        PRIMARY KEY (path)
    );
    """
    try:
        session.execute_scheme(sql)
    except (ydb.issues.AlreadyExists):
        pass
    except Exception as e:
        print("Error creating table: {}, sql:\n{}".format(e, sql), file=sys.stderr)
        raise e

def upload_codeowners(session, entries):

    # Вставка данных в таблицу
    for entry in entries:
        path, owners = entry
        sql = f"""
        UPSERT INTO codeowners (path, owners) VALUES 
            ("{path}", {str(owners).replace("'", '"')});
        """
        session.transaction().execute(sql, commit_tx=True)


def create_table(session, table_path):
         # Создание таблицы, если ее еще нет
    table= f"{table_path}/temp"
    sql= f"""
        --!syntax_v1
        CREATE TABLE IF NOT EXISTS `{table}` (
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
            PRIMARY KEY(test_id)
        );
        """
    

    try:
        session.execute_scheme(sql)
    except (ydb.issues.AlreadyExists):
        pass
    except Exception as e:
        print("Error creating table: {}, sql:\n{}".format(e, sql), file=sys.stderr)
        raise e

def upload_results(session, sql):
    session.transaction().execute(sql, commit_tx=True)

def prepare_and_upload_tests(pool,path,results, batch_size):
     # Вставка данных
    table= f"{path}/temp"
    total_records = len(results)
    for start in range(0, total_records, batch_size):
        end = min(start + batch_size, total_records)
        batch = results[start:end]

        # Вставка данных батчами
        #with session.transaction() as tx:
        for index, result in enumerate(batch):
            relative_index = start + index
            sql= f"""
                --!syntax_v1
                UPSERT INTO `{table}` 
                (test_id, pull, run_time, suite_name, raw_test_name, file, test_name, fixture, time, status)
                VALUES ('{pull}_{run_time}_{relative_index}', 
                '{pull}', 
                DateTime::FromSeconds({run_time}),
                '{result["suite_name"]}', 
                '{result["raw_test_name"]}', 
                '{result["file"]}', 
                '{result["test_name"]}', 
                '{result["fixture"]}', 
                {result["time"]}, 
                '{result["status"]}'
                );
                """
            pool.retry_operation_sync(lambda session: upload_results(session, sql))
    

def save_to_db(endpoint, database, path, token, results, batch_size):
    
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.iam.ServiceAccountCredentials.from_file(token),
    )
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=15)
        except concurrent.futures._base.TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            sys.exit(1)

    
        driver.wait(fail_fast=True, timeout=5)
       

        with ydb.SessionPool(driver) as pool:
            #pool.retry_operation_sync(lambda session: create_table(session, path))
            pool.retry_operation_sync(lambda session: create_table_codeowners(session, path))
            pool.retry_operation_sync(lambda session: upload_codeowners(session, entries))
            #prepare_and_upload_tests(pool,path, results, batch_size)
                
               # time.sleep(1)

    driver.stop()


if __name__ == '__main__':
    xml_file = '/home/kirrysin/fork/ydb/.github/scripts/orig_junit.xml'
    pull=123
    run_time=1719874785
    # Параметры подключения к YDB
    DATABASE_ENDPOINT = "grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135"
    DATABASE_PATH = "/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8"
    path = 'test_results'
    token = '/home/kirrysin/fork/ydb/.github/scripts/my-robot-key.json'  # Токен для аутентификации
    if not os.path.exists(token):
        print(f"IAM token file not found by path {token}")
        sys.exit(1)

    results = parse_junit_xml(xml_file)
    owners=parse_codeowners("/home/kirrysin/fork/ydb/.github/CODEOWNERS")
    save_to_db(DATABASE_ENDPOINT, DATABASE_PATH, path, token, results, batch_size=10)
    print("JUnit XML результаты успешно загружены в YDB!")
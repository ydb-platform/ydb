
#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import os
import sys
import ydb
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import fnmatch

def parse_codeowners(codeowners_file):
    # Чтение файла CODEOWNERS
    entries = []

    with open(codeowners_file, 'r') as file:
        for line in file:
            if not line.strip() or line.startswith("#"):
                continue
            path, *owners = line.split()
            entries.append((path, owners))

    return entries



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

def parse_junit_xml(xml_file, pull, run_time):
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
                "pull": pull,
                "run_time": run_time,
                "suite_name": suite_name,
                "raw_test_name": rawname,
                "file": filename,
                "test_name": testname,
                "fixture": fixture,
                "time": float(time),
                "status": status
            })
    return results

def get_codeowners_for_tests(entries, tests_data):
    tests_data_with_owners=[]
    for test in tests_data:
        #Функция для поиска владельцев для заданного пути на основе записей из CODEOWNERS.
        owners = []
        target_path = test['suite_name']
        for path, path_owners in entries:
            path = path +'/' if path[-1] != "/" else path
            path = path +'*' if path[-1] != "*" else path
            if fnmatch.fnmatch(f'/{target_path}', path):
                owners.extend(path_owners)
        
        test['owners']= ','.join(owners)
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
    except (ydb.issues.AlreadyExists):
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
    sql= f"""
    --!syntax_v1
    CREATE TABLE IF NOT EXISTS `{table_path}` (
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
        owners Utf8,
        PRIMARY KEY(test_id)
    );
    """
    create_table(session, sql)
   
def upload_results(pool, sql):
    with pool.checkout() as session:
        try:
            session.transaction().execute(sql, commit_tx=True)
        except Exception as e:
            print(f"Error creating table: {e}, sql:\n{sql}", file=sys.stderr)
            raise e

def prepare_and_upload_tests(pool, path, results, batch_size):

    table = f"{path}/temp"
    total_records = len(results)

    with ThreadPoolExecutor(max_workers=4) as executor:
        
        for start in range(0, total_records, batch_size):
            futures = []
            end = min(start + batch_size, total_records)
            batch = results[start:end]

            sql = f"""--!syntax_v1
             UPSERT INTO `{table}` (pull, run_time, test_id, suite_name, raw_test_name, file, test_name, fixture, time, status, owners) VALUES"""
            values = []
            for index, result in enumerate(batch):
                values.append(f"""
                ("{result['pull']}", DateTime::FromSeconds({result['run_time']}), "{result['pull']}_{result['run_time']}_{start+index}", "{result['suite_name']}", "{result['raw_test_name']}", "{result['file']}", "{result['test_name']}", "{result['fixture']}", {result['time']}, "{result['status']}, "{result['owners']}")
                """)
            sql += ", ".join(values) + ";"

            futures.append(executor.submit(upload_results, pool, sql))
        
        for future in as_completed(futures):
            future.result()  # Raise exception if occurred
def create_pool(endpoint, database, token):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.iam.ServiceAccountCredentials.from_file(token),
    )
    
    driver = ydb.Driver(driver_config)
    driver.wait(fail_fast=True, timeout=5)
    return ydb.SessionPool(driver)
if __name__ == '__main__':
    # Использование скрипта:
    # python3 script.py <path_to_junit_xml> <path_to_codeowners> <database_endpoint> <path_in_database>
    #xml_file = sys.argv[1]
   # codeowners_file = sys.argv[2]
   # db_endpoint = sys.argv[3]
   # path_in_database = sys.argv[4]

    pull=222
    run_time=1719874785
    # Параметры подключения к YDB
    endpoint = "grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135"
    database = "/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8"

    path_in_database = 'test_results'
    token = '/home/kirrysin/fork/ydb/.github/scripts/my-robot-key.json'  
    xml_file = '/home/kirrysin/fork/ydb/.github/scripts/orig_junit.xml'
    codeowners = '/home/kirrysin/fork/ydb/.github/CODEOWNERS'


    pool = create_pool(endpoint, database, token)
    # Создание таблиц
    with pool.checkout() as session:
        create_codeowners_table(session, f'{path_in_database}/codeowners')
        create_tests_table(session, f'{path_in_database}/tests')

    # Парсинг и загрузка codeowners
   # entries = parse_codeowners(codeowners_file)
   # with pool.checkout() as session:
   #     upload_codeowners(session, entries)

    # Парсинг и загрузка результатов тестов
    results = parse_junit_xml(xml_file,pull,run_time)
    result_with_owners=get_codeowners_for_tests(parse_codeowners(codeowners),results)
    prepare_and_upload_tests(pool, path_in_database, results, batch_size=250)
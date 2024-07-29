#!/usr/bin/env python3

import configparser
import os
import ydb


dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../config/ydb_qa_db.ini"
config.read(config_file_path)

build_preset = os.environ.get("build_preset")
branch = os.environ.get("branch_to_compare")

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def get_build_size():
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]="/home/kirrysin/fork/ydb/.github/scripts/my-robot-key.json"
        #return 0
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    sql = f"""
    
 
    """

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        



python
import ydb
import ydb.iam

# Конфигурация подключения к YDB
endpoint = "grpc://ydb-ru.yandex.net:2135"
database = "/your/database/path"
sa_key_file = "path/to/your/service_account_key.json"

# Настройка драйвера YDB
driver_config = ydb.DriverConfig(
    endpoint, 
    database, 
    credentials=ydb.iam.ApiKey(service_account_key_file=sa_key_file)
)

# Инициализация драйвера
driver = ydb.Driver(driver_config)
driver.wait(fail_fast=True, timeout=5)
session = driver.table_client.session().create()

table_path = database + "/your_table"
page_size = 100

try:
    for record in get_records_with_pagination(session, table_path, page_size):
        print(record)

finally:
    driver.stop()

def get_records_with_pagination(session, table_path, page_size):
    last_key = None
    while True:
        # Создание запроса SELECT с фильтрацией по ключам
        query = f"""
        SELECT * FROM {table_path}
        WHERE Key > $last_key
        ORDER BY Key
        LIMIT $page_size;
        """
        
        params = {
            "$last_key": ydb.Value.OptionalString(last_key),
            "$page_size": ydb.Value.Int32(page_size)
        }
        
        result_set = session.transaction().execute(query, params)
        records = list(result_set[0].rows)
        
        if not records:
            break
        
        for record in records:
            yield record
        
        last_key = records[-1]['Key']

# Пример использования функции get_records_with_pagination


if __name__ == "__main__":
    get_build_size()

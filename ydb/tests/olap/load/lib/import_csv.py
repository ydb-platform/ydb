from __future__ import annotations
from .conftest import LoadSuiteBase
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from time import time
import yatest.common
import allure
import os
import pytest
import requests
import zipfile


class ImportFileCsv(LoadSuiteBase):
    table_path = get_external_param('table-path-import-csv', f'{YdbCluster.tables_path}/import_test_table')
    file_url = get_external_param('file-url-import-csv', 'https://proxy.sandbox.yandex-team.ru/8980545283')
    zip_file_name = 'data.csv.zip'
    file_name = 'data.csv'

    def download_and_extract_file(self):
        response = requests.get(self.file_url)
        with open(self.zip_file_name, 'wb') as f:
            f.write(response.content)

        with zipfile.ZipFile(self.zip_file_name, 'r') as zip_ref:
            zip_ref.extractall()

        os.remove(self.zip_file_name)

    def drop_table_if_exists(self):
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['sql', '-s', 'DROP TABLE IF EXISTS `{self.table_path}`'])

    def create_table(self):
        sql_filename = 'create.sql'
        sql_text = f'''
CREATE TABLE `{self.table_path}` (
    `event_time` Text NOT NULL,
    `event_type` Text,
    `product_id` Uint64,
    `category_id` Uint64,
    `category_code` Text,
    `brand` Text,
    `price` Double,
    `user_id` Uint64,
    `user_session` Text,
    PRIMARY KEY (`event_time`)
)
WITH (
    STORE = COLUMN
    , AUTO_PARTITIONING_BY_SIZE = ENABLED
    , AUTO_PARTITIONING_BY_LOAD = ENABLED
    , UNIFORM_PARTITIONS = 100
    , AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    , AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000
);
'''
        with open(sql_filename, 'w') as f:
            f.write(sql_text)
        try:
            yatest.common.execute(YdbCliHelper.get_cli_command() + ['sql', '-f', sql_filename])
        finally:
            os.remove(sql_filename)

    def init(self):
        self.download_and_extract_file()
        self.drop_table_if_exists()
        self.create_table()

    def import_data(self):
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['import', 'file', 'csv', '-p', self.table_path, self.file_name, '--header'])

    def test(self):
        start_time = time()
        result = YdbCliHelper.ImportCsvRunResult()
        result.traceback = None
        try:
            self.save_nodes_state()
            with allure.step("init"):
                self.init()
            result.file_size_bytes = os.path.getsize(self.file_name)
            start_time = time()
            with allure.step("import data"):
                self.import_data()
        except BaseException as e:
            result.add_error(str(e))
            result.traceback = e.__traceback__
        result.time = time() - start_time
        if result.time > 0:
            result.import_speed = result.file_size_bytes / result.time
        else:
            result.import_speed = 0
        self.process_import_result(result)
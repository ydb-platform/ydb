from __future__ import annotations
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from .upload import UploadSuiteBase
import logging
import os
import json
import yatest.common


class ImportFileCsvBase(UploadSuiteBase):
    query_name: str = 'ImportFileCsv'  # Override UploadSuiteBase.query_name
    table_name: str = ''
    table_path: str = ''
    iterations: int = 1

    def init(self):
        # Create tables
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'query', '-p', YdbCluster.tables_path, 'init', '--suite-path', self.get_external_path(), '--clear'])

        import_dir = os.path.join(self.get_external_path(), "import")
        table_names = sorted([name for name in os.listdir(import_dir) if os.path.isdir(os.path.join(import_dir, name))])
        if not table_names:
            raise RuntimeError(f"Found no directories in {import_dir}")
        self.table_name = table_names[0]  # importing just one table
        logging.info(f'Importing table: {self.table_name}')

    def import_data(self):
        self.table_path = f'{YdbCluster.tables_path}/{self.table_name}'
        logging.info(f'Table path: {self.table_path}')
        import_dir = os.path.join(self.get_external_path(), 'import', self.table_name)
        csv_files = [f for f in os.listdir(import_dir) if os.path.isfile(os.path.join(import_dir, f)) and f.endswith('.csv')]
        if not csv_files:
            raise RuntimeError(f'No .csv files found in {import_dir}')
        import_path = os.path.join(import_dir, csv_files[0])
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['import', 'file', 'csv', '-p', self.table_path, import_path, '--header'])

    def validate(self, result: YdbCliHelper.WorkloadRunResult):
        select_command = yatest.common.execute(YdbCliHelper.get_cli_command() + ['sql', '-s', f'SELECT COUNT (*) AS count FROM `{self.table_path}`', '--format', 'json-unicode'])
        select_command_result = select_command.stdout.decode('utf-8')
        count = json.loads(select_command_result)["count"]
        assert count > 0, f'No rows imported into {self.table_path}'
        logging.info(f'Rows in table {self.table_path} after import: {count}')
        result.add_stat(self.query_name, 'rows_in_table', count)

    def save_result_additional_info(self, result: YdbCliHelper.WorkloadRunResult):
        import_dir = os.path.join(self.get_external_path(), 'import', self.table_name)
        file_size = sum(
            os.path.getsize(os.path.join(import_dir, f))
            for f in os.listdir(import_dir)
            if os.path.isfile(os.path.join(import_dir, f)) and f.endswith('.csv')
        )
        logging.info(f'File size: {file_size} bytes')
        result.add_stat(self.query_name, 'file_size', file_size)
        import_time = result.iterations[0].time
        logging.info(f'Result import time: {import_time} s')
        result.add_stat(self.query_name, 'import_time', import_time)
        import_speed = 0
        if import_time > 0:
            import_speed = file_size / import_time / 1024 / 1024  # MB/s
        logging.info(f'Result import speed: {import_speed} MB/s')
        result.add_stat(self.query_name, 'import_speed', import_speed)

    @classmethod
    def teardown_class(cls) -> None:
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'query', '-p', YdbCluster.tables_path, 'clean'])
        super().teardown_class()



class TestImportFileCsv(ImportFileCsvBase):
    external_folder: str = 'ecommerce'

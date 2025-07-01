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
    cpu_cores: float = 0.0
    cpu_time: float = 0.0
    send_format: str = ''  # Optional parameter for send format

    def init(self):
        # Create tables
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'query', '-p', YdbCluster.get_tables_path(), 'init', '--suite-path', self.get_external_path(), '--clear'])

        import_dir = os.path.join(self.get_external_path(), "import")
        table_names = sorted([name for name in os.listdir(import_dir) if os.path.isdir(os.path.join(import_dir, name))])
        if not table_names:
            raise RuntimeError(f"Found no directories in {import_dir}")
        self.table_name = table_names[0]  # importing just one table
        logging.info(f'Importing table: {self.table_name}')

    def import_data(self):
        self.table_path = YdbCluster.get_tables_path(self.table_name)
        logging.info(f'Table path: {self.table_path}')
        import_dir = os.path.join(self.get_external_path(), 'import', self.table_name)
        csv_files = [f for f in os.listdir(import_dir) if os.path.isfile(os.path.join(import_dir, f)) and f.endswith('.csv')]
        if not csv_files:
            raise RuntimeError(f'No .csv files found in {import_dir}')
        import_path = os.path.join(import_dir, csv_files[0])

        cmd = ['/usr/bin/time'] + YdbCliHelper.get_cli_command() + ['import', 'file', 'csv', '-p', self.table_path, import_path, '--header']
        if self.send_format:
            cmd.extend(['--send-format', self.send_format])

        result = yatest.common.execute(cmd)

        assert result.returncode == 0, f'Import failed with return code {result.returncode} and stderr: {result.stderr.decode("utf-8")}'

        stderr_output = result.stderr.decode('utf-8')
        for line in stderr_output.split('\n'):
            if 'CPU' in line:
                try:
                    # Parsing a string like "2018.00user 58.02system 1:19.91elapsed 2597%CPU"
                    parts = line.split()
                    user_time = float(parts[0].replace('user', ''))
                    system_time = float(parts[1].replace('system', ''))
                    self.cpu_time = user_time + system_time
                    cpu_percent = float(line.split('%CPU')[0].strip().split()[-1])
                    self.cpu_cores = cpu_percent / 100.0
                    logging.info(f'CPU cores used: {self.cpu_cores}')
                    logging.info(f'Total CPU time (user + system): {self.cpu_time:.2f} seconds')
                except (ValueError, IndexError) as e:
                    logging.warning(f'Failed to parse CPU usage information: {e}')

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
        result.add_stat(self.query_name, 'cpu_cores', self.cpu_cores)
        result.add_stat(self.query_name, 'cpu_time', self.cpu_time)

    @classmethod
    def teardown_class(cls) -> None:
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'query', '-p', YdbCluster.get_tables_path(), 'clean'])
        super().teardown_class()


class TestImportFileCsv(ImportFileCsvBase):
    external_folder: str = 'ecommerce'


class TestImportFileCsvArrow(ImportFileCsvBase):
    external_folder: str = 'ecommerce'
    send_format: str = 'arrow'

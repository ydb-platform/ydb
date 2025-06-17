from __future__ import annotations
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
import yatest.common
import os
from .upload import UploadSuiteBase


class ImportFileCsvBase(UploadSuiteBase):
    query_name: str = 'ImportFileCsv' # Override UploadSuiteBase.query_name
    table_path: str = ''
    iterations: int = 1

    def init(self):
        # Create tables
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'query', '-p', YdbCluster.tables_path, 'init', '--suite-path', self.data_folder, '--clear'])

        import_dir = os.path.join(self.get_external_path(), "import")
        table_names = [name for name in os.listdir(import_dir) if os.path.isdir(os.path.join(import_dir, name))]
        if not table_names:
            raise RuntimeError(f"Found no directories in {import_dir}")
        self.table_name = table_names[0] # importing just one table

    def import_data(self):
        table_path = f'{YdbCluster.tables_path}/{self.table_name}'
        import_dir = os.path.join(self.get_external_path(), 'import', self.table_name)
        csv_files = [f for f in os.listdir(import_dir) if os.path.isfile(os.path.join(import_dir, f)) and f.endswith('.csv')]
        if not csv_files:
            raise RuntimeError(f'No .csv files found in {import_dir}')
        import_path = os.path.join(import_dir, csv_files[0])
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['import', 'file', 'csv', '-p', table_path, import_path, '--header'])

    def save_result_additional_info(self, result: YdbCliHelper.WorkloadRunResult):
        import_dir = os.path.join(self.get_external_path(), 'import', self.table_name)
        file_size = sum(
            os.path.getsize(os.path.join(import_dir, f))
            for f in os.listdir(import_dir)
            if os.path.isfile(os.path.join(import_dir, f)) and f.endswith('.csv')
        )
        result.add_stat(self.query_name, 'file_size', file_size)
        import_speed = 0
        import_time = result.iterations[0].time
        if import_time > 0:
            import_speed = file_size / import_time
        result.add_stat(self.query_name, 'import_speed', import_speed)
        result.add_stat(self.query_name, 'import_time', import_time)


class TestImportFileCsv(ImportFileCsvBase):
    external_folder: str = 'ecommerce'
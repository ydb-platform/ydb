from __future__ import annotations
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
import yatest.common
import os
from .upload import UploadSuiteBase


class ImportFileCsvBase(UploadSuiteBase):
    query_name: str = 'ImportFileCsv' # Override UploadSuiteBase.query_name
    data_folder: str = ''
    table_path: str = ''
    iterations: int = 1

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.data_folder = cls.get_external_path()
        super().setup_class()

    def drop_table_if_exists(self):
        tables_path = YdbCluster.tables_path
        for table_name in self.table_names:
            full_table_path = f'{tables_path}/{table_name}'
            yatest.common.execute(YdbCliHelper.get_cli_command() + ['sql', '-s', f'DROP TABLE IF EXISTS `{full_table_path}`'])

    def create_table(self):
        init_dir = os.path.join(self.data_folder, 'init')
        for fname in sorted(os.listdir(init_dir)):
            if fname.endswith('.sql'):
                sql_path = os.path.join(init_dir, fname)
                yatest.common.execute(YdbCliHelper.get_cli_command() + ['sql', '-f', sql_path])

    def init(self):
        import_path = os.path.join(self.data_folder, "import")
        self.table_names = [name for name in os.listdir(import_path) if os.path.isdir(os.path.join(import_path, name))]
        if not self.table_names:
            raise RuntimeError(f"Found no directories in {import_path}")
        self.table_name = self.table_names[0] # importing just one table
        self.table_path = f'{YdbCluster.tables_path}/{self.table_name}'
        self.drop_tables_if_exists()
        self.create_table()

    def import_data(self):
        import_path = os.path.join(self.data_folder, 'import', self.table_name, '*') # All files in the table directory
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['import', 'file', 'csv', '-p', self.table_path, import_path, '--header'])

    def save_result_additional_info(self, result: YdbCliHelper.WorkloadRunResult):
        import_dir = os.path.join(self.data_folder, 'import', self.table_name)
        file_size = sum(
            os.path.getsize(os.path.join(import_dir, f))
            for f in os.listdir(import_dir)
            if os.path.isfile(os.path.join(import_dir, f)) and f.endswith('.csv')
        )
        result.add_stat(self.query_name, 'file_size', file_size)
        import_speed = 0
        if result.time > 0:
            import_speed = file_size / result.time
        result.add_stat(self.query_name, 'import_speed', import_speed)


class TestImportFileCsv(ImportFileCsvBase):
    external_folder: str = 'ecommerce'
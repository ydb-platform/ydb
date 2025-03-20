import pytest
from ydb.tests.olap.lib.ydb_cli import YdbCli
from hamcrest import assert_that, has_item, not_, has_property, contains_string


class TestOlapTableRename(object):
    @classmethod
    def setup_class(cls):
        cls.cli = YdbCli()
        cls.database = '/my-db'
        cls.base_table_path = 'olap_table_rename_test'

        cls.table_schema = '''
            CREATE TABLE {path} (
                id Uint64,
                data String,
                PRIMARY KEY (id)
            ) WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
        '''

    def setup_method(self, method):
        self.table_name = f'{self.base_table_path}_{method.__name__}'
        self.cli.execute_scheme(self.table_schema.format(path=self.table_name))

    def teardown_method(self, method):
        try:
            self.cli.execute_scheme(f'DROP TABLE `{self.table_name}`')
        except Exception:
            pass

    @pytest.mark.olap
    @pytest.mark.operations
    def test_olap_table_rename(self):
        new_table_name = f'{self.table_name}_renamed'
        test_data = [(1, "Test1"), (2, "Test2")]

        self.cli.execute_ydb(f'''
            UPSERT INTO `{self.table_name}`
            SELECT * FROM AS_TABLE({test_data});
        ''')

        self.cli.execute_scheme(
            f'ALTER TABLE `{self.table_name}` RENAME TO `{new_table_name}`'
        )

        with pytest.raises(Exception) as excinfo:
            self.cli.execute_ydb(f'SELECT * FROM `{self.table_name}`')
        assert_that(str(excinfo.value), contains_string("Table not found"))

        desc = self.cli.describe_table(new_table_name)
        assert_that(desc.columns, has_item(has_property('name', 'id')))
        assert_that(desc.columns, has_item(has_property('name', 'data')))

        result = self.cli.execute_ydb(f'''
            SELECT data FROM `{new_table_name}` ORDER BY id
        ''')
        assert [r.data for r in result] == ['Test1', 'Test2']

        table_info = self.cli.execute_ydb(f'''
            SELECT TableSettings FROM .describe_table('{new_table_name}')
        ''')

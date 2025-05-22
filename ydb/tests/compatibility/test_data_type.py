import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.datashard.lib.create_table import create_table_sql_request
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, cleanup_type_name, format_sql_value

TABLE_NAME = "table"


class TestDataType(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):

        yield from self.setup_cluster(extra_feature_flags={
            "enable_parameterized_decimal": True,
            "enable_table_datetime64": True,
            })

    def write_data(self):
        values = []
        for key in range(1, self.count_rows + 1):
            values.append(
                f'''(
                    {", ".join([format_sql_value(pk_types[type_name](key), type_name) for type_name in pk_types.keys()])},
                    {", ".join([format_sql_value(self.all_types[type_name](key), type_name) for type_name in self.all_types.keys()])}
                    )
                    '''
            )
        upsert_sql = f"""
            UPSERT INTO `{TABLE_NAME}` (
                {", ".join([f"pk_{cleanup_type_name(type_name)}" for type_name in pk_types.keys()])},
                {", ".join([f"col_{cleanup_type_name(type_name)}" for type_name in self.all_types.keys()])}
            )
            VALUES {",".join(values)};
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(upsert_sql)

    def check_table(self):
        queries = []
        for i in range(1, self.count_rows):
            queries.append(
                f"SELECT * FROM {TABLE_NAME} WHARE pk_Int64 = {i}"
            )

        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in queries:
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1

    def create_table(self):
        pk_columns = {
            "pk_": pk_types.keys(),
        }
        query = create_table_sql_request(
            TABLE_NAME, columns=self.columns, pk_colums=pk_columns, index_colums={}, unique="", sync=""
        )
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def test_data_type(self):
        self.count_rows = 30
        self.all_types = {**pk_types, **non_pk_types}
        self.columns = {
            "pk_": pk_types.keys(),
            "col_": self.all_types.keys(),
        }
        self.create_table()

        self.write_data()
        self.check_table()

        self.change_cluster_version()

        self.write_data()
        self.check_table()

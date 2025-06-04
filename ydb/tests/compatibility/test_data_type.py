import pytest
import math

from datetime import datetime, timedelta
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.datashard.lib.create_table import create_table_sql_request
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, cleanup_type_name, format_sql_value


class TestDataType(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.table_name = "table"
        self.count_rows = 30
        self.all_types = {**pk_types, **non_pk_types}
        self.columns = {
            "pk_": pk_types.keys(),
            "col_": self.all_types.keys(),
        }
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
                "enable_table_datetime64": True,
            }
        )

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
            UPSERT INTO `{self.table_name}` (
                {", ".join([f"pk_{cleanup_type_name(type_name)}" for type_name in pk_types.keys()])},
                {", ".join([f"col_{cleanup_type_name(type_name)}" for type_name in self.all_types.keys()])}
            )
            VALUES {",".join(values)};
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(upsert_sql)

    def check_table(self):
        queries = []
        for i in range(1, self.count_rows + 1):
            queries.append(f"SELECT * FROM {self.table_name} WHERE pk_Int64 = {i}")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            count = 0
            for query in queries:
                count += 1
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1
                rows = result_sets[0].rows
                for row in rows:
                    for prefix in self.columns.keys():
                        for type_name in self.columns[prefix]:
                            self.assert_type(type_name, count, row[f"{prefix}{cleanup_type_name(type_name)}"])

    def assert_type(self, data_type: str, values: int, values_from_rows):
        if data_type == "String" or data_type == "Yson":
            assert values_from_rows.decode("utf-8") == self.all_types[data_type](
                values
            ), f"{data_type}, expected {self.all_types[data_type](values)}, received {values_from_rows.decode('utf-8')}"
        elif data_type == "Float" or data_type == "DyNumber":
            assert math.isclose(
                float(values_from_rows), float(self.all_types[data_type](values)), rel_tol=1e-3
            ), f"{data_type}, expected {self.all_types[data_type](values)}, received {values_from_rows}"
        elif data_type == "Interval" or data_type == "Interval64":
            assert values_from_rows == timedelta(
                microseconds=self.all_types[data_type](values)
            ), f"{data_type}, expected {timedelta(microseconds=self.all_types[data_type](values))}, received {values_from_rows}"
        elif data_type == "Timestamp" or data_type == "Timestamp64":
            assert values_from_rows == datetime.fromtimestamp(
                self.all_types[data_type](values) / 1_000_000 - 3 * 60 * 60
            ), f"{data_type}, expected {datetime.fromtimestamp(self.all_types[data_type](values)/1_000_000)}, received {values_from_rows}"
        elif data_type == "Json" or data_type == "JsonDocument":
            assert str(values_from_rows).replace("'", "\"") == str(
                self.all_types[data_type](values)
            ), f"{data_type}, expected {self.all_types[data_type](values)}, received {values_from_rows}"
        else:
            assert str(values_from_rows) == str(
                self.all_types[data_type](values)
            ), f"{data_type}, expected {self.all_types[data_type](values)}, received {values_from_rows}"

    def create_table(self):
        pk_columns = {
            "pk_": pk_types.keys(),
        }
        query = create_table_sql_request(
            self.table_name, columns=self.columns, pk_columns=pk_columns, index_columns={}, unique="", sync=""
        )
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def test_data_type(self):
        self.create_table()

        self.write_data()
        self.check_table()

        self.change_cluster_version()

        self.check_table()
        self.write_data()
        self.check_table()

        self.change_cluster_version()

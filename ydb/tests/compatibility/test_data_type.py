import pytest
import math

from datetime import datetime, timedelta
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_columnshard_table_sql_request
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, cleanup_type_name, format_sql_value


class TestDataType(RestartToAnotherVersionFixture):
    @pytest.fixture
    def store_type(self, request):
        return request.param

    @pytest.fixture(autouse=True, scope="function")
    def setup(self, store_type):
        self.store_type = store_type
        
        if self.store_type == "COLUMN":

            simple_pk_types = {
                "Int64": lambda i: i,
                "Int32": lambda i: i,
                "Int16": lambda i: i,
                "Decimal(15,0)": lambda i: f"{i}",
                "Decimal(22,9)": lambda i: f"{i}.123456789",
                "Decimal(35,10)": lambda i: f"{i}.1234567890",
            }
            
            simple_col_types = {
                "Int64": lambda i: i,
                "Int32": lambda i: i,
                "Int16": lambda i: i,
                "Decimal(15,0)": lambda i: f"{i}",
                "Decimal(22,9)": lambda i: f"{i}.123456789",
                "Decimal(35,10)": lambda i: f"{i}.1234567890",
            }
            
            self.pk_types = []
            self.pk_types.append({"Int64": lambda i: i})
            self.count_table = 1
            
            for type_name, lamb in simple_pk_types.items():
                self.pk_types[self.count_table - 1][type_name] = lamb
                if len(self.pk_types[self.count_table - 1]) >= 6:
                    self.pk_types.append({"Int64": lambda i: i})
                    self.count_table += 1
            
            self.all_types = {**simple_pk_types, **simple_col_types}
        else:
            self.pk_types = []
            self.pk_types.append({"Int64": lambda i: i})
            self.count_table = 1
            
            for type_name, lamb in pk_types.items():
                self.pk_types[self.count_table - 1][type_name] = lamb
                if len(self.pk_types[self.count_table - 1]) >= 20:
                    self.pk_types.append({"Int64": lambda i: i})
                    self.count_table += 1
            
            self.all_types = {**pk_types, **non_pk_types}
        
        self.table_names = []
        self.count_rows = 30
        self.columns = []
        for i in range(self.count_table):
            if self.store_type == "COLUMN":
                col_types = {**simple_pk_types, **simple_col_types}
            else:
                col_types = self.all_types
            
            self.columns.append(
                {
                    "pk_": self.pk_types[i].keys(),
                    "col_": col_types.keys(),
                }
            )
            self.table_names.append(f"table_{i}_{self.store_type.lower()}")
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
                "enable_table_datetime64": True,
            },
            column_shard_config={
                'disabled_on_scheme_shard': False,
            }
        )

    def write_data(self):
        if self.store_type == "COLUMN":
            for i in range(self.count_table):
                for key in range(1, self.count_rows + 1):
                    pk_params = {}
                    for type_name in self.pk_types[i].keys():
                        param_name = f"pk_{cleanup_type_name(type_name)}"
                        value = self.pk_types[i][type_name](key)
                        
                        if type_name == "Int64":
                            pk_params[param_name] = value
                        elif type_name == "Int32":
                            pk_params[param_name] = (value, ydb.PrimitiveType.Int32)
                        elif type_name == "Int16":
                            pk_params[param_name] = (value, ydb.PrimitiveType.Int16)
                        elif type_name in ["Decimal(35,10)", "Decimal(22,9)", "Decimal(15,0)"]:
                            from decimal import Decimal
                            if type_name == "Decimal(15,0)":
                                pk_params[param_name] = (Decimal(str(value)), ydb.DecimalType(15, 0))
                            elif type_name == "Decimal(22,9)":
                                pk_params[param_name] = (Decimal(str(value)), ydb.DecimalType(22, 9))
                            elif type_name == "Decimal(35,10)":
                                pk_params[param_name] = (Decimal(str(value)), ydb.DecimalType(35, 10))

                    col_params = {}
                    for type_name in self.columns[i]["col_"]:
                        param_name = f"col_{cleanup_type_name(type_name)}"
                        value = self.all_types[type_name](key)
                        
                        if type_name == "Int64":
                            col_params[param_name] = value
                        elif type_name == "Int32":
                            col_params[param_name] = (value, ydb.PrimitiveType.Int32)
                        elif type_name == "Int16":
                            col_params[param_name] = (value, ydb.PrimitiveType.Int16)
                        elif type_name in ["Decimal(35,10)", "Decimal(22,9)", "Decimal(15,0)"]:
                            from decimal import Decimal
                            if type_name == "Decimal(15,0)":
                                col_params[param_name] = (Decimal(str(value)), ydb.DecimalType(15, 0))
                            elif type_name == "Decimal(22,9)":
                                col_params[param_name] = (Decimal(str(value)), ydb.DecimalType(22, 9))
                            elif type_name == "Decimal(35,10)":
                                col_params[param_name] = (Decimal(str(value)), ydb.DecimalType(35, 10))

                    from collections import OrderedDict
                    ordered_params = OrderedDict()

                    for type_name in self.pk_types[i].keys():
                        param_name = f"pk_{cleanup_type_name(type_name)}"
                        ordered_params[param_name] = pk_params[param_name]

                    for type_name in self.columns[i]['col_']:
                        param_name = f"col_{cleanup_type_name(type_name)}"
                        ordered_params[param_name] = col_params[param_name]

                    param_names = []
                    for type_name in self.pk_types[i].keys():
                        param_names.append(f"pk_{cleanup_type_name(type_name)}")
                    for type_name in self.columns[i]['col_']:
                        param_names.append(f"col_{cleanup_type_name(type_name)}")
                    
                    query = f"""
                    UPSERT INTO `{self.table_names[i]}` (
                        {", ".join([f"pk_{cleanup_type_name(type_name)}" for type_name in self.pk_types[i].keys()])},
                        {", ".join([f"col_{cleanup_type_name(type_name)}" for type_name in self.columns[i]['col_']])}
                    )
                    VALUES (
                        {", ".join([f"${param_name}" for param_name in param_names])}
                    );
                    """
                    
                    with ydb.QuerySessionPool(self.driver) as session_pool:
                        session_pool.execute_with_retries(query, parameters=ordered_params)
        else:
            querys = []
            for i in range(self.count_table):
                values = []
                for key in range(1, self.count_rows + 1):
                    values.append(
                        f'''(
                            {", ".join([format_sql_value(self.pk_types[i][type_name](key), type_name) for type_name in self.pk_types[i].keys()])},
                            {", ".join([format_sql_value(self.all_types[type_name](key), type_name) for type_name in self.columns[i]['col_']])}
                            )
                            '''
                    )
                querys.append(
                    f"""
                    UPSERT INTO `{self.table_names[i]}` (
                        {", ".join([f"pk_{cleanup_type_name(type_name)}" for type_name in self.pk_types[i].keys()])},
                        {", ".join([f"col_{cleanup_type_name(type_name)}" for type_name in self.columns[i]['col_']])}
                    )
                    VALUES {",".join(values)};
                """
                )
            with ydb.QuerySessionPool(self.driver) as session_pool:
                for query in querys:
                    session_pool.execute_with_retries(query)

    def check_table(self):
        queries = []
        for i in range(1, self.count_rows + 1):
            for numb_table in range(self.count_table):
                queries.append(f"SELECT * FROM {self.table_names[numb_table]} WHERE pk_Int64 = {i}")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query_index = 0
            for query in queries:
                table_index = query_index % self.count_table
                row_value = (query_index // self.count_table) + 1
                
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1
                rows = result_sets[0].rows
                for row in rows:
                    for prefix in self.columns[table_index].keys():
                        for type_name in self.columns[table_index][prefix]:
                            self.assert_type(type_name, row_value, row[f"{prefix}{cleanup_type_name(type_name)}"])
                query_index += 1

    def assert_type(self, data_type: str, values: int, values_from_rows):
        if data_type == "String" or data_type == "Yson":
            assert values_from_rows.decode("utf-8") == self.all_types[data_type](
                values
            ), f"{data_type}, expected {self.all_types[data_type](values)}, received {values_from_rows.decode("utf-8")}"
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
                self.all_types[data_type](values) / 1_000_000
            ), f"{data_type}, expected {datetime.fromtimestamp(self.all_types[data_type](values)/1_000_000)}, received {values_from_rows}"
        elif data_type == "Json" or data_type == "JsonDocument":
            assert str(values_from_rows).replace("'", "\"") == str(
                self.all_types[data_type](values)
            ), f"{data_type}, expected {self.all_types[data_type](values)}, received {values_from_rows}"
        elif data_type.startswith("Decimal("):
            expected_value = self.all_types[data_type](values)
            assert str(values_from_rows) == str(expected_value), f"{data_type}, expected {expected_value}, received {values_from_rows}"
        else:
            assert str(values_from_rows) == str(
                self.all_types[data_type](values)
            ), f"{data_type}, expected {self.all_types[data_type](values)}, received {values_from_rows}"

    def create_table(self):
        if self.store_type == "COLUMN" and min(self.versions) < (25, 1):
            pytest.skip("COLUMN tables are not supported in this version")
        
        pk_columns = []
        for i in range(self.count_table):
            pk_columns.append(
                {
                    "pk_": self.pk_types[i].keys(),
                }
            )

        querys = []
        for i in range(self.count_table):
            if self.store_type == "COLUMN":
                querys.append(
                    create_columnshard_table_sql_request(
                        self.table_names[i],
                        columns=self.columns[i],
                        pk_columns=pk_columns[i],
                        index_columns={},
                        unique="",
                        sync="",
                    )
                )
            else:
                querys.append(
                    create_table_sql_request(
                        self.table_names[i],
                        columns=self.columns[i],
                        pk_columns=pk_columns[i],
                        index_columns={},
                        unique="",
                        sync="",
                    )
                )
        
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in querys:
                session_pool.execute_with_retries(query)

    @pytest.mark.parametrize("store_type", ["ROW", "COLUMN"], indirect=True)
    def test_data_type(self):
        if any("Decimal" in type_name for type_name in self.all_types.keys()) and self.store_type == "COLUMN":
            if (min(self.versions) < (25, 1)):
                pytest.skip("Decimal types are not supported in columnshard in this version")

        self.create_table()

        self.write_data()
        self.check_table()

        self.change_cluster_version()

        self.check_table()
        self.write_data()
        self.check_table()

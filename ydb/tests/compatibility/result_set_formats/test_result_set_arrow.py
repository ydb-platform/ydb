import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.datashard.lib.create_table import create_table_sql_request
from ydb.tests.datashard.lib.types_of_variables import (
    pk_types,
    non_pk_types,
    cleanup_type_name,
    format_sql_value,
    types_not_supported_yet_in_columnshard
)

import pyarrow as pa
from random import randint


primitive_type_to_arrow_type = {
    "Int64": pa.int64(),
    "Uint64": pa.uint64(),
    "Int32": pa.int32(),
    "Uint32": pa.uint32(),
    "Int16": pa.int16(),
    "Uint16": pa.uint16(),
    "Int8": pa.int8(),
    "Uint8": pa.uint8(),
    "Bool": pa.uint8(),
    "Decimal(15,0)": pa.binary(16),
    "Decimal(22,9)": pa.binary(16),
    "Decimal(35,10)": pa.binary(16),
    "DyNumber": pa.string(),
    "String": pa.binary(),
    "Utf8": pa.string(),
    "UUID": pa.binary(16),
    "Date": pa.uint16(),
    "Datetime": pa.uint32(),
    "Timestamp": pa.uint64(),
    "Interval": pa.int64(),
    "Date32": pa.int32(),
    "Datetime64": pa.int64(),
    "Timestamp64": pa.int64(),
    "Interval64": pa.int64(),
    "Float": pa.float32(),
    "Double": pa.float64(),
    "Json": pa.string(),
    "JsonDocument": pa.string(),
    "Yson": pa.binary(),
}


class TestResultSetArrow(RestartToAnotherVersionFixture):
    @pytest.fixture()
    def store_type(self, request):
        return request.param

    @pytest.fixture(autouse=True, scope="function")
    def setup(self, store_type):
        self.store_type = store_type

        if min(self.versions) < (25, 3, 2):
            pytest.skip("Arrow result set format is not supported in <= 25.3.1")

        if min(self.versions) < (26, 1):
            types_not_supported_yet_in_columnshard.add("Bool")

        supported_pk_types = pk_types if store_type == "ROW" else {k: v for k, v in pk_types.items() if k not in types_not_supported_yet_in_columnshard}
        supported_non_pk_types = non_pk_types if store_type == "ROW" else {k: v for k, v in non_pk_types.items() if k not in types_not_supported_yet_in_columnshard}
        self.all_types = {**supported_pk_types, **supported_non_pk_types}

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_arrow_result_set_format": True
                },
            column_shard_config={
                "disabled_on_scheme_shard": False,
            }
        )

    @pytest.mark.parametrize("store_type", ["ROW", "COLUMM"])
    def test_types_mapping(self):
        table_name = "test_arrow"
        rows_count = 500

        self._create_table(table_name)
        self._fill_table(table_name, rows_count)

        result_sets = self._select_table(table_name)
        assert len(result_sets) != 0
        self._validate_response_types(result_sets, rows_count)

        self.change_cluster_version()

        result_sets = self._select_table(table_name)
        assert len(result_sets) != 0
        self._validate_response_types(result_sets, rows_count)

    def _create_table(self, table_name):
        query = create_table_sql_request(
            table_name,
            columns={"pk_": {"Uint64": None}, "col_": self.all_types.keys()},
            pk_columns={"pk_": {"Uint64": None}},
            index_columns={},
            unique="",
            sync="",
            column_table=self.store_type == "COLUMN"
        )

        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(query)

    def _fill_table(self, table_name, rows_count, batch_size=100, offset=0):
        type_names = list(self.all_types.keys())
        columns = ["pk_Uint64"]
        for type_name in type_names:
            columns.append(f"col_{cleanup_type_name(type_name)}")
        columns_str = "(" + ", ".join(columns) + ")"

        values = []
        for i in range(rows_count):
            value = [format_sql_value(i + offset, "Uint64", self.store_type != "COLUMN")]
            for type_name in type_names:
                if i % 3 == 0:
                    value.append(format_sql_value(self.all_types[type_name](randint(0, 127)), type_name))
                else:
                    value.append('NULL')
            values.append("(" + ", ".join(value) + ")")

        with ydb.QuerySessionPool(self.driver) as pool:
            for batch_start in range(0, rows_count, batch_size):
                batch_rows = values[batch_start:batch_start + batch_size]
                if not batch_rows:
                    continue
                query = f"UPSERT INTO {table_name} {columns_str} VALUES {', '.join(batch_rows)};"
                print(query, end='\n\n')
                pool.execute_with_retries(query)

    def _select_table(self, table_name):
        columns = ["pk_Uint64", *[f"col_{cleanup_type_name(type_name)}" for type_name in self.all_types.keys()]]
        query = f"SELECT {', '.join(columns)} FROM {table_name};"
        with ydb.QuerySessionPool(self.driver) as pool:
            result_sets = pool.execute_with_retries(query, result_set_format=ydb.QueryResultSetFormat.ARROW)
        return result_sets

    def _validate_response_types(self, result_sets, rows_count):
        result_rows_count = 0
        for result_set in result_sets:
            assert len(result_set.data) != 0
            assert len(result_set.arrow_format_meta.schema) != 0

            schema: pa.Schema = pa.ipc.read_schema(pa.py_buffer(result_set.arrow_format_meta.schema))
            batch: pa.RecordBatch = pa.ipc.read_record_batch(pa.py_buffer(result_set.data), schema)

            result_rows_count += batch.num_rows
            assert batch.num_columns == len(self.all_types) + 1

            for type_name in self.all_types.keys():
                print(type_name)
                assert schema.field(f"col_{cleanup_type_name(type_name)}").type == primitive_type_to_arrow_type[type_name]

        assert result_rows_count == rows_count

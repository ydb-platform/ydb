# -*- coding: utf-8 -*-
import pytest
import math
from datetime import timedelta, datetime

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, cleanup_type_name, type_to_literal_lambda


class TestDefaultColumns(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.table_name = "test_default_columns"
        self.partitions_count = 64
        self.rows_per_partition = 10
        self.all_types = {**pk_types, **non_pk_types}

        if min(self.versions) < (25, 3):
            pytest.skip("Only available since 25-3")

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_add_colums_with_defaults": True,
            }
        )

    # First way: create table with default columns
    def test_create_table(self):
        self.create_table(with_columns=True)

        self.fill_table()
        self.check_table(with_columns=True)

        self.change_cluster_version()

        self.check_table(with_columns=True)
        self.fill_table()
        self.check_table(with_columns=True)

    # Second way: add columns with default values
    def test_add_columns(self):
        self.create_table(with_columns=False)

        self.fill_table()

        self.check_table(with_columns=False)
        self.add_columns()
        self.check_table(with_columns=True)

        self.change_cluster_version()

        self.check_table(with_columns=True)
        self.drop_columns()
        self.check_table(with_columns=False)

        self.fill_table()

        self.add_columns()
        self.check_table(with_columns=True)

    def assert_type(self, data_type: str, values: int, values_from_rows):
        result = self.all_types[data_type](values)
        if data_type == "String" or data_type == "Yson":
            assert values_from_rows.decode("utf-8") == result, f"{data_type}, expected {result}, received {values_from_rows.decode('utf-8')}"
        elif data_type == "Float" or data_type == "DyNumber":
            assert math.isclose(float(values_from_rows), float(result), rel_tol=1e-3), f"{data_type}, expected {result}, received {values_from_rows}"
        elif data_type == "Interval" or data_type == "Interval64":
            assert values_from_rows == timedelta(days=result), f"{data_type}, expected {timedelta(days=result)}, received {values_from_rows}"
        elif data_type == "Timestamp" or data_type == "Timestamp64":
            assert values_from_rows == datetime.fromtimestamp(result / 1_000_000), f"{data_type}, expected {datetime.fromtimestamp(result / 1_000_000)}, received {values_from_rows}"
        elif data_type == "Json" or data_type == "JsonDocument":
            assert str(values_from_rows).replace("'", "\"") == str(result), f"{data_type}, expected {result}, received {values_from_rows}"
        else:
            assert str(values_from_rows) == str(result), f"{data_type}, expected {result}, received {values_from_rows}"

    def create_table(self, with_columns):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(f"""
                CREATE TABLE `{self.table_name}` (
                    K1 UInt64 NOT NULL,
                    K2 UInt64 DEFAULT 0,
                    {" ".join([f"V_{cleanup_type_name(name)} {name} NOT NULL DEFAULT {type_to_literal_lambda[name](i)}," for i, name in enumerate(self.all_types.keys())]) if with_columns else ""}
                    PRIMARY KEY (K1, K2)
                ) WITH (
                    UNIFORM_PARTITIONS = {self.partitions_count}
                )
            """)

    def fill_table(self):
        last_partition_num = 2 ** 64 - self.rows_per_partition - 1
        queries = []

        for partition_num in range(0, last_partition_num, last_partition_num // (self.partitions_count - 1)):
            query = f"""
                UPSERT INTO `{self.table_name}` (K1)
                VALUES {", ".join([f"({partition_num + i})" for i in range(self.rows_per_partition)])};
            """
            queries.append(query)

        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in queries:
                session_pool.execute_with_retries(query)

    def check_table(self, with_columns):
        last_partition_num = 2 ** 64 - self.rows_per_partition - 1
        queries = []

        projection = ", ".join([f"V_{cleanup_type_name(name)}" for name in self.all_types.keys()]) if with_columns else ""

        for partition_num in range(0, last_partition_num, last_partition_num // (self.partitions_count - 1)):
            for i in range(self.rows_per_partition):
                query = f"""
                    SELECT {projection if projection else "*"} FROM `{self.table_name}` WHERE K1 = {partition_num + i};
                """
                queries.append(query)

        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in queries:
                result_sets = session_pool.execute_with_retries(query)

                rows = []
                for result_set in result_sets:
                    rows.extend(result_set.rows)

                assert len(rows) == 1
                for i, type_name in enumerate(self.all_types.keys()):
                    if with_columns:
                        self.assert_type(type_name, i, rows[0][f"V_{cleanup_type_name(type_name)}"])
                    else:
                        assert f"V_{cleanup_type_name(type_name)}" not in rows[0], f"Column V_{cleanup_type_name(type_name)} should not be in the table"

    def add_columns(self):
        queries = []
        for i, name in enumerate(self.all_types.keys()):
            query = f"""
                ALTER TABLE `{self.table_name}`
                ADD COLUMN V_{cleanup_type_name(name)} {name} NOT NULL DEFAULT {type_to_literal_lambda[name](i)};
            """
            queries.append(query)
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in queries:
                session_pool.execute_with_retries(query)

    def drop_columns(self):
        queries = []
        for name in self.all_types.keys():
            query = f"""
                ALTER TABLE `{self.table_name}`
                DROP COLUMN V_{cleanup_type_name(name)};
            """
            queries.append(query)
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in queries:
                session_pool.execute_with_retries(query)

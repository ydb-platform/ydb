# -*- coding: utf-8 -*-
import pytest
import yatest
import os
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb

from decimal import Decimal


class TestCompatibility(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        output_path = yatest.common.test_output_path()
        self.output_f = open(os.path.join(output_path, "out.log"), "w")
        yield from self.setup_cluster(
            extra_feature_flags={
                # "enable_table_datetime64": True # uncomment for 64 datetime in tpc-h/tpc-ds
                },
            column_shard_config={
                'disabled_on_scheme_shard': False,
            }
        )

    def execute_scan_query(self, query_body):
        query = ydb.ScanQuery(query_body, {})
        it = self.driver.table_client.scan_query(query)
        result_set = []

        try:
            while True:
                result = next(it)
                result_set.extend(result.result_set.rows)
        except StopIteration:
            pass

        return result_set

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_simple(self, store_type):
        def upsert_and_check_sum(self, iteration_count=1, start_index=0):
            id_ = start_index

            upsert_count = 200
            iteration_count = iteration_count
            for i in range(iteration_count):
                rows = []
                for j in range(upsert_count):
                    row = {}
                    row["id"] = id_
                    row["value"] = 1
                    row["payload"] = "DEADBEEF" * 1024 * 16  # 128 kb
                    row["income"] = Decimal("123.001").quantize(Decimal('0.000000000'))

                    rows.append(row)
                    id_ += 1

                column_types = ydb.BulkUpsertColumns()
                column_types.add_column("id", ydb.PrimitiveType.Uint64)
                column_types.add_column("value", ydb.PrimitiveType.Uint64)
                column_types.add_column("payload", ydb.PrimitiveType.Utf8)
                column_types.add_column("income", ydb.DecimalType())
                self.driver.table_client.bulk_upsert(
                    "Root/sample_table", rows, column_types
                )

            query_body = "SELECT SUM(value) as sum_value from `sample_table`"
            assert self.execute_scan_query(query_body)[0]['sum_value'] == upsert_count * iteration_count + start_index

        def create_table(self, store_type):
            with ydb.SessionPool(self.driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(
                        """create table `sample_table` (
                            id Uint64 NOT NULL, value Uint64,
                            payload Utf8, income Decimal(22,9),
                            PRIMARY KEY(id)
                            ) WITH (
                            STORE = {store_type},
                            AUTO_PARTITIONING_BY_SIZE = ENABLED,
                            AUTO_PARTITIONING_PARTITION_SIZE_MB = 1);""".format(store_type=store_type.upper())
                    )

        create_table(self, store_type)
        upsert_and_check_sum(self)
        self.change_cluster_version()
        assert self.execute_scan_query('select count(*) as row_count from `sample_table`')[0]['row_count'] == 200, 'Expected 200 rows after update version'
        upsert_and_check_sum(self, iteration_count=2, start_index=100)
        assert self.execute_scan_query('select count(*) as row_count from `sample_table`')[0]['row_count'] == 500, 'Expected 500 rows: update 100-200 rows and added 300 rows'

    @pytest.mark.parametrize("store_type, date_args", [
        pytest.param("row",    ["--datetime-types=dt32"], id="row"),
        pytest.param("column", ["--datetime-types=dt32"], id="column"),
        pytest.param("column", ["--datetime-types=dt64"], id="column-date64")
    ])
    def test_tpch1(self, store_type, date_args):
        result_json_path = os.path.join(yatest.common.test_output_path(), "result.json")
        query_output_path = os.path.join(yatest.common.test_output_path(), "query_output.json")
        init_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "init",
            "--store={}".format(store_type),
            "--partition-size=25",
        ] + date_args  # use 32 bit dates instead of 64 (not supported in 24-4)]

        import_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "import",
            "generator",
            "--scale=0.1",
        ]
        run_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "run",
            "--scale=0.1",
            "--check-canonical",
            "--retries",
            "5",  # in row tables we have to retry query by design
            "--json",
            result_json_path,
            "--output",
            query_output_path,
        ]
        clean_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "clean"
        ]

        yatest.common.execute(init_command, wait=True, stdout=self.output_f)
        yatest.common.execute(import_command, wait=True, stdout=self.output_f)
        yatest.common.execute(run_command, wait=True, stdout=self.output_f)
        self.change_cluster_version()
        yatest.common.execute(run_command, wait=True, stdout=self.output_f)
        yatest.common.execute(clean_command, wait=True, stdout=self.output_f)

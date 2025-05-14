# -*- coding: utf-8 -*-
import pytest
import yatest
import os
import time
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb

from decimal import Decimal


last_stable_binary_path = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-last-stable")
current_binary_path = kikimr_driver_path()

all_binary_combinations = [
    [[last_stable_binary_path], [current_binary_path]],
    [[last_stable_binary_path], [last_stable_binary_path, current_binary_path]],
    [[current_binary_path], [last_stable_binary_path]],
    [[current_binary_path], [current_binary_path]],
]
all_binary_combinations_ids = [
    "last_stable_to_current",
    "last_stable_to_current_mixed",
    "current_to_last_stable",
    "current_to_current",
]


class TestCompatibility(object):
    @pytest.fixture(autouse=True, params=all_binary_combinations, ids=all_binary_combinations_ids)
    def setup(self, request):
        self.all_binary_paths = request.param
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=self.all_binary_paths[0],
            use_in_memory_pdisks=False,

            extra_feature_flags={
                "suppress_compatibility_check": True,
                # "enable_table_datetime64": True # uncomment for 64 datetime in tpc-h/tpc-ds
                },
            column_shard_config={
                'disabled_on_scheme_shard': False,
            },
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)
        output_path = yatest.common.test_output_path()
        self.output_f = open(os.path.join(output_path, "out.log"), "w")

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        self.driver.wait()
        yield
        self.cluster.stop()

    def change_cluster_version(self, new_binary_paths):
        self.config.set_binary_paths(new_binary_paths)
        self.cluster.update_configurator_and_restart(self.config)
        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        self.driver.wait()
        # TODO: remove sleep
        # without sleep there are errors like
        # ydb.issues.Unavailable: message: "Failed to resolve tablet: 72075186224037909 after several retries." severity: 1 (server_code: 400050)
        time.sleep(60)

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
        self.change_cluster_version(self.all_binary_paths[1])
        assert self.execute_scan_query('select count(*) as row_count from `sample_table`')[0]['row_count'] == 200, 'Expected 200 rows after update version'
        upsert_and_check_sum(self, iteration_count=2, start_index=100)
        assert self.execute_scan_query('select count(*) as row_count from `sample_table`')[0]['row_count'] == 500, 'Expected 500 rows: update 100-200 rows and added 300 rows'

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_tpch1(self, store_type):
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
            "--datetime",  # use 32 bit dates instead of 64 (not supported in 24-4)
            "--partition-size=25",
        ]
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
            "--scale=1",
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
            "--scale=1",
            "--exclude",
            "17",  # not working for row tables
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
        self.change_cluster_version(self.all_binary_paths[1])
        yatest.common.execute(run_command, wait=True, stdout=self.output_f)
        yatest.common.execute(clean_command, wait=True, stdout=self.output_f)

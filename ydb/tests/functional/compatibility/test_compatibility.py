# -*- coding: utf-8 -*-
import time
import pytest
import logging
import yatest
import os
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb

from decimal import Decimal


last_stable_binary_path = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-last-stable")
prev_stable_binary_path = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-prev-stable")
current_binary_path = kikimr_driver_path()

all_binary_combinations = [
    [last_stable_binary_path, current_binary_path],
    [prev_stable_binary_path, current_binary_path],
    [prev_stable_binary_path, last_stable_binary_path],
    [last_stable_binary_path, prev_stable_binary_path],
]
all_binary_combinations_ids = [
    "last_stable_to_current",
    "prev_stable_to_current",
    "prev_stable_to_last_stable",
    "last_stable_to_prev_stable"
    ]

logger = logging.getLogger(__name__)

class TestCompatibility(object):
    @pytest.fixture(autouse=True, params=all_binary_combinations, ids=all_binary_combinations_ids)
    def setup(self, request):
        binary_paths = request.param
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=binary_paths,
            use_in_memory_pdisks=False,
            # uncomment for 64 datetime in tpc-h/tpc-ds
            # extra_feature_flags={"enable_table_datetime64": True},

            column_shard_config={
                'disabled_on_scheme_shard': False,
            },
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start(version_id=1)
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
    def read_update_data(self, iteration_count=1):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        self.log_node_versions()
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                id_ = 0

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
                query = ydb.ScanQuery(query_body, {})
                it = self.driver.table_client.scan_query(query)
                result_set = []

                while True:
                    try:
                        result = next(it)
                        result_set = result_set + result.result_set.rows
                    except StopIteration:
                        break
                
                
                for row in result_set:
                    print(" ".join([str(x) for x in list(row.values())]))

                assert len(result_set) == 1
                assert len(result_set[0]) == 1
                result = list(result_set)
                assert len(result) == 1
                assert result[0]['sum_value'] == upsert_count * iteration_count
    def create_table_column(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    "create table `sample_table` (id Uint64 NOT NULL, value Uint64, payload Utf8, income Decimal(22,9), PRIMARY KEY(id)) WITH (STORE = COLUMN,AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 1);"
                )
    def create_table_row(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    "create table `sample_table` (id Uint64, value Uint64, payload Utf8, income Decimal(22,9), PRIMARY KEY(id)) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 1);"
                    )
    def log_node_versions(self):
        for node_id, node in enumerate(self.cluster.nodes.values()):
            node.get_config_version()
            get_version_command = [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "--endpoint",
                self.endpoint,
                "--database=/Root",
                "yql",
                "--script",
                f'select version() as node_{node_id}_version'
            ]
            yatest.common.execute(get_version_command, wait=True, stdout=self.output_f, stderr=self.output_f)
    def exec_query(self, query: str):
        command = [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "-e",
                 self.endpoint,
                "-d"
                "/Root",
                "yql",
                "--script",
                f"{query}"
            ]
        yatest.common.execute(command, wait=True, stdout=self.output_f, stderr=self.output_f)
        
    def change_cluster_version(self,new_version='next_version'):
        version_id = None
        if new_version == 'next_version':
            version_id = 2
        elif new_version == 'combined':
            version_id = None
            
        self.cluster.change_node_version(version_id=version_id)
        time.sleep(180)
        self.log_node_versions()
    def log_database_scheme(self):
        for node_id, node in enumerate(self.cluster.nodes.values()):
            node.get_config_version()
            get_version_command = [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "-e",
                "grpc://localhost:%d" % node.grpc_port,
                "-d"
                "/Root",
                "scheme",
                "ls"
 
            ]
            yatest.common.execute(get_version_command, wait=True, stdout=self.output_f, stderr=self.output_f)
    def log_nodes_version(self):
        for node_id, node in enumerate(self.cluster.nodes.values()):
            node.get_config_version()
            get_version_command = [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "-e",
                "grpc://localhost:%d" % node.grpc_port,
                "-d"
                "/Root",
                "scheme",
                "ls"
 
            ]
            yatest.common.execute(get_version_command, wait=True, stdout=self.output_f, stderr=self.output_f)
    @pytest.mark.parametrize("version_change_to", ['combined','next_version'])
    def test_simple(self,version_change_to):
        self.create_table_row()
        self.read_update_data()
        self.exec_query('select count(*) from `sample_table`')
        time.sleep(10)
        self.log_nodes_version()
        self.exec_query('select count(*) from `sample_table`')
        
        self.change_cluster_version(new_version=version_change_to)
        
        self.log_database_scheme()
        self.exec_query('select count(*) from `sample_table`')
        self.read_update_data(iteration_count=2)
        self.exec_query('select count(*) from `sample_table`')
    @pytest.mark.parametrize("version_change_to", ['combined','next_version'])
    def test_simple_column(self,version_change_to):
        self.create_table_column()
        self.read_update_data()
        self.exec_query('select count(*) from `sample_table`')
        time.sleep(10)
        self.log_nodes_version()
        self.exec_query('select count(*) from `sample_table`')
        
        self.change_cluster_version(new_version=version_change_to)
        
        self.log_database_scheme()
        self.exec_query('select count(*) from `sample_table`')
        self.read_update_data(iteration_count=2)
        self.exec_query('select count(*) from `sample_table`')

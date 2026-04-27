# -*- coding: utf-8 -*-
import os
import pytest
import library.python.port_manager
import yatest

from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels


class TestYdbTopicWorkload(StressFixture):
    def _create_serverless_databases(self):
        timeout_seconds = 120
        self.shared_database_name = "/Root/shared_db"

        self.cluster.remove_database(
            self.shared_database_name,
            timeout_seconds=timeout_seconds
        )

        self.cluster.create_hostel_database(
            self.shared_database_name,
            storage_pool_units_count={
                'hdd': 1
            },
            timeout_seconds=timeout_seconds
        )

        self.cluster.register_and_start_slots(self.shared_database_name, count=1)
        self.cluster.wait_tenant_up(self.shared_database_name)

        self.serverless_database_name = "/Root/serverless_db"

        self.cluster.remove_database(self.serverless_database_name,
                                     timeout_seconds=timeout_seconds)

        self.cluster.create_serverless_database(
            self.serverless_database_name,
            hostel_db=self.shared_database_name,
            timeout_seconds=timeout_seconds,
            schema_quotas=None,
            disk_quotas=None,
            attributes={
                "cloud_id": "CLOUD_ID_VAL",
                "folder_id": "FOLDER_ID_VAL",
                "database_id": "DATABASE_ID_VAL",
            },
        )

    def get_kafka_api_ports(self):
        ports = []
        for node in self.cluster.nodes.values():
            ports.append(node.get_kafka_api_port())
        return ports

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        port_manager = library.python.port_manager.PortManager()
        self.kafka_api_port = port_manager.get_port()
        cluster_generator = self.setup_cluster(
            kafka_api_port=self.kafka_api_port,
            extra_feature_flags=[
                "enable_kafka_native_balancing",
                "enable_kafka_transactions",
                "enable_topic_compactification_by_key",
                "enable_serverless_exclusive_dynamic_nodes"
            ],
            erasure=Erasure.NONE,
            nodes=1,
            enable_metering=True,
            additional_log_configs={
                'TX_PROXY': LogLevels.DEBUG,
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_WORKER': LogLevels.DEBUG,
                'KQP_GATEWAY': LogLevels.DEBUG,
                'GRPC_PROXY': LogLevels.TRACE,
                'KQP_YQL': LogLevels.DEBUG,
                'TX_DATASHARD': LogLevels.DEBUG,
                'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
                'SCHEMESHARD_DESCRIBE': LogLevels.DEBUG,

                'SCHEME_BOARD_POPULATOR': LogLevels.DEBUG,

                'SCHEME_BOARD_REPLICA': LogLevels.ERROR,
                'SCHEME_BOARD_SUBSCRIBER': LogLevels.ERROR,
                'TX_PROXY_SCHEME_CACHE': LogLevels.ERROR,

                'CMS': LogLevels.DEBUG,
                'CMS_TENANTS': LogLevels.DEBUG,
                'DISCOVERY': LogLevels.TRACE,
                'GRPC_SERVER': LogLevels.DEBUG,
                'KAFKA_PROXY': LogLevels.DEBUG,
            },
            datashard_config={
                'keep_snapshot_timeout': 5000,
                'stats_report_interval_seconds': 1,
            },
            column_shard_config={
                'disabled_on_scheme_shard': False,
                'max_read_staleness_ms': 200
            },
            table_service_config={
            },
        )
        next(cluster_generator)
        try:
            self._create_serverless_databases()
            yield
        finally:
            try:
                next(cluster_generator)
            except StopIteration:
                pass

    def test(self):
        kafka_api_ports = self.get_kafka_api_ports()
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.serverless_database_name,
            "--bootstrap", f"http://localhost:{kafka_api_ports[-1]}",
            "--source-path", "test-topic",
            "--target-path", "target-topic",
            "--consumer", "workload-consumer-0",
            "--num-workers", "2",
            "--duration", "120"
        ])

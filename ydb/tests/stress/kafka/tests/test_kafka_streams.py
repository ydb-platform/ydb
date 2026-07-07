# -*- coding: utf-8 -*-
import os
import pytest
import library.python.port_manager
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTopicWorkload(StressFixture):
    tenant_database = "/Root/db1"

    def _create_tenant_database(self):
        timeout_seconds = 120

        self.cluster.remove_database(
            self.tenant_database,
            timeout_seconds=timeout_seconds,
        )
        self.cluster.create_database(
            self.tenant_database,
            storage_pool_units_count={
                "hdd": 1,
            },
            timeout_seconds=timeout_seconds,
        )
        slots = self.cluster.register_and_start_slots(self.tenant_database, count=1)
        self.cluster.wait_tenant_up(self.tenant_database)
        return slots

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        port_manager = library.python.port_manager.PortManager()
        self.kafka_api_port = port_manager.get_port()
        yield from self.setup_cluster(
            kafka_api_port=self.kafka_api_port,
            extra_feature_flags=[
                "enable_kafka_native_balancing",
                "enable_kafka_transactions",
                "enable_topic_compactification_by_key"
            ],
        )

    def get_kafka_api_ports(self):
        ports = []
        for node in self.cluster.nodes.values():
            ports.append(node.get_kafka_api_port())
        return ports

    def test(self):
        kafka_api_ports = self.get_kafka_api_ports()
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--bootstrap", f"http://localhost:{kafka_api_ports[-1]}",
            "--source-path", "test-topic",
            "--target-path", "target-topic",
            "--consumer", "workload-consumer-0",
            "--num-workers", "2",
            "--duration", "280"
        ])

    def test_tenant_database(self):
        tenant_slots = self._create_tenant_database()

        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.tenant_database,
            "--bootstrap", f"http://localhost:{tenant_slots[-1].get_kafka_api_port()}",
            "--source-path", "test-topic",
            "--target-path", "target-topic",
            "--consumer", "workload-consumer-0",
            "--num-workers", "2",
            "--duration", "280"
        ])

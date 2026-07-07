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
        self.cluster.register_and_start_slots(self.tenant_database, count=1)
        self.cluster.wait_tenant_up(self.tenant_database)

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

    def get_kafka_api_port(self, database):
        tenant_slots = [
            slot for slot in self.cluster.slots.values()
            if getattr(slot, "_tenant_affiliation", None) == database
        ]
        if tenant_slots:
            return tenant_slots[-1].get_kafka_api_port()

        if database == self.database:
            return self.get_kafka_api_ports()[-1]

        raise RuntimeError(f"Cannot find Kafka proxy port for database {database}")

    def run_workload(self, database):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", database,
            "--bootstrap", f"http://localhost:{self.get_kafka_api_port(database)}",
            "--source-path", "test-topic",
            "--target-path", "target-topic",
            "--consumer", "workload-consumer-0",
            "--num-workers", "2",
            "--duration", "280"
        ])

    def test(self):
        self.run_workload(self.database)

    def test_tenant_database(self):
        self._create_tenant_database()

        self.run_workload(self.tenant_database)

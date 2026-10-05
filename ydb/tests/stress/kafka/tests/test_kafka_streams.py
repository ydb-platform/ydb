# -*- coding: utf-8 -*-
import os
import pytest
import library.python.port_manager
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTopicWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, request):
        port_manager = library.python.port_manager.PortManager()
        self.kafka_api_port = port_manager.get_port()
        extra_feature_flags = [
            "enable_kafka_native_balancing",
            "enable_kafka_transactions",
            "enable_topic_compactification_by_key",
        ]
        if request.node.name in ("test_batched_source", "test_direct_batched_produce"):
            extra_feature_flags.extend([
                "enable_topic_write_offset_delta_in_keys",
                "enable_topic_messages_batching",
            ])
        yield from self.setup_cluster(
            kafka_api_port=self.kafka_api_port,
            extra_feature_flags=extra_feature_flags,
            kafka_auto_create_topics=True,
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

    def run_workload(self, database, suffix="", extra_args=None):
        cmd = [
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", database,
            "--bootstrap", f"http://localhost:{self.get_kafka_api_port(database)}",
            "--source-path", f"test-topic{suffix}",
            "--target-path", f"target-topic{suffix}",
            "--consumer", "workload-consumer-0",
            "--num-workers", "2",
            "--duration", "120"
        ]
        if extra_args:
            cmd.extend(extra_args)
        yatest.common.execute(cmd)

    def test(self):
        self.run_workload(self.database)

    def test_batched_source(self):
        self.run_workload(
            self.database,
            suffix="-batch",
            extra_args=[
                "--num-workers", "1",
                "--source-writer", "kafka",
            ],
        )

    def test_direct_batched_produce(self):
        self.run_workload(
            self.database,
            suffix="-direct-batch",
            extra_args=[
                "--source-writer", "kafka-direct",
            ],
        )

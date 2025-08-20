# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTopicWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        port_manager = yatest.common.network.PortManager()
        self.kafka_api_port = port_manager.get_port()
        yield from self.setup_cluster(
            kafka_api_port=self.kafka_api_port,
            extra_feature_flags=[
                "enable_kafka_native_balancing",
                "enable_kafka_transactions",
                "enable_topic_compactification_by_key"
            ],
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--bootstrap", f"http://localhost:{self.kafka_api_port}",
            "--source-path", "test-topic",
            "--target-path", "target-topic",
            "--consumer", "workload-consumer-0",
            "--num-workers", "2",
            "--duration", "120"
        ])

# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTopicWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            kafka_api_port=9092,
            extra_feature_flags={
                "enable_kafka_native_balancing": True,
                "enable_kafka_transactions": True
            }
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--bootstrap", "http://localhost:9092",
            "--sourcePath", "test-topic",
            "--targetPath", "target-topic",
            "--consumer", "workload-consumer-0",
            "--numWorkers", "2"
        ])

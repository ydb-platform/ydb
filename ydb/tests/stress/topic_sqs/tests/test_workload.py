# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTopicSqsWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_topic_message_level_parallelism": True
            },
            http_proxy_config={
                "enabled": True,
                "sqs_topic_enabled": True,
                "yandex_cloud_service_region": ["ru-test"],
            },
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
            "--sqs-endpoint", self.http_proxy_endpoint + '/Root',
        ])

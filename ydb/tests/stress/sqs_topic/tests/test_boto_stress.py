# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.stress.sqs_topic.workload.boto_stress import (
    BotoStressWorkload,
    DEFAULT_DURATION_SECONDS,
    DEFAULT_WORKERS,
)


class TestSqsTopicBotoStress(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_topic_message_level_parallelism": True,
            },
            http_proxy_config={
                "enabled": True,
                "sqs_topic_enabled": True,
                "ymq_enabled": False,
                "yandex_cloud_service_region": ["ru-central1", "ru-test"],
            },
        )

    def test_boto_write_read_commit_stress(self):
        with BotoStressWorkload(
            endpoint=self.endpoint,
            database=self.database,
            duration=DEFAULT_DURATION_SECONDS,
            sqs_endpoint=self.http_proxy_endpoint + self.database,
            workers=DEFAULT_WORKERS,
        ) as workload:
            workload.run()

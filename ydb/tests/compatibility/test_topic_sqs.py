# -*- coding: utf-8 -*-
import concurrent.futures
import pytest
import time

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture, string_version_to_tuple, logger
from ydb.tests.stress.topic_sqs.workload import Workload


MIN_SUPPORTED_VERSION = "stable-26-1-2"


def skip_if_unsupported(versions):
    if min(versions) < string_version_to_tuple(MIN_SUPPORTED_VERSION):
        pytest.skip(f"Only available since {MIN_SUPPORTED_VERSION}")


class TestTopicSqsRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)
        #
        # Setup cluster
        #
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

    def test_write_and_read(self):
        logger.info(f"endpoint: {self.http_proxy_endpoint}")

        duration = 30

        # endpoint, database, duration, sqs_endpoint
        utils = Workload(self.endpoint, self.database, duration, self.http_proxy_endpoint + "/Root")

        utils.create_topic()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            logger.info("Starting workload")
            runners = [
                executor.submit(utils.write_to_topic),
                executor.submit(utils.read_from_topic),
            ]

            for _ in self.roll():
                time.sleep(1)

            logger.info("Waiting for workload task")
            for nn, runner in enumerate(concurrent.futures.as_completed(runners)):
                try:
                    runner.result()
                    logger.info("Workload task #%d completed, stdout: %s", nn, runner.result().stdout)
                except Exception:
                    logger.exception("Workload task #%d failed, stdout: %s", nn, runner.result().stdout)
                    raise

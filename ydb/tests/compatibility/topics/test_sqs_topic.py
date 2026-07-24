# -*- coding: utf-8 -*-
import pytest
import time

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture, string_version_to_tuple, logger
from ydb.tests.stress.sqs_topic.workload import Workload


MIN_SUPPORTED_VERSION = "stable-26-2"
ITERATION_DURATION_SECONDS = 10
COUNT_GROWTH_TIMEOUT = 120


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
            extra_feature_flags=[
                "enable_topic_message_level_parallelism",
            ],
            http_proxy_config={
                "enabled": True,
                "sqs_topic_enabled": True,
                "yandex_cloud_service_region": ["ru-test"],
            },
        )

    def _wait_count_growth(self, get_current, prev, what, timeout=COUNT_GROWTH_TIMEOUT, on_stall=None):
        deadline = time.time() + timeout
        last = prev
        last_error = None
        while time.time() < deadline:
            try:
                current = get_current()
                last = current
                last_error = None
                if current > prev:
                    logger.info("%s grew: %s -> %s", what, prev, current)
                    return current
                logger.info("%s has not grown yet: prev=%s current=%s", what, prev, current)
            except Exception as e:
                last_error = e
                logger.warning("Failed to get %s: %r", what, e)
            if on_stall is not None:
                on_stall()
            else:
                time.sleep(1)
        raise AssertionError(
            f"{what} did not grow within {timeout}s: prev={prev}, last={last}, error={last_error!r}"
        )

    def test_write_and_read(self):
        logger.info(f"endpoint: {self.http_proxy_endpoint}")

        utils = Workload(
            self.endpoint,
            self.database_path,
            ITERATION_DURATION_SECONDS,
            self.http_proxy_endpoint + self.database_path,
        )

        with utils:
            # keep_messages_order=False: otherwise committed offset stalls mid-rolling
            # once a gap appears in the shared consumer contig.
            utils.create_topics(keep_messages_order=False)

            prev_written = 0
            prev_committed = 0

            for iteration, _ in enumerate(self.roll()):
                logger.info("Running SQS workload after roll iteration #%d", iteration)
                utils.endpoint = self.endpoint
                utils.sqs_endpoint = self.http_proxy_endpoint + self.database_path

                # Write first, then read: verifies each side independently and avoids
                # reader starvation / connection flakes during mixed-version rolling.
                utils.write_to_topic()
                prev_written = self._wait_count_growth(
                    lambda: utils.get_written_messages_count(self.driver),
                    prev_written,
                    "written messages count",
                )

                prev_committed = self._wait_count_growth(
                    lambda: utils.get_committed_messages_count(self.driver),
                    prev_committed,
                    "committed messages count",
                    on_stall=utils.read_from_topic,
                )

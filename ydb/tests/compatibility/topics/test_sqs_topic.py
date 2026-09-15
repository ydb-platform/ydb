# -*- coding: utf-8 -*-
import pytest
import time

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture, string_version_to_tuple, logger
from ydb.tests.stress.sqs_topic.workload import Workload


MIN_SUPPORTED_VERSION = "stable-26-2"
ITERATION_DURATION_SECONDS = 10
COUNT_GROWTH_TIMEOUT = 120
# Few workers: a large inflight tail becomes a contig gap on the next roll().
WRITE_WORKERS = 2
READ_WORKERS = 2


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

    def _wait_count(self, get_current, done, what, timeout=COUNT_GROWTH_TIMEOUT, on_stall=None):
        deadline = time.time() + timeout
        last = None
        last_error = None
        while time.time() < deadline:
            try:
                current = get_current()
                last = current
                last_error = None
                if done(current):
                    logger.info("%s: %s", what, current)
                    return current
                logger.info("%s not reached yet: current=%s", what, current)
            except Exception as e:
                last_error = e
                logger.warning("Failed to get %s: %r", what, e)
            if on_stall is not None:
                on_stall()
            else:
                time.sleep(1)
        raise AssertionError(
            f"{what} was not reached within {timeout}s: last={last}, error={last_error!r}"
        )

    def test_write_and_read(self):
        logger.info(f"endpoint: {self.http_proxy_endpoint}")

        utils = Workload(
            self.endpoint,
            self.database_path,
            ITERATION_DURATION_SECONDS,
            self.http_proxy_endpoint + self.database_path,
            write_workers=WRITE_WORKERS,
            read_workers=READ_WORKERS,
        )

        with utils:
            # keep_messages_order=False: otherwise a message-group lock from a
            # killed node can block later reads of that group during rolling.
            utils.create_topics(keep_messages_order=False)

            prev_written = 0

            for iteration, _ in enumerate(self.roll()):
                logger.info("Running SQS workload after roll iteration #%d", iteration)
                utils.endpoint = self.endpoint
                utils.sqs_endpoint = self.http_proxy_endpoint + self.database_path

                # Write first, then drain: committed_offset is the MLP contig
                # watermark, so a leftover gap stalls the next mixed-version step.
                utils.write_to_topic()
                written = self._wait_count(
                    lambda: utils.get_written_messages_count(self.driver),
                    lambda current: current > prev_written,
                    f"written messages count > {prev_written}",
                )
                self._wait_count(
                    lambda: utils.get_committed_messages_count(self.driver),
                    lambda current: current >= written,
                    f"committed messages count >= {written}",
                    on_stall=utils.read_from_topic,
                )
                prev_written = written

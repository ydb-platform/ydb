# -*- coding: utf-8 -*-
import concurrent.futures
import logging
import os
import pytest
import uuid
import yatest

from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RollingUpgradeAndDowngradeFixture, RestartToAnotherVersionFixture, string_version_to_tuple


class Workload:
    def __init__(self, driver, endpoint):
        self.driver = driver
        self.endpoint = endpoint
        self.id = f"{uuid.uuid1()}".replace("-", "_")
        self.topic_name = f"source_topic_{self.id}"
        self.message_count = 0
        self.processed_message_count = 0
        self.consumers = 1
        self.restart_interval = 10

    def get_command(self, subcmds: list[str]) -> list[str]:
        return (
            [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "--endpoint", self.endpoint,
                "--database=/Root",
                "workload",
                "topic",
            ]
            + subcmds
            + ["--topic", self.topic_name]
        )

    def create_topic(self):
        subcmds = [
            'init',
            '--consumers', str(self.consumers),
            '--partitions', '4',
            '--cleanup-policy-compact',
        ]
        yatest.common.execute(
            self.get_command(subcmds=subcmds)
        )

    def drop_topic(self):
        subcmds = [
            'clean',
        ]
        yatest.common.execute(
            self.get_command(subcmds=subcmds)
        )

    def write_to_topic(self, duration, message_rate, message_size, keys_count, key_prefix, producers):
        subcmds = [
            'run',
            'write',
            '--seconds', str(duration),
            '--message-rate', str(message_rate),
            '--message-size', str(message_size),
            '--key-count', str(keys_count),
            '--key-prefix', str(key_prefix),
            '--threads', str(producers),
            '--warmup', '0',
        ]
        yatest.common.execute(
            self.get_command(subcmds=subcmds)
        )

    def read_from_topic(self, duration):
        subcmds = [
            'run',
            'read',
            '--seconds', str(duration),
            '--consumers', str(self.consumers),
            '--no-commit',
        ]
        yatest.common.execute(
            self.get_command(subcmds=subcmds)
        )

    def run_stress_test(self, duration):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            logging.info("Starting workload")
            readers = [
                executor.submit(self.read_from_topic, duration=duration),
            ]
            writers = [
                executor.submit(self.write_to_topic, duration=duration, message_rate=3000, message_size=45, keys_count=10, key_prefix="small_record", producers=1),
                executor.submit(self.write_to_topic, duration=duration, message_rate=300, message_size=450, keys_count=10, key_prefix="medium_record", producers=1),
                executor.submit(self.write_to_topic, duration=duration, message_rate=1, message_size=100000, keys_count=10, key_prefix="big_record", producers=1),
            ]
            runners = readers + writers
            logging.info("Waiting for workload task")
            for nn, runner in enumerate(concurrent.futures.as_completed(runners)):
                try:
                    runner.result()
                    logging.info("Workload task #%d completed", nn)
                except Exception:
                    logging.exception("Workload task #%d failed", nn)
            logging.info("Checking results")
            for runner in runners:
                runner.result()


MIN_SUPPORTED_VERSION = "stable-25-1-4"


def skip_if_unsupported(versions):
    if min(versions) < string_version_to_tuple(MIN_SUPPORTED_VERSION):
        pytest.skip(f"Only available since {MIN_SUPPORTED_VERSION}")


class TestKafkaTopicMixedClusterFixture(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)
        yield from self.setup_cluster()

    def test_workload(self):
        utils = Workload(self.driver, self.endpoint)

        utils.create_topic()

        utils.run_stress_test(duration=20)

        utils.drop_topic()


@pytest.mark.skip(reason="Redundant")
class TestKafkaTopicRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)
        yield from self.setup_cluster()

    def test_workload(self):
        utils = Workload(self.driver, self.endpoint)

        utils.create_topic()

        for _ in self.roll():
            utils.run_stress_test(duration=10)

        utils.drop_topic()


class TestKafkaTopicRestartToAnotherVersion(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        # create topic with a version that supports cleanup-policy=compact
        start_version_indices = [i for i, v in enumerate(self.versions) if not (v < string_version_to_tuple(MIN_SUPPORTED_VERSION))]
        if not start_version_indices:
            pytest.skip(f"Topic may be created only since {MIN_SUPPORTED_VERSION}")
        assert self.current_binary_paths_index is not None
        self.current_binary_paths_index = start_version_indices[0]

        yield from self.setup_cluster()

    def test_workload(self):
        utils = Workload(self.driver, self.endpoint)

        utils.create_topic()
        if self.current_binary_paths_index != 0:
            self.current_binary_paths_index = -1
            self.change_cluster_version()  # current_binary_paths_index -> 0

        utils.run_stress_test(duration=20)
        self.change_cluster_version()
        utils.run_stress_test(duration=20)
        self.change_cluster_version()
        utils.run_stress_test(duration=20)

        utils.drop_topic()

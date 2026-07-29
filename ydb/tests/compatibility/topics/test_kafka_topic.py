# -*- coding: utf-8 -*-
import concurrent.futures
import logging
import os
import pytest
import uuid
import yatest

from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RollingUpgradeAndDowngradeFixture, RestartToAnotherVersionFixture, string_version_to_tuple


class Workload:
    def __init__(self, fixture):
        self.fixture = fixture
        self.id = f"{uuid.uuid1()}".replace("-", "_")
        self.topic_name = f"source_topic_{self.id}"
        self.message_count = 0
        self.processed_message_count = 0
        self.consumers = 1
        self.restart_interval = 10

    @property
    def driver(self):
        return self.fixture.driver

    @property
    def endpoint(self):
        return self.fixture.endpoint

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

    def create_topic(self, partitions=4):
        subcmds = [
            'init',
            '--consumers', str(self.consumers),
            '--partitions', str(partitions),
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
MAX_HEADER_SIZE_64_VERSION = "stable-26-3"


def skip_if_unsupported(versions):
    if min(versions) < string_version_to_tuple(MIN_SUPPORTED_VERSION):
        pytest.skip(f"Only available since {MIN_SUPPORTED_VERSION}")


def skip_unless_max_header_size_downgrade(versions):
    if len(versions) != 2:
        pytest.skip("Only restart between two versions is supported")

    if versions[0] < string_version_to_tuple(MAX_HEADER_SIZE_64_VERSION):
        pytest.skip(f"Initial version must have MAX_HEADER_SIZE=64, available since {MAX_HEADER_SIZE_64_VERSION}")

    if versions[1] >= string_version_to_tuple(MAX_HEADER_SIZE_64_VERSION):
        pytest.skip("Target version must have MAX_HEADER_SIZE=32")


def enable_aggressive_blob_compaction(fixture):
    fixture.config.yaml_config["pqconfig"]["compaction_config"] = {
        "blobs_count": 2,
        "blobs_size": 1,
    }


class TestKafkaTopicMixedClusterFixture(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)
        yield from self.setup_cluster()

    def test_workload(self):
        utils = Workload(self)

        utils.create_topic()

        utils.run_stress_test(duration=20)

        utils.drop_topic()


class TestKafkaTopicDowngradeAfterHeadPackingChange(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)
        skip_unless_max_header_size_downgrade(self.versions)

        # Batching flags default to false. Do not mention them explicitly here:
        # older rollback targets do not know all of these fields and fail while
        # parsing the compatibility test YAML config.
        yield from self.setup_cluster()

    def test_topic_survives_downgrade_after_small_head_batches(self):
        utils = Workload(self)

        enable_aggressive_blob_compaction(self)
        utils.create_topic(partitions=1)

        # Small records are the interesting case for the packing heuristic:
        # compressed payload overhead can be between the old 32-byte and the
        # new 64-byte TBatchHeader estimate.
        for message_size in (24, 32, 40, 45, 50, 64, 80):
            utils.write_to_topic(
                duration=5,
                message_rate=3000,
                message_size=message_size,
                keys_count=10,
                key_prefix=f"head_packing_before_downgrade_{message_size}",
                producers=1,
            )

        self.change_cluster_version()

        # The downgrade restart initializes partition heads. Further writes
        # drive blob compaction immediately because pqconfig.compaction_config
        # uses tiny thresholds; before the fix old binaries could abort in
        # TPartitionBlobEncoder::SerializeForKey().
        for message_size in (24, 32, 40, 45, 50, 64, 80):
            utils.write_to_topic(
                duration=5,
                message_rate=3000,
                message_size=message_size,
                keys_count=10,
                key_prefix=f"head_packing_after_downgrade_{message_size}",
                producers=1,
            )
        utils.read_from_topic(duration=10)

        utils.drop_topic()


@pytest.mark.skip(reason="Redundant")
class TestKafkaTopicRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        skip_if_unsupported(self.versions)
        yield from self.setup_cluster()

    def test_workload(self):
        utils = Workload(self)

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
        utils = Workload(self)

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

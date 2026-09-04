# -*- coding: utf-8 -*-
import concurrent.futures
from collections import Counter
import logging
import os
import pytest
import signal
import subprocess
import tarfile
import tempfile
import urllib.request
import uuid
import yatest

from ydb.tests.library.compatibility.fixtures import (
    MixedClusterFixture,
    RollingUpgradeAndDowngradeFixture,
    RestartToAnotherVersionFixture,
    string_version_to_tuple,
)
from ydb.tests.oss.ydb_sdk_import import ydb
from test_topic import (
    BATCHING_FLAG,
    CurrentToCurrentVersionFixture,
    OFFSET_DELTA_FLAG,
    STABLE_26_3,
    read_messages,
    set_feature_flags,
    wait_topic_end_offset,
    write_kafka_batch,
    write_raw_messages,
)


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
KAFKA_WORKLOAD_CONSUMER = "workload-consumer"
KAFKA_CHECKER_CONSUMER = "targetCheckerConsumer"
KAFKA_STREAMS_JAR_URL = "https://storage.yandexcloud.net/ydb-ci/kafka/e2e-kafka-api-tests-1.0-with-parameter-choice.jar"
KAFKA_JDK_URL = "https://storage.yandexcloud.net/ydb-ci/kafka/jdk-linux-x86_64.yandex.tgz"


def skip_if_unsupported(versions):
    if min(versions) < string_version_to_tuple(MIN_SUPPORTED_VERSION):
        pytest.skip(f"Only available since {MIN_SUPPORTED_VERSION}")


def create_kafka_streams_topic(driver, topic, consumers):
    try:
        driver.topic_client.drop_topic(topic)
    except ydb.SchemeError:
        pass
    driver.topic_client.create_topic(topic, consumers=consumers, min_active_partitions=1)


def kill_process_tree(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError:
        process.kill()


def assert_kafka_streams_forwarded_payloads(read, expected_payloads):
    expected = [
        payload if isinstance(payload, bytes) else payload.encode("utf-8")
        for payload in expected_payloads
    ]
    expected_counts = Counter(expected)
    actual_counts = Counter()
    unexpected = []

    for message in read:
        matches = [
            payload
            for payload in expected_counts
            if message.endswith(payload)
        ]
        if len(matches) == 1:
            actual_counts[matches[0]] += 1
        else:
            unexpected.append(message)

    assert not unexpected, f"Unexpected Kafka Streams payloads: {unexpected!r}"
    assert actual_counts == expected_counts


class KafkaStreamsRuntime:
    def __init__(self):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.workdir = self.tempdir.name

    def __enter__(self):
        jar_path = os.path.join(self.workdir, "e2e-kafka-api-tests-1.0-with-parameter-choice.jar")
        jdk_path = os.path.join(self.workdir, "jdk-linux-x86_64.yandex.tgz")
        urllib.request.urlretrieve(KAFKA_STREAMS_JAR_URL, jar_path)
        urllib.request.urlretrieve(KAFKA_JDK_URL, jdk_path)
        with tarfile.open(jdk_path, "r:gz") as archive:
            archive.extractall(path=self.workdir, filter='data')
        self.java_path = os.path.join(self.workdir, "bin", "java")
        self.jar_path = jar_path
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tempdir.cleanup()

    def start(self, bootstrap, source_topic, target_topic):
        return subprocess.Popen([
            self.java_path,
            "-jar",
            self.jar_path,
            bootstrap,
            f"streams-store-{uuid.uuid4().hex}",
            source_topic,
            target_topic,
            KAFKA_WORKLOAD_CONSUMER,
            "0",
            "0",
        ], start_new_session=True)


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


class TestKafkaTopicMessagesBatchingDisabledRead(CurrentToCurrentVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.all_binary_paths[0] != self.all_binary_paths[1]:
            pytest.skip("This test covers disabling batching without changing the binary version")
        if self.versions[0] < STABLE_26_3:
            pytest.skip("Topic message batching is available since stable-26-3")

        yield from self.setup_cluster(
            kafka_api_port="auto",
            extra_feature_flags=[
                "enable_kafka_native_balancing",
                OFFSET_DELTA_FLAG,
                BATCHING_FLAG,
            ],
        )

    def get_kafka_api_port(self):
        ports = [
            node.get_kafka_api_port()
            for node in self.cluster.nodes.values()
            if node.get_kafka_api_port() is not None
        ]
        assert ports
        return ports[-1]

    def copy_with_kafka_streams(self, source_topic, target_topic, expected_count):
        process = None
        with KafkaStreamsRuntime() as runtime:
            process = runtime.start(
                f"http://localhost:{self.get_kafka_api_port()}",
                source_topic,
                target_topic,
            )
            try:
                wait_topic_end_offset(self.driver, target_topic, expected_count)
            finally:
                kill_process_tree(process)
                process.wait(timeout=30)

    # Let Kafka Streams write through Kafka while batching is enabled, then disable batching and
    # verify that the topic protocol can read all messages written through the Kafka path.
    def test_kafka_written_messages_are_read_after_batching_flag_disable(self):
        source_topic = f"kafka_source_{uuid.uuid4().hex}"
        target_topic = f"kafka_target_{uuid.uuid4().hex}"
        messages = [
            f"kafka-batching-compat-message-{i}"
            for i in range(20)
        ]

        create_kafka_streams_topic(self.driver, source_topic, [KAFKA_WORKLOAD_CONSUMER])
        create_kafka_streams_topic(self.driver, target_topic, [KAFKA_CHECKER_CONSUMER])

        process = None
        with KafkaStreamsRuntime() as runtime:
            process = runtime.start(
                f"http://localhost:{self.get_kafka_api_port()}",
                source_topic,
                target_topic,
            )
            try:
                write_raw_messages(self.driver, source_topic, messages)
                wait_topic_end_offset(self.driver, target_topic, len(messages))
            finally:
                kill_process_tree(process)
                process.wait(timeout=30)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        read = read_messages(
            self.driver,
            target_topic,
            KAFKA_CHECKER_CONSUMER,
            len(messages),
        )
        assert len(read) == len(messages)
        assert_kafka_streams_forwarded_payloads(read, messages)

    # Write a physical Kafka batch through the topic protocol, disable batching, and verify that
    # Kafka fetch can still read and forward every logical record from that stored batch.
    def test_kafka_batch_written_with_topic_protocol_is_fetched_after_batching_flag_disable(self):
        source_topic = f"kafka_batch_source_{uuid.uuid4().hex}"
        target_topic = f"kafka_batch_target_{uuid.uuid4().hex}"
        messages = [
            f"kafka-fetch-after-disable-{i}".encode("utf-8")
            for i in range(5)
        ]

        create_kafka_streams_topic(self.driver, source_topic, [KAFKA_WORKLOAD_CONSUMER])
        create_kafka_streams_topic(self.driver, target_topic, [KAFKA_CHECKER_CONSUMER])
        write_kafka_batch(self.driver, source_topic, messages)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        self.copy_with_kafka_streams(source_topic, target_topic, len(messages))

        read = read_messages(
            self.driver,
            target_topic,
            KAFKA_CHECKER_CONSUMER,
            len(messages),
        )
        assert len(read) == len(messages)
        assert_kafka_streams_forwarded_payloads(read, messages)

    # Mix plain topic messages and a physical Kafka batch in one topic, disable batching, and verify
    # Kafka fetch sees a contiguous logical stream across the format boundary.
    def test_kafka_plain_and_batched_messages_survive_batching_flag_disable(self):
        source_topic = f"kafka_mixed_source_{uuid.uuid4().hex}"
        target_topic = f"kafka_mixed_target_{uuid.uuid4().hex}"
        plain_messages = [
            f"kafka-mixed-plain-{i}"
            for i in range(3)
        ]
        batch_messages = [
            f"kafka-mixed-batch-{i}".encode("utf-8")
            for i in range(5)
        ]

        create_kafka_streams_topic(self.driver, source_topic, [KAFKA_WORKLOAD_CONSUMER])
        create_kafka_streams_topic(self.driver, target_topic, [KAFKA_CHECKER_CONSUMER])
        write_raw_messages(self.driver, source_topic, plain_messages)
        write_kafka_batch(self.driver, source_topic, batch_messages, base_sequence=100)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        self.copy_with_kafka_streams(source_topic, target_topic, len(plain_messages) + len(batch_messages))

        read = read_messages(
            self.driver,
            target_topic,
            KAFKA_CHECKER_CONSUMER,
            len(plain_messages) + len(batch_messages),
        )
        assert len(read) == len(plain_messages) + len(batch_messages)
        assert {message.decode("utf-8") for message in read} == set(plain_messages) | {
            message.decode("utf-8")
            for message in batch_messages
        }

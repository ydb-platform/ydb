# -*- coding: utf-8 -*-
import pytest
import time
import uuid

from pprint import pprint

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class Workload:
    def __init__(self, driver, endpoint):
        self.driver = driver
        self.endpoint = endpoint
        self.id = f"{uuid.uuid1()}".replace("-", "_")
        self.topic_name = f"source_topic_{self.id}"
        self.message_count = 0
        self.processed_message_count = 0

    def create_topic(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"CREATE TOPIC {self.topic_name} (CONSUMER `test-consumer`);"
            )

    def write_to_topic(self):
        finished_at = time.time() + 5

        with self.driver.topic_client.writer(self.topic_name, producer_id="producer-id") as writer:
            while time.time() < finished_at:
                writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))
                self.message_count += 1

    def read_from_topic(self):
        iteration = 0
        while iteration < 5:
            iteration = iteration + 1

            end_offset = 0
            try:
                describe = self.driver.topic_client.describe_topic(self.topic_name, include_stats=True)
                pprint(vars(describe))
                end_offset = describe.partitions[0].partition_stats.partition_end
            except Exception:
                time.sleep(1)
                continue

            with self.driver.topic_client.reader(self.topic_name, consumer='test-consumer') as reader:
                while True:
                    try:
                        message = reader.receive_message(timeout=1)
                    except TimeoutError:
                        break

                    reader.commit(message)
                    self.processed_message_count = message.offset + 1

                    if self.processed_message_count == end_offset:
                        break

            if self.processed_message_count == end_offset:
                break

            time.sleep(1)

        if self.processed_message_count != end_offset:
            raise Exception(f"Received {self.processed_message_count} messages but written {self.message_count}")


class TestTopicMixedClusterFixture(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster()

    def test_write_and_read(self):
        utils = Workload(self.driver, self.endpoint)

        utils.create_topic()

        utils.write_to_topic()
        utils.read_from_topic()


class TestTopicRestartToAnotherVersion(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster()

    def test_write_and_read(self):
        utils = Workload(self.driver, self.endpoint)

        utils.create_topic()

        utils.read_from_topic()
        utils.write_to_topic()
        utils.read_from_topic()

        self.change_cluster_version()

        utils.read_from_topic()
        utils.write_to_topic()
        utils.read_from_topic()


class TestTopicRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster()

    def test_write_and_read(self):
        utils = Workload(self.driver, self.endpoint)

        utils.create_topic()

        utils.write_to_topic()
        for _ in self.roll():
            utils.read_from_topic()
            utils.write_to_topic()

        utils.read_from_topic()

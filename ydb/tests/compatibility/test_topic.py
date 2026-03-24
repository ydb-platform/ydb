# -*- coding: utf-8 -*-
import pytest
import time
import uuid

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture, RollingDowngradeAndUpgradeFixture, string_version_to_tuple
from ydb.tests.oss.ydb_sdk_import import ydb


class Workload:
    def __init__(self, driver, endpoint):
        self.driver = driver
        self.endpoint = endpoint
        self.id = f"{uuid.uuid1()}".replace("-", "_")
        self.topic_name = f"source_topic_{self.id}"
        self.message_count = 0
        self.processed_message_count = 0

    def create_topic(self, *, availability_period=None, partition_count=1):
        consumer_extra_options = []
        if availability_period:
            consumer_extra_options.append(f"availability_period=Interval('{availability_period}')")
        consumer_extra_options_str = f"WITH ({', '.join(consumer_extra_options)})" if consumer_extra_options else ""
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"CREATE TOPIC {self.topic_name} (CONSUMER `test-consumer` {consumer_extra_options_str}) WITH (MIN_ACTIVE_PARTITIONS = {partition_count});"
            )

    def write_to_topic(self):
        finished_at = time.time() + 5

        with self.driver.topic_client.writer(self.topic_name, producer_id="producer-id") as writer:
            while time.time() < finished_at:
                writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))
                self.message_count += 1

    def write_to_topic_in_transaction(self, partition_id, message_count):
        messages = []
        for i in range(message_count):
            messages.append(f"transaction-message-{i}-{time.time()}")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            def callee(tx):
                writer = self.driver.topic_client.tx_writer(tx, self.topic_name, partition_id=partition_id)
                for message in messages:
                    writer.write(ydb.TopicWriterMessage(message))

            session_pool.retry_tx_sync(callee)
            self.message_count += len(messages)

        return messages

    def read_from_topic(self):
        iteration = 0
        while iteration < 5:
            iteration = iteration + 1

            total_count = 0
            try:
                describe = self.driver.topic_client.describe_topic(self.topic_name, include_stats=True)
                for p in describe.partitions:
                    total_count += p.partition_stats.partition_end
            except Exception:
                time.sleep(1)
                continue

            if total_count != self.message_count:
                raise Exception(f"all mesages wasn`t written: writen {total_count} messages but {self.message_count}")

            with self.driver.topic_client.reader(self.topic_name, consumer='test-consumer') as reader:
                while True:
                    try:
                        message = reader.receive_message(timeout=1)
                    except TimeoutError:
                        break

                    reader.commit(message)
                    self.processed_message_count += 1

                    if self.processed_message_count == total_count:
                        break

            if self.processed_message_count == total_count:
                break

            time.sleep(1)

        if self.processed_message_count != total_count:
            raise Exception(f"Received {self.processed_message_count} messages but written {self.message_count}")


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


class TestTopicRollingDowngrade(RollingDowngradeAndUpgradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_write_and_read_with_availability_period(self):
        MIN_SUPPORTED_VERSION = "stable-25-4"
        if self.versions[0] < string_version_to_tuple(MIN_SUPPORTED_VERSION):
            pytest.skip(f"Only available since {MIN_SUPPORTED_VERSION}")

        utils = Workload(self.driver, self.endpoint)

        utils.create_topic(availability_period='PT2H')

        utils.write_to_topic()
        for _ in self.roll():
            utils.read_from_topic()
            utils.write_to_topic()

        utils.read_from_topic()


class TestTopicTransaction(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_write_and_read_in_transaction(self):
        utils = Workload(self.driver, self.endpoint)

        utils.create_topic(partition_count=2)

        # Write messages in transaction
        expected_message_count = 0
        expected_message_count += len(utils.write_to_topic_in_transaction(partition_id=0, message_count=3000))
        expected_message_count += len(utils.write_to_topic_in_transaction(partition_id=1, message_count=3000))

        # Read and verify messages
        utils.read_from_topic()

        # Verify that all expected messages were processed
        if utils.processed_message_count < expected_message_count:
            raise Exception(
                f"Not all transaction messages were processed. "
                f"Expected {expected_message_count}, got {utils.processed_message_count}"
            )

    def test_mixed_write_transaction_and_regular(self):
        utils = Workload(self.driver, self.endpoint)

        utils.create_topic(partition_count=2)

        message_count_before_transaction = 200
        message_count_after_transaction = 100

        # Write some messages regularly
        with utils.driver.topic_client.writer(utils.topic_name, partition_id=0, producer_id="regular-producer-1") as writer:
            for i in range(message_count_before_transaction):
                writer.write(ydb.TopicWriterMessage(f"regular-message-{i}"))
                utils.message_count += 1

        # Write some messages in transaction
        expected_transactional_message_count = 0
        expected_transactional_message_count += len(utils.write_to_topic_in_transaction(partition_id=0, message_count=2000))
        expected_transactional_message_count += len(utils.write_to_topic_in_transaction(partition_id=1, message_count=2000))

        # Write more messages regularly
        with utils.driver.topic_client.writer(utils.topic_name, partition_id=1, producer_id="regular-producer-2") as writer:
            for i in range(message_count_after_transaction):
                writer.write(ydb.TopicWriterMessage(f"regular-message-{i+message_count_before_transaction}"))
                utils.message_count += 1

        # Read all messages
        utils.read_from_topic()

        expected_total = message_count_before_transaction + expected_transactional_message_count + message_count_after_transaction
        if utils.processed_message_count != expected_total:
            raise Exception(
                f"Expected {expected_total} messages to be processed, got {utils.processed_message_count}"
            )

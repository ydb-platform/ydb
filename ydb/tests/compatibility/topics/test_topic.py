# -*- coding: utf-8 -*-
import asyncio
import datetime
import struct
import time
import uuid

import pytest

from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
    RollingDowngradeAndUpgradeFixture,
    current_binary_path,
    current_name,
    path_to_version,
    string_version_to_tuple,
)
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb._grpc.grpcwrapper.ydb_topic import StreamWriteMessage
from ydb._topic_writer.topic_writer import InternalMessage, PublicMessage
from ydb._topic_writer.topic_writer_asyncio import WriterAsyncIOStream


OFFSET_DELTA_FLAG = "enable_topic_write_offset_delta_in_keys"
BATCHING_FLAG = "enable_topic_messages_batching"
TOPIC_BATCHING_CODEC = 5

STABLE_26_3 = string_version_to_tuple("stable-26-3")


class CurrentToCurrentVersionFixture(RestartToAnotherVersionFixture):
    @pytest.fixture(
        autouse=True,
        params=[[current_binary_path, current_binary_path]],
        ids=[f"restart_{current_name}_to_{current_name}"],
    )
    def base_setup(self, request):
        self.current_binary_paths_index = 0
        self.all_binary_paths = request.param
        self.versions = [path_to_version[path] for path in self.all_binary_paths]


class Workload:
    def __init__(self, fixture):
        self.fixture = fixture
        self.id = f"{uuid.uuid1()}".replace("-", "_")
        self.topic_name = f"source_topic_{self.id}"
        self.message_count = 0
        self.processed_message_count = 0

    @property
    def driver(self):
        return self.fixture.driver

    @property
    def endpoint(self):
        return self.fixture.endpoint

    def create_topic(self, *, availability_period=None, partition_count=1):
        consumer_extra_options = []
        if availability_period:
            consumer_extra_options.append(f"availability_period=Interval('{availability_period}')")
        consumer_extra_options_str = f"WITH ({', '.join(consumer_extra_options)})" if consumer_extra_options else ""
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"CREATE TOPIC {self.topic_name} (CONSUMER `test-consumer` {consumer_extra_options_str}) WITH (MIN_ACTIVE_PARTITIONS = {partition_count});"
            )

    def write_to_topic(self, topic_writer: ydb.TopicWriter | None = None):
        finished_at = time.time() + 5

        def write_loop(writer):
            while time.time() < finished_at:
                writer.write(ydb.TopicWriterMessage(f"message-{time.time()}"))
                self.message_count += 1

        if topic_writer is not None:
            write_loop(topic_writer)
            # Long-lived writer: write() only enqueues; without flush, partition stats
            # lag behind message_count until close(). Short-lived `with writer` flushes on exit.
            topic_writer.flush()
        else:
            with self.driver.topic_client.writer(self.topic_name, producer_id="producer-id") as writer:
                write_loop(writer)

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

    def read_from_topic(self, topic_reader: ydb.TopicReader | None = None):
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

            if topic_reader is not None:
                self._read_from_topic(total_count, topic_reader)
            else:
                with self.driver.topic_client.reader(self.topic_name, consumer='test-consumer') as reader:
                    self._read_from_topic(total_count, reader)

            if self.processed_message_count == total_count:
                break

            time.sleep(1)

        if self.processed_message_count != total_count:
            raise Exception(f"Received {self.processed_message_count} messages but written {self.message_count}")

    def _read_from_topic(self, total_count: int, reader: ydb.TopicReader):
        while True:
            try:
                message = reader.receive_message(timeout=1)
            except TimeoutError:
                break

            reader.commit(message)
            self.processed_message_count += 1

            if self.processed_message_count == total_count:
                break


def _make_crc32c_table():
    polynomial = 0x82F63B78
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ (polynomial & -(crc & 1))
        table.append(crc)
    return table


_CRC32C_TABLE = _make_crc32c_table()


def _crc32c(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def _varint(value, bits=64):
    value = (value << 1) ^ (value >> (bits - 1))
    result = bytearray()
    while value & ~0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _kafka_bytes(value):
    if value is None:
        return _varint(-1, bits=32)
    return _varint(len(value), bits=32) + value


def _kafka_record(value, offset_delta, timestamp_delta=0):
    record_body = b"".join([
        struct.pack(">b", 0),  # attributes
        _varint(timestamp_delta),
        _varint(offset_delta),
        _kafka_bytes(None),  # key
        _kafka_bytes(value),
        _varint(0, bits=32),  # headers
    ])
    return _varint(len(record_body), bits=32) + record_body


def make_kafka_batch_payload(values, base_sequence=1):
    records = b"".join(
        _kafka_record(value, offset_delta=i, timestamp_delta=i)
        for i, value in enumerate(values)
    )
    records_array = struct.pack(">i", len(values)) + records

    base_timestamp = 1000
    crc_body = b"".join([
        struct.pack(">h", 0),  # attributes: no compression
        struct.pack(">i", len(values) - 1),
        struct.pack(">q", base_timestamp),
        struct.pack(">q", base_timestamp + len(values) - 1),
        struct.pack(">q", 42),  # producer id
        struct.pack(">h", 0),  # producer epoch
        struct.pack(">i", base_sequence),
        records_array,
    ])
    prefix_after_length = struct.pack(">ib", -1, 2)
    batch_length = len(prefix_after_length) + 4 + len(crc_body)
    crc = _crc32c(crc_body)

    return b"".join([
        struct.pack(">q", 0),  # base offset
        struct.pack(">i", batch_length),
        prefix_after_length,
        struct.pack(">I", crc),
        crc_body,
    ])


async def _write_kafka_batch_async(driver, topic_name, values, base_sequence):
    stream = await WriterAsyncIOStream.create(
        driver,
        StreamWriteMessage.InitRequest(
            path=topic_name,
            producer_id=f"kafka-batch-producer-{uuid.uuid4().hex}",
            write_session_meta={},
            partitioning=StreamWriteMessage.PartitioningPartitionID(0),
            get_last_seq_no=True,
        ),
    )
    try:
        payload = make_kafka_batch_payload(values, base_sequence=base_sequence)
        message = InternalMessage(
            PublicMessage(
                payload,
                seqno=base_sequence + len(values) - 1,
                created_at=datetime.datetime.now(datetime.timezone.utc),
            )
        )
        message.codec = TOPIC_BATCHING_CODEC
        stream.write([message])
        response = await stream.receive()
        assert len(response.acks) == 1
        assert isinstance(
            response.acks[0].message_write_status,
            StreamWriteMessage.WriteResponse.WriteAck.StatusWritten,
        )
    finally:
        await stream.close()


def write_kafka_batch(driver, topic_name, values, base_sequence=1):
    asyncio.run(_write_kafka_batch_async(driver, topic_name, values, base_sequence))


def write_raw_messages_in_transaction(driver, topic_name, values, producer_id=None, partition_id=0):
    with ydb.QuerySessionPool(driver) as session_pool:
        def callee(tx):
            writer = driver.topic_client.tx_writer(
                tx,
                topic_name,
                producer_id=producer_id or f"tx-producer-{uuid.uuid4().hex}",
                partition_id=partition_id,
                codec=ydb.TopicCodec.RAW,
            )
            for value in values:
                writer.write(ydb.TopicWriterMessage(value), timeout=30)
            writer.flush(timeout=30)
            writer.close(flush=False)

        session_pool.retry_tx_sync(callee)


def read_messages(driver, topic_name, consumer, expected_count, timeout=60):
    messages = []
    with driver.topic_client.reader(topic_name, consumer=consumer) as reader:
        deadline = time.time() + timeout
        while len(messages) < expected_count and time.time() < deadline:
            try:
                message = reader.receive_message(timeout=1)
            except TimeoutError:
                continue
            messages.append(message.data)
            reader.commit(message)

    assert len(messages) == expected_count
    return messages


def write_raw_messages(driver, topic_name, values, producer_id=None):
    with driver.topic_client.writer(
        topic_name,
        producer_id=producer_id or f"producer-{uuid.uuid4().hex}",
        codec=ydb.TopicCodec.RAW,
    ) as writer:
        for value in values:
            writer.write(ydb.TopicWriterMessage(value))


def wait_topic_end_offset(driver, topic_name, expected_count, timeout=90):
    deadline = time.time() + timeout
    last_count = 0
    while time.time() < deadline:
        description = driver.topic_client.describe_topic(topic_name, include_stats=True)
        last_count = sum(partition.partition_stats.partition_end for partition in description.partitions)
        if last_count >= expected_count:
            return last_count
        time.sleep(1)
    raise AssertionError(f"{topic_name} end offset did not reach {expected_count}: got {last_count}")


def set_feature_flags(config, **values):
    flags = config.yaml_config.setdefault("feature_flags", {})
    for name, value in values.items():
        flags[name] = value


class TestTopicRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster()

    def test_write_and_read(self):
        utils = Workload(self)

        utils.create_topic()

        utils.write_to_topic()
        for _ in self.roll():
            utils.read_from_topic()
            utils.write_to_topic()

        utils.read_from_topic()

    def test_write_and_read_with_long_live_consumer(self):
        utils = Workload(self)

        utils.create_topic()

        stable_driver = self.create_driver()
        with stable_driver.topic_client.reader(utils.topic_name, consumer='test-consumer') as reader:
            utils.write_to_topic()
            for _ in self.roll():
                utils.read_from_topic(topic_reader=reader)
                utils.write_to_topic()

            utils.read_from_topic(topic_reader=reader)

    def test_write_and_read_with_long_live_producer(self):
        utils = Workload(self)

        utils.create_topic()

        stable_driver = self.create_driver()
        with stable_driver.topic_client.writer(utils.topic_name, producer_id="producer-id") as writer:
            utils.write_to_topic(topic_writer=writer)
            for _ in self.roll():
                utils.read_from_topic()
                utils.write_to_topic(topic_writer=writer)

            utils.read_from_topic()


class TestTopicRollingDowngrade(RollingDowngradeAndUpgradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_write_and_read_with_availability_period(self):
        MIN_SUPPORTED_VERSION = "stable-25-4"
        if self.versions[0] < string_version_to_tuple(MIN_SUPPORTED_VERSION):
            pytest.skip(f"Only available since {MIN_SUPPORTED_VERSION}")

        utils = Workload(self)

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
        utils = Workload(self)

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
        utils = Workload(self)

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

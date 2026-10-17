# -*- coding: utf-8 -*-
import asyncio
import concurrent.futures
import datetime
import gzip
import logging
import random
import struct
import time
import uuid

import pytest

from ydb.tests.library.common.types import TabletTypes
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
from ydb import _apis
from ydb._grpc.grpcwrapper.common_utils import GrpcWrapperAsyncIO
from ydb._grpc.grpcwrapper.ydb_topic import Codec, StreamReadMessage, StreamWriteMessage
from ydb._topic_writer.topic_writer import InternalMessage, PublicMessage
from ydb._topic_writer.topic_writer_asyncio import WriterAsyncIOStream


OFFSET_DELTA_FLAG = "enable_topic_write_offset_delta_in_keys"
BATCHING_FLAG = "enable_topic_messages_batching"
TOPIC_COMPACTION_FLAG = "enable_topic_compactification_by_key"
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


logger = logging.getLogger(__name__)


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

    def create_topic(
        self,
        *,
        availability_period=None,
        partition_count=1,
        consumers=("test-consumer",),
    ):
        consumer_extra_options = []
        if availability_period:
            consumer_extra_options.append(f"availability_period=Interval('{availability_period}')")
        consumer_extra_options_str = f"WITH ({', '.join(consumer_extra_options)})" if consumer_extra_options else ""
        consumers_str = ", ".join(
            f"CONSUMER `{consumer}` {consumer_extra_options_str}"
            for consumer in consumers
        )
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"CREATE TOPIC {self.topic_name} ({consumers_str}) WITH (MIN_ACTIVE_PARTITIONS = {partition_count});"
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


def _read_varint(data, pos):
    value = 0
    shift = 0
    while True:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7

    return (value >> 1) ^ -(value & 1), pos


def _kafka_bytes(value):
    if value is None:
        return _varint(-1, bits=32)
    return _varint(len(value), bits=32) + value


def _read_kafka_bytes(data, pos):
    length, pos = _read_varint(data, pos)
    if length < 0:
        return None, pos
    return data[pos:pos + length], pos + length


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


def read_kafka_batch_payload_values(payload):
    base_offset, batch_length, partition_leader_epoch, magic = struct.unpack_from(
        ">qiib",
        payload,
        0,
    )
    assert base_offset == 0
    assert batch_length == len(payload) - 12
    assert partition_leader_epoch == -1
    assert magic == 2

    crc, = struct.unpack_from(">I", payload, 17)
    assert crc == _crc32c(payload[21:])

    pos = 21
    attributes, last_offset_delta, base_timestamp, max_timestamp, producer_id = struct.unpack_from(
        ">hiqqq",
        payload,
        pos,
    )
    pos += 30
    producer_epoch, base_sequence, records_count = struct.unpack_from(">hii", payload, pos)
    pos += 10

    assert attributes == 0
    assert last_offset_delta == records_count - 1
    assert base_timestamp <= max_timestamp
    assert producer_id == 42
    assert producer_epoch == 0
    assert base_sequence >= 0

    values = []
    for _ in range(records_count):
        record_length, pos = _read_varint(payload, pos)
        record_end = pos + record_length
        attributes, = struct.unpack_from(">b", payload, pos)
        pos += 1
        _, pos = _read_varint(payload, pos)  # timestamp delta
        _, pos = _read_varint(payload, pos)  # offset delta
        key, pos = _read_kafka_bytes(payload, pos)
        value, pos = _read_kafka_bytes(payload, pos)
        headers_count, pos = _read_varint(payload, pos)

        assert attributes == 0
        assert key is None
        assert headers_count == 0
        assert pos == record_end
        values.append(value)

    assert pos == len(payload)
    return values


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


async def _write_kafka_batch_in_transaction_async(driver, tx, topic_name, values, base_sequence, producer_id):
    payload = make_kafka_batch_payload(values, base_sequence=base_sequence)
    stream = await WriterAsyncIOStream.create(
        driver,
        StreamWriteMessage.InitRequest(
            path=topic_name,
            producer_id=producer_id or f"tx-kafka-batch-producer-{uuid.uuid4().hex}",
            write_session_meta={},
            partitioning=StreamWriteMessage.PartitioningPartitionID(0),
            get_last_seq_no=True,
        ),
        tx_identity=tx._tx_identity(),
    )
    try:
        message = InternalMessage(
            PublicMessage(
                payload,
                seqno=base_sequence + len(values) - 1,
                created_at=datetime.datetime.now(datetime.timezone.utc),
            )
        )
        message.codec = TOPIC_BATCHING_CODEC
        stream.write([message])
        response = await asyncio.wait_for(stream.receive(), timeout=30)
        assert len(response.acks) == 1
        assert isinstance(
            response.acks[0].message_write_status,
            StreamWriteMessage.WriteResponse.WriteAck.StatusWrittenInTx,
        )
    finally:
        await stream.close()


def write_kafka_batch_in_transaction(driver, topic_name, values, base_sequence=1, producer_id=None):
    with ydb.QuerySessionPool(driver) as session_pool:
        def callee(tx):
            asyncio.run(_write_kafka_batch_in_transaction_async(
                driver,
                tx,
                topic_name,
                values,
                base_sequence,
                producer_id,
            ))

        session_pool.retry_tx_sync(callee)


def write_kafka_batch_and_rollback_transaction(driver, topic_name, values, base_sequence=1, producer_id=None):
    with ydb.QuerySessionPool(driver) as session_pool:
        with session_pool.checkout() as session:
            tx = session.transaction().begin()
            try:
                asyncio.run(_write_kafka_batch_in_transaction_async(
                    driver,
                    tx,
                    topic_name,
                    values,
                    base_sequence,
                    producer_id or f"tx-kafka-batch-rollback-producer-{uuid.uuid4().hex}",
                ))
            finally:
                tx.rollback()


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


class BatchingSupportedInitRequest(StreamReadMessage.InitRequest):
    def to_proto(self):
        proto = super().to_proto()
        proto.is_batching_supported = True
        return proto


async def _read_topic_batches_async(driver, topic_name, consumer, read_offset=0, timeout=30):
    stream = GrpcWrapperAsyncIO(StreamReadMessage.FromServer.from_proto)
    await stream.start(driver, _apis.TopicService.Stub, _apis.TopicService.StreamRead)
    try:
        stream.write(StreamReadMessage.FromClient(BatchingSupportedInitRequest(
            topics_read_settings=[
                StreamReadMessage.InitRequest.TopicReadSettings(path=topic_name)
            ],
            consumer=consumer,
            auto_partitioning_support=True,
        )))
        stream.write(StreamReadMessage.FromClient(StreamReadMessage.ReadRequest(bytes_size=50 * 1024 * 1024)))

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                message = await stream.receive(timeout=5)
            except TimeoutError:
                continue
            server_message = message.server_message
            if isinstance(server_message, StreamReadMessage.StartPartitionSessionRequest):
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.StartPartitionSessionResponse(
                    partition_session_id=server_message.partition_session.partition_session_id,
                    read_offset=read_offset,
                    commit_offset=None,
                )))
            elif isinstance(server_message, StreamReadMessage.StopPartitionSessionRequest):
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.StopPartitionSessionResponse(
                    partition_session_id=server_message.partition_session_id,
                )))
            elif isinstance(server_message, StreamReadMessage.ReadResponse):
                batches = [
                    batch
                    for partition_data in server_message.partition_data
                    for batch in partition_data.batches
                    if batch.message_data
                ]
                if batches:
                    return batches

        raise TimeoutError("No topic batches received")
    finally:
        stream.close()


def read_topic_batches(driver, topic_name, consumer, read_offset=0):
    return asyncio.run(_read_topic_batches_async(driver, topic_name, consumer, read_offset=read_offset))


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


def read_messages_with_offsets(driver, topic_name, consumer, expected_count, timeout=60):
    messages = []
    with driver.topic_client.reader(topic_name, consumer=consumer) as reader:
        deadline = time.time() + timeout
        while len(messages) < expected_count and time.time() < deadline:
            try:
                message = reader.receive_message(timeout=1)
            except TimeoutError:
                continue
            messages.append((message.offset, message.data))
            reader.commit(message)

    assert len(messages) == expected_count
    return messages


async def _read_messages_with_offsets_from_read_offset_async(
    driver,
    topic_name,
    consumer,
    read_offset,
    expected_count,
    timeout=60,
):
    messages = []
    stream = GrpcWrapperAsyncIO(StreamReadMessage.FromServer.from_proto)
    await stream.start(driver, _apis.TopicService.Stub, _apis.TopicService.StreamRead)
    try:
        stream.write(StreamReadMessage.FromClient(StreamReadMessage.InitRequest(
            topics_read_settings=[
                StreamReadMessage.InitRequest.TopicReadSettings(path=topic_name)
            ],
            consumer=consumer,
            auto_partitioning_support=True,
        )))

        deadline = time.time() + timeout
        while len(messages) < expected_count and time.time() < deadline:
            try:
                message = await stream.receive(timeout=min(5, max(1, deadline - time.time())))
            except TimeoutError:
                continue

            server_message = message.server_message
            if isinstance(server_message, StreamReadMessage.StartPartitionSessionRequest):
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.StartPartitionSessionResponse(
                    partition_session_id=server_message.partition_session.partition_session_id,
                    read_offset=read_offset,
                    commit_offset=None,
                )))
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.ReadRequest(bytes_size=50 * 1024 * 1024)))
            elif isinstance(server_message, StreamReadMessage.StopPartitionSessionRequest):
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.StopPartitionSessionResponse(
                    partition_session_id=server_message.partition_session_id,
                )))
            elif isinstance(server_message, StreamReadMessage.ReadResponse):
                for partition_data in server_message.partition_data:
                    for batch in partition_data.batches:
                        for message_data in batch.message_data:
                            data = message_data.data
                            if batch.codec == Codec.CODEC_GZIP:
                                data = gzip.decompress(data)
                            assert batch.codec in (Codec.CODEC_RAW, Codec.CODEC_GZIP)
                            messages.append((message_data.offset, data))
                if len(messages) < expected_count:
                    stream.write(StreamReadMessage.FromClient(StreamReadMessage.ReadRequest(bytes_size=50 * 1024 * 1024)))

        assert len(messages) == expected_count
        return messages
    finally:
        stream.close()


def read_messages_with_offsets_from_read_offset(
    driver,
    topic_name,
    consumer,
    read_offset,
    expected_count,
    timeout=60,
):
    return asyncio.run(_read_messages_with_offsets_from_read_offset_async(
        driver,
        topic_name,
        consumer,
        read_offset,
        expected_count,
        timeout=timeout,
    ))


def read_available_messages(driver, topic_name, consumer, min_count, timeout=120, idle_timeout=5):
    messages = []
    with driver.topic_client.reader(topic_name, consumer=consumer) as reader:
        deadline = time.time() + timeout
        idle_deadline = None
        while time.time() < deadline:
            try:
                message = reader.receive_message(timeout=1)
            except TimeoutError:
                if len(messages) >= min_count:
                    if idle_deadline is None:
                        idle_deadline = time.time() + idle_timeout
                    elif time.time() >= idle_deadline:
                        break
                continue
            idle_deadline = None
            messages.append(message.data)
            reader.commit(message)

    assert len(messages) >= min_count
    return messages


def read_messages_in_batches(driver, topic_name, consumer, expected_count, max_messages=None, max_bytes=None, timeout=60):
    messages = []
    batch_sizes = []
    with driver.topic_client.reader(topic_name, consumer=consumer) as reader:
        deadline = time.time() + timeout
        while len(messages) < expected_count and time.time() < deadline:
            try:
                batch = reader.receive_batch(
                    max_messages=max_messages,
                    max_bytes=max_bytes,
                    timeout=1,
                )
            except TimeoutError:
                continue
            if batch is None or batch.empty():
                continue
            batch_sizes.append(len(batch.messages))
            messages.extend(message.data for message in batch.messages)
            reader.commit(batch)

    assert len(messages) == expected_count
    return messages, batch_sizes


def write_raw_messages(driver, topic_name, values, producer_id=None):
    with driver.topic_client.writer(
        topic_name,
        producer_id=producer_id or f"producer-{uuid.uuid4().hex}",
        codec=ydb.TopicCodec.RAW,
    ) as writer:
        for value in values:
            writer.write(ydb.TopicWriterMessage(value))


def write_keyed_messages(driver, topic_name, key_to_values):
    for key, values in key_to_values.items():
        write_raw_messages(driver, topic_name, values, producer_id=key)


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


def create_compacted_topic(driver, topic_name, consumers=("test-consumer",)):
    driver.topic_client.create_topic(
        topic_name,
        min_active_partitions=1,
        attributes={"_cleanup_policy": "compact"},
        consumers=list(consumers),
    )


def set_feature_flags(config, **values):
    flags = config.yaml_config.setdefault("feature_flags", {})
    for name, value in values.items():
        flags[name] = value


def remove_feature_flags(config, *names):
    flags = config.yaml_config.setdefault("feature_flags", {})
    for name in names:
        flags.pop(name, None)


def set_aggressive_topic_compaction(config):
    compaction_config = config.yaml_config.setdefault("pqconfig", {}).setdefault("compaction_config", {})
    compaction_config["blobs_count"] = 0
    compaction_config["max_blobs_count"] = 1


def restart_cluster_with_current_config(fixture):
    fixture.stop_driver()
    fixture.cluster.update_configurator_and_restart(fixture.config)
    fixture.driver = fixture.create_driver()
    time.sleep(60)


def run_offset_delta_toggle_workload(fixture, topic_name, consumer, message_count=1000, timeout=180):
    written_before_disable = [
        f"offset-delta-before-{i}-" + ("x" * 4096)
        for i in range(message_count)
    ]
    write_raw_messages(fixture.driver, topic_name, written_before_disable)

    remove_feature_flags(fixture.config, OFFSET_DELTA_FLAG)
    fixture.change_cluster_version()

    written_after_disable = [
        f"offset-delta-after-{i}-" + ("y" * 4096)
        for i in range(message_count)
    ]
    expected_messages = [
        message.encode("utf-8")
        for message in written_before_disable + written_after_disable
    ]

    reader_driver = fixture.create_driver()
    writer_driver = fixture.create_driver()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            read_future = executor.submit(
                read_messages,
                reader_driver,
                topic_name,
                consumer,
                len(expected_messages),
                timeout,
            )
            write_future = executor.submit(
                write_raw_messages,
                writer_driver,
                topic_name,
                written_after_disable,
            )

            write_future.result(timeout=timeout)
            assert read_future.result(timeout=timeout) == expected_messages
    finally:
        reader_driver.stop()
        writer_driver.stop()


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


class TestTopicOffsetDeltaKeysFlagDisable(CurrentToCurrentVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.all_binary_paths[0] != self.all_binary_paths[1]:
            pytest.skip("This test covers disabling offset-delta keys without changing the binary version")
        if self.versions[0] < STABLE_26_3:
            pytest.skip("Offset delta in keys is written only since stable-26-3")

        yield from self.setup_cluster(extra_feature_flags=[OFFSET_DELTA_FLAG])

    # Keep the binary version fixed, but remove the offset-delta key flag after writes; this isolates
    # mixed key-format handling from any old-binary downgrade behavior.
    def test_offset_delta_keys_can_mix_old_and_new_keys_after_flag_disable_current(self):
        utils = Workload(self)
        utils.create_topic()

        run_offset_delta_toggle_workload(self, utils.topic_name, "test-consumer")


class TestTopicOffsetDeltaKeysCompactionFlagDisable(CurrentToCurrentVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.all_binary_paths[0] != self.all_binary_paths[1]:
            pytest.skip("This test covers disabling offset-delta keys without changing the binary version")
        if self.versions[0] < STABLE_26_3:
            pytest.skip("Offset delta in keys is written only since stable-26-3")

        yield from self.setup_cluster(extra_feature_flags=[OFFSET_DELTA_FLAG, TOPIC_COMPACTION_FLAG])

    # Write a compacted topic with aggressive PQ compaction enabled, remove the offset-delta key flag,
    # restart current, and verify that compacted old-key data can still be read and extended.
    def test_offset_delta_keys_survive_compaction_after_flag_disable(self):
        set_aggressive_topic_compaction(self.config)
        restart_cluster_with_current_config(self)

        topic_name = f"compacted_offset_delta_{uuid.uuid4().hex}"
        create_compacted_topic(self.driver, topic_name)

        before = {
            f"compact-key-{key}": [
                f"compact-before-key-{key}-{message}"
                for message in range(8)
            ]
            for key in range(5)
        }
        write_keyed_messages(self.driver, topic_name, before)
        wait_topic_end_offset(self.driver, topic_name, 40)
        restart_cluster_with_current_config(self)

        remove_feature_flags(self.config, OFFSET_DELTA_FLAG)
        self.change_cluster_version()

        after = {
            "compact-key-after-0": ["compact-after-key-0"],
            "compact-key-after-1": ["compact-after-key-1"],
        }
        write_keyed_messages(self.driver, topic_name, after)

        read = read_available_messages(self.driver, topic_name, "test-consumer", 7, timeout=180)
        expected_payloads = {
            values[-1].encode("utf-8")
            for values in before.values()
        }
        expected_payloads.update(
            message.encode("utf-8")
            for values in after.values()
            for message in values
        )
        assert expected_payloads.issubset(set(read))


class TestTopicMessagesBatchingDisabledRead(CurrentToCurrentVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.all_binary_paths[0] != self.all_binary_paths[1]:
            pytest.skip("This test covers disabling batching without changing the binary version")
        if self.versions[0] < STABLE_26_3:
            pytest.skip("Topic message batching is available since stable-26-3")

        yield from self.setup_cluster(extra_feature_flags=[OFFSET_DELTA_FLAG, BATCHING_FLAG])

    # Store a physical Kafka batch while batching is enabled, disable the flag, and verify that a
    # normal topic reader still receives all logical messages while a batch-aware reader sees one batch.
    def test_kafka_batch_written_with_topic_protocol_is_read_after_flag_disable(self):
        utils = Workload(self)
        utils.create_topic(consumers=("test-consumer", "batch-consumer"))

        batch_values = [
            f"topic-batch-message-{i}".encode("utf-8")
            for i in range(5)
        ]
        write_kafka_batch(self.driver, utils.topic_name, batch_values)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        assert read_messages(
            self.driver,
            utils.topic_name,
            "test-consumer",
            len(batch_values),
        ) == batch_values

        batches = read_topic_batches(self.driver, utils.topic_name, "batch-consumer")
        messages = [
            message
            for batch in batches
            for message in batch.message_data
        ]

        assert len(messages) == 1
        assert batches[0].codec == TOPIC_BATCHING_CODEC
        payload = messages[0].data
        assert read_kafka_batch_payload_values(payload) == batch_values

    # Disable batching after writing a physical Kafka batch, commit only the first logical messages,
    # restart, and verify that reading resumes from the middle of the stored physical batch.
    def test_kafka_batch_cut_reader_commits_middle_after_batching_disable(self):
        utils = Workload(self)
        utils.create_topic()

        batch_values = [
            f"topic-batch-commit-middle-{i}".encode("utf-8")
            for i in range(5)
        ]
        write_kafka_batch(self.driver, utils.topic_name, batch_values)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        with self.driver.topic_client.reader(utils.topic_name, consumer="test-consumer") as reader:
            first = reader.receive_message(timeout=30)
            second = reader.receive_message(timeout=30)
            assert [first.data, second.data] == batch_values[:2]
            reader.commit(first)
            reader.commit(second)

        self.change_cluster_version()

        assert read_messages(
            self.driver,
            utils.topic_name,
            "test-consumer",
            len(batch_values) - 2,
        ) == batch_values[2:]

    # Disable batching after writing a physical Kafka batch and read it with tight client-side count
    # and byte limits, verifying that the cut path returns logical messages without offset gaps.
    def test_kafka_batch_cut_respects_count_and_bytes_limits_after_flag_disable(self):
        utils = Workload(self)
        utils.create_topic(consumers=("count-limit-consumer", "bytes-limit-consumer"))

        batch_values = [
            f"topic-batch-limit-{i}-".encode("utf-8") + (b"x" * 4096)
            for i in range(5)
        ]
        write_kafka_batch(self.driver, utils.topic_name, batch_values)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        count_limited_messages, count_limited_batch_sizes = read_messages_in_batches(
            self.driver,
            utils.topic_name,
            "count-limit-consumer",
            len(batch_values),
            max_messages=1,
        )
        assert count_limited_messages == batch_values
        assert count_limited_batch_sizes == [1] * len(batch_values)

        bytes_limited_messages, bytes_limited_batch_sizes = read_messages_in_batches(
            self.driver,
            utils.topic_name,
            "bytes-limit-consumer",
            len(batch_values),
            max_bytes=1024,
        )
        assert bytes_limited_messages == batch_values
        assert bytes_limited_batch_sizes == [1] * len(batch_values)

    # Commit a physical Kafka batch through a YDB transaction, disable batching, and verify that the
    # committed transactional batch is cut into logical messages for normal topic readers.
    def test_transactional_kafka_batch_is_read_after_batching_flag_disable(self):
        utils = Workload(self)
        utils.create_topic(consumers=("test-consumer", "batch-consumer"))

        batch_values = [
            f"tx-topic-batch-message-{i}".encode("utf-8")
            for i in range(5)
        ]
        write_kafka_batch_in_transaction(self.driver, utils.topic_name, batch_values)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        assert read_messages(
            self.driver,
            utils.topic_name,
            "test-consumer",
            len(batch_values),
        ) == batch_values

        batches = read_topic_batches(self.driver, utils.topic_name, "batch-consumer")
        messages = [
            message
            for batch in batches
            for message in batch.message_data
        ]

        assert len(messages) == 1
        assert batches[0].codec == TOPIC_BATCHING_CODEC
        assert read_kafka_batch_payload_values(messages[0].data) == batch_values

    # Commit a physical Kafka batch through a YDB transaction, disable batching, commit only a prefix
    # of the logical messages, restart, and verify reading resumes from the middle of the batch.
    def test_transactional_kafka_batch_commit_middle_after_batching_disable(self):
        utils = Workload(self)
        utils.create_topic()

        batch_values = [
            f"tx-topic-batch-commit-middle-{i}".encode("utf-8")
            for i in range(5)
        ]
        write_kafka_batch_in_transaction(self.driver, utils.topic_name, batch_values)

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        with self.driver.topic_client.reader(utils.topic_name, consumer="test-consumer") as reader:
            first = reader.receive_message(timeout=30)
            second = reader.receive_message(timeout=30)
            assert [first.data, second.data] == batch_values[:2]
            reader.commit(first)
            reader.commit(second)

        self.change_cluster_version()

        assert read_messages(
            self.driver,
            utils.topic_name,
            "test-consumer",
            len(batch_values) - 2,
        ) == batch_values[2:]

    # Roll back a transactional physical Kafka batch, commit a second batch, disable batching, and
    # verify the rolled-back batch does not appear while the committed batch is cut correctly.
    def test_transactional_kafka_batch_rollback_is_not_read_after_batching_disable(self):
        utils = Workload(self)
        utils.create_topic()

        rolled_back_values = [
            f"tx-rolled-back-batch-message-{i}".encode("utf-8")
            for i in range(5)
        ]
        committed_values = [
            f"tx-committed-batch-message-{i}".encode("utf-8")
            for i in range(5)
        ]
        write_kafka_batch_and_rollback_transaction(
            self.driver,
            utils.topic_name,
            rolled_back_values,
            producer_id="tx-rollback-batch-producer",
        )
        write_kafka_batch_in_transaction(
            self.driver,
            utils.topic_name,
            committed_values,
            base_sequence=100,
            producer_id="tx-committed-batch-producer",
        )

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        read = read_messages(
            self.driver,
            utils.topic_name,
            "test-consumer",
            len(committed_values),
        )
        assert read == committed_values
        assert set(read).isdisjoint(rolled_back_values)

    # Mix transactional plain writes and a transactional physical Kafka batch in one topic, disable
    # batching, and verify the normal reader sees the logical stream across the transaction boundary.
    def test_transactional_plain_and_batched_messages_survive_batching_flag_disable(self):
        utils = Workload(self)
        utils.create_topic()

        plain_before = [
            f"tx-plain-before-{i}"
            for i in range(3)
        ]
        batch_values = [
            f"tx-batch-message-{i}".encode("utf-8")
            for i in range(5)
        ]
        plain_after = [
            f"tx-plain-after-{i}"
            for i in range(3)
        ]
        write_raw_messages_in_transaction(
            self.driver,
            utils.topic_name,
            plain_before,
            producer_id="tx-plain-before-producer",
        )
        write_kafka_batch_in_transaction(
            self.driver,
            utils.topic_name,
            batch_values,
            base_sequence=100,
            producer_id="tx-kafka-batch-producer",
        )
        write_raw_messages_in_transaction(
            self.driver,
            utils.topic_name,
            plain_after,
            producer_id="tx-plain-after-producer",
        )

        set_feature_flags(self.config, **{BATCHING_FLAG: False})
        self.change_cluster_version()

        assert read_messages(
            self.driver,
            utils.topic_name,
            "test-consumer",
            len(plain_before) + len(batch_values) + len(plain_after),
        ) == [
            message.encode("utf-8")
            for message in plain_before
        ] + batch_values + [
            message.encode("utf-8")
            for message in plain_after
        ]


class TestTopicTransactionMidBlobRead(CurrentToCurrentVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    # Reproduce the #48508 shape for regular topic reads: write a non-zero parent offset prefix,
    # commit a transaction that stores messages from a supportive partition, restart, then start
    # read sessions from offsets inside the tx-written blob and verify returned offsets stay in
    # parent key space.
    def test_mid_blob_read_after_transaction_commit_uses_parent_offsets(self):
        utils = Workload(self)
        consumers = (
            "tx-mid-blob-consumer-1",
            "tx-mid-blob-consumer-50",
            "tx-mid-blob-consumer-150",
            "tx-mid-blob-consumer-last",
        )
        utils.create_topic(consumers=consumers)

        prefix_count = 1000
        tx_count = 300
        prefix_messages = [
            f"tx-mid-blob-prefix-{i}"
            for i in range(prefix_count)
        ]
        tx_messages = [
            f"tx-mid-blob-transaction-{i}-" + ("x" * 4096)
            for i in range(tx_count)
        ]

        write_raw_messages(
            self.driver,
            utils.topic_name,
            prefix_messages,
            producer_id="tx-mid-blob-prefix-producer",
        )
        write_raw_messages_in_transaction(
            self.driver,
            utils.topic_name,
            tx_messages,
            producer_id="tx-mid-blob-transaction-producer",
        )
        wait_topic_end_offset(self.driver, utils.topic_name, prefix_count + tx_count)

        restart_cluster_with_current_config(self)

        for consumer, delta in [
            ("tx-mid-blob-consumer-1", 1),
            ("tx-mid-blob-consumer-50", 50),
            ("tx-mid-blob-consumer-150", 150),
            ("tx-mid-blob-consumer-last", tx_count - 1),
        ]:
            read_offset = prefix_count + delta
            read_result = read_messages_with_offsets_from_read_offset(
                self.driver,
                utils.topic_name,
                consumer,
                read_offset,
                tx_count - delta,
                timeout=60,
            )
            assert [offset for offset, _ in read_result] == list(
                range(read_offset, prefix_count + tx_count)
            )
            assert [data for _, data in read_result] == [
                message.encode("utf-8")
                for message in tx_messages[delta:]
            ]


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


class BlobSerializationWorkload:
    """Write interleaved small/large messages with metadata, then verify after tablet/version restarts."""

    CONSUMER_BEFORE = "blob-serialization-consumer-before"
    CONSUMER_AFTER = "blob-serialization-consumer-after"
    PRODUCER_ID = "blob-serialization-producer"

    # Interleave tiny and large payloads so body blobs mix PartData / packing paths.
    # Sizes around 512KiB exercise multipart client blobs.
    MESSAGE_SPECS = (
        (1, False),
        (512 * 1024, True),
        (2, False),
        (1024 * 1024, True),
        (100, False),
        (512 * 1024 + 1, True),
        (255, False),
        (2 * 1024 * 1024, True),
        (0, False),
        (64 * 1024, False),
        (3, False),
        (512 * 1024 - 1, True),
        (10, False),
        (1024 * 1024 + 17, True),
        (7, False),
    )

    def __init__(self, fixture, seed=42):
        self.fixture = fixture
        self.rng = random.Random(seed)
        self.topic_name = f"blob_serialization_topic_{uuid.uuid4().hex}"
        self.expected = []  # list of dicts: offset, seqno, data, metadata_items

    @property
    def driver(self):
        return self.fixture.driver

    def create_topic(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"CREATE TOPIC `{self.topic_name}` "
                f"(CONSUMER `{self.CONSUMER_BEFORE}`, CONSUMER `{self.CONSUMER_AFTER}`) "
                f"WITH (MIN_ACTIVE_PARTITIONS = 1);"
            )

    def _make_payload(self, index, size, incompressible):
        if size == 0:
            return b""
        if incompressible:
            # Distinct pseudo-random bytes (columnar pack often falls back to EUncompressed).
            if hasattr(self.rng, "randbytes"):
                return self.rng.randbytes(size)
            return bytes(self.rng.getrandbits(8) for _ in range(size))
        # Highly compressible — prefers ECompressed packing.
        return bytes([index % 256]) * size

    def _make_metadata(self, index):
        kind = index % 4
        if kind == 0:
            return {}
        if kind == 1:
            return {"idx": str(index), "tag": "small-meta"}
        if kind == 2:
            return {
                "idx": str(index),
                "bin": bytes([index % 256, (index * 3) % 256, 0xFF]),
                "unicode": f"значение-{index}",
            }
        return {
            "idx": str(index),
            "long": ("x" * 200) + str(index),
            "empty": "",
        }

    @staticmethod
    def _normalize_metadata(metadata):
        if not metadata:
            return {}
        result = {}
        for key, value in metadata.items():
            if isinstance(key, (bytes, bytearray)):
                key = key.decode("utf-8")
            else:
                key = str(key)
            if isinstance(value, bytes):
                result[key] = value
            elif isinstance(value, bytearray):
                result[key] = bytes(value)
            else:
                result[key] = value.encode("utf-8")
        return result

    def write_all(self):
        self.expected.clear()
        with self.driver.topic_client.writer(
            self.topic_name,
            producer_id=self.PRODUCER_ID,
            partition_id=0,
            auto_seqno=False,
        ) as writer:
            for index, (size, incompressible) in enumerate(self.MESSAGE_SPECS):
                data = self._make_payload(index, size, incompressible)
                metadata = self._make_metadata(index)
                seqno = index + 1
                writer.write(
                    ydb.TopicWriterMessage(
                        data,
                        metadata_items=metadata,
                        seqno=seqno,
                    )
                )
                self.expected.append(
                    {
                        "offset": index,
                        "seqno": seqno,
                        "data": data,
                        "metadata_items": self._normalize_metadata(metadata),
                    }
                )
                # Flush periodically so several body blobs are persisted, not one giant head.
                if index % 3 == 2:
                    writer.flush()
            writer.flush()

        self._wait_written(len(self.expected))

    def _wait_written(self, expected_count, timeout_sec=120):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            try:
                describe = self.driver.topic_client.describe_topic(self.topic_name, include_stats=True)
                end = sum(p.partition_stats.partition_end for p in describe.partitions)
                if end >= expected_count:
                    return
            except Exception as exc:
                logger.info("describe_topic while waiting write: %s", exc)
            time.sleep(1)
        raise AssertionError(f"messages were not persisted in time: want {expected_count}")

    def restart_pq_tablets(self):
        response = self.fixture.cluster.client.tablet_state(TabletTypes.PERSQUEUE)
        tablet_ids = [info.TabletId for info in response.TabletStateInfo]
        assert tablet_ids, "no PERSQUEUE tablets found"
        for tablet_id in tablet_ids:
            logger.info("Restarting PERSQUEUE tablet %s", tablet_id)
            self.fixture.cluster.client.tablet_kill(tablet_id)

        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                self.driver.topic_client.describe_topic(self.topic_name, include_stats=True)
                return
            except Exception as exc:
                logger.info("topic not ready after tablet restart: %s", exc)
                time.sleep(1)
        raise AssertionError("topic did not become ready after PQ tablet restart")

    def read_and_verify(self, consumer, timeout_sec=180):
        received = []
        deadline = time.time() + timeout_sec
        with self.driver.topic_client.reader(self.topic_name, consumer=consumer) as reader:
            while len(received) < len(self.expected) and time.time() < deadline:
                try:
                    message = reader.receive_message(timeout=5)
                except TimeoutError:
                    continue
                data = message.data if isinstance(message.data, bytes) else bytes(message.data)
                received.append(
                    {
                        "offset": message.offset,
                        "seqno": message.seqno,
                        "data": data,
                        "metadata_items": self._normalize_metadata(message.metadata_items or {}),
                    }
                )
                reader.commit(message)

        assert len(received) == len(self.expected), (
            f"message count mismatch: got {len(received)}, want {len(self.expected)}"
        )
        for i, (actual, expect) in enumerate(zip(received, self.expected)):
            assert actual["offset"] == expect["offset"], (
                f"msg[{i}] offset: got {actual['offset']}, want {expect['offset']}"
            )
            assert actual["seqno"] == expect["seqno"], (
                f"msg[{i}] seqno: got {actual['seqno']}, want {expect['seqno']}"
            )
            assert actual["data"] == expect["data"], (
                f"msg[{i}] data mismatch: got len={len(actual['data'])}, "
                f"want len={len(expect['data'])}"
            )
            assert actual["metadata_items"] == expect["metadata_items"], (
                f"msg[{i}] metadata mismatch: got {actual['metadata_items']!r}, "
                f"want {expect['metadata_items']!r}"
            )


class BlobSerializationTxWorkload(BlobSerializationWorkload):
    """Same verification as BlobSerializationWorkload, but messages are written via several large txs."""

    CONSUMER_BEFORE = "blob-serialization-tx-consumer-before"
    CONSUMER_AFTER = "blob-serialization-tx-consumer-after"
    PRODUCER_ID = "blob-serialization-tx-producer"
    MIN_TX_BYTES = 16 * 1024 * 1024

    # Each entry is one transaction: several messages, total payload > 16 MiB.
    # Small/large and compressible/incompressible are interleaved inside a tx.
    _MB = 1024 * 1024
    TRANSACTION_SPECS = (
        (
            (1, False),
            (4 * _MB, True),
            (2, False),
            (4 * _MB + 1, True),
            (100, False),
            (4 * _MB - 1, False),
            (512 * 1024, True),
            (5 * _MB, True),
        ),
        (
            (3, False),
            (5 * _MB, True),
            (10, False),
            (5 * _MB, False),
            (64 * 1024, False),
            (6 * _MB + 17, True),
            (7, False),
        ),
        (
            (0, False),
            (3 * _MB, True),
            (255, False),
            (8 * _MB, True),
            (1024 * 1024, False),
            (5 * _MB - 100, True),
            (11, False),
        ),
    )

    def __init__(self, fixture, seed=43):
        super().__init__(fixture, seed=seed)
        self.topic_name = f"blob_serialization_tx_topic_{uuid.uuid4().hex}"

    def write_all(self):
        self.expected.clear()
        offset = 0
        seqno = 1

        with ydb.QuerySessionPool(self.driver) as session_pool:
            for tx_index, specs in enumerate(self.TRANSACTION_SPECS):
                batch = []
                for size, incompressible in specs:
                    data = self._make_payload(offset, size, incompressible)
                    metadata = self._make_metadata(offset)
                    batch.append((data, metadata, seqno, offset))
                    seqno += 1
                    offset += 1

                total_bytes = sum(len(item[0]) for item in batch)
                assert total_bytes > self.MIN_TX_BYTES, (
                    f"tx[{tx_index}] payload too small: {total_bytes} <= {self.MIN_TX_BYTES}"
                )
                assert len(batch) > 1, f"tx[{tx_index}] must contain several messages"

                def callee(tx, messages=batch):
                    writer = self.driver.topic_client.tx_writer(
                        tx,
                        self.topic_name,
                        producer_id=self.PRODUCER_ID,
                        partition_id=0,
                        auto_seqno=False,
                    )
                    for data, metadata, message_seqno, _ in messages:
                        writer.write(
                            ydb.TopicWriterMessage(
                                data,
                                metadata_items=metadata,
                                seqno=message_seqno,
                            )
                        )

                session_pool.retry_tx_sync(callee)
                logger.info(
                    "Committed blob-serialization tx[%s] with %s messages (%s bytes)",
                    tx_index,
                    len(batch),
                    total_bytes,
                )

                for data, metadata, message_seqno, message_offset in batch:
                    self.expected.append(
                        {
                            "offset": message_offset,
                            "seqno": message_seqno,
                            "data": data,
                            "metadata_items": self._normalize_metadata(metadata),
                        }
                    )

        self._wait_written(len(self.expected), timeout_sec=300)


class TestTopicBlobSerializationRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(use_in_memory_pdisks=False)

    def test_write_restart_upgrade_downgrade_preserves_messages(self):
        workload = BlobSerializationWorkload(self)

        workload.create_topic()
        workload.write_all()

        workload.restart_pq_tablets()
        workload.read_and_verify(workload.CONSUMER_BEFORE)

        self.change_cluster_version()

        workload.restart_pq_tablets()
        workload.read_and_verify(workload.CONSUMER_AFTER)


class TestTopicBlobSerializationTxRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(use_in_memory_pdisks=False)

    def test_tx_write_restart_upgrade_downgrade_preserves_messages(self):
        workload = BlobSerializationTxWorkload(self)

        workload.create_topic()
        workload.write_all()

        workload.restart_pq_tablets()
        workload.read_and_verify(workload.CONSUMER_BEFORE, timeout_sec=600)

        self.change_cluster_version()

        workload.restart_pq_tablets()
        workload.read_and_verify(workload.CONSUMER_AFTER, timeout_sec=600)

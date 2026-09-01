# -*- coding: utf-8 -*-
"""Compatibility tests for Topic DirectRead across mixed binary versions.

Existing topic compatibility tests did not catch #50054 / #50053:

* ``TestTopicRollingUpdate`` uses the Python TopicReader. DirectRead is off,
  so prepare and serve are the same CmdRead. A size-limited mid-blob stop
  only splits the stream across responses; the next request continues from
  the last offset and nothing is lost.
* Messages in those tests are tiny (``message-{timestamp}``), so a blob
  rarely contains a remainder that ``ReadToBlobEnd`` would drain.
* ``MixedClusterFixture`` was not used for regular topic reads, only for
  Kafka. The bug needs a 26-2 proxy (omits ``ReadToBlobEnd``) talking to a
  26-3 tablet (proto default ``false`` stops mid-blob).
* DirectRead retries of the same ``DirectReadId`` must return the same
  records. Prepare-with-read-to-blob-end plus serve/retry-without it made
  the staged result larger than what the client got, and the session
  skipped the remainder of the blob.

This module drives DirectRead over gRPC with a byte limit that stops
mid-blob when ``ReadToBlobEnd`` is false. Scenarios cover mixed proxies,
tablet moves, restart, rolling upgrade/downgrade, commits, mid-blob
offsets, codecs, transactions and several partitions.
"""
import asyncio
import gzip
import logging
import os
import time
import uuid

import pytest

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.compatibility.fixtures import (
    MixedClusterFixture,
    RestartToAnotherVersionFixture,
    RollingDowngradeAndUpgradeFixture,
    RollingUpgradeAndDowngradeFixture,
    string_version_to_tuple,
)
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb import _apis
from ydb._grpc.grpcwrapper.common_utils import GrpcWrapperAsyncIO
from ydb._grpc.grpcwrapper.ydb_topic import Codec

from test_topic import wait_topic_end_offset, write_raw_messages, write_raw_messages_in_transaction

try:
    from ydb.public.api.protos import ydb_topic_pb2
except ImportError:
    from contrib.ydb.public.api.protos import ydb_topic_pb2


logger = logging.getLogger(__name__)

STABLE_26_2 = string_version_to_tuple("stable-26-2")

# Large enough to fill PQ blobs; small ReadRequest.bytes_size then stops
# mid-blob when ReadToBlobEnd is false (1 message) and drains the blob when
# it is true (the 26-2 behavior / the mixed-version contract).
MESSAGE_PAYLOAD_SIZE = 512 * 1024
MESSAGE_COUNT = 24
STEP_MESSAGE_COUNT = 8
SMALL_MESSAGE_COUNT = 16
SMALL_PAYLOAD_SIZE = 64
DIRECT_READ_BYTES_SIZE = 256 * 1024


class _Proto:
    def __init__(self, proto):
        self._proto = proto

    def to_proto(self):
        return self._proto


def _skip_if_direct_read_unsupported(versions):
    if min(versions) < STABLE_26_2:
        pytest.skip("DirectRead is required since stable-26-2")


def _create_topic(driver, topic_name, consumers, partition_count=1):
    if isinstance(consumers, str):
        consumers = (consumers,)
    consumers_sql = ", ".join(f"CONSUMER `{consumer}`" for consumer in consumers)
    with ydb.QuerySessionPool(driver) as session_pool:
        session_pool.execute_with_retries(
            f"CREATE TOPIC `{topic_name}` ({consumers_sql}) "
            f"WITH (MIN_ACTIVE_PARTITIONS = {partition_count});"
        )


def _payloads(count=MESSAGE_COUNT, prefix="direct-read-blob", size=MESSAGE_PAYLOAD_SIZE, compressible=True):
    body = (b"x" * size) if compressible else os.urandom(size)
    return [
        f"{prefix}-{index:04d}-".encode("utf-8") + body
        for index in range(count)
    ]


def _write_partition(driver, topic_name, values, partition_id, producer_id=None, codec=ydb.TopicCodec.RAW):
    with driver.topic_client.writer(
        topic_name,
        producer_id=producer_id or f"producer-{uuid.uuid4().hex}",
        partition_id=partition_id,
        codec=codec,
    ) as writer:
        for value in values:
            writer.write(ydb.TopicWriterMessage(value))


def _extract_messages(partition_data):
    messages = []
    for batch in partition_data.batches:
        for message_data in batch.message_data:
            data = message_data.data
            if batch.codec == Codec.CODEC_GZIP:
                data = gzip.decompress(data)
            messages.append((message_data.offset, data))
    return messages


def _driver_for_node(fixture, node_id):
    node = fixture.cluster.nodes.get(node_id)
    if node is None:
        raise KeyError(f"no cluster node with id {node_id}")
    driver = ydb.Driver(
        ydb.DriverConfig(
            database=fixture.database_path,
            endpoint="grpc://%s:%s" % (node.host, node.port),
            disable_discovery=True,
        )
    )
    driver.wait(timeout=30)
    return driver


def _node_ids(fixture):
    return list(fixture.cluster.nodes.keys())


def _proxy_node_ids(fixture):
    """One node per binary so mixed clusters hit both old and new Topic proxies."""
    by_binary = {}
    for node in fixture.cluster.nodes.values():
        by_binary.setdefault(node.binary_path, node.node_id)
    return list(by_binary.values())


def _check_status(name, message):
    status = getattr(message, "status", 0)
    if status not in (0, StatusIds.SUCCESS):
        raise AssertionError(
            f"{name} status={status} issues={getattr(message, 'issues', None)} "
            f"kind={message.WhichOneof('server_message')}"
        )


async def _open_direct_read_stream(driver, session_id, topic_name, consumer):
    stream = GrpcWrapperAsyncIO(lambda message: message)
    await stream.start(driver, _apis.TopicService.Stub, "StreamDirectRead")
    request = ydb_topic_pb2.StreamDirectReadMessage.FromClient()
    request.init_request.session_id = session_id
    request.init_request.consumer = consumer
    topic = request.init_request.topics_read_settings.add()
    topic.path = topic_name
    stream.write(_Proto(request))

    deadline = time.time() + 30
    while time.time() < deadline:
        response = await stream.receive(timeout=5)
        _check_status("StreamDirectRead", response)
        kind = response.WhichOneof("server_message")
        if kind == "init_response":
            return stream
        if kind == "stop_direct_read_partition_session":
            continue
        logger.info("unexpected StreamDirectRead message during init: %s", kind)
    stream.close()
    raise TimeoutError("no StreamDirectRead InitResponse")


async def _start_direct_read_partition(stream, partition_session_id, generation, last_direct_read_id):
    request = ydb_topic_pb2.StreamDirectReadMessage.FromClient()
    start = request.start_direct_read_partition_session_request
    start.partition_session_id = partition_session_id
    start.last_direct_read_id = last_direct_read_id
    start.generation = generation
    stream.write(_Proto(request))


def _restart_pq_tablets(fixture, topic_name):
    response = fixture.cluster.client.tablet_state(TabletTypes.PERSQUEUE)
    tablet_ids = [info.TabletId for info in response.TabletStateInfo]
    assert tablet_ids, "no PERSQUEUE tablets found"
    for tablet_id in tablet_ids:
        logger.info("Restarting PERSQUEUE tablet %s", tablet_id)
        fixture.cluster.client.tablet_kill(tablet_id)

    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            fixture.driver.topic_client.describe_topic(topic_name, include_stats=True)
            return
        except Exception as exc:
            logger.info("topic not ready after tablet restart: %s", exc)
            time.sleep(1)
    raise AssertionError("topic did not become ready after PQ tablet restart")


async def _pump(name, stream, queue):
    try:
        while True:
            message = await stream.receive()
            await queue.put((name, message, None))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await queue.put((name, None, exc))


async def _direct_read_messages_async(
    fixture,
    topic_name,
    consumer,
    expected_count,
    read_offset=0,
    bytes_size=DIRECT_READ_BYTES_SIZE,
    timeout=180,
    restart_tablets_after=None,
    control_node_id=None,
    commit=False,
    max_offset=None,
    read_partition_ids=None,
    idle_timeout=None,
):
    """Read via StreamRead(direct_read=true) + StreamDirectRead on the partition node.

    ``read_offset=None`` leaves StartPartitionSessionResponse.read_offset unset
    so the server continues from the committed offset.
    """
    control = GrpcWrapperAsyncIO(lambda message: message)
    node_drivers = {}
    data_streams = {}
    pumps = []
    started_partitions = set()
    received = []
    session_id = None
    last_direct_read_id = {}
    session_to_partition = {}
    tablets_restarted = False
    pending_commits = 0
    started = False
    last_progress = time.time()
    incoming = asyncio.Queue()
    own_control_driver = False
    control_driver = fixture.driver
    if control_node_id is not None:
        control_driver = _driver_for_node(fixture, control_node_id)
        own_control_driver = True

    def send_read():
        request = ydb_topic_pb2.StreamReadMessage.FromClient()
        request.read_request.bytes_size = bytes_size
        control.write(_Proto(request))

    def send_commit(partition_session_id, start, end):
        nonlocal pending_commits
        request = ydb_topic_pb2.StreamReadMessage.FromClient()
        part = request.commit_offset_request.commit_offsets.add()
        part.partition_session_id = partition_session_id
        offsets = part.offsets.add()
        offsets.start = start
        offsets.end = end
        control.write(_Proto(request))
        pending_commits += 1

    def start_pump(name, stream):
        pumps.append(asyncio.create_task(_pump(name, stream, incoming)))

    def should_stop():
        if len(received) < expected_count:
            return False
        if expected_count == 0 and not started:
            return False
        if idle_timeout is not None and time.time() - last_progress < idle_timeout:
            return False
        if commit and pending_commits > 0:
            return False
        return True

    async def open_data_stream(node_id):
        if node_id not in node_drivers:
            node_drivers[node_id] = _driver_for_node(fixture, node_id)
        if node_id not in data_streams:
            data_streams[node_id] = await _open_direct_read_stream(
                node_drivers[node_id],
                session_id,
                topic_name,
                consumer,
            )
            start_pump(("data", node_id), data_streams[node_id])
        return data_streams[node_id]

    async def ensure_data_stream(preferred_node_id, partition_session_id, generation):
        candidates = [preferred_node_id] + [node_id for node_id in _node_ids(fixture) if node_id != preferred_node_id]
        last_error = None
        for node_id in candidates:
            if node_id not in fixture.cluster.nodes:
                continue
            key = (node_id, partition_session_id, generation)
            if key in started_partitions:
                return
            try:
                stream = await open_data_stream(node_id)
                last_id = last_direct_read_id.get(partition_session_id, 0)
                await _start_direct_read_partition(stream, partition_session_id, generation, last_id)
                started_partitions.add(key)
                return
            except Exception as exc:
                last_error = exc
                logger.info("DirectRead stream to node %s failed: %s", node_id, exc)
        raise last_error or RuntimeError("no node accepted StreamDirectRead")

    try:
        await control.start(control_driver, _apis.TopicService.Stub, _apis.TopicService.StreamRead)
        start_pump("control", control)

        init = ydb_topic_pb2.StreamReadMessage.FromClient()
        init.init_request.consumer = consumer
        init.init_request.direct_read = True
        init.init_request.auto_partitioning_support = True
        topic = init.init_request.topics_read_settings.add()
        topic.path = topic_name
        if read_partition_ids:
            topic.partition_ids.extend(read_partition_ids)
        control.write(_Proto(init))

        deadline = time.time() + timeout
        while not should_stop() and time.time() < deadline:
            remaining = max(0.5, deadline - time.time())
            try:
                name, message, error = await asyncio.wait_for(incoming.get(), timeout=min(5, remaining))
            except TimeoutError:
                continue
            if error is not None:
                logger.info("direct read stream %s failed: %s", name, error)
                continue

            status = getattr(message, "status", 0)
            if status not in (0, StatusIds.SUCCESS):
                # Tablet moves and mixed-version retries surface as stream errors;
                # UpdatePartitionSession / a new StreamDirectRead should recover.
                logger.info(
                    "%s status=%s issues=%s kind=%s",
                    name,
                    status,
                    getattr(message, "issues", None),
                    message.WhichOneof("server_message"),
                )
                continue

            if name == "control":
                kind = message.WhichOneof("server_message")
                if kind == "init_response":
                    session_id = message.init_response.session_id
                elif kind == "start_partition_session_request":
                    start = message.start_partition_session_request
                    location = start.partition_location
                    session_to_partition[start.partition_session.partition_session_id] = (
                        start.partition_session.partition_id
                    )
                    response = ydb_topic_pb2.StreamReadMessage.FromClient()
                    confirm = response.start_partition_session_response
                    confirm.partition_session_id = start.partition_session.partition_session_id
                    if read_offset is not None:
                        confirm.read_offset = read_offset
                    if max_offset is not None:
                        confirm.max_offset = max_offset
                    control.write(_Proto(response))
                    if not session_id:
                        raise AssertionError("StartPartitionSessionRequest before InitResponse")
                    await ensure_data_stream(
                        location.node_id,
                        start.partition_session.partition_session_id,
                        location.generation,
                    )
                    started = True
                    last_progress = time.time()
                    send_read()
                elif kind == "update_partition_session":
                    update = message.update_partition_session
                    await ensure_data_stream(
                        update.partition_location.node_id,
                        update.partition_session_id,
                        update.partition_location.generation,
                    )
                elif kind == "stop_partition_session_request":
                    stop = message.stop_partition_session_request
                    response = ydb_topic_pb2.StreamReadMessage.FromClient()
                    confirm = response.stop_partition_session_response
                    confirm.partition_session_id = stop.partition_session_id
                    confirm.graceful = stop.graceful
                    control.write(_Proto(response))
                elif kind == "commit_offset_response":
                    pending_commits = max(0, pending_commits - 1)
                elif kind == "read_response":
                    raise AssertionError(
                        "control session delivered ReadResponse with direct_read=true; "
                        "DirectRead data must arrive via StreamDirectRead"
                    )
                elif kind is not None:
                    logger.info("control session message: %s", kind)
                continue

            kind = message.WhichOneof("server_message")
            if kind == "direct_read_response":
                data = message.direct_read_response
                batch = _extract_messages(data.partition_data)
                partition_id = session_to_partition.get(data.partition_session_id, 0)
                received.extend((partition_id, offset, payload) for offset, payload in batch)
                last_direct_read_id[data.partition_session_id] = data.direct_read_id
                last_progress = time.time()
                if commit and batch:
                    send_commit(data.partition_session_id, batch[0][0], batch[-1][0] + 1)
                ack = ydb_topic_pb2.StreamReadMessage.FromClient()
                ack.direct_read_ack.partition_session_id = data.partition_session_id
                ack.direct_read_ack.direct_read_id = data.direct_read_id
                control.write(_Proto(ack))
                send_read()
                if (
                    restart_tablets_after is not None
                    and not tablets_restarted
                    and len(received) >= restart_tablets_after
                ):
                    tablets_restarted = True
                    await asyncio.get_running_loop().run_in_executor(
                        None,
                        _restart_pq_tablets,
                        fixture,
                        topic_name,
                    )
            elif kind == "start_direct_read_partition_session_response":
                send_read()
            elif kind == "stop_direct_read_partition_session":
                logger.info(
                    "StopDirectReadPartitionSession partition_session_id=%s",
                    message.stop_direct_read_partition_session.partition_session_id,
                )
            elif kind is not None:
                logger.info("data session message: %s", kind)

        if len(received) < expected_count:
            raise AssertionError(
                f"DirectRead got {len(received)} messages, expected at least {expected_count}; "
                f"messages={[(pid, offset) for pid, offset, _ in received]!r}"
            )
        if commit and pending_commits > 0:
            raise AssertionError(f"DirectRead timed out waiting for {pending_commits} commit responses")
        return received
    finally:
        control.close()
        for stream in data_streams.values():
            stream.close()
        for task in pumps:
            task.cancel()
        for task in pumps:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        for driver in node_drivers.values():
            driver.stop()
        if own_control_driver:
            control_driver.stop()


def direct_read_messages(fixture, topic_name, consumer, expected_count, **kwargs):
    return asyncio.run(_direct_read_messages_async(
        fixture,
        topic_name,
        consumer,
        expected_count,
        **kwargs,
    ))


def _partition_messages(received, partition_id=0):
    return [(offset, payload) for pid, offset, payload in received if pid == partition_id]


def _assert_direct_read_complete(received, expected_payloads, start_offset=0, partition_id=0):
    part = _partition_messages(received, partition_id)
    offsets = [offset for offset, _ in part]
    payloads = [data for _, data in part]
    expected_offsets = list(range(start_offset, start_offset + len(expected_payloads)))
    assert offsets == expected_offsets, (
        f"partition {partition_id}: DirectRead skipped or reordered offsets: "
        f"got {offsets}, want {expected_offsets}"
    )
    assert payloads == expected_payloads, (
        f"partition {partition_id}: DirectRead payload mismatch: "
        f"got {len(payloads)} messages, want {len(expected_payloads)}"
    )


def _prepare_topic(fixture, consumer, partition_count=1, extra_consumers=()):
    topic_name = f"direct_read_{uuid.uuid4().hex}"
    consumers = (consumer,) + tuple(extra_consumers)
    _create_topic(fixture.driver, topic_name, consumers, partition_count=partition_count)
    return topic_name


def _write_blob_messages(
    fixture,
    topic_name,
    count=MESSAGE_COUNT,
    prefix="direct-read-blob",
    partition_id=None,
    size=MESSAGE_PAYLOAD_SIZE,
    codec=ydb.TopicCodec.RAW,
    compressible=True,
):
    expected = _payloads(count=count, prefix=prefix, size=size, compressible=compressible)
    if partition_id is None and codec == ydb.TopicCodec.RAW:
        write_raw_messages(fixture.driver, topic_name, expected, producer_id=prefix)
    else:
        _write_partition(
            fixture.driver,
            topic_name,
            expected,
            0 if partition_id is None else partition_id,
            producer_id=prefix,
            codec=codec,
        )
    return expected


def _roll_direct_read(fixture, write_each_step, commit=False, resume_from_commit=False):
    consumer = "rolling-direct-read-consumer"
    topic_name = _prepare_topic(fixture, consumer)
    expected = []
    received = []
    next_offset = 0

    def read_available(count):
        nonlocal next_offset
        if count <= 0:
            return
        kwargs = {
            "timeout": 120,
            "commit": commit or resume_from_commit,
        }
        if resume_from_commit:
            kwargs["read_offset"] = None
        else:
            kwargs["read_offset"] = next_offset
        chunk = direct_read_messages(
            fixture,
            topic_name,
            consumer,
            count,
            **kwargs,
        )
        received.extend(chunk)
        next_offset = received[-1][1] + 1

    for step, _ in enumerate(fixture.roll()):
        if write_each_step or step == 0:
            count = STEP_MESSAGE_COUNT if write_each_step else MESSAGE_COUNT
            chunk = _write_blob_messages(
                fixture,
                topic_name,
                count=count,
                prefix=f"roll-{step}",
            )
            expected.extend(chunk)
            wait_topic_end_offset(fixture.driver, topic_name, len(expected))
        remaining = len(expected) - len(received)
        if remaining:
            read_available(min(2, remaining) if not write_each_step else remaining)

    remaining = len(expected) - len(received)
    if remaining:
        read_available(remaining)
    _assert_direct_read_complete(received, expected)


class TestTopicDirectReadMixedCluster(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        _skip_if_direct_read_unsupported(self.versions)
        yield from self.setup_cluster()

    def test_direct_read_mid_blob_survives_tablet_move(self):
        consumer = "direct-read-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(self, topic_name)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected),
            restart_tablets_after=3,
        )
        _assert_direct_read_complete(received, expected)

    def test_direct_read_through_each_proxy_version(self):
        # 26-2 proxies omit ReadToBlobEnd; 26-3 tablets used to treat that as
        # false and stop mid-blob. Pin the control session to each binary.
        proxy_nodes = _proxy_node_ids(self)
        topic_name = _prepare_topic(
            self,
            "unused-consumer",
            extra_consumers=tuple(f"proxy-{node_id}" for node_id in proxy_nodes),
        )
        expected = _write_blob_messages(self, topic_name)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        for node_id in proxy_nodes:
            received = direct_read_messages(
                self,
                topic_name,
                f"proxy-{node_id}",
                len(expected),
                control_node_id=node_id,
            )
            _assert_direct_read_complete(received, expected)

    def test_direct_read_each_proxy_survives_tablet_move(self):
        proxy_nodes = _proxy_node_ids(self)
        topic_name = _prepare_topic(
            self,
            "unused-consumer",
            extra_consumers=tuple(f"proxy-kill-{node_id}" for node_id in proxy_nodes),
        )
        expected = _write_blob_messages(self, topic_name)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        for node_id in proxy_nodes:
            received = direct_read_messages(
                self,
                topic_name,
                f"proxy-kill-{node_id}",
                len(expected),
                control_node_id=node_id,
                restart_tablets_after=3,
            )
            _assert_direct_read_complete(received, expected)

    def test_direct_read_from_mid_blob_offset(self):
        consumer = "mid-blob-offset-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(self, topic_name)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        start_offset = 5
        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected) - start_offset,
            read_offset=start_offset,
            restart_tablets_after=2,
        )
        _assert_direct_read_complete(received, expected[start_offset:], start_offset=start_offset)

    def test_direct_read_commit_middle_then_resume(self):
        consumer = "commit-middle-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(self, topic_name)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        first = direct_read_messages(
            self,
            topic_name,
            consumer,
            2,
            commit=True,
        )
        rest = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected) - len(first),
            read_offset=None,
        )
        _assert_direct_read_complete(first + rest, expected)

    def test_direct_read_commit_all_then_empty_resume(self):
        consumer = "commit-all-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(self, topic_name, count=STEP_MESSAGE_COUNT)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        first = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected),
            commit=True,
        )
        _assert_direct_read_complete(first, expected)
        empty = direct_read_messages(
            self,
            topic_name,
            consumer,
            0,
            read_offset=None,
            idle_timeout=8,
            timeout=60,
        )
        assert empty == []

    def test_direct_read_after_transaction_write(self):
        consumer = "tx-direct-read-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _payloads(count=STEP_MESSAGE_COUNT, prefix="tx-direct-read")
        write_raw_messages_in_transaction(
            self.driver,
            topic_name,
            expected,
            producer_id="tx-direct-read-producer",
        )
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected),
            restart_tablets_after=2,
        )
        _assert_direct_read_complete(received, expected)

    def test_direct_read_two_partitions(self):
        consumer = "two-partition-consumer"
        topic_name = _prepare_topic(self, consumer, partition_count=2)
        expected_0 = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="part-0",
            partition_id=0,
        )
        expected_1 = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="part-1",
            partition_id=1,
        )
        wait_topic_end_offset(self.driver, topic_name, len(expected_0) + len(expected_1))

        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected_0) + len(expected_1),
            restart_tablets_after=3,
        )
        _assert_direct_read_complete(received, expected_0, partition_id=0)
        _assert_direct_read_complete(received, expected_1, partition_id=1)

    def test_direct_read_specific_partition(self):
        consumer = "one-partition-filter-consumer"
        topic_name = _prepare_topic(self, consumer, partition_count=2)
        _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="filtered-part-0",
            partition_id=0,
        )
        expected_1 = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="filtered-part-1",
            partition_id=1,
        )
        wait_topic_end_offset(self.driver, topic_name, 2 * STEP_MESSAGE_COUNT)

        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected_1),
            read_partition_ids=[1],
        )
        assert _partition_messages(received, 0) == []
        _assert_direct_read_complete(received, expected_1, partition_id=1)

    def test_direct_read_max_offset(self):
        consumer = "max-offset-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(self, topic_name, count=STEP_MESSAGE_COUNT)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        max_offset = 3
        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            max_offset + 1,
            max_offset=max_offset,
            idle_timeout=8,
        )
        _assert_direct_read_complete(received, expected[: max_offset + 1])

    def test_direct_read_gzip_mid_blob_survives_tablet_move(self):
        consumer = "gzip-direct-read-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="gzip-blob",
            codec=ydb.TopicCodec.GZIP,
            compressible=False,
        )
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected),
            restart_tablets_after=2,
        )
        _assert_direct_read_complete(received, expected)

    def test_direct_read_small_messages_survives_tablet_move(self):
        consumer = "small-direct-read-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(
            self,
            topic_name,
            count=SMALL_MESSAGE_COUNT,
            prefix="small",
            size=SMALL_PAYLOAD_SIZE,
        )
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected),
            restart_tablets_after=4,
        )
        _assert_direct_read_complete(received, expected)

    def test_direct_read_two_consumers(self):
        topic_name = _prepare_topic(
            self,
            "consumer-a",
            extra_consumers=("consumer-b",),
        )
        expected = _write_blob_messages(self, topic_name, count=STEP_MESSAGE_COUNT)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        for consumer in ("consumer-a", "consumer-b"):
            received = direct_read_messages(self, topic_name, consumer, len(expected))
            _assert_direct_read_complete(received, expected)

    def test_direct_read_then_append_more(self):
        consumer = "append-direct-read-consumer"
        topic_name = _prepare_topic(self, consumer)
        first_expected = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="append-first",
        )
        wait_topic_end_offset(self.driver, topic_name, len(first_expected))

        first = direct_read_messages(self, topic_name, consumer, len(first_expected), commit=True)
        _assert_direct_read_complete(first, first_expected)

        rest_expected = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="append-rest",
        )
        wait_topic_end_offset(self.driver, topic_name, len(first_expected) + len(rest_expected))

        rest = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(rest_expected),
            read_offset=None,
        )
        _assert_direct_read_complete(
            rest,
            rest_expected,
            start_offset=len(first_expected),
        )


class TestTopicDirectReadRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        _skip_if_direct_read_unsupported(self.versions)
        yield from self.setup_cluster()

    def test_write_then_direct_read_after_version_change(self):
        consumer = "restart-after-write-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(self, topic_name)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        self.change_cluster_version()

        received = direct_read_messages(self, topic_name, consumer, len(expected))
        _assert_direct_read_complete(received, expected)

    def test_direct_read_prefix_then_remainder_after_version_change(self):
        consumer = "restart-split-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(self, topic_name)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        first = direct_read_messages(self, topic_name, consumer, 2, commit=True)

        self.change_cluster_version()

        rest = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected) - len(first),
            read_offset=None,
        )
        _assert_direct_read_complete(first + rest, expected)

    def test_direct_read_from_mid_blob_offset_after_version_change(self):
        consumer = "restart-mid-offset-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(self, topic_name)
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        self.change_cluster_version()

        start_offset = 5
        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected) - start_offset,
            read_offset=start_offset,
        )
        _assert_direct_read_complete(received, expected[start_offset:], start_offset=start_offset)

    def test_direct_read_after_tx_write_and_version_change(self):
        consumer = "restart-tx-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _payloads(count=STEP_MESSAGE_COUNT, prefix="restart-tx")
        write_raw_messages_in_transaction(
            self.driver,
            topic_name,
            expected,
            producer_id="restart-tx-producer",
        )
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        self.change_cluster_version()

        received = direct_read_messages(self, topic_name, consumer, len(expected))
        _assert_direct_read_complete(received, expected)

    def test_direct_read_gzip_after_version_change(self):
        consumer = "restart-gzip-consumer"
        topic_name = _prepare_topic(self, consumer)
        expected = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="restart-gzip",
            codec=ydb.TopicCodec.GZIP,
            compressible=False,
        )
        wait_topic_end_offset(self.driver, topic_name, len(expected))

        self.change_cluster_version()

        received = direct_read_messages(self, topic_name, consumer, len(expected))
        _assert_direct_read_complete(received, expected)

    def test_direct_read_two_partitions_after_version_change(self):
        consumer = "restart-two-partition-consumer"
        topic_name = _prepare_topic(self, consumer, partition_count=2)
        expected_0 = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="restart-part-0",
            partition_id=0,
        )
        expected_1 = _write_blob_messages(
            self,
            topic_name,
            count=STEP_MESSAGE_COUNT,
            prefix="restart-part-1",
            partition_id=1,
        )
        wait_topic_end_offset(self.driver, topic_name, len(expected_0) + len(expected_1))

        self.change_cluster_version()

        received = direct_read_messages(
            self,
            topic_name,
            consumer,
            len(expected_0) + len(expected_1),
        )
        _assert_direct_read_complete(received, expected_0, partition_id=0)
        _assert_direct_read_complete(received, expected_1, partition_id=1)


class TestTopicDirectReadRolling(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        _skip_if_direct_read_unsupported(self.versions)
        yield from self.setup_cluster()

    def test_direct_read_mid_blob_during_rolling(self):
        _roll_direct_read(self, write_each_step=False)

    def test_write_and_direct_read_during_rolling(self):
        _roll_direct_read(self, write_each_step=True)

    def test_commit_and_resume_during_rolling(self):
        _roll_direct_read(self, write_each_step=False, resume_from_commit=True)


class TestTopicDirectReadRollingDowngrade(RollingDowngradeAndUpgradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        _skip_if_direct_read_unsupported(self.versions)
        yield from self.setup_cluster()

    def test_direct_read_mid_blob_during_downgrade(self):
        _roll_direct_read(self, write_each_step=False)

    def test_write_and_direct_read_during_downgrade(self):
        _roll_direct_read(self, write_each_step=True)

    def test_commit_and_resume_during_downgrade(self):
        _roll_direct_read(self, write_each_step=False, resume_from_commit=True)

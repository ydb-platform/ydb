# -*- coding: utf-8 -*-
import logging
import random
import time
import uuid

import pytest

from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
    RollingDowngradeAndUpgradeFixture,
    string_version_to_tuple,
)
from ydb.tests.oss.ydb_sdk_import import ydb

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

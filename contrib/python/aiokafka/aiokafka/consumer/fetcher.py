import asyncio
import collections
import contextlib
import logging
import random
import time
from itertools import chain

import async_timeout

import aiokafka.errors as Errors
from aiokafka.errors import ConsumerStoppedError, KafkaTimeoutError, RecordTooLargeError
from aiokafka.protocol.fetch import FetchRequest
from aiokafka.protocol.offset import OffsetRequest
from aiokafka.record.control_record import ABORT_MARKER, ControlRecord
from aiokafka.record.memory_records import MemoryRecords
from aiokafka.structs import ConsumerRecord, OffsetAndTimestamp, TopicPartition
from aiokafka.util import create_future, create_task

log = logging.getLogger(__name__)

UNKNOWN_OFFSET = -1

# Isolation levels
READ_UNCOMMITTED = 0
READ_COMMITTED = 1


class OffsetResetStrategy:
    LATEST = -1
    EARLIEST = -2
    NONE = 0

    @classmethod
    def from_str(cls, name):
        name = name.lower()
        if name == "latest":
            return cls.LATEST
        if name == "earliest":
            return cls.EARLIEST
        if name == "none":
            return cls.NONE
        else:
            log.warning("Unrecognized ``auto_offset_reset`` config, using NONE")
            return cls.NONE

    @classmethod
    def to_str(cls, value):
        if value == cls.LATEST:
            return "latest"
        if value == cls.EARLIEST:
            return "earliest"
        if value == cls.NONE:
            return "none"
        else:
            return f"timestamp({value})"


class FetchResult:
    def __init__(self, tp, *, assignment, partition_records, backoff):
        self._topic_partition = tp
        self._partition_records = partition_records

        self._created = time.monotonic()
        self._backoff = backoff

        self._assignment = assignment

    def calculate_backoff(self):
        lifetime = time.monotonic() - self._created
        if lifetime < self._backoff:
            return self._backoff - lifetime
        return 0

    def check_assignment(self, tp):
        assignment = self._assignment

        # There are cases where the returned offset from broker differs from
        # what was requested. This would not be much of an issue if the user
        # could not seek to a different position between yielding results,
        # so we should double check that position did not change between each
        # result yielding.
        return_result = True
        if assignment.active:
            tp = self._topic_partition
            tp_state = assignment.state_value(tp)
            if tp_state.paused:
                return_result = False
            else:
                position = tp_state.position
                if position != self._partition_records.next_fetch_offset:
                    return_result = False
        else:
            return_result = False

        if not return_result:
            # this can happen when a rebalance happened before
            # fetched records are returned
            log.debug(
                "Not returning fetched records for partition %s"
                " since it is no fetchable (unassigned or paused)",
                tp,
            )
            self._partition_records = None
            return False
        return True

    def _update_position(self):
        state = self._assignment.state_value(self._topic_partition)
        state.consumed_to(self._partition_records.next_fetch_offset)

    def getone(self):
        tp = self._topic_partition
        if not self.check_assignment(tp) or not self.has_more():
            return None

        msg = next(self._partition_records, None)
        # We should update position in any case
        self._update_position()
        if msg is None:
            self._partition_records = None
        return msg

    def getall(self, max_records=None):
        tp = self._topic_partition
        if not self.check_assignment(tp) or not self.has_more():
            return []

        ret_list = []
        for msg in self._partition_records:
            ret_list.append(msg)
            if max_records is not None and len(ret_list) >= max_records:
                self._update_position()
                break
        else:
            self._update_position()
            self._partition_records = None

        return ret_list

    def has_more(self):
        return self._partition_records is not None

    def __repr__(self):
        return f"<FetchResult position={self._partition_records.next_fetch_offset!r}>"


class FetchError:
    def __init__(self, *, error, backoff):
        self._error = error
        self._created = time.monotonic()
        self._backoff = backoff

    def calculate_backoff(self):
        lifetime = time.monotonic() - self._created
        if lifetime < self._backoff:
            return self._backoff - lifetime
        return 0

    def check_raise(self):
        # TODO: Do we need to raise error if partition not assigned anymore
        raise self._error

    def __repr__(self):
        return f"<FetchError error={self._error!r}>"


class PartitionRecords:
    def __init__(
        self,
        tp,
        records,
        aborted_transactions,
        fetch_offset,
        key_deserializer,
        value_deserializer,
        check_crcs,
        isolation_level,
    ):
        self._tp = tp
        self._records = records
        self._aborted_transactions = sorted(
            aborted_transactions or [], key=lambda x: x[1]
        )
        self._aborted_producers = set()
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._check_crcs = check_crcs
        self._isolation_level = isolation_level

        # Even without consuming any records we may need to force position to
        # a next offset. In cases like aborted transactions, control batches,
        # empty compacted batches, etc.
        self.next_fetch_offset = fetch_offset

        self._records_iterator = self._unpack_records()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._records_iterator)
        except StopIteration:
            # Break reference cycle just in case
            self._records_iterator = None
            raise

    def _unpack_records(self):
        # NOTE: if the batch is not compressed it's equal to 1 record in
        #       v0 and v1.
        tp = self._tp
        records = self._records
        while records.has_next():
            next_batch = records.next_batch()
            if self._check_crcs and not next_batch.validate_crc():
                # This iterator will be closed after the exception, so we don't
                # try to drain other batches here. They will be refetched.
                raise Errors.CorruptRecordException(f"Invalid CRC - {tp}")

            if (
                self._isolation_level == READ_COMMITTED
                and next_batch.producer_id is not None
            ):
                self._consume_aborted_up_to(next_batch.base_offset)

                if (
                    next_batch.is_control_batch
                    and self._contains_abort_marker(next_batch)
                ):  # fmt: skip
                    # Using `discard` instead of `remove`, because Kafka
                    # may return an abort marker for an otherwise empty
                    # topic-partition.
                    self._aborted_producers.discard(next_batch.producer_id)

                if (
                    next_batch.is_transactional
                    and next_batch.producer_id in self._aborted_producers
                ):
                    log.debug(
                        "Skipping aborted record batch from partition %s with"
                        " producer_id %s and offsets %s to %s",
                        tp,
                        next_batch.producer_id,
                        next_batch.base_offset,
                        next_batch.next_offset - 1,
                    )
                    self.next_fetch_offset = next_batch.next_offset
                    continue

            # We skip control batches no matter the isolation level
            if next_batch.is_control_batch:
                self.next_fetch_offset = next_batch.next_offset
                continue

            for record in next_batch:
                # It's OK for the offset to be larger than the current
                # partition. It will happen in compacted topics.
                if record.offset < self.next_fetch_offset:
                    # Probably just a compressed messageset, it's ok to skip.
                    continue
                consumer_record = self._consumer_record(tp, record)
                self.next_fetch_offset = record.offset + 1
                yield consumer_record

            # Message format v2 preserves the last offset in a batch even if
            # the last record is removed through compaction. By using the next
            # offset computed from the last offset in the batch, we ensure that
            # the offset of the next fetch will point to the next batch, which
            # avoids unnecessary re-fetching of the same batch (in the worst
            # case, the consumer could get stuck fetching the same batch
            # repeatedly).
            self.next_fetch_offset = next_batch.next_offset

    def _consume_aborted_up_to(self, batch_offset):
        # Consume aborted transactions list up to this one to form
        # aborted_producers list
        aborted_transactions = self._aborted_transactions
        while aborted_transactions:
            producer_id, first_offset = aborted_transactions[0]
            if first_offset <= batch_offset:
                self._aborted_producers.add(producer_id)
                aborted_transactions.pop(0)
            else:
                break

    def _contains_abort_marker(self, next_batch):
        # Control Marker is used to specify when we can stop
        # aborting batches
        try:
            control_record = next(next_batch)
        except StopIteration:  # pragma: no cover
            raise Errors.KafkaError(
                "Control batch did not contain any records"
            ) from None
        return ControlRecord.parse(control_record.key) == ABORT_MARKER

    def _consumer_record(self, tp, record):
        key_size = len(record.key) if record.key is not None else -1
        value_size = len(record.value) if record.value is not None else -1

        if self._key_deserializer:
            key = self._key_deserializer(record.key)
        else:
            key = record.key
        if self._value_deserializer:
            value = self._value_deserializer(record.value)
        else:
            value = record.value

        return ConsumerRecord(
            tp.topic,
            tp.partition,
            record.offset,
            record.timestamp,
            record.timestamp_type,
            key,
            value,
            record.checksum,
            key_size,
            value_size,
            tuple(record.headers),
        )


class Fetcher:
    """Initialize a Kafka Message Fetcher.

    Parameters:
        client (AIOKafkaClient): kafka client
        subscription (SubscriptionState): instance of SubscriptionState
            located in aiokafka.consumer.subscription_state
        key_deserializer (callable): Any callable that takes a
            raw message key and returns a deserialized key.
        value_deserializer (callable, optional): Any callable that takes a
            raw message value and returns a deserialized value.
        fetch_min_bytes (int): Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            fetch_max_wait_ms for more data to accumulate. Default: 1.
        fetch_max_bytes (int): The maximum amount of data the server should
            return for a fetch request. This is not an absolute maximum, if
            the first message in the first non-empty partition of the fetch
            is larger than this value, the message will still be returned
            to ensure that the consumer can make progress. NOTE: consumer
            performs fetches to multiple brokers in parallel so memory
            usage will depend on the number of brokers containing
            partitions for the topic.
            Supported Kafka version >= 0.10.1.0. Default: 52428800 (50 Mb).
        fetch_max_wait_ms (int): The maximum amount of time in milliseconds
            the server will block before answering the fetch request if
            there isn't sufficient data to immediately satisfy the
            requirement given by fetch_min_bytes. Default: 500.
        max_partition_fetch_bytes (int): The maximum amount of data
            per-partition the server will return. The maximum total memory
            used for a request = #partitions * max_partition_fetch_bytes.
            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition. Default: 1048576.
        check_crcs (bool): Automatically check the CRC32 of the records
            consumed. This ensures no on-the-wire or on-disk corruption to
            the messages occurred. This check adds some overhead, so it may
            be disabled in cases seeking extreme performance. Default: True
        fetcher_timeout (float): Maximum polling interval in the background
            fetching routine. Default: 0.2
        prefetch_backoff (float): number of seconds to wait until
            consumption of partition is paused. Paused partitions will not
            request new data from Kafka server (will not be included in
            next poll request).
        auto_offset_reset (str): A policy for resetting offsets on
            OffsetOutOfRange errors: 'earliest' will move to the oldest
            available message, 'latest' will move to the most recent. Any
            ofther value will raise the exception. Default: 'latest'.
        isolation_level (str): Controls how to read messages written
            transactionally. See consumer description.
    """

    def __init__(
        self,
        client,
        subscriptions,
        *,
        key_deserializer=None,
        value_deserializer=None,
        fetch_min_bytes=1,
        fetch_max_bytes=52428800,
        fetch_max_wait_ms=500,
        max_partition_fetch_bytes=1048576,
        check_crcs=True,
        fetcher_timeout=0.2,
        prefetch_backoff=0.1,
        retry_backoff_ms=100,
        auto_offset_reset="latest",
        isolation_level="read_uncommitted",
    ):
        self._client = client
        self._loop = client._loop
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_bytes = fetch_max_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._check_crcs = check_crcs
        self._fetcher_timeout = fetcher_timeout
        self._prefetch_backoff = prefetch_backoff
        self._retry_backoff = retry_backoff_ms / 1000
        self._subscriptions = subscriptions
        self._default_reset_strategy = OffsetResetStrategy.from_str(auto_offset_reset)

        if isolation_level == "read_uncommitted":
            self._isolation_level = READ_UNCOMMITTED
        elif isolation_level == "read_committed":
            self._isolation_level = READ_COMMITTED
        else:
            raise ValueError(f"Incorrect isolation level {isolation_level}")

        self._records = collections.OrderedDict()
        self._in_flight = set()
        self._pending_tasks = set()

        self._wait_consume_future = None
        self._fetch_waiters = set()

        # SubscriptionState will pass Coordination critical errors to those
        # waiters directly
        self._subscriptions.register_fetch_waiters(self._fetch_waiters)

        self._fetch_task = create_task(self._fetch_requests_routine())

        self._closed = False

    async def close(self):
        self._closed = True

        self._fetch_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._fetch_task

        # Fail all pending fetchone/fetchall calls
        for waiter in self._fetch_waiters:
            self._notify(waiter)

        for x in self._pending_tasks:
            x.cancel()
            await x

    def _notify(self, future):
        if future is not None and not future.done():
            future.set_result(None)

    def _create_fetch_waiter(self):
        # Creating a fetch waiter is usually not that frequent of an operation,
        # (get methods will return all data first, before a waiter is created)

        fut = self._loop.create_future()
        self._fetch_waiters.add(fut)
        fut.add_done_callback(lambda f, waiters=self._fetch_waiters: waiters.remove(f))
        return fut

    @property
    def error_future(self):
        return self._fetch_task

    async def _fetch_requests_routine(self):
        """Implements a background task to populate internal fetch queue
        ``self._records`` with prefetched messages. This helps isolate the
        ``getall/getone`` calls from actual calls to broker. This way we don't
        need to think of what happens if user calls get in 2 tasks, etc.

            The loop is quite complicated due to a large set of events that
        can allow new fetches to be send. Those include previous fetches,
        offset resets, metadata updates to discover new leaders for partitions,
        data consumed for partition.

            Previously the offset reset was performed separately, but it did
        not perform too reliably. In ``kafka-python`` and Java client the reset
        is perform in ``poll()`` before each fetch, which works good for sync
        systems. But with ``aiokafka`` the user can actually break such
        behaviour quite easily by performing actions from different tasks.
        """
        try:
            assignment = None

            def start_pending_task(coro, node_id, self=self):
                task = create_task(coro)
                self._pending_tasks.add(task)
                self._in_flight.add(node_id)

                def on_done(fut, self=self):
                    self._in_flight.discard(node_id)

                task.add_done_callback(on_done)

            while True:
                # If we lose assignment we just cancel all current tasks,
                # wait for new assignment and restart the loop
                if assignment is None or not assignment.active:
                    for task in self._pending_tasks:
                        # Those tasks should have proper handling for
                        # cancellation
                        if not task.done():
                            task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                    self._pending_tasks.clear()
                    self._records.clear()

                    subscription = self._subscriptions.subscription
                    if subscription is None or subscription.assignment is None:
                        try:
                            waiter = self._subscriptions.wait_for_assignment()
                            await waiter
                        except Errors.KafkaError:
                            # Critical coordination waiters will be passed
                            # to user, but fetcher can just ignore those
                            continue
                    assignment = self._subscriptions.subscription.assignment
                assert assignment is not None and assignment.active

                # Reset consuming signal future.
                self._wait_consume_future = create_future()
                # Determine what action to take per node
                (
                    fetch_requests,
                    reset_requests,
                    timeout,
                    invalid_metadata,
                    resume_futures,
                ) = self._get_actions_per_node(assignment)

                # Start fetch tasks
                for node_id, request in fetch_requests:
                    start_pending_task(
                        self._proc_fetch_request(assignment, node_id, request),
                        node_id=node_id,
                    )
                # Start update position tasks
                for node_id, tps in reset_requests.items():
                    start_pending_task(
                        self._update_fetch_positions(assignment, node_id, tps),
                        node_id=node_id,
                    )
                # Apart from pending requests we also need to react to other
                # events to send new fetches as soon as possible
                other_futs = [self._wait_consume_future, assignment.unassign_future]
                if invalid_metadata:
                    fut = self._client.force_metadata_update()
                    other_futs.append(fut)

                done_set, _ = await asyncio.wait(
                    set(chain(self._pending_tasks, other_futs, resume_futures)),
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Process fetch tasks results if any
                done_pending = self._pending_tasks.intersection(done_set)
                if done_pending:
                    has_new_data = any(fut.result() for fut in done_pending)
                    if has_new_data:
                        for waiter in self._fetch_waiters:
                            # we added some messages to self._records,
                            # wake up waiters
                            self._notify(waiter)
                    self._pending_tasks -= done_pending
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # pragma: no cover
            log.exception("Unexpected error in fetcher routine")
            raise Errors.KafkaError("Unexpected error during data retrieval") from exc

    def _get_actions_per_node(self, assignment):
        """For each assigned partition determine the action needed to be
        performed and group those by leader node id.
        """
        # create the fetch info as a dict of lists of partition info tuples
        # which can be passed to FetchRequest() via .items()
        fetchable = collections.defaultdict(list)
        awaiting_reset = collections.defaultdict(list)
        backoff_by_nodes = collections.defaultdict(list)
        resume_futures = []
        invalid_metadata = False

        for tp in assignment.tps:
            tp_state = assignment.state_value(tp)

            node_id = self._client.cluster.leader_for_partition(tp)
            backoff = 0
            if tp in self._records:
                # We have data still not consumed by user. In this case we
                # usually wait for the user to finish consumption, but to avoid
                # blocking other partitions we have a timeout here.
                record = self._records[tp]
                backoff = record.calculate_backoff()
                if backoff:
                    backoff_by_nodes[node_id].append(backoff)
            elif node_id in self._in_flight:
                # We have in-flight fetches to this node
                continue
            elif node_id is None or node_id == -1:
                log.debug(
                    "No leader found for partition %s. Waiting metadata update", tp
                )
                invalid_metadata = True
            elif not tp_state.has_valid_position:
                awaiting_reset[node_id].append(tp)
            elif tp_state.paused:
                resume_futures.append(tp_state.resume_fut)
            else:
                position = tp_state.position
                fetchable[node_id].append((tp, position))
                log.debug(
                    "Adding fetch request for partition %s at offset %d", tp, position
                )

        fetch_requests = []
        for node_id, partition_data in fetchable.items():
            if node_id in backoff_by_nodes:
                # At least one partition is still waiting to be consumed
                continue
            if node_id in awaiting_reset:
                # First we need to reset offset for some partitions, then we
                # will fetch next page of results
                continue

            # Shuffle partition data to help get more equal consumption
            random.shuffle(partition_data)

            # Create fetch request
            by_topics = collections.defaultdict(list)
            for tp, position in partition_data:
                by_topics[tp.topic].append(
                    (tp.partition, position, self._max_partition_fetch_bytes)
                )
            req = FetchRequest(
                self._fetch_max_wait_ms,
                self._fetch_min_bytes,
                self._fetch_max_bytes,
                self._isolation_level,
                list(by_topics.items()),
            )
            fetch_requests.append((node_id, req))

        if backoff_by_nodes:
            # Return min time till any node will be ready to send event
            # (max of it's backoffs)
            backoff = min(map(max, backoff_by_nodes.values()))
        else:
            backoff = self._fetcher_timeout
        return (
            fetch_requests,
            awaiting_reset,
            backoff,
            invalid_metadata,
            resume_futures,
        )

    async def _proc_fetch_request(self, assignment, node_id, request):
        needs_wakeup = False
        try:
            response = await self._client.send(node_id, request)
        except Errors.KafkaError as err:
            log.error("Failed fetch messages from %s: %s", node_id, err)
            await asyncio.sleep(self._retry_backoff)
            return False
        except asyncio.CancelledError:
            # Either `close()` or partition unassigned. Either way the result
            # is no longer of interest.
            return False

        if not assignment.active:
            log.debug(
                "Discarding fetch response since the assignment changed during fetch"
            )
            return False

        fetch_offsets = {}
        for topic, partitions in request.topics:
            for partition, offset, _ in partitions:
                fetch_offsets[TopicPartition(topic, partition)] = offset

        now_ms = int(1000 * time.time())
        for topic, partitions in response.topics:
            for partition, error_code, highwater, *part_data in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                fetch_offset = fetch_offsets[tp]
                tp_state = assignment.state_value(tp)
                if not tp_state.has_valid_position or tp_state.position != fetch_offset:
                    log.debug(
                        "Discarding fetch response for partition %s "
                        "since its offset %s does not match the current "
                        "position",
                        tp,
                        fetch_offset,
                    )
                    continue

                if error_type is Errors.NoError:
                    if response.API_VERSION >= 4:
                        aborted_transactions = part_data[-2]
                        lso = part_data[-3]
                    else:
                        aborted_transactions = None
                        lso = None
                    tp_state.highwater = highwater
                    tp_state.lso = lso
                    tp_state.timestamp = now_ms

                    # part_data also contains lso, aborted_transactions.
                    # message_set is last
                    records = MemoryRecords(part_data[-1])
                    if records.has_next():
                        log.debug(
                            "Adding fetched record for partition %s with"
                            " offset %d to buffered record list",
                            tp,
                            fetch_offset,
                        )

                        partition_records = PartitionRecords(
                            tp,
                            records,
                            aborted_transactions,
                            fetch_offset,
                            self._key_deserializer,
                            self._value_deserializer,
                            self._check_crcs,
                            self._isolation_level,
                        )

                        self._records[tp] = FetchResult(
                            tp,
                            partition_records=partition_records,
                            assignment=assignment,
                            backoff=self._prefetch_backoff,
                        )

                        # We added at least 1 successful record
                        needs_wakeup = True
                    elif records.size_in_bytes() > 0:
                        # we did not read a single message from a non-empty
                        # buffer because that message's size is larger than
                        # fetch size, in this case record this exception
                        err = RecordTooLargeError(
                            "There are some messages at [Partition=Offset]: "
                            "%s=%s whose size is larger than the fetch size %s"
                            " and hence cannot be ever returned. "
                            "Increase the fetch size, or decrease the maximum "
                            "message size the broker will allow.",
                            tp,
                            fetch_offset,
                            self._max_partition_fetch_bytes,
                        )
                        self._set_error(tp, err)
                        tp_state.consumed_to(tp_state.position + 1)
                        needs_wakeup = True

                elif error_type in (
                    Errors.NotLeaderForPartitionError,
                    Errors.UnknownTopicOrPartitionError,
                ):
                    self._client.force_metadata_update()
                elif error_type is Errors.OffsetOutOfRangeError:
                    if self._default_reset_strategy != OffsetResetStrategy.NONE:
                        tp_state.await_reset(self._default_reset_strategy)
                    else:
                        err = Errors.OffsetOutOfRangeError({tp: fetch_offset})
                        self._set_error(tp, err)
                        needs_wakeup = True
                    log.info(
                        "Fetch offset %s is out of range for partition %s,"
                        " resetting offset",
                        fetch_offset,
                        tp,
                    )
                elif error_type is Errors.TopicAuthorizationFailedError:
                    log.warning("Not authorized to read from topic %s.", tp.topic)
                    err = Errors.TopicAuthorizationFailedError(tp.topic)
                    self._set_error(tp, err)
                    needs_wakeup = True
                else:
                    log.warning(
                        "Unexpected error while fetching data: %s", error_type.__name__
                    )
        return needs_wakeup

    def _set_error(self, tp, error):
        assert tp not in self._records, self._records[tp]
        self._records[tp] = FetchError(error=error, backoff=self._prefetch_backoff)

    async def _update_fetch_positions(self, assignment, node_id, tps):
        """This task will be called if there is no valid position for
        partition. It may be right after assignment, on seek_to_* calls of
        Consumer or if current position went out of range.
        """
        log.debug("Updating fetch positions for partitions %s", tps)
        needs_wakeup = False
        # The list of partitions provided can consist of paritions that are
        # awaiting reset already and freshly assigned partitions. The second
        # ones can yet have no commit point fetched, so we will check that.
        for tp in tps:
            tp_state = assignment.state_value(tp)
            if tp_state.has_valid_position or tp_state.awaiting_reset:
                continue

            try:
                committed = await tp_state.fetch_committed()
            except asyncio.CancelledError:
                return needs_wakeup
            assert committed is not None

            # There could have been a seek() call of some sort while
            # waiting for committed point
            if tp_state.has_valid_position or tp_state.awaiting_reset:
                continue

            if committed.offset == UNKNOWN_OFFSET:
                # No offset stored in Kafka, need to reset
                if self._default_reset_strategy != OffsetResetStrategy.NONE:
                    tp_state.await_reset(self._default_reset_strategy)
                else:
                    err = Errors.NoOffsetForPartitionError(tp)
                    self._set_error(tp, err)
                    needs_wakeup = True
                log.debug("No committed offset found for %s", tp)
            else:
                log.debug(
                    "Resetting offset for partition %s to the committed offset %s",
                    tp,
                    committed,
                )
                tp_state.reset_to(committed.offset)

        topic_data = collections.defaultdict(list)
        needs_reset = []
        for tp in tps:
            tp_state = assignment.state_value(tp)
            if not tp_state.awaiting_reset:
                continue
            needs_reset.append(tp)

            strategy = tp_state.reset_strategy
            assert strategy is not None
            log.debug(
                "Resetting offset for partition %s using %s strategy.",
                tp,
                OffsetResetStrategy.to_str(strategy),
            )
            topic_data[tp.topic].append((tp.partition, strategy))

        if not topic_data:
            return needs_wakeup

        try:
            try:
                offsets = await self._proc_offset_request(node_id, topic_data)
            except Errors.KafkaError as err:
                log.error("Failed fetch offsets from %s: %s", node_id, err)
                await asyncio.sleep(self._retry_backoff)
                return needs_wakeup
        except asyncio.CancelledError:
            return needs_wakeup

        for tp in needs_reset:
            offset = offsets[tp][0]
            tp_state = assignment.state_value(tp)
            # There could have been some `seek` call while fetching offset
            if tp_state.awaiting_reset:
                tp_state.reset_to(offset)
        return needs_wakeup

    async def _retrieve_offsets(self, timestamps, timeout_ms=None):
        """Fetch offset for each partition passed in ``timestamps`` map.

        Blocks until offsets are obtained, a non-retriable exception is raised
        or ``timeout_ms`` passed.

        Arguments:
            timestamps: {TopicPartition: int} dict with timestamps to fetch
                offsets by. -1 for the latest available, -2 for the earliest
                available. Otherwise timestamp is treated as epoch
                milliseconds.

        Returns:
            {TopicPartition: (int, int)}: Mapping of partition to
                retrieved offset and timestamp. If offset does not exist for
                the provided timestamp, that partition will be missing from
                this mapping.
        """
        if not timestamps:
            return {}

        timeout = None if timeout_ms is None else timeout_ms / 1000
        try:
            async with async_timeout.timeout(timeout):
                while True:
                    try:
                        offsets = await self._proc_offset_requests(timestamps)
                    except Errors.KafkaError as error:  # noqa: PERF203
                        if not error.retriable:
                            raise
                        if error.invalid_metadata:
                            self._client.force_metadata_update()
                        await asyncio.sleep(self._retry_backoff)
                    else:
                        return offsets
        except asyncio.TimeoutError as exc:
            raise KafkaTimeoutError(
                f"Failed to get offsets by times in {timeout_ms} ms"
            ) from exc

    async def _proc_offset_requests(self, timestamps):
        """Fetch offsets for each partition in timestamps dict. This may send
        request to multiple nodes, based on who is Leader for partition.

        Arguments:
            timestamps (dict): {TopicPartition: int} mapping of fetching
                timestamps.

        Returns:
            Future: resolves to a mapping of retrieved offsets
        """
        # The could be several places where we triggered an update, so only
        # wait for it once here
        await self._client._maybe_wait_metadata()

        # Group it in hierarhy `node` -> `topic` -> `[(partition, offset)]`
        timestamps_by_node = collections.defaultdict(
            lambda: collections.defaultdict(list)
        )

        for partition, timestamp in timestamps.items():
            node_id = self._client.cluster.leader_for_partition(partition)
            if node_id is None:
                # triggers a metadata update
                self._client.add_topic(partition.topic)
                log.debug(
                    "Partition %s is unknown for fetching offset,"
                    " wait for metadata refresh",
                    partition,
                )
                raise Errors.StaleMetadata(partition)
            elif node_id == -1:
                log.debug(
                    "Leader for partition %s unavailable for fetching "
                    "offset, wait for metadata refresh",
                    partition,
                )
                raise Errors.LeaderNotAvailableError(partition)
            else:
                timestamps_by_node[node_id][partition.topic].append(
                    (partition.partition, timestamp)
                )

        futs = []
        for node_id, topic_data in timestamps_by_node.items():
            futs.append(self._proc_offset_request(node_id, topic_data))
        offsets = {}
        res = await asyncio.gather(*futs)
        for partial_offsets in res:
            offsets.update(partial_offsets)
        return offsets

    async def _proc_offset_request(self, node_id, topic_data):
        request = OffsetRequest(-1, self._isolation_level, list(topic_data.items()))

        response = await self._client.send(node_id, request)

        res_offsets = {}
        for topic, part_data in response.topics:
            for part, error_code, *partition_info in part_data:
                partition = TopicPartition(topic, part)
                error_type = Errors.for_code(error_code)
                if error_type is Errors.NoError:
                    if response.API_VERSION == 0:
                        offsets = partition_info[0]
                        assert len(offsets) <= 1, (
                            "Expected OffsetResponse with one offset"
                        )
                        if offsets:
                            offset = offsets[0]
                            log.debug(
                                "Handling v0 ListOffsetResponse response for "
                                "%s. Fetched offset %s",
                                partition,
                                offset,
                            )
                            res_offsets[partition] = (offset, None)
                        else:
                            res_offsets[partition] = (UNKNOWN_OFFSET, None)
                    else:
                        timestamp, offset = partition_info
                        log.debug(
                            "Handling ListOffsetResponse response for "
                            "%s. Fetched offset %s, timestamp %s",
                            partition,
                            offset,
                            timestamp,
                        )
                        res_offsets[partition] = (offset, timestamp)
                elif error_type is Errors.UnsupportedForMessageFormatError:
                    # The message format on the broker side is before 0.10.0,
                    # we will simply put None in the response.
                    log.debug(
                        "Cannot search by timestamp for partition %s "
                        "because the message format version is before "
                        "0.10.0",
                        partition,
                    )
                elif error_type is Errors.NotLeaderForPartitionError:
                    log.debug(
                        "Attempt to fetch offsets for partition %s "
                        "failed "
                        "due to obsolete leadership information, retrying.",
                        partition,
                    )
                    raise error_type(partition)
                elif error_type is Errors.UnknownTopicOrPartitionError:
                    log.warning(
                        "Received unknown topic or partition error in "
                        "ListOffset request for partition %s. The "
                        "topic/partition may not exist or the user may not "
                        "have Describe access to it.",
                        partition,
                    )
                    raise error_type(partition)
                else:
                    log.warning(
                        "Attempt to fetch offsets for partition %s failed due to: %s",
                        partition,
                        error_type,
                    )
                    raise error_type(partition)
        return res_offsets

    async def next_record(self, partitions):
        """Return one fetched records

        This method will contain a little overhead as we will do more work this
        way:
            * Notify prefetch routine per every consumed partition
            * Assure message marked for autocommit

        """
        while True:
            if self._closed:
                raise ConsumerStoppedError()

            # While the background routine will fetch new records up till new
            # assignment is finished, we don't want to return records, that may
            # not belong to this instance after rebalance.
            if self._subscriptions.reassignment_in_progress:
                await self._subscriptions.wait_for_assignment()

            for tp in list(self._records.keys()):
                if partitions and tp not in partitions:
                    # Cleanup results for unassigned partitions
                    if not self._subscriptions.is_assigned(tp):
                        del self._records[tp]
                    continue
                res_or_error = self._records[tp]
                if type(res_or_error) is FetchResult:
                    message = res_or_error.getone()
                    if message is None:
                        # We already processed all messages, request new ones
                        del self._records[tp]
                        self._notify(self._wait_consume_future)
                    else:
                        return message
                else:
                    # Remove error, so we can fetch on partition again
                    del self._records[tp]
                    self._notify(self._wait_consume_future)
                    res_or_error.check_raise()

            # No messages ready. Wait for some to arrive
            waiter = self._create_fetch_waiter()
            await waiter

    async def fetched_records(self, partitions, timeout=0, max_records=None):
        """Returns previously fetched records and updates consumed offsets."""
        while True:
            # While the background routine will fetch new records up till new
            # assignment is finished, we don't want to return records, that may
            # not belong to this instance after rebalance.
            if self._subscriptions.reassignment_in_progress:
                await self._subscriptions.wait_for_assignment()

            start_time = time.monotonic()
            drained = {}
            for tp in list(self._records.keys()):
                if partitions and tp not in partitions:
                    # Cleanup results for unassigned partitions
                    if not self._subscriptions.is_assigned(tp):
                        del self._records[tp]
                    continue
                res_or_error = self._records[tp]
                if type(res_or_error) is FetchResult:
                    records = res_or_error.getall(max_records)
                    if not res_or_error.has_more():
                        # We processed all messages - request new ones
                        del self._records[tp]
                        self._notify(self._wait_consume_future)
                    if not records:
                        continue
                    drained[tp] = records
                    if max_records is not None:
                        max_records -= len(drained[tp])
                        assert max_records >= 0  # Just in case
                        if max_records == 0:
                            break
                elif drained:
                    # We already got some messages from another partition -
                    # return them. We will raise this error on next call
                    return drained
                else:
                    # Remove error, so we can fetch on partition again
                    del self._records[tp]
                    self._notify(self._wait_consume_future)
                    res_or_error.check_raise()

            if drained or not timeout:
                return drained

            waiter = self._create_fetch_waiter()
            done, pending = await asyncio.wait([waiter], timeout=timeout)

            if not done or self._closed:
                if pending:
                    fut = pending.pop()
                    fut.cancel()
                return {}

            if waiter.done():
                waiter.result()  # Check for authorization errors

            # Decrease timeout accordingly
            timeout = timeout - (time.monotonic() - start_time)
            timeout = max(0, timeout)

    async def get_offsets_by_times(self, timestamps, timeout_ms):
        offsets = await self._retrieve_offsets(timestamps, timeout_ms)
        for tp in timestamps:
            if tp not in offsets:
                offsets[tp] = None
            else:
                offset, timestamp = offsets[tp]
                if offset == UNKNOWN_OFFSET:
                    offsets[tp] = None
                else:
                    offsets[tp] = OffsetAndTimestamp(offset, timestamp)
        return offsets

    async def beginning_offsets(self, partitions, timeout_ms):
        timestamps = dict.fromkeys(partitions, OffsetResetStrategy.EARLIEST)
        offsets = await self._retrieve_offsets(timestamps, timeout_ms)
        return {tp: offset for (tp, (offset, ts)) in offsets.items()}

    async def end_offsets(self, partitions, timeout_ms):
        timestamps = dict.fromkeys(partitions, OffsetResetStrategy.LATEST)
        offsets = await self._retrieve_offsets(timestamps, timeout_ms)
        return {tp: offset for (tp, (offset, ts)) in offsets.items()}

    def request_offset_reset(self, tps, strategy):
        """Force a position reset. Called from Consumer of `seek_to_*` API's."""
        assignment = self._subscriptions.subscription.assignment
        assert assignment is not None

        waiters = []
        for tp in tps:
            tp_state = assignment.state_value(tp)
            tp_state.await_reset(strategy)
            waiters.append(tp_state.wait_for_position())
            # Invalidate previous fetch result
            if tp in self._records:
                del self._records[tp]

        # XXX: Maybe we should use a different future or rename? Not really
        # describing the purpose.
        self._notify(self._wait_consume_future)

        return asyncio.gather(*waiters)

    def seek_to(self, tp, offset):
        """Force a position change to specific offset. Called from
        `Consumer.seek()` API.
        """
        self._subscriptions.seek(tp, offset)
        if tp in self._records:
            del self._records[tp]

        # XXX: Maybe we should use a different future or rename? Not really
        # describing the purpose.
        self._notify(self._wait_consume_future)

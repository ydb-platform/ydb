import asyncio
import logging
import re
import sys
import traceback
import warnings

from aiokafka import __version__
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.client import AIOKafkaClient
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.errors import (
    ConsumerStoppedError,
    IllegalOperation,
    IllegalStateError,
    RecordTooLargeError,
)
from aiokafka.structs import ConsumerRecord, TopicPartition
from aiokafka.util import commit_structure_validate, get_running_loop

from .fetcher import Fetcher, OffsetResetStrategy
from .group_coordinator import GroupCoordinator, NoGroupCoordinator
from .subscription_state import SubscriptionState

log = logging.getLogger(__name__)


class AIOKafkaConsumer:
    """
    A client that consumes records from a Kafka cluster.

    The consumer will transparently handle the failure of servers in the Kafka
    cluster, and adapt as topic-partitions are created or migrate between
    brokers.

    It also interacts with the assigned Kafka Group Coordinator node to allow
    multiple consumers to load balance consumption of topics (feature of Kafka
    >= 0.9.0.0).

    .. _kip-62:
        https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread

    Arguments:
        *topics (list(str)): optional list of topics to subscribe to. If not set,
            call :meth:`.subscribe` or :meth:`.assign` before consuming records.
            Passing topics directly is same as calling :meth:`.subscribe` API.
        bootstrap_servers (str, list(str)): a ``host[:port]`` string (or list of
            ``host[:port]`` strings) that the consumer should contact to bootstrap
            initial cluster metadata.

            This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to ``localhost:9092``.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to :class:`~.consumer.group_coordinator.GroupCoordinator`
            for logging with respect to consumer group administration. Default:
            ``aiokafka-{version}``
        group_id (str or None): name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            Default: None
        group_instance_id (str or None): name of the group instance ID used for
            static membership (KIP-345)
        key_deserializer (Callable): Any callable that takes a
            raw message key and returns a deserialized key.
        value_deserializer (Callable, Optional): Any callable that takes a
            raw message value and returns a deserialized value.
        fetch_min_bytes (int): Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            `fetch_max_wait_ms` for more data to accumulate. Default: 1.
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
            used for a request ``= #partitions * max_partition_fetch_bytes``.
            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition. Default: 1048576.
        max_poll_records (int): The maximum number of records returned in a
            single call to :meth:`.getmany`. Defaults ``None``, no limit.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 40000.
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        auto_offset_reset (str): A policy for resetting offsets on
            :exc:`.OffsetOutOfRangeError` errors: ``earliest`` will move to the oldest
            available message, ``latest`` will move to the most recent, and
            ``none`` will raise an exception so you can handle this case.
            Default: ``latest``.
        enable_auto_commit (bool): If true the consumer's offset will be
            periodically committed in the background. Default: True.
        auto_commit_interval_ms (int): milliseconds between automatic
            offset commits, if enable_auto_commit is True. Default: 5000.
        check_crcs (bool): Automatically check the CRC32 of the records
            consumed. This ensures no on-the-wire or on-disk corruption to
            the messages occurred. This check adds some overhead, so it may
            be disabled in cases seeking extreme performance. Default: True
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        partition_assignment_strategy (list): List of objects to use to
            distribute partition ownership amongst consumer instances when
            group management is used. This preference is implicit in the order
            of the strategies in the list. When assignment strategy changes:
            to support a change to the assignment strategy, new versions must
            enable support both for the old assignment strategy and the new
            one. The coordinator will choose the old assignment strategy until
            all members have been updated. Then it will choose the new
            strategy. Default: [:class:`.RoundRobinPartitionAssignor`]

        max_poll_interval_ms (int): Maximum allowed time between calls to
            consume messages (e.g., :meth:`.getmany`). If this interval
            is exceeded the consumer is considered failed and the group will
            rebalance in order to reassign the partitions to another consumer
            group member. If API methods block waiting for messages, that time
            does not count against this timeout. See `KIP-62`_ for more
            information. Default 300000
        rebalance_timeout_ms (int): The maximum time server will wait for this
            consumer to rejoin the group in a case of rebalance. In Java client
            this behaviour is bound to `max.poll.interval.ms` configuration,
            but as ``aiokafka`` will rejoin the group in the background, we
            decouple this setting to allow finer tuning by users that use
            :class:`.ConsumerRebalanceListener` to delay rebalacing. Defaults
            to ``session_timeout_ms``
        session_timeout_ms (int): Client group session and failure detection
            timeout. The consumer sends periodic heartbeats
            (`heartbeat.interval.ms`) to indicate its liveness to the broker.
            If no hearts are received by the broker for a group member within
            the session timeout, the broker will remove the consumer from the
            group and trigger a rebalance. The allowed range is configured with
            the **broker** configuration properties
            `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            Default: 10000
        heartbeat_interval_ms (int): The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group. The
            value must be set lower than `session_timeout_ms`, but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances. Default: 3000

        consumer_timeout_ms (int): maximum wait timeout for background fetching
            routine. Mostly defines how fast the system will see rebalance and
            request new data for new partitions. Default: 200
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: ``PLAINTEXT``, ``SSL``, ``SASL_PLAINTEXT``,
            ``SASL_SSL``. Default: ``PLAINTEXT``.
        ssl_context (ssl.SSLContext): pre-configured :class:`~ssl.SSLContext`
            for wrapping socket connections. Directly passed into asyncio's
            :meth:`~asyncio.loop.create_connection`. For more information see
            :ref:`ssl_auth`. Default: None.
        exclude_internal_topics (bool): Whether records from internal topics
            (such as offsets) should be exposed to the consumer. If set to True
            the only way to receive records from an internal topic is
            subscribing to it. Requires 0.10+ Default: True
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks. Default: 540000 (9 minutes).
        isolation_level (str): Controls how to read messages written
            transactionally.

            If set to ``read_committed``, :meth:`.getmany` will only return
            transactional messages which have been committed.
            If set to ``read_uncommitted`` (the default), :meth:`.getmany` will
            return all messages, even transactional messages which have been
            aborted.

            Non-transactional messages will be returned unconditionally in
            either mode.

            Messages will always be returned in offset order. Hence, in
            `read_committed` mode, :meth:`.getmany` will only return
            messages up to the last stable offset (LSO), which is the one less
            than the offset of the first open transaction. In particular any
            messages appearing after messages belonging to ongoing transactions
            will be withheld until the relevant transaction has been completed.
            As a result, `read_committed` consumers will not be able to read up
            to the high watermark when there are in flight transactions.
            Further, when in `read_committed` the seek_to_end method will
            return the LSO. See method docs below. Default: ``read_uncommitted``

        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for ``SASL_PLAINTEXT`` or ``SASL_SSL``. Valid values are:
            ``PLAIN``, ``GSSAPI``, ``SCRAM-SHA-256``, ``SCRAM-SHA-512``,
            ``OAUTHBEARER``.
            Default: ``PLAIN``
        sasl_plain_username (str): username for SASL ``PLAIN`` authentication.
            Default: None
        sasl_plain_password (str): password for SASL ``PLAIN`` authentication.
            Default: None
        sasl_oauth_token_provider (~aiokafka.abc.AbstractTokenProvider):
            OAuthBearer token provider instance.
            Default: None

    Note:
        Many configuration parameters are taken from Java Client:
        https://kafka.apache.org/documentation.html#newconsumerconfigs

    """

    _closed = None  # Serves as an uninitialized flag for __del__
    _source_traceback = None

    def __init__(
        self,
        *topics,
        loop=None,
        bootstrap_servers="localhost",
        client_id="aiokafka-" + __version__,
        group_id=None,
        group_instance_id=None,
        key_deserializer=None,
        value_deserializer=None,
        fetch_max_wait_ms=500,
        fetch_max_bytes=52428800,
        fetch_min_bytes=1,
        max_partition_fetch_bytes=1 * 1024 * 1024,
        request_timeout_ms=40 * 1000,
        retry_backoff_ms=100,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        check_crcs=True,
        metadata_max_age_ms=5 * 60 * 1000,
        partition_assignment_strategy=(RoundRobinPartitionAssignor,),
        max_poll_interval_ms=300000,
        rebalance_timeout_ms=None,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
        consumer_timeout_ms=200,
        max_poll_records=None,
        ssl_context=None,
        security_protocol="PLAINTEXT",
        exclude_internal_topics=True,
        connections_max_idle_ms=540000,
        isolation_level="read_uncommitted",
        sasl_mechanism="PLAIN",
        sasl_plain_password=None,
        sasl_plain_username=None,
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name=None,
        sasl_oauth_token_provider=None,
    ):
        if loop is None:
            loop = get_running_loop()
        else:
            warnings.warn(
                "The loop argument is deprecated since 0.7.1, "
                "and scheduled for removal in 0.9.0",
                DeprecationWarning,
                stacklevel=2,
            )

        if max_poll_records is not None and (
            not isinstance(max_poll_records, int) or max_poll_records < 1
        ):
            raise ValueError("`max_poll_records` should be positive Integer")

        if rebalance_timeout_ms is None:
            rebalance_timeout_ms = session_timeout_ms

        self._client = AIOKafkaClient(
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            ssl_context=ssl_context,
            security_protocol=security_protocol,
            connections_max_idle_ms=connections_max_idle_ms,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            sasl_oauth_token_provider=sasl_oauth_token_provider,
        )

        self._group_id = group_id
        self._group_instance_id = group_instance_id
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._session_timeout_ms = session_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._auto_offset_reset = auto_offset_reset
        self._request_timeout_ms = request_timeout_ms
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._partition_assignment_strategy = partition_assignment_strategy
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_bytes = fetch_max_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._exclude_internal_topics = exclude_internal_topics
        self._max_poll_records = max_poll_records
        self._consumer_timeout = consumer_timeout_ms / 1000
        self._isolation_level = isolation_level
        self._rebalance_timeout_ms = rebalance_timeout_ms
        self._max_poll_interval_ms = max_poll_interval_ms

        self._check_crcs = check_crcs
        self._subscription = SubscriptionState(loop=loop)
        self._fetcher = None
        self._coordinator = None
        self._loop = loop

        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self._closed = False

        if topics:
            topics = self._validate_topics(topics)
            self._client.set_topics(topics)
            self._subscription.subscribe(topics=topics)

    def __del__(self, _warnings=warnings):
        if self._closed is False:
            _warnings.warn(
                f"Unclosed AIOKafkaConsumer {self!r}",
                ResourceWarning,
                source=self,
            )
            context = {
                "consumer": self,
                "message": "Unclosed AIOKafkaConsumer",
            }
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def start(self):
        """Connect to Kafka cluster. This will:

        * Load metadata for all cluster nodes and partition allocation
        * Wait for possible topic autocreation
        * Join group if ``group_id`` provided
        """
        assert self._loop is get_running_loop(), (
            "Please create objects with the same loop as running with"
        )
        assert self._fetcher is None, "Did you call `start` twice?"
        await self._client.bootstrap()
        await self._wait_topics()

        self._fetcher = Fetcher(
            self._client,
            self._subscription,
            key_deserializer=self._key_deserializer,
            value_deserializer=self._value_deserializer,
            fetch_min_bytes=self._fetch_min_bytes,
            fetch_max_bytes=self._fetch_max_bytes,
            fetch_max_wait_ms=self._fetch_max_wait_ms,
            max_partition_fetch_bytes=self._max_partition_fetch_bytes,
            check_crcs=self._check_crcs,
            fetcher_timeout=self._consumer_timeout,
            retry_backoff_ms=self._retry_backoff_ms,
            auto_offset_reset=self._auto_offset_reset,
            isolation_level=self._isolation_level,
        )

        if self._group_id is not None:
            # using group coordinator for automatic partitions assignment
            self._coordinator = GroupCoordinator(
                self._client,
                self._subscription,
                group_id=self._group_id,
                group_instance_id=self._group_instance_id,
                heartbeat_interval_ms=self._heartbeat_interval_ms,
                session_timeout_ms=self._session_timeout_ms,
                retry_backoff_ms=self._retry_backoff_ms,
                enable_auto_commit=self._enable_auto_commit,
                auto_commit_interval_ms=self._auto_commit_interval_ms,
                assignors=self._partition_assignment_strategy,
                exclude_internal_topics=self._exclude_internal_topics,
                rebalance_timeout_ms=self._rebalance_timeout_ms,
                max_poll_interval_ms=self._max_poll_interval_ms,
            )
            if self._subscription.subscription is not None:
                if self._subscription.partitions_auto_assigned():
                    # Either we passed `topics` to constructor or `subscribe`
                    # was called before `start`
                    await self._subscription.wait_for_assignment()
                else:
                    # `assign` was called before `start`. We did not start
                    # this task on that call, as coordinator was yet to be
                    # created
                    self._coordinator.start_commit_offsets_refresh_task(
                        self._subscription.subscription.assignment
                    )
        else:
            # Using a simple assignment coordinator for reassignment on
            # metadata changes
            self._coordinator = NoGroupCoordinator(
                self._client,
                self._subscription,
                exclude_internal_topics=self._exclude_internal_topics,
            )

            if (
                self._subscription.subscription is not None
                and self._subscription.partitions_auto_assigned()
            ):
                # Either we passed `topics` to constructor or `subscribe`
                # was called before `start`
                await self._client.force_metadata_update()
                self._coordinator.assign_all_partitions(check_unknown=True)

    async def _wait_topics(self):
        if self._subscription.subscription is not None:
            for topic in self._subscription.subscription.topics:
                await self._client._wait_on_metadata(topic)

    def _validate_topics(self, topics):
        if not isinstance(topics, tuple | set | list):
            raise TypeError("Topics should be list of strings")
        return set(topics)

    def assign(self, partitions):
        """Manually assign a list of :class:`.TopicPartition` to this consumer.

        This interface does not support incremental assignment and will
        replace the previous assignment (if there was one).

        Arguments:
            partitions (list(TopicPartition)): assignment for this instance.

        Raises:
            IllegalStateError: if consumer has already called :meth:`subscribe`

        Warning:
            It is not possible to use both manual partition assignment with
            :meth:`assign` and group assignment with :meth:`subscribe`.

        Note:
            Manual topic assignment through this method does not use the
            consumer's group management functionality. As such, there will be
            **no rebalance operation triggered** when group membership or
            cluster and topic metadata change.
        """
        self._subscription.assign_from_user(partitions)
        self._client.set_topics([tp.topic for tp in partitions])

        # If called before `start` we will delegate this to `start` call
        if self._coordinator is not None and self._group_id is not None:
            # refresh commit positions for all assigned partitions
            assignment = self._subscription.subscription.assignment
            self._coordinator.start_commit_offsets_refresh_task(assignment)

    def assignment(self):
        """Get the set of partitions currently assigned to this consumer.

        If partitions were directly assigned using :meth:`assign`, then this will
        simply return the same partitions that were previously assigned.

        If topics were subscribed using :meth:`subscribe`, then this will give
        the set of topic partitions currently assigned to the consumer (which
        may be empty if the assignment hasn't happened yet or if the partitions
        are in the process of being reassigned).

        Returns:
            set(TopicPartition): the set of partitions currently assigned to
            this consumer
        """
        return self._subscription.assigned_partitions()

    async def stop(self):
        """Close the consumer, while waiting for finalizers:

        * Commit last consumed message if autocommit enabled
        * Leave group if used Consumer Groups
        """
        if self._closed:
            return
        log.debug("Closing the KafkaConsumer.")
        self._closed = True
        if self._coordinator:
            await self._coordinator.close()
        if self._fetcher:
            await self._fetcher.close()
        await self._client.close()
        log.debug("The KafkaConsumer has closed.")

    async def commit(self, offsets=None):
        """Commit offsets to Kafka.

        This commits offsets only to Kafka. The offsets committed using this
        API will be used on the first fetch after every rebalance and also on
        startup. As such, if you need to store offsets in anything other than
        Kafka, this API should not be used.

        Currently only supports kafka-topic offset storage (not Zookeeper)

        When explicitly passing `offsets` use either offset of next record,
        or tuple of offset and metadata::

            tp = TopicPartition(msg.topic, msg.partition)
            metadata = "Some utf-8 metadata"
            # Either
            await consumer.commit({tp: msg.offset + 1})
            # Or position directly
            await consumer.commit({tp: (msg.offset + 1, metadata)})

        .. note:: If you want *fire and forget* commit, like
            :meth:`~kafka.KafkaConsumer.commit_async` in `kafka-python`_, just
            run it in a task. Something like::

                fut = loop.create_task(consumer.commit())
                fut.add_done_callback(on_commit_done)

        Arguments:
            offsets (dict, Optional): A mapping from :class:`.TopicPartition` to
              ``(offset, metadata)`` to commit with the configured ``group_id``.
              Defaults to current consumed offsets for all subscribed partitions.
        Raises:
            ~aiokafka.errors.CommitFailedError: If membership already changed on broker.
            ~aiokafka.errors.IllegalOperation: If used with ``group_id == None``.
            ~aiokafka.errors.IllegalStateError: If partitions not assigned.
            ~aiokafka.errors.KafkaError: If commit failed on broker side. This
                could be due to invalid offset, too long metadata, authorization
                failure, etc.
            ValueError: If offsets is of wrong format.

        .. versionchanged:: 0.4.0

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` in case of unassigned
            partition.

        .. versionchanged:: 0.4.0

            Will now raise :exc:`~aiokafka.errors.CommitFailedError` in case
            membership changed, as (possibly) this partition is handled by
            another consumer.

        .. _kafka-python: https://github.com/dpkp/kafka-python
        """
        if self._group_id is None:
            raise IllegalOperation("Requires group_id")

        subscription = self._subscription.subscription
        if subscription is None:
            raise IllegalStateError("Not subscribed to any topics")
        assignment = subscription.assignment
        if assignment is None:
            raise IllegalStateError("No partitions assigned")

        if offsets is None:
            offsets = assignment.all_consumed_offsets()
        else:
            offsets = commit_structure_validate(offsets)
            for tp in offsets:
                if tp not in assignment.tps:
                    raise IllegalStateError(f"Partition {tp} is not assigned")

        await self._coordinator.commit_offsets(assignment, offsets)

    async def committed(self, partition):
        """Get the last committed offset for the given partition. (whether the
        commit happened by this process or another).

        This offset will be used as the position for the consumer in the event
        of a failure.

        This call will block to do a remote call to get the latest offset, as
        those are not cached by consumer (Transactional Producer can change
        them without Consumer knowledge as of Kafka 0.11.0)

        Arguments:
            partition (TopicPartition): the partition to check

        Returns:
            The last committed offset, or None if there was no prior commit.

        Raises:
            IllegalOperation: If used with ``group_id == None``
        """
        if self._group_id is None:
            raise IllegalOperation("Requires group_id")

        commit_map = await self._coordinator.fetch_committed_offsets([partition])
        if partition in commit_map:
            committed = commit_map[partition].offset
            if committed == -1:
                committed = None
        else:
            committed = None
        return committed

    async def topics(self):
        """Get all topics the user is authorized to view.

        Returns:
            set: topics
        """
        cluster = await self._client.fetch_all_metadata()
        return cluster.topics()

    def partitions_for_topic(self, topic):
        """Get metadata about the partitions for a given topic.

        This method will return `None` if Consumer does not already have
        metadata for this topic.

        Arguments:
            topic (str): topic to check

        Returns:
            set: partition ids
        """
        return self._client.cluster.partitions_for_topic(topic)

    async def position(self, partition):
        """Get the offset of the *next record* that will be fetched (if a
        record with that offset exists on broker).

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int: offset

        Raises:
            IllegalStateError: partition is not assigned

        .. versionchanged:: 0.4.0

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` in case of unassigned
            partition
        """
        while True:
            if not self._subscription.is_assigned(partition):
                raise IllegalStateError(f"Partition {partition} is not assigned")

            assignment = self._subscription.subscription.assignment
            tp_state = assignment.state_value(partition)
            if not tp_state.has_valid_position:
                self._coordinator.check_errors()
                await asyncio.wait(
                    [tp_state.wait_for_position(), assignment.unassign_future],
                    timeout=self._request_timeout_ms / 1000,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not tp_state.has_valid_position:
                    if self._subscription.subscription is None:
                        raise IllegalStateError(
                            f"Partition {partition} is not assigned"
                        )
                    if self._subscription.subscription.assignment is None:
                        self._coordinator.check_errors()
                        await self._subscription.wait_for_assignment()
                    continue
            return tp_state.position

    def highwater(self, partition):
        """Last known highwater offset for a partition.

        A highwater offset is the offset that will be assigned to the next
        message that is produced. It may be useful for calculating lag, by
        comparing with the reported position. Note that both position and
        highwater refer to the *next* offset - i.e., highwater offset is one
        greater than the newest available message.

        Highwater offsets are returned as part of ``FetchResponse``, so will
        not be available if messages for this partition were not requested yet.

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int or None: offset if available
        """
        assert self._subscription.is_assigned(partition), "Partition is not assigned"
        assignment = self._subscription.subscription.assignment
        return assignment.state_value(partition).highwater

    def last_stable_offset(self, partition):
        """Returns the Last Stable Offset of a topic. It will be the last
        offset up to which point all transactions were completed. Only
        available in with isolation_level `read_committed`, in
        `read_uncommitted` will always return -1. Will return None for older
        Brokers.

        As with :meth:`highwater` will not be available until some messages are
        consumed.

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int or None: offset if available
        """
        assert self._subscription.is_assigned(partition), "Partition is not assigned"
        assignment = self._subscription.subscription.assignment
        return assignment.state_value(partition).lso

    def last_poll_timestamp(self, partition):
        """Returns the timestamp of the last poll of this partition (in ms).
        It is the last time :meth:`highwater` and :meth:`last_stable_offset` were
        updated. However it does not mean that new messages were received.

        As with :meth:`highwater` will not be available until some messages are
        consumed.

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int or None: timestamp if available
        """
        assert self._subscription.is_assigned(partition), "Partition is not assigned"
        assignment = self._subscription.subscription.assignment
        return assignment.state_value(partition).timestamp

    def seek(self, partition, offset):
        """Manually specify the fetch offset for a :class:`.TopicPartition`.

        Overrides the fetch offsets that the consumer will use on the next
        :meth:`getmany`/:meth:`getone` call. If this API is invoked for the same
        partition more than once, the latest offset will be used on the next
        fetch.

        Note:
            You may lose data if this API is arbitrarily used in the middle
            of consumption to reset the fetch offsets. Use it either on
            rebalance listeners or after all pending messages are processed.

        Arguments:
            partition (TopicPartition): partition for seek operation
            offset (int): message offset in partition

        Raises:
            ValueError: if offset is not a positive integer
            IllegalStateError: partition is not currently assigned

        .. versionchanged:: 0.4.0

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` and :exc:`ValueError` in
            respective cases.
        """
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("Offset must be a positive integer")
        log.debug("Seeking to offset %s for partition %s", offset, partition)
        self._fetcher.seek_to(partition, offset)

    async def seek_to_beginning(self, *partitions):
        """Seek to the oldest available offset for partitions.

        Arguments:
            *partitions: Optionally provide specific :class:`.TopicPartition`,
                otherwise default to all assigned partitions.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            TypeError: If partitions are not instances of :class:`.TopicPartition`

        .. versionadded:: 0.3.0

        """
        if not all(isinstance(p, TopicPartition) for p in partitions):
            raise TypeError("partitions must be TopicPartition instances")

        if not partitions:
            partitions = self._subscription.assigned_partitions()
            assert partitions, "No partitions are currently assigned"
        else:
            not_assigned = set(partitions) - self._subscription.assigned_partitions()
            if not_assigned:
                raise IllegalStateError(f"Partitions {not_assigned} are not assigned")

        for tp in partitions:
            log.debug("Seeking to beginning of partition %s", tp)

        fut = self._fetcher.request_offset_reset(
            partitions, OffsetResetStrategy.EARLIEST
        )
        assignment = self._subscription.subscription.assignment
        await asyncio.wait(
            [fut, assignment.unassign_future],
            timeout=self._request_timeout_ms / 1000,
            return_when=asyncio.FIRST_COMPLETED,
        )
        self._coordinator.check_errors()
        return fut.done()

    async def seek_to_end(self, *partitions):
        """Seek to the most recent available offset for partitions.

        Arguments:
            *partitions: Optionally provide specific :class:`.TopicPartition`,
                otherwise default to all assigned partitions.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            TypeError: If partitions are not instances of :class:`.TopicPartition`

        .. versionadded:: 0.3.0

        """
        if not all(isinstance(p, TopicPartition) for p in partitions):
            raise TypeError("partitions must be TopicPartition instances")

        if not partitions:
            partitions = self._subscription.assigned_partitions()
            assert partitions, "No partitions are currently assigned"
        else:
            not_assigned = set(partitions) - self._subscription.assigned_partitions()
            if not_assigned:
                raise IllegalStateError(f"Partitions {not_assigned} are not assigned")

        for tp in partitions:
            log.debug("Seeking to end of partition %s", tp)
        fut = self._fetcher.request_offset_reset(partitions, OffsetResetStrategy.LATEST)
        assignment = self._subscription.subscription.assignment
        await asyncio.wait(
            [fut, assignment.unassign_future],
            timeout=self._request_timeout_ms / 1000,
            return_when=asyncio.FIRST_COMPLETED,
        )
        self._coordinator.check_errors()
        return fut.done()

    async def seek_to_committed(self, *partitions):
        """Seek to the committed offset for partitions.

        Arguments:
            *partitions: Optionally provide specific :class:`.TopicPartition`,
                otherwise default to all assigned partitions.

        Returns:
            dict(TopicPartition, int): mapping
            of the currently committed offsets.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            IllegalOperation: If used with ``group_id == None``

        .. versionchanged:: 0.3.0

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` in case of unassigned
            partition
        """
        if not all(isinstance(p, TopicPartition) for p in partitions):
            raise TypeError("partitions must be TopicPartition instances")

        if not partitions:
            partitions = self._subscription.assigned_partitions()
            assert partitions, "No partitions are currently assigned"
        else:
            not_assigned = set(partitions) - self._subscription.assigned_partitions()
            if not_assigned:
                raise IllegalStateError(f"Partitions {not_assigned} are not assigned")

        committed_offsets = {}
        for tp in partitions:
            offset = await self.committed(tp)
            committed_offsets[tp] = offset
            log.debug("Seeking to committed of partition %s %s", tp, offset)
            if offset and offset > 0:
                self._fetcher.seek_to(tp, offset)
        return committed_offsets

    async def offsets_for_times(self, timestamps):
        """
        Look up the offsets for the given partitions by timestamp. The returned
        offset for each partition is the earliest offset whose timestamp is
        greater than or equal to the given timestamp in the corresponding
        partition.

        The consumer does not have to be assigned the partitions.

        If the message format version in a partition is before 0.10.0, i.e.
        the messages do not have timestamps, ``None`` will be returned for that
        partition.

        Note:
            This method may block indefinitely if the partition does not exist.

        Arguments:
            timestamps (dict(TopicPartition, int)): mapping from partition
                to the timestamp to look up. Unit should be milliseconds since
                beginning of the epoch (midnight Jan 1, 1970 (UTC))

        Returns:
            dict(TopicPartition, OffsetAndTimestamp): mapping from
            partition to the timestamp and offset of the first message with
            timestamp greater than or equal to the target timestamp. None will
            be returned for the partition if there is no such message.

        Raises:
            ValueError: If the target timestamp is negative
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in `request_timeout_ms`

        .. versionadded:: 0.3.0

        """
        for tp, ts in timestamps.items():
            timestamps[tp] = int(ts)
            if ts < 0:
                raise ValueError(
                    f"The target time for partition {tp} is {ts}."
                    " The target time cannot be negative."
                )
        offsets = await self._fetcher.get_offsets_by_times(
            timestamps, self._request_timeout_ms
        )
        return offsets

    async def beginning_offsets(self, partitions):
        """Get the first offset for the given partitions.

        This method does not change the current consumer position of the
        partitions.

        Note:
            This method may block indefinitely if the partition does not exist.

        Arguments:
            partitions (list[TopicPartition]): List of :class:`.TopicPartition`
                instances to fetch offsets for.

        Returns:
            dict [TopicPartition, int]: mapping of partition to  earliest
            available offset.

        Raises:
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in `request_timeout_ms`.

        .. versionadded:: 0.3.0

        """
        offsets = await self._fetcher.beginning_offsets(
            partitions, self._request_timeout_ms
        )
        return offsets

    async def end_offsets(self, partitions):
        """Get the last offset for the given partitions. The last offset of a
        partition is the offset of the upcoming message, i.e. the offset of the
        last available message + 1.

        This method does not change the current consumer position of the
        partitions.

        Note:
            This method may block indefinitely if the partition does not exist.

        Arguments:
            partitions (list[TopicPartition]): List of :class:`.TopicPartition`
                instances to fetch offsets for.

        Returns:
            dict [TopicPartition, int]: mapping of partition to last
            available offset + 1.

        Raises:
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in ``request_timeout_ms``

        .. versionadded:: 0.3.0

        """
        offsets = await self._fetcher.end_offsets(partitions, self._request_timeout_ms)
        return offsets

    def subscribe(self, topics=(), pattern=None, listener=None):
        """Subscribe to a list of topics, or a topic regex pattern.

        Partitions will be dynamically assigned via a group coordinator.
        Topic subscriptions are not incremental: this list will replace the
        current assignment (if there is one).

        This method is incompatible with :meth:`assign`.

        Arguments:
           topics (list): List of topics for subscription.
           pattern (str): Pattern to match available topics. You must provide
               either topics or pattern, but not both.
           listener (ConsumerRebalanceListener): Optionally include listener
               callback, which will be called before and after each rebalance
               operation.
               As part of group management, the consumer will keep track of
               the list of consumers that belong to a particular group and
               will trigger a rebalance operation if one of the following
               events trigger:

               * Number of partitions change for any of the subscribed topics
               * Topic is created or deleted
               * An existing member of the consumer group dies
               * A new member is added to the consumer group

               When any of these events are triggered, the provided listener
               will be invoked first to indicate that the consumer's
               assignment has been revoked, and then again when the new
               assignment has been received. Note that this listener will
               immediately override any listener set in a previous call
               to subscribe. It is guaranteed, however, that the partitions
               revoked/assigned
               through this interface are from topics subscribed in this call.
        Raises:
            IllegalStateError: if called after previously calling :meth:`assign`
            ValueError: if neither topics or pattern is provided or both
               are provided
            TypeError: if listener is not a :class:`.ConsumerRebalanceListener`
        """
        if not (topics or pattern):
            raise TypeError("You should provide either `topics` or `pattern`")
        if topics and pattern:
            raise TypeError("You can't provide both `topics` and `pattern`")
        if listener is not None and not isinstance(listener, ConsumerRebalanceListener):
            raise TypeError(
                "listener should be an instance of ConsumerRebalanceListener"
            )
        if pattern is not None:
            try:
                pattern = re.compile(pattern)
            except re.error as err:
                raise ValueError(f"{pattern!r} is not a valid pattern: {err}") from err
            self._subscription.subscribe_pattern(pattern=pattern, listener=listener)
            # NOTE: set_topics will trigger a rebalance, so the coordinator
            # will get the initial subscription shortly by ``metadata_changed``
            # handler.
            self._client.set_topics([])
            log.info("Subscribed to topic pattern: %s", pattern)
        elif topics:
            topics = self._validate_topics(topics)
            self._subscription.subscribe(topics=topics, listener=listener)
            self._client.set_topics(self._subscription.subscription.topics)
            if self._group_id is None:
                # We have reset the assignment, but client.set_topics will
                # not always do a metadata update. We force it to do it even
                # if metadata did not change. This will trigger a reassignment
                # on NoGroupCoordinator, but only if snapshot did not change,
                # thus we reset it too.
                self._client.force_metadata_update()
                if self._coordinator is not None:
                    self._coordinator._metadata_snapshot = {}
            log.info("Subscribed to topic(s): %s", topics)

    def subscription(self):
        """Get the current topics subscription.

        Returns:
            frozenset(str): a set of topics
        """
        return self._subscription.topics

    def unsubscribe(self):
        """Unsubscribe from all topics and clear all assigned partitions."""
        self._subscription.unsubscribe()
        if self._group_id is not None:
            self._coordinator.maybe_leave_group()
        self._client.set_topics([])
        log.info("Unsubscribed all topics or patterns and assigned partitions")

    async def getone(self, *partitions) -> ConsumerRecord:
        """
        Get one message from Kafka.
        If no new messages prefetched, this method will wait for it.

        Arguments:
            partitions (list(TopicPartition)): Optional list of partitions to
                return from. If no partitions specified then returned message
                will be from any partition, which consumer is subscribed to.

        Returns:
            ~aiokafka.structs.ConsumerRecord: the message

        Will return instance of

        .. code:: python

            collections.namedtuple(
                "ConsumerRecord",
                ["topic", "partition", "offset", "key", "value"])

        Example usage:

        .. code:: python

            while True:
                message = await consumer.getone()
                topic = message.topic
                partition = message.partition
                # Process message
                print(message.offset, message.key, message.value)

        """
        assert all(isinstance(k, TopicPartition) for k in partitions)
        if self._closed:
            raise ConsumerStoppedError()

        # Raise coordination errors if any
        self._coordinator.check_errors()

        with self._subscription.fetch_context():
            msg = await self._fetcher.next_record(partitions)
        return msg

    async def getmany(
        self, *partitions, timeout_ms=0, max_records=None
    ) -> dict[TopicPartition, list[ConsumerRecord]]:
        """Get messages from assigned topics / partitions.

        Prefetched messages are returned in batches by topic-partition.
        If messages is not available in the prefetched buffer this method waits
        `timeout_ms` milliseconds.

        Arguments:
            partitions (list[TopicPartition]): The partitions that need
                fetching message. If no one partition specified then all
                subscribed partitions will be used
            timeout_ms (int, Optional): milliseconds spent waiting if
                data is not available in the buffer. If 0, returns immediately
                with any records that are available currently in the buffer,
                else returns empty. Must not be negative. Default: 0
        Returns:
            dict(TopicPartition, list[ConsumerRecord]): topic to list of
            records since the last fetch for the subscribed list of topics and
            partitions

        Example usage:


        .. code:: python

            data = await consumer.getmany()
            for tp, messages in data.items():
                topic = tp.topic
                partition = tp.partition
                for message in messages:
                    # Process message
                    print(message.offset, message.key, message.value)

        """
        assert all(isinstance(k, TopicPartition) for k in partitions)
        if self._closed:
            raise ConsumerStoppedError()

        if max_records is not None and (
            not isinstance(max_records, int) or max_records < 1
        ):
            raise ValueError("`max_records` must be a positive Integer")

        # Raise coordination errors if any
        self._coordinator.check_errors()

        timeout = timeout_ms / 1000
        with self._subscription.fetch_context():
            records = await self._fetcher.fetched_records(
                partitions, timeout, max_records=max_records or self._max_poll_records
            )
        return records

    def pause(self, *partitions):
        """Suspend fetching from the requested partitions.

        Future calls to :meth:`.getmany` will not return any records from these
        partitions until they have been resumed using :meth:`.resume`.

        Note: This method does not affect partition subscription.
        In particular, it does not cause a group rebalance when automatic
        assignment is used.

        Arguments:
            *partitions (list[TopicPartition]): Partitions to pause.
        """
        if not all(isinstance(p, TopicPartition) for p in partitions):
            raise TypeError("partitions must be TopicPartition namedtuples")

        for partition in partitions:
            log.debug("Pausing partition %s", partition)
            self._subscription.pause(partition)

    def paused(self):
        """Get the partitions that were previously paused using
        :meth:`.pause`.

        Returns:
            set[TopicPartition]: partitions
        """
        return self._subscription.paused_partitions()

    def resume(self, *partitions):
        """Resume fetching from the specified (paused) partitions.

        Arguments:
            *partitions (list[TopicPartition]): Partitions to resume.
        """
        if not all(isinstance(p, TopicPartition) for p in partitions):
            raise TypeError("partitions must be TopicPartition namedtuples")

        for partition in partitions:
            log.debug("Resuming partition %s", partition)
            self._subscription.resume(partition)

    def __aiter__(self):
        if self._closed:
            raise ConsumerStoppedError()
        return self

    async def __anext__(self) -> ConsumerRecord:
        """Asyncio iterator interface for consumer

        Note:
            TopicAuthorizationFailedError and OffsetOutOfRangeError
            exceptions can be raised in iterator.
            All other KafkaError exceptions will be logged and not raised
        """
        while True:
            try:
                return await self.getone()
            except ConsumerStoppedError:  # noqa: PERF203
                raise StopAsyncIteration from None
            except RecordTooLargeError:
                log.exception("error in consumer iterator: %s")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

import asyncio
import collections
import logging
import time

import aiokafka.errors as Errors
from aiokafka.client import ConnectionGroup, CoordinationType
from aiokafka.errors import (
    ConcurrentTransactions,
    CoordinatorLoadInProgressError,
    CoordinatorNotAvailableError,
    DuplicateSequenceNumber,
    GroupAuthorizationFailedError,
    IncompatibleBrokerVersion,
    InvalidProducerEpoch,
    InvalidProducerIdMapping,
    InvalidTxnState,
    KafkaError,
    NotCoordinatorError,
    OperationNotAttempted,
    OutOfOrderSequenceNumber,
    ProducerFenced,
    RequestTimedOutError,
    TopicAuthorizationFailedError,
    TransactionalIdAuthorizationFailed,
    UnknownTopicOrPartitionError,
)
from aiokafka.protocol.produce import ProduceRequest
from aiokafka.protocol.transaction import (
    AddOffsetsToTxnRequest,
    AddPartitionsToTxnRequest,
    EndTxnRequest,
    InitProducerIdRequest,
    TxnOffsetCommitRequest,
)
from aiokafka.structs import TopicPartition
from aiokafka.util import create_task

log = logging.getLogger(__name__)

BACKOFF_OVERRIDE = 0.02  # 20ms wait between transactions is better than 100ms.


class Sender:
    """Background processing abstraction for Producer. By all means just
    separates batch delivery and transaction management from the main Producer
    code
    """

    def __init__(
        self,
        client,
        *,
        acks,
        txn_manager,
        message_accumulator,
        retry_backoff_ms,
        linger_ms,
        request_timeout_ms,
    ):
        self.client = client
        self._txn_manager = txn_manager
        self._acks = acks

        self._message_accumulator = message_accumulator
        self._sender_task = None
        self._in_flight = set()
        self._muted_partitions = set()
        self._coordinators = {}
        self._retry_backoff = retry_backoff_ms / 1000
        self._request_timeout_ms = request_timeout_ms
        self._linger_time = linger_ms / 1000

    async def start(self):
        # If producer is idempotent we need to assure we have PID found
        await self._maybe_wait_for_pid()
        self._sender_task = create_task(self._sender_routine())
        self._sender_task.add_done_callback(self._fail_all)

    def _fail_all(self, task):
        """Called when sender fails. Will fail all pending batches, as they
        will never be delivered as well as fail transaction
        """
        if task.cancelled():
            return
        task_exception = task.exception()

        if task_exception is not None:
            self._message_accumulator.fail_all(task_exception)
            if self._txn_manager is not None:
                self._txn_manager.fatal_error(task_exception)

    @property
    def sender_task(self):
        return self._sender_task

    async def close(self):
        if self._sender_task is not None and not self._sender_task.done():
            self._sender_task.cancel()
            await self._sender_task

    async def _sender_routine(self):
        """Background task, that sends pending batches to leader nodes for
        batch's partition. This incapsulates same logic as Java's `Sender`
        background thread. Because we use asyncio this is more event based
        loop, rather than counting timeout till next possible even like in
        Java.
        """

        tasks = set()
        txn_task = None  # Track a single task for transaction interactions
        try:
            while True:
                # If indempotence or transactions are turned on we need to
                # have a valid PID to send any request below
                await self._maybe_wait_for_pid()

                waiters = set()
                # As transaction coordination is done via a single, separate
                # socket we do not need to pump it to several nodes, as we do
                # with produce requests.
                # We will only have 1 task at a time and will try to spawn
                # another once that is done.
                txn_manager = self._txn_manager
                muted_partitions = self._muted_partitions
                if txn_manager is not None and txn_manager.transactional_id is not None:
                    if txn_task is None or txn_task.done():
                        txn_task = self._maybe_do_transactional_request()
                        if txn_task is not None:
                            tasks.add(txn_task)
                        else:
                            # Waiters will not be awaited on exit, tasks will
                            waiters.add(txn_manager.make_task_waiter())
                    # We can't have a race condition between
                    # AddPartitionsToTxnRequest and a ProduceRequest, so we
                    # mute the partition until added.
                    muted_partitions = (
                        muted_partitions | txn_manager.partitions_to_add()
                    )
                (
                    batches,
                    unknown_leaders_exist,
                ) = self._message_accumulator.drain_by_nodes(
                    ignore_nodes=self._in_flight,
                    muted_partitions=muted_partitions,
                )

                # create produce task for every batch
                for node_id, node_batches in batches.items():
                    task = create_task(self._send_produce_req(node_id, node_batches))
                    self._in_flight.add(node_id)
                    for tp in node_batches:
                        self._muted_partitions.add(tp)
                    tasks.add(task)

                if unknown_leaders_exist:
                    # we have at least one unknown partition's leader,
                    # try to update cluster metadata and wait backoff time
                    fut = self.client.force_metadata_update()
                    waiters |= tasks.union([fut])
                else:
                    fut = self._message_accumulator.data_waiter()
                    waiters |= tasks.union([fut])

                # wait when:
                # * At least one of produce task is finished
                # * Data for new partition arrived
                # * Metadata update if partition leader unknown
                done, _ = await asyncio.wait(
                    waiters,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # done tasks should never produce errors, if they are it's a
                # bug
                for task in done:
                    task.result()

                tasks -= done

        except asyncio.CancelledError:
            # done tasks should never produce errors, if they are it's a bug
            for task in tasks:
                await task
        except (
            ProducerFenced,
            OutOfOrderSequenceNumber,
            TransactionalIdAuthorizationFailed,
        ):
            raise
        except Exception as exc:  # pragma: no cover
            log.exception("Unexpected error in sender routine")
            raise KafkaError("Unexpected error during batch delivery") from exc

    async def _maybe_wait_for_pid(self):
        if self._txn_manager is None or self._txn_manager.has_pid():
            return

        while True:
            # If transactions are used we can't just send to a random node, but
            # need to find a suitable coordination node
            if self._txn_manager.transactional_id is not None:
                node_id = await self._find_coordinator(
                    CoordinationType.TRANSACTION, self._txn_manager.transactional_id
                )
            else:
                node_id = self.client.get_random_node()
            success = await self._do_init_pid(node_id)
            if not success:
                await self.client.force_metadata_update()
            else:
                break

    def _coordinator_dead(self, coordinator_type):
        self._coordinators.pop(coordinator_type, None)

    async def _find_coordinator(self, coordinator_type, coordinator_key):
        assert self._txn_manager is not None
        if coordinator_type in self._coordinators:
            return self._coordinators[coordinator_type]
        while True:
            try:
                coordinator_id = await self.client.coordinator_lookup(
                    coordinator_type, coordinator_key
                )
            except Errors.TransactionalIdAuthorizationFailed as err:
                new_err = Errors.TransactionalIdAuthorizationFailed(
                    self._txn_manager.transactional_id
                )
                raise new_err from err
            except Errors.GroupAuthorizationFailedError as err:
                new_err = Errors.GroupAuthorizationFailedError(coordinator_key)
                raise new_err from err
            except (
                Errors.CoordinatorNotAvailableError,
                Errors.NodeNotReadyError,
                Errors.RequestTimedOutError,
            ):
                await self.client.force_metadata_update()
                await asyncio.sleep(self._retry_backoff)
                continue
            except Errors.KafkaError as err:
                log.error("FindCoordinator Request failed: %s", err)
                raise KafkaError(repr(err)) from err

            # Try to connect to confirm that the connection can be
            # established.
            ready = await self.client.ready(
                coordinator_id, group=ConnectionGroup.COORDINATION
            )
            if not ready:
                await asyncio.sleep(self._retry_backoff)
                continue

            self._coordinators[coordinator_type] = coordinator_id

            if coordinator_type == CoordinationType.GROUP:
                log.info(
                    "Discovered coordinator %s for group id %s",
                    coordinator_id,
                    coordinator_key,
                )
            else:
                log.info(
                    "Discovered coordinator %s for transactional id %s",
                    coordinator_id,
                    coordinator_key,
                )
            return coordinator_id

    async def _do_init_pid(self, node_id):
        handler = InitPIDHandler(self)
        return await handler.do(node_id)

    ###########################################################################
    # Message delivery handler('s')
    ###########################################################################

    async def _send_produce_req(self, node_id, batches):
        """Create produce request to node
        If producer configured with `retries`>0 and produce response contain
        "failed" partitions produce request for this partition will try
        resend to broker `retries` times with `retry_timeout_ms` timeouts.

        Arguments:
            node_id (int): kafka broker identifier
            batches (dict): dictionary of {TopicPartition: MessageBatch}
        """
        t0 = time.monotonic()

        handler = SendProduceReqHandler(self, batches)
        await handler.do(node_id)

        # if batches for node is processed in less than a linger seconds
        # then waiting for the remaining time
        sleep_time = self._linger_time - (time.monotonic() - t0)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

        self._in_flight.remove(node_id)
        for tp in batches:
            self._muted_partitions.remove(tp)

    ###########################################################################
    # Transaction handler('s')
    ###########################################################################

    def _maybe_do_transactional_request(self):
        txn_manager = self._txn_manager

        # If we have any new partitions, still not added to the transaction
        # we need to do that before committing
        tps = txn_manager.partitions_to_add()
        if tps:
            return create_task(self._do_add_partitions_to_txn(tps))

        # We need to add group to transaction before we can commit the offset
        group_id = txn_manager.consumer_group_to_add()
        if group_id is not None:
            return create_task(self._do_add_offsets_to_txn(group_id))

        # Now commit the added group's offset
        commit_data = txn_manager.offsets_to_commit()
        if commit_data is not None:
            offsets, group_id = commit_data
            return create_task(self._do_txn_offset_commit(offsets, group_id))

        commit_result = txn_manager.needs_transaction_commit()
        if commit_result is not None:
            return create_task(self._do_txn_commit(commit_result))

        return None

    async def _do_add_partitions_to_txn(self, tps):
        # First assert we have a valid coordinator to send the request to
        node_id = await self._find_coordinator(
            CoordinationType.TRANSACTION, self._txn_manager.transactional_id
        )
        handler = AddPartitionsToTxnHandler(self, tps)
        return await handler.do(node_id)

    async def _do_add_offsets_to_txn(self, group_id):
        # First assert we have a valid coordinator to send the request to
        node_id = await self._find_coordinator(
            CoordinationType.TRANSACTION, self._txn_manager.transactional_id
        )
        handler = AddOffsetsToTxnHandler(self, group_id)
        return await handler.do(node_id)

    async def _do_txn_offset_commit(self, offsets, group_id):
        # Fast return if nothing to commit
        if not offsets:
            return
        # NOTE: We send this one to GROUP coordinator, not TRANSACTION
        try:
            node_id = await self._find_coordinator(CoordinationType.GROUP, group_id)
        except GroupAuthorizationFailedError as exc:
            self._txn_manager.error_transaction(exc)
            return
        log.debug(
            "Sending offset-commit request with %s for group %s to %s",
            offsets,
            group_id,
            node_id,
        )
        handler = TxnOffsetCommitHandler(self, offsets, group_id)
        await handler.do(node_id)

    async def _do_txn_commit(self, commit_result):
        """Committing transaction should be done with care.
            Transactional requests will be blocked by this coroutine, so no new
        offsets or new partitions will be added.
            Produce requests will be stopped, as accumulator will not be
        yielding any new batches.
        """
        # First we need to ensure that all pending messages were flushed
        # before committing. Note, that this will only flush batches available
        # till this point, no new ones.
        await self._message_accumulator.flush_for_commit()

        txn_manager = self._txn_manager

        # If we never sent any data to begin with, no need to commit
        if txn_manager.is_empty_transaction():
            txn_manager.complete_transaction()
            return

        # First assert we have a valid coordinator to send the request to
        node_id = await self._find_coordinator(
            CoordinationType.TRANSACTION, txn_manager.transactional_id
        )

        handler = EndTxnHandler(self, commit_result)
        await handler.do(node_id)


class BaseHandler:
    group = ConnectionGroup.DEFAULT

    def __init__(self, sender):
        self._sender = sender
        self._default_backoff = sender._retry_backoff

    async def do(self, node_id):
        req = self.create_request()
        try:
            resp = await self._sender.client.send(node_id, req, group=self.group)
        except IncompatibleBrokerVersion:
            raise
        except (Errors.NodeNotReadyError, Errors.RequestTimedOutError) as err:
            log.warning("Cannot send %r to %s: %r", req.__class__, node_id, err)
            retry_backoff = self.handle_error()
            await asyncio.sleep(retry_backoff)
            return False
        except KafkaError as err:
            log.warning("Could not send %r: %r", req.__class__, err)
            await asyncio.sleep(self._default_backoff)
            return False

        retry_backoff = self.handle_response(resp)
        if retry_backoff is not None:
            await asyncio.sleep(retry_backoff)
            return False  # Failure
        else:
            return True  # Success

    def create_request(self):
        raise NotImplementedError  # pragma: no cover

    def handle_response(self, response):
        raise NotImplementedError  # pragma: no cover

    def handle_error(self):
        raise NotImplementedError  # pragma: no cover


class InitPIDHandler(BaseHandler):
    def create_request(self):
        txn_manager = self._sender._txn_manager
        return InitProducerIdRequest(
            transactional_id=txn_manager.transactional_id,
            transaction_timeout_ms=txn_manager.transaction_timeout_ms,
        )

    def handle_response(self, resp):
        txn_manager = self._sender._txn_manager
        error_type = Errors.for_code(resp.error_code)
        if error_type is Errors.NoError:
            log.debug(
                "Successfully found PID=%s EPOCH=%s for Producer %s",
                resp.producer_id,
                resp.producer_epoch,
                self._sender.client._client_id,
            )
            self._sender._txn_manager.set_pid_and_epoch(
                resp.producer_id, resp.producer_epoch
            )
            return None
        elif (
            error_type is CoordinatorNotAvailableError
            or error_type is NotCoordinatorError
        ):
            self._sender._coordinator_dead(CoordinationType.TRANSACTION)
        elif (
            error_type is CoordinatorLoadInProgressError
            or error_type is ConcurrentTransactions
        ):
            pass
        elif error_type is TransactionalIdAuthorizationFailed:
            raise error_type(txn_manager.transactional_id)
        else:
            log.error("Unexpected error during InitProducerIdRequest: %s", error_type)
            raise error_type()

        return self._default_backoff

    def handle_error(self):
        self._sender._coordinator_dead(CoordinationType.TRANSACTION)
        return self._default_backoff


class AddPartitionsToTxnHandler(BaseHandler):
    group = ConnectionGroup.COORDINATION

    def __init__(self, sender, topic_partitions):
        super().__init__(sender)
        self._tps = topic_partitions

    def create_request(self):
        txn_manager = self._sender._txn_manager

        partition_data = collections.defaultdict(list)
        for tp in self._tps:
            partition_data[tp.topic].append(tp.partition)

        req = AddPartitionsToTxnRequest(
            transactional_id=txn_manager.transactional_id,
            producer_id=txn_manager.producer_id,
            producer_epoch=txn_manager.producer_epoch,
            topics=list(partition_data.items()),
        )
        return req

    def handle_response(self, resp):
        txn_manager = self._sender._txn_manager

        unauthorized_topics = set()
        for topic, partitions in resp.errors:
            for partition, error_code in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)

                if error_type is Errors.NoError:
                    log.debug("Added partition %s to transaction", tp)
                    txn_manager.partition_added(tp)
                elif (
                    error_type is CoordinatorNotAvailableError
                    or error_type is NotCoordinatorError
                ):
                    self._sender._coordinator_dead(CoordinationType.TRANSACTION)
                    return self._default_backoff
                elif error_type is ConcurrentTransactions:
                    # See KAFKA-5477: There is some time between commit and
                    # actual transaction marker write, that will produce this
                    # ConcurrentTransactions. We don't want the 100ms latency
                    # in that case.
                    if not txn_manager.txn_partitions:
                        return BACKOFF_OVERRIDE
                    else:
                        return self._default_backoff
                elif (
                    error_type is CoordinatorLoadInProgressError
                    or error_type is UnknownTopicOrPartitionError
                ):
                    return self._default_backoff
                elif error_type is InvalidProducerEpoch:
                    raise ProducerFenced()
                elif (
                    error_type is InvalidProducerIdMapping
                    or error_type is InvalidTxnState
                ):
                    raise error_type()
                elif error_type is TopicAuthorizationFailedError:
                    unauthorized_topics.add(topic)
                elif error_type is OperationNotAttempted:
                    pass
                elif error_type is TransactionalIdAuthorizationFailed:
                    raise error_type(txn_manager.transactional_id)
                else:
                    log.error(
                        "Could not add partition %s due to unexpected error: %s",
                        partition,
                        error_type,
                    )
                    raise error_type()
        if unauthorized_topics:
            txn_manager.error_transaction(
                TopicAuthorizationFailedError(unauthorized_topics)
            )
        return None

    def handle_error(self):
        self._sender._coordinator_dead(CoordinationType.TRANSACTION)
        return self._default_backoff


class AddOffsetsToTxnHandler(BaseHandler):
    group = ConnectionGroup.COORDINATION

    def __init__(self, sender, group_id):
        super().__init__(sender)
        self._group_id = group_id

    def create_request(self):
        txn_manager = self._sender._txn_manager

        req = AddOffsetsToTxnRequest(
            transactional_id=txn_manager.transactional_id,
            producer_id=txn_manager.producer_id,
            producer_epoch=txn_manager.producer_epoch,
            group_id=self._group_id,
        )
        return req

    def handle_response(self, resp):
        txn_manager = self._sender._txn_manager
        group_id = self._group_id

        error_type = Errors.for_code(resp.error_code)
        if error_type is Errors.NoError:
            log.debug("Successfully added consumer group %s to transaction", group_id)
            txn_manager.consumer_group_added(group_id)
            return None
        elif (
            error_type is CoordinatorNotAvailableError
            or error_type is NotCoordinatorError
        ):
            self._sender._coordinator_dead(CoordinationType.TRANSACTION)
        elif (
            error_type is CoordinatorLoadInProgressError
            or error_type is ConcurrentTransactions
        ):
            # We will just retry after backoff
            pass
        elif error_type is InvalidProducerEpoch:
            raise ProducerFenced()
        elif error_type is InvalidTxnState:
            raise error_type()
        elif error_type is TransactionalIdAuthorizationFailed:
            raise error_type(txn_manager.transactional_id)
        elif error_type is GroupAuthorizationFailedError:
            txn_manager.error_transaction(error_type(self._group_id))
            return None
        else:
            log.error(
                "Could not add consumer group due to unexpected error: %s", error_type
            )
            raise error_type()

        return self._default_backoff

    def handle_error(self):
        self._sender._coordinator_dead(CoordinationType.TRANSACTION)
        return self._default_backoff


class TxnOffsetCommitHandler(BaseHandler):
    group = ConnectionGroup.COORDINATION

    def __init__(self, sender, offsets, group_id):
        super().__init__(sender)
        self._offsets = offsets
        self._group_id = group_id

    def create_request(self):
        txn_manager = self._sender._txn_manager
        # create the offset commit request structure
        offset_data = collections.defaultdict(list)
        for tp, offset in sorted(self._offsets.items()):
            offset_data[tp.topic].append(
                (tp.partition, offset.offset, offset.metadata),
            )

        req = TxnOffsetCommitRequest(
            transactional_id=txn_manager.transactional_id,
            group_id=self._group_id,
            producer_id=txn_manager.producer_id,
            producer_epoch=txn_manager.producer_epoch,
            topics=list(offset_data.items()),
        )
        return req

    def handle_response(self, resp):
        txn_manager = self._sender._txn_manager
        group_id = self._group_id

        for topic, partitions in resp.errors:
            for partition, error_code in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)

                if error_type is Errors.NoError:
                    offset = self._offsets[tp].offset
                    log.debug(
                        "Offset %s for partition %s committed to group %s",
                        offset,
                        tp,
                        group_id,
                    )
                    txn_manager.offset_committed(tp, offset, group_id)
                elif (
                    error_type is CoordinatorNotAvailableError
                    or error_type is NotCoordinatorError
                    # Copied from Java. Not sure why it's only in this case
                    or error_type is RequestTimedOutError
                ):
                    self._sender._coordinator_dead(CoordinationType.GROUP)
                    return self._default_backoff
                elif (
                    error_type is CoordinatorLoadInProgressError
                    or error_type is UnknownTopicOrPartitionError
                ):
                    # We will just retry after backoff
                    return self._default_backoff
                elif error_type is InvalidProducerEpoch:
                    raise ProducerFenced()
                elif error_type is TransactionalIdAuthorizationFailed:
                    raise error_type(txn_manager.transactional_id)
                elif error_type is GroupAuthorizationFailedError:
                    exc = error_type(self._group_id)
                    txn_manager.error_transaction(exc)
                    return None
                else:
                    log.error(
                        "Could not commit offset for partition %s due to "
                        "unexpected error: %s",
                        partition,
                        error_type,
                    )
                    raise error_type()

        return None

    def handle_error(self):
        self._sender._coordinator_dead(CoordinationType.GROUP)
        return self._default_backoff


class EndTxnHandler(BaseHandler):
    group = ConnectionGroup.COORDINATION

    def __init__(self, sender, commit_result):
        super().__init__(sender)
        self._commit_result = commit_result

    def create_request(self):
        txn_manager = self._sender._txn_manager
        req = EndTxnRequest(
            transactional_id=txn_manager.transactional_id,
            producer_id=txn_manager.producer_id,
            producer_epoch=txn_manager.producer_epoch,
            transaction_result=self._commit_result,
        )
        return req

    def handle_response(self, resp):
        txn_manager = self._sender._txn_manager
        error_type = Errors.for_code(resp.error_code)

        if error_type is Errors.NoError:
            txn_manager.complete_transaction()
            return None
        elif (
            error_type is CoordinatorNotAvailableError
            or error_type is NotCoordinatorError
        ):
            self._sender._coordinator_dead(CoordinationType.TRANSACTION)
        elif (
            error_type is CoordinatorLoadInProgressError
            or error_type is ConcurrentTransactions
        ):
            # We will just retry after backoff
            pass
        elif error_type is InvalidProducerEpoch:
            raise ProducerFenced()
        elif error_type is InvalidTxnState:
            raise error_type()
        else:
            log.error(
                "Could not end transaction due to unexpected error: %s", error_type
            )
            raise error_type()

        return self._default_backoff

    def handle_error(self):
        self._sender._coordinator_dead(CoordinationType.TRANSACTION)
        return self._default_backoff


class SendProduceReqHandler(BaseHandler):
    def __init__(self, sender, batches):
        super().__init__(sender)
        self._batches = batches
        self._client = sender.client
        self._to_reenqueue = []

    def create_request(self):
        topics = collections.defaultdict(list)
        for tp, batch in self._batches.items():
            topics[tp.topic].append(
                (tp.partition, batch.get_data_buffer()),
            )

        kwargs = {}
        if self._sender._txn_manager is not None:
            kwargs["transactional_id"] = self._sender._txn_manager.transactional_id
        else:
            kwargs["transactional_id"] = None

        request = ProduceRequest(
            required_acks=self._sender._acks,
            timeout=self._sender._request_timeout_ms,
            topics=list(topics.items()),
            **kwargs,
        )
        return request

    async def do(self, node_id):
        request = self.create_request()
        try:
            response = await self._client.send(node_id, request)
        except KafkaError as err:
            log.warning("Got error produce response: %s", err)
            if getattr(err, "invalid_metadata", False):
                self._client.force_metadata_update()

            for batch in self._batches.values():
                if not self._can_retry(err, batch):
                    batch.failure(exception=err)
                else:
                    self._to_reenqueue.append(batch)
        else:
            # noacks, just mark batches as "done"
            if request.required_acks == 0:
                for batch in self._batches.values():
                    batch.done_noack()
            else:
                self.handle_response(response)

        if self._to_reenqueue:
            # Wait backoff before reequeue
            await asyncio.sleep(self._default_backoff)

            for batch in self._to_reenqueue:
                self._sender._message_accumulator.reenqueue(batch)
            # If some error started metadata refresh we have to wait before
            # trying again
            await self._client._maybe_wait_metadata()

    def handle_response(self, response):
        for topic, partitions in response.topics:
            for partition_info in partitions:
                global_error = None
                log_start_offset = None
                if response.API_VERSION < 2:
                    partition, error_code, offset = partition_info
                    # Mimic CREATE_TIME to take user provided timestamp
                    timestamp = -1
                elif 2 <= response.API_VERSION <= 4:
                    partition, error_code, offset, timestamp = partition_info
                elif 5 <= response.API_VERSION <= 7:
                    (
                        partition,
                        error_code,
                        offset,
                        timestamp,
                        log_start_offset,
                    ) = partition_info
                else:
                    # the ignored parameter is record_error of type
                    # list[(batch_index: int, error_message: str)]
                    (
                        partition,
                        error_code,
                        offset,
                        timestamp,
                        log_start_offset,
                        _,
                        global_error,
                    ) = partition_info
                tp = TopicPartition(topic, partition)
                error = Errors.for_code(error_code)
                batch = self._batches.get(tp)
                if batch is None:
                    continue

                if error is Errors.NoError:
                    batch.done(offset, timestamp, log_start_offset)
                elif error is DuplicateSequenceNumber:
                    # If we have received a duplicate sequence error,
                    # it means that the sequence number has advanced
                    # beyond the sequence of the current batch, and we
                    # haven't retained batch metadata on the broker to
                    # return the correct offset and timestamp.
                    #
                    # The only thing we can do is to return success to
                    # the user and not return a valid offset and
                    # timestamp.
                    batch.done(offset, timestamp, log_start_offset)
                elif not self._can_retry(error(), batch):
                    if error is InvalidProducerEpoch:
                        exc = ProducerFenced()
                    elif error is TopicAuthorizationFailedError:
                        exc = error(topic)
                    else:
                        exc = error()
                    batch.failure(exception=exc)
                else:
                    log.warning(
                        "Got error produce response on topic-partition"
                        " %s, retrying. Error: %s",
                        tp,
                        global_error or error,
                    )
                    # Ok, we can retry this batch
                    if getattr(error, "invalid_metadata", False):
                        self._client.force_metadata_update()
                    self._to_reenqueue.append(batch)

    def handle_error(self):
        return self._default_backoff

    def _can_retry(self, error, batch):
        # If indempotence is enabled we never expire batches, but retry until
        # we succeed. We can be sure, that no duplicates will be introduced
        # as long as we set proper sequence, pid and epoch.
        if self._sender._txn_manager is None and batch.expired():
            return False
        return error.retriable

import asyncio
import collections
import copy
import logging
import time

import aiokafka.errors as Errors
from aiokafka.client import ConnectionGroup, CoordinationType
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.coordinator.protocol import ConsumerProtocol
from aiokafka.protocol.api import Response
from aiokafka.protocol.commit import OffsetCommitRequest, OffsetFetchRequest
from aiokafka.protocol.group import (
    HeartbeatRequest,
    JoinGroupRequest,
    LeaveGroupRequest,
    SyncGroupRequest,
)
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from aiokafka.util import create_future, create_task

log = logging.getLogger(__name__)

UNKNOWN_OFFSET = -1


class BaseCoordinator:
    def __init__(
        self,
        client,
        subscription,
        *,
        exclude_internal_topics=True,
    ):
        self._client = client
        self._exclude_internal_topics = exclude_internal_topics
        self._subscription = subscription

        self._metadata_snapshot = {}  # Is updated by metadata listener
        self._cluster = client.cluster

        # update initial subscription state using currently known metadata
        self._handle_metadata_update(self._cluster)
        self._cluster.add_listener(self._handle_metadata_update)

    def _handle_metadata_update(self, cluster):
        subscription = self._subscription
        if subscription.subscribed_pattern:
            topics = [
                topic
                for topic in cluster.topics(self._exclude_internal_topics)
                if subscription.subscribed_pattern.match(topic)
            ]

            if (
                subscription.subscription is None
                or set(topics) != subscription.subscription.topics
            ):
                subscription.subscribe_from_pattern(topics)

        if (
            subscription.partitions_auto_assigned()
            and self._group_subscription is not None
        ):
            metadata_snapshot = self._get_metadata_snapshot()
            if self._metadata_snapshot != metadata_snapshot:
                log.info(
                    "Metadata for topic has changed from %s to %s. ",
                    self._metadata_snapshot,
                    metadata_snapshot,
                )
                self._metadata_snapshot = metadata_snapshot
                self._on_metadata_change()

    def _get_metadata_snapshot(self):
        partitions_per_topic = {}
        for topic in self._group_subscription:
            partitions = self._cluster.partitions_for_topic(topic) or []
            # Partitions are always from 0 to N, so no reason to check each
            # partition separately, only length is enough
            partitions_per_topic[topic] = len(partitions)
        return partitions_per_topic


class NoGroupCoordinator(BaseCoordinator):
    """
    When `group_id` consumer option is not used we don't have the functionality
    provided by Coordinator node in Kafka cluster, like committing offsets (
    Kafka based offset storage) or automatic partition assignment between
    consumers. But `GroupCoordinator` class has some other responsibilities,
    that this class takes care of to avoid code duplication, like:

        * Static topic partition assignment when we subscribed to topic.
          Partition changes will be noticed by metadata update and assigned.
        * Pattern topic subscription. New topics will be noticed by metadata
          update and added to subscription.
    """

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        # Reset all committed points, as the GroupCoordinator would
        self._reset_committed_task = create_task(self._reset_committed_routine())

    def _on_metadata_change(self):
        self.assign_all_partitions()

    def assign_all_partitions(self, check_unknown=False):
        """Assign all partitions from subscribed topics to this consumer.
        If `check_unknown` we will raise UnknownTopicOrPartitionError if
        subscribed topic is not found in metadata response.
        """
        partitions = []
        for topic in self._subscription.subscription.topics:
            p_ids = self._cluster.partitions_for_topic(topic)
            if not p_ids:
                if check_unknown:
                    raise Errors.UnknownTopicOrPartitionError()
                else:
                    # We probably just changed subscription during metadata
                    # update. No problem, lets wait for the next metadata
                    # update
                    continue
            partitions += [TopicPartition(topic, p_id) for p_id in p_ids]

        # If assignment did not change no need to reset it
        assignment = self._subscription.subscription.assignment
        if assignment is None or set(partitions) != assignment.tps:
            self._subscription.assign_from_subscribed(partitions)

    async def _reset_committed_routine(self):
        """Group coordinator will reset committed points to UNKNOWN_OFFSET
        if no commit is found for group. In the NoGroup mode we need to force
        it after each assignment
        """
        event_waiter = None
        try:
            while True:
                if self._subscription.subscription is None:
                    await self._subscription.wait_for_subscription()
                    continue

                assignment = self._subscription.subscription.assignment
                if assignment is None:
                    await self._subscription.wait_for_assignment()
                    continue

                commit_refresh_needed = assignment.commit_refresh_needed
                commit_refresh_needed.clear()

                for tp in assignment.requesting_committed():
                    tp_state = assignment.state_value(tp)
                    tp_state.update_committed(OffsetAndMetadata(UNKNOWN_OFFSET, ""))

                event_waiter = create_task(commit_refresh_needed.wait())

                await asyncio.wait(
                    [assignment.unassign_future, event_waiter],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not event_waiter.done():
                    event_waiter.cancel()
                    event_waiter = None

        except asyncio.CancelledError:
            pass

        # Just to make sure we properly close started tasks we cancel
        # event.wait() task
        if event_waiter is not None and not event_waiter.done():
            event_waiter.cancel()
            event_waiter = None

    @property
    def _group_subscription(self):
        return self._subscription.subscription.topics

    async def close(self):
        self._reset_committed_task.cancel()
        await self._reset_committed_task
        self._reset_committed_task = None

    def check_errors(self):
        if self._reset_committed_task.done():  # pragma: no cover
            self._reset_committed_task.result()


class GroupCoordinator(BaseCoordinator):
    """
    GroupCoordinator implements group management for single group member
    by interacting with a designated Kafka broker (the coordinator). Group
    semantics are provided by extending this class.

    From a high level, Kafka's group management protocol consists of the
    following sequence of actions:

    1. Group Registration: Group members register with the coordinator
       providing their own metadata
       (such as the set of topics they are interested in).

    2. Group/Leader Selection: The coordinator (one of Kafka nodes) select
       the members of the group and chooses one member (one of client's)
       as the leader.

    3. State Assignment: The leader receives metadata for all members and
       assigns partitions to them.

    4. Group Stabilization: Each member receives the state assigned by the
       leader and begins processing.
       Between each phase coordinator awaits all clients to respond. If some
       do not respond in time - it will revoke their membership

    NOTE: Try to maintain same log messages and behaviour as Java and
          kafka-python clients:

        https://github.com/apache/kafka/blob/0.10.1.1/clients/src/main/java/\
          org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
        https://github.com/apache/kafka/blob/0.10.1.1/clients/src/main/java/\
          org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java
    """

    def __init__(
        self,
        client,
        subscription,
        *,
        group_id="aiokafka-default-group",
        group_instance_id=None,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
        retry_backoff_ms=100,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        assignors=(RoundRobinPartitionAssignor,),
        exclude_internal_topics=True,
        max_poll_interval_ms=300000,
        rebalance_timeout_ms=30000,
    ):
        """Initialize the coordination manager.

        Parameters (see AIOKafkaConsumer)
        """
        # Leader node will keep track of the whole group's metadata.
        self._group_subscription = None

        super().__init__(
            client,
            subscription,
            exclude_internal_topics=exclude_internal_topics,
        )

        self._session_timeout_ms = session_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._max_poll_interval = max_poll_interval_ms / 1000
        self._rebalance_timeout_ms = rebalance_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._assignors = assignors
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms

        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
        self.group_id = group_id
        self._group_instance_id = group_instance_id
        self.coordinator_id = None

        # Coordination flags and futures
        self._performed_join_prepare = False
        self._rejoin_needed_fut = create_future()
        self._coordinator_dead_fut = create_future()

        self._coordination_task = create_task(self._coordination_routine())

        # Will be started/stopped by coordination task
        self._heartbeat_task = None
        self._commit_refresh_task = None

        # Those are mostly unrecoverable exceptions, but user may perform an
        # action to handle those (for example add permission for this group).
        # Thus we set exception and pause coordination until user consumes it.
        self._pending_exception = None
        self._error_consumed_fut = None

        self._coordinator_lookup_lock = asyncio.Lock()
        # Will synchronize edits to TopicPartitionState.committed, as it may be
        # changed from user code by calling ``commit()``.
        self._commit_lock = asyncio.Lock()

        self._next_autocommit_deadline = (
            time.monotonic() + auto_commit_interval_ms / 1000
        )

        # Will be set on close
        self._closing = create_future()

    def _on_metadata_change(self):
        self.request_rejoin()

    async def _send_req(self, request):
        """Send request to coordinator node. In case the coordinator is not
        ready a respective error will be raised.
        """
        node_id = self.coordinator_id
        if node_id is None:
            raise Errors.GroupCoordinatorNotAvailableError()
        try:
            resp = await self._client.send(
                node_id, request, group=ConnectionGroup.COORDINATION
            )
        except Errors.KafkaError as err:
            log.error(
                "Error sending %s to node %s [%s] -- marking coordinator dead",
                request.__class__.__name__,
                node_id,
                err,
            )
            self.coordinator_dead()
            raise
        return resp

    def check_errors(self):
        """Check if coordinator is well and no authorization or unrecoverable
        errors occurred
        """
        if self._coordination_task.done():
            self._coordination_task.result()
        if self._error_consumed_fut is not None:
            self._error_consumed_fut.set_result(None)
            self._error_consumed_fut = None
        if self._pending_exception is not None:
            exc = self._pending_exception
            self._pending_exception = None
            raise exc

    def _push_error_to_user(self, exc):
        """Most critical errors are not something we can continue execution
        without user action. Well right now we just drop the Consumer, but
        java client would certainly be ok if we just poll another time, maybe
        it will need to rejoin, but not fail with GroupAuthorizationFailedError
        till the end of days...
        XXX: Research if we can't have the same error several times. For
             example if user gets GroupAuthorizationFailedError and adds
             permission for the group, would Consumer work right away or would
             still raise exception a few times?
        """
        exc = copy.copy(exc)
        self._subscription.abort_waiters(exc)
        self._pending_exception = exc
        self._error_consumed_fut = create_future()
        return asyncio.wait(
            [self._error_consumed_fut, self._closing],
            return_when=asyncio.FIRST_COMPLETED,
        )

    async def close(self):
        """Close the coordinator, leave the current group
        and reset local generation/memberId."""
        if self._closing.done():
            return

        self._closing.set_result(None)
        # We must let the coordination task properly finish all pending work
        if not self._coordination_task.done():
            await self._coordination_task
        await self._stop_heartbeat_task()
        await self._stop_commit_offsets_refresh_task()

        await self._maybe_leave_group()

    def maybe_leave_group(self):
        task = create_task(self._maybe_leave_group())
        return task

    async def _maybe_leave_group(self):
        if self.generation > 0 and self._group_instance_id is None:
            # this is a minimal effort attempt to leave the group. we do not
            # attempt any resending if the request fails or times out.
            # Note: do not send this leave request if we are running in static
            # partition assignment mode (when group_instance_id has been set).
            request = LeaveGroupRequest(self.group_id, self.member_id)
            try:
                await self._send_req(request)
            except Errors.KafkaError as err:
                log.error("LeaveGroup request failed: %s", err)
            else:
                log.info("LeaveGroup request succeeded")
        self.reset_generation()

    def _lookup_assignor(self, name):
        for assignor in self._assignors:
            if assignor.name == name:
                return assignor
        return None

    async def _on_join_prepare(self, previous_assignment):
        self._subscription.begin_reassignment()
        self._group_subscription = None

        # commit offsets prior to rebalance if auto-commit enabled
        if previous_assignment is not None:
            try:
                await self._maybe_do_last_autocommit(previous_assignment)
            except Errors.KafkaError as err:
                # We would retry any retriable commit already
                log.error("OffsetCommit failed before join, ignoring: %s", err)
            revoked = previous_assignment.tps
        else:
            revoked = set()

        # execute the user's callback before rebalance
        log.info(
            "Revoking previously assigned partitions %s for group %s",
            revoked,
            self.group_id,
        )
        if self._subscription.listener:
            try:
                res = self._subscription.listener.on_partitions_revoked(revoked)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                log.exception(
                    "User provided subscription listener %s"
                    " for group %s failed on_partitions_revoked",
                    self._subscription.listener,
                    self.group_id,
                )

    async def _perform_assignment(self, response: Response):
        assignment_strategy = response.group_protocol
        members = response.members
        assignor = self._lookup_assignor(assignment_strategy)
        assert assignor, f"Invalid assignment protocol: {assignment_strategy}"
        member_metadata = {}
        all_subscribed_topics = set()
        for member in members:
            if response.API_VERSION == 5:
                member_id, _group_instance_id, metadata_bytes = member
            else:
                member_id, metadata_bytes = member
            metadata = ConsumerProtocol.METADATA.decode(metadata_bytes)
            member_metadata[member_id] = metadata
            all_subscribed_topics.update(metadata.subscription)

        # the leader will begin watching for changes to any of the topics
        # the group is interested in, which ensures that all metadata changes
        # will eventually be seen
        self._group_subscription = all_subscribed_topics
        if not self._subscription.subscribed_pattern:
            self._client.set_topics(self._group_subscription)
        # If somewhere we forced a metadata update (like in some `set_topics`
        # call) we should wait for it before performing assignment
        await self._client._maybe_wait_metadata()

        log.debug(
            "Performing assignment for group %s using strategy %s"
            " with subscriptions %s",
            self.group_id,
            assignor.name,
            member_metadata,
        )

        assignments = assignor.assign(self._cluster, member_metadata)
        log.debug("Finished assignment for group %s: %s", self.group_id, assignments)

        # `set_topics()` will not trigger a metadata update if we only
        # removed some topics (client has all needed metadata already),
        # so we force a snapshot update.
        self._metadata_snapshot = self._get_metadata_snapshot()

        return assignments

    async def _on_join_complete(
        self, generation, member_id, protocol, member_assignment_bytes
    ):
        assignor = self._lookup_assignor(protocol)
        assert assignor, f"invalid assignment protocol: {protocol}"

        assignment = ConsumerProtocol.ASSIGNMENT.decode(member_assignment_bytes)

        # update partition assignment
        self._subscription.assign_from_subscribed(assignment.partitions())
        # The await bellow can change subscription, remember the ongoing one.
        subscription = self._subscription.subscription

        # give the assignor a chance to update internal state
        # based on the received assignment
        assignor.on_assignment(assignment)

        # We need to start this task before callback to avoid deadlocks.
        # Callback can rely on something like ``Consumer.position()`` that
        # requires committed point to be refreshed.
        await self._stop_commit_offsets_refresh_task()
        self.start_commit_offsets_refresh_task(subscription.assignment)

        assigned = set(self._subscription.assigned_partitions())
        log.info(
            "Setting newly assigned partitions %s for group %s", assigned, self.group_id
        )

        # execute the user's callback after rebalance
        if self._subscription.listener:
            try:
                res = self._subscription.listener.on_partitions_assigned(assigned)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                log.exception(
                    "User provided listener %s for group %s"
                    " failed on partition assignment: %s",
                    self._subscription.listener,
                    self.group_id,
                    assigned,
                )

    def coordinator_dead(self):
        """Mark the current coordinator as dead.
        NOTE: this will not force a group rejoin. If new coordinator is able to
        recognize this member we will just continue with current generation.
        """
        if self.coordinator_id is not None:
            log.warning(
                "Marking the coordinator dead (node %s)for group %s.",
                self.coordinator_id,
                self.group_id,
            )
            self.coordinator_id = None
            self._coordinator_dead_fut.set_result(None)

    def reset_generation(self):
        """Coordinator did not recognize either generation or member_id. Will
        need to re-join the group.
        """
        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
        self.request_rejoin()

    def request_rejoin(self):
        if not self._rejoin_needed_fut.done():
            self._rejoin_needed_fut.set_result(None)

    def need_rejoin(self, subscription):
        """Check whether the group should be rejoined

        Returns:
            bool: True if consumer should rejoin group, False otherwise
        """
        return subscription.assignment is None or self._rejoin_needed_fut.done()

    async def ensure_coordinator_known(self):
        """Block until the coordinator for this group is known."""
        if self.coordinator_id is not None:
            return

        async with self._coordinator_lookup_lock:
            retry_backoff = self._retry_backoff_ms / 1000
            while self.coordinator_id is None and not self._closing.done():
                try:
                    coordinator_id = await self._client.coordinator_lookup(
                        CoordinationType.GROUP, self.group_id
                    )
                except Errors.GroupAuthorizationFailedError as err:
                    new_err = Errors.GroupAuthorizationFailedError(self.group_id)
                    raise new_err from err
                except Errors.KafkaError as err:
                    log.error("Group Coordinator Request failed: %s", err)
                    if err.retriable:
                        await self._client.force_metadata_update()
                        await asyncio.sleep(retry_backoff)
                        continue
                    else:
                        raise

                # Try to connect to confirm that the connection can be
                # established.
                ready = await self._client.ready(
                    coordinator_id, group=ConnectionGroup.COORDINATION
                )
                if not ready:
                    await asyncio.sleep(retry_backoff)
                    continue

                self.coordinator_id = coordinator_id
                self._coordinator_dead_fut = create_future()
                log.info(
                    "Discovered coordinator %s for group %s",
                    self.coordinator_id,
                    self.group_id,
                )

    async def _coordination_routine(self):
        try:
            await self.__coordination_routine()
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception as exc:
            log.exception("Unexpected error in coordinator routine")
            kafka_exc = Errors.KafkaError(
                f"Unexpected error during coordination {exc!r}"
            )
            self._subscription.abort_waiters(kafka_exc)
            raise kafka_exc from exc

    async def __coordination_routine(self):
        """Main background task, that keeps track of changes in group
        coordination. This task will spawn/stop heartbeat task and perform
        autocommit in times it's safe to do so.
        """
        subscription = self._subscription.subscription
        assignment = None

        while not self._closing.done():
            # Check if there was a change to subscription
            if subscription is not None and not subscription.active:
                # The subscription can change few times, so we can not rely on
                # flags or topic lists. For example if user changes
                # subscription from X to Y and back to X we still need to
                # rejoin group.
                self.request_rejoin()
                subscription = self._subscription.subscription
            if subscription is None:
                await asyncio.wait(
                    [self._subscription.wait_for_subscription(), self._closing],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if self._closing.done():
                    break
                subscription = self._subscription.subscription
            assert subscription is not None and subscription.active
            auto_assigned = self._subscription.partitions_auto_assigned()

            # Ensure active group
            try:
                await self.ensure_coordinator_known()
                if auto_assigned and self.need_rejoin(subscription):
                    new_assignment = await self.ensure_active_group(
                        subscription, assignment
                    )
                    if new_assignment is None or not new_assignment.active:
                        continue
                    else:
                        assignment = new_assignment
                else:
                    assignment = subscription.assignment

                assert assignment is not None and assignment.active

                # We will only try to commit offsets once here. In error case
                # the returned wait_timeout will be ``retry_backoff``. In
                # success case time to next autocommit deadline. If autocommit
                # is disabled timeout will be ``None``, ie. no timeout.
                wait_timeout = await self._maybe_do_autocommit(assignment)

            except Errors.KafkaError as exc:
                await self._push_error_to_user(exc)
                continue

            futures = [
                self._closing,  # Will exit fast if close() called
                self._coordinator_dead_fut,
                subscription.unsubscribe_future,
            ]
            # In case of manual assignment this future will be always set and
            # we don't want a heavy loop here.
            # NOTE: metadata changes are for partition count and pattern
            # subscription, which is irrelevant in case of user assignment.
            if auto_assigned:
                futures.append(self._rejoin_needed_fut)

            # We should always watch for other task raising critical or
            # unexpected errors, so we attach those as futures too. We will
            # check them right after wait.
            if self._heartbeat_task:
                futures.append(self._heartbeat_task)
            if self._commit_refresh_task:
                futures.append(self._commit_refresh_task)

            await asyncio.wait(
                futures, timeout=wait_timeout, return_when=asyncio.FIRST_COMPLETED
            )

            # Handle exceptions in other background tasks
            for task in [self._heartbeat_task, self._commit_refresh_task]:
                if task and task.done():
                    exc = task.exception()
                    if exc:
                        await self._push_error_to_user(exc)

        # Closing finallization
        if assignment is not None:
            try:
                await self._maybe_do_last_autocommit(assignment)
            except Errors.KafkaError as err:
                # We did all we could, all we can is show this to user
                log.error("Failed to commit on finallization: %s", err)

    async def ensure_active_group(self, subscription, prev_assignment):
        # due to a race condition between the initial metadata
        # fetch and the initial rebalance, we need to ensure that
        # the metadata is fresh before joining initially. This
        # ensures that we have matched the pattern against the
        # cluster's topics at least once before joining.
        # Also the rebalance can be issued by another node, that
        # discovered a new topic, which is still unknown to this
        # one.
        if self._subscription.subscribed_pattern:
            await self._client.force_metadata_update()
            if not subscription.active:
                return None

        if not self._performed_join_prepare:
            # NOTE: We pass the previously used assignment here.
            await self._on_join_prepare(prev_assignment)
            self._performed_join_prepare = True

        # NOTE: we did not stop heartbeat task before to keep the
        # member alive during the callback, as it can commit offsets.
        # See the ``RebalanceInProgressError`` case in heartbeat
        # handling.
        await self._stop_heartbeat_task()

        # We will not attempt rejoin if there is no activity on consumer
        idle_time = self._subscription.fetcher_idle_time
        if prev_assignment is not None and idle_time >= self._max_poll_interval:
            await asyncio.sleep(self._retry_backoff_ms / 1000)
            return None

        # We will only try to perform the rejoin once. If it fails,
        # we will spin this loop another time, checking for coordinator
        # and subscription changes.
        # NOTE: We do re-join in sync. The group rebalance will fail on
        # subscription change and coordinator failure by itself and
        # this way we don't need to worry about racing or cancellation
        # issues that could occur if re-join were to be a task.
        success = await self._do_rejoin_group(subscription)
        if success:
            self._performed_join_prepare = False
            self._start_heartbeat_task()
            return subscription.assignment
        return None

    def _start_heartbeat_task(self):
        if self._heartbeat_task is None:
            self._heartbeat_task = create_task(self._heartbeat_routine())

    async def _stop_heartbeat_task(self):
        if self._heartbeat_task is not None:
            if not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                await self._heartbeat_task
            self._heartbeat_task = None

    async def _heartbeat_routine(self):
        last_ok_heartbeat = time.monotonic()
        hb_interval = self._heartbeat_interval_ms / 1000
        session_timeout = self._session_timeout_ms / 1000
        retry_backoff = self._retry_backoff_ms / 1000
        sleep_time = hb_interval

        # There is no point to heartbeat after Broker stopped recognizing
        # this consumer, so we stop after resetting generation.
        while self.member_id != JoinGroupRequest.UNKNOWN_MEMBER_ID:
            try:
                await asyncio.sleep(sleep_time)
                await self.ensure_coordinator_known()

                t0 = time.monotonic()
                success = await self._do_heartbeat()
            except asyncio.CancelledError:
                break

            # NOTE: We let all other errors propagate up to coordination
            # routine

            if success:
                last_ok_heartbeat = time.monotonic()
                sleep_time = max((0, hb_interval - last_ok_heartbeat + t0))
            else:
                sleep_time = retry_backoff

            session_time = time.monotonic() - last_ok_heartbeat
            if session_time > session_timeout:
                # the session timeout has expired without seeing a successful
                # heartbeat, so we should probably make sure the coordinator
                # is still healthy.
                log.error("Heartbeat session expired - marking coordinator dead")
                self.coordinator_dead()

            # If consumer is idle (no records consumed) for too long we need
            # to leave the group
            idle_time = self._subscription.fetcher_idle_time
            if idle_time < self._max_poll_interval:
                sleep_time = min(sleep_time, self._max_poll_interval - idle_time)
            else:
                await self._maybe_leave_group()

        log.debug("Stopping heartbeat task")

    async def _do_heartbeat(self):
        request = HeartbeatRequest(self.group_id, self.generation, self.member_id)
        log.debug(
            "Heartbeat: %s[%s] %s", self.group_id, self.generation, self.member_id
        )

        # _send_req may fail with error like `RequestTimedOutError`
        # we need to catch it so coordinator_routine won't fail
        try:
            resp = await self._send_req(request)
        except Errors.KafkaError as err:
            log.error("Heartbeat send request failed: %s. Will retry.", err)
            return False
        error_type = Errors.for_code(resp.error_code)
        if error_type is Errors.NoError:
            log.debug(
                "Received successful heartbeat response for group %s", self.group_id
            )
            return True
        if error_type in (
            Errors.GroupCoordinatorNotAvailableError,
            Errors.NotCoordinatorForGroupError,
        ):
            log.warning(
                "Heartbeat failed for group %s: coordinator (node %s)"
                " is either not started or not valid",
                self.group_id,
                self.coordinator_id,
            )
            self.coordinator_dead()
        elif error_type is Errors.RebalanceInProgressError:
            log.warning(
                "Heartbeat failed for group %s because it is rebalancing", self.group_id
            )
            self.request_rejoin()
            # it is valid to continue heartbeating while the group is
            # rebalancing. This ensures that the coordinator keeps the
            # member in the group for as long as the duration of the
            # rebalance timeout. If we stop sending heartbeats,
            # however, then the session timeout may expire before we
            # can rejoin.
            return True
        elif error_type is Errors.IllegalGenerationError:
            log.warning(
                "Heartbeat failed for group %s: generation id is not  current.",
                self.group_id,
            )
            self.reset_generation()
        elif error_type is Errors.UnknownMemberIdError:
            log.warning(
                "Heartbeat failed: local member_id was not recognized;"
                " resetting and re-joining group"
            )
            self.reset_generation()
        elif error_type is Errors.GroupAuthorizationFailedError:
            raise error_type(self.group_id)
        else:
            err = Errors.KafkaError(
                f"Unexpected exception in heartbeat task: {error_type()!r}"
            )
            log.error("Heartbeat failed: %r", err)
            raise err
        return False

    def start_commit_offsets_refresh_task(self, assignment):
        if self._commit_refresh_task is not None:
            self._commit_refresh_task.cancel()
        self._commit_refresh_task = create_task(
            self._commit_refresh_routine(assignment)
        )

    async def _stop_commit_offsets_refresh_task(self):
        # The previous task should end after assignment changed
        if self._commit_refresh_task is not None:
            if not self._commit_refresh_task.done():
                self._commit_refresh_task.cancel()
                await self._commit_refresh_task
            self._commit_refresh_task = None

    async def _commit_refresh_routine(self, assignment):
        """Task that will do a commit cache refresh if someone is waiting for
        it.
        """
        retry_backoff_ms = self._retry_backoff_ms / 1000
        commit_refresh_needed = assignment.commit_refresh_needed
        event_waiter = None
        try:
            while assignment.active:
                commit_refresh_needed.clear()
                success = await self._maybe_refresh_commit_offsets(assignment)

                wait_futures = [assignment.unassign_future]
                if not success:
                    timeout = retry_backoff_ms
                else:
                    timeout = None
                    event_waiter = create_task(commit_refresh_needed.wait())
                    wait_futures.append(event_waiter)

                await asyncio.wait(
                    wait_futures, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                )
        except asyncio.CancelledError:
            pass
        except Exception:
            # Reset event to continue in case user fixes the problem
            commit_refresh_needed.set()
            raise

        # Just to make sure we properly close started tasks we cancel
        # event.wait() task
        if event_waiter is not None and not event_waiter.done():
            event_waiter.cancel()
            event_waiter = None

    async def _do_rejoin_group(self, subscription):
        rebalance = CoordinatorGroupRebalance(
            self,
            self.group_id,
            self.coordinator_id,
            subscription,
            self._assignors,
            self._session_timeout_ms,
            self._retry_backoff_ms,
        )
        assignment = await rebalance.perform_group_join()

        if not subscription.active:
            log.debug(
                "Subscription changed during rebalance from %s to %s. Rejoining group.",
                subscription.topics,
                self._subscription.topics,
            )
            return False
        if assignment is None:
            # wait backoff and try again
            await asyncio.sleep(self._retry_backoff_ms / 1000)
            return False

        protocol, member_assignment_bytes = assignment
        await self._on_join_complete(
            self.generation, self.member_id, protocol, member_assignment_bytes
        )
        return True

    async def _maybe_do_autocommit(self, assignment):
        if not self._enable_auto_commit:
            return None
        now = time.monotonic()
        interval = self._auto_commit_interval_ms / 1000
        backoff = self._retry_backoff_ms / 1000
        if now > self._next_autocommit_deadline:
            try:
                async with self._commit_lock:
                    await self._do_commit_offsets(
                        assignment, assignment.all_consumed_offsets()
                    )
            except Errors.KafkaError as error:
                log.warning("Auto offset commit failed: %s", error)
                if self._is_commit_retriable(error):
                    # Retry after backoff.
                    self._next_autocommit_deadline = time.monotonic() + backoff
                    return backoff
                else:
                    raise
            # If we had an unrecoverable error we expect the user to handle it
            # from another source (say Fetcher, like authorization errors).
            self._next_autocommit_deadline = now + interval

        return max(0, self._next_autocommit_deadline - time.monotonic())

    def _is_commit_retriable(self, error):
        # Java client raises CommitFailedError which is retriable and thus
        # masks those 3. We raise error that we got explicitly, so treat them
        # as retriable.
        return error.retriable or isinstance(
            error,
            Errors.UnknownMemberIdError
            | Errors.IllegalGenerationError
            | Errors.RebalanceInProgressError,
        )

    async def _maybe_do_last_autocommit(self, assignment):
        if not self._enable_auto_commit:
            return
        await self.commit_offsets(assignment, assignment.all_consumed_offsets())

    async def commit_offsets(self, assignment, offsets):
        """Commit specific offsets

        Arguments:
            offsets (dict {TopicPartition: OffsetAndMetadata}): what to commit

        Raises KafkaError on failure
        """
        while True:
            await self.ensure_coordinator_known()
            try:
                async with self._commit_lock:
                    await asyncio.shield(self._do_commit_offsets(assignment, offsets))
            except (
                Errors.UnknownMemberIdError,
                Errors.IllegalGenerationError,
                Errors.RebalanceInProgressError,
            ) as exc:
                raise Errors.CommitFailedError(
                    "Commit cannot be completed since the group has already "
                    "rebalanced and may have assigned the partitions "
                    "to another member"
                ) from exc
            except Errors.KafkaError as err:
                if not err.retriable:
                    raise
                else:
                    # wait backoff and try again
                    await asyncio.sleep(self._retry_backoff_ms / 1000)
            else:
                break

    async def _do_commit_offsets(self, assignment, offsets):
        # Fast return if nothing to commit
        if not offsets:
            return

        # create the offset commit request
        offset_data = collections.defaultdict(list)
        for tp, offset in offsets.items():
            offset_data[tp.topic].append(
                (tp.partition, offset.offset, offset.metadata),
            )

        request = OffsetCommitRequest(
            self.group_id,
            self.generation,
            self.member_id,
            OffsetCommitRequest.DEFAULT_RETENTION_TIME,
            list(offset_data.items()),
        )

        log.debug(
            "Sending offset-commit request with %s for group %s to %s",
            offsets,
            self.group_id,
            self.coordinator_id,
        )

        response = await self._send_req(request)

        errored = collections.OrderedDict()
        unauthorized_topics = set()
        for topic, partitions in response.topics:
            for partition, error_code in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                offset = offsets[tp]
                if error_type is Errors.NoError:
                    log.debug("Committed offset %s for partition %s", offset, tp)
                elif error_type is Errors.GroupAuthorizationFailedError:
                    log.error(
                        "OffsetCommit failed for group %s - %s",
                        self.group_id,
                        error_type.__name__,
                    )
                    errored[tp] = error_type(self.group_id)
                elif error_type is Errors.TopicAuthorizationFailedError:
                    unauthorized_topics.add(topic)
                elif error_type in (
                    Errors.OffsetMetadataTooLargeError,
                    Errors.InvalidCommitOffsetSizeError,
                ):
                    # raise the error to the user
                    log.info(
                        "OffsetCommit failed for group %s on partition %s"
                        " due to %s, will retry",
                        self.group_id,
                        tp,
                        error_type.__name__,
                    )
                    errored[tp] = error_type()
                elif error_type is Errors.GroupLoadInProgressError:
                    # just retry
                    log.info(
                        "OffsetCommit failed for group %s because group is"
                        " initializing (%s), will retry",
                        self.group_id,
                        error_type.__name__,
                    )
                    errored[tp] = error_type()
                elif error_type in (
                    Errors.GroupCoordinatorNotAvailableError,
                    Errors.NotCoordinatorForGroupError,
                    Errors.RequestTimedOutError,
                ):
                    log.info(
                        "OffsetCommit failed for group %s due to a"
                        " coordinator error (%s), will find new coordinator"
                        " and retry",
                        self.group_id,
                        error_type.__name__,
                    )
                    self.coordinator_dead()
                    errored[tp] = error_type()
                elif error_type in (
                    Errors.UnknownMemberIdError,
                    Errors.IllegalGenerationError,
                    Errors.RebalanceInProgressError,
                ):
                    # need to re-join group
                    error = error_type(self.group_id)
                    log.error(
                        "OffsetCommit failed for group %s due to group"
                        " error (%s), will rejoin",
                        self.group_id,
                        error,
                    )
                    if error_type is Errors.RebalanceInProgressError:
                        self.request_rejoin()
                    else:
                        self.reset_generation()
                    errored[tp] = error

                else:
                    log.error(
                        "OffsetCommit failed for group %s on partition %s"
                        " with offset %s: %s",
                        self.group_id,
                        tp,
                        offset,
                        error_type.__name__,
                    )
                    errored[tp] = error_type()

        if errored:
            first_error, *_ = errored.values()
            raise first_error
        if unauthorized_topics:
            log.error(
                "OffsetCommit failed for unauthorized topics %s", unauthorized_topics
            )
            raise Errors.TopicAuthorizationFailedError(unauthorized_topics)

    async def _maybe_refresh_commit_offsets(self, assignment):
        need_update = assignment.requesting_committed()
        if need_update:
            try:
                offsets = await self._do_fetch_commit_offsets(need_update)
            except Errors.KafkaError as err:
                if not err.retriable:
                    raise
                else:
                    log.debug("Failed to fetch committed offsets: %r", err)
                return False
            for tp in need_update:
                tp_state = assignment.state_value(tp)
                if tp in offsets:
                    tp_state.update_committed(offsets[tp])
                else:
                    tp_state.update_committed(OffsetAndMetadata(UNKNOWN_OFFSET, ""))
        return True

    async def fetch_committed_offsets(self, partitions):
        """Fetch the current committed offsets for specified partitions

        Arguments:
            partitions (list of TopicPartition): partitions to fetch

        Returns:
            dict: {TopicPartition: OffsetAndMetadata}
        """
        if not partitions:
            return {}

        while True:
            await self.ensure_coordinator_known()
            try:
                offsets = await self._do_fetch_commit_offsets(partitions)
            except Errors.KafkaError as err:
                if not err.retriable:
                    raise
                else:
                    # wait backoff and try again
                    await asyncio.sleep(self._retry_backoff_ms / 1000)
            else:
                return offsets

    async def _do_fetch_commit_offsets(self, partitions):
        log.debug("Fetching committed offsets for partitions: %s", partitions)
        # construct the request
        partitions_by_topic = collections.defaultdict(list)
        for tp in partitions:
            partitions_by_topic[tp.topic].append(tp.partition)

        request = OffsetFetchRequest(self.group_id, list(partitions_by_topic.items()))
        response = await self._send_req(request)
        offsets = {}
        for topic, topic_partitions in response.topics:
            for partition, offset, metadata, error_code in topic_partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if error_type is not Errors.NoError:
                    error = error_type()
                    log.debug("Error fetching offset for %s: %s", tp, error)
                    if error_type is Errors.GroupLoadInProgressError:
                        # just retry
                        raise error
                    elif error_type is Errors.NotCoordinatorForGroupError:
                        # re-discover the coordinator and retry
                        self.coordinator_dead()
                        raise error
                    elif error_type is Errors.UnknownTopicOrPartitionError:
                        log.warning("OffsetFetchRequest -- unknown topic %s", topic)
                        continue
                    elif error_type is Errors.GroupAuthorizationFailedError:
                        raise error_type(self.group_id)
                    else:
                        log.error(
                            "Unknown error fetching offsets for %s: %s", tp, error
                        )
                        raise Errors.KafkaError(repr(error))
                # record the position with the offset
                # (-1 indicates no committed offset to fetch)
                if offset == UNKNOWN_OFFSET:
                    log.debug("No committed offset for partition %s", tp)
                else:
                    offsets[tp] = OffsetAndMetadata(offset, metadata)
        return offsets


class CoordinatorGroupRebalance:
    """ An adapter, that encapsulates rebalance logic and will have a copy of
        assigned topics, so we can detect assignment changes. This includes
        subscription pattern changes.

        On how to handle cases read in https://cwiki.apache.org/confluence/\
            display/KAFKA/Kafka+Client-side+Assignment+Proposal
    """

    def __init__(
        self,
        coordinator,
        group_id,
        coordinator_id,
        subscription,
        assignors,
        session_timeout_ms,
        retry_backoff_ms,
    ):
        self._coordinator = coordinator
        self.group_id = group_id
        self.coordinator_id = coordinator_id

        self._subscription = subscription
        self._assignors = assignors
        self._session_timeout_ms = session_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._rebalance_timeout_ms = self._coordinator._rebalance_timeout_ms

    async def perform_group_join(self):
        """Join the group and return the assignment for the next generation.

        This function handles both JoinGroup and SyncGroup, delegating to
        _perform_assignment() if elected as leader by the coordinator node.

        Returns encoded-bytes assignment returned from the group leader
        """
        # send a join group request to the coordinator
        log.info("(Re-)joining group %s", self.group_id)

        topics = self._subscription.topics
        metadata_list = []
        for assignor in self._assignors:
            metadata = assignor.metadata(topics)
            if not isinstance(metadata, bytes):
                metadata = metadata.encode()
            group_protocol = (assignor.name, metadata)
            metadata_list.append(group_protocol)
            # for KIP-394 we may have to send a second join request
            try_join = True
            while try_join:
                try_join = False

                request = JoinGroupRequest(
                    self.group_id,
                    self._session_timeout_ms,
                    self._rebalance_timeout_ms,
                    self._coordinator.member_id,
                    self._coordinator._group_instance_id,
                    ConsumerProtocol.PROTOCOL_TYPE,
                    metadata_list,
                )

                # create the request for the coordinator
                log.debug(
                    "Sending JoinGroup (%s) to coordinator %s",
                    request,
                    self.coordinator_id,
                )
                try:
                    response = await self._coordinator._send_req(request)
                except Errors.KafkaError:
                    # Return right away. It's a connection error, so backoff will be
                    # handled by coordinator lookup
                    return None
                if not self._subscription.active:
                    # Subscription changed. Ignore response and restart group join
                    return None

                error_type = Errors.for_code(response.error_code)

                if error_type is Errors.MemberIdRequired:
                    self._coordinator.member_id = response.member_id
                    try_join = True

        if error_type is Errors.NoError:
            log.debug("Join group response %s", response)
            self._coordinator.member_id = response.member_id
            self._coordinator.generation = response.generation_id
            protocol = response.group_protocol
            log.info(
                "Joined group '%s' (generation %s) with member_id %s",
                self.group_id,
                response.generation_id,
                response.member_id,
            )

            if response.leader_id == response.member_id:
                log.info(
                    "Elected group leader -- performing partition assignments using %s",
                    protocol,
                )
                assignment_bytes = await self._on_join_leader(response)
            else:
                assignment_bytes = await self._on_join_follower()

            if assignment_bytes is None:
                return None
            return (protocol, assignment_bytes)
        elif error_type is Errors.GroupLoadInProgressError:
            # Backoff and retry
            log.debug(
                "Attempt to join group %s rejected since coordinator %s"
                " is loading the group.",
                self.group_id,
                self.coordinator_id,
            )
            await asyncio.sleep(self._retry_backoff_ms / 1000)
        elif error_type is Errors.UnknownMemberIdError:
            # reset the member id and retry immediately
            self._coordinator.reset_generation()
            log.debug(
                "Attempt to join group %s failed due to unknown member id",
                self.group_id,
            )
        elif error_type in (
            Errors.GroupCoordinatorNotAvailableError,
            Errors.NotCoordinatorForGroupError,
        ):
            # Coordinator changed we should be able to find it immediately
            err = error_type()
            self._coordinator.coordinator_dead()
            log.debug(
                "Attempt to join group %s failed due to obsolete "
                "coordinator information: %s",
                self.group_id,
                err,
            )
        elif error_type in (
            Errors.InconsistentGroupProtocolError,
            Errors.InvalidSessionTimeoutError,
            Errors.InvalidGroupIdError,
        ):
            err = error_type()
            log.error("Attempt to join group failed due to fatal error: %s", err)
            raise err
        elif error_type is Errors.GroupAuthorizationFailedError:
            raise error_type(self.group_id)
        else:
            err = error_type()
            log.error(
                "Unexpected error in join group '%s' response: %s", self.group_id, err
            )
            raise Errors.KafkaError(repr(err))
        return None

    async def _on_join_follower(self):
        # send follower's sync group with an empty assignment
        request = SyncGroupRequest(
            self.group_id,
            self._coordinator.generation,
            self._coordinator.member_id,
            self._coordinator._group_instance_id,
            [],
        )
        log.debug(
            "Sending follower SyncGroup for group %s to coordinator %s: %s",
            self.group_id,
            self.coordinator_id,
            request,
        )
        return await self._send_sync_group_request(request)

    async def _on_join_leader(self, response):
        """
        Perform leader synchronization and send back the assignment
        for the group via SyncGroupRequest

        Arguments:
            response (JoinResponse): broker response to parse

        Returns:
            Future: resolves to member assignment encoded-bytes
        """
        try:
            group_assignment = await self._coordinator._perform_assignment(response)
        except Exception as e:
            raise Errors.KafkaError(repr(e)) from e

        assignment_req = []
        for member_id, assignment in group_assignment.items():
            if not isinstance(assignment, bytes):
                assignment = assignment.encode()
            assignment_req.append((member_id, assignment))

        request = SyncGroupRequest(
            self.group_id,
            self._coordinator.generation,
            self._coordinator.member_id,
            self._coordinator._group_instance_id,
            assignment_req,
        )

        log.debug(
            "Sending leader SyncGroup for group %s to coordinator %s: %s",
            self.group_id,
            self.coordinator_id,
            request,
        )
        return await self._send_sync_group_request(request)

    async def _send_sync_group_request(self, request):
        # We need to reset the rejoin future right after the assignment to
        # capture metadata changes after join group was performed. We do not
        # set it directly after JoinGroup to avoid a false rejoin in case
        # ``_perform_assignment()`` does a metadata update.
        self._coordinator._rejoin_needed_fut = create_future()
        req_generation = self._coordinator.generation
        req_member_id = self._coordinator.member_id

        try:
            response = await self._coordinator._send_req(request)
        except Errors.KafkaError:
            # We lost connection to coordinator. No need to try and finish this
            # group join, just rejoin again.
            self._coordinator.request_rejoin()
            return None

        error_type = Errors.for_code(response.error_code)
        if error_type is Errors.NoError:
            log.info(
                "Successfully synced group %s with generation %s",
                self.group_id,
                self._coordinator.generation,
            )
            # make sure the right member_id/generation is set in case they changed
            # while the rejoin was taking place
            self._coordinator.generation = req_generation
            self._coordinator.member_id = req_member_id
            return response.member_assignment

        # Error case
        self._coordinator.request_rejoin()
        if error_type is Errors.RebalanceInProgressError:
            log.debug(
                "SyncGroup for group %s failed due to group rebalance", self.group_id
            )
        elif error_type in (Errors.UnknownMemberIdError, Errors.IllegalGenerationError):
            err = error_type()
            log.debug("SyncGroup for group %s failed due to %s,", self.group_id, err)
            self._coordinator.reset_generation()
        elif error_type in (
            Errors.GroupCoordinatorNotAvailableError,
            Errors.NotCoordinatorForGroupError,
        ):
            err = error_type()
            log.debug("SyncGroup for group %s failed due to %s", self.group_id, err)
            self._coordinator.coordinator_dead()
        elif error_type is Errors.GroupAuthorizationFailedError:
            raise error_type(self.group_id)
        else:
            err = error_type()
            log.error("Unexpected error from SyncGroup: %s", err)
            raise Errors.KafkaError(repr(err))

        return None

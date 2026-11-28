from __future__ import annotations

import asyncio
import concurrent.futures
import copy
import datetime
import inspect
import uuid
import logging
import weakref
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, List, Mapping, Optional, Tuple, Union

from .topic_writer import (
    InternalMessage,
    Message,
    PublicMessage,
    PublicWriterSettings,
    PublicWriteResult,
    PublicWriteResultTypes,
    TopicWriterClosedError,
    TopicWriterError,
    TopicWriterPartitionSplitError,
    TopicWriterStopped,
    internal_message_size_bytes,
    validate_buffer_limits,
)
from .topic_writer_asyncio import WriterAsyncIO, WriterBuffer
from .topic_writer_partition_chooser import (
    PublicPartitionByKeyBound,
    PublicPartitionByKeyKafka,
    PublicPartitionChooser,
)
from .. import _apis, issues
from ..settings import BaseRequestSettings
from .._topic_common.common import create_result_wrapper
from .._grpc.grpcwrapper.common_utils import to_thread
from .._grpc.grpcwrapper import ydb_topic as _ydb_topic
from .._grpc.grpcwrapper import ydb_topic_public_types as _ydb_topic_public_types
from .._grpc.grpcwrapper.ydb_topic_public_types import PublicAutoPartitioningStrategy, PublicCodec

_PartitionInfo = _ydb_topic_public_types.PublicDescribeTopicResult.PartitionInfo

logger = logging.getLogger(__name__)

# An OVERLOADED response may be an in-progress split/merge (children not yet visible in
# DescribeTopic) or ordinary transient overload. Re-describe a few times before deciding.
_REPARTITION_DISCOVER_ATTEMPTS = 4
_REPARTITION_DISCOVER_DELAY = 0.25

# Bound the RPC itself: cancelling an executor waiter cannot stop a running sync call.
_DESCRIBE_TIMEOUT = 30.0

# A per-partition sub-writer with no in-flight messages and no writes for this long is closed;
# it is recreated on demand. Set <= 0 to disable idle eviction.
_DEFAULT_WRITER_IDLE_TIMEOUT = 60.0

# A writer opened against a partition that is no longer active never finishes its init handshake.
# Since sub-writers are created while the orchestrator lock is held, an unbounded wait there stalls
# every write, flush and repartition, so the wait is capped.
_WRITER_INIT_TIMEOUT = 30.0


class _IncompletePartitionTopology(TopicWriterError):
    pass


@dataclass
class MultiWriterSettings:
    """Settings for the multi-partition (write-by-key) topic writer.

    order of fields IS NOT stable, use keywords only
    """

    topic: str
    producer_id_prefix: Optional[str] = None
    partition_chooser: Optional[PublicPartitionChooser] = None
    auto_seqno: bool = True
    auto_created_at: bool = True
    codec: Optional[PublicCodec] = None
    encoders: Optional[Mapping[PublicCodec, Callable[[bytes], bytes]]] = None
    encoder_executor: Optional[concurrent.futures.Executor] = None
    max_buffer_size_bytes: Optional[int] = None
    max_buffer_messages: Optional[int] = None
    buffer_wait_timeout_sec: Optional[float] = None
    # Idle per-partition sub-writers are closed after this many seconds and recreated on demand.
    # None -> default; <= 0 disables eviction.
    writer_idle_timeout_sec: Optional[float] = None

    def __post_init__(self):
        validate_buffer_limits(self.max_buffer_size_bytes, self.max_buffer_messages, self.buffer_wait_timeout_sec)
        if self.producer_id_prefix is None:
            self.producer_id_prefix = uuid.uuid4().hex
        # partition_chooser is left as-is; when None the writer picks one adaptively
        # after describing the topic (Bound if the topic reports key ranges, else Kafka).


@dataclass
class _InflightMessage:
    message: PublicMessage
    user_future: asyncio.Future
    seqno: int
    partition_id: int
    sub_future: Optional[asyncio.Future] = field(default=None)
    size_bytes: int = 0
    _flush_waiter: Optional[asyncio.Future] = field(default=None, init=False)

    def flush_waiter(self) -> asyncio.Future:
        # The caller can cancel user_future while this message is still being delivered.
        if self._flush_waiter is None:
            self._flush_waiter = self.user_future.get_loop().create_future()
        return self._flush_waiter

    def finish(self, outcome: Union[PublicWriteResultTypes, BaseException]) -> None:
        for future in (self.user_future, self._flush_waiter):
            if future is None or future.done():
                continue
            if isinstance(outcome, BaseException):
                future.set_exception(outcome)
                future.exception()  # write() does not expose an acknowledgement to the caller.
            else:
                future.set_result(outcome)


def _is_overloaded(err: BaseException) -> bool:
    return isinstance(err, issues.Overloaded)


class TopicWriterMultiAsyncIO:
    """One logical writer that routes messages to per-partition sub-writers by key.

    Each partition is served by an ordinary :class:`WriterAsyncIO` (buffering,
    encoding, reconnection and token refresh are reused as-is). On top of that this
    class:

    * routes each message to a partition via the partition chooser;
    * owns the in-flight messages and assigns their sequence numbers, so that on an
      auto-partition split it can transparently resend the un-acked messages of the
      split partition to its children — without duplicating messages that were
      already persisted (the ``maxSeqNo`` cut).
    """

    def __init__(self, driver, settings: MultiWriterSettings, _parent=None):
        self._loop = asyncio.get_running_loop()
        self._driver = driver
        self._parent = _parent  # keep parent client alive against GC
        self._settings = settings
        # producer_id_prefix is guaranteed set in __post_init__
        prefix = settings.producer_id_prefix
        assert prefix is not None
        self._prefix: str = prefix
        # Resolved in _init(): the configured chooser, or an adaptive default.
        self._chooser: Optional[PublicPartitionChooser] = settings.partition_chooser
        self._closing = False
        self._closed = False
        self._write_error: asyncio.Future[BaseException] = self._loop.create_future()
        self._buffer = None
        if settings.max_buffer_size_bytes is not None or settings.max_buffer_messages is not None:
            self._buffer = WriterBuffer(
                settings.max_buffer_size_bytes, settings.max_buffer_messages, settings.buffer_wait_timeout_sec
            )
        self._lock = asyncio.Lock()
        self._writers: Dict[int, WriterAsyncIO] = {}
        self._partitions: Dict[int, object] = {}
        # partition_id -> {seqno -> in-flight message}, un-acked messages we may resend.
        self._inflight: Dict[int, Dict[int, _InflightMessage]] = {}
        # One sequence for the whole writer, seeded above the last_seqno of every producer we
        # open, so a message keeps its number when a split moves it to another partition.
        self._seqno: int = 0
        # partition_id -> last persisted seqno reported by the current sub-writer's init.
        self._server_init_seqno: Dict[int, int] = {}
        # partition_id -> server's last persisted seqno for a retired producer. Final once read:
        # nothing writes under that producer id again.
        self._retired_max_seqno: Dict[int, int] = {}
        # partition_id -> highest acked seqno.
        self._max_acked: Dict[int, int] = {}
        # Partitions whose sub-writer is being torn down for a repartition or a recovery. Ack
        # failures from such a writer are expected and must not reach the user.
        self._retiring: set = set()
        # partition_id -> in-progress repartition task, so repeated OVERLOADED for one partition
        # coalesce and close() can cancel and await them.
        self._repartition_tasks: Dict[int, asyncio.Future] = {}
        # partition_id -> monotonic time of the last write/creation, for idle eviction.
        self._last_write_at: Dict[int, float] = {}
        self._idle_timeout = (
            settings.writer_idle_timeout_sec
            if settings.writer_idle_timeout_sec is not None
            else _DEFAULT_WRITER_IDLE_TIMEOUT
        )
        self._init_task = asyncio.ensure_future(self._init())
        self._reaper_task: Optional[asyncio.Future] = None
        if self._idle_timeout > 0:
            # Hold only a weakref so the reaper does not keep the writer alive against GC.
            self._reaper_task = asyncio.ensure_future(self._idle_reaper(weakref.ref(self), self._idle_timeout))

    async def __aenter__(self) -> "TopicWriterMultiAsyncIO":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.close()
        except BaseException:
            if exc_val is None:
                raise

    def __del__(self):
        if self._closed or self._loop.is_closed():
            return
        try:
            logger.debug("Topic multi-writer was not closed properly. Consider using method close().")
            task = self._loop.create_task(self.close(flush=False))
            task.set_name("close multiwriter")
        except BaseException:
            logger.warning("Something went wrong during multi-writer close in __del__")

    async def _describe(self):
        req = _ydb_topic_public_types.DescribeTopicRequestParams(path=self._settings.topic, include_stats=False)
        args = (
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.DescribeTopic,
            create_result_wrapper(_ydb_topic.DescribeTopicResult),
        )
        settings = BaseRequestSettings().with_timeout(_DESCRIBE_TIMEOUT)
        if inspect.iscoroutinefunction(self._driver.__call__):
            res = self._driver(*args, settings=settings)
        else:
            res = await to_thread(self._driver, *args, executor=None, settings=settings)
        if inspect.isawaitable(res):
            res = await res
        return res.to_public()

    async def _init(self):
        for attempt in range(_REPARTITION_DISCOVER_ATTEMPTS):
            description = await self._describe()
            leaves = [p for p in description.partitions if p.active and not p.child_partition_ids]
            chooser = self._chooser or self._default_chooser(leaves, description)
            if leaves and (
                not isinstance(chooser, PublicPartitionByKeyBound) or self._children_cover_parent(None, leaves)
            ):
                chooser.add_partitions(leaves)
                self._chooser = chooser
                self._partitions = {p.partition_id: p for p in leaves}
                return
            if attempt + 1 < _REPARTITION_DISCOVER_ATTEMPTS:
                await asyncio.sleep(_REPARTITION_DISCOVER_DELAY)
        raise _IncompletePartitionTopology("active partitions do not cover the topic key range")

    @staticmethod
    def _default_chooser(partitions, description=None) -> PublicPartitionChooser:
        # Route by key range on auto-partitioned topics, else by Kafka hash. Prefer the
        # topic's auto-partitioning strategy as the signal: a single auto partition can
        # report no key_range at all (open b""..b""), yet its split-children are bounded,
        # which only the Bound chooser can accept.
        aps = getattr(description, "auto_partitioning_settings", None)
        auto_enabled = aps is not None and aps.strategy not in (
            None,
            PublicAutoPartitioningStrategy.UNSPECIFIED,
            PublicAutoPartitioningStrategy.DISABLED,
        )
        has_key_ranges = any(p.key_range is not None for p in partitions)
        if auto_enabled or has_key_ranges:
            return PublicPartitionByKeyBound()
        return PublicPartitionByKeyKafka()

    async def wait_init(self):
        await asyncio.shield(self._init_task)

    def _check_closed(self):
        if self._closing or self._closed:
            raise TopicWriterClosedError()

    def _build_writer_settings(
        self,
        partition_id: int,
        with_split_hook: bool,
        pin_partition: bool = True,
    ) -> PublicWriterSettings:
        return PublicWriterSettings(
            topic=self._settings.topic,
            producer_id="%s-%d" % (self._prefix, partition_id),
            # Unpinned (pin_partition=False) leaves the session routed by message group instead,
            # which is the only way to reach a partition that is no longer active.
            partition_id=partition_id if pin_partition else None,
            # The multi-writer assigns sequence numbers itself so it can resend
            # messages to child partitions after a split, keeping them monotonic.
            auto_seqno=False,
            auto_created_at=False,
            codec=self._settings.codec,
            encoders=self._settings.encoders,
            encoder_executor=self._settings.encoder_executor,
            _on_check_retriable_error=self._make_overloaded_hook(partition_id) if with_split_hook else None,
        )

    async def _get_or_create_writer(self, partition_id: int) -> WriterAsyncIO:
        writer = self._writers.get(partition_id)
        if writer is None:
            writer = WriterAsyncIO(self._driver, self._build_writer_settings(partition_id, with_split_hook=True))
            # Seed the seqno cursor from the producer's last persisted seqno so a
            # stable producer_id_prefix resumes numbering instead of colliding. The wait is
            # bounded: a partition that went inactive between the routing decision and this call
            # never completes init, and we hold the orchestrator lock here.
            try:
                init_info = await asyncio.wait_for(writer.wait_init(), timeout=_WRITER_INIT_TIMEOUT)
            except BaseException:
                # Do not register a writer that never became usable, and do not leak its stream.
                await self._safe_close(writer)
                raise
            self._writers[partition_id] = writer
            self._last_write_at[partition_id] = self._loop.time()
            last_seqno = init_info.last_seqno or 0
            self._server_init_seqno[partition_id] = last_seqno
            # Lift the shared cursor above every producer we have opened: each partition has its
            # own producer_id and therefore its own persisted history, and a seqno we hand out
            # must be new in whichever partition the message ends up in.
            self._seqno = max(self._seqno, last_seqno)
        return writer

    async def _writer_for_migration(self, message: PublicMessage) -> Tuple[int, WriterAsyncIO]:
        assert self._chooser is not None
        for attempt in range(_REPARTITION_DISCOVER_ATTEMPTS):
            partition_id = self._chooser.choose_partition(message)
            try:
                return partition_id, await self._get_or_create_writer(partition_id)
            except (asyncio.TimeoutError, TopicWriterPartitionSplitError):
                if attempt + 1 == _REPARTITION_DISCOVER_ATTEMPTS:
                    raise
                await self._handle_repartition_locked(partition_id, recover=False)
                if partition_id in self._partitions:
                    raise
        raise AssertionError("unreachable")

    def _schedule_repartition(self, partition_id: int, *, refresh_only: bool = False) -> Optional[asyncio.Future]:
        """Start (or join) the repartition of one partition.

        A burst of OVERLOADED for the same partition must not start several concurrent
        recoveries of it, and the writer must own the task so close() can cancel and await it
        instead of leaving it to describe topics and open sub-writers after shutdown.
        """
        if self._closed:
            return None
        running = self._repartition_tasks.get(partition_id)
        if running is not None and not running.done():
            return running

        recovery = (
            self._handle_repartition(partition_id, recover=False)
            if refresh_only
            else self._on_partition_overloaded(partition_id)
        )
        task = self._loop.create_task(recovery)
        task.set_name("multiwriter repartition %d" % partition_id)
        self._repartition_tasks[partition_id] = task

        def _forget(finished: asyncio.Future) -> None:
            if not finished.cancelled():
                finished.exception()  # The write waiting for this refresh may have been cancelled.
            if self._repartition_tasks.get(partition_id) is finished:
                del self._repartition_tasks[partition_id]

        task.add_done_callback(_forget)
        return task

    def _make_overloaded_hook(self, partition_id: int):
        def hook(err: BaseException) -> bool:
            if _is_overloaded(err):
                logger.debug("multi-writer: partition %d overloaded, re-describing (split/merge)", partition_id)
                self._schedule_repartition(partition_id)
                return True
            return False

        return hook

    def _attach_ack(self, entry: _InflightMessage) -> None:
        sub_future = entry.sub_future
        assert sub_future is not None
        sub_future.add_done_callback(lambda f: self._on_sub_result(entry.partition_id, entry.seqno, f))

    def _finish_message(self, entry: _InflightMessage, outcome: Union[PublicWriteResultTypes, BaseException]) -> None:
        if self._buffer is not None:
            self._buffer.release(entry.size_bytes)
        entry.finish(outcome)

    def _fail_message(self, entry: _InflightMessage, err: BaseException) -> None:
        if not self._write_error.done():
            self._write_error.set_result(err)
        self._finish_message(entry, err)

    def _on_sub_result(self, partition_id: int, seqno: int, sub_future: asyncio.Future) -> None:
        entry = self._inflight.get(partition_id, {}).get(seqno)
        if entry is None or entry.sub_future is not sub_future:
            return  # stale: the message was already resolved or moved to a child

        if sub_future.cancelled():
            return

        exc = sub_future.exception()
        if exc is not None:
            if isinstance(exc, TopicWriterPartitionSplitError) or partition_id in self._retiring:
                # Expected while the partition is torn down: leave the message in flight so the
                # repartition (or the in-place recovery) resends it.
                return
            self._inflight.get(partition_id, {}).pop(seqno, None)
            self._fail_message(entry, exc)
            return

        # A success is always honoured, including one that lands while the sub-writer is being
        # closed: the server persisted the message, and ignoring the ack would leave the dedup
        # cut too low and resend an already-written message to the child.
        self._inflight.get(partition_id, {}).pop(seqno, None)
        self._max_acked[partition_id] = max(self._max_acked.get(partition_id, 0), seqno)
        self._finish_message(entry, sub_future.result())

    async def _quiesce_writer(self, partition_id: int) -> None:
        """Close the partition's sub-writer and settle the acks it was still holding.

        Closing fails every pending ack, which must not reach the user because those messages
        are about to be resent. ``_retiring`` suppresses exactly those failures while letting
        successes through, and the yield below gives already-scheduled ack callbacks a chance to
        run before the caller reads the dedup cut.
        """
        self._retiring.add(partition_id)
        writer = self._writers.pop(partition_id, None)
        if writer is not None:
            await self._safe_close(writer)
        await asyncio.sleep(0)

    @staticmethod
    async def _safe_close(writer: WriterAsyncIO) -> None:
        # A sub-writer stopped by the split hook re-raises TopicWriterPartitionSplitError from
        # close(); closing a writer we are discarding must never abort a repartition.
        try:
            await writer.close(flush=False)
        except Exception:  # noqa: BLE001
            logger.debug("multi-writer: ignoring error while closing a discarded sub-writer", exc_info=True)

    async def _probe_server_seqno(self, partition_id: int) -> int:
        """Ask the server how far this partition's producer actually got.

        The answer must come from the server, not from the acks we happened to receive: a message
        can be persisted and its ack lost when the session dies, and treating it as unwritten is
        exactly what produces a duplicate on resend.

        The session is opened WITHOUT a partition id. By the time we ask, the partition is
        usually inactive, and a session pinned to an inactive partition never completes its init
        handshake -- it would hang here holding the orchestrator lock. Unpinned, the session is
        routed by message group and still reports this producer's persisted seqno. Both reference
        implementations read the cut the same way (C++ `CreateWriteSession(..., false)` sets only
        ProducerId/MessageGroupId, Go `createNonDirectWriter` only WithProducerID).

        Results are cached: a retired producer receives no further writes, so its value is final.
        """
        cached = self._retired_max_seqno.get(partition_id)
        if cached is not None:
            return cached

        probe = WriterAsyncIO(
            self._driver,
            self._build_writer_settings(partition_id, with_split_hook=False, pin_partition=False),
        )
        try:
            init_info = await asyncio.wait_for(probe.wait_init(), timeout=_WRITER_INIT_TIMEOUT)
        finally:
            await self._safe_close(probe)

        last_seqno = init_info.last_seqno or 0
        self._retired_max_seqno[partition_id] = last_seqno
        logger.debug("multi-writer: partition %d persisted up to seqno %d (server)", partition_id, last_seqno)
        return last_seqno

    async def _max_seqno_cut(self, partition_id: int) -> int:
        """Dedup cut for a repartition: messages at or below it were persisted to the retiring
        partition and must not be resent to the child.

        Only this partition's own producer is asked. A message sitting here cannot have been
        persisted under any producer it used earlier in its life: every move is gated by a cut of
        at least that producer's server seqno, so anything already stored there was resolved as
        written on the spot and never travelled. Its number is therefore strictly above that
        value, and a retired producer never grows -- asking again would only repeat an answer that
        is already known to be too low to matter.

        Walking further would not merely be redundant. A merge child has two parents whose
        branches numbered independently, so the maximum over them pulls in the sibling branch's
        history, which says nothing about messages that came down this branch and can be
        arbitrarily higher than theirs.

        Raises if the server cannot be asked. Falling back to the highest ack we saw would look
        like it worked while quietly reopening the duplicate window this exists to close; the
        caller turns the failure into terminal errors on the affected messages instead.
        """
        acked = self._max_acked.get(partition_id, 0)
        cut = max(acked, await self._probe_server_seqno(partition_id))
        logger.debug(
            "multi-writer: dedup cut for partition %d is %d (highest ack seen %d)",
            partition_id,
            cut,
            acked,
        )
        return cut

    @staticmethod
    def _children_cover_parent(parent, children: List[_PartitionInfo]) -> bool:
        """Validate coverage before retiring a parent, so a gap cannot misroute its keys."""
        parent_range = getattr(parent, "key_range", None)
        if parent_range is None and all(child.key_range is None for child in children):
            return bool(children)  # fixed-partition topics do not report key ranges

        ranges = []
        for child in children:
            child_range = getattr(child, "key_range", None)
            if child_range is None:
                return False  # inconsistent with a bounded parent -> treat the view as incomplete
            ranges.append((child_range.from_bound, child_range.to_bound))
        ranges.sort(key=lambda r: r[0])

        # Coverage is "at least the parent's range", not "exactly": a merge child owns the ranges
        # of both its parents, so it legitimately covers more than the parent we started from.
        # A single auto partition may omit key_range before its first split.
        parent_end = parent_range.to_bound if parent_range is not None else b""
        cursor = parent_range.from_bound if parent_range is not None else b""
        for from_bound, to_bound in ranges:
            if from_bound > cursor:
                return False  # gap between the covered prefix and this child
            if not to_bound:
                return True  # this child runs to the end of the key space
            if to_bound > cursor:
                cursor = to_bound
            if parent_end and cursor >= parent_end:
                return True
        return False

    @staticmethod
    def _active_descendants(partition_id: int, partitions: List[_PartitionInfo]) -> List[_PartitionInfo]:
        by_id = {p.partition_id: p for p in partitions}
        children = {p.partition_id: set(p.child_partition_ids) for p in partitions}
        for partition in partitions:
            for parent in partition.parent_partition_ids:
                children.setdefault(parent, set()).add(partition.partition_id)
        pending = list(children.get(partition_id, ()))
        visited = {partition_id}
        leaves = []
        while pending:
            current_id = pending.pop()
            if current_id in visited:
                continue
            visited.add(current_id)
            current = by_id.get(current_id)
            descendants = children.get(current_id, set())
            if current is not None and current.active and not descendants:
                leaves.append(current)
            else:
                pending.extend(descendants)
        return leaves

    async def _discover_children(self, partition_id: int) -> Tuple[List[int], List[_PartitionInfo]]:
        """Find all connected retiring parents and their active descendants.

        A merge can connect parents through intermediate, already retired partitions.
        Every added parent contributes all its descendants, which may reach further parents.
        """
        for attempt in range(_REPARTITION_DISCOVER_ATTEMPTS):
            description = await self._describe()
            parents = {p.partition_id: set(p.parent_partition_ids) for p in description.partitions}
            for partition in description.partitions:
                for child_id in partition.child_partition_ids:
                    parents.setdefault(child_id, set()).add(partition.partition_id)

            children: Dict[int, _PartitionInfo] = {}
            retired = set()
            seen_ancestors = set()
            pending = [partition_id]
            complete = True
            while pending:
                old = pending.pop()
                if old in retired:
                    continue
                retired.add(old)
                descendants = self._active_descendants(old, description.partitions)
                complete = complete and self._children_cover_parent(self._partitions.get(old), descendants)
                ancestors: List[int] = []
                for child in descendants:
                    children[child.partition_id] = child
                    ancestors.extend(parents.get(child.partition_id, ()))
                while ancestors:
                    ancestor = ancestors.pop()
                    if ancestor in seen_ancestors:
                        continue
                    seen_ancestors.add(ancestor)
                    if ancestor in self._partitions:
                        if ancestor not in retired:
                            pending.append(ancestor)
                    else:
                        ancestors.extend(parents.get(ancestor, ()))

            if children and complete:
                return sorted(retired), [children[child_id] for child_id in sorted(children)]
            if attempt + 1 < _REPARTITION_DISCOVER_ATTEMPTS:
                await asyncio.sleep(_REPARTITION_DISCOVER_DELAY)
        if not children and any(
            p.partition_id == partition_id and p.active and not p.child_partition_ids for p in description.partitions
        ):
            return [], []
        raise _IncompletePartitionTopology("active descendants do not cover partition %d" % partition_id)

    def _fail_partition_inflight(self, partition_id: int, err: BaseException) -> None:
        """Give every in-flight message of an unusable partition a terminal outcome.

        Once repartition and recovery have both failed there is no sub-writer left to ack these
        messages, so their futures would never resolve and flush()/close(flush=True) would wait
        on them forever. An accepted message must always end in success or failure.
        """
        entries = self._inflight.pop(partition_id, {})
        if not entries:
            return
        logger.error(
            "multi-writer: partition %d is unusable, failing %d in-flight messages: %s",
            partition_id,
            len(entries),
            err,
        )
        for entry in entries.values():
            entry.sub_future = None
            self._fail_message(entry, err)

    async def _on_partition_overloaded(self, partition_id: int):
        """Entry point for the OVERLOADED hook: handle a repartition, or recover on failure.

        The hook force-stops the sub-writer, so if handling fails we must not leave the
        partition's messages stranded — recreate the writer and resend them. If that fails too,
        the messages are failed explicitly rather than left without an owner.
        """
        try:
            await self._handle_repartition(partition_id)
            return
        except asyncio.CancelledError:
            raise
        except Exception as err:
            logger.exception("multi-writer: repartition of partition %d failed; recovering", partition_id)
            failure: BaseException = err

        try:
            async with self._lock:
                if partition_id in self._partitions and not isinstance(failure, _IncompletePartitionTopology):
                    await self._recover_partition(partition_id)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as err:
            logger.exception("multi-writer: recovery of partition %d failed", partition_id)
            failure = err

        async with self._lock:
            await self._quiesce_writer(partition_id)
            self._retiring.discard(partition_id)
            self._fail_partition_inflight(partition_id, failure)

    async def _handle_repartition(self, partition_id: int, *, recover: bool = True):
        """Resolve an OVERLOADED partition under the routing lock."""
        async with self._lock:
            await self._handle_repartition_locked(partition_id, recover=recover)

    async def _handle_repartition_locked(self, partition_id: int, *, recover: bool = True):
        if partition_id not in self._partitions:
            return  # already handled by a sibling parent's event
        assert self._chooser is not None

        retired, children = await self._discover_children(partition_id)
        if not children:
            if recover:
                await self._recover_partition(partition_id)
            return

        new_children = [c for c in children if c.partition_id not in self._partitions]
        if new_children:
            self._chooser.add_partitions(new_children)
            for child in new_children:
                self._partitions[child.partition_id] = child
        for old in retired:
            self._chooser.remove_partition(old)
            self._partitions.pop(old, None)

        # Stop all retired writers before probing or migrating any of their messages.
        try:
            self._retiring.update(retired)
            for old in retired:
                await self._quiesce_writer(old)

            cuts = {}
            for old in retired:
                if self._inflight.get(old):
                    cuts[old] = await self._max_seqno_cut(old)
                else:
                    self._inflight.pop(old, None)
            await self._migrate_messages(cuts)
        except Exception as err:
            for old in retired:
                self._fail_partition_inflight(old, err)
            raise
        finally:
            self._retiring.difference_update(retired)

    async def _recover_partition(self, partition_id: int):
        # The hook stopped the sub-writer; drop it and resend the partition's in-flight
        # messages to a fresh writer for the SAME partition, keeping their seqnos.
        await self._quiesce_writer(partition_id)
        try:
            writer = await self._get_or_create_writer(partition_id)

            # The partition is still active, so this writer's init carries the server's real last
            # persisted seqno -- an exact dedup cut, unlike the split case. Messages at or below it
            # were written and their ack was lost with the stream; resending one is not idempotent
            # here, because the writer rejects a seqno it has already seen ("Message seqno is
            # duplicated") before the server ever gets a chance to deduplicate it, and that error
            # would abort the resend of every message after it.
            cut = max(self._server_init_seqno.get(partition_id, 0), self._max_acked.get(partition_id, 0))
            for seqno, entry in sorted(self._inflight.get(partition_id, {}).items()):
                if seqno <= cut:
                    self._inflight.get(partition_id, {}).pop(seqno, None)
                    self._max_acked[partition_id] = max(self._max_acked.get(partition_id, 0), seqno)
                    self._finish_message(entry, PublicWriteResult.Written(offset=-1))
                    continue
                entry.message.seqno = seqno
                sub_future = await writer._write_routed_message(entry.message)
                entry.sub_future = sub_future
                self._attach_ack(entry)
        finally:
            self._retiring.discard(partition_id)

    def _migration_conflict(
        self,
        child_id: int,
        seqno: int,
        target: Dict[int, _InflightMessage],
    ) -> Optional[BaseException]:
        """Reject a collision with the target's in-flight messages or persisted history."""
        if seqno in target:
            return TopicWriterError("seqno %d is already in flight on partition %d" % (seqno, child_id))
        server_seqno = self._server_init_seqno.get(child_id, 0)
        if seqno <= server_seqno:
            return TopicWriterError(
                "seqno %d cannot be resent to partition %d: its producer has already persisted"
                " up to %d" % (seqno, child_id, server_seqno)
            )
        return None

    async def _migrate_messages(self, cuts: Dict[int, int]):
        entries = []
        for partition_id, cut in cuts.items():
            for seqno, entry in list(self._inflight.get(partition_id, {}).items()):
                if seqno <= cut:
                    self._inflight.get(partition_id, {}).pop(seqno, None)
                    self._finish_message(entry, PublicWriteResult.Written(offset=-1))
                else:
                    entries.append((seqno, partition_id, entry))

        # Sorting the combined tail also orders every target's subsequence, including merges.
        for seqno, partition_id, entry in sorted(entries, key=lambda item: (item[0], item[1])):
            if self._inflight.get(partition_id, {}).get(seqno) is not entry:
                continue
            assert self._chooser is not None
            try:
                child_id, child_writer = await self._writer_for_migration(entry.message)
            except Exception as err:  # noqa: BLE001
                # The message cannot be placed: no ready leaf owns its key, or the child itself
                # went inactive (splits cascade). Fail it and everything after it -- dropping
                # would lose the message silently, and skipping ahead would reorder the key.
                logger.warning(
                    "multi-writer: cannot migrate messages of partition %d, failing seqno >= %d: %s",
                    partition_id,
                    seqno,
                    err,
                )
                for old in cuts:
                    self._fail_partition_inflight(old, err)
                break

            if self._inflight.get(partition_id, {}).get(seqno) is not entry:
                continue  # acked while the child writer was being opened -> nothing to resend

            target = self._inflight.setdefault(child_id, {})
            conflict = self._migration_conflict(child_id, seqno, target)
            if conflict is not None:
                logger.error("multi-writer: %s", conflict)
                self._inflight.get(partition_id, {}).pop(seqno, None)
                self._fail_message(entry, conflict)
                continue

            entry.message.seqno = seqno
            try:
                sub_future = await child_writer._write_routed_message(entry.message)
            except Exception as err:
                for old in cuts:
                    self._fail_partition_inflight(old, err)
                break
            self._inflight.get(partition_id, {}).pop(seqno, None)
            entry.partition_id = child_id
            target[seqno] = entry
            entry.sub_future = sub_future
            self._attach_ack(entry)

        for old in cuts:
            self._inflight.pop(old, None)

    async def write_with_ack_future(
        self,
        messages: Union[Message, List[Message]],
    ) -> Union[asyncio.Future, List[asyncio.Future]]:
        self._check_closed()

        input_single_message = not isinstance(messages, list)
        raw = messages if isinstance(messages, list) else [messages]
        converted = []
        for source in raw:
            message = copy.copy(PublicMessage._create_message(source))
            if self._settings.auto_seqno:
                if message.seqno not in (None, 0):
                    raise TopicWriterError("Explicit seqno and auto_seq setting is mutual exclusive")
            elif message.seqno in (None, 0):
                raise TopicWriterError("Empty seqno and auto_seq setting is disabled")
            if message.metadata_items is not None:
                message.metadata_items = message.metadata_items.copy()
            if self._settings.auto_created_at:
                if message.created_at is not None:
                    raise TopicWriterError(
                        "Explicit set auto_created_at and setting auto_created_at is mutual exclusive"
                    )
                message.created_at = datetime.datetime.now(datetime.timezone.utc)
            converted.append(message)

        await self.wait_init()
        futures: List[asyncio.Future] = []
        for message in converted:
            futures.append(await self._admit_message(message))

        return futures[0] if input_single_message else futures

    async def _admit_message(self, message: PublicMessage) -> asyncio.Future:
        assert self._chooser is not None
        attempts = 0
        deadline = None
        while True:
            async with self._lock:
                self._check_closed()
                partition_id = self._chooser.choose_partition(message)
                if self._buffer is not None and self._buffer.is_full():
                    self._next_seqno(message)  # Reject a stale manual number even when the buffer is full.
                    if deadline is None:
                        deadline = self._buffer.deadline()
                    recovery = None
                else:
                    try:
                        writer = await self._get_or_create_writer(partition_id)
                        self._check_closed()
                        return await self._enqueue_message(writer, partition_id, message, self._next_seqno(message))
                    except (asyncio.TimeoutError, TopicWriterPartitionSplitError) as err:
                        self._check_closed()
                        attempts += 1
                        if attempts >= _REPARTITION_DISCOVER_ATTEMPTS:
                            raise
                        init_timeout = err if isinstance(err, asyncio.TimeoutError) else None
                        recovery = self._schedule_repartition(partition_id, refresh_only=init_timeout is not None)
                        assert recovery is not None

            if recovery is None:
                assert self._buffer is not None
                await self._buffer.wait_for_space(self._check_closed, deadline)
            else:
                await asyncio.shield(recovery)
                if init_timeout is not None and partition_id in self._partitions:
                    raise init_timeout

    def _next_seqno(self, message: PublicMessage) -> int:
        if self._settings.auto_seqno:
            return self._seqno + 1
        assert message.seqno is not None
        if message.seqno <= self._seqno:
            raise TopicWriterError(
                "Message seqno must increase across the whole multi-writer: %d <= %d" % (message.seqno, self._seqno)
            )
        return message.seqno

    async def _enqueue_message(
        self, writer: WriterAsyncIO, partition_id: int, message: PublicMessage, seqno: int
    ) -> asyncio.Future:
        message.seqno = seqno
        size_bytes = 0
        if self._buffer is not None and self._buffer.max_bytes is not None:
            size_bytes = internal_message_size_bytes(InternalMessage(message))
            if message.key is not None:
                size_bytes += len(message.key.encode("utf-8"))
        sub_future = await writer._write_routed_message(message)
        if self._buffer is not None:
            self._buffer.add(size_bytes, 1)
        self._seqno = seqno
        self._last_write_at[partition_id] = self._loop.time()
        entry = _InflightMessage(
            message, self._loop.create_future(), seqno, partition_id, sub_future, size_bytes=size_bytes
        )
        self._inflight.setdefault(partition_id, {})[seqno] = entry
        self._attach_ack(entry)
        return entry.user_future

    async def write(self, messages: Union[Message, List[Message]]):
        await self.write_with_ack_future(messages)

    async def write_with_ack(
        self,
        messages: Union[Message, List[Message]],
    ) -> Union[PublicWriteResultTypes, List[PublicWriteResultTypes]]:
        futures = await self.write_with_ack_future(messages)
        future_list = futures if isinstance(futures, list) else [futures]
        await asyncio.wait(future_list)
        results = [f.result() for f in future_list]
        return results if isinstance(futures, list) else results[0]

    @staticmethod
    async def _idle_reaper(mw_ref: "weakref.ref", idle_timeout: float):
        interval = max(1.0, idle_timeout / 3)
        try:
            while True:
                await asyncio.sleep(interval)
                mw = mw_ref()
                if mw is None or mw._closed:
                    return
                try:
                    await mw._evict_idle_writers()
                except Exception:  # noqa: BLE001
                    logger.debug("multi-writer: idle eviction pass failed", exc_info=True)
                del mw  # drop the strong ref so the writer can be GC'd while we sleep
        except asyncio.CancelledError:
            pass

    async def _evict_idle_writers(self):
        # Close sub-writers with no in-flight messages that have not been written to for the idle
        # timeout; they are recreated on demand. Done under the lock so a concurrent write cannot
        # open a second session for the same producer while we close the old one.
        now = self._loop.time()
        async with self._lock:
            for partition_id in list(self._writers.keys()):
                if self._inflight.get(partition_id):
                    continue  # has un-acked messages -> not idle
                if now - self._last_write_at.get(partition_id, now) < self._idle_timeout:
                    continue
                writer = self._writers.pop(partition_id)
                self._last_write_at.pop(partition_id, None)
                logger.debug("multi-writer: evicting idle sub-writer for partition %d", partition_id)
                await self._safe_close(writer)

    def _pending_entries(self) -> Iterator[_InflightMessage]:
        for entries in self._inflight.values():
            yield from entries.values()

    async def _flush_impl(self):
        await self.wait_init()
        if self._write_error.done():
            raise self._write_error.result()
        async with self._lock:
            writers = list(self._writers.values())
            pending = [entry.flush_waiter() for entry in self._pending_entries()]

        async def wait_for_acks():
            results = await asyncio.gather(*(w.flush() for w in writers), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException) and not isinstance(result, TopicWriterPartitionSplitError):
                    raise result
            if pending:
                # Cancelling this waiter must not cancel accepted messages.
                await asyncio.wait(pending)
                for future in pending:
                    future.result()

        waiter = self._loop.create_task(wait_for_acks())
        try:
            await asyncio.wait([waiter, self._write_error], return_when=asyncio.FIRST_COMPLETED)
            if self._write_error.done():
                raise self._write_error.result()
            await waiter
        finally:
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)

    async def flush(self):
        self._check_closed()
        await self._flush_impl()

    async def close(self, *, flush: bool = True):
        if self._closed:
            return

        error: Optional[BaseException] = None
        # Stop new admissions before taking the flush snapshot. Recovery still serves accepted writes.
        self._closing = True
        if self._buffer is not None:
            self._buffer.updated.set()
        init_done = self._init_task.done() and not self._init_task.cancelled() and self._init_task.exception() is None
        if flush and init_done:
            try:
                await self._flush_impl()
            except BaseException as err:
                error = err
                logger.debug("multi-writer: flush during close failed", exc_info=True)

        self._closed = True
        if not self._init_task.done():
            self._init_task.cancel()
        if self._reaper_task is not None and not self._reaper_task.done():
            self._reaper_task.cancel()
        await asyncio.gather(self._init_task, return_exceptions=True)

        # Stop repartitions before touching the writers: an in-flight one holds the lock, opens
        # sub-writers and mutates the topology, none of which may outlive the multi-writer.
        repartitions = list(self._repartition_tasks.values())
        self._repartition_tasks.clear()
        for task in repartitions:
            if not task.done():
                task.cancel()
        if repartitions:
            await asyncio.gather(*repartitions, return_exceptions=True)

        async with self._lock:
            writers = list(self._writers.values())
            self._writers.clear()
            pending = list(self._pending_entries())
            self._inflight.clear()
        results = await asyncio.gather(*(w.close(flush=False) for w in writers), return_exceptions=True)
        for entry in pending:
            self._finish_message(entry, TopicWriterStopped())
        if flush:
            for result in results:
                if isinstance(result, BaseException) and not isinstance(result, TopicWriterPartitionSplitError):
                    error = error or result
            if error is not None:
                raise error

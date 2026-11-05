"""Workflow-side stream object for Workflow Streams.

Instantiate :class:`WorkflowStream` once from your workflow's ``@workflow.init``
method. The constructor registers the stream signal, update, and query
handlers on the current workflow via
:func:`temporalio.workflow.set_signal_handler`,
:func:`temporalio.workflow.set_update_handler`, and
:func:`temporalio.workflow.set_query_handler`.

For workflows that support continue-as-new, include a
``WorkflowStreamState | None`` field on the workflow input and pass it as
``prior_state`` — it is ``None`` on fresh starts and carries accumulated
state on continue-as-new.

Workflow-side and client-side topic handles
(:meth:`WorkflowTopicHandle.publish` and
:meth:`TopicHandle.publish`) both use the synchronous payload
converter for per-item ``Payload`` construction. The codec chain
(e.g. encryption, compression) is **not** run per item on either
side — it runs once at the envelope level when Temporal's SDK
encodes the signal/update that carries the batch. Running it per
item as well would double-encrypt, because every signal arg
already goes through the client's ``DataConverter.encode`` at
dispatch time.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from datetime import timedelta
from typing import Any, Callable, NoReturn, TypeVar, overload

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.exceptions import ApplicationError

from ._topic_handle import WorkflowTopicHandle
from ._types import (
    STREAM_DRAINING_ERROR_TYPE,
    TRUNCATED_OFFSET_ERROR_TYPE,
    PollInput,
    PollResult,
    PublisherState,
    PublishInput,
    WorkflowStreamItem,
    WorkflowStreamState,
    _decode_payload,
    _encode_payload,
    _WorkflowStreamWireItem,
)

_PUBLISH_SIGNAL = "__temporal_workflow_stream_publish"
_POLL_UPDATE = "__temporal_workflow_stream_poll"
_OFFSET_QUERY = "__temporal_workflow_stream_offset"

_MAX_POLL_RESPONSE_BYTES = 1_000_000

T = TypeVar("T")


def _payload_wire_size(payload: Payload, topic: str) -> int:
    """Approximate poll-response contribution of a single item.

    Wire form is ``_WorkflowStreamWireItem(topic, base64(proto(Payload)), offset)``.
    Base64 inflates by ~4/3; we use the serialized length as a
    conservative approximation.
    """
    return (payload.ByteSize() * 4 + 2) // 3 + len(topic)


class WorkflowStream:
    """Workflow-side stream object — append-only log with publish/poll handlers.

    .. warning::
        This class is experimental and may change in future versions.

    Construct once from ``@workflow.init``; the constructor registers
    the stream signal, update, and query handlers on the current
    workflow. Raises :class:`RuntimeError` if a ``WorkflowStream`` has
    already been registered on the workflow.

    Registered handlers:

    - ``__temporal_workflow_stream_publish`` signal — external publish with dedup
    - ``__temporal_workflow_stream_poll`` update — long-poll subscription
    - ``__temporal_workflow_stream_offset`` query — current log length

    Note:
        Because the publish handler is registered dynamically from
        ``__init__``, on the activation where the stream is
        constructed the publish signal can be buffered until after
        class-level signal/update handlers are scheduled. Define
        such handlers as ``async`` and ``await asyncio.sleep(0)``
        before reading stream state, so the publish signal is
        processed first.
    """

    def __init__(self, prior_state: WorkflowStreamState | None = None) -> None:
        """Initialize stream state and register workflow handlers.

        Must be called directly from the workflow's ``@workflow.init``
        method. Calls made from ``@workflow.run``, helper methods, or
        signal/update/query handlers raise :class:`RuntimeError`.

        The check inspects the immediate caller's frame and requires the
        function name to be ``__init__``.

        Args:
            prior_state: State carried from a previous run via
                :meth:`get_state` through continue-as-new, or ``None``
                on first start.

        Raises:
            RuntimeError: If not called directly from a method named
                ``__init__``, or if the stream signal handler is
                already registered on this workflow (i.e.,
                ``WorkflowStream`` was instantiated twice).

        Note:
            When carrying state across continue-as-new, type the
            carrying field as ``WorkflowStreamState | None``, not
            ``Any``. The default data converter deserializes ``Any``
            fields as plain dicts, which silently strips the
            ``WorkflowStreamState`` type and breaks the new run.
        """
        caller = sys._getframe(1)
        caller_name = caller.f_code.co_name
        if caller_name != "__init__":
            raise RuntimeError(
                "WorkflowStream must be constructed directly from the workflow's "
                f"@workflow.init method, not from {caller_name!r}."
            )
        if workflow.get_signal_handler(_PUBLISH_SIGNAL) is not None:
            raise RuntimeError(
                "WorkflowStream is already registered on this workflow. "
                "Construct WorkflowStream(...) at most once from @workflow.init."
            )

        if prior_state is not None:
            self._log: list[WorkflowStreamItem[Payload]] = [
                WorkflowStreamItem(topic=item.topic, data=_decode_payload(item.data))
                for item in prior_state.log
            ]
            self._base_offset: int = prior_state.base_offset
            self._publishers: dict[str, PublisherState] = {
                pid: PublisherState(sequence=ps.sequence, last_seen=ps.last_seen)
                for pid, ps in prior_state.publishers.items()
            }
        else:
            self._log = []
            self._base_offset = 0
            self._publishers = {}
        self._detaching: bool = False
        self._topic_types: dict[str, type[Any]] = {}

        workflow.set_signal_handler(_PUBLISH_SIGNAL, self._on_publish)
        workflow.set_update_handler(
            _POLL_UPDATE, self._on_poll, validator=self._validate_poll
        )
        workflow.set_query_handler(_OFFSET_QUERY, self._on_offset)

    def _publish_to_topic(self, topic: str, value: Any) -> None:
        """Internal publish path used by :class:`WorkflowTopicHandle`.

        Not part of the public API — call
        :meth:`WorkflowTopicHandle.publish` instead.
        """
        if isinstance(value, Payload):
            payload = value
        else:
            payload = workflow.payload_converter().to_payloads([value])[0]
        self._log.append(WorkflowStreamItem(topic=topic, data=payload))

    @overload
    def topic(self, name: str) -> WorkflowTopicHandle[Any]: ...
    @overload
    def topic(self, name: str, *, type: type[T]) -> WorkflowTopicHandle[T]: ...

    def topic(
        self, name: str, *, type: type[T] | None = None
    ) -> WorkflowTopicHandle[T] | WorkflowTopicHandle[Any]:
        """Return a typed handle for publishing to ``name`` from this workflow.

        The handle records the topic name and value type so call sites
        do not have to repeat them. Each :class:`WorkflowStream`
        instance binds a topic name to exactly one type: a second call
        with an unequal type raises ``RuntimeError``. Repeating the
        same call with the same type is idempotent and returns an
        equivalent handle.

        Type uniformity is checked only on this stream instance — it
        does not coordinate across publishers (other workflows,
        activities, external clients). The check uses Python equality
        on the type object; subtype and union-superset relationships
        are not recognized.

        Omitting ``type`` (or passing ``type=typing.Any``) is the
        documented escape hatch for heterogeneous topics. Pre-built
        ``Payload`` values can be passed to
        :meth:`WorkflowTopicHandle.publish` regardless of the bound
        type (zero-copy fast path) — there is no need to bind the
        topic to ``Payload`` itself.

        Args:
            name: Topic name.
            type: Value type bound to this handle. Defaults to
                ``typing.Any`` (heterogeneous topic).

        Returns:
            :class:`WorkflowTopicHandle` bound to ``name`` and the
            resolved type.

        Raises:
            RuntimeError: If ``name`` is already bound on this stream
                to a different type.
        """
        bound: Any = Any if type is None else type
        if bound is Payload:
            raise RuntimeError(
                "Cannot bind a topic to type=Payload. Pre-built Payload "
                "values can be passed to WorkflowTopicHandle.publish on "
                "any-typed handle (zero-copy fast path); omit type (or "
                "pass type=typing.Any) for heterogeneous topics."
            )
        existing = self._topic_types.get(name)
        if existing is not None and existing != bound:
            raise RuntimeError(
                f"Topic {name!r} is already bound to type {existing!r} on this "
                f"workflow stream; refusing to rebind to {bound!r}. Use a "
                f"single type per topic, or omit type (=typing.Any) for "
                f"heterogeneous topics."
            )
        self._topic_types[name] = bound
        return WorkflowTopicHandle(self, name, bound)

    def get_state(
        self, *, publisher_ttl: timedelta = timedelta(seconds=900)
    ) -> WorkflowStreamState:
        """Return a serializable snapshot of stream state for continue-as-new.

        Drops dedup state for publishers idle longer than
        ``publisher_ttl``. The TTL must exceed the
        ``max_retry_duration`` of any client that may still be
        retrying a failed flush.

        Args:
            publisher_ttl: Duration after which an idle publisher's
                dedup state is dropped. Default 15 minutes.
        """
        now = workflow.now()

        active_publishers = {
            pid: ps
            for pid, ps in self._publishers.items()
            if now - ps.last_seen < publisher_ttl
        }

        return WorkflowStreamState(
            log=[
                _WorkflowStreamWireItem(
                    topic=item.topic, data=_encode_payload(item.data)
                )
                for item in self._log
            ],
            base_offset=self._base_offset,
            publishers=active_publishers,
        )

    def detach_pollers(self) -> None:
        """Release waiting pollers and reject new poll updates.

        After this call the stream's ``__temporal_workflow_stream_poll``
        update handler releases its in-flight subscribers on this run:
        each waiting poll returns its current item batch (often empty)
        so the consumer can either follow continue-as-new or stop, and
        new polls are rejected at the validator. Publishes still land
        in the in-memory log and ``get_state`` / ``continue_as_new``
        remain valid — the stream is being held open just long enough
        to snapshot state and hand off to the next run.

        Call this before
        ``await workflow.wait_condition(workflow.all_handlers_finished)``
        and ``workflow.continue_as_new()``.
        """
        self._detaching = True

    async def continue_as_new(
        self,
        build_args: Callable[[WorkflowStreamState], Sequence[Any]],
        *,
        publisher_ttl: timedelta = timedelta(seconds=900),
    ) -> NoReturn:
        """Detach pollers, wait for handlers, continue-as-new with built args.

        Replaces this three-line recipe for the common case where the
        only continue-as-new parameter that varies is ``args``:

        .. code-block:: python

            self.stream.detach_pollers()
            await workflow.wait_condition(workflow.all_handlers_finished)
            workflow.continue_as_new(args=...)

        ``build_args`` is invoked *after* pollers have been detached,
        with the post-detach :class:`WorkflowStreamState` as its single
        argument. The caller threads that state into whatever input
        dataclass the workflow expects:

        .. code-block:: python

            await self.stream.continue_as_new(lambda state: [WorkflowInput(
                items_processed=self.items_processed,
                stream_state=state,
            )])

        Workflows that need to override other CAN parameters
        (``task_queue``, ``retry_policy``, ``run_timeout``, etc.) should
        keep using the explicit ``detach_pollers`` / ``wait_condition`` /
        ``workflow.continue_as_new(...)`` recipe.

        Args:
            build_args: Callable that receives the post-detach stream
                state and returns the positional ``args`` for the new
                run.
            publisher_ttl: Forwarded to :meth:`get_state`.

        Does not return; ``workflow.continue_as_new`` raises an internal
        exception that the SDK uses to close the run.
        """
        self.detach_pollers()
        await workflow.wait_condition(workflow.all_handlers_finished)
        workflow.continue_as_new(
            args=build_args(self.get_state(publisher_ttl=publisher_ttl)),
        )

    def truncate(self, up_to_offset: int) -> None:
        """Discard log entries before ``up_to_offset``.

        After truncation, polls requesting an offset before the new
        base will receive an ApplicationError. All global offsets
        remain monotonic.

        Raises ApplicationError (not ValueError) when ``up_to_offset``
        is past the end of the log so that callers invoking this from
        an update handler surface it as an update failure rather than
        a workflow-task poison pill.

        Args:
            up_to_offset: The global offset to truncate up to
                (exclusive). Entries at offsets
                ``[base_offset, up_to_offset)`` are discarded.
        """
        log_index = up_to_offset - self._base_offset
        if log_index <= 0:
            return
        if log_index > len(self._log):
            raise ApplicationError(
                f"Cannot truncate to offset {up_to_offset}: "
                f"valid range is [{self._base_offset}, {self._base_offset + len(self._log)})",
                type="TruncateOutOfRange",
            )
        self._log = self._log[log_index:]
        self._base_offset = up_to_offset

    def _on_publish(self, payload: PublishInput) -> None:
        """Receive publications from external clients (activities, starters).

        Deduplicates using (publisher_id, sequence). If publisher_id is
        set and the sequence is <= the last seen sequence for that
        publisher, the entire batch is dropped as a duplicate. Batches
        are atomic: the dedup decision applies to the whole batch, not
        individual items.

        This block is a polyfill for missing server-side ``request_id``
        dedup across continue-as-new. If the SDK ever exposes
        ``request_id`` on signals and the server dedups it across CAN,
        this branch and the ``_publishers`` state become redundant. See
        DESIGN §"Replace workflow-side dedup with server-side
        request_id" for the migration plan.
        """
        if payload.publisher_id:
            existing = self._publishers.get(payload.publisher_id)
            if existing is not None and payload.sequence <= existing.sequence:
                return
            self._publishers[payload.publisher_id] = PublisherState(
                sequence=payload.sequence,
                last_seen=workflow.now(),
            )
        for entry in payload.items:
            self._log.append(
                WorkflowStreamItem(topic=entry.topic, data=_decode_payload(entry.data))
            )

    async def _on_poll(self, payload: PollInput) -> PollResult:
        """Long-poll: block until new items available or detaching, then return."""
        # Re-evaluate the predicate against current ``_base_offset`` on
        # every iteration: a ``truncate()`` between this poll's arrival
        # and the wait firing changes ``log_offset`` underneath us, so
        # capturing it as a local would freeze the wait against stale
        # state and the poll would only return when the long-poll RPC
        # times out.
        await workflow.wait_condition(
            lambda: (
                payload.from_offset < self._base_offset
                or len(self._log) > payload.from_offset - self._base_offset
                or self._detaching
            ),
        )
        log_offset = payload.from_offset - self._base_offset
        if log_offset < 0:
            if payload.from_offset == 0:
                # "From the beginning" — start at whatever is available.
                log_offset = 0
            else:
                # Subscriber had a specific position that's been
                # truncated. ApplicationError fails this update (client
                # gets the error) without crashing the workflow task —
                # avoids a poison pill during replay.
                raise ApplicationError(
                    f"Requested offset {payload.from_offset} has been truncated. "
                    f"Current base offset is {self._base_offset}.",
                    type=TRUNCATED_OFFSET_ERROR_TYPE,
                )
        all_new = self._log[log_offset:]
        if payload.topics:
            topic_set = set(payload.topics)
            candidates = [
                (self._base_offset + log_offset + i, item)
                for i, item in enumerate(all_new)
                if item.topic in topic_set
            ]
        else:
            candidates = [
                (self._base_offset + log_offset + i, item)
                for i, item in enumerate(all_new)
            ]
        # Cap response size to ~1MB wire bytes.
        wire_items: list[_WorkflowStreamWireItem] = []
        size = 0
        more_ready = False
        next_offset = self._base_offset + len(self._log)
        for off, item in candidates:
            item_size = _payload_wire_size(item.data, item.topic)
            if size + item_size > _MAX_POLL_RESPONSE_BYTES and wire_items:
                # Resume from this item on the next poll.
                next_offset = off
                more_ready = True
                break
            size += item_size
            wire_items.append(
                _WorkflowStreamWireItem(
                    topic=item.topic, data=_encode_payload(item.data), offset=off
                )
            )
        return PollResult(
            items=wire_items,
            next_offset=next_offset,
            more_ready=more_ready,
        )

    def _validate_poll(self, _payload: PollInput) -> None:
        """Reject new polls when pollers are detached for continue-as-new.

        Uses the well-known ``StreamDraining`` type so a subscriber recognizes
        the rollover-in-progress and retries until its poll lands on the
        successor run, rather than surfacing the rejection as an error.
        """
        if self._detaching:
            raise ApplicationError(
                "Workflow pollers are detached for continue-as-new",
                type=STREAM_DRAINING_ERROR_TYPE,
            )

    def _on_offset(self) -> int:
        """Return the current global offset (base_offset + log length)."""
        return self._base_offset + len(self._log)

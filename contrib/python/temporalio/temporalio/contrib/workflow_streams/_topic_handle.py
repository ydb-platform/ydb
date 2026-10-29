"""Typed topic handles for Workflow Streams.

A topic handle is a thin typed view over an underlying publisher. It
carries the topic name and the value type ``T`` so call sites do not
have to repeat them on every publish, and so cross-language SDKs can
mirror the binding cleanly.

Type-uniformity is enforced per publisher instance: each
:class:`WorkflowStreamClient` (or :class:`WorkflowStream`) maps a topic
name to exactly one bound ``T``. Re-binding the same name to an
unequal type raises ``RuntimeError``. The check uses Python equality
on the type object — primitives, dataclasses, generic aliases, and
unions all compare structurally — and intentionally does not attempt
to recognize subtype or union-superset relationships.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar

from temporalio.api.common.v1 import Payload

from ._types import WorkflowStreamItem

if TYPE_CHECKING:
    from ._client import WorkflowStreamClient
    from ._stream import WorkflowStream

T = TypeVar("T")


class TopicHandle(Generic[T]):
    """Client-side handle for publishing to and subscribing from a single topic.

    .. warning::
        This class is experimental and may change in future versions.

    Constructed via :meth:`WorkflowStreamClient.topic`. Publishes share
    the underlying client's batching, dedup, and codec path; this
    object holds only the topic name and bound type.
    """

    def __init__(
        self,
        client: WorkflowStreamClient,
        name: str,
        type: type[T],
    ) -> None:
        """Bind the handle to a client, topic name, and type.

        Prefer :meth:`WorkflowStreamClient.topic` over calling this
        directly; the factory is what records the per-client type
        binding and rejects conflicts.
        """
        self._client = client
        self._name = name
        self._type = type

    @property
    def name(self) -> str:
        """The topic name this handle is bound to."""
        return self._name

    @property
    def type(self) -> type[T]:
        """The value type this handle is bound to."""
        return self._type

    def publish(self, value: T | Payload, *, force_flush: bool = False) -> None:
        """Buffer ``value`` for publishing on this topic.

        Equivalent to the underlying client's publish path; the value
        flows through the same buffer, batch interval, and dedup
        sequence.

        Args:
            value: Value to publish. Goes through the client's sync
                payload converter at flush time. A pre-built
                :class:`temporalio.api.common.v1.Payload` bypasses
                conversion (zero-copy fast path), regardless of the
                handle's bound type.
            force_flush: If True, wake the flusher to send immediately
                (fire-and-forget — does not block the caller).
        """
        self._client._publish_to_topic(self._name, value, force_flush=force_flush)

    async def subscribe(
        self,
        from_offset: int = 0,
        *,
        poll_cooldown: timedelta = timedelta(milliseconds=100),
    ) -> AsyncIterator[WorkflowStreamItem[T]]:
        """Async iterator over items on this topic, decoded as ``T``.

        For raw ``Payload`` access, or any other decode type that
        differs from the handle's bound ``T``, use
        :meth:`WorkflowStreamClient.subscribe` directly with an
        explicit ``result_type`` (typically
        :class:`temporalio.common.RawValue`). The handle's bound
        type intentionally cannot be ``Payload`` — the converter has
        no Payload decode path.

        Args:
            from_offset: Global offset to start reading from.
            poll_cooldown: Minimum interval between polls when there
                are no new items.
        """
        async for item in self._client.subscribe(
            [self._name],
            from_offset=from_offset,
            result_type=self._type,
            poll_cooldown=poll_cooldown,
        ):
            yield item


class WorkflowTopicHandle(Generic[T]):
    """Workflow-side handle for publishing to a single topic.

    .. warning::
        This class is experimental and may change in future versions.

    Constructed via :meth:`WorkflowStream.topic`. Has no
    ``subscribe`` — workflows do not consume their own stream.
    """

    def __init__(
        self,
        stream: WorkflowStream,
        name: str,
        type: type[T],
    ) -> None:
        """Bind the handle to a stream, topic name, and type.

        Prefer :meth:`WorkflowStream.topic` over calling this directly;
        the factory is what records the per-stream type binding and
        rejects conflicts.
        """
        self._stream = stream
        self._name = name
        self._type = type

    @property
    def name(self) -> str:
        """The topic name this handle is bound to."""
        return self._name

    @property
    def type(self) -> type[T]:
        """The value type this handle is bound to."""
        return self._type

    def publish(self, value: T | Payload) -> None:
        """Append ``value`` to the workflow stream on this topic.

        Args:
            value: Value to publish. Goes through the workflow's sync
                payload converter. A pre-built
                :class:`temporalio.api.common.v1.Payload` bypasses
                conversion, regardless of the handle's bound type.
        """
        self._stream._publish_to_topic(self._name, value)

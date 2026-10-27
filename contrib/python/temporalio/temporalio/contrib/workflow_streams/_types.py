"""Shared data types for the Workflow Streams contrib module.

The user-facing ``data`` fields on :class:`WorkflowStreamItem` are
:class:`temporalio.api.common.v1.Payload`. Per-item values are converted to
``Payload`` by the payload converter at publish time, and the resulting
bytes/metadata are preserved per item so subscribers can decode with
``subscribe(result_type=T)``. The codec chain (e.g. encryption, compression)
applies once at the outer signal/update envelope level — not separately to each
embedded item — so codec behavior is symmetric between workflow-side and
client-side publishing.

The wire representation (``PublishEntry``, ``_WorkflowStreamWireItem``) uses
base64-encoded ``Payload.SerializeToString()`` bytes because the default JSON
payload converter cannot serialize a ``Payload`` embedded inside a dataclass
(it only special-cases top-level Payloads on signal/update args).
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic, TypeVar

from temporalio.api.common.v1 import Payload

T = TypeVar("T")

# Well-known ``ApplicationError.type`` values the stream workflow uses to reject
# polls, and which ``WorkflowStreamClient.subscribe`` recognizes to drive retry
# behavior. Defined here so the raise sites (``_stream.py``) and the handling
# sites (``_client.py``) cannot diverge.
STREAM_DRAINING_ERROR_TYPE = "StreamDraining"
TRUNCATED_OFFSET_ERROR_TYPE = "TruncatedOffset"


# basedpyright flags _-prefixed module-level functions as unused even when
# sibling modules import them (_stream.py, _client.py). Vanilla pyright does
# not. Suppressions below are required for `poe lint`.
def _encode_payload(payload: Payload) -> str:  # pyright: ignore[reportUnusedFunction]
    """Wire format: base64(Payload.SerializeToString())."""
    return base64.b64encode(payload.SerializeToString()).decode("ascii")


def _decode_payload(wire: str) -> Payload:  # pyright: ignore[reportUnusedFunction]
    """Inverse of :func:`_encode_payload`."""
    payload = Payload()
    payload.ParseFromString(base64.b64decode(wire))
    return payload


@dataclass
class WorkflowStreamItem(Generic[T]):
    """A single item in the workflow stream's log.

    .. warning::
        This class is experimental and may change in future versions.

    The ``data`` field carries the decoded value produced by
    :meth:`WorkflowStreamClient.subscribe`. The generic parameter ``T``
    matches the ``result_type`` passed to ``subscribe``: an instance of
    ``T`` when ``result_type=T``, the converter's default ``Any``
    decoding when ``result_type`` is omitted, or a
    :class:`temporalio.common.RawValue` wrapping the original
    ``Payload`` when ``result_type=RawValue``.

    The ``offset`` field is populated at poll time from the item's
    position in the global log.
    """

    topic: str
    data: T
    offset: int = 0


@dataclass
class PublishEntry:
    """A single entry to publish via signal (wire type).

    .. warning::
        This class is experimental and may change in future versions.

    ``data`` is base64-encoded ``Payload.SerializeToString()`` output —
    see module docstring for why a nested ``Payload`` cannot be used
    directly.
    """

    topic: str
    data: str


@dataclass
class PublishInput:
    """Signal payload: batch of entries to publish.

    .. warning::
        This class is experimental and may change in future versions.

    Includes publisher_id and sequence to ensure exactly-once delivery.
    """

    items: list[PublishEntry] = field(default_factory=list)
    publisher_id: str = ""
    sequence: int = 0


@dataclass
class PollInput:
    """Update payload: request to poll for new items.

    .. warning::
        This class is experimental and may change in future versions.
    """

    topics: list[str] = field(default_factory=list)
    from_offset: int = 0


@dataclass
class _WorkflowStreamWireItem:
    """Wire representation of a WorkflowStreamItem (base64 of serialized Payload)."""

    topic: str
    data: str
    offset: int = 0


@dataclass
class PollResult:
    """Update response: items matching the poll request.

    .. warning::
        This class is experimental and may change in future versions.

    ``items`` use the wire representation. When ``more_ready`` is True,
    the response was truncated to stay within size limits and the
    subscriber should poll again immediately rather than applying a
    cooldown delay.
    """

    items: list[_WorkflowStreamWireItem] = field(default_factory=list)
    next_offset: int = 0
    more_ready: bool = False


@dataclass
class PublisherState:
    """Per-publisher dedup state.

    .. warning::
        This class is experimental and may change in future versions.

    Tracks the last accepted ``sequence`` and the ``workflow.now()`` at
    which it was accepted, used together for at-least-once dedup and
    TTL-based pruning at continue-as-new time.
    """

    sequence: int
    last_seen: datetime


@dataclass
class WorkflowStreamState:
    """Serializable snapshot of stream state for continue-as-new.

    .. warning::
        This class is experimental and may change in future versions.

    The containing workflow input must type the field as
    ``WorkflowStreamState | None``, not ``Any``, so the default data converter
    can reconstruct the dataclass from JSON.

    Log items use the wire representation for serialization stability.
    """

    log: list[_WorkflowStreamWireItem] = field(default_factory=list)
    base_offset: int = 0
    publishers: dict[str, PublisherState] = field(default_factory=dict)

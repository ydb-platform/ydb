"""Serialization context types for data conversion."""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from typing_extensions import Self


class SerializationContext(ABC):
    """Base serialization context.

    Provides contextual information during serialization and deserialization operations.

    Examples:
        In client code, when starting a workflow, or sending a signal/update/query to a workflow,
        or receiving the result of an update/query, or handling an exception from a workflow, the
        context type is :py:class:`WorkflowSerializationContext` and the workflow ID set of the
        target workflow will be set in the context.

        In workflow code, when operating on a payload being sent/received to/from a child workflow,
        or handling an exception from a child workflow, the context type is
        :py:class:`WorkflowSerializationContext` and the workflow ID is that of the child workflow,
        not of the currently executing (i.e. parent) workflow.

        In workflow code, when operating on a payload to be sent/received to/from an activity, the
        context type is :py:class:`ActivitySerializationContext` and the workflow ID is that of the
        currently-executing workflow. ActivitySerializationContext is also set on data converter
        operations in the activity context.
    """

    pass


@dataclass(frozen=True)
class WorkflowSerializationContext(SerializationContext):
    """Serialization context for workflows.

    See :py:class:`SerializationContext` for more details.
    """

    namespace: str
    """The namespace the workflow is running in."""

    workflow_id: str
    """The ID of the workflow.

    Note that this is the ID of the workflow of which the payload being operated on is an input or
    output. Note also that when creating/describing schedules, this may be the workflow ID prefix
    as configured, not the final workflow ID when the workflow is created by the schedule.
    """


@dataclass(frozen=True)
class ActivitySerializationContext(SerializationContext):
    """Serialization context for activities.

    See :py:class:`SerializationContext` for more details.
    """

    namespace: str
    """Workflow/activity namespace."""

    activity_id: str | None
    """Activity ID. Optional if this is an activity started from a workflow."""

    activity_type: str | None
    """Activity type.

    .. deprecated::
        This value may not be set in some bidirectional situations, it should
        not be relied on.
    """

    activity_task_queue: str | None
    """Activity task queue.

    .. deprecated::
        This value may not be set in some bidirectional situations, it should
        not be relied on.
    """

    workflow_id: str | None
    """Workflow ID. Only set if this is an activity started from a workflow.

    Note, when creating/describing schedules, this may be the workflow ID prefix as
    configured, not the final workflow ID when the workflow is created by the schedule."""

    workflow_type: str | None
    """Workflow type if this is an activity started from a workflow."""

    is_local: bool
    """Whether the activity is a local activity started from a workflow."""


class WithSerializationContext(ABC):
    """Interface for classes that can use serialization context.

    The following classes may implement this interface:
    - :py:class:`PayloadConverter`
    - :py:class:`PayloadCodec`
    - :py:class:`FailureConverter`
    - :py:class:`EncodingPayloadConverter`

    During data converter operations (encoding/decoding, serialization/deserialization, and failure
    conversion), instances of classes implementing this interface will be replaced by the result of
    calling with_context(context). This allows overridden methods (encode/decode,
    to_payload/from_payload, etc) to use the context.
    """

    def with_context(self, context: SerializationContext) -> Self:  # type: ignore[reportUnusedParameter]
        """Return a copy of this object configured to use the given context.

        Args:
            context: The serialization context to use.

        Returns:
            A new instance configured with the context.
        """
        raise NotImplementedError()

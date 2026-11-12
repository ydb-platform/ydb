"""Workflow Streams for Temporal workflows.

.. warning::
    This package is experimental and may change in future versions.

The Workflow Streams contrib library gives a workflow a durable,
offset-addressed event channel built from Signals and polling Updates
with an SSE bridge. Cost scales with durable batches, not tokens.
Latency is around 100ms per roundtrip; not for ultra-low-latency voice.

See :py:class:`WorkflowStream` for the workflow-side stream object and
:py:class:`WorkflowStreamClient` for the external client interface.
"""

from temporalio.contrib.workflow_streams._client import WorkflowStreamClient
from temporalio.contrib.workflow_streams._stream import WorkflowStream
from temporalio.contrib.workflow_streams._topic_handle import (
    TopicHandle,
    WorkflowTopicHandle,
)
from temporalio.contrib.workflow_streams._types import (
    PollInput,
    PollResult,
    PublishEntry,
    PublisherState,
    PublishInput,
    WorkflowStreamItem,
    WorkflowStreamState,
)

__all__ = [
    "PollInput",
    "PollResult",
    "PublishEntry",
    "PublishInput",
    "PublisherState",
    "TopicHandle",
    "WorkflowStream",
    "WorkflowStreamClient",
    "WorkflowStreamItem",
    "WorkflowStreamState",
    "WorkflowTopicHandle",
]

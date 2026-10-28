"""Parameters for configuring Temporal activity execution for model calls."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from agents import Agent, TResponseInputItem

from temporalio.common import Priority, RetryPolicy
from temporalio.workflow import ActivityCancellationType, VersioningIntent


class ModelSummaryProvider(ABC):
    """Abstract base class for providing model summaries. Essentially just a callable,
    but the arguments are sufficiently complex to benefit from names.
    """

    @abstractmethod
    def provide(
        self,
        agent: Agent[Any] | None,
        instructions: str | None,
        input: str | list[TResponseInputItem],
    ) -> str:
        """Given the provided information, produce a summary for the model invocation activity."""
        pass


@dataclass
class ModelActivityParameters:
    """Parameters for configuring Temporal activity execution for model calls.

    This class encapsulates all the parameters that can be used to configure
    how Temporal activities are executed when making model calls through the
    OpenAI Agents integration.
    """

    task_queue: str | None = None
    """Specific task queue to use for model activities."""

    schedule_to_close_timeout: timedelta | None = None
    """Maximum time from scheduling to completion."""

    schedule_to_start_timeout: timedelta | None = None
    """Maximum time from scheduling to starting."""

    start_to_close_timeout: timedelta | None = timedelta(seconds=60)
    """Maximum time for the activity to complete."""

    heartbeat_timeout: timedelta | None = None
    """Maximum time between heartbeats. For streaming
    (``Runner.run_streamed``), set this lower than
    ``start_to_close_timeout`` so a stuck model call is detected before the
    overall activity timeout fires."""

    retry_policy: RetryPolicy | None = None
    """Policy for retrying failed activities."""

    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL
    """How the activity handles cancellation."""

    versioning_intent: VersioningIntent | None = None
    """Versioning intent for the activity."""

    summary_override: None | (str | ModelSummaryProvider) = None
    """Summary for the activity execution."""

    priority: Priority = Priority.default
    """Priority for the activity execution."""

    use_local_activity: bool = False
    """Whether to use a local activity. If changed during a workflow execution, that would break determinism."""

    streaming_topic: str | None = None
    """Stream topic to publish raw model stream events to when the workflow
    calls ``Runner.run_streamed``. Required for ``Runner.run_streamed``;
    if left as ``None``, ``run_streamed`` raises before scheduling any
    activity. The workflow must host a
    :class:`temporalio.contrib.workflow_streams.WorkflowStream` to receive
    the publishes; otherwise the signals are unhandled and dropped.

    Streaming is incompatible with ``use_local_activity`` (local activities
    do not support heartbeats or the workflow stream signal channel).

    .. warning::
        Streaming support is experimental and may change in future
        versions."""

    streaming_batch_interval: timedelta = timedelta(milliseconds=100)
    """Interval between automatic flushes for the stream publisher used
    by the streaming activity.

    .. warning::
        Streaming support is experimental and may change in future
        versions."""

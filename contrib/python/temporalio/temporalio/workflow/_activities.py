from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Concatenate, Generic, TypedDict, overload

import temporalio.bridge.proto.workflow_commands
import temporalio.common

from ..types import (
    AnyType,
    CallableAsyncNoParam,
    CallableAsyncSingleParam,
    CallableSyncNoParam,
    CallableSyncSingleParam,
    MethodAsyncNoParam,
    MethodAsyncSingleParam,
    MethodSyncNoParam,
    MethodSyncSingleParam,
    MultiParamSpec,
    ParamType,
    ReturnType,
    SelfType,
)
from ._context import _Runtime
from ._exceptions import VersioningIntent

__all__ = [
    "ActivityCancellationType",
    "ActivityConfig",
    "ActivityHandle",
    "LocalActivityConfig",
    "execute_activity",
    "execute_activity_class",
    "execute_activity_method",
    "execute_local_activity",
    "execute_local_activity_class",
    "execute_local_activity_method",
    "start_activity",
    "start_activity_class",
    "start_activity_method",
    "start_local_activity",
    "start_local_activity_class",
    "start_local_activity_method",
]

# See https://mypy.readthedocs.io/en/latest/runtime_troubles.html#using-classes-that-are-generic-in-stubs-but-not-at-runtime
if TYPE_CHECKING:

    class _AsyncioTask(asyncio.Task[AnyType]):
        pass

else:
    # TODO: inherited classes should be other way around?
    class _AsyncioTask(Generic[AnyType], asyncio.Task):
        pass


class ActivityHandle(_AsyncioTask[ReturnType]):  # type: ignore[type-var]
    """Handle returned from :py:func:`start_activity` and
    :py:func:`start_local_activity`.

    This extends :py:class:`asyncio.Task` and supports all task features.
    """

    pass


class ActivityCancellationType(IntEnum):
    """How an activity cancellation should be handled."""

    TRY_CANCEL = int(
        temporalio.bridge.proto.workflow_commands.ActivityCancellationType.TRY_CANCEL
    )
    WAIT_CANCELLATION_COMPLETED = int(
        temporalio.bridge.proto.workflow_commands.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED
    )
    ABANDON = int(
        temporalio.bridge.proto.workflow_commands.ActivityCancellationType.ABANDON
    )


class ActivityConfig(TypedDict, total=False):
    """TypedDict of config that can be used for :py:func:`start_activity` and
    :py:func:`execute_activity`.
    """

    task_queue: str | None
    schedule_to_close_timeout: timedelta | None
    schedule_to_start_timeout: timedelta | None
    start_to_close_timeout: timedelta | None
    heartbeat_timeout: timedelta | None
    retry_policy: temporalio.common.RetryPolicy | None
    cancellation_type: ActivityCancellationType
    activity_id: str | None
    versioning_intent: VersioningIntent | None
    summary: str | None
    priority: temporalio.common.Priority


# Overload for async no-param activity
@overload
def start_activity(
    activity: CallableAsyncNoParam[ReturnType],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_activity(
    activity: CallableSyncNoParam[ReturnType],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_activity(
    activity: CallableAsyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_activity(
    activity: CallableSyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_activity(
    activity: Callable[..., Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_activity(
    activity: Callable[..., ReturnType],
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for string-name activity
@overload
def start_activity(
    activity: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]: ...


def start_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]:
    """Start an activity and return its handle.

    At least one of ``schedule_to_close_timeout`` or ``start_to_close_timeout``
    must be present.

    Args:
        activity: Activity name or function reference.
        arg: Single argument to the activity.
        args: Multiple arguments to the activity. Cannot be set if arg is.
        task_queue: Task queue to run the activity on. Defaults to the current
            workflow's task queue.
        result_type: For string activities, this can set the specific result
            type hint to deserialize into.
        schedule_to_close_timeout: Max amount of time the activity can take from
            first being scheduled to being completed before it times out. This
            is inclusive of all retries.
        schedule_to_start_timeout: Max amount of time the activity can take to
            be started from first being scheduled.
        start_to_close_timeout: Max amount of time a single activity run can
            take from when it starts to when it completes. This is per retry.
        heartbeat_timeout: How frequently an activity must invoke heartbeat
            while running before it is considered timed out.
        retry_policy: How an activity is retried on failure. If unset, a
            server-defined default is used. Set maximum attempts to 1 to disable
            retries.
        cancellation_type: How the activity is treated when it is cancelled from
            the workflow.
        activity_id: Optional unique identifier for the activity. This is an
            advanced setting that should not be set unless users are sure they
            need to. Contact Temporal before setting this value.
        versioning_intent: When using the Worker Versioning feature, specifies whether this Activity
            should run on a worker with a compatible Build Id or not.
            Deprecated: Use Worker Deployment versioning instead.
        summary: A single-line fixed summary for this activity that may appear in UI/CLI.
            This can be in single-line Temporal markdown format.
        priority: Priority of the activity.

    Returns:
        An activity handle to the activity which is an async task.
    """
    return _Runtime.current().workflow_start_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        task_queue=task_queue,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


# Overload for async no-param activity
@overload
async def execute_activity(
    activity: CallableAsyncNoParam[ReturnType],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_activity(
    activity: CallableSyncNoParam[ReturnType],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_activity(
    activity: CallableAsyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_activity(
    activity: CallableSyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_activity(
    activity: Callable[..., Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_activity(
    activity: Callable[..., ReturnType],
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for string-name activity
@overload
async def execute_activity(
    activity: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any: ...


async def execute_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start an activity and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_activity`.
    """
    # We call the runtime directly instead of top-level start_activity to ensure
    # we don't miss new parameters
    return await _Runtime.current().workflow_start_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        task_queue=task_queue,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


# Overload for async no-param activity
@overload
def start_activity_class(
    activity: type[CallableAsyncNoParam[ReturnType]],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_activity_class(
    activity: type[CallableSyncNoParam[ReturnType]],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_activity_class(
    activity: type[CallableAsyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_activity_class(
    activity: type[CallableSyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_activity_class(
    activity: type[Callable[..., Awaitable[ReturnType]]],  # type: ignore[reportOverlappingOverload]
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_activity_class(  # type: ignore[reportOverlappingOverload]
    activity: type[Callable[..., ReturnType]],  # type: ignore[reportOverlappingOverload]
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


def start_activity_class(
    activity: type[Callable],  # type: ignore[reportOverlappingOverload]
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]:
    """Start an activity from a callable class.

    See :py:meth:`start_activity` for parameter and return details.
    """
    return _Runtime.current().workflow_start_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        task_queue=task_queue,
        result_type=None,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


# Overload for async no-param activity
@overload
async def execute_activity_class(
    activity: type[CallableAsyncNoParam[ReturnType]],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_activity_class(
    activity: type[CallableSyncNoParam[ReturnType]],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_activity_class(
    activity: type[CallableAsyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_activity_class(
    activity: type[CallableSyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_activity_class(
    activity: type[Callable[..., Awaitable[ReturnType]]],  # type: ignore[reportOverlappingOverload]
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_activity_class(
    activity: type[Callable[..., ReturnType]],  # type: ignore[reportOverlappingOverload]
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


async def execute_activity_class(
    activity: type[Callable],  # type: ignore[reportOverlappingOverload]
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start an activity from a callable class and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_activity_class`.
    """
    return await _Runtime.current().workflow_start_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        task_queue=task_queue,
        result_type=None,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


# Overload for async no-param activity
@overload
def start_activity_method(
    activity: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_activity_method(
    activity: MethodSyncNoParam[SelfType, ReturnType],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_activity_method(
    activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_activity_method(
    activity: MethodSyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


def start_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]:
    """Start an activity from a method.

    See :py:meth:`start_activity` for parameter and return details.
    """
    return _Runtime.current().workflow_start_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        task_queue=task_queue,
        result_type=None,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


# Overload for async no-param activity
@overload
async def execute_activity_method(
    activity: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_activity_method(
    activity: MethodSyncNoParam[SelfType, ReturnType],
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_activity_method(
    activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_activity_method(
    activity: MethodSyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
    *,
    args: Sequence[Any],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


async def execute_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start an activity from a method and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_activity_method`.
    """
    # We call the runtime directly instead of top-level start_activity to ensure
    # we don't miss new parameters
    return await _Runtime.current().workflow_start_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        task_queue=task_queue,
        result_type=None,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


class LocalActivityConfig(TypedDict, total=False):
    """TypedDict of config that can be used for :py:func:`start_local_activity`
    and :py:func:`execute_local_activity`.
    """

    schedule_to_close_timeout: timedelta | None
    schedule_to_start_timeout: timedelta | None
    start_to_close_timeout: timedelta | None
    retry_policy: temporalio.common.RetryPolicy | None
    local_retry_threshold: timedelta | None
    cancellation_type: ActivityCancellationType
    activity_id: str | None
    summary: str | None


# Overload for async no-param activity
@overload
def start_local_activity(
    activity: CallableAsyncNoParam[ReturnType],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_local_activity(
    activity: CallableSyncNoParam[ReturnType],
    *,
    activity_id: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_local_activity(
    activity: CallableAsyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_local_activity(
    activity: CallableSyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_local_activity(
    activity: Callable[..., Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_local_activity(
    activity: Callable[..., ReturnType],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for string-name activity
@overload
def start_local_activity(
    activity: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[Any]: ...


def start_local_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[Any]:
    """Start a local activity and return its handle.

    At least one of ``schedule_to_close_timeout`` or ``start_to_close_timeout``
    must be present.

    Args:
        activity: Activity name or function reference.
        arg: Single argument to the activity.
        args: Multiple arguments to the activity. Cannot be set if arg is.
        result_type: For string activities, this can set the specific result
            type hint to deserialize into.
        schedule_to_close_timeout: Max amount of time the activity can take from
            first being scheduled to being completed before it times out. This
            is inclusive of all retries.
        schedule_to_start_timeout: Max amount of time the activity can take to
            be started from first being scheduled.
        start_to_close_timeout: Max amount of time a single activity run can
            take from when it starts to when it completes. This is per retry.
        retry_policy: How an activity is retried on failure. If unset, an
            SDK-defined default is used. Set maximum attempts to 1 to disable
            retries.
        cancellation_type: How the activity is treated when it is cancelled from
            the workflow.
        activity_id: Optional unique identifier for the activity. This is an
            advanced setting that should not be set unless users are sure they
            need to. Contact Temporal before setting this value.
        summary: Optional summary for the activity.

    Returns:
        An activity handle to the activity which is an async task.
    """
    return _Runtime.current().workflow_start_local_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


# Overload for async no-param activity
@overload
async def execute_local_activity(
    activity: CallableAsyncNoParam[ReturnType],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_local_activity(
    activity: CallableSyncNoParam[ReturnType],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_local_activity(
    activity: CallableAsyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_local_activity(
    activity: CallableSyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_local_activity(
    activity: Callable[..., Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_local_activity(
    activity: Callable[..., ReturnType],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for string-name activity
@overload
async def execute_local_activity(
    activity: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> Any: ...


async def execute_local_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> Any:
    """Start a local activity and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_local_activity`.
    """
    # We call the runtime directly instead of top-level start_local_activity to
    # ensure we don't miss new parameters
    return await _Runtime.current().workflow_start_local_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


# Overload for async no-param activity
@overload
def start_local_activity_class(
    activity: type[CallableAsyncNoParam[ReturnType]],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_local_activity_class(
    activity: type[CallableSyncNoParam[ReturnType]],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_local_activity_class(
    activity: type[CallableAsyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_local_activity_class(
    activity: type[CallableSyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_local_activity_class(
    activity: type[Callable[..., Awaitable[ReturnType]]],  # type: ignore[reportInvalidTypeForm]
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_local_activity_class(  # type: ignore[reportOverlappingOverload]
    activity: type[Callable[..., ReturnType]],  # type: ignore[reportInvalidTypeForm]
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
) -> ActivityHandle[ReturnType]: ...


def start_local_activity_class(
    activity: type[Callable],  # type: ignore[reportInvalidTypeForm]
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[Any]:
    """Start a local activity from a callable class.

    See :py:meth:`start_local_activity` for parameter and return details.
    """
    return _Runtime.current().workflow_start_local_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        result_type=None,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


# Overload for async no-param activity
@overload
async def execute_local_activity_class(
    activity: type[CallableAsyncNoParam[ReturnType]],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_local_activity_class(
    activity: type[CallableSyncNoParam[ReturnType]],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_local_activity_class(
    activity: type[CallableAsyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_local_activity_class(
    activity: type[CallableSyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_local_activity_class(  # type: ignore[reportOverlappingOverload]
    activity: type[Callable[..., Awaitable[ReturnType]]],  # type: ignore[reportInvalidTypeForm]
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_local_activity_class(
    activity: type[Callable[..., ReturnType]],  # type: ignore[reportInvalidTypeForm]
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


async def execute_local_activity_class(
    activity: type[Callable],  # type: ignore[reportInvalidTypeForm]
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> Any:
    """Start a local activity from a callable class and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_local_activity_class`.
    """
    # We call the runtime directly instead of top-level start_local_activity to
    # ensure we don't miss new parameters
    return await _Runtime.current().workflow_start_local_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        result_type=None,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


# Overload for async no-param activity
@overload
def start_local_activity_method(
    activity: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_local_activity_method(
    activity: MethodSyncNoParam[SelfType, ReturnType],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_local_activity_method(
    activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_local_activity_method(
    activity: MethodSyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_local_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_local_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[ReturnType]: ...


def start_local_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ActivityHandle[Any]:
    """Start a local activity from a method.

    See :py:meth:`start_local_activity` for parameter and return details.
    """
    return _Runtime.current().workflow_start_local_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        result_type=None,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )


# Overload for async no-param activity
@overload
async def execute_local_activity_method(
    activity: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_local_activity_method(
    activity: MethodSyncNoParam[SelfType, ReturnType],
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_local_activity_method(
    activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_local_activity_method(
    activity: MethodSyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_local_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_local_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> ReturnType: ...


async def execute_local_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    summary: str | None = None,
) -> Any:
    """Start a local activity from a method and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_local_activity_method`.
    """
    # We call the runtime directly instead of top-level start_local_activity to
    # ensure we don't miss new parameters
    return await _Runtime.current().workflow_start_local_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        result_type=None,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        cancellation_type=cancellation_type,
        activity_id=activity_id,
        summary=summary,
    )

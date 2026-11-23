from __future__ import annotations

import asyncio
import contextvars
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from random import Random
from typing import TYPE_CHECKING, Any, NoReturn, overload

import nexusrpc
from nexusrpc import InputT, OutputT

import temporalio.api.common.v1
import temporalio.common
import temporalio.converter

from ..types import AnyType, ParamType
from ._exceptions import _NotInWorkflowEventLoopError

if TYPE_CHECKING:
    from ._activities import ActivityCancellationType, ActivityHandle
    from ._exceptions import ContinueAsNewVersioningBehavior, VersioningIntent
    from ._nexus import NexusOperationCancellationType, NexusOperationHandle
    from ._workflow_ops import (
        ChildWorkflowCancellationType,
        ChildWorkflowHandle,
        ExternalWorkflowHandle,
        ParentClosePolicy,
    )

__all__ = [
    "Info",
    "ParentInfo",
    "RootInfo",
    "UpdateInfo",
    "cancellation_reason",
    "current_update_info",
    "deprecate_patch",
    "extern_functions",
    "get_current_details",
    "get_last_completion_result",
    "get_last_failure",
    "has_last_completion_result",
    "in_workflow",
    "info",
    "instance",
    "is_failure_exception",
    "memo",
    "memo_value",
    "metric_meter",
    "new_random",
    "now",
    "patched",
    "payload_converter",
    "random",
    "random_seed",
    "register_random_seed_callback",
    "set_current_details",
    "sleep",
    "time",
    "time_ns",
    "upsert_memo",
    "upsert_search_attributes",
    "uuid4",
    "uuid7",
    "wait_condition",
]


@dataclass(frozen=True)
class Info:
    """Information about the running workflow.

    Retrieved inside a workflow via :py:func:`info`. This object is immutable
    with the exception of the :py:attr:`search_attributes` and
    :py:attr:`typed_search_attributes` which is updated on
    :py:func:`upsert_search_attributes`.

    Note, required fields may be added here in future versions. This class
    should never be constructed by users.
    """

    attempt: int
    continued_run_id: str | None
    cron_schedule: str | None
    execution_timeout: timedelta | None
    first_execution_run_id: str
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    namespace: str
    parent: ParentInfo | None
    root: RootInfo | None
    priority: temporalio.common.Priority
    """The priority of this workflow execution. If not set, or this server predates priorities,
    then returns a default instance."""
    raw_memo: Mapping[str, temporalio.api.common.v1.Payload]
    retry_policy: temporalio.common.RetryPolicy | None
    run_id: str
    run_timeout: timedelta | None

    search_attributes: temporalio.common.SearchAttributes
    """Search attributes for the workflow.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """

    start_time: datetime
    """The start time of the first task executed by the workflow."""

    task_queue: str
    task_timeout: timedelta

    typed_search_attributes: temporalio.common.TypedSearchAttributes
    """Search attributes for the workflow.

    Note, this may have invalid values or be missing values if passing the
    deprecated form of dictionary attributes to
    :py:meth:`upsert_search_attributes`.
    """

    workflow_id: str

    workflow_start_time: datetime
    """The start time of the workflow based on the workflow initialization."""

    workflow_type: str

    def _logger_details(self) -> Mapping[str, Any]:
        return {
            # TODO(cretz): worker ID?
            "attempt": self.attempt,
            "namespace": self.namespace,
            "run_id": self.run_id,
            "task_queue": self.task_queue,
            "workflow_id": self.workflow_id,
            "workflow_type": self.workflow_type,
        }

    def get_current_build_id(self) -> str:
        """Get the Build ID of the worker which executed the current Workflow Task.

        May be undefined if the task was completed by a worker without a Build ID. If this worker is
        the one executing this task for the first time and has a Build ID set, then its ID will be
        used. This value may change over the lifetime of the workflow run, but is deterministic and
        safe to use for branching.

        .. deprecated::
            Use get_current_deployment_version instead.
        """
        return _Runtime.current().workflow_get_current_build_id()

    def get_current_deployment_version(
        self,
    ) -> temporalio.common.WorkerDeploymentVersion | None:
        """Get the deployment version of the worker which executed the current Workflow Task.

        May be None if the task was completed by a worker without a deployment version or build
        id. If this worker is the one executing this task for the first time and has a deployment
        version set, then its ID will be used. This value may change over the lifetime of the
        workflow run, but is deterministic and safe to use for branching.
        """
        return _Runtime.current().workflow_get_current_deployment_version()

    def get_current_history_length(self) -> int:
        """Get the current number of events in history.

        Note, this value may not be up to date if accessed inside a query.

        Returns:
            Current number of events in history (up until the current task).
        """
        return _Runtime.current().workflow_get_current_history_length()

    def get_current_history_size(self) -> int:
        """Get the current byte size of history.

        Note, this value may not be up to date if accessed inside a query.

        Returns:
            Current byte-size of history (up until the current task).
        """
        return _Runtime.current().workflow_get_current_history_size()

    def is_continue_as_new_suggested(self) -> bool:
        """Get whether or not continue as new is suggested.

        Note, this value may not be up to date if accessed inside a query.

        Returns:
            True if the server is configured to suggest continue as new and it
            is suggested.
        """
        return _Runtime.current().workflow_is_continue_as_new_suggested()

    def is_target_worker_deployment_version_changed(self) -> bool:
        """Check whether the target worker deployment version has changed.

        Note: Upgrade-on-Continue-as-New is currently experimental.

        Returns:
            True if the target worker deployment version has changed.
        """
        return _Runtime.current().workflow_is_target_worker_deployment_version_changed()


@dataclass(frozen=True)
class ParentInfo:
    """Information about the parent workflow."""

    namespace: str
    run_id: str
    workflow_id: str


@dataclass(frozen=True)
class RootInfo:
    """Information about the root workflow."""

    run_id: str
    workflow_id: str


@dataclass(frozen=True)
class UpdateInfo:
    """Information about a workflow update."""

    id: str
    """Update ID."""

    name: str
    """Update type name."""

    @property
    def _logger_details(self) -> Mapping[str, Any]:
        """Data to be included in string appended to default logging output."""
        return {
            "update_id": self.id,
            "update_name": self.name,
        }


class _Runtime(ABC):
    @staticmethod
    def current() -> _Runtime:
        loop = _Runtime.maybe_current()
        if not loop:
            raise _NotInWorkflowEventLoopError("Not in workflow event loop")
        return loop

    @staticmethod
    def maybe_current() -> _Runtime | None:
        try:
            return getattr(
                asyncio.get_running_loop(), "__temporal_workflow_runtime", None
            )
        except RuntimeError:
            return None

    @staticmethod
    def set_on_loop(loop: asyncio.AbstractEventLoop, runtime: _Runtime | None) -> None:
        if runtime:
            setattr(loop, "__temporal_workflow_runtime", runtime)
        elif hasattr(loop, "__temporal_workflow_runtime"):
            delattr(loop, "__temporal_workflow_runtime")

    def __init__(self) -> None:
        super().__init__()
        self._logger_details: Mapping[str, Any] | None = None

    @property
    def logger_details(self) -> Mapping[str, Any]:
        if self._logger_details is None:
            self._logger_details = self.workflow_info()._logger_details()
        return self._logger_details

    @abstractmethod
    def workflow_all_handlers_finished(self) -> bool: ...

    @abstractmethod
    def workflow_continue_as_new(
        self,
        *args: Any,
        workflow: None | Callable | str,
        task_queue: str | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        backoff_start_interval: timedelta | None,
        retry_policy: temporalio.common.RetryPolicy | None,
        memo: Mapping[str, Any] | None,
        search_attributes: None
        | (
            temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
        ),
        versioning_intent: VersioningIntent | None,
        initial_versioning_behavior: ContinueAsNewVersioningBehavior | None,
    ) -> NoReturn: ...

    @abstractmethod
    def workflow_cancellation_reason(self) -> str | None: ...

    @abstractmethod
    def workflow_extern_functions(self) -> Mapping[str, Callable]: ...

    @abstractmethod
    def workflow_get_current_build_id(self) -> str: ...

    @abstractmethod
    def workflow_get_current_deployment_version(
        self,
    ) -> temporalio.common.WorkerDeploymentVersion | None: ...

    @abstractmethod
    def workflow_get_current_history_length(self) -> int: ...

    @abstractmethod
    def workflow_get_current_history_size(self) -> int: ...

    @abstractmethod
    def workflow_get_external_workflow_handle(
        self, id: str, *, run_id: str | None
    ) -> ExternalWorkflowHandle[Any]: ...

    @abstractmethod
    def workflow_get_query_handler(self, name: str | None) -> Callable | None: ...

    @abstractmethod
    def workflow_get_signal_handler(self, name: str | None) -> Callable | None: ...

    @abstractmethod
    def workflow_get_update_handler(self, name: str | None) -> Callable | None: ...

    @abstractmethod
    def workflow_get_update_validator(self, name: str | None) -> Callable | None: ...

    @abstractmethod
    def workflow_info(self) -> Info: ...

    @abstractmethod
    def workflow_instance(self) -> Any: ...

    @abstractmethod
    def workflow_is_continue_as_new_suggested(self) -> bool: ...

    @abstractmethod
    def workflow_is_target_worker_deployment_version_changed(self) -> bool: ...

    @abstractmethod
    def workflow_is_replaying(self) -> bool: ...

    @abstractmethod
    def workflow_is_replaying_history_events(self) -> bool: ...

    @abstractmethod
    def workflow_is_read_only(self) -> bool: ...

    @abstractmethod
    def workflow_memo(self) -> Mapping[str, Any]: ...

    @abstractmethod
    def workflow_memo_value(
        self, key: str, default: Any, *, type_hint: type | None
    ) -> Any: ...

    @abstractmethod
    def workflow_upsert_memo(self, updates: Mapping[str, Any]) -> None: ...

    @abstractmethod
    def workflow_metric_meter(self) -> temporalio.common.MetricMeter: ...

    @abstractmethod
    def workflow_patch(self, id: str, *, deprecated: bool) -> bool: ...

    @abstractmethod
    def workflow_payload_converter(self) -> temporalio.converter.PayloadConverter: ...

    @abstractmethod
    def workflow_random(self) -> Random: ...

    @abstractmethod
    def workflow_set_query_handler(
        self, name: str | None, handler: Callable | None
    ) -> None: ...

    @abstractmethod
    def workflow_set_signal_handler(
        self, name: str | None, handler: Callable | None
    ) -> None: ...

    @abstractmethod
    def workflow_set_update_handler(
        self,
        name: str | None,
        handler: Callable | None,
        validator: Callable | None,
    ) -> None: ...

    @abstractmethod
    def workflow_start_activity(
        self,
        activity: Any,
        *args: Any,
        task_queue: str | None,
        result_type: type | None,
        schedule_to_close_timeout: timedelta | None,
        schedule_to_start_timeout: timedelta | None,
        start_to_close_timeout: timedelta | None,
        heartbeat_timeout: timedelta | None,
        retry_policy: temporalio.common.RetryPolicy | None,
        cancellation_type: ActivityCancellationType,
        activity_id: str | None,
        versioning_intent: VersioningIntent | None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> ActivityHandle[Any]: ...

    @abstractmethod
    async def workflow_start_child_workflow(
        self,
        workflow: Any,
        *args: Any,
        id: str,
        task_queue: str | None,
        result_type: type | None,
        cancellation_type: ChildWorkflowCancellationType,
        parent_close_policy: ParentClosePolicy,
        execution_timeout: timedelta | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
        retry_policy: temporalio.common.RetryPolicy | None,
        cron_schedule: str,
        memo: Mapping[str, Any] | None,
        search_attributes: None
        | (
            temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
        ),
        versioning_intent: VersioningIntent | None,
        static_summary: str | None = None,
        static_details: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> ChildWorkflowHandle[Any, Any]: ...

    @abstractmethod
    def workflow_start_local_activity(
        self,
        activity: Any,
        *args: Any,
        result_type: type | None,
        schedule_to_close_timeout: timedelta | None,
        schedule_to_start_timeout: timedelta | None,
        start_to_close_timeout: timedelta | None,
        retry_policy: temporalio.common.RetryPolicy | None,
        local_retry_threshold: timedelta | None,
        cancellation_type: ActivityCancellationType,
        activity_id: str | None,
        summary: str | None,
    ) -> ActivityHandle[Any]: ...

    @abstractmethod
    async def workflow_start_nexus_operation(
        self,
        endpoint: str,
        service: str,
        operation: nexusrpc.Operation[InputT, OutputT] | str | Callable[..., Any],
        input: Any,
        output_type: type[OutputT] | None,
        schedule_to_close_timeout: timedelta | None,
        schedule_to_start_timeout: timedelta | None,
        start_to_close_timeout: timedelta | None,
        cancellation_type: NexusOperationCancellationType,
        headers: Mapping[str, str] | None,
        summary: str | None,
    ) -> NexusOperationHandle[OutputT]: ...

    @abstractmethod
    def workflow_time_ns(self) -> int: ...

    @abstractmethod
    def workflow_upsert_search_attributes(
        self,
        attributes: (
            temporalio.common.SearchAttributes
            | Sequence[temporalio.common.SearchAttributeUpdate]
        ),
    ) -> None: ...

    @abstractmethod
    async def workflow_sleep(
        self, duration: float, *, summary: str | None = None
    ) -> None: ...

    @abstractmethod
    async def workflow_wait_condition(
        self,
        fn: Callable[[], bool],
        *,
        timeout: float | None = None,
        timeout_summary: str | None = None,
    ) -> None: ...

    @abstractmethod
    def workflow_get_current_details(self) -> str: ...

    @abstractmethod
    def workflow_set_current_details(self, details: str): ...

    @abstractmethod
    def workflow_is_failure_exception(self, err: BaseException) -> bool: ...

    @abstractmethod
    def workflow_has_last_completion_result(self) -> bool: ...

    @abstractmethod
    def workflow_last_completion_result(self, type_hint: type | None) -> Any | None: ...

    @abstractmethod
    def workflow_last_failure(self) -> BaseException | None: ...

    @abstractmethod
    def workflow_random_seed(self) -> int: ...

    @abstractmethod
    def workflow_register_random_seed_callback(
        self, callback: Callable[[int], None]
    ) -> None: ...


_current_update_info: contextvars.ContextVar[UpdateInfo] = contextvars.ContextVar(
    "__temporal_current_update_info"
)


def _set_current_update_info(info: UpdateInfo) -> None:  # type: ignore[reportUnusedFunction]
    _current_update_info.set(info)


def current_update_info() -> UpdateInfo | None:
    """Info for the current update if any.

    This is powered by :py:mod:`contextvars` so it is only valid within the
    update handler and coroutines/tasks it has started.

    Returns:
        Info for the current update handler the code calling this is executing
            within if any.
    """
    return _current_update_info.get(None)


def deprecate_patch(id: str) -> None:
    """Mark a patch as deprecated.

    This marks a workflow that had :py:func:`patched` in a previous version of
    the code as no longer applicable because all workflows that use the old code
    path are done and will never be queried again. Therefore the old code path
    is removed as well.

    Args:
        id: The identifier originally used with :py:func:`patched`.
    """
    _Runtime.current().workflow_patch(id, deprecated=True)


def extern_functions() -> Mapping[str, Callable]:
    """External functions available in the workflow sandbox.

    Returns:
        Mapping of external functions that can be called from inside a workflow
        sandbox.
    """
    return _Runtime.current().workflow_extern_functions()


def info() -> Info:
    """Current workflow's info.

    Returns:
        Info for the currently running workflow.
    """
    return _Runtime.current().workflow_info()


def instance() -> Any:
    """Current workflow's instance.

    Returns:
        The currently running workflow instance.
    """
    return _Runtime.current().workflow_instance()


def in_workflow() -> bool:
    """Whether the code is currently running in a workflow."""
    return _Runtime.maybe_current() is not None


def cancellation_reason() -> str | None:
    """Reason the workflow was cancelled, or None if no external cancellation
    request has been received.

    A non-None value (including an empty string) indicates that the workflow
    received an explicit cancellation request from the server. This can be used
    when catching an :py:class:`asyncio.CancelledError` to distinguish a
    workflow-level cancel from a cancel that originated from inner asyncio task
    cancellation.

    Note, this only reflects cancellation requested via the server; it is not
    set for cache eviction or for cancels of inner tasks/scopes.

    Returns:
        The reason string sent with the workflow cancellation request (which
        may be empty), or ``None`` if the workflow has not been cancelled via
        an external request.
    """
    return _Runtime.current().workflow_cancellation_reason()


def memo() -> Mapping[str, Any]:
    """Current workflow's memo values, converted without type hints.

    Since type hints are not used, the default converted values will come back.
    For example, if the memo was originally created with a dataclass, the value
    will be a dict. To convert using proper type hints, use
    :py:func:`memo_value`.

    Returns:
        Mapping of all memo keys and they values without type hints.
    """
    return _Runtime.current().workflow_memo()


def is_failure_exception(err: BaseException) -> bool:
    """Checks if the given exception is a workflow failure in the current workflow.

    Returns:
        True if the given exception is a workflow failure in the current workflow.
    """
    return _Runtime.current().workflow_is_failure_exception(err)


@overload
def memo_value(key: str, default: Any = temporalio.common._arg_unset) -> Any: ...


@overload
def memo_value(key: str, *, type_hint: type[ParamType]) -> ParamType: ...


@overload
def memo_value(
    key: str, default: AnyType, *, type_hint: type[ParamType]
) -> AnyType | ParamType: ...


def memo_value(
    key: str,
    default: Any = temporalio.common._arg_unset,
    *,
    type_hint: type | None = None,
) -> Any:
    """Memo value for the given key, optional default, and optional type
    hint.

    Args:
        key: Key to get memo value for.
        default: Default to use if key is not present. If unset, a
            :py:class:`KeyError` is raised when the key does not exist.
        type_hint: Type hint to use when converting.

    Returns:
        Memo value, converted with the type hint if present.

    Raises:
        KeyError: Key not present and default not set.
    """
    return _Runtime.current().workflow_memo_value(key, default, type_hint=type_hint)


def upsert_memo(updates: Mapping[str, Any]) -> None:
    """Adds, modifies, and/or removes memos, with upsert semantics.

    Every memo that has a matching key has its value replaced with the one specified in ``updates``.
    If the value is set to ``None``, the memo is removed instead.
    For every key with no existing memo, a new memo is added with specified value (unless the value is ``None``).
    Memos with keys not included in ``updates`` remain unchanged.
    """
    return _Runtime.current().workflow_upsert_memo(updates)


def get_current_details() -> str:
    """Get the current details of the workflow which may appear in the UI/CLI.
    Unlike static details set at start, this value can be updated throughout
    the life of the workflow and is independent of the static details.
    This can be in Temporal markdown format and can span multiple lines.
    """
    return _Runtime.current().workflow_get_current_details()


def has_last_completion_result() -> bool:
    """Gets whether there is a last completion result of the workflow."""
    return _Runtime.current().workflow_has_last_completion_result()


@overload
def get_last_completion_result() -> Any | None: ...


@overload
def get_last_completion_result(type_hint: type[ParamType]) -> ParamType | None: ...


def get_last_completion_result(type_hint: type | None = None) -> Any | None:
    """Get the result of the last run of the workflow. This will be None if there was
    no previous completion or the result was None. has_last_completion_result()
    can be used to differentiate.
    """
    return _Runtime.current().workflow_last_completion_result(type_hint)


def get_last_failure() -> BaseException | None:
    """Get the last failure of the workflow if it has run previously."""
    return _Runtime.current().workflow_last_failure()


def set_current_details(description: str) -> None:
    """Set the current details of the workflow which may appear in the UI/CLI.
    Unlike static details set at start, this value can be updated throughout
    the life of the workflow and is independent of the static details.
    This can be in Temporal markdown format and can span multiple lines.
    """
    _Runtime.current().workflow_set_current_details(description)


def metric_meter() -> temporalio.common.MetricMeter:
    """Get the metric meter for the current workflow.

    This meter is replay safe which means that metrics will not be recorded
    during replay.

    Returns:
        Current metric meter for this workflow for recording metrics.
    """
    return _Runtime.current().workflow_metric_meter()


def now() -> datetime:
    """Current time from the workflow perspective.

    This is the workflow equivalent of :py:func:`datetime.now` with the
    :py:attr:`timezone.utc` parameter.

    Returns:
        UTC datetime for the current workflow time. The datetime does have UTC
        set as the time zone.
    """
    return datetime.fromtimestamp(time(), timezone.utc)


def patched(id: str) -> bool:
    """Patch a workflow.

    When called, this will only return true if code should take the newer path
    which means this is either not replaying or is replaying and has seen this
    patch before.

    Use :py:func:`deprecate_patch` when all workflows are done and will never be
    queried again. The old code path can be used at that time too.

    Args:
        id: The identifier for this patch. This identifier may be used
            repeatedly in the same workflow to represent the same patch

    Returns:
        True if this should take the newer path, false if it should take the
        older path.
    """
    return _Runtime.current().workflow_patch(id, deprecated=False)


def payload_converter() -> temporalio.converter.PayloadConverter:
    """Get the payload converter for the current workflow.

    The returned converter has :py:class:`temporalio.converter.WorkflowSerializationContext` set.
    This is often used for dynamic workflows/signals/queries to convert
    payloads.
    """
    return _Runtime.current().workflow_payload_converter()


def random() -> Random:
    """Get a deterministic pseudo-random number generator.

    Note, this random number generator is not cryptographically safe and should
    not be used for security purposes.

    Returns:
        The deterministically-seeded pseudo-random number generator.
    """
    return _Runtime.current().workflow_random()


def random_seed() -> int:
    """Get the current random seed value from core.

    This returns the seed value currently being used by the workflow's
    deterministic random number generator.

    Returns:
        The current random seed as an integer.
    """
    return _Runtime.current().workflow_random_seed()


def register_random_seed_callback(callback: Callable[[int], None]) -> None:
    """Register a callback to be notified when the random seed changes.

    The callback will be invoked whenever the workflow receives a new random
    seed from the core. This is useful for maintaining external random number
    generators that need to stay in sync with the workflow's randomness.

    Args:
        callback: Function to be called with the new seed value when it changes.
    """
    return _Runtime.current().workflow_register_random_seed_callback(callback)


def new_random() -> Random:
    """Create a Random instance that automatically reseeds when the workflow seed changes.

    This creates a new Random instance that is initially seeded with the current
    workflow seed, and automatically registers a callback to reseed itself
    whenever the workflow receives a new seed from core.

    Returns:
        A Random instance that stays synchronized with the workflow's randomness.
    """
    current_seed = random_seed()
    auto_random = Random(current_seed)

    def reseed_callback(new_seed: int) -> None:
        auto_random.seed(new_seed)

    register_random_seed_callback(reseed_callback)
    return auto_random


def time() -> float:
    """Current seconds since the epoch from the workflow perspective.

    This is the workflow equivalent of :py:func:`time.time`.

    Returns:
        Seconds since the epoch as a float.
    """
    return time_ns() / 1e9


def time_ns() -> int:
    """Current nanoseconds since the epoch from the workflow perspective.

    This is the workflow equivalent of :py:func:`time.time_ns`.

    Returns:
        Nanoseconds since the epoch
    """
    return _Runtime.current().workflow_time_ns()


def upsert_search_attributes(
    attributes: (
        temporalio.common.SearchAttributes
        | Sequence[temporalio.common.SearchAttributeUpdate]
    ),
) -> None:
    """Upsert search attributes for this workflow.

    Args:
        attributes: The attributes to set. This should be a sequence of
            updates (i.e. values created via value_set and value_unset calls on
            search attribute keys). The dictionary form of attributes is
            DEPRECATED and if used, result in invalid key types on the
            typed_search_attributes property in the info.
    """
    if not attributes:
        return
    temporalio.common._warn_on_deprecated_search_attributes(attributes)
    _Runtime.current().workflow_upsert_search_attributes(attributes)


def uuid4() -> uuid.UUID:
    """Get a new, determinism-safe v4 UUID based on :py:func:`random`.

    Note, this UUID is not cryptographically safe and should not be used for
    security purposes.

    Returns:
        A deterministically-seeded v4 UUID.
    """
    return uuid.UUID(bytes=random().getrandbits(16 * 8).to_bytes(16, "big"), version=4)


def uuid7() -> uuid.UUID:
    """Get a new, determinism-safe v7 UUID based on :py:func:`time_ns` and
    :py:func:`random`.

    Per RFC 9562, the UUID's leading 48 bits are the current workflow time as
    milliseconds since the epoch, so UUIDs from successive workflow tasks sort
    by creation time. The remaining 74 bits are random. UUIDs generated within
    the same workflow task share the same workflow time and are not guaranteed
    to be monotonically ordered with respect to one another.

    Note, this UUID is not cryptographically safe and should not be used for
    security purposes.

    Returns:
        A deterministically-seeded v7 UUID.
    """
    # uuid.UUID's version parameter only accepts 1-5 before Python 3.14, so
    # the version and variant bits are set manually.
    unix_ts_ms = (time_ns() // 1_000_000) & 0xFFFF_FFFF_FFFF
    rand = random()
    rand_a = rand.getrandbits(12)
    rand_b = rand.getrandbits(62)
    return uuid.UUID(
        int=(unix_ts_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    )


async def sleep(duration: float | timedelta, *, summary: str | None = None) -> None:
    """Sleep for the given duration.

    Args:
        duration: Duration to sleep in seconds or as a timedelta.
        summary: A single-line fixed summary for this timer that may appear in UI/CLI.
            This can be in single-line Temporal markdown format.
    """
    await _Runtime.current().workflow_sleep(
        duration=(
            duration.total_seconds() if isinstance(duration, timedelta) else duration
        ),
        summary=summary,
    )


async def wait_condition(
    fn: Callable[[], bool],
    *,
    timeout: timedelta | float | None = None,
    timeout_summary: str | None = None,
) -> None:
    """Wait on a callback to become true.

    This function returns when the callback returns true (invoked each loop
    iteration) or the timeout has been reached.

    Importantly, using `None` value for ``timeout`` means that no Temporal timer is created. This
    means changing from `None` to a positive value (or the inverse) constitutes a nondeterministic
    change to workflow code. Using ``0`` will cause this function to immediately throw a timeout and
    should not be used.

    Args:
        fn: Non-async callback that accepts no parameters and returns a boolean.
        timeout: Optional number of seconds to wait until throwing
            :py:class:`asyncio.TimeoutError`.
        timeout_summary: Optional simple string identifying the timer (created if ``timeout`` is
            present) that may be visible in UI/CLI. While it can be normal text, it is best to treat
            as a timer ID.
    """
    await _Runtime.current().workflow_wait_condition(
        fn,
        timeout=timeout.total_seconds() if isinstance(timeout, timedelta) else timeout,
        timeout_summary=timeout_summary,
    )

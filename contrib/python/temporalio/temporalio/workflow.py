"""Utilities that can decorate or be called inside workflows."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
import threading
import uuid
import warnings
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum, IntEnum
from functools import partial
from random import Random
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import nexusrpc
import nexusrpc.handler
from nexusrpc import InputT, OutputT
from typing_extensions import (
    Concatenate,
    Literal,
    Protocol,
    TypedDict,
    runtime_checkable,
)

import temporalio.api.common.v1
import temporalio.bridge.proto.child_workflow
import temporalio.bridge.proto.workflow_commands
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.nexus
import temporalio.workflow
from temporalio.nexus._util import ServiceHandlerT

from .types import (
    AnyType,
    CallableAsyncNoParam,
    CallableAsyncSingleParam,
    CallableAsyncType,
    CallableSyncNoParam,
    CallableSyncOrAsyncReturnNoneType,
    CallableSyncOrAsyncType,
    CallableSyncSingleParam,
    CallableType,
    ClassType,
    MethodAsyncNoParam,
    MethodAsyncSingleParam,
    MethodSyncNoParam,
    MethodSyncOrAsyncNoParam,
    MethodSyncOrAsyncSingleParam,
    MethodSyncSingleParam,
    MultiParamSpec,
    ParamType,
    ProtocolReturnType,
    ReturnType,
    SelfType,
)


@overload
def defn(cls: ClassType) -> ClassType: ...


@overload
def defn(
    *,
    name: Optional[str] = None,
    sandboxed: bool = True,
    failure_exception_types: Sequence[Type[BaseException]] = [],
    versioning_behavior: temporalio.common.VersioningBehavior = temporalio.common.VersioningBehavior.UNSPECIFIED,
) -> Callable[[ClassType], ClassType]: ...


@overload
def defn(
    *,
    sandboxed: bool = True,
    dynamic: bool = False,
    versioning_behavior: temporalio.common.VersioningBehavior = temporalio.common.VersioningBehavior.UNSPECIFIED,
) -> Callable[[ClassType], ClassType]: ...


def defn(
    cls: Optional[ClassType] = None,
    *,
    name: Optional[str] = None,
    sandboxed: bool = True,
    dynamic: bool = False,
    failure_exception_types: Sequence[Type[BaseException]] = [],
    versioning_behavior: temporalio.common.VersioningBehavior = temporalio.common.VersioningBehavior.UNSPECIFIED,
):
    """Decorator for workflow classes.

    This must be set on any registered workflow class (it is ignored if on a
    base class).

    Args:
        cls: The class to decorate.
        name: Name to use for the workflow. Defaults to class ``__name__``. This
            cannot be set if dynamic is set.
        sandboxed: Whether the workflow should run in a sandbox. Default is
            true.
        dynamic: If true, this activity will be dynamic. Dynamic workflows have
            to accept a single 'Sequence[RawValue]' parameter. This cannot be
            set to true if name is present.
        failure_exception_types: The types of exceptions that, if a
            workflow-thrown exception extends, will cause the workflow/update to
            fail instead of suspending the workflow via task failure. These are
            applied in addition to ones set on the worker constructor. If
            ``Exception`` is set, it effectively will fail a workflow/update in
            all user exception cases. WARNING: This setting is experimental.
        versioning_behavior: Specifies the versioning behavior to use for this workflow.
            WARNING: This setting is experimental.
    """

    def decorator(cls: ClassType) -> ClassType:
        # This performs validation
        _Definition._apply_to_class(
            cls,
            workflow_name=name or cls.__name__ if not dynamic else None,
            sandboxed=sandboxed,
            failure_exception_types=failure_exception_types,
            versioning_behavior=versioning_behavior,
        )
        return cls

    if cls is not None:
        return decorator(cls)
    return decorator


def init(
    init_fn: CallableType,
) -> CallableType:
    """Decorator for the workflow init method.

    This may be used on the __init__ method of the workflow class to specify
    that it accepts the same workflow input arguments as the ``@workflow.run``
    method. If used, the parameters of your  __init__ and ``@workflow.run``
    methods must be identical.

    Args:
        init_fn: The __init__ method to decorate.
    """
    if init_fn.__name__ != "__init__":
        raise ValueError("@workflow.init may only be used on the __init__ method")

    setattr(init_fn, "__temporal_workflow_init", True)
    return init_fn


def run(fn: CallableAsyncType) -> CallableAsyncType:
    """Decorator for the workflow run method.

    This must be used on one and only one async method defined on the same class
    as ``@workflow.defn``. This can be defined on a base class method but must
    then be explicitly overridden and defined on the workflow class.

    Run methods can only have positional parameters. Best practice is to only
    take a single object/dataclass argument that can accept more fields later if
    needed.

    Args:
        fn: The function to decorate.
    """
    if not inspect.iscoroutinefunction(fn):
        raise ValueError("Workflow run method must be an async function")
    # Disallow local classes because we need to have the class globally
    # referenceable by name
    if "<locals>" in fn.__qualname__:
        raise ValueError(
            "Local classes unsupported, @workflow.run cannot be on a local class"
        )
    setattr(fn, "__temporal_workflow_run", True)
    # TODO(cretz): Why is MyPy unhappy with this return?
    return fn  # type: ignore[return-value]


class HandlerUnfinishedPolicy(Enum):
    """Actions taken if a workflow terminates with running handlers.

    Policy defining actions taken when a workflow exits while update or signal handlers are running.
    The workflow exit may be due to successful return, failure, cancellation, or continue-as-new.
    """

    WARN_AND_ABANDON = 1
    """Issue a warning in addition to abandoning."""
    ABANDON = 2
    """Abandon the handler.

    In the case of an update handler this means that the client will receive an error rather than
    the update result."""


class UnfinishedUpdateHandlersWarning(RuntimeWarning):
    """The workflow exited before all update handlers had finished executing."""


class UnfinishedSignalHandlersWarning(RuntimeWarning):
    """The workflow exited before all signal handlers had finished executing."""


@overload
def signal(
    fn: CallableSyncOrAsyncReturnNoneType,
) -> CallableSyncOrAsyncReturnNoneType: ...


@overload
def signal(
    *,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Callable[
    [CallableSyncOrAsyncReturnNoneType], CallableSyncOrAsyncReturnNoneType
]: ...


@overload
def signal(
    *,
    name: str,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Callable[
    [CallableSyncOrAsyncReturnNoneType], CallableSyncOrAsyncReturnNoneType
]: ...


@overload
def signal(
    *,
    dynamic: Literal[True],
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Callable[
    [CallableSyncOrAsyncReturnNoneType], CallableSyncOrAsyncReturnNoneType
]: ...


def signal(
    fn: Optional[CallableSyncOrAsyncReturnNoneType] = None,
    *,
    name: Optional[str] = None,
    dynamic: Optional[bool] = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
):
    """Decorator for a workflow signal method.

    This is used on any async or non-async method that you wish to be called upon
    receiving a signal. If a function overrides one with this decorator, it too
    must be decorated.

    Signal methods can only have positional parameters. Best practice for
    non-dynamic signal methods is to only take a single object/dataclass
    argument that can accept more fields later if needed. Return values from
    signal methods are ignored.

    Args:
        fn: The function to decorate.
        name: Signal name. Defaults to method ``__name__``. Cannot be present
            when ``dynamic`` is present.
        dynamic: If true, this handles all signals not otherwise handled. The
            parameters of the method must be self, a string name, and a
            ``*args`` positional varargs. Cannot be present when ``name`` is
            present.
        unfinished_policy: Actions taken if a workflow terminates with
            a running instance of this handler.
        description: A short description of the signal that may appear in the UI/CLI.
    """

    def decorator(
        name: Optional[str],
        unfinished_policy: HandlerUnfinishedPolicy,
        fn: CallableSyncOrAsyncReturnNoneType,
    ) -> CallableSyncOrAsyncReturnNoneType:
        if not name and not dynamic:
            name = fn.__name__
        defn = _SignalDefinition(
            name=name,
            fn=fn,
            is_method=True,
            unfinished_policy=unfinished_policy,
            description=description,
        )
        setattr(fn, "__temporal_signal_definition", defn)
        if defn.dynamic_vararg:
            warnings.warn(
                "Dynamic signals with vararg third param is deprecated, use Sequence[RawValue]",
                DeprecationWarning,
                stacklevel=2,
            )
        return fn

    if not fn:
        if name is not None and dynamic:
            raise RuntimeError("Cannot provide name and dynamic boolean")
        return partial(decorator, name, unfinished_policy)
    else:
        return decorator(fn.__name__, unfinished_policy, fn)


@overload
def query(fn: CallableType) -> CallableType: ...


@overload
def query(
    *, name: str, description: Optional[str] = None
) -> Callable[[CallableType], CallableType]: ...


@overload
def query(
    *, dynamic: Literal[True], description: Optional[str] = None
) -> Callable[[CallableType], CallableType]: ...


@overload
def query(*, description: str) -> Callable[[CallableType], CallableType]: ...


def query(
    fn: Optional[CallableType] = None,
    *,
    name: Optional[str] = None,
    dynamic: Optional[bool] = False,
    description: Optional[str] = None,
):
    """Decorator for a workflow query method.

    This is used on any non-async method that expects to handle a query. If a
    function overrides one with this decorator, it too must be decorated.

    Query methods can only have positional parameters. Best practice for
    non-dynamic query methods is to only take a single object/dataclass
    argument that can accept more fields later if needed. The return value is
    the resulting query value. Query methods must not mutate any workflow state.

    Args:
        fn: The function to decorate.
        name: Query name. Defaults to method ``__name__``. Cannot be present
            when ``dynamic`` is present.
        dynamic: If true, this handles all queries not otherwise handled. The
            parameters of the method should be self, a string name, and a
            ``Sequence[RawValue]``. An older form of this accepted vararg
            parameters which will now warn. Cannot be present when ``name`` is
            present.
        description: A short description of the query that may appear in the UI/CLI.
    """

    def decorator(
        name: Optional[str],
        description: Optional[str],
        fn: CallableType,
        *,
        bypass_async_check: bool = False,
    ) -> CallableType:
        if not name and not dynamic:
            name = fn.__name__
        if not bypass_async_check and inspect.iscoroutinefunction(fn):
            warnings.warn(
                "Queries as async def functions are deprecated",
                DeprecationWarning,
                stacklevel=2,
            )
        defn = _QueryDefinition(
            name=name, fn=fn, is_method=True, description=description
        )
        setattr(fn, "__temporal_query_definition", defn)
        if defn.dynamic_vararg:
            warnings.warn(
                "Dynamic queries with vararg third param is deprecated, use Sequence[RawValue]",
                DeprecationWarning,
                stacklevel=2,
            )
        return fn

    if name is not None or dynamic or description:
        if name is not None and dynamic:
            raise RuntimeError("Cannot provide name and dynamic boolean")
        return partial(decorator, name, description)
    if fn is None:
        raise RuntimeError("Cannot create query without function or name or dynamic")
    if inspect.iscoroutinefunction(fn):
        warnings.warn(
            "Queries as async def functions are deprecated",
            DeprecationWarning,
            stacklevel=2,
        )
    return decorator(fn.__name__, description, fn, bypass_async_check=True)


@dataclass(frozen=True)
class DynamicWorkflowConfig:
    """Returned by functions using the :py:func:`dynamic_config` decorator, see it for more."""

    failure_exception_types: Optional[Sequence[Type[BaseException]]] = None
    """The types of exceptions that, if a workflow-thrown exception extends, will cause the
    workflow/update to fail instead of suspending the workflow via task failure. These are applied
    in addition to ones set on the worker constructor. If ``Exception`` is set, it effectively will
    fail a workflow/update in all user exception cases.

    Always overrides the equivalent parameter on :py:func:`defn` if set not-None.

        WARNING: This setting is experimental.
    """
    versioning_behavior: temporalio.common.VersioningBehavior = (
        temporalio.common.VersioningBehavior.UNSPECIFIED
    )
    """Specifies the versioning behavior to use for this workflow.

    Always overrides the equivalent parameter on :py:func:`defn`.

        WARNING: This setting is experimental.
    """


def dynamic_config(
    fn: MethodSyncNoParam[SelfType, DynamicWorkflowConfig],
) -> MethodSyncNoParam[SelfType, DynamicWorkflowConfig]:
    """Decorator to allow configuring a dynamic workflow's behavior.

    Because dynamic workflows may conceptually represent more than one workflow type, it may be
    desirable to have different settings for fields that would normally be passed to
    :py:func:`defn`, but vary based on the workflow type name or other information available in
    the workflow's context. This function will be called after the workflow's :py:func:`init`,
    if it has one, but before the workflow's :py:func:`run` method.

    The method must only take self as a parameter, and any values set in the class it returns will
    override those provided to :py:func:`defn`.

    Cannot be specified on non-dynamic workflows.

    Args:
        fn: The function to decorate.
    """
    if inspect.iscoroutinefunction(fn):
        raise ValueError("Workflow dynamic_config method must be synchronous")
    params = list(inspect.signature(fn).parameters.values())
    if len(params) != 1:
        raise ValueError("Workflow dynamic_config method must only take self parameter")

    # Add marker attribute
    setattr(fn, "__temporal_workflow_dynamic_config", True)
    return fn


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
    continued_run_id: Optional[str]
    cron_schedule: Optional[str]
    execution_timeout: Optional[timedelta]
    first_execution_run_id: str
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    namespace: str
    parent: Optional[ParentInfo]
    root: Optional[RootInfo]
    priority: temporalio.common.Priority
    """The priority of this workflow execution. If not set, or this server predates priorities,
    then returns a default instance."""
    raw_memo: Mapping[str, temporalio.api.common.v1.Payload]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    run_id: str
    run_timeout: Optional[timedelta]

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
    ) -> Optional[temporalio.common.WorkerDeploymentVersion]:
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
    def maybe_current() -> Optional[_Runtime]:
        try:
            return getattr(
                asyncio.get_running_loop(), "__temporal_workflow_runtime", None
            )
        except RuntimeError:
            return None

    @staticmethod
    def set_on_loop(
        loop: asyncio.AbstractEventLoop, runtime: Optional[_Runtime]
    ) -> None:
        if runtime:
            setattr(loop, "__temporal_workflow_runtime", runtime)
        elif hasattr(loop, "__temporal_workflow_runtime"):
            delattr(loop, "__temporal_workflow_runtime")

    def __init__(self) -> None:
        super().__init__()
        self._logger_details: Optional[Mapping[str, Any]] = None

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
        workflow: Union[None, Callable, str],
        task_queue: Optional[str],
        run_timeout: Optional[timedelta],
        task_timeout: Optional[timedelta],
        retry_policy: Optional[temporalio.common.RetryPolicy],
        memo: Optional[Mapping[str, Any]],
        search_attributes: Optional[
            Union[
                temporalio.common.SearchAttributes,
                temporalio.common.TypedSearchAttributes,
            ]
        ],
        versioning_intent: Optional[VersioningIntent],
    ) -> NoReturn: ...

    @abstractmethod
    def workflow_extern_functions(self) -> Mapping[str, Callable]: ...

    @abstractmethod
    def workflow_get_current_build_id(self) -> str: ...

    @abstractmethod
    def workflow_get_current_deployment_version(
        self,
    ) -> Optional[temporalio.common.WorkerDeploymentVersion]: ...

    @abstractmethod
    def workflow_get_current_history_length(self) -> int: ...

    @abstractmethod
    def workflow_get_current_history_size(self) -> int: ...

    @abstractmethod
    def workflow_get_external_workflow_handle(
        self, id: str, *, run_id: Optional[str]
    ) -> ExternalWorkflowHandle[Any]: ...

    @abstractmethod
    def workflow_get_query_handler(self, name: Optional[str]) -> Optional[Callable]: ...

    @abstractmethod
    def workflow_get_signal_handler(
        self, name: Optional[str]
    ) -> Optional[Callable]: ...

    @abstractmethod
    def workflow_get_update_handler(
        self, name: Optional[str]
    ) -> Optional[Callable]: ...

    @abstractmethod
    def workflow_get_update_validator(
        self, name: Optional[str]
    ) -> Optional[Callable]: ...

    @abstractmethod
    def workflow_info(self) -> Info: ...

    @abstractmethod
    def workflow_instance(self) -> Any: ...

    @abstractmethod
    def workflow_is_continue_as_new_suggested(self) -> bool: ...

    @abstractmethod
    def workflow_is_replaying(self) -> bool: ...

    @abstractmethod
    def workflow_memo(self) -> Mapping[str, Any]: ...

    @abstractmethod
    def workflow_memo_value(
        self, key: str, default: Any, *, type_hint: Optional[Type]
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
        self, name: Optional[str], handler: Optional[Callable]
    ) -> None: ...

    @abstractmethod
    def workflow_set_signal_handler(
        self, name: Optional[str], handler: Optional[Callable]
    ) -> None: ...

    @abstractmethod
    def workflow_set_update_handler(
        self,
        name: Optional[str],
        handler: Optional[Callable],
        validator: Optional[Callable],
    ) -> None: ...

    @abstractmethod
    def workflow_start_activity(
        self,
        activity: Any,
        *args: Any,
        task_queue: Optional[str],
        result_type: Optional[Type],
        schedule_to_close_timeout: Optional[timedelta],
        schedule_to_start_timeout: Optional[timedelta],
        start_to_close_timeout: Optional[timedelta],
        heartbeat_timeout: Optional[timedelta],
        retry_policy: Optional[temporalio.common.RetryPolicy],
        cancellation_type: ActivityCancellationType,
        activity_id: Optional[str],
        versioning_intent: Optional[VersioningIntent],
        summary: Optional[str] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> ActivityHandle[Any]: ...

    @abstractmethod
    async def workflow_start_child_workflow(
        self,
        workflow: Any,
        *args: Any,
        id: str,
        task_queue: Optional[str],
        result_type: Optional[Type],
        cancellation_type: ChildWorkflowCancellationType,
        parent_close_policy: ParentClosePolicy,
        execution_timeout: Optional[timedelta],
        run_timeout: Optional[timedelta],
        task_timeout: Optional[timedelta],
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
        retry_policy: Optional[temporalio.common.RetryPolicy],
        cron_schedule: str,
        memo: Optional[Mapping[str, Any]],
        search_attributes: Optional[
            Union[
                temporalio.common.SearchAttributes,
                temporalio.common.TypedSearchAttributes,
            ]
        ],
        versioning_intent: Optional[VersioningIntent],
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> ChildWorkflowHandle[Any, Any]: ...

    @abstractmethod
    def workflow_start_local_activity(
        self,
        activity: Any,
        *args: Any,
        result_type: Optional[Type],
        schedule_to_close_timeout: Optional[timedelta],
        schedule_to_start_timeout: Optional[timedelta],
        start_to_close_timeout: Optional[timedelta],
        retry_policy: Optional[temporalio.common.RetryPolicy],
        local_retry_threshold: Optional[timedelta],
        cancellation_type: ActivityCancellationType,
        activity_id: Optional[str],
    ) -> ActivityHandle[Any]: ...

    @abstractmethod
    async def workflow_start_nexus_operation(
        self,
        endpoint: str,
        service: str,
        operation: Union[nexusrpc.Operation[InputT, OutputT], str, Callable[..., Any]],
        input: Any,
        output_type: Optional[Type[OutputT]],
        schedule_to_close_timeout: Optional[timedelta],
        cancellation_type: temporalio.workflow.NexusOperationCancellationType,
        headers: Optional[Mapping[str, str]],
    ) -> NexusOperationHandle[OutputT]: ...

    @abstractmethod
    def workflow_time_ns(self) -> int: ...

    @abstractmethod
    def workflow_upsert_search_attributes(
        self,
        attributes: Union[
            temporalio.common.SearchAttributes,
            Sequence[temporalio.common.SearchAttributeUpdate],
        ],
    ) -> None: ...

    @abstractmethod
    async def workflow_sleep(
        self, duration: float, *, summary: Optional[str] = None
    ) -> None: ...

    @abstractmethod
    async def workflow_wait_condition(
        self,
        fn: Callable[[], bool],
        *,
        timeout: Optional[float] = None,
        timeout_summary: Optional[str] = None,
    ) -> None: ...

    @abstractmethod
    def workflow_get_current_details(self) -> str: ...

    @abstractmethod
    def workflow_set_current_details(self, details: str): ...


_current_update_info: contextvars.ContextVar[UpdateInfo] = contextvars.ContextVar(
    "__temporal_current_update_info"
)


def _set_current_update_info(info: UpdateInfo) -> None:
    _current_update_info.set(info)


def current_update_info() -> Optional[UpdateInfo]:
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


@overload
def memo_value(key: str, default: Any = temporalio.common._arg_unset) -> Any: ...


@overload
def memo_value(key: str, *, type_hint: Type[ParamType]) -> ParamType: ...


@overload
def memo_value(
    key: str, default: AnyType, *, type_hint: Type[ParamType]
) -> Union[AnyType, ParamType]: ...


def memo_value(
    key: str,
    default: Any = temporalio.common._arg_unset,
    *,
    type_hint: Optional[Type] = None,
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
    attributes: Union[
        temporalio.common.SearchAttributes,
        Sequence[temporalio.common.SearchAttributeUpdate],
    ],
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


# Needs to be defined here to avoid a circular import
@runtime_checkable
class UpdateMethodMultiParam(Protocol[MultiParamSpec, ProtocolReturnType]):
    """Decorated workflow update functions implement this."""

    _defn: temporalio.workflow._UpdateDefinition

    def __call__(
        self, *args: MultiParamSpec.args, **kwargs: MultiParamSpec.kwargs
    ) -> Union[ProtocolReturnType, Awaitable[ProtocolReturnType]]:
        """Generic callable type callback."""
        ...

    def validator(
        self, vfunc: Callable[MultiParamSpec, None]
    ) -> Callable[MultiParamSpec, None]:
        """Use to decorate a function to validate the arguments passed to the update handler."""
        ...


@overload
def update(
    fn: Callable[MultiParamSpec, Awaitable[ReturnType]],
) -> UpdateMethodMultiParam[MultiParamSpec, ReturnType]: ...


@overload
def update(
    fn: Callable[MultiParamSpec, ReturnType],
) -> UpdateMethodMultiParam[MultiParamSpec, ReturnType]: ...


@overload
def update(
    *,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Callable[
    [Callable[MultiParamSpec, ReturnType]],
    UpdateMethodMultiParam[MultiParamSpec, ReturnType],
]: ...


@overload
def update(
    *,
    name: str,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Callable[
    [Callable[MultiParamSpec, ReturnType]],
    UpdateMethodMultiParam[MultiParamSpec, ReturnType],
]: ...


@overload
def update(
    *,
    dynamic: Literal[True],
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
) -> Callable[
    [Callable[MultiParamSpec, ReturnType]],
    UpdateMethodMultiParam[MultiParamSpec, ReturnType],
]: ...


def update(
    fn: Optional[CallableSyncOrAsyncType] = None,
    *,
    name: Optional[str] = None,
    dynamic: Optional[bool] = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: Optional[str] = None,
):
    """Decorator for a workflow update handler method.

    This is used on any async or non-async method that you wish to be called upon
    receiving an update. If a function overrides one with this decorator, it too
    must be decorated.

    You may also optionally define a validator method that will be called before
    this handler you have applied this decorator to. You can specify the validator
    with ``@update_handler_function_name.validator``.

    Update methods can only have positional parameters. Best practice for
    non-dynamic update methods is to only take a single object/dataclass
    argument that can accept more fields later if needed. The handler may return
    a serializable value which will be sent back to the caller of the update.

    Args:
        fn: The function to decorate.
        name: Update name. Defaults to method ``__name__``. Cannot be present
            when ``dynamic`` is present.
        dynamic: If true, this handles all updates not otherwise handled. The
            parameters of the method must be self, a string name, and a
            ``*args`` positional varargs. Cannot be present when ``name`` is
            present.
        unfinished_policy: Actions taken if a workflow terminates with
            a running instance of this handler.
        description: A short description of the update that may appear in the UI/CLI.
    """

    def decorator(
        name: Optional[str],
        unfinished_policy: HandlerUnfinishedPolicy,
        fn: CallableSyncOrAsyncType,
    ) -> CallableSyncOrAsyncType:
        if not name and not dynamic:
            name = fn.__name__
        defn = _UpdateDefinition(
            name=name,
            fn=fn,
            is_method=True,
            unfinished_policy=unfinished_policy,
            description=description,
        )
        if defn.dynamic_vararg:
            raise RuntimeError(
                "Dynamic updates do not support a vararg third param, use Sequence[RawValue]",
            )
        setattr(fn, "_defn", defn)
        setattr(fn, "validator", partial(_update_validator, defn))
        return fn

    if not fn:
        if name is not None and dynamic:
            raise RuntimeError("Cannot provide name and dynamic boolean")
        return partial(decorator, name, unfinished_policy)
    else:
        return decorator(fn.__name__, unfinished_policy, fn)


def _update_validator(
    update_def: _UpdateDefinition, fn: Optional[Callable[..., None]] = None
) -> Optional[Callable[..., None]]:
    """Decorator for a workflow update validator method."""
    if fn is not None:
        update_def.set_validator(fn)
    return fn


def uuid4() -> uuid.UUID:
    """Get a new, determinism-safe v4 UUID based on :py:func:`random`.

    Note, this UUID is not cryptographically safe and should not be used for
    security purposes.

    Returns:
        A deterministically-seeded v4 UUID.
    """
    return uuid.UUID(bytes=random().getrandbits(16 * 8).to_bytes(16, "big"), version=4)


async def sleep(
    duration: Union[float, timedelta], *, summary: Optional[str] = None
) -> None:
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
    timeout: Optional[Union[timedelta, float]] = None,
    timeout_summary: Optional[str] = None,
) -> None:
    """Wait on a callback to become true.

    This function returns when the callback returns true (invoked each loop
    iteration) or the timeout has been reached.

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


_sandbox_unrestricted = threading.local()
_in_sandbox = threading.local()
_imports_passed_through = threading.local()


class unsafe:
    """Contains static methods that should not normally be called during
    workflow execution except in advanced cases.
    """

    def __init__(self) -> None:  # noqa: D107
        raise NotImplementedError

    @staticmethod
    def in_sandbox() -> bool:
        """Whether the code is executing on a sandboxed thread.

        Returns:
            True if the code is executing in the sandbox thread.
        """
        return getattr(_in_sandbox, "value", False)

    @staticmethod
    def _set_in_sandbox(v: bool) -> None:
        _in_sandbox.value = v

    @staticmethod
    def is_replaying() -> bool:
        """Whether the workflow is currently replaying.

        Returns:
            True if the workflow is currently replaying
        """
        return _Runtime.current().workflow_is_replaying()

    @staticmethod
    def is_sandbox_unrestricted() -> bool:
        """Whether the current block of code is not restricted via sandbox.

        Returns:
            True if the current code is not restricted in the sandbox.
        """
        # Activations happen in different threads than init and possibly the
        # local hasn't been initialized in _that_ thread, so we allow unset here
        # instead of just setting value = False globally.
        return getattr(_sandbox_unrestricted, "value", False)

    @staticmethod
    @contextmanager
    def sandbox_unrestricted() -> Iterator[None]:
        """A context manager to run code without sandbox restrictions."""
        # Only apply if not already applied. Nested calls just continue
        # unrestricted.
        if unsafe.is_sandbox_unrestricted():
            yield None
            return
        _sandbox_unrestricted.value = True
        try:
            yield None
        finally:
            _sandbox_unrestricted.value = False

    @staticmethod
    def is_imports_passed_through() -> bool:
        """Whether the current block of code is in
        :py:meth:imports_passed_through.

        Returns:
            True if the current code's imports will be passed through
        """
        # See comment in is_sandbox_unrestricted for why we allow unset instead
        # of just global false.
        return getattr(_imports_passed_through, "value", False)

    @staticmethod
    @contextmanager
    def imports_passed_through() -> Iterator[None]:
        """Context manager to mark all imports that occur within it as passed
        through (meaning not reloaded by the sandbox).
        """
        # Only apply if not already applied. Nested calls just continue
        # passed through.
        if unsafe.is_imports_passed_through():
            yield None
            return
        _imports_passed_through.value = True
        try:
            yield None
        finally:
            _imports_passed_through.value = False


class LoggerAdapter(logging.LoggerAdapter):
    """Adapter that adds details to the log about the running workflow.

    Attributes:
        workflow_info_on_message: Boolean for whether a string representation of
            a dict of some workflow info will be appended to each message.
            Default is True.
        workflow_info_on_extra: Boolean for whether a ``temporal_workflow``
            dictionary value will be added to the ``extra`` dictionary with some
            workflow info, making it present on the ``LogRecord.__dict__`` for
            use by others. Default is True.
        full_workflow_info_on_extra: Boolean for whether a ``workflow_info``
            value will be added to the ``extra`` dictionary with the entire
            workflow info, making it present on the ``LogRecord.__dict__`` for
            use by others. Default is False.
        log_during_replay: Boolean for whether logs should occur during replay.
            Default is False.

    Values added to ``extra`` are merged with the ``extra`` dictionary from a
    logging call, with values from the logging call taking precedence. I.e. the
    behavior is that of ``merge_extra=True`` in Python >= 3.13.
    """

    def __init__(
        self, logger: logging.Logger, extra: Optional[Mapping[str, Any]]
    ) -> None:
        """Create the logger adapter."""
        super().__init__(logger, extra or {})
        self.workflow_info_on_message = True
        self.workflow_info_on_extra = True
        self.full_workflow_info_on_extra = False
        self.log_during_replay = False

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> Tuple[Any, MutableMapping[str, Any]]:
        """Override to add workflow details."""
        extra: Dict[str, Any] = {}
        msg_extra: Dict[str, Any] = {}

        if (
            self.workflow_info_on_message
            or self.workflow_info_on_extra
            or self.full_workflow_info_on_extra
        ):
            runtime = _Runtime.maybe_current()
            if runtime:
                workflow_details = runtime.logger_details
                if self.workflow_info_on_message:
                    msg_extra.update(workflow_details)
                if self.workflow_info_on_extra:
                    extra["temporal_workflow"] = workflow_details
                if self.full_workflow_info_on_extra:
                    extra["workflow_info"] = runtime.workflow_info()
            update_info = current_update_info()
            if update_info:
                update_details = update_info._logger_details
                if self.workflow_info_on_message:
                    msg_extra.update(update_details)
                if self.workflow_info_on_extra:
                    extra.setdefault("temporal_workflow", {}).update(update_details)

        kwargs["extra"] = {**extra, **(kwargs.get("extra") or {})}
        if msg_extra:
            msg = f"{msg} ({msg_extra})"
        return (msg, kwargs)

    def isEnabledFor(self, level: int) -> bool:
        """Override to ignore replay logs."""
        if not self.log_during_replay and unsafe.is_replaying():
            return False
        return super().isEnabledFor(level)

    @property
    def base_logger(self) -> logging.Logger:
        """Underlying logger usable for actions such as adding
        handlers/formatters.
        """
        return self.logger


logger = LoggerAdapter(logging.getLogger(__name__), None)
"""Logger that will have contextual workflow details embedded.

Logs are skipped during replay by default.
"""


@dataclass(frozen=True)
class _Definition:
    name: Optional[str]
    cls: Type
    run_fn: Callable[..., Awaitable]
    signals: Mapping[Optional[str], _SignalDefinition]
    queries: Mapping[Optional[str], _QueryDefinition]
    updates: Mapping[Optional[str], _UpdateDefinition]
    sandboxed: bool
    failure_exception_types: Sequence[Type[BaseException]]
    # Types loaded on post init if both are None
    arg_types: Optional[List[Type]] = None
    ret_type: Optional[Type] = None
    versioning_behavior: Optional[temporalio.common.VersioningBehavior] = None
    dynamic_config_fn: Optional[Callable[..., DynamicWorkflowConfig]] = None

    @staticmethod
    def from_class(cls: Type) -> Optional[_Definition]:
        # We make sure to only return it if it's on _this_ class
        defn = getattr(cls, "__temporal_workflow_definition", None)
        if defn and defn.cls == cls:
            return defn
        return None

    @staticmethod
    def must_from_class(cls: Type) -> _Definition:
        ret = _Definition.from_class(cls)
        if ret:
            return ret
        cls_name = getattr(cls, "__name__", "<unknown>")
        raise ValueError(
            f"Workflow {cls_name} missing attributes, was it decorated with @workflow.defn?"
        )

    @staticmethod
    def from_run_fn(fn: Callable[..., Awaitable[Any]]) -> Optional[_Definition]:
        return getattr(fn, "__temporal_workflow_definition", None)

    @staticmethod
    def must_from_run_fn(fn: Callable[..., Awaitable[Any]]) -> _Definition:
        ret = _Definition.from_run_fn(fn)
        if ret:
            return ret
        fn_name = getattr(fn, "__qualname__", "<unknown>")
        raise ValueError(
            f"Function {fn_name} missing attributes, was it decorated with @workflow.run and was its class decorated with @workflow.defn?"
        )

    @classmethod
    def get_name_and_result_type(
        cls, name_or_run_fn: Union[str, Callable[..., Awaitable[Any]]]
    ) -> Tuple[str, Optional[Type]]:
        if isinstance(name_or_run_fn, str):
            return name_or_run_fn, None
        elif callable(name_or_run_fn):
            defn = cls.must_from_run_fn(name_or_run_fn)
            if not defn.name:
                raise ValueError("Cannot invoke dynamic workflow explicitly")
            return defn.name, defn.ret_type
        else:
            raise TypeError("Workflow must be a string or callable")

    @staticmethod
    def _apply_to_class(
        cls: Type,
        *,
        workflow_name: Optional[str],
        sandboxed: bool,
        failure_exception_types: Sequence[Type[BaseException]],
        versioning_behavior: temporalio.common.VersioningBehavior,
    ) -> None:
        # Check it's not being doubly applied
        if _Definition.from_class(cls):
            raise ValueError("Class already contains workflow definition")
        issues: List[str] = []

        # Collect run fn and all signal/query/update fns
        init_fn: Optional[Callable[..., None]] = None
        run_fn: Optional[Callable[..., Awaitable[Any]]] = None
        dynamic_config_fn: Optional[Callable[..., DynamicWorkflowConfig]] = None
        seen_run_attr = False
        signals: Dict[Optional[str], _SignalDefinition] = {}
        queries: Dict[Optional[str], _QueryDefinition] = {}
        updates: Dict[Optional[str], _UpdateDefinition] = {}
        for name, member in inspect.getmembers(cls):
            if hasattr(member, "__temporal_workflow_run"):
                seen_run_attr = True
                if not _is_unbound_method_on_cls(member, cls):
                    issues.append(
                        f"@workflow.run method {name} must be defined on {cls.__qualname__}"
                    )
                elif run_fn is not None:
                    issues.append(
                        f"Multiple @workflow.run methods found (at least on {name} and {run_fn.__name__})"
                    )
                else:
                    # We can guarantee the @workflow.run decorator did
                    # validation of the function itself
                    run_fn = member
            elif hasattr(member, "__temporal_signal_definition"):
                signal_defn = cast(
                    _SignalDefinition, getattr(member, "__temporal_signal_definition")
                )
                if signal_defn.name in signals:
                    defn_name = signal_defn.name or "<dynamic>"
                    # TODO(cretz): Remove cast when https://github.com/python/mypy/issues/5485 fixed
                    other_fn = cast(Callable, signals[signal_defn.name].fn)
                    issues.append(
                        f"Multiple signal methods found for {defn_name} "
                        f"(at least on {name} and {other_fn.__name__})"
                    )
                else:
                    signals[signal_defn.name] = signal_defn
            elif hasattr(member, "__temporal_query_definition"):
                query_defn = cast(
                    _QueryDefinition, getattr(member, "__temporal_query_definition")
                )
                if query_defn.name in queries:
                    defn_name = query_defn.name or "<dynamic>"
                    issues.append(
                        f"Multiple query methods found for {defn_name} "
                        f"(at least on {name} and {queries[query_defn.name].fn.__name__})"
                    )
                else:
                    queries[query_defn.name] = query_defn
            elif name == "__init__" and hasattr(member, "__temporal_workflow_init"):
                init_fn = member
            elif hasattr(member, "__temporal_workflow_dynamic_config"):
                if workflow_name:
                    issues.append(
                        "@workflow.dynamic_config can only be used in dynamic workflows, but "
                        f"workflow class {workflow_name} ({cls.__name__}) is not dynamic"
                    )
                if dynamic_config_fn:
                    issues.append(
                        "@workflow.dynamic_config can only be defined once per workflow"
                    )
                dynamic_config_fn = member
            elif isinstance(member, UpdateMethodMultiParam):
                update_defn = member._defn
                if update_defn.name in updates:
                    defn_name = update_defn.name or "<dynamic>"
                    issues.append(
                        f"Multiple update methods found for {defn_name} "
                        f"(at least on {name} and {updates[update_defn.name].fn.__name__})"
                    )
                elif update_defn.validator and not _parameters_identical_up_to_naming(
                    update_defn.fn, update_defn.validator
                ):
                    issues.append(
                        f"Update validator method {update_defn.validator.__name__} parameters "
                        f"do not match update method {update_defn.fn.__name__} parameters"
                    )
                else:
                    updates[update_defn.name] = update_defn

        # Check base classes haven't defined things with different decorators
        for base_cls in inspect.getmro(cls)[1:]:
            for _, base_member in inspect.getmembers(base_cls):
                # We only care about methods defined on this class
                if not inspect.isfunction(base_member) or not _is_unbound_method_on_cls(
                    base_member, base_cls
                ):
                    continue
                if hasattr(base_member, "__temporal_workflow_run"):
                    seen_run_attr = True
                    if not run_fn or base_member.__name__ != run_fn.__name__:
                        issues.append(
                            f"@workflow.run defined on {base_member.__qualname__} but not on the override"
                        )
                elif hasattr(base_member, "__temporal_signal_definition"):
                    signal_defn = cast(
                        _SignalDefinition,
                        getattr(base_member, "__temporal_signal_definition"),
                    )
                    if signal_defn.name not in signals:
                        issues.append(
                            f"@workflow.signal defined on {base_member.__qualname__} but not on the override"
                        )
                elif hasattr(base_member, "__temporal_query_definition"):
                    query_defn = cast(
                        _QueryDefinition,
                        getattr(base_member, "__temporal_query_definition"),
                    )
                    if query_defn.name not in queries:
                        issues.append(
                            f"@workflow.query defined on {base_member.__qualname__} but not on the override"
                        )
                elif isinstance(base_member, UpdateMethodMultiParam):
                    update_defn = base_member._defn
                    if update_defn.name not in updates:
                        issues.append(
                            f"@workflow.update defined on {base_member.__qualname__} but not on the override"
                        )

        if not seen_run_attr:
            issues.append("Missing @workflow.run method")
        if init_fn and run_fn:
            if not _parameters_identical_up_to_naming(init_fn, run_fn):
                issues.append(
                    "@workflow.init and @workflow.run method parameters do not match"
                )
        if issues:
            if len(issues) == 1:
                raise ValueError(f"Invalid workflow class: {issues[0]}")
            raise ValueError(
                f"Invalid workflow class for {len(issues)} reasons: {', '.join(issues)}"
            )

        assert run_fn
        assert seen_run_attr
        defn = _Definition(
            name=workflow_name,
            cls=cls,
            run_fn=run_fn,
            signals=signals,
            queries=queries,
            updates=updates,
            sandboxed=sandboxed,
            failure_exception_types=failure_exception_types,
            versioning_behavior=versioning_behavior,
            dynamic_config_fn=dynamic_config_fn,
        )
        setattr(cls, "__temporal_workflow_definition", defn)
        setattr(run_fn, "__temporal_workflow_definition", defn)

    def __post_init__(self) -> None:
        if self.arg_types is None and self.ret_type is None:
            dynamic = self.name is None
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.run_fn)
            # If dynamic, must be a sequence of raw values
            if dynamic and (
                not arg_types
                or len(arg_types) != 1
                or arg_types[0] != Sequence[temporalio.common.RawValue]
            ):
                raise TypeError(
                    "Dynamic workflow must accept a single Sequence[temporalio.common.RawValue]"
                )
            object.__setattr__(self, "arg_types", arg_types)
            object.__setattr__(self, "ret_type", ret_type)


def _parameters_identical_up_to_naming(fn1: Callable, fn2: Callable) -> bool:
    """Return True if the functions have identical parameter lists, ignoring parameter names."""

    def params(fn: Callable) -> List[inspect.Parameter]:
        # Ignore name when comparing parameters (remaining fields are kind,
        # default, and annotation).
        return [p.replace(name="x") for p in inspect.signature(fn).parameters.values()]

    # We require that any type annotations present match exactly; i.e. we do
    # not support any notion of subtype compatibility.
    return params(fn1) == params(fn2)


# Async safe version of partial
def _bind_method(obj: Any, fn: Callable[..., Any]) -> Callable[..., Any]:
    # Curry instance on the definition function since that represents an
    # unbound method
    if inspect.iscoroutinefunction(fn):
        # We cannot use functools.partial here because in <= 3.7 that isn't
        # considered an inspect.iscoroutinefunction
        fn = cast(Callable[..., Awaitable[Any]], fn)

        async def with_object(*args, **kwargs) -> Any:
            return await fn(obj, *args, **kwargs)

        return with_object
    return partial(fn, obj)


# Returns true if normal form, false if vararg form
def _assert_dynamic_handler_args(
    fn: Callable, arg_types: Optional[List[Type]], is_method: bool
) -> bool:
    # Dynamic query/signal/update must have three args: self, name, and
    # Sequence[RawValue]. An older form accepted varargs for the third param for signals/queries so
    # we will too (but will warn in the signal/query code).
    params = list(inspect.signature(fn).parameters.values())
    total_expected_params = 3 if is_method else 2
    if (
        len(params) == total_expected_params
        and params[-2].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        and params[-1].kind is inspect.Parameter.VAR_POSITIONAL
    ):
        # Old var-arg form
        return False
    if (
        not arg_types
        or len(arg_types) != 2
        or arg_types[0] != str
        or arg_types[1] != Sequence[temporalio.common.RawValue]
    ):
        raise RuntimeError(
            "Dynamic handler must have 3 arguments: self, str, and Sequence[temporalio.common.RawValue]"
        )
    return True


@dataclass(frozen=True)
class _SignalDefinition:
    # None if dynamic
    name: Optional[str]
    fn: Callable[..., Union[None, Awaitable[None]]]
    is_method: bool
    unfinished_policy: HandlerUnfinishedPolicy = (
        HandlerUnfinishedPolicy.WARN_AND_ABANDON
    )
    description: Optional[str] = None
    # Types loaded on post init if None
    arg_types: Optional[List[Type]] = None
    dynamic_vararg: bool = False

    @staticmethod
    def from_fn(fn: Callable) -> Optional[_SignalDefinition]:
        return getattr(fn, "__temporal_signal_definition", None)

    @staticmethod
    def must_name_from_fn_or_str(signal: Union[str, Callable]) -> str:
        if callable(signal):
            defn = _SignalDefinition.from_fn(signal)
            if not defn:
                raise RuntimeError(
                    f"Signal definition not found on {signal.__qualname__}, "
                    "is it decorated with @workflow.signal?"
                )
            elif not defn.name:
                raise RuntimeError("Cannot invoke dynamic signal definition")
            # TODO(cretz): Check count/type of args at runtime?
            return defn.name
        return str(signal)

    def __post_init__(self) -> None:
        if self.arg_types is None:
            arg_types, _ = temporalio.common._type_hints_from_func(self.fn)
            # If dynamic, assert it
            if not self.name:
                object.__setattr__(
                    self,
                    "dynamic_vararg",
                    not _assert_dynamic_handler_args(
                        self.fn, arg_types, self.is_method
                    ),
                )
            object.__setattr__(self, "arg_types", arg_types)

    def bind_fn(self, obj: Any) -> Callable[..., Any]:
        return _bind_method(obj, self.fn)


@dataclass(frozen=True)
class _QueryDefinition:
    # None if dynamic
    name: Optional[str]
    fn: Callable[..., Any]
    is_method: bool
    description: Optional[str] = None
    # Types loaded on post init if both are None
    arg_types: Optional[List[Type]] = None
    ret_type: Optional[Type] = None
    dynamic_vararg: bool = False

    @staticmethod
    def from_fn(fn: Callable) -> Optional[_QueryDefinition]:
        return getattr(fn, "__temporal_query_definition", None)

    def __post_init__(self) -> None:
        if self.arg_types is None and self.ret_type is None:
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.fn)
            # If dynamic, assert it
            if not self.name:
                object.__setattr__(
                    self,
                    "dynamic_vararg",
                    not _assert_dynamic_handler_args(
                        self.fn, arg_types, self.is_method
                    ),
                )
            object.__setattr__(self, "arg_types", arg_types)
            object.__setattr__(self, "ret_type", ret_type)

    def bind_fn(self, obj: Any) -> Callable[..., Any]:
        return _bind_method(obj, self.fn)


@dataclass(frozen=True)
class _UpdateDefinition:
    # None if dynamic
    name: Optional[str]
    fn: Callable[..., Union[Any, Awaitable[Any]]]
    is_method: bool
    unfinished_policy: HandlerUnfinishedPolicy = (
        HandlerUnfinishedPolicy.WARN_AND_ABANDON
    )
    description: Optional[str] = None
    # Types loaded on post init if None
    arg_types: Optional[List[Type]] = None
    ret_type: Optional[Type] = None
    validator: Optional[Callable[..., None]] = None
    dynamic_vararg: bool = False

    def __post_init__(self) -> None:
        if self.arg_types is None:
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.fn)
            # Disallow dynamic varargs
            if not self.name and not _assert_dynamic_handler_args(
                self.fn, arg_types, self.is_method
            ):
                raise RuntimeError(
                    "Dynamic updates do not support a vararg third param, use Sequence[RawValue]",
                )
            object.__setattr__(self, "arg_types", arg_types)
            object.__setattr__(self, "ret_type", ret_type)

    def bind_fn(self, obj: Any) -> Callable[..., Any]:
        return _bind_method(obj, self.fn)

    def bind_validator(self, obj: Any) -> Callable[..., Any]:
        if self.validator is not None:
            return _bind_method(obj, self.validator)
        return lambda *args, **kwargs: None

    def set_validator(self, validator: Callable[..., None]) -> None:
        if self.validator:
            raise RuntimeError(f"Validator already set for update {self.name}")
        object.__setattr__(self, "validator", validator)

    @classmethod
    def get_name_and_result_type(
        cls,
        name_or_update_fn: Union[str, Callable[..., Any]],
    ) -> Tuple[str, Optional[Type]]:
        if isinstance(name_or_update_fn, temporalio.workflow.UpdateMethodMultiParam):
            defn = name_or_update_fn._defn
            if not defn.name:
                raise RuntimeError("Cannot invoke dynamic update definition")
            # TODO(cretz): Check count/type of args at runtime?
            return defn.name, defn.ret_type
        else:
            return str(name_or_update_fn), None


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

    task_queue: Optional[str]
    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    heartbeat_timeout: Optional[timedelta]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    cancellation_type: ActivityCancellationType
    activity_id: Optional[str]
    versioning_intent: Optional[VersioningIntent]
    summary: Optional[str]
    priority: temporalio.common.Priority


# Overload for async no-param activity
@overload
def start_activity(
    activity: CallableAsyncNoParam[ReturnType],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_activity(
    activity: CallableSyncNoParam[ReturnType],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_activity(
    activity: CallableAsyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_activity(
    activity: CallableSyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_activity(
    activity: Callable[..., Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_activity(
    activity: Callable[..., ReturnType],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for string-name activity
@overload
def start_activity(
    activity: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    result_type: Optional[Type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]: ...


def start_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    result_type: Optional[Type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
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
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_activity(
    activity: CallableSyncNoParam[ReturnType],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_activity(
    activity: CallableAsyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_activity(
    activity: CallableSyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_activity(
    activity: Callable[..., Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_activity(
    activity: Callable[..., ReturnType],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for string-name activity
@overload
async def execute_activity(
    activity: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    result_type: Optional[Type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any: ...


async def execute_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    result_type: Optional[Type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
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
    activity: Type[CallableAsyncNoParam[ReturnType]],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_activity_class(
    activity: Type[CallableSyncNoParam[ReturnType]],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_activity_class(
    activity: Type[CallableAsyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_activity_class(
    activity: Type[CallableSyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_activity_class(
    activity: Type[Callable[..., Awaitable[ReturnType]]],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_activity_class(
    activity: Type[Callable[..., ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


def start_activity_class(
    activity: Type[Callable],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
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
    activity: Type[CallableAsyncNoParam[ReturnType]],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_activity_class(
    activity: Type[CallableSyncNoParam[ReturnType]],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_activity_class(
    activity: Type[CallableAsyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_activity_class(
    activity: Type[CallableSyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_activity_class(
    activity: Type[Callable[..., Awaitable[ReturnType]]],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_activity_class(
    activity: Type[Callable[..., ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


async def execute_activity_class(
    activity: Type[Callable],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
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
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_activity_method(
    activity: MethodSyncNoParam[SelfType, ReturnType],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_activity_method(
    activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_activity_method(
    activity: MethodSyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[ReturnType]: ...


def start_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
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
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_activity_method(
    activity: MethodSyncNoParam[SelfType, ReturnType],
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_activity_method(
    activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_activity_method(
    activity: MethodSyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
    *,
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


async def execute_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    summary: Optional[str] = None,
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

    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    local_retry_threshold: Optional[timedelta]
    cancellation_type: ActivityCancellationType
    activity_id: Optional[str]


# Overload for async no-param activity
@overload
def start_local_activity(
    activity: CallableAsyncNoParam[ReturnType],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_local_activity(
    activity: CallableSyncNoParam[ReturnType],
    *,
    activity_id: Optional[str] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_local_activity(
    activity: CallableAsyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_local_activity(
    activity: CallableSyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_local_activity(
    activity: Callable[..., Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_local_activity(
    activity: Callable[..., ReturnType],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for string-name activity
@overload
def start_local_activity(
    activity: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: Optional[Type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[Any]: ...


def start_local_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: Optional[Type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
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
    )


# Overload for async no-param activity
@overload
async def execute_local_activity(
    activity: CallableAsyncNoParam[ReturnType],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_local_activity(
    activity: CallableSyncNoParam[ReturnType],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_local_activity(
    activity: CallableAsyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_local_activity(
    activity: CallableSyncSingleParam[ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_local_activity(
    activity: Callable[..., Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_local_activity(
    activity: Callable[..., ReturnType],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for string-name activity
@overload
async def execute_local_activity(
    activity: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: Optional[Type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> Any: ...


async def execute_local_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: Optional[Type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
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
    )


# Overload for async no-param activity
@overload
def start_local_activity_class(
    activity: Type[CallableAsyncNoParam[ReturnType]],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_local_activity_class(
    activity: Type[CallableSyncNoParam[ReturnType]],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_local_activity_class(
    activity: Type[CallableAsyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_local_activity_class(
    activity: Type[CallableSyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_local_activity_class(
    activity: Type[Callable[..., Awaitable[ReturnType]]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_local_activity_class(
    activity: Type[Callable[..., ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


def start_local_activity_class(
    activity: Type[Callable],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
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
    )


# Overload for async no-param activity
@overload
async def execute_local_activity_class(
    activity: Type[CallableAsyncNoParam[ReturnType]],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_local_activity_class(
    activity: Type[CallableSyncNoParam[ReturnType]],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_local_activity_class(
    activity: Type[CallableAsyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_local_activity_class(
    activity: Type[CallableSyncSingleParam[ParamType, ReturnType]],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_local_activity_class(
    activity: Type[Callable[..., Awaitable[ReturnType]]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_local_activity_class(
    activity: Type[Callable[..., ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


async def execute_local_activity_class(
    activity: Type[Callable],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
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
    )


# Overload for async no-param activity
@overload
def start_local_activity_method(
    activity: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync no-param activity
@overload
def start_local_activity_method(
    activity: MethodSyncNoParam[SelfType, ReturnType],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async single-param activity
@overload
def start_local_activity_method(
    activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync single-param activity
@overload
def start_local_activity_method(
    activity: MethodSyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for async multi-param activity
@overload
def start_local_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


# Overload for sync multi-param activity
@overload
def start_local_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]: ...


def start_local_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
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
    )


# Overload for async no-param activity
@overload
async def execute_local_activity_method(
    activity: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync no-param activity
@overload
async def execute_local_activity_method(
    activity: MethodSyncNoParam[SelfType, ReturnType],
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for async single-param activity
@overload
async def execute_local_activity_method(
    activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync single-param activity
@overload
async def execute_local_activity_method(
    activity: MethodSyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for async multi-param activity
@overload
async def execute_local_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


# Overload for sync multi-param activity
@overload
async def execute_local_activity_method(
    activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
    *,
    args: Sequence[Any],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
) -> ReturnType: ...


async def execute_local_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    local_retry_threshold: Optional[timedelta] = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: Optional[str] = None,
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
    )


class ChildWorkflowHandle(_AsyncioTask[ReturnType], Generic[SelfType, ReturnType]):  # type: ignore[type-var]
    """Handle for interacting with a child workflow.

    This is created via :py:func:`start_child_workflow`.

    This extends :py:class:`asyncio.Task` and supports all task features.
    """

    @property
    def id(self) -> str:
        """ID for the workflow."""
        raise NotImplementedError

    @property
    def first_execution_run_id(self) -> Optional[str]:
        """Run ID for the workflow."""
        raise NotImplementedError

    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncNoParam[SelfType, None],
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncSingleParam[SelfType, ParamType, None],
        arg: ParamType,
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: Callable[
            Concatenate[SelfType, MultiParamSpec], Union[Awaitable[None], None]
        ],
        *,
        args: Sequence[Any],
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None: ...

    async def signal(
        self,
        signal: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None:
        """Signal this child workflow.

        Args:
            signal: Name or method reference for the signal.
            arg: Single argument to the signal.
            args: Multiple arguments to the signal. Cannot be set if arg is.

        """
        raise NotImplementedError


class ChildWorkflowCancellationType(IntEnum):
    """How a child workflow cancellation should be handled."""

    ABANDON = int(
        temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.ABANDON
    )
    TRY_CANCEL = int(
        temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.TRY_CANCEL
    )
    WAIT_CANCELLATION_COMPLETED = int(
        temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED
    )
    WAIT_CANCELLATION_REQUESTED = int(
        temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_REQUESTED
    )


class ParentClosePolicy(IntEnum):
    """How a child workflow should be handled when the parent closes."""

    UNSPECIFIED = int(
        temporalio.bridge.proto.child_workflow.ParentClosePolicy.PARENT_CLOSE_POLICY_UNSPECIFIED
    )
    TERMINATE = int(
        temporalio.bridge.proto.child_workflow.ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE
    )
    ABANDON = int(
        temporalio.bridge.proto.child_workflow.ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON
    )
    REQUEST_CANCEL = int(
        temporalio.bridge.proto.child_workflow.ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL
    )


class ChildWorkflowConfig(TypedDict, total=False):
    """TypedDict of config that can be used for :py:func:`start_child_workflow`
    and :py:func:`execute_child_workflow`.
    """

    id: Optional[str]
    task_queue: Optional[str]
    cancellation_type: ChildWorkflowCancellationType
    parent_close_policy: ParentClosePolicy
    execution_timeout: Optional[timedelta]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy
    retry_policy: Optional[temporalio.common.RetryPolicy]
    cron_schedule: str
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ]
    versioning_intent: Optional[VersioningIntent]
    static_summary: Optional[str]
    static_details: Optional[str]
    priority: temporalio.common.Priority


# Overload for no-param workflow
@overload
async def start_child_workflow(
    workflow: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[SelfType, ReturnType]: ...


# Overload for single-param workflow
@overload
async def start_child_workflow(
    workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[SelfType, ReturnType]: ...


# Overload for multi-param workflow
@overload
async def start_child_workflow(
    workflow: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[SelfType, ReturnType]: ...


# Overload for string-name workflow
@overload
async def start_child_workflow(
    workflow: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    result_type: Optional[Type] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[Any, Any]: ...


async def start_child_workflow(
    workflow: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    result_type: Optional[Type] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[Any, Any]:
    """Start a child workflow and return its handle.

    Args:
        workflow: String name or class method decorated with ``@workflow.run``
            for the workflow to start.
        arg: Single argument to the child workflow.
        args: Multiple arguments to the child workflow. Cannot be set if arg is.
        id: Optional unique identifier for the workflow execution. If not set,
            defaults to :py:func:`uuid4`.
        task_queue: Task queue to run the workflow on. Defaults to the current
            workflow's task queue.
        result_type: For string workflows, this can set the specific result type
            hint to deserialize into.
        cancellation_type: How the child workflow will react to cancellation.
        parent_close_policy: How to handle the child workflow when the parent
            workflow closes.
        execution_timeout: Total workflow execution timeout including
            retries and continue as new.
        run_timeout: Timeout of a single workflow run.
        task_timeout: Timeout of a single workflow task.
        id_reuse_policy: How already-existing IDs are treated.
        retry_policy: Retry policy for the workflow.
        cron_schedule: See https://docs.temporal.io/docs/content/what-is-a-temporal-cron-job/
        memo: Memo for the workflow.
        search_attributes: Search attributes for the workflow. The dictionary
            form of this is DEPRECATED.
        versioning_intent:  When using the Worker Versioning feature, specifies whether this Child
            Workflow should run on a worker with a compatible Build Id or not.
            Deprecated: Use Worker Deployment versioning instead.
        static_summary: A single-line fixed summary for this child workflow execution that may appear
            in the UI/CLI. This can be in single-line Temporal markdown format.
        static_details: General fixed details for this child workflow execution that may appear in
            UI/CLI. This can be in Temporal markdown format and can span multiple lines. This is
            a fixed value on the workflow that cannot be updated. For details that can be
            updated, use :py:meth:`Workflow.get_current_details` within the workflow.
        priority: Priority to use for this workflow.

    Returns:
        A workflow handle to the started/existing workflow.
    """
    temporalio.common._warn_on_deprecated_search_attributes(search_attributes)
    return await _Runtime.current().workflow_start_child_workflow(
        workflow,
        *temporalio.common._arg_or_args(arg, args),
        id=id or str(uuid4()),
        task_queue=task_queue,
        result_type=result_type,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
        static_summary=static_summary,
        static_details=static_details,
        priority=priority,
    )


# Overload for no-param workflow
@overload
async def execute_child_workflow(
    workflow: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for single-param workflow
@overload
async def execute_child_workflow(
    workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for multi-param workflow
@overload
async def execute_child_workflow(
    workflow: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for string-name workflow
@overload
async def execute_child_workflow(
    workflow: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    result_type: Optional[Type] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any: ...


async def execute_child_workflow(
    workflow: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: Optional[str] = None,
    task_queue: Optional[str] = None,
    result_type: Optional[Type] = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: Optional[timedelta] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    cron_schedule: str = "",
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
    static_summary: Optional[str] = None,
    static_details: Optional[str] = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start a child workflow and wait for completion.

    This is a shortcut for ``await (await`` :py:meth:`start_child_workflow` ``)``.
    """
    temporalio.common._warn_on_deprecated_search_attributes(search_attributes)
    # We call the runtime directly instead of top-level start_child_workflow to
    # ensure we don't miss new parameters
    handle = await _Runtime.current().workflow_start_child_workflow(
        workflow,
        *temporalio.common._arg_or_args(arg, args),
        id=id or str(uuid4()),
        task_queue=task_queue,
        result_type=result_type,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
        static_summary=static_summary,
        static_details=static_details,
        priority=priority,
    )
    return await handle


class NexusOperationHandle(Generic[OutputT]):
    """Handle for interacting with a Nexus operation.

    .. warning::
        This API is experimental and unstable.
    """

    # TODO(nexus-preview): should attempts to instantiate directly throw?

    def cancel(self) -> bool:
        """Request cancellation of the operation."""
        raise NotImplementedError

    def __await__(self) -> Generator[Any, Any, OutputT]:
        """Support await."""
        raise NotImplementedError

    @property
    def operation_token(self) -> Optional[str]:
        """The operation token for this handle."""
        raise NotImplementedError


class ExternalWorkflowHandle(Generic[SelfType]):
    """Handle for interacting with an external workflow.

    This is created via :py:func:`get_external_workflow_handle` or
    :py:func:`get_external_workflow_handle_for`.
    """

    @property
    def id(self) -> str:
        """ID for the workflow."""
        raise NotImplementedError

    @property
    def run_id(self) -> Optional[str]:
        """Run ID for the workflow if any."""
        raise NotImplementedError

    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncNoParam[SelfType, None],
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncSingleParam[SelfType, ParamType, None],
        arg: ParamType,
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None: ...

    async def signal(
        self,
        signal: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None:
        """Signal this external workflow.

        Args:
            signal: Name or method reference for the signal.
            arg: Single argument to the signal.
            args: Multiple arguments to the signal. Cannot be set if arg is.

        """
        raise NotImplementedError

    async def cancel(self) -> None:
        """Send a cancellation request to this external workflow.

        This will fail if the workflow cannot accept the request (e.g. if the
        workflow is not found).
        """
        raise NotImplementedError


def get_external_workflow_handle(
    workflow_id: str,
    *,
    run_id: Optional[str] = None,
) -> ExternalWorkflowHandle[Any]:
    """Get a workflow handle to an existing workflow by its ID.

    Args:
        workflow_id: Workflow ID to get a handle to.
        run_id: Optional run ID for the workflow.

    Returns:
        The external workflow handle.
    """
    return _Runtime.current().workflow_get_external_workflow_handle(
        workflow_id, run_id=run_id
    )


def get_external_workflow_handle_for(
    workflow: Union[
        MethodAsyncNoParam[SelfType, Any], MethodAsyncSingleParam[SelfType, Any, Any]
    ],
    workflow_id: str,
    *,
    run_id: Optional[str] = None,
) -> ExternalWorkflowHandle[SelfType]:
    """Get a typed workflow handle to an existing workflow by its ID.

    This is the same as :py:func:`get_external_workflow_handle` but typed. Note,
    the workflow type given is not validated, it is only for typing.

    Args:
        workflow: The workflow run method to use for typing the handle.
        workflow_id: Workflow ID to get a handle to.
        run_id: Optional run ID for the workflow.

    Returns:
        The external workflow handle.
    """
    return get_external_workflow_handle(workflow_id, run_id=run_id)


class ContinueAsNewError(BaseException):
    """Error thrown by :py:func:`continue_as_new`.

    This should not be caught, but instead be allowed to throw out of the
    workflow which then triggers the continue as new. This should never be
    instantiated directly.
    """

    def __init__(self, *args: object) -> None:
        """Direct instantiation is disabled. Use :py:func:`continue_as_new`."""
        if type(self) == ContinueAsNewError:
            raise RuntimeError("Cannot instantiate ContinueAsNewError directly")
        super().__init__(*args)


# Overload for self (unfortunately, cannot type args)
@overload
def continue_as_new(
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
) -> NoReturn: ...


# Overload for no-param workflow
@overload
def continue_as_new(
    *,
    workflow: MethodAsyncNoParam[SelfType, Any],
    task_queue: Optional[str] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
) -> NoReturn: ...


# Overload for single-param workflow
@overload
def continue_as_new(
    arg: ParamType,
    *,
    workflow: MethodAsyncSingleParam[SelfType, ParamType, Any],
    task_queue: Optional[str] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
) -> NoReturn: ...


# Overload for multi-param workflow
@overload
def continue_as_new(
    *,
    workflow: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[Any]],
    args: Sequence[Any],
    task_queue: Optional[str] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
) -> NoReturn: ...


# Overload for string-name workflow
@overload
def continue_as_new(
    *,
    workflow: str,
    args: Sequence[Any] = [],
    task_queue: Optional[str] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
) -> NoReturn: ...


def continue_as_new(
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    workflow: Union[None, Callable, str] = None,
    task_queue: Optional[str] = None,
    run_timeout: Optional[timedelta] = None,
    task_timeout: Optional[timedelta] = None,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    memo: Optional[Mapping[str, Any]] = None,
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ] = None,
    versioning_intent: Optional[VersioningIntent] = None,
) -> NoReturn:
    """Stop the workflow immediately and continue as new.

    Args:
        arg: Single argument to the continued workflow.
        args: Multiple arguments to the continued workflow. Cannot be set if arg
            is.
        workflow: Specific workflow to continue to. Defaults to the current
            workflow.
        task_queue: Task queue to run the workflow on. Defaults to the current
            workflow's task queue.
        run_timeout: Timeout of a single workflow run. Defaults to the current
            workflow's run timeout.
        task_timeout: Timeout of a single workflow task. Defaults to the current
            workflow's task timeout.
        memo: Memo for the workflow. Defaults to the current workflow's memo.
        search_attributes: Search attributes for the workflow. Defaults to the
            current workflow's search attributes. The dictionary form of this is
            DEPRECATED.
        versioning_intent: When using the Worker Versioning feature, specifies whether this Workflow
            should Continue-as-New onto a worker with a compatible Build Id or not.
            Deprecated: Use Worker Deployment versioning instead.

    Returns:
        Never returns, always raises a :py:class:`ContinueAsNewError`.

    Raises:
        ContinueAsNewError: Always raised by this function. Should not be caught
            but instead be allowed to
    """
    temporalio.common._warn_on_deprecated_search_attributes(search_attributes)
    _Runtime.current().workflow_continue_as_new(
        *temporalio.common._arg_or_args(arg, args),
        workflow=workflow,
        task_queue=task_queue,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        retry_policy=retry_policy,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
    )


def get_signal_handler(name: str) -> Optional[Callable]:
    """Get the signal handler for the given name if any.

    This includes handlers created via the ``@workflow.signal`` decorator.

    Args:
        name: Name of the signal.

    Returns:
        Callable for the signal if any. If a handler is not found for the name,
        this will not return the dynamic handler even if there is one.
    """
    return _Runtime.current().workflow_get_signal_handler(name)


def set_signal_handler(name: str, handler: Optional[Callable]) -> None:
    """Set or unset the signal handler for the given name.

    This overrides any existing handlers for the given name, including handlers
    created via the ``@workflow.signal`` decorator.

    When set, all unhandled past signals for the given name are immediately sent
    to the handler.

    Args:
        name: Name of the signal.
        handler: Callable to set or None to unset.
    """
    _Runtime.current().workflow_set_signal_handler(name, handler)


def get_dynamic_signal_handler() -> Optional[Callable]:
    """Get the dynamic signal handler if any.

    This includes dynamic handlers created via the ``@workflow.signal``
    decorator.

    Returns:
        Callable for the dynamic signal handler if any.
    """
    return _Runtime.current().workflow_get_signal_handler(None)


def set_dynamic_signal_handler(handler: Optional[Callable]) -> None:
    """Set or unset the dynamic signal handler.

    This overrides the existing dynamic handler even if it was created via the
    ``@workflow.signal`` decorator.

    When set, all unhandled past signals are immediately sent to the handler.

    Args:
        handler: Callable to set or None to unset.
    """
    _Runtime.current().workflow_set_signal_handler(None, handler)


def get_query_handler(name: str) -> Optional[Callable]:
    """Get the query handler for the given name if any.

    This includes handlers created via the ``@workflow.query`` decorator.

    Args:
        name: Name of the query.

    Returns:
        Callable for the query if any. If a handler is not found for the name,
        this will not return the dynamic handler even if there is one.
    """
    return _Runtime.current().workflow_get_query_handler(name)


def set_query_handler(name: str, handler: Optional[Callable]) -> None:
    """Set or unset the query handler for the given name.

    This overrides any existing handlers for the given name, including handlers
    created via the ``@workflow.query`` decorator.

    Args:
        name: Name of the query.
        handler: Callable to set or None to unset.
    """
    _Runtime.current().workflow_set_query_handler(name, handler)


def get_dynamic_query_handler() -> Optional[Callable]:
    """Get the dynamic query handler if any.

    This includes dynamic handlers created via the ``@workflow.query``
    decorator.

    Returns:
        Callable for the dynamic query handler if any.
    """
    return _Runtime.current().workflow_get_query_handler(None)


def set_dynamic_query_handler(handler: Optional[Callable]) -> None:
    """Set or unset the dynamic query handler.

    This overrides the existing dynamic handler even if it was created via the
    ``@workflow.query`` decorator.

    Args:
        handler: Callable to set or None to unset.
    """
    _Runtime.current().workflow_set_query_handler(None, handler)


def get_update_handler(name: str) -> Optional[Callable]:
    """Get the update handler for the given name if any.

    This includes handlers created via the ``@workflow.update`` decorator.

    Args:
        name: Name of the update.

    Returns:
        Callable for the update if any. If a handler is not found for the name,
        this will not return the dynamic handler even if there is one.
    """
    return _Runtime.current().workflow_get_update_handler(name)


def set_update_handler(
    name: str, handler: Optional[Callable], *, validator: Optional[Callable] = None
) -> None:
    """Set or unset the update handler for the given name.

    This overrides any existing handlers for the given name, including handlers
    created via the ``@workflow.update`` decorator.

    Args:
        name: Name of the update.
        handler: Callable to set or None to unset.
        validator: Callable to set or None to unset as the update validator.
    """
    _Runtime.current().workflow_set_update_handler(name, handler, validator)


def get_dynamic_update_handler() -> Optional[Callable]:
    """Get the dynamic update handler if any.

    This includes dynamic handlers created via the ``@workflow.update``
    decorator.

    Returns:
        Callable for the dynamic update handler if any.
    """
    return _Runtime.current().workflow_get_update_handler(None)


def set_dynamic_update_handler(
    handler: Optional[Callable], *, validator: Optional[Callable] = None
) -> None:
    """Set or unset the dynamic update handler.

    This overrides the existing dynamic handler even if it was created via the
    ``@workflow.update`` decorator.

    Args:
        handler: Callable to set or None to unset.
        validator: Callable to set or None to unset as the update validator.
    """
    _Runtime.current().workflow_set_update_handler(None, handler, validator)


def all_handlers_finished() -> bool:
    """Whether update and signal handlers have finished executing.

    Consider waiting on this condition before workflow return or continue-as-new, to prevent
    interruption of in-progress handlers by workflow exit:
    ``await workflow.wait_condition(lambda: workflow.all_handlers_finished())``

    Returns:
        True if there are no in-progress update or signal handler executions.
    """
    return _Runtime.current().workflow_all_handlers_finished()


def as_completed(
    fs: Iterable[Awaitable[AnyType]], *, timeout: Optional[float] = None
) -> Iterator[Awaitable[AnyType]]:
    """Return an iterator whose values are coroutines.

    This is a deterministic version of :py:func:`asyncio.as_completed`. This
    function should be used instead of that one in workflows.
    """
    # Taken almost verbatim from
    # https://github.com/python/cpython/blob/v3.12.3/Lib/asyncio/tasks.py#L584
    # but the "set" is changed out for a "list" and fixed up some typing/format

    if asyncio.isfuture(fs) or asyncio.iscoroutine(fs):
        raise TypeError(f"expect an iterable of futures, not {type(fs).__name__}")

    done: asyncio.Queue[Optional[asyncio.Future]] = asyncio.Queue()

    loop = asyncio.get_event_loop()
    todo: List[asyncio.Future] = [asyncio.ensure_future(f, loop=loop) for f in list(fs)]
    timeout_handle = None

    def _on_timeout():
        for f in todo:
            f.remove_done_callback(_on_completion)
            done.put_nowait(None)  # Queue a dummy value for _wait_for_one().
        todo.clear()  # Can't do todo.remove(f) in the loop.

    def _on_completion(f):
        if not todo:
            return  # _on_timeout() was here first.
        todo.remove(f)
        done.put_nowait(f)
        if not todo and timeout_handle is not None:
            timeout_handle.cancel()

    async def _wait_for_one():
        f = await done.get()
        if f is None:
            # Dummy value from _on_timeout().
            raise asyncio.TimeoutError
        return f.result()  # May raise f.exception().

    for f in todo:
        f.add_done_callback(_on_completion)
    if todo and timeout is not None:
        timeout_handle = loop.call_later(timeout, _on_timeout)
    for _ in range(len(todo)):
        yield _wait_for_one()


if TYPE_CHECKING:
    _FT = TypeVar("_FT", bound=asyncio.Future[Any])
else:
    _FT = TypeVar("_FT", bound=asyncio.Future)


@overload
async def wait(  # type: ignore[misc]
    fs: Iterable[_FT],
    *,
    timeout: Optional[float] = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> Tuple[List[_FT], List[_FT]]: ...


@overload
async def wait(
    fs: Iterable[asyncio.Task[AnyType]],
    *,
    timeout: Optional[float] = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> Tuple[List[asyncio.Task[AnyType]], set[asyncio.Task[AnyType]]]: ...


async def wait(
    fs: Iterable,
    *,
    timeout: Optional[float] = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> Tuple:
    """Wait for the Futures or Tasks given by fs to complete.

    This is a deterministic version of :py:func:`asyncio.wait`. This function
    should be used instead of that one in workflows.
    """
    # Taken almost verbatim from
    # https://github.com/python/cpython/blob/v3.12.3/Lib/asyncio/tasks.py#L435
    # but the "set" is changed out for a "list" and fixed up some typing/format

    if asyncio.isfuture(fs) or asyncio.iscoroutine(fs):
        raise TypeError(f"expect a list of futures, not {type(fs).__name__}")
    if not fs:
        raise ValueError("Set of Tasks/Futures is empty.")
    if return_when not in (
        asyncio.FIRST_COMPLETED,
        asyncio.FIRST_EXCEPTION,
        asyncio.ALL_COMPLETED,
    ):
        raise ValueError(f"Invalid return_when value: {return_when}")

    fs = list(fs)

    if any(asyncio.iscoroutine(f) for f in fs):
        raise TypeError("Passing coroutines is forbidden, use tasks explicitly.")

    loop = asyncio.get_running_loop()
    return await _wait(fs, timeout, return_when, loop)


async def _wait(
    fs: Iterable[Union[asyncio.Future, asyncio.Task]],
    timeout: Optional[float],
    return_when: str,
    loop: asyncio.AbstractEventLoop,
) -> Tuple[List, List]:
    # Taken almost verbatim from
    # https://github.com/python/cpython/blob/v3.12.3/Lib/asyncio/tasks.py#L522
    # but the "set" is changed out for a "list" and fixed up some typing/format

    assert fs, "Set of Futures is empty."
    waiter = loop.create_future()
    timeout_handle = None
    if timeout is not None:
        timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    counter = len(fs)  # type: ignore[arg-type]

    def _on_completion(f):
        nonlocal counter
        counter -= 1
        if (
            counter <= 0
            or return_when == asyncio.FIRST_COMPLETED
            or return_when == asyncio.FIRST_EXCEPTION
            and (not f.cancelled() and f.exception() is not None)
        ):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)

    for f in fs:
        f.add_done_callback(_on_completion)

    try:
        await waiter
    finally:
        if timeout_handle is not None:
            timeout_handle.cancel()
        for f in fs:
            f.remove_done_callback(_on_completion)

    done, pending = [], []
    for f in fs:
        if f.done():
            done.append(f)
        else:
            pending.append(f)
    return done, pending


def _release_waiter(waiter: asyncio.Future[Any], *args) -> None:
    # Taken almost verbatim from
    # https://github.com/python/cpython/blob/v3.12.3/Lib/asyncio/tasks.py#L467

    if not waiter.done():
        waiter.set_result(None)


def _is_unbound_method_on_cls(fn: Callable[..., Any], cls: Type) -> bool:
    # Python 3 does not make this easy, ref https://stackoverflow.com/questions/3589311
    return (
        inspect.isfunction(fn)
        and inspect.getmodule(fn) is inspect.getmodule(cls)
        and fn.__qualname__.rsplit(".", 1)[0] == cls.__name__
    )


class _UnexpectedEvictionError(temporalio.exceptions.TemporalError):
    def __init__(
        self,
        reason: temporalio.bridge.proto.workflow_activation.RemoveFromCache.EvictionReason.ValueType,
        message: str,
    ) -> None:
        self.reason = temporalio.bridge.proto.workflow_activation.RemoveFromCache.EvictionReason.Name(
            reason
        )
        self.message = message
        super().__init__(f"{self.reason}: {message}")


class NondeterminismError(temporalio.exceptions.TemporalError):
    """Error that can be thrown during replay for non-deterministic workflow."""

    def __init__(self, message: str) -> None:
        """Initialize a nondeterminism error."""
        super().__init__(message)
        self.message = message


class ReadOnlyContextError(temporalio.exceptions.TemporalError):
    """Error thrown when trying to do mutable workflow calls in a read-only
    context like a query or update validator.
    """

    def __init__(self, message: str) -> None:
        """Initialize a read-only context error."""
        super().__init__(message)
        self.message = message


class _NotInWorkflowEventLoopError(temporalio.exceptions.TemporalError):
    def __init__(self, *args: object) -> None:
        super().__init__("Not in workflow event loop")
        self.message = "Not in workflow event loop"


class VersioningIntent(Enum):
    """Indicates whether the user intends certain commands to be run on a compatible worker Build
    Id version or not.

    `COMPATIBLE` indicates that the command should run on a worker with compatible version if
    possible. It may not be possible if the target task queue does not also have knowledge of the
    current worker's Build Id.

    `DEFAULT` indicates that the command should run on the target task queue's current
    overall-default Build Id.

    Where this type is accepted optionally, an unset value indicates that the SDK should choose the
    most sensible default behavior for the type of command, accounting for whether the command will
    be run on the same task queue as the current worker.

    .. deprecated::
        Use Worker Deployment versioning instead.
    """

    COMPATIBLE = 1
    DEFAULT = 2

    def _to_proto(self) -> temporalio.bridge.proto.common.VersioningIntent.ValueType:
        if self == VersioningIntent.COMPATIBLE:
            return temporalio.bridge.proto.common.VersioningIntent.COMPATIBLE
        elif self == VersioningIntent.DEFAULT:
            return temporalio.bridge.proto.common.VersioningIntent.DEFAULT
        return temporalio.bridge.proto.common.VersioningIntent.UNSPECIFIED


ServiceT = TypeVar("ServiceT")


class NexusOperationCancellationType(IntEnum):
    """Defines behavior of a Nexus operation when the caller workflow initiates cancellation.

    Pass one of these values to :py:meth:`NexusClient.start_operation` to define cancellation
    behavior.

    To initiate cancellation, use :py:meth:`NexusOperationHandle.cancel` and then `await` the
    operation handle. This will result in a :py:class:`exceptions.NexusOperationError`. The values
    of this enum define what is guaranteed to have happened by that point.
    """

    ABANDON = int(temporalio.bridge.proto.nexus.NexusOperationCancellationType.ABANDON)
    """Do not send any cancellation request to the operation handler; just report cancellation to the caller"""

    TRY_CANCEL = int(
        temporalio.bridge.proto.nexus.NexusOperationCancellationType.TRY_CANCEL
    )
    """Send a cancellation request but immediately report cancellation to the caller. Note that this
    does not guarantee that cancellation is delivered to the operation handler if the caller exits
    before the delivery is done.
    """

    WAIT_REQUESTED = int(
        temporalio.bridge.proto.nexus.NexusOperationCancellationType.WAIT_CANCELLATION_REQUESTED
    )
    """Send a cancellation request and wait for confirmation that the request was received.
    Does not wait for the operation to complete.
    """

    WAIT_COMPLETED = int(
        temporalio.bridge.proto.nexus.NexusOperationCancellationType.WAIT_CANCELLATION_COMPLETED
    )
    """Send a cancellation request and wait for the operation to complete.
    Note that the operation may not complete as cancelled (for example, if it catches the
    :py:exc:`asyncio.CancelledError` resulting from the cancellation request)."""


class NexusClient(ABC, Generic[ServiceT]):
    """A client for invoking Nexus operations.

    .. warning::
        This API is experimental and unstable.

    Example::

        nexus_client = workflow.create_nexus_client(
            endpoint=my_nexus_endpoint,
            service=MyService,
        )
        handle = await nexus_client.start_operation(
            operation=MyService.my_operation,
            input=MyOperationInput(value="hello"),
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        result = await handle.result()
    """

    # Overload for nexusrpc.Operation
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: nexusrpc.Operation[InputT, OutputT],
        input: InputT,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for string operation name
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: str,
        input: Any,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for workflow_run_operation methods
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [ServiceHandlerT, temporalio.nexus.WorkflowRunOperationContext, InputT],
            Awaitable[temporalio.nexus.WorkflowHandle[OutputT]],
        ],
        input: InputT,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for sync_operation methods (async def)
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [ServiceHandlerT, nexusrpc.handler.StartOperationContext, InputT],
            Awaitable[OutputT],
        ],
        input: InputT,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for sync_operation methods (def)
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [ServiceHandlerT, nexusrpc.handler.StartOperationContext, InputT],
            OutputT,
        ],
        input: InputT,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> NexusOperationHandle[OutputT]: ...

    @abstractmethod
    async def start_operation(
        self,
        operation: Any,
        input: Any,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """Start a Nexus operation and return its handle.

        Args:
            operation: The Nexus operation.
            input: The Nexus operation input.
            output_type: The Nexus operation output type.
            schedule_to_close_timeout: Timeout for the entire operation attempt.
            headers: Headers to send with the Nexus HTTP request.

        Returns:
            A handle to the Nexus operation. The result can be obtained as
            ```python
            await handle.result()
            ```
        """
        ...

    # Overload for nexusrpc.Operation
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: nexusrpc.Operation[InputT, OutputT],
        input: InputT,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> OutputT: ...

    # Overload for string operation name
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: str,
        input: Any,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> OutputT: ...

    # Overload for workflow_run_operation methods
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [ServiceHandlerT, temporalio.nexus.WorkflowRunOperationContext, InputT],
            Awaitable[temporalio.nexus.WorkflowHandle[OutputT]],
        ],
        input: InputT,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> OutputT: ...

    # TODO(nexus-preview): in practice, both these overloads match an async def sync
    # operation (i.e. either can be deleted without causing a type error).

    # Overload for sync_operation methods (async def)
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [ServiceT, nexusrpc.handler.StartOperationContext, InputT],
            Awaitable[OutputT],
        ],
        input: InputT,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> OutputT: ...

    # Overload for sync_operation methods (def)
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [ServiceT, nexusrpc.handler.StartOperationContext, InputT],
            OutputT,
        ],
        input: InputT,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> OutputT: ...

    @abstractmethod
    async def execute_operation(
        self,
        operation: Any,
        input: Any,
        *,
        output_type: Optional[Type[OutputT]] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """Execute a Nexus operation and return its result.

        Args:
            operation: The Nexus operation.
            input: The Nexus operation input.
            output_type: The Nexus operation output type.
            schedule_to_close_timeout: Timeout for the entire operation attempt.
            headers: Headers to send with the Nexus HTTP request.

        Returns:
            The operation result.
        """
        ...


class _NexusClient(NexusClient[ServiceT]):
    def __init__(
        self,
        *,
        endpoint: str,
        service: Union[Type[ServiceT], str],
    ) -> None:
        """Create a Nexus client.

        Args:
            service: The Nexus service.
            endpoint: The Nexus endpoint.
        """
        # If service is not a str, then it must be a service interface or implementation
        # class.
        if isinstance(service, str):
            self.service_name = service
        elif service_defn := nexusrpc.get_service_definition(service):
            self.service_name = service_defn.name
        else:
            raise ValueError(
                f"`service` may be a name (str), or a class decorated with either "
                f"@nexusrpc.handler.service_handler or @nexusrpc.service. "
                f"Invalid service type: {type(service)}"
            )
        self.endpoint = endpoint

    async def start_operation(
        self,
        operation: Any,
        input: Any,
        *,
        output_type: Optional[Type] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        return (
            await temporalio.workflow._Runtime.current().workflow_start_nexus_operation(
                endpoint=self.endpoint,
                service=self.service_name,
                operation=operation,
                input=input,
                output_type=output_type,
                schedule_to_close_timeout=schedule_to_close_timeout,
                cancellation_type=cancellation_type,
                headers=headers,
            )
        )

    async def execute_operation(
        self,
        operation: Any,
        input: Any,
        *,
        output_type: Optional[Type] = None,
        schedule_to_close_timeout: Optional[timedelta] = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Any:
        handle = await self.start_operation(
            operation,
            input,
            output_type=output_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            cancellation_type=cancellation_type,
            headers=headers,
        )
        return await handle


@overload
def create_nexus_client(
    *,
    service: Type[ServiceT],
    endpoint: str,
) -> NexusClient[ServiceT]: ...


@overload
def create_nexus_client(
    *,
    service: str,
    endpoint: str,
) -> NexusClient[Any]: ...


def create_nexus_client(
    *,
    service: Union[Type[ServiceT], str],
    endpoint: str,
) -> NexusClient[ServiceT]:
    """Create a Nexus client.

    .. warning::
        This API is experimental and unstable.

    Args:
        service: The Nexus service.
        endpoint: The Nexus endpoint.
    """
    return _NexusClient(endpoint=endpoint, service=service)

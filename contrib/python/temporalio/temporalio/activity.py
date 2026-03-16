"""Functions that can be called inside of activities.

Most of these functions use :py:mod:`contextvars` to obtain the current activity
in context. This is already set before the start of the activity. Activities
that make calls that do not automatically propagate the context, such as calls
in another thread, should not use the calls herein unless the context is
explicitly propagated.
"""

from __future__ import annotations

import asyncio
import contextvars
import dataclasses
import inspect
import logging
import threading
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    overload,
)

import temporalio.bridge
import temporalio.bridge.proto
import temporalio.bridge.proto.activity_task
import temporalio.common
import temporalio.converter

from .types import CallableType

if TYPE_CHECKING:
    from temporalio.client import Client


@overload
def defn(fn: CallableType) -> CallableType: ...


@overload
def defn(
    *, name: Optional[str] = None, no_thread_cancel_exception: bool = False
) -> Callable[[CallableType], CallableType]: ...


@overload
def defn(
    *, no_thread_cancel_exception: bool = False, dynamic: bool = False
) -> Callable[[CallableType], CallableType]: ...


def defn(
    fn: Optional[CallableType] = None,
    *,
    name: Optional[str] = None,
    no_thread_cancel_exception: bool = False,
    dynamic: bool = False,
):
    """Decorator for activity functions.

    Activities can be async or non-async.

    Args:
        fn: The function to decorate.
        name: Name to use for the activity. Defaults to function ``__name__``.
            This cannot be set if dynamic is set.
        no_thread_cancel_exception: If set to true, an exception will not be
            raised in synchronous, threaded activities upon cancellation.
        dynamic: If true, this activity will be dynamic. Dynamic activities have
            to accept a single 'Sequence[RawValue]' parameter. This cannot be
            set to true if name is present.
    """

    def decorator(fn: CallableType) -> CallableType:
        # This performs validation
        _Definition._apply_to_callable(
            fn,
            activity_name=name or fn.__name__ if not dynamic else None,
            no_thread_cancel_exception=no_thread_cancel_exception,
        )
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


@dataclass(frozen=True)
class Info:
    """Information about the running activity.

    Retrieved inside an activity via :py:func:`info`.
    """

    activity_id: str
    activity_type: str
    attempt: int
    current_attempt_scheduled_time: datetime
    heartbeat_details: Sequence[Any]
    heartbeat_timeout: Optional[timedelta]
    is_local: bool
    schedule_to_close_timeout: Optional[timedelta]
    scheduled_time: datetime
    start_to_close_timeout: Optional[timedelta]
    started_time: datetime
    task_queue: str
    task_token: bytes
    workflow_id: str
    workflow_namespace: str
    workflow_run_id: str
    workflow_type: str
    priority: temporalio.common.Priority
    # TODO(cretz): Consider putting identity on here for "worker_id" for logger?

    def _logger_details(self) -> Mapping[str, Any]:
        return {
            "activity_id": self.activity_id,
            "activity_type": self.activity_type,
            "attempt": self.attempt,
            "namespace": self.workflow_namespace,
            "task_queue": self.task_queue,
            "workflow_id": self.workflow_id,
            "workflow_run_id": self.workflow_run_id,
            "workflow_type": self.workflow_type,
        }


_current_context: contextvars.ContextVar[_Context] = contextvars.ContextVar("activity")


@dataclass
class _ActivityCancellationDetailsHolder:
    details: Optional[ActivityCancellationDetails] = None


@dataclass(frozen=True)
class ActivityCancellationDetails:
    """Provides the reasons for the activity's cancellation. Cancellation details are set once and do not change once set."""

    not_found: bool = False
    cancel_requested: bool = False
    paused: bool = False
    timed_out: bool = False
    worker_shutdown: bool = False

    @staticmethod
    def _from_proto(
        proto: temporalio.bridge.proto.activity_task.ActivityCancellationDetails,
    ) -> ActivityCancellationDetails:
        return ActivityCancellationDetails(
            not_found=proto.is_not_found,
            cancel_requested=proto.is_cancelled,
            paused=proto.is_paused,
            timed_out=proto.is_timed_out,
            worker_shutdown=proto.is_worker_shutdown,
        )


@dataclass
class _Context:
    info: Callable[[], Info]
    # This is optional because during interceptor init it is not present
    heartbeat: Optional[Callable[..., None]]
    cancelled_event: _CompositeEvent
    worker_shutdown_event: _CompositeEvent
    shield_thread_cancel_exception: Optional[Callable[[], AbstractContextManager]]
    payload_converter_class_or_instance: Union[
        Type[temporalio.converter.PayloadConverter],
        temporalio.converter.PayloadConverter,
    ]
    runtime_metric_meter: Optional[temporalio.common.MetricMeter]
    client: Optional[Client]
    cancellation_details: _ActivityCancellationDetailsHolder
    _logger_details: Optional[Mapping[str, Any]] = None
    _payload_converter: Optional[temporalio.converter.PayloadConverter] = None
    _metric_meter: Optional[temporalio.common.MetricMeter] = None

    @staticmethod
    def current() -> _Context:
        context = _current_context.get(None)
        if not context:
            raise RuntimeError("Not in activity context")
        return context

    @staticmethod
    def set(context: _Context) -> contextvars.Token:
        return _current_context.set(context)

    @staticmethod
    def reset(token: contextvars.Token) -> None:
        _current_context.reset(token)

    @property
    def logger_details(self) -> Mapping[str, Any]:
        if self._logger_details is None:
            self._logger_details = self.info()._logger_details()
        return self._logger_details

    @property
    def payload_converter(self) -> temporalio.converter.PayloadConverter:
        if not self._payload_converter:
            if isinstance(
                self.payload_converter_class_or_instance,
                temporalio.converter.PayloadConverter,
            ):
                self._payload_converter = self.payload_converter_class_or_instance
            else:
                self._payload_converter = self.payload_converter_class_or_instance()
        return self._payload_converter

    @property
    def metric_meter(self) -> temporalio.common.MetricMeter:
        # If there isn't a runtime metric meter, then we're in a non-threaded
        # sync function and we don't support cross-process metrics
        if not self.runtime_metric_meter:
            raise RuntimeError(
                "Metrics meter not available in non-threaded sync activities like mulitprocess"
            )
        # Create the meter lazily if not already created. We are ok creating
        # multiple in the rare race where a user calls this property on
        # different threads inside the same activity. The meter is immutable and
        # it's better than a lock.
        if not self._metric_meter:
            info = self.info()
            self._metric_meter = self.runtime_metric_meter.with_additional_attributes(
                {
                    "namespace": info.workflow_namespace,
                    "task_queue": info.task_queue,
                    "activity_type": info.activity_type,
                }
            )
        return self._metric_meter


@dataclass
class _CompositeEvent:
    # This should always be present, but is sometimes lazily set internally
    thread_event: Optional[threading.Event]
    # Async event only for async activities
    async_event: Optional[asyncio.Event]

    def set(self) -> None:
        if not self.thread_event:
            raise RuntimeError("Missing event")
        self.thread_event.set()
        if self.async_event:
            self.async_event.set()

    def is_set(self) -> bool:
        if not self.thread_event:
            raise RuntimeError("Missing event")
        return self.thread_event.is_set()

    async def wait(self) -> None:
        if not self.async_event:
            raise RuntimeError("not in async activity")
        await self.async_event.wait()

    def wait_sync(self, timeout: Optional[float] = None) -> None:
        if not self.thread_event:
            raise RuntimeError("Missing event")
        self.thread_event.wait(timeout)


def client() -> Client:
    """Return a Temporal Client for use in the current activity.

    The client is only available in `async def` activities.

    In tests it is not available automatically, but you can pass a client when creating a
    :py:class:`temporalio.testing.ActivityEnvironment`.

    Returns:
        :py:class:`temporalio.client.Client` for use in the current activity.

    Raises:
        RuntimeError: When the client is not available.
    """
    client = _Context.current().client
    if not client:
        raise RuntimeError(
            "No client available. The client is only available in `async def` "
            "activities; not in `def` activities. In tests you can pass a "
            "client when creating ActivityEnvironment."
        )
    return client


def in_activity() -> bool:
    """Whether the current code is inside an activity.

    Returns:
        True if in an activity, False otherwise.
    """
    return _current_context.get(None) is not None


def info() -> Info:
    """Current activity's info.

    Returns:
        Info for the currently running activity.

    Raises:
        RuntimeError: When not in an activity.
    """
    return _Context.current().info()


def cancellation_details() -> Optional[ActivityCancellationDetails]:
    """Cancellation details of the current activity, if any. Once set, cancellation details do not change."""
    return _Context.current().cancellation_details.details


def heartbeat(*details: Any) -> None:
    """Send a heartbeat for the current activity.

    Raises:
        RuntimeError: When not in an activity.
    """
    heartbeat_fn = _Context.current().heartbeat
    if not heartbeat_fn:
        raise RuntimeError("Can only execute heartbeat after interceptor init")
    heartbeat_fn(*details)


def is_cancelled() -> bool:
    """Whether a cancellation was ever requested on this activity.

    Returns:
        True if the activity has had a cancellation request, False otherwise.

    Raises:
        RuntimeError: When not in an activity.
    """
    return _Context.current().cancelled_event.is_set()


@contextmanager
def shield_thread_cancel_exception() -> Iterator[None]:
    """Context manager for synchronous multithreaded activities to delay
    cancellation exceptions.

    By default, synchronous multithreaded activities have an exception thrown
    inside when cancellation occurs. Code within a "with" block of this context
    manager will delay that throwing until the end. Even if the block returns a
    value or throws its own exception, if a cancellation exception is pending,
    it is thrown instead. Therefore users are encouraged to not throw out of
    this block and can surround this with a try/except if they wish to catch a
    cancellation.

    This properly supports nested calls and will only throw after the last one.

    This just runs the blocks with no extra effects for async activities or
    synchronous multiprocess/other activities.

    Raises:
        temporalio.exceptions.CancelledError: If a cancellation occurs anytime
            during this block and this is not nested in another shield block.
    """
    shield_context = _Context.current().shield_thread_cancel_exception
    if not shield_context:
        yield None
    else:
        with shield_context():
            yield None


async def wait_for_cancelled() -> None:
    """Asynchronously wait for this activity to get a cancellation request.

    Raises:
        RuntimeError: When not in an async activity.
    """
    await _Context.current().cancelled_event.wait()


def wait_for_cancelled_sync(timeout: Optional[Union[timedelta, float]] = None) -> None:
    """Synchronously block while waiting for a cancellation request on this
    activity.

    This is essentially a wrapper around :py:meth:`threading.Event.wait`.

    Args:
        timeout: Max amount of time to wait for cancellation.

    Raises:
        RuntimeError: When not in an activity.
    """
    _Context.current().cancelled_event.wait_sync(
        timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
    )


def is_worker_shutdown() -> bool:
    """Whether shutdown has been invoked on the worker.

    Returns:
        True if shutdown has been called on the worker, False otherwise.

    Raises:
        RuntimeError: When not in an activity.
    """
    return _Context.current().worker_shutdown_event.is_set()


async def wait_for_worker_shutdown() -> None:
    """Asynchronously wait for shutdown to be called on the worker.

    Raises:
        RuntimeError: When not in an async activity.
    """
    await _Context.current().worker_shutdown_event.wait()


def wait_for_worker_shutdown_sync(
    timeout: Optional[Union[timedelta, float]] = None,
) -> None:
    """Synchronously block while waiting for shutdown to be called on the
    worker.

    This is essentially a wrapper around :py:meth:`threading.Event.wait`.

    Args:
        timeout: Max amount of time to wait for shutdown to be called on the
            worker.

    Raises:
        RuntimeError: When not in an activity.
    """
    _Context.current().worker_shutdown_event.wait_sync(
        timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
    )


def raise_complete_async() -> NoReturn:
    """Raise an error that says the activity will be completed
    asynchronously.
    """
    raise _CompleteAsyncError()


class _CompleteAsyncError(BaseException):
    pass


def payload_converter() -> temporalio.converter.PayloadConverter:
    """Get the payload converter for the current activity.

    This is often used for dynamic activities to convert payloads.
    """
    return _Context.current().payload_converter


def metric_meter() -> temporalio.common.MetricMeter:
    """Get the metric meter for the current activity.

    .. warning::
        This is only available in async or synchronous threaded activities. An
        error is raised on non-thread-based sync activities when trying to
        access this.

    Returns:
        Current metric meter for this activity for recording metrics.

    Raises:
        RuntimeError: When not in an activity or in a non-thread-based
            synchronous activity.
    """
    return _Context.current().metric_meter


class LoggerAdapter(logging.LoggerAdapter):
    """Adapter that adds details to the log about the running activity.

    Attributes:
        activity_info_on_message: Boolean for whether a string representation of
            a dict of some activity info will be appended to each message.
            Default is True.
        activity_info_on_extra: Boolean for whether a ``temporal_activity``
            dictionary value will be added to the ``extra`` dictionary with some
            activity info, making it present on the ``LogRecord.__dict__`` for
            use by others. Default is True.
        full_activity_info_on_extra: Boolean for whether an ``activity_info``
            value will be added to the ``extra`` dictionary with the entire
            activity info, making it present on the ``LogRecord.__dict__`` for
            use by others. Default is False.
    """

    def __init__(
        self, logger: logging.Logger, extra: Optional[Mapping[str, Any]]
    ) -> None:
        """Create the logger adapter."""
        super().__init__(logger, extra or {})
        self.activity_info_on_message = True
        self.activity_info_on_extra = True
        self.full_activity_info_on_extra = False

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> Tuple[Any, MutableMapping[str, Any]]:
        """Override to add activity details."""
        if (
            self.activity_info_on_message
            or self.activity_info_on_extra
            or self.full_activity_info_on_extra
        ):
            context = _current_context.get(None)
            if context:
                if self.activity_info_on_message:
                    msg = f"{msg} ({context.logger_details})"
                if self.activity_info_on_extra:
                    # Extra can be absent or None, this handles both
                    extra = kwargs.get("extra", None) or {}
                    extra["temporal_activity"] = context.logger_details
                    kwargs["extra"] = extra
                if self.full_activity_info_on_extra:
                    # Extra can be absent or None, this handles both
                    extra = kwargs.get("extra", None) or {}
                    extra["activity_info"] = context.info()
                    kwargs["extra"] = extra
        return (msg, kwargs)

    @property
    def base_logger(self) -> logging.Logger:
        """Underlying logger usable for actions such as adding
        handlers/formatters.
        """
        return self.logger


logger = LoggerAdapter(logging.getLogger(__name__), None)
"""Logger that will have contextual activity details embedded."""


@dataclass(frozen=True)
class _Definition:
    name: Optional[str]
    fn: Callable
    is_async: bool
    no_thread_cancel_exception: bool
    # Types loaded on post init if both are None
    arg_types: Optional[List[Type]] = None
    ret_type: Optional[Type] = None

    @staticmethod
    def from_callable(fn: Callable) -> Optional[_Definition]:
        defn = getattr(fn, "__temporal_activity_definition", None)
        if isinstance(defn, _Definition):
            # We have to replace the function with the given callable here
            # because the one passed in may be a method or some other partial
            # that represents the real callable instead of what the decorator
            # used.
            defn = dataclasses.replace(defn, fn=fn)
        return defn

    @staticmethod
    def must_from_callable(fn: Callable) -> _Definition:
        ret = _Definition.from_callable(fn)
        if ret:
            return ret
        fn_name = getattr(fn, "__name__", "<unknown>")
        raise TypeError(
            f"Activity {fn_name} missing attributes, was it decorated with @activity.defn?"
        )

    @staticmethod
    def _apply_to_callable(
        fn: Callable,
        *,
        activity_name: Optional[str],
        no_thread_cancel_exception: bool = False,
    ) -> None:
        # Validate the activity
        if hasattr(fn, "__temporal_activity_definition"):
            raise ValueError("Function already contains activity definition")
        elif not callable(fn):
            raise TypeError("Activity is not callable")
        # We do not allow keyword only arguments in activities
        sig = inspect.signature(fn)
        for param in sig.parameters.values():
            if param.kind == inspect.Parameter.KEYWORD_ONLY:
                raise TypeError("Activity cannot have keyword-only arguments")
        setattr(
            fn,
            "__temporal_activity_definition",
            _Definition(
                name=activity_name,
                fn=fn,
                # iscoroutinefunction does not return true for async __call__
                # TODO(cretz): Why can't MyPy handle this?
                is_async=(
                    inspect.iscoroutinefunction(fn)
                    or inspect.iscoroutinefunction(fn.__call__)  # type: ignore
                ),
                no_thread_cancel_exception=no_thread_cancel_exception,
            ),
        )

    def __post_init__(self) -> None:
        if self.arg_types is None and self.ret_type is None:
            dynamic = self.name is None
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.fn)
            # If dynamic, must be a sequence of raw values
            if dynamic and (
                not arg_types
                or len(arg_types) != 1
                or arg_types[0] != Sequence[temporalio.common.RawValue]
            ):
                raise TypeError(
                    "Dynamic activity must accept a single Sequence[temporalio.common.RawValue]"
                )
            object.__setattr__(self, "arg_types", arg_types)
            object.__setattr__(self, "ret_type", ret_type)

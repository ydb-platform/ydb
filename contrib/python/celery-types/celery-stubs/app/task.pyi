from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import (
    Any,
    Generic,
    Literal,
    TypeVar,
    overload,
)

import billiard
import celery
import celery.result
import kombu
from celery import canvas
from celery.app.base import Celery
from celery.backends.base import Backend
from celery.canvas import Signature, xmap, xstarmap
from celery.exceptions import Retry
from celery.result import EagerResult
from celery.utils.threads import _LocalStack
from celery.worker.request import _DeliveryInfo
from typing_extensions import ParamSpec

_P = ParamSpec("_P")
_R_co = TypeVar("_R_co", covariant=True)
_SigR = TypeVar("_SigR")

class Context:
    args: Sequence[Any] | None
    callbacks: Sequence[Mapping[str, Any]] | None
    called_directly: bool
    chain: str | None
    chord: str | None
    correlation_id: str | None
    delivery_info: _DeliveryInfo | None
    errbacks: Sequence[Mapping[str, Any]] | None
    eta: int | None
    expires: int | None
    group: str | None
    group_index: int | None
    headers: dict[str, Any] | None
    hostname: str | None
    id: str | None
    ignore_result: bool
    is_eager: bool
    kwargs: Mapping[str, Any] | None
    logfile: str | None
    loglevel: int | None
    origin: Any
    parent_id: str | None
    properties: Any | None
    retries: int
    reply_to: Any
    replaced_task_nesting: int
    root_id: str | None
    shadow: Any
    taskset: str | None  # compat alias to group
    timelimit: tuple[int, int] | tuple[None, None] | None
    utc: bool | None
    def __init__(self, *args: dict[str, Any], **kwargs: Any) -> None: ...
    def update(self, *args: dict[str, Any], **kwargs: Any) -> None: ...
    def clear(self) -> None: ...
    def get(self, key: str, default: Any = ...) -> Any: ...
    def as_execution_options(self) -> dict[str, Any]: ...
    @property
    def children(self) -> list[str]: ...

class Task(Generic[_P, _R_co]):
    name: str
    typing: bool
    max_retries: int | None
    default_retry_delay: int
    rate_limit: str | None
    ignore_result: bool
    trail: bool
    send_events: bool
    store_errors_even_if_ignored: bool
    serializer: str
    time_limit: int | None
    soft_time_limit: int | None
    autoregister: bool
    track_started: bool
    acks_late: bool
    acks_on_failure_or_timeout: bool
    reject_on_worker_lost: bool
    throws: tuple[type[Exception], ...]
    expires: float | datetime | None
    priority: int | None
    resultrepr_maxsize: int
    request_stack: _LocalStack[Context]
    abstract: bool
    @classmethod
    def bind(cls, app: Celery) -> Celery: ...
    @classmethod
    def on_bound(cls, app: Celery) -> None: ...
    @property
    def app(self) -> Celery: ...
    @classmethod
    def annotate(cls) -> None: ...
    @classmethod
    def add_around(cls, attr: str, around: Any) -> None: ...
    # TODO(sbdchd): might be able to use overloads to handle the case where
    # `bind=True` passes in the first argument.
    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _R_co: ...
    def run(self, *args: _P.args, **kwargs: _P.kwargs) -> _R_co: ...
    def start_strategy(self, app: Celery, consumer: Any, **kwargs: Any) -> Any: ...
    def delay(
        self, *args: _P.args, **kwargs: _P.kwargs
    ) -> celery.result.AsyncResult[_R_co]: ...
    def apply_async(
        self,
        args: tuple[Any, ...] | None = ...,
        kwargs: dict[str, Any] | None = ...,
        task_id: str | None = ...,
        producer: kombu.Producer | None = ...,
        link: Signature[Any] | list[Signature[Any]] | None = ...,
        link_error: Signature[Any] | list[Signature[Any]] | None = ...,
        shadow: str | None = ...,
        *,
        # options
        countdown: float = ...,
        eta: datetime = ...,
        expires: float | datetime = ...,
        retry: bool = ...,
        retry_policy: Mapping[str, Any] = ...,
        queue: str | kombu.Queue = ...,
        exchange: str | kombu.Exchange = ...,
        routing_key: str = ...,
        priority: int = ...,
        serializer: str = ...,
        compression: str = ...,
        add_to_parent: bool = ...,
        publisher: kombu.Producer = ...,
        headers: dict[str, str] = ...,
        ignore_result: bool = ...,
        time_limit: int = ...,
        soft_time_limit: int = ...,
    ) -> celery.result.AsyncResult[_R_co]: ...
    def shadow_name(
        self, args: tuple[Any, ...], kwargs: dict[str, Any], options: dict[str, Any]
    ) -> None: ...
    def signature_from_request(
        self,
        request: Context | None = ...,
        args: tuple[Any, ...] = ...,
        kwargs: dict[str, Any] | None = ...,
        queue: str | None = ...,
        **extra_options: Any,
    ) -> Signature[_R_co]: ...
    subtask_from_request = signature_from_request
    def retry(
        self,
        args: tuple[Any, ...] | None = ...,
        kwargs: dict[str, Any] | None = ...,
        exc: Exception | None = ...,
        throw: bool = ...,
        eta: datetime | None = ...,
        countdown: float | None = ...,
        max_retries: int | None = ...,
        *,
        # options
        task_id: str | None = ...,
        producer: kombu.Producer | None = ...,
        link: Signature[Any] | list[Signature[Any]] | None = ...,
        link_error: Signature[Any] | list[Signature[Any]] | None = ...,
        shadow: str | None = ...,
        expires: float | datetime = ...,
        retry: bool = ...,
        retry_policy: Mapping[str, Any] = ...,
        queue: str | kombu.Queue = ...,
        exchange: str | kombu.Exchange = ...,
        routing_key: str = ...,
        priority: int = ...,
        serializer: str = ...,
        compression: str = ...,
        add_to_parent: bool = ...,
        publisher: kombu.Producer = ...,
        headers: dict[str, str] = ...,
        ignore_result: bool = ...,
        time_limit: int = ...,
        soft_time_limit: int = ...,
    ) -> Retry: ...
    def apply(
        self,
        args: tuple[Any, ...] | None = ...,
        kwargs: dict[str, Any] | None = ...,
        link: Signature[Any] | list[Signature[Any]] | None = ...,
        link_error: Signature[Any] | list[Signature[Any]] | None = ...,
        task_id: str | None = ...,
        retries: int | None = ...,
        throw: bool | None = ...,
        logfile: str | None = ...,
        loglevel: str | None = ...,
        headers: Mapping[str, str] | None = ...,
        *,
        # options
        ignore_result: bool = ...,
        exchange: str = ...,
        routing_key: str = ...,
        priority: int = ...,
    ) -> EagerResult[_R_co]: ...
    def AsyncResult(
        self, task_id: str, **kwargs: Any
    ) -> celery.result.AsyncResult[_R_co]: ...
    def signature(
        self, args: tuple[Any, ...] | None = ..., *starargs: Any, **starkwargs: Any
    ) -> Signature[_R_co]: ...
    def subtask(
        self, args: tuple[Any, ...] | None = ..., *starargs: Any, **starkwargs: Any
    ) -> Signature[_R_co]: ...
    def s(self, *args: Any, **kwargs: Any) -> Signature[_R_co]: ...
    def si(self, *args: _P.args, **kwargs: _P.kwargs) -> Signature[_R_co]: ...
    def chunks(self, it: Iterable[Any], n: int) -> canvas.chunks: ...
    def map(self, it: Iterable[Any]) -> xmap: ...
    def starmap(self, it: Iterable[Any]) -> xstarmap: ...
    def send_event(
        self,
        type_: str,
        retry: bool = ...,
        retry_policy: Mapping[str, int] | None = ...,
        **fields: Any,
    ) -> list[tuple[object, object]]: ...
    def replace(self, sig: Signature[_SigR]) -> _SigR: ...
    @overload
    def add_to_chord(
        self, sig: Signature[Any], lazy: Literal[True]
    ) -> celery.result.AsyncResult[Any]: ...
    @overload
    def add_to_chord(
        self, sig: Signature[Any], lazy: Literal[False] = ...
    ) -> EagerResult[Any]: ...
    def update_state(
        self,
        task_id: str | None = ...,
        state: str | None = ...,
        meta: dict[str, Any] | None = ...,
        **kwargs: Any,
    ) -> None: ...
    def before_start(
        self, task_id: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None: ...
    def on_success(
        self, retval: Any, task_id: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None: ...
    def on_retry(
        self,
        exc: Exception,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: billiard.einfo.ExceptionInfo,
    ) -> None: ...
    def on_failure(
        self,
        exc: Exception,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: billiard.einfo.ExceptionInfo,
    ) -> None: ...
    def after_return(
        self,
        status: str,
        retval: Any,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: billiard.einfo.ExceptionInfo,
    ) -> None: ...
    def add_trail(self, result: Any) -> Any: ...
    def push_request(self, *args: _P.args, **kwargs: _P.kwargs) -> None: ...
    def pop_request(self) -> None: ...
    @property
    def request(self) -> Context: ...
    @property
    def backend(self) -> Backend: ...
    @backend.setter
    def backend(self, value: Backend) -> None: ...
    @property
    def __name__(self) -> str: ...

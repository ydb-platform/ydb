from collections.abc import Callable, Sequence
from datetime import datetime
from typing import (
    Any,
    Concatenate,
    Literal,
    TypeVar,
    overload,
)

from celery.app import beat as beat
from celery.app import control as control
from celery.app import events as events
from celery.app import task as task
from celery.app.task import Context, Task
from celery.utils.threads import _LocalStack
from typing_extensions import ParamSpec

_T = TypeVar("_T", bound=Task[Any, Any])
_P = ParamSpec("_P")
_R = TypeVar("_R")

@overload
def shared_task(fun: Callable[_P, _R]) -> Task[_P, _R]: ...
@overload
def shared_task(
    *,
    name: str = ...,
    serializer: str = ...,
    bind: Literal[False] = ...,
    autoretry_for: Sequence[type[BaseException]] = ...,
    dont_autoretry_for: Sequence[type[BaseException]] = ...,
    max_retries: int | None = ...,
    default_retry_delay: int = ...,
    acks_late: bool = ...,
    ignore_result: bool = ...,
    soft_time_limit: int = ...,
    time_limit: int = ...,
    base: None = ...,
    retry_kwargs: dict[str, Any] = ...,
    retry_backoff: bool | int = ...,
    retry_backoff_max: int = ...,
    retry_jitter: bool = ...,
    typing: bool = ...,
    rate_limit: str | None = ...,
    trail: bool = ...,
    send_events: bool = ...,
    store_errors_even_if_ignored: bool = ...,
    autoregister: bool = ...,
    track_started: bool = ...,
    acks_on_failure_or_timeout: bool = ...,
    reject_on_worker_lost: bool = ...,
    throws: tuple[type[Exception], ...] = ...,
    expires: float | datetime | None = ...,
    priority: int | None = ...,
    resultrepr_maxsize: int = ...,
    request_stack: _LocalStack[Context] = ...,
    abstract: bool = ...,
    queue: str = ...,
    pydantic: bool = ...,
) -> Callable[[Callable[_P, _R]], Task[_P, _R]]: ...
@overload
def shared_task(
    *,
    name: str = ...,
    serializer: str = ...,
    bind: Literal[True],
    autoretry_for: Sequence[type[BaseException]] = ...,
    dont_autoretry_for: Sequence[type[BaseException]] = ...,
    max_retries: int | None = ...,
    default_retry_delay: int = ...,
    acks_late: bool = ...,
    ignore_result: bool = ...,
    soft_time_limit: int = ...,
    time_limit: int = ...,
    base: None = ...,
    retry_kwargs: dict[str, Any] = ...,
    retry_backoff: bool | int = ...,
    retry_backoff_max: int = ...,
    retry_jitter: bool = ...,
    typing: bool = ...,
    rate_limit: str | None = ...,
    trail: bool = ...,
    send_events: bool = ...,
    store_errors_even_if_ignored: bool = ...,
    autoregister: bool = ...,
    track_started: bool = ...,
    acks_on_failure_or_timeout: bool = ...,
    reject_on_worker_lost: bool = ...,
    throws: tuple[type[Exception], ...] = ...,
    expires: float | datetime | None = ...,
    priority: int | None = ...,
    resultrepr_maxsize: int = ...,
    request_stack: _LocalStack[Context] = ...,
    abstract: bool = ...,
    queue: str = ...,
    pydantic: bool = ...,
) -> Callable[[Callable[Concatenate[Task[_P, _R], _P], _R]], Task[_P, _R]]: ...
@overload
def shared_task(
    *,
    name: str = ...,
    serializer: str = ...,
    bind: Literal[True],
    autoretry_for: Sequence[type[BaseException]] = ...,
    dont_autoretry_for: Sequence[type[BaseException]] = ...,
    max_retries: int | None = ...,
    default_retry_delay: int = ...,
    acks_late: bool = ...,
    ignore_result: bool = ...,
    soft_time_limit: int = ...,
    time_limit: int = ...,
    base: type[_T],
    retry_kwargs: dict[str, Any] = ...,
    retry_backoff: bool | int = ...,
    retry_backoff_max: int = ...,
    retry_jitter: bool = ...,
    typing: bool = ...,
    rate_limit: str | None = ...,
    trail: bool = ...,
    send_events: bool = ...,
    store_errors_even_if_ignored: bool = ...,
    autoregister: bool = ...,
    track_started: bool = ...,
    acks_on_failure_or_timeout: bool = ...,
    reject_on_worker_lost: bool = ...,
    throws: tuple[type[Exception], ...] = ...,
    expires: float | datetime | None = ...,
    priority: int | None = ...,
    resultrepr_maxsize: int = ...,
    request_stack: _LocalStack[Context] = ...,
    abstract: bool = ...,
    queue: str = ...,
    **options: Any,
) -> Callable[
    [
        Callable[
            Concatenate[_T, _P],
            _R,
        ]
        | Callable[
            [],
            _R,
        ]
    ],
    _T,
]: ...
@overload
def shared_task(
    *,
    name: str = ...,
    serializer: str = ...,
    bind: bool = ...,
    autoretry_for: Sequence[type[BaseException]] = ...,
    dont_autoretry_for: Sequence[type[BaseException]] = ...,
    max_retries: int | None = ...,
    default_retry_delay: int = ...,
    acks_late: bool = ...,
    ignore_result: bool = ...,
    soft_time_limit: int = ...,
    time_limit: int = ...,
    base: type[_T],
    retry_kwargs: dict[str, Any] = ...,
    retry_backoff: bool | int = ...,
    retry_backoff_max: int = ...,
    retry_jitter: bool = ...,
    typing: bool = ...,
    rate_limit: str | None = ...,
    trail: bool = ...,
    send_events: bool = ...,
    store_errors_even_if_ignored: bool = ...,
    autoregister: bool = ...,
    track_started: bool = ...,
    acks_on_failure_or_timeout: bool = ...,
    reject_on_worker_lost: bool = ...,
    throws: tuple[type[Exception], ...] = ...,
    expires: float | datetime | None = ...,
    priority: int | None = ...,
    resultrepr_maxsize: int = ...,
    request_stack: _LocalStack[Context] = ...,
    abstract: bool = ...,
    queue: str = ...,
    **options: Any,
) -> Callable[[Callable[..., Any]], _T]: ...

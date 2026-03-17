from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime
from types import TracebackType
from typing import (
    Any,
    Generic,
    Literal,
    TypeAlias,
    TypeVar,
)

import kombu
from celery.app.base import Celery
from celery.backends.base import Backend
from celery.utils.graph import DependencyGraph, GraphFormatter
from vine import promise

@contextmanager
def denied_join_result() -> Iterator[None]: ...
@contextmanager
def allow_join_result() -> Iterator[None]: ...

class ResultBase:
    parent: ResultBase | None

_State: TypeAlias = Literal["PENDING", "STARTED", "RETRY", "FAILURE", "SUCCESS"]

_R_co = TypeVar("_R_co", covariant=True)

class AsyncResult(ResultBase, Generic[_R_co]):
    app: Celery
    id: str
    backend: Backend
    parent: ResultBase | None
    on_ready: promise
    def __init__(
        self,
        id: str,
        backend: Backend | None = ...,
        task_name: str | None = ...,
        app: Celery | None = ...,
        parent: ResultBase | None = ...,
    ) -> None: ...
    @property
    def ignored(self) -> bool: ...
    @ignored.setter
    def ignored(self, value: bool) -> None: ...
    def then(
        self,
        callback: Callable[..., Any],
        on_error: Callable[..., Any] | None = ...,
        weak: bool = ...,
    ) -> promise: ...
    def as_tuple(
        self,
    ) -> tuple[int, tuple[int, Any | None, None] | None, None]: ...
    def forget(self) -> None: ...
    def revoke(
        self,
        connection: kombu.Connection | None = ...,
        terminate: bool = ...,
        signal: str | None = ...,
        wait: bool = ...,
        timeout: float | None = ...,
    ) -> None: ...
    def get(
        self,
        timeout: float | None = ...,
        propagate: bool = ...,
        interval: float = ...,
        no_ack: bool = ...,
        follow_parents: bool = ...,
        callback: Callable[..., Any] | None = ...,
        on_message: Callable[..., Any] | None = ...,
        on_interval: Callable[..., Any] | None = ...,
        disable_sync_subtasks: bool = ...,
        EXCEPTION_STATES: frozenset[str] = ...,
        PROPAGATE_STATES: frozenset[str] = ...,
    ) -> _R_co: ...
    def collect(
        self, intermediate: bool = ..., **kwargs: Any
    ) -> Iterator[tuple[AsyncResult[Any], object]]: ...
    def get_leaf(self) -> object: ...
    def iterdeps(
        self, intermediate: bool = ...
    ) -> Iterator[tuple[AsyncResult[Any] | None, AsyncResult[Any]]]: ...
    def ready(self) -> bool: ...
    def successful(self) -> bool: ...
    def failed(self) -> bool: ...
    def throw(self, *args: Any, **kwargs: Any) -> None: ...
    def maybe_throw(
        self, propagate: bool = ..., callback: Callable[..., Any] | None = ...
    ) -> object: ...
    def build_graph(
        self, intermediate: bool = ..., formatter: GraphFormatter | None = ...
    ) -> DependencyGraph: ...
    @property
    def graph(self) -> DependencyGraph: ...
    @property
    def supports_native_join(self) -> bool: ...
    @property
    def children(
        self,
    ) -> list[tuple[int, tuple[int, Any | None, None] | None, None]] | None: ...
    @property
    def result(self) -> _R_co | BaseException: ...
    @property
    def info(self) -> Any: ...
    @property
    def traceback(self) -> TracebackType | None: ...
    @property
    def state(self) -> _State: ...
    @property
    def status(
        self,
    ) -> _State: ...
    @property
    def task_id(self) -> str: ...
    @task_id.setter
    def task_id(self, id: str) -> None: ...
    @property
    def name(self) -> str | None: ...
    @property
    def args(self) -> tuple[Any, ...] | None: ...
    @property
    def kwargs(self) -> Mapping[str, Any] | None: ...
    @property
    def worker(self) -> str | None: ...
    @property
    def date_done(self) -> datetime | None: ...
    @property
    def retries(self) -> int | None: ...
    @property
    def queue(self) -> str | None: ...

class EagerResult(AsyncResult[_R_co]):
    def __init__(
        self,
        id: str,
        ret_value: _R_co,
        state: str,
        traceback: str | None = ...,
        name: str | None = ...,
    ) -> None: ...

class ResultSet(ResultBase):
    results: list[AsyncResult[Any]] | None
    app: Celery
    def __init__(
        self,
        results: list[AsyncResult[Any]] | None,
        app: Celery | None = ...,
        ready_barrier: Any | None = ...,
        **kwargs: Any,
    ) -> None: ...

class GroupResult(ResultSet):
    id: str | None
    def __init__(
        self,
        id: str | None = ...,
        results: list[AsyncResult[Any]] | None = ...,
        parent: ResultBase | None = ...,
        app: Celery | None = ...,
        **kwargs: Any,
    ) -> None: ...
    @classmethod
    def restore(
        cls,
        id: str,
        backend: Backend | None = ...,
        app: Celery | None = ...,
    ) -> GroupResult: ...
    def save(self, backend: Backend | None = ...) -> Any: ...
    def delete(self, backend: Backend | None = ...) -> None: ...
    @property
    def children(self) -> list[AsyncResult[Any]] | None: ...

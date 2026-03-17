from collections.abc import Callable, Generator, Sequence
from functools import partial
from typing import Any, TextIO, TypeVar

from celery.app.base import Celery
from celery.result import AsyncResult

_T = TypeVar("_T")

E_STILL_WAITING: str

humanize_seconds: partial[str]

class Sentinel(Exception): ...

class ManagerMixin:
    def _init_manager(
        self,
        block_timeout: float = 180,
        no_join: bool = False,
        stdout: TextIO | None = None,
        stderr: TextIO | None = None,
    ) -> None: ...
    def remark(self, s: str, sep: str = "-") -> None: ...
    def missing_results(self, r: Sequence[AsyncResult[Any]]) -> Sequence[str]: ...
    def wait_for(
        self,
        fun: Callable[..., _T],
        catch: Sequence[Any],
        desc: str = "thing",
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        errback: Callable[..., Any] | None = None,
        max_retries: int = 10,
        interval_start: float = 0.1,
        interval_step: float = 0.5,
        interval_max: float = 5.0,
        emit_warning: bool = False,
        **options: Any,
    ) -> _T: ...
    def ensure_not_for_a_while(
        self,
        fun: Callable[..., _T],
        catch: Sequence[Any],
        desc: str = "thing",
        max_retries: int = 20,
        interval_start: float = 0.1,
        interval_step: float = 0.02,
        interval_max: float = 1.0,
        emit_warning: bool = False,
        **options: Any,
    ) -> _T: ...
    def retry_over_time(self, *args: Any, **kwargs: Any) -> Any: ...
    def join(
        self,
        r: Any,
        propagate: bool = False,
        max_retries: int = 10,
        **kwargs: Any,
    ) -> Any: ...
    def inspect(self, timeout: float = 3.0) -> Any: ...
    def query_tasks(
        self, ids: Any, timeout: float = 0.5
    ) -> Generator[tuple[str, dict[str, tuple[str, Any]]], None, None]: ...
    def query_task_states(
        self, ids: list[str], timeout: float = 0.5
    ) -> dict[str, set[str]]: ...
    def assert_accepted(
        self,
        ids: list[str],
        interval: float = 0.5,
        desc: str = "waiting for tasks to be accepted",
        **policy: Any,
    ) -> bool | Sentinel: ...
    def assert_received(
        self,
        ids: list[str],
        interval: float = 0.5,
        desc: str = "waiting for tasks to be received",
        **policy: Any,
    ) -> bool | Sentinel: ...
    def assert_result_tasks_in_progress_or_completed(
        self,
        async_results: Sequence[AsyncResult[Any]],
        interval: float = 0.5,
        desc: str = "waiting for tasks to be started or completed",
        **policy: Any,
    ) -> bool | Sentinel: ...
    def assert_task_state_from_result(
        self,
        fun: Callable[..., _T],
        results: Sequence[AsyncResult[Any]],
        interval: float = 0.5,
        **policy: Any,
    ) -> _T | Sentinel: ...
    @staticmethod
    def is_result_task_in_progress(
        results: Sequence[AsyncResult[Any]], **kwargs: Any
    ) -> bool: ...
    def assert_task_worker_state(
        self,
        fun: Callable[..., _T],
        ids: list[str],
        interval: float = 0.5,
        **policy: Any,
    ) -> _T | Sentinel: ...
    def is_received(self, ids: list[str], **kwargs: Any) -> bool: ...
    def is_accepted(self, ids: list[str], **kwargs: Any) -> bool: ...
    def _ids_matches_state(
        self, expected_states: list[str], ids: list[str], timeout: float = 0.5
    ) -> bool: ...
    def true_or_raise(
        self, fun: Callable[..., _T], *args: Any, **kwargs: Any
    ) -> _T | Sentinel: ...
    def wait_until_idle(self) -> None: ...

class Manager(ManagerMixin):
    def __init__(self, app: Celery, **kwargs: Any) -> None: ...

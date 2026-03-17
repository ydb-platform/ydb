from collections.abc import Callable
from datetime import timedelta
from typing import Any, NamedTuple

from celery.app.task import Context
from celery.result import ResultSet
from typing_extensions import override

class pending_results_t(NamedTuple):
    concrete: dict[Any, Any]
    weak: dict[Any, Any]

class Backend:
    READY_STATES: frozenset[str]
    UNREADY_STATES: frozenset[str]
    EXCEPTION_STATES: frozenset[str]

    subpolling_interval: float | None
    supports_native_join: bool
    supports_autoexpire: bool
    persistent: bool

    retry_policy: dict[str, int | float]
    def __init__(
        self,
        app: Any,
        serializer: str | None = ...,
        max_cached_results: int | None = ...,
        accept: list[str] | None = ...,
        expires: float | timedelta | None = ...,
        expires_type: Callable[[Any], Any] | None = ...,
        url: str | None = ...,
        **kwargs: Any,
    ) -> None: ...
    def as_uri(self, include_password: bool = ...) -> str: ...
    def mark_as_started(self, task_id: str, **meta: Any) -> None: ...
    def mark_as_done(
        self,
        task_id: str,
        result: Any,
        request: Any = ...,
        store_result: bool = ...,
        state: str = ...,
    ) -> None: ...
    def mark_as_failure(
        self,
        task_id: str,
        exc: Exception,
        traceback: str | None = ...,
        request: Any = ...,
        store_result: bool = ...,
        call_errbacks: bool = ...,
        state: str = ...,
    ) -> None: ...
    def mark_as_revoked(
        self,
        task_id: str,
        reason: str = ...,
        request: Any = ...,
        store_result: bool = ...,
        state: str = ...,
    ) -> None: ...
    def mark_as_retry(
        self,
        task_id: str,
        exc: Exception,
        traceback: str | None = ...,
        request: Any = ...,
        store_result: bool = ...,
        state: str = ...,
    ) -> None: ...
    def store_result(
        self,
        task_id: str,
        result: Any,
        state: str,
        traceback: str | None = ...,
        request: Any = ...,
        **kwargs: Any,
    ) -> Any: ...
    def forget(self, task_id: str) -> None: ...
    def get_state(self, task_id: str) -> str: ...
    def get_result(self, task_id: str) -> Any: ...
    def get_children(self, task_id: str) -> list[Any] | None: ...
    def reload_task_result(self, task_id: str) -> None: ...
    def reload_group_result(self, group_id: str) -> None: ...
    def get_group_meta(self, group_id: str, cache: bool = ...) -> Any: ...
    def restore_group(self, group_id: str, cache: bool = ...) -> ResultSet | None: ...
    def cleanup(self) -> None: ...
    def process_cleanup(self) -> None: ...
    def apply_chord(
        self, header_result_args: tuple[Any, Any], body: Any, **kwargs: Any
    ) -> None: ...
    def current_task_children(self, request: Context | None = ...) -> list[Any]: ...

class SyncBackendMixin:
    def iter_native(
        self,
        result: ResultSet,
        timeout: float | None = ...,
        interval: float = ...,
        no_ack: bool = ...,
        on_message: Callable[[Any], None] | None = ...,
        on_interval: Callable[[], None] | None = ...,
    ) -> None: ...
    def wait_for_pending(
        self,
        result: ResultSet,
        timeout: float | None = ...,
        interval: float = ...,
        no_ack: bool = ...,
        on_message: Callable[[Any], None] | None = ...,
        on_interval: Callable[[], None] | None = ...,
        callback: Callable[[Any], Any] | None = ...,
        propagate: bool = ...,
    ) -> None: ...
    def wait_for(
        self,
        task_id: str,
        timeout: float | None = ...,
        interval: float = ...,
        no_ack: bool = ...,
        on_interval: Callable[[], None] | None = ...,
    ) -> Any: ...

class BaseBackend(Backend, SyncBackendMixin): ...

class BaseKeyValueStoreBackend(Backend):
    def get(self, key: str) -> Any: ...
    def mget(self, keys: list[str]) -> list[Any]: ...
    def set(self, key: str, value: Any) -> None: ...
    def delete(self, key: str) -> None: ...
    def incr(self, key: str) -> int: ...

class KeyValueStoreBackend(BaseKeyValueStoreBackend, SyncBackendMixin): ...

class DisabledBackend(BaseBackend):
    @override
    def store_result(self, *args: Any, **kwargs: Any) -> None: ...

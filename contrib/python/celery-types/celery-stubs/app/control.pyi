from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Literal, TypeAlias, TypedDict

from celery.app.base import Celery
from celery.result import _State
from kombu import Connection
from kombu.pidbox import Mailbox as KombuMailbox

_Reply: TypeAlias = Any

_Destination: TypeAlias = Sequence[str] | tuple[str, ...]

class _ReportReply(TypedDict):
    ok: str

class _ClockReply(TypedDict):
    clock: int

class _TaskInfo(TypedDict):
    id: str
    name: str
    args: list[Any]
    kwargs: dict[str, Any]
    type: str
    hostname: str
    time_start: float
    acknowledged: bool
    delivery_info: dict[str, Any]
    worker_pid: int

class _TaskScheduledInfo(TypedDict):
    eta: str
    priority: int
    request: _TaskInfo

class _StatInfo(TypedDict):
    broker: dict[str, Any]
    clock: int
    uptime: int
    pid: int
    pool: dict[str, Any]
    prefetch_count: int
    rusage: dict[str, Any]
    total: dict[str, int]

class _PingReply(TypedDict):
    ok: Literal["pong"]

class _QueueInfo(TypedDict):
    name: str
    exchange: dict[str, Any]
    routing_key: str
    queue_arguments: None | list[Any]
    binding_arguments: None | list[Any]
    consumer_arguments: None | list[Any]
    durable: bool
    exclusive: bool
    auto_delete: bool
    no_ack: bool
    alias: None | str
    bindings: list[Any]
    no_declare: bool
    expires: None | int
    message_ttl: None | int
    max_length: None | int
    max_length_bytes: None | int
    max_priority: None | int

def flatten_reply(reply: Iterable[Mapping[str, _Reply]]) -> dict[str, _Reply]: ...

class Inspect:
    app: Celery | None
    def __init__(
        self,
        destination: _Destination | None = ...,
        timeout: float = ...,
        callback: Callable[..., Any] | None = ...,
        connection: Connection | None = ...,
        app: Celery | None = ...,
        limit: int | None = ...,
        pattern: str | None = ...,
        matcher: Callable[..., Any] | None = ...,
    ) -> None: ...
    def _prepare(self, reply: bool) -> None | dict[str, _Reply]: ...
    def _request(
        self, command: str, **kwargs: Any
    ) -> None | list[dict[str, _Reply]]: ...
    def report(self) -> None | dict[str, _ReportReply]: ...
    def clock(self) -> None | dict[str, _ClockReply]: ...
    def active(self, safe: Any | None = ...) -> None | dict[str, list[_TaskInfo]]: ...
    def scheduled(
        self, safe: Any | None = ...
    ) -> None | dict[str, list[_TaskScheduledInfo]]: ...
    def reserved(self, safe: Any | None = ...) -> None | dict[str, list[_TaskInfo]]: ...
    def stats(self) -> None | dict[str, _StatInfo]: ...
    def revoked(self) -> None | dict[str, list[str]]: ...
    def registered(self, *taskinfoitems: Any) -> None | dict[str, list[str]]: ...
    registered_tasks = registered
    def ping(
        self, destination: _Destination | None = ...
    ) -> None | dict[str, _PingReply]: ...
    def active_queues(self) -> None | dict[str, list[_QueueInfo]]: ...
    def query_task(
        self, *ids: str
    ) -> None | dict[str, dict[str, tuple[_State, _TaskInfo]]]: ...
    def conf(self, with_defaults: bool = ...) -> None | dict[str, dict[str, Any]]: ...
    def hello(
        self, from_node: Any, revoked: Any | None = ...
    ) -> None | dict[str, _Reply]: ...
    def memsample(self) -> None | dict[str, _Reply]: ...
    def memdump(self, samples: int = ...) -> None | dict[str, _Reply]: ...
    def objgraph(
        self, type: str = ..., n: int = ..., max_depth: int = ...
    ) -> None | dict[str, dict[str, str]]: ...

class Control:
    Mailbox: KombuMailbox
    app: Celery | None
    def __init__(self, app: Celery | None = ...) -> None: ...
    def inspect(
        self,
        destination: _Destination | None = ...,
        timeout: float = ...,
        callback: Callable[..., Any] | None = ...,
        connection: Connection | None = ...,
        app: Celery | None = ...,
        limit: int | None = ...,
        pattern: str | None = ...,
        matcher: Callable[..., Any] | None = ...,
    ) -> Inspect: ...
    def purge(self, connection: Connection | None = ...) -> int: ...
    discard_all = purge
    def election(
        self,
        id: str,
        topic: str,
        action: str | None = ...,
        connection: Connection | None = ...,
    ) -> None: ...
    def revoke(
        self,
        task_id: str | Sequence[str],
        destination: _Destination | None = ...,
        terminate: bool = ...,
        signal: str = ...,
        **kwargs: Any,
    ) -> list[dict[str, _Reply]] | None: ...
    def terminate(
        self,
        task_id: str,
        destination: _Destination | None = ...,
        signal: str = ...,
        **kwargs: Any,
    ) -> list[dict[str, _Reply]] | None: ...
    def ping(
        self,
        destination: _Destination | None = ...,
        timeout: float = ...,
        **kwargs: Any,
    ) -> list[dict[str, _Reply]] | None: ...
    def rate_limit(
        self,
        task_name: str,
        rate_limit: int | str,
        destination: _Destination | None = ...,
        **kwargs: Any,
    ) -> list[dict[str, _Reply]] | None: ...
    def add_consumer(
        self,
        queue: str,
        exchange: str | None = ...,
        exchange_type: str = ...,
        routing_key: str | None = ...,
        options: Mapping[str, Any] | None = ...,
        destination: _Destination | None = ...,
        **kwargs: Any,
    ) -> list[dict[str, _Reply]] | None: ...
    def cancel_consumer(
        self, queue: str, destination: _Destination | None = ..., **kwargs: Any
    ) -> list[dict[str, _Reply]] | None: ...
    def time_limit(
        self,
        task_name: str,
        soft: float | None = ...,
        hard: float | None = ...,
        destination: _Destination | None = ...,
        **kwargs: Any,
    ) -> list[dict[str, _Reply]] | None: ...
    def enable_events(
        self, destination: _Destination | None = ..., **kwargs: Any
    ) -> list[dict[str, _Reply]] | None: ...
    def disable_events(
        self, destination: _Destination | None = ..., **kwargs: Any
    ) -> list[dict[str, _Reply]] | None: ...
    def pool_grow(
        self, n: int = ..., destination: _Destination | None = ..., **kwargs: Any
    ) -> list[dict[str, _Reply]] | None: ...
    def pool_shrink(
        self, n: int = ..., destination: _Destination | None = ..., **kwargs: Any
    ) -> list[dict[str, _Reply]] | None: ...
    def autoscale(
        self, max: int, min: int, destination: _Destination | None = ..., **kwargs: Any
    ) -> list[dict[str, _Reply]] | None: ...
    def shutdown(
        self, destination: _Destination | None = ..., **kwargs: Any
    ) -> list[dict[str, _Reply]] | None: ...
    def pool_restart(
        self,
        modules: Sequence[str] | None = ...,
        reload: bool = ...,
        reloader: Callable[..., Any] | None = ...,
        destination: _Destination | None = ...,
        **kwargs: Any,
    ) -> list[dict[str, _Reply]] | None: ...
    def heartbeat(
        self, destination: _Destination | None = ..., **kwargs: Any
    ) -> list[dict[str, _Reply]] | None: ...
    def broadcast(
        self,
        command: str,
        arguments: Mapping[str, Any] | None = ...,
        destination: _Destination | None = ...,
        connection: Connection | None = ...,
        reply: bool = ...,
        timeout: float = ...,
        limit: int | None = ...,
        callback: Callable[..., Any] | None = ...,
        channel: Any | None = ...,
        pattern: str | None = ...,
        matcher: Callable[..., Any] = ...,
        **extra_kwargs: Any,
    ) -> list[dict[str, _Reply]] | None: ...

import datetime
from collections import defaultdict
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import (
    Any,
    Concatenate,
    Generic,
    Literal,
    NoReturn,
    overload,
)

import celery.app
import celery.result
import kombu
from celery.app.amqp import AMQP
from celery.app.beat import Beat as CeleryBeat
from celery.app.control import Control
from celery.app.events import Events
from celery.app.log import Logging
from celery.app.registry import TaskRegistry
from celery.app.routes import Router
from celery.app.task import Context
from celery.app.task import Task as CeleryTask
from celery.app.utils import Settings
from celery.apps.worker import Worker as CeleryWorker
from celery.backends.base import Backend
from celery.canvas import Signature, chord
from celery.loaders.base import BaseLoader
from celery.schedules import BaseSchedule
from celery.utils.dispatch import Signal
from celery.utils.objects import FallbackContext
from celery.utils.threads import _LocalStack
from celery.worker import WorkController as CeleryWorkController
from typing_extensions import ParamSpec, Self, TypeVar

_T = TypeVar("_T", bound=CeleryTask[Any, Any])
_T_Global = TypeVar(
    "_T_Global",
    bound=CeleryTask[Any, Any],
    default=CeleryTask[Any, Any],
)
_P = ParamSpec("_P")
_R = TypeVar("_R")

class Celery(Generic[_T_Global]):
    steps: defaultdict[str, set[Any]]

    on_configure: Signal
    on_after_configure: Signal
    on_after_finalize: Signal
    on_after_fork: Signal
    def __init__(
        self,
        main: str | None = ...,
        loader: Any | None = ...,
        backend: str | type[Backend] | None = ...,
        amqp: str | type[AMQP] | None = ...,
        events: str | type[celery.app.events.Events] | None = ...,
        log: str | type[Logging] | None = ...,
        control: str | type[celery.app.control.Control] | None = ...,
        set_as_current: bool = ...,
        tasks: str | type[TaskRegistry] | None = ...,
        broker: str | None = ...,
        imports: list[str] | tuple[str, ...] | None = ...,
        include: list[str] | tuple[str, ...] | None = ...,
        changes: dict[str, Any] | None = ...,
        config_source: str | object | None = ...,
        fixups: list[str] | None = ...,
        task_cls: str | type[_T_Global] | None = ...,
        autofinalize: bool = ...,
        namespace: str | None = ...,
        strict_typing: bool = ...,
        broker_connection_retry: bool = ...,
        broker_connection_max_retries: int = ...,
        broker_channel_error_retry: bool = ...,
        broker_login_method: str = ...,
        broker_transport_options: dict[str, Any] = ...,
        broker_connection_retry_on_startup: bool = ...,
        broker_connection_timeout: float = ...,
        result_backend_transport_options: dict[str, Any] | None = ...,
        result_extended: bool = ...,
        result_expires: datetime.timedelta = ...,
        beat_schedule: dict[str, Any] | None = ...,
        task_queues: list[kombu.Queue] | None = ...,
        task_default_queue: str = ...,
        task_default_exchange: str = ...,
        task_default_exchange_type: str = ...,
        task_default_routing_key: str = ...,
        task_default_delivery_mode: str = ...,
        task_create_missing_queues: bool = ...,
        task_routes: dict[Any, Any] | Sequence[Any] = ...,
        task_acks_late: bool = ...,
        task_time_limit: int = ...,
        task_soft_time_limit: int = ...,
        task_track_started: bool = ...,
        task_serializer: str = ...,
        worker_prefetch_multiplier: int = ...,
        worker_max_tasks_per_child: int = ...,
        worker_concurrency: int = ...,
        worker_max_memory_per_child: int = ...,
        worker_disable_rate_limits: bool = ...,
        worker_cancel_long_running_tasks_on_connection_loss: bool = ...,
        worker_hijack_root_logger: bool = ...,
        worker_log_format: str = ...,
        worker_task_log_format: str = ...,
        worker_redirect_stdouts: bool = ...,
        worker_redirect_stdouts_level: str = ...,
    ) -> None: ...
    def _task_from_fun(
        self,
        fun: Callable[_P, _R],
        name: str | None = ...,
        base: type[_T_Global] | None = ...,
        bind: bool = ...,
        # options
        autoretry_for: Sequence[type[BaseException]] = ...,
        dont_autoretry_for: Sequence[type[BaseException]] = ...,
        retry_kwargs: dict[str, Any] = ...,
        retry_backoff: bool | int = ...,
        retry_backoff_max: int = ...,
        retry_jitter: bool = ...,
        # from task
        typing: bool = ...,
        max_retries: int | None = ...,
        default_retry_delay: int = ...,
        rate_limit: str | None = ...,
        ignore_result: bool = ...,
        trail: bool = ...,
        send_events: bool = ...,
        store_errors_even_if_ignored: bool = ...,
        serializer: str = ...,
        time_limit: int | None = ...,
        soft_time_limit: int | None = ...,
        autoregister: bool = ...,
        track_started: bool = ...,
        acks_late: bool = ...,
        acks_on_failure_or_timeout: bool = ...,
        reject_on_worker_lost: bool = ...,
        throws: tuple[type[Exception], ...] = ...,
        expires: float | datetime.datetime | None = ...,
        priority: int | None = ...,
        resultrepr_maxsize: int = ...,
        request_stack: _LocalStack[Context] = ...,
        abstract: bool = ...,
        after_return: Callable[..., Any] = ...,
        on_retry: Callable[..., Any] = ...,
    ) -> _T_Global: ...
    def on_init(self) -> None: ...
    def set_current(self) -> None: ...
    def set_default(self) -> None: ...
    def close(self) -> None: ...
    def start(self, argv: list[str] | None = ...) -> NoReturn: ...
    def worker_main(self, argv: list[str] | None = ...) -> NoReturn: ...
    @overload
    def task(self, fun: Callable[_P, _R]) -> _T_Global: ...
    @overload
    def task(
        self,
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
        expires: float | datetime.datetime | None = ...,
        priority: int | None = ...,
        resultrepr_maxsize: int = ...,
        request_stack: _LocalStack[Context] = ...,
        abstract: bool = ...,
        queue: str = ...,
        after_return: Callable[..., Any] = ...,
        on_retry: Callable[..., Any] = ...,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], _T]: ...
    @overload
    def task(
        self,
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
        expires: float | datetime.datetime | None = ...,
        priority: int | None = ...,
        resultrepr_maxsize: int = ...,
        request_stack: _LocalStack[Context] = ...,
        abstract: bool = ...,
        queue: str = ...,
        after_return: Callable[..., Any] = ...,
        on_retry: Callable[..., Any] = ...,
        **options: Any,
    ) -> Callable[[Callable[_P, _R]], _T_Global]: ...
    @overload
    def task(
        self,
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
        expires: float | datetime.datetime | None = ...,
        priority: int | None = ...,
        resultrepr_maxsize: int = ...,
        request_stack: _LocalStack[Context] = ...,
        abstract: bool = ...,
        queue: str = ...,
        after_return: Callable[..., Any] = ...,
        on_retry: Callable[..., Any] = ...,
        **options: Any,
    ) -> Callable[[Callable[Concatenate[_T_Global, _P], _R]], _T_Global]: ...
    def register_task(self, task: _T | type[_T], **options: Any) -> _T: ...
    def gen_task_name(self, name: str, module: str) -> str: ...
    def finalize(self, auto: bool = ...) -> None: ...
    def add_defaults(self, fun: Callable[[], dict[str, Any]]) -> None: ...
    def config_from_object(
        self,
        obj: Any,
        silent: bool = ...,
        force: bool = ...,
        namespace: str | None = ...,
    ) -> Settings: ...
    def config_from_envvar(
        self, variable_name: str, silent: bool = ..., force: bool = ...
    ) -> None: ...
    def config_from_cmdline(self, argv: list[str], namespace: str = ...) -> None: ...
    def setup_security(
        self,
        allowed_serializers: set[str] | None = ...,
        key: str | None = ...,
        cert: str | None = ...,
        store: str | None = ...,
        digest: str = ...,
        serializer: str = ...,
    ) -> None: ...
    def autodiscover_tasks(
        self,
        packages: list[str] | Callable[[], list[str]] | None = ...,
        related_name: str = ...,
        force: bool = ...,
    ) -> None: ...
    def send_task(
        self,
        name: str,
        args: Sequence[Any] | None = ...,
        kwargs: dict[str, Any] | None = ...,
        countdown: float | None = ...,
        eta: datetime.datetime | None = ...,
        task_id: str | None = ...,
        producer: kombu.Producer | None = ...,
        connection: kombu.Connection | None = ...,
        router: Router | None = ...,
        result_cls: type[celery.result.AsyncResult[Any]] | None = ...,
        expires: float | datetime.datetime | None = ...,
        publisher: kombu.Producer | None = ...,
        link: Signature[Any] | None = ...,
        link_error: Signature[Any] | None = ...,
        add_to_parent: bool = ...,
        group_id: str | None = ...,
        retries: int = ...,
        chord: chord | None = ...,
        reply_to: str | None = ...,
        time_limit: int | None = ...,
        soft_time_limit: int | None = ...,
        root_id: str | None = ...,
        parent_id: str | None = ...,
        route_name: str | None = ...,
        shadow: str | None = ...,
        chain: Any | None = ...,
        task_type: Any | None = ...,
        # options
        ignore_result: bool = ...,
        **options: Any,
    ) -> celery.result.AsyncResult[Any]: ...
    def connection_for_read(
        self, url: str | None = ..., **kwargs: Any
    ) -> kombu.Connection: ...
    def connection_for_write(
        self, url: str | None = ..., **kwargs: Any
    ) -> kombu.Connection: ...
    def connection(
        self,
        hostname: str | None = ...,
        userid: str | None = ...,
        password: str | None = ...,
        virtual_host: str | None = ...,
        port: int | None = ...,
        ssl: bool | dict[str, Any] | None = ...,
        connect_timeout: int | None = ...,
        transport: str | None = ...,
        transport_options: dict[str, Any] | None = ...,
        heartbeat: int | None = ...,
        login_method: int | None = ...,
        failover_strategy: str | Callable[[], Any] | None = ...,
        **kwargs: Any,
    ) -> kombu.Connection: ...
    broker_connection = connection
    def connection_or_acquire(
        self, connection: kombu.Connection | None = ..., pool: bool = ...
    ) -> FallbackContext[Any, Any]: ...
    default_connection = connection_or_acquire
    def producer_or_acquire(
        self, producer: kombu.Producer | None = ...
    ) -> FallbackContext[Any, Any]: ...
    default_producer = producer_or_acquire
    def prepare_config(self, c: Settings) -> Settings: ...
    def now(self) -> datetime.datetime: ...
    def select_queues(self, queues: Sequence[str] | None = ...) -> None: ...
    def either(self, default_key: str, *defaults: Any) -> Any: ...
    def bugreport(self) -> str: ...
    def signature(self, *args: Any, **kwargs: Any) -> Signature[Any]: ...
    def add_periodic_task(
        self,
        schedule: BaseSchedule | float,
        sig: Signature[Any],
        args: tuple[Any, ...] = ...,
        kwargs: dict[str, Any] = ...,
        name: str | None = ...,
        **opts: Any,
    ) -> str: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        typ: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    @property
    def Worker(self) -> type[CeleryWorker]: ...
    @property
    def WorkController(self) -> type[CeleryWorkController]: ...
    @property
    def Beat(self) -> type[CeleryBeat]: ...
    @property
    def Task(self) -> type[_T_Global]: ...
    @property
    def annotations(self) -> list[dict[str, Any]]: ...
    @property
    def AsyncResult(self) -> type[celery.result.AsyncResult[Any]]: ...
    @property
    def ResultSet(self) -> type[celery.result.ResultSet]: ...
    @property
    def GroupResult(self) -> type[celery.result.GroupResult]: ...
    @property
    def pool(self) -> kombu.pools.ProducerPool: ...
    @property
    def current_task(self) -> _T_Global | None: ...
    @property
    def current_worker_task(self) -> _T_Global | None: ...
    @property
    def oid(self) -> str: ...
    @property
    def amqp(self) -> AMQP: ...
    @property
    def backend(self) -> Backend: ...
    @property
    def conf(self) -> Settings: ...
    @conf.setter
    def conf(self, d: Any) -> None: ...
    @property
    def control(self) -> Control: ...
    @property
    def events(self) -> Events: ...
    @property
    def loader(self) -> BaseLoader: ...
    @property
    def log(self) -> Logging: ...
    @property
    def tasks(self) -> TaskRegistry: ...
    @property
    def producer_pool(self) -> kombu.pools.ProducerPool: ...
    def uses_utc_timezone(self) -> bool: ...
    @property
    def timezone(self) -> datetime.timezone: ...

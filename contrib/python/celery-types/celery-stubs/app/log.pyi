from logging import Logger, LogRecord
from typing import Any

from celery.app.base import Celery
from celery.local import class_property
from celery.utils.log import ColorFormatter, LoggingProxy
from celery.utils.term import colored
from typing_extensions import override

__all__ = ("Logging", "TaskFormatter")

class TaskFormatter(ColorFormatter):
    @override
    def format(self, record: LogRecord) -> str: ...

class Logging:
    app: Celery
    loglevel: int
    format: str
    task_format: str
    colorize: bool
    def __init__(self, app: Celery) -> None: ...
    def setup(
        self,
        loglevel: int | None = ...,
        logfile: str | None = ...,
        redirect_stdouts: bool = ...,
        redirect_level: str = ...,
        colorize: bool | None = ...,
        hostname: str | None = ...,
    ) -> list[tuple[Any, Any]]: ...
    def redirect_stdouts(self, loglevel: int | None = ..., name: str = ...) -> None: ...
    def setup_logging_subsystem(
        self,
        loglevel: str | None = ...,
        logfile: Any | None = ...,
        format: str | None = ...,
        colorize: bool | None = ...,
        hostname: str | None = ...,
        **kwargs: Any,
    ) -> Any: ...
    def setup_task_loggers(
        self,
        loglevel: int | str | None = ...,
        logfile: Any | None = ...,
        format: str | None = ...,
        colorize: bool | None = ...,
        propagate: bool = ...,
        **kwargs: Any,
    ) -> Logger: ...
    def redirect_stdouts_to_logger(
        self,
        logger: Logger,
        loglevel: int | str | None = ...,
        stdout: bool = ...,
        stderr: bool = ...,
    ) -> LoggingProxy: ...
    def supports_color(
        self, colorize: bool | None = ..., logfile: Any | None = ...
    ) -> bool: ...
    def colored(
        self, logfile: Any | None = ..., enabled: bool | None = ...
    ) -> colored: ...
    def setup_handlers(
        self,
        logger: Logger,
        logfile: Any,
        format: str,
        colorize: bool,
        formatter: Any = ...,
        **kwargs: Any,
    ) -> Logger: ...
    def get_default_logger(self, name: str = ..., **kwargs: Any) -> Logger: ...
    @class_property
    def already_setup(self) -> bool: ...  # pyright: ignore[reportRedeclaration]
    @already_setup.setter  # type: ignore[no-redef]
    def already_setup(self, was_setup: bool) -> None: ...

from __future__ import annotations

import dataclasses
import functools
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, overload
from weakref import WeakSet

from opentelemetry import trace
from opentelemetry._logs import Logger, LoggerProvider, LogRecord, NoOpLoggerProvider, SeverityNumber

from logfire._internal.constants import LEVEL_NUMBERS

if TYPE_CHECKING:
    from opentelemetry.util.types import _ExtendedAttributes  # type: ignore


@dataclass
class ProxyLoggerProvider(LoggerProvider):
    """A logger provider that wraps another internal logger provider allowing it to be re-assigned."""

    provider: LoggerProvider

    loggers: WeakSet[ProxyLogger] = dataclasses.field(default_factory=WeakSet)  # type: ignore[reportUnknownVariableType]
    lock: Lock = dataclasses.field(default_factory=Lock)
    suppressed_scopes: set[str] = dataclasses.field(default_factory=set)  # type: ignore[reportUnknownVariableType]
    min_level: int = 0

    def get_logger(
        self,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
        attributes: _ExtendedAttributes | None = None,
    ) -> Logger:
        with self.lock:
            if name in self.suppressed_scopes:
                provider = NoOpLoggerProvider()
            else:
                provider = self.provider
            inner_logger = provider.get_logger(name, version, schema_url, attributes)
            logger = ProxyLogger(inner_logger, self.min_level, name, version, schema_url, attributes)
            self.loggers.add(logger)
            return logger

    def set_min_level(self, min_level: int) -> None:
        with self.lock:
            self.min_level = min_level
            for logger in self.loggers:
                logger.min_level = min_level

    def suppress_scopes(self, *scopes: str) -> None:
        with self.lock:
            self.suppressed_scopes.update(scopes)
            for logger in self.loggers:
                if logger.name in scopes:
                    logger.set_logger(NoOpLoggerProvider())

    def set_provider(self, logger_provider: LoggerProvider) -> None:
        with self.lock:
            self.provider = logger_provider
            for logger in self.loggers:
                logger.set_logger(NoOpLoggerProvider() if logger.name in self.suppressed_scopes else logger_provider)

    def __getattr__(self, item: str) -> Any:
        try:
            result = getattr(self.provider, item)
        except AttributeError:
            if item in ['shutdown', 'force_flush']:
                # These methods don't exist on the default NoOpLoggerProvider
                return lambda *_, **__: None  # type: ignore
            raise  # pragma: no cover

        if callable(result):

            @functools.wraps(result)
            def wrapper(*args: Any, **kwargs: Any):
                with self.lock:
                    return result(*args, **kwargs)

            return wrapper
        else:
            return result


@dataclass(eq=False)
class ProxyLogger(Logger):
    logger: Logger
    min_level: int
    name: str
    version: str | None = None
    schema_url: str | None = None
    attributes: _ExtendedAttributes | None = None

    @overload
    def emit(self, record: LogRecord) -> None: ...

    @overload
    def emit(self, **kwargs: Any) -> None: ...

    def emit(self, record: LogRecord | None = None, **kwargs: Any) -> None:  # type: ignore
        # If record is not provided, create one from kwargs
        if record is None:
            record = LogRecord(**kwargs)

        if record.severity_number is not None:
            if record.severity_number.value < self.min_level:
                return
        elif record.severity_text and (level_name := record.severity_text.lower()) in LEVEL_NUMBERS:
            level_number = LEVEL_NUMBERS[level_name]
            if level_number < self.min_level:
                return
            # record.severity_number is known to be None here, so we can safely set it
            record.severity_number = SeverityNumber(level_number)

        if not record.trace_id:
            span_context = trace.get_current_span().get_span_context()
            record.trace_id = span_context.trace_id
            record.span_id = span_context.span_id
            record.trace_flags = span_context.trace_flags
        self.logger.emit(record)

    def set_logger(self, provider: LoggerProvider) -> None:
        self.logger = provider.get_logger(self.name, self.version, self.schema_url, self.attributes)

    def __getattr__(self, item: str):
        return getattr(self.logger, item)

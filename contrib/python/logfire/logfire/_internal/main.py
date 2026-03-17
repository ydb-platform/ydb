from __future__ import annotations

import contextlib
import inspect
import json
import sys
import warnings
from collections.abc import Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from contextvars import Token
from enum import Enum
from functools import cached_property, partial
from time import time
from typing import (  # NOQA UP035
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import opentelemetry.context as context_api
import opentelemetry.trace as trace_api
from opentelemetry.context import Context
from opentelemetry.metrics import CallbackT, Counter, Histogram, UpDownCounter
from opentelemetry.sdk.trace import ReadableSpan, Span
from opentelemetry.trace import SpanContext, SpanKind
from opentelemetry.util import types as otel_types
from typing_extensions import LiteralString, ParamSpec

from ..version import VERSION
from . import async_
from .auto_trace import AutoTraceModule, install_auto_tracing
from .config import GLOBAL_CONFIG, LogfireConfig
from .config_params import PydanticPluginRecordValues
from .constants import (
    ATTRIBUTES_JSON_SCHEMA_KEY,
    ATTRIBUTES_LOG_LEVEL_NUM_KEY,
    ATTRIBUTES_MESSAGE_KEY,
    ATTRIBUTES_MESSAGE_TEMPLATE_KEY,
    ATTRIBUTES_SAMPLE_RATE_KEY,
    ATTRIBUTES_SPAN_TYPE_KEY,
    ATTRIBUTES_TAGS_KEY,
    DISABLE_CONSOLE_KEY,
    LEVEL_NUMBERS,
    OTLP_MAX_INT_SIZE,
    LevelName,
    log_level_attributes,
)
from .formatter import logfire_format, logfire_format_with_magic
from .instrument import instrument
from .json_encoder import logfire_json_dumps
from .json_schema import (
    JsonSchemaProperties,
    attributes_json_schema,
    attributes_json_schema_properties,
    create_json_schema,
)
from .metrics import ProxyMeterProvider
from .stack_info import get_user_stack_info
from .tracer import (
    ProxyTracerProvider,
    _LogfireWrappedSpan,  # type: ignore
    _ProxyTracer,  # type: ignore
    set_exception_status,
)
from .utils import get_version, handle_internal_errors, log_internal_error, uniquify_sequence

if TYPE_CHECKING:
    from types import ModuleType
    from wsgiref.types import WSGIApplication

    import anthropic
    import httpx
    import openai
    import pydantic_ai.models
    import requests
    from django.http import HttpRequest, HttpResponse
    from fastapi import FastAPI
    from flask.app import Flask
    from opentelemetry.instrumentation.asgi.types import ClientRequestHook, ClientResponseHook, ServerRequestHook
    from opentelemetry.metrics import _Gauge as Gauge
    from pymongo.monitoring import CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent
    from sqlalchemy import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.websockets import WebSocket
    from surrealdb.connections.async_template import AsyncTemplate
    from surrealdb.connections.sync_template import SyncTemplate
    from typing_extensions import Unpack

    from ..integrations.aiohttp_client import (
        RequestHook as AiohttpClientRequestHook,
        ResponseHook as AiohttpClientResponseHook,
    )
    from ..integrations.flask import (
        CommenterOptions as FlaskCommenterOptions,
        RequestHook as FlaskRequestHook,
        ResponseHook as FlaskResponseHook,
    )
    from ..integrations.httpx import (
        AsyncRequestHook as HttpxAsyncRequestHook,
        AsyncResponseHook as HttpxAsyncResponseHook,
        RequestHook as HttpxRequestHook,
        ResponseHook as HttpxResponseHook,
    )
    from ..integrations.psycopg import CommenterOptions as PsycopgCommenterOptions
    from ..integrations.redis import RequestHook as RedisRequestHook, ResponseHook as RedisResponseHook
    from ..integrations.sqlalchemy import CommenterOptions as SQLAlchemyCommenterOptions
    from ..integrations.wsgi import RequestHook as WSGIRequestHook, ResponseHook as WSGIResponseHook
    from ..variables import (
        ResolveFunction,
        ValidationReport,
        Variable,
        VariablesConfig,
    )
    from .integrations.asgi import ASGIApp, ASGIInstrumentKwargs
    from .integrations.aws_lambda import LambdaEvent, LambdaHandler
    from .integrations.llm_providers.semconv import SemconvVersion
    from .integrations.mysql import MySQLConnection
    from .integrations.psycopg import Psycopg2Connection, PsycopgConnection
    from .integrations.sqlite3 import SQLite3Connection
    from .integrations.system_metrics import Base as SystemMetricsBase, Config as SystemMetricsConfig
    from .utils import SysExcInfo

    # This is the type of the exc_info/_exc_info parameter of the log methods.
    # sys.exc_info() returns a tuple of (type, value, traceback) or (None, None, None).
    # We just need the exception, but we allow the user to pass the tuple because:
    # 1. It's convenient to pass the result of sys.exc_info() directly
    # 2. It mirrors the exc_info argument of the stdlib logging methods
    # 3. The argument name exc_info is very suggestive of the sys function.
    ExcInfo = Union[SysExcInfo, BaseException, bool, None]

T = TypeVar('T')


class Logfire:
    """The main logfire class."""

    def __init__(
        self,
        *,
        config: LogfireConfig = GLOBAL_CONFIG,
        sample_rate: float | None = None,
        tags: Sequence[str] = (),
        console_log: bool = True,
        otel_scope: str = 'logfire',
    ) -> None:
        self._tags = tuple(tags)
        self._config = config
        self._sample_rate = sample_rate
        self._console_log = console_log
        self._otel_scope = otel_scope
        self._variables: dict[str, Variable[Any]] = {}

    @property
    def config(self) -> LogfireConfig:
        return self._config

    @property
    def resource_attributes(self) -> Mapping[str, Any]:
        return self._tracer_provider.resource.attributes

    @cached_property
    def _tracer_provider(self) -> ProxyTracerProvider:
        self._config.warn_if_not_initialized('No logs or spans will be created')
        return self._config.get_tracer_provider()

    @cached_property
    def _meter_provider(self) -> ProxyMeterProvider:  # pragma: no cover
        return self._config.get_meter_provider()

    @cached_property
    def _meter(self):
        return self._meter_provider.get_meter(self._otel_scope, VERSION)

    @cached_property
    def _logs_tracer(self) -> _ProxyTracer:
        return self._get_tracer(is_span_tracer=False)

    @cached_property
    def _spans_tracer(self) -> _ProxyTracer:
        return self._get_tracer(is_span_tracer=True)

    def _get_tracer(self, *, is_span_tracer: bool) -> _ProxyTracer:
        return self._tracer_provider.get_tracer(
            self._otel_scope,
            VERSION,
            is_span_tracer=is_span_tracer,
        )

    # If any changes are made to this method, they may need to be reflected in `_fast_span` as well.
    def _span(
        self,
        msg_template: str,
        attributes: dict[str, Any],
        *,
        _tags: Sequence[str] | None = None,
        _span_name: str | None = None,
        _level: LevelName | int | None = None,
        _links: Sequence[tuple[SpanContext, otel_types.Attributes]] = (),
        _span_kind: SpanKind = SpanKind.INTERNAL,
    ) -> LogfireSpan:
        try:
            if _level is not None:
                level_attributes = log_level_attributes(_level)
                level_num = level_attributes[ATTRIBUTES_LOG_LEVEL_NUM_KEY]
                if level_num < self.config.min_level:
                    return NoopSpan()  # type: ignore
            else:
                level_attributes = None

            stack_info = get_user_stack_info()
            merged_attributes = {**stack_info, **attributes}

            if self._config.inspect_arguments:
                fstring_frame = inspect.currentframe().f_back  # type: ignore
            else:
                fstring_frame = None

            log_message, extra_attrs, msg_template = logfire_format_with_magic(
                msg_template,
                merged_attributes,
                self._config.scrubber,
                fstring_frame=fstring_frame,
            )
            merged_attributes.update(extra_attrs)
            attributes.update(extra_attrs)  # for the JSON schema
            merged_attributes[ATTRIBUTES_MESSAGE_TEMPLATE_KEY] = msg_template
            merged_attributes[ATTRIBUTES_MESSAGE_KEY] = log_message

            otlp_attributes = prepare_otlp_attributes(merged_attributes)

            if json_schema_properties := attributes_json_schema_properties(attributes):
                otlp_attributes[ATTRIBUTES_JSON_SCHEMA_KEY] = attributes_json_schema(json_schema_properties)

            tags = (self._tags or ()) + tuple(_tags or ())
            if tags:
                otlp_attributes[ATTRIBUTES_TAGS_KEY] = uniquify_sequence(tags)

            sample_rate = (
                self._sample_rate
                if self._sample_rate is not None
                else otlp_attributes.pop(ATTRIBUTES_SAMPLE_RATE_KEY, None)
            )
            if sample_rate is not None and sample_rate != 1:  # pragma: no cover
                otlp_attributes[ATTRIBUTES_SAMPLE_RATE_KEY] = sample_rate

            if level_attributes is not None:
                otlp_attributes.update(level_attributes)

            return LogfireSpan(
                _span_name or msg_template,
                otlp_attributes,
                self._spans_tracer,
                json_schema_properties,
                links=_links,
                span_kind=_span_kind,
            )
        except Exception:
            log_internal_error()
            return NoopSpan()  # type: ignore

    def _fast_span(self, name: str, attributes: otel_types.Attributes, **kwargs: Any) -> FastLogfireSpan:
        """A simple version of `_span` optimized for auto-tracing that doesn't support message formatting.

        Returns a similarly simplified version of `LogfireSpan` which must immediately be used as a context manager.
        """
        try:
            span = self._spans_tracer.start_span(name=name, attributes=attributes, **kwargs)
            return FastLogfireSpan(span)
        except Exception:  # pragma: no cover
            log_internal_error()
            return NoopSpan()  # type: ignore

    def _instrument_span_with_args(
        self, name: str, attributes: dict[str, otel_types.AttributeValue], function_args: dict[str, Any], **kwargs: Any
    ) -> FastLogfireSpan:
        """A version of `_span` used by `@instrument` with `extract_args=True`.

        This is a bit faster than `_span` but not as fast as `_fast_span` because it supports message formatting
        and arbitrary types of attributes.
        """
        try:
            msg_template: str = attributes[ATTRIBUTES_MESSAGE_TEMPLATE_KEY]  # type: ignore
            attributes[ATTRIBUTES_MESSAGE_KEY] = logfire_format(msg_template, function_args, self._config.scrubber)
            if json_schema_properties := attributes_json_schema_properties(function_args):  # pragma: no branch
                attributes[ATTRIBUTES_JSON_SCHEMA_KEY] = attributes_json_schema(json_schema_properties)
            attributes.update(prepare_otlp_attributes(function_args))
            return self._fast_span(name, attributes, **kwargs)
        except Exception:  # pragma: no cover
            log_internal_error()
            return NoopSpan()  # type: ignore

    def trace(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _exc_info: ExcInfo = False,
        **attributes: Any,
    ) -> None:
        """Log a trace message.

        ```py
        import logfire

        logfire.configure()

        logfire.trace('This is a trace log')
        ```

        Args:
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            _tags: An optional sequence of tags to include in the log.
            _exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.

                Set to `True` to use the currently handled exception.
        """
        if any(k.startswith('_') for k in attributes):
            raise ValueError('Attribute keys cannot start with an underscore.')
        self.log('trace', msg_template, attributes, tags=_tags, exc_info=_exc_info)

    def debug(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _exc_info: ExcInfo = False,
        **attributes: Any,
    ) -> None:
        """Log a debug message.

        ```py
        import logfire

        logfire.configure()

        logfire.debug('This is a debug log')
        ```

        Args:
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            _tags: An optional sequence of tags to include in the log.
            _exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.

                Set to `True` to use the currently handled exception.
        """
        if any(k.startswith('_') for k in attributes):
            raise ValueError('Attribute keys cannot start with an underscore.')
        self.log('debug', msg_template, attributes, tags=_tags, exc_info=_exc_info)

    def info(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _exc_info: ExcInfo = False,
        **attributes: Any,
    ) -> None:
        """Log an info message.

        ```py
        import logfire

        logfire.configure()

        logfire.info('This is an info log')
        ```

        Args:
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            _tags: An optional sequence of tags to include in the log.
            _exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.

                Set to `True` to use the currently handled exception.
        """
        if any(k.startswith('_') for k in attributes):
            raise ValueError('Attribute keys cannot start with an underscore.')
        self.log('info', msg_template, attributes, tags=_tags, exc_info=_exc_info)

    def notice(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _exc_info: ExcInfo = False,
        **attributes: Any,
    ) -> None:
        """Log a notice message.

        ```py
        import logfire

        logfire.configure()

        logfire.notice('This is a notice log')
        ```

        Args:
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            _tags: An optional sequence of tags to include in the log.
            _exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.

                Set to `True` to use the currently handled exception.
        """
        if any(k.startswith('_') for k in attributes):
            raise ValueError('Attribute keys cannot start with an underscore.')
        self.log('notice', msg_template, attributes, tags=_tags, exc_info=_exc_info)

    def warning(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _exc_info: ExcInfo = False,
        **attributes: Any,
    ) -> None:
        """Log a warning message.

        ```py
        import logfire

        logfire.configure()

        logfire.warning('This is a warning log')
        ```

        `logfire.warn` is an alias of `logfire.warning`.

        Args:
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            _tags: An optional sequence of tags to include in the log.
            _exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.

                Set to `True` to use the currently handled exception.
        """
        if any(k.startswith('_') for k in attributes):
            raise ValueError('Attribute keys cannot start with an underscore.')
        self.log('warn', msg_template, attributes, tags=_tags, exc_info=_exc_info)

    warn = warning

    def error(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _exc_info: ExcInfo = False,
        **attributes: Any,
    ) -> None:
        """Log an error message.

        ```py
        import logfire

        logfire.configure()

        logfire.error('This is an error log')
        ```

        Args:
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            _tags: An optional sequence of tags to include in the log.
            _exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.

                Set to `True` to use the currently handled exception.
        """
        if any(k.startswith('_') for k in attributes):
            raise ValueError('Attribute keys cannot start with an underscore.')
        self.log('error', msg_template, attributes, tags=_tags, exc_info=_exc_info)

    def fatal(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _exc_info: ExcInfo = False,
        **attributes: Any,
    ) -> None:
        """Log a fatal message.

        ```py
        import logfire

        logfire.configure()

        logfire.fatal('This is a fatal log')
        ```

        Args:
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            _tags: An optional sequence of tags to include in the log.
            _exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.

                Set to `True` to use the currently handled exception.
        """
        if any(k.startswith('_') for k in attributes):
            raise ValueError('Attribute keys cannot start with an underscore.')
        self.log('fatal', msg_template, attributes, tags=_tags, exc_info=_exc_info)

    def exception(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _exc_info: ExcInfo = True,
        **attributes: Any,
    ) -> None:
        """The same as `error` but with `_exc_info=True` by default.

        This means that a traceback will be logged for any currently handled exception.

        Args:
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            _tags: An optional sequence of tags to include in the log.
            _exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.
        """
        if any(k.startswith('_') for k in attributes):  # pragma: no cover
            raise ValueError('Attribute keys cannot start with an underscore.')
        self.log('error', msg_template, attributes, tags=_tags, exc_info=_exc_info)

    def span(
        self,
        msg_template: str,
        /,
        *,
        _tags: Sequence[str] | None = None,
        _span_name: str | None = None,
        _level: LevelName | None = None,
        _links: Sequence[tuple[SpanContext, otel_types.Attributes]] = (),
        _span_kind: SpanKind = SpanKind.INTERNAL,
        **attributes: Any,
    ) -> LogfireSpan:
        """Context manager for creating a span.

        ```py
        import logfire

        logfire.configure()

        with logfire.span('This is a span {a=}', a='data'):
            logfire.info('new log 1')
        ```

        Args:
            msg_template: The template for the span message.
            _span_name: The span name. If not provided, the `msg_template` will be used.
            _tags: An optional sequence of tags to include in the span.
            _level: An optional log level name.
            _links: An optional sequence of links to other spans. Each link is a tuple of a span context and attributes.
            _span_kind: The [OpenTelemetry span kind](https://opentelemetry.io/docs/concepts/signals/traces/#span-kind).
                If not provided, defaults to `INTERNAL`.
                Users don't typically need to set this.
                Not related to the `kind` column of the `records` table in Logfire.
            attributes: The arguments to include in the span and format the message template with.
                Attributes starting with an underscore are not allowed.
        """
        if any(k.startswith('_') for k in attributes):
            raise ValueError('Attribute keys cannot start with an underscore.')
        return self._span(
            msg_template,
            attributes,
            _tags=_tags,
            _span_name=_span_name,
            _level=_level,
            _links=_links,
            _span_kind=_span_kind,
        )

    @overload
    def instrument(
        self,
        msg_template: LiteralString | None = None,
        *,
        span_name: str | None = None,
        extract_args: bool | Iterable[str] = True,
        record_return: bool = False,
        allow_generator: bool = False,
        new_trace: bool = False,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """Decorator for instrumenting a function as a span.

        ```py
        import logfire

        logfire.configure()


        @logfire.instrument('This is a span {a=}')
        def my_function(a: int):
            logfire.info('new log {a=}', a=a)
        ```

        Args:
            msg_template: The template for the span message. If not provided, the module and function name will be used.
            span_name: The span name. If not provided, the `msg_template` will be used.
            extract_args: By default, all function call arguments are logged as span attributes.
                Set to `False` to disable this, or pass an iterable of argument names to include.
            record_return: Set to `True` to record the return value of the function as an attribute.
                Ignored for generators.
            allow_generator: Set to `True` to prevent a warning when instrumenting a generator function.
                Read https://logfire.pydantic.dev/docs/guides/advanced/generators/#using-logfireinstrument first.
            new_trace: Set to `True` to start a new trace with a span link to the current span
                instead of creating a child of the current span.
        """

    @overload
    def instrument(self, func: Callable[P, R]) -> Callable[P, R]:
        """Decorator for instrumenting a function as a span, with default configuration.

        ```py
        import logfire

        logfire.configure()


        @logfire.instrument
        def my_function(a: int):
            logfire.info('new log {a=}', a=a)
        ```
        """

    def instrument(  # type: ignore[reportInconsistentOverload]
        self,
        msg_template: Callable[P, R] | LiteralString | None = None,
        *,
        span_name: str | None = None,
        extract_args: bool | Iterable[str] = True,
        record_return: bool = False,
        allow_generator: bool = False,
        new_trace: bool = False,
    ) -> Callable[[Callable[P, R]], Callable[P, R]] | Callable[P, R]:
        """Decorator for instrumenting a function as a span.

        ```py
        import logfire

        logfire.configure()


        @logfire.instrument('This is a span {a=}')
        def my_function(a: int):
            logfire.info('new log {a=}', a=a)
        ```

        Args:
            msg_template: The template for the span message. If not provided, the module and function name will be used.
            span_name: The span name. If not provided, the `msg_template` will be used.
            extract_args: By default, all function call arguments are logged as span attributes.
                Set to `False` to disable this, or pass an iterable of argument names to include.
            record_return: Set to `True` to record the return value of the function as an attribute.
                Ignored for generators.
            allow_generator: Set to `True` to prevent a warning when instrumenting a generator function.
                Read https://logfire.pydantic.dev/docs/guides/advanced/generators/#using-logfireinstrument first.
            new_trace: Set to `True` to start a new trace with a span link to the current span
                instead of creating a child of the current span.
        """
        if callable(msg_template):
            return self.instrument()(msg_template)
        return instrument(
            self, tuple(self._tags), msg_template, span_name, extract_args, record_return, allow_generator, new_trace
        )

    def log(
        self,
        level: LevelName | int,
        msg_template: str,
        attributes: dict[str, Any] | None = None,
        tags: Sequence[str] | None = None,
        exc_info: ExcInfo = False,
        console_log: bool | None = None,
    ) -> None:
        """Log a message.

        ```py
        import logfire

        logfire.configure()

        logfire.log('info', 'This is a log {a}', {'a': 'Apple'})
        ```

        Args:
            level: The level of the log.
            msg_template: The message to log.
            attributes: The attributes to bind to the log.
            tags: An optional sequence of tags to include in the log.
            exc_info: Set to an exception or a tuple as returned by [`sys.exc_info()`][sys.exc_info]
                to record a traceback with the log message.

                Set to `True` to use the currently handled exception.
            console_log: Whether to log to the console, defaults to `True`.
        """
        with handle_internal_errors:
            level_attributes = log_level_attributes(level)
            level_num = level_attributes[ATTRIBUTES_LOG_LEVEL_NUM_KEY]
            if level_num < self.config.min_level:
                return

            stack_info = get_user_stack_info()

            attributes = attributes or {}
            merged_attributes = {**stack_info, **attributes}
            if (msg := attributes.pop(ATTRIBUTES_MESSAGE_KEY, None)) is None:
                fstring_frame = None
                if self._config.inspect_arguments:
                    fstring_frame = inspect.currentframe()
                    if fstring_frame.f_back.f_code.co_filename == Logfire.log.__code__.co_filename:  # type: ignore
                        # fstring_frame.f_back should be the user's frame.
                        # The user called logfire.info or a similar method rather than calling logfire.log directly.
                        fstring_frame = fstring_frame.f_back  # type: ignore

                msg, extra_attrs, msg_template = logfire_format_with_magic(
                    msg_template,
                    merged_attributes,
                    self._config.scrubber,
                    fstring_frame=fstring_frame,
                )
                if extra_attrs:
                    merged_attributes.update(extra_attrs)
                    # Only do this if extra_attrs is not empty since the copy of `attributes` might be expensive.
                    # We update both because attributes_json_schema_properties looks at `attributes`.
                    attributes = {**attributes, **extra_attrs}
            else:
                # The message has already been filled in, presumably by a logging integration.
                # Make sure it's a string.
                msg = merged_attributes[ATTRIBUTES_MESSAGE_KEY] = str(msg)
                msg_template = str(msg_template)

            otlp_attributes = prepare_otlp_attributes(merged_attributes)
            otlp_attributes = {
                ATTRIBUTES_SPAN_TYPE_KEY: 'log',
                **level_attributes,
                ATTRIBUTES_MESSAGE_TEMPLATE_KEY: msg_template,
                ATTRIBUTES_MESSAGE_KEY: msg,
                **otlp_attributes,
            }
            if json_schema_properties := attributes_json_schema_properties(attributes):
                otlp_attributes[ATTRIBUTES_JSON_SCHEMA_KEY] = attributes_json_schema(json_schema_properties)

            tags = self._tags + tuple(tags or ())
            if tags:
                otlp_attributes[ATTRIBUTES_TAGS_KEY] = uniquify_sequence(tags)

            sample_rate = (
                self._sample_rate
                if self._sample_rate is not None
                else otlp_attributes.pop(ATTRIBUTES_SAMPLE_RATE_KEY, None)
            )
            if sample_rate is not None and sample_rate != 1:  # pragma: no cover
                otlp_attributes[ATTRIBUTES_SAMPLE_RATE_KEY] = sample_rate

            if not (self._console_log if console_log is None else console_log):
                otlp_attributes[DISABLE_CONSOLE_KEY] = True
            start_time = self._config.advanced.ns_timestamp_generator()

            span = self._logs_tracer.start_span(
                msg_template,
                attributes=otlp_attributes,
                start_time=start_time,
            )

            if not span.is_recording():
                return

            if exc_info:
                if exc_info is True:
                    exc_info = sys.exc_info()
                if isinstance(exc_info, tuple):
                    exc_info = exc_info[1]
                if isinstance(exc_info, BaseException):
                    span.record_exception(exc_info)
                    if otlp_attributes[ATTRIBUTES_LOG_LEVEL_NUM_KEY] >= LEVEL_NUMBERS['error']:  # type: ignore
                        # Set the status description to the exception message.
                        # OTEL only lets us set the description when the status code is ERROR,
                        # which we only want to do when the log level is error.
                        set_exception_status(span, exc_info)
                elif exc_info is not None:  # pragma: no cover
                    raise TypeError(f'Invalid type for exc_info: {exc_info.__class__.__name__}')

            span.end(start_time)

    def with_tags(self, *tags: str) -> Logfire:
        """A new Logfire instance which always uses the given tags.

        ```py
        import logfire

        logfire.configure()

        local_logfire = logfire.with_tags('tag1')
        local_logfire.info('a log message', _tags=['tag2'])

        # This is equivalent to:
        logfire.info('a log message', _tags=['tag1', 'tag2'])
        ```

        Args:
            tags: The tags to add.

        Returns:
            A new Logfire instance with the `tags` added to any existing tags.
        """
        return self.with_settings(tags=tags)

    def with_trace_sample_rate(self, sample_rate: float) -> Logfire:  # pragma: no cover
        """A new Logfire instance with the given sampling ratio applied.

        Args:
            sample_rate: The sampling ratio to use.

        Returns:
            A new Logfire instance with the sampling ratio applied.
        """
        if sample_rate > 1 or sample_rate < 0:
            raise ValueError('sample_rate must be between 0 and 1')
        return Logfire(
            config=self._config,
            tags=self._tags,
            sample_rate=sample_rate,
        )

    def with_settings(
        self,
        *,
        tags: Sequence[str] = (),
        console_log: bool | None = None,
        custom_scope_suffix: str | None = None,
    ) -> Logfire:
        """A new Logfire instance which uses the given settings.

        Args:
            tags: Sequence of tags to include in the log.
            console_log: Whether to log to the console, defaults to `True`.
            custom_scope_suffix: A custom suffix to append to `logfire.` e.g. `logfire.loguru`.

                It should only be used when instrumenting another library with Logfire, such as structlog or loguru.

                See the `instrumenting_module_name` parameter on
                [TracerProvider.get_tracer][opentelemetry.sdk.trace.TracerProvider.get_tracer] for more info.

        Returns:
            A new Logfire instance with the given settings applied.
        """
        # TODO add sample_rate once it's more stable
        return Logfire(
            config=self._config,
            tags=self._tags + tuple(tags),
            sample_rate=self._sample_rate,
            console_log=self._console_log if console_log is None else console_log,
            otel_scope=self._otel_scope if custom_scope_suffix is None else f'logfire.{custom_scope_suffix}',
        )

    def force_flush(self, timeout_millis: int = 3_000) -> bool:  # pragma: no cover
        """Force flush all spans and metrics.

        Args:
            timeout_millis: The timeout in milliseconds.

        Returns:
            Whether the flush of spans was successful.
        """
        return self._config.force_flush(timeout_millis)

    def log_slow_async_callbacks(self, slow_duration: float = 0.1) -> AbstractContextManager[None]:
        """Log a warning whenever a function running in the asyncio event loop blocks for too long.

        This works by patching the `asyncio.events.Handle._run` method.

        Args:
            slow_duration: the threshold in seconds for when a callback is considered slow.

        Returns:
            A context manager that will revert the patch when exited.
                This context manager doesn't take into account threads or other concurrency.
                Calling this method will immediately apply the patch
                without waiting for the context manager to be opened,
                i.e. it's not necessary to use this as a context manager.
        """
        return async_.log_slow_callbacks(self, slow_duration)

    def install_auto_tracing(
        self,
        modules: Sequence[str] | Callable[[AutoTraceModule], bool],
        *,
        min_duration: float,
        check_imported_modules: Literal['error', 'warn', 'ignore'] = 'error',
    ) -> None:
        """Install automatic tracing.

        See the [Auto-Tracing guide](https://logfire.pydantic.dev/docs/guides/onboarding_checklist/add_auto_tracing/)
        for more info.

        This will trace all non-generator function calls in the modules specified by the modules argument.
        It's equivalent to wrapping the body of every function in matching modules in `with logfire.span(...):`.

        !!! note
            This function MUST be called before any of the modules to be traced are imported.

            Generator functions will not be traced for reasons explained [here](https://logfire.pydantic.dev/docs/guides/advanced/generators/).

        This works by inserting a new meta path finder into `sys.meta_path`, so inserting another finder before it
        may prevent it from working.

        It relies on being able to retrieve the source code via at least one other existing finder in the meta path,
        so it may not work if standard finders are not present or if the source code is not available.
        A modified version of the source code is then compiled and executed in place of the original module.

        Args:
            modules: List of module names to trace, or a function which returns True for modules that should be traced.
                If a list is provided, any submodules within a given module will also be traced.
            min_duration: A minimum duration in seconds for which a function must run before it's traced.
                Setting to `0` causes all functions to be traced from the beginning.
                Otherwise, the first time(s) each function is called, it will be timed but not traced.
                Only after the function has run for at least `min_duration` will it be traced in subsequent calls.
            check_imported_modules: If this is `'error'` (the default), then an exception will be raised if any of the
                modules in `sys.modules` (i.e. modules that have already been imported) match the modules to trace.
                Set to `'warn'` to issue a warning instead, or `'ignore'` to skip the check.
        """
        install_auto_tracing(self, modules, check_imported_modules=check_imported_modules, min_duration=min_duration)

    def _warn_if_not_initialized_for_instrumentation(self):
        self.config.warn_if_not_initialized('Instrumentation will have no effect')

    def instrument_surrealdb(
        self, obj: SyncTemplate | AsyncTemplate | type[SyncTemplate] | type[AsyncTemplate] | None = None
    ) -> None:
        """Instrument [SurrealDB](https://surrealdb.com/) connections, creating a span for each method.

        Args:
            obj: Pass a single connection instance to instrument only that connection.
                Pass a connection class to instrument all instances of that class.
                By default, all connection classes are instrumented.
        """
        from .integrations.surrealdb import instrument_surrealdb

        self._warn_if_not_initialized_for_instrumentation()
        instrument_surrealdb(obj, self)

    def instrument_mcp(self, *, propagate_otel_context: bool = True) -> None:
        """Instrument the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).

        Instruments both the client and server side. If possible, calling this in both the client and server
        processes is recommended for nice distributed traces.

        Args:
            propagate_otel_context: Whether to enable propagation of the OpenTelemetry context
                for distributed tracing.
                Set to False to prevent setting extra fields like `traceparent` on the metadata of requests.
        """
        from .integrations.mcp import instrument_mcp

        self._warn_if_not_initialized_for_instrumentation()
        instrument_mcp(self, propagate_otel_context)

    def instrument_pydantic(
        self,
        record: PydanticPluginRecordValues = 'all',
        include: Iterable[str] = (),
        exclude: Iterable[str] = (),
    ) -> None:
        """Instrument Pydantic model validations.

        This must be called before defining and importing the model classes you want to instrument.
        See the [Pydantic integration guide](https://logfire.pydantic.dev/docs/integrations/pydantic/) for more info.

        Args:
            record: The record mode for the Pydantic plugin. It can be one of the following values:

                - `all`: Send traces and metrics for all events. This is default value.
                - `failure`: Send metrics for all validations and traces only for validation failures.
                - `metrics`: Send only metrics.
                - `off`: Disable instrumentation.
            include:
                By default, third party modules are not instrumented. This option allows you to include specific modules.
            exclude:
                Exclude specific modules from instrumentation.
        """
        # Note that unlike most instrument_* methods, we intentionally don't call
        # _warn_if_not_initialized_for_instrumentation, because this method needs to be called early.

        if record != 'off':
            import pydantic

            if get_version(pydantic.__version__) < get_version('2.5.0'):
                raise RuntimeError('The Pydantic plugin requires Pydantic 2.5.0 or newer.')

        from logfire.integrations.pydantic import PydanticPlugin, set_pydantic_plugin_config

        if isinstance(include, str):
            include = {include}

        if isinstance(exclude, str):
            exclude = {exclude}

        # TODO instrument using this instance, i.e. pass `self` somewhere, rather than always using the global instance
        set_pydantic_plugin_config(
            PydanticPlugin(
                record=record,
                include=set(include),
                exclude=set(exclude),
            )
        )

    @overload
    def instrument_pydantic_ai(
        self,
        obj: pydantic_ai.Agent | None = None,
        /,
        *,
        include_binary_content: bool | None = None,
        include_content: bool | None = None,
        version: Literal[1, 2, 3] | None = None,
        event_mode: Literal['attributes', 'logs'] | None = None,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def instrument_pydantic_ai(
        self,
        obj: pydantic_ai.models.Model,
        /,
        *,
        include_binary_content: bool | None = None,
        include_content: bool | None = None,
        version: Literal[1, 2, 3] | None = None,
        event_mode: Literal['attributes', 'logs'] | None = None,
        **kwargs: Any,
    ) -> pydantic_ai.models.Model: ...

    def instrument_pydantic_ai(
        self,
        obj: pydantic_ai.Agent | pydantic_ai.models.Model | None = None,
        /,
        *,
        include_binary_content: bool | None = None,
        include_content: bool | None = None,
        version: Literal[1, 2, 3] | None = None,
        event_mode: Literal['attributes', 'logs'] | None = None,
        **kwargs: Any,
    ) -> pydantic_ai.models.Model | None:
        """Instrument Pydantic AI.

        Args:
            obj: What to instrument.
                By default, all agents are instrumented.
                You can also pass a specific model or agent.
                If you pass a model, a new instrumented model will be returned.
            include_binary_content: Whether to include base64 encoded binary content (e.g. images) in the telemetry.
                On by default. Requires Pydantic AI 0.2.5 or newer.
            include_content: Whether to include prompts, completions, and tool call arguments and responses
                in the telemetry. On by default. Requires Pydantic AI 0.3.4 or newer.
            version: Version of the data format. This is unrelated to the Pydantic AI package version.
                Requires Pydantic AI 0.7.5 or newer.
                Version 1 is based on the legacy event-based OpenTelemetry GenAI spec
                    and will be removed in a future release.
                    The parameter `event_mode` is only relevant for version 1.
                Version 2 uses the newer OpenTelemetry GenAI spec and stores messages in the following attributes:
                    - `gen_ai.system_instructions` for instructions passed to the agent.
                    - `gen_ai.input.messages` and `gen_ai.output.messages` on model request spans.
                    - `pydantic_ai.all_messages` on agent run spans.
                Version 3 changes the names of some attributes and spans but not the shape of the data.
                The default version depends on Pydantic AI.
            event_mode: The mode for emitting events in version 1.
                If `'attributes'`, events are attached to the span as attributes.
                If `'logs'`, events are emitted as OpenTelemetry log-based events.
            kwargs: Additional keyword arguments to pass to
                [`InstrumentationSettings`](https://ai.pydantic.dev/api/models/instrumented/#pydantic_ai.models.instrumented.InstrumentationSettings)
                for future compatibility.
        """
        from .integrations.pydantic_ai import instrument_pydantic_ai

        self._warn_if_not_initialized_for_instrumentation()

        return instrument_pydantic_ai(
            self,
            obj=obj,
            event_mode=event_mode,
            version=version,
            include_content=include_content,
            include_binary_content=include_binary_content,
            **kwargs,
        )

    def instrument_fastapi(
        self,
        app: FastAPI,
        *,
        capture_headers: bool = False,
        request_attributes_mapper: Callable[
            [
                Request | WebSocket,
                dict[str, Any],
            ],
            dict[str, Any] | None,
        ]
        | None = None,
        excluded_urls: str | Iterable[str] | None = None,
        record_send_receive: bool = False,
        extra_spans: bool = False,
        **opentelemetry_kwargs: Any,
    ) -> AbstractContextManager[None]:
        """Instrument a FastAPI app so that spans and logs are automatically created for each request.

        Uses the [OpenTelemetry FastAPI Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html)
        under the hood, with some additional features.

        Args:
            app: The FastAPI app to instrument.
            capture_headers: Set to `True` to capture all request and response headers.
            request_attributes_mapper: A function that takes a [`Request`][fastapi.Request] or [`WebSocket`][fastapi.WebSocket]
                and a dictionary of attributes and returns a new dictionary of attributes.
                The input dictionary will contain:

                - `values`: A dictionary mapping argument names of the endpoint function to parsed and validated values.
                - `errors`: A list of validation errors for any invalid inputs.

                The returned dictionary will be used as the attributes for a log message.
                If `None` is returned, no log message will be created.

                You can use this to e.g. only log validation errors, or nothing at all.
                You can also add custom attributes.

                The default implementation will return the input dictionary unchanged.
                The function mustn't modify the contents of `values` or `errors`.
            excluded_urls: A string of comma-separated regexes which will exclude a request from tracing if the full URL
                matches any of the regexes. This applies to both the Logfire and OpenTelemetry instrumentation.
                If not provided, the environment variables
                `OTEL_PYTHON_FASTAPI_EXCLUDED_URLS` and `OTEL_PYTHON_EXCLUDED_URLS` will be checked.
            record_send_receive: Set to `True` to allow the OpenTelemetry ASGI middleware to create send/receive spans.

                These are disabled by default to reduce overhead and the number of spans created,
                since many can be created for a single request, and they are not often useful.
                If enabled, they will be set to debug level, meaning they will usually still be hidden in the UI.
            extra_spans: Whether to include the extra 'FastAPI arguments' and 'endpoint function' spans.
            opentelemetry_kwargs: Additional keyword arguments to pass to the OpenTelemetry FastAPI instrumentation.

        Returns:
            A context manager that will revert the instrumentation when exited.
                This context manager doesn't take into account threads or other concurrency.
                Calling this method will immediately apply the instrumentation
                without waiting for the context manager to be opened,
                i.e. it's not necessary to use this as a context manager.
        """
        from .integrations.fastapi import instrument_fastapi

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_fastapi(
            self,
            app,
            capture_headers=capture_headers,
            request_attributes_mapper=request_attributes_mapper,
            excluded_urls=excluded_urls,
            record_send_receive=record_send_receive,
            extra_spans=extra_spans,
            **opentelemetry_kwargs,
        )

    def instrument_openai(
        self,
        openai_client: openai.OpenAI
        | openai.AsyncOpenAI
        | type[openai.OpenAI]
        | type[openai.AsyncOpenAI]
        | None = None,
        *,
        suppress_other_instrumentation: bool = True,
        version: SemconvVersion | Sequence[SemconvVersion] = 1,
    ) -> AbstractContextManager[None]:
        """Instrument an OpenAI client so that spans are automatically created for each request.

        This instruments the [standard OpenAI SDK](https://pypi.org/project/openai/) package, for instrumentation
        of the OpenAI "agents" framework, see [`instrument_openai_agents()`][logfire.Logfire.instrument_openai_agents].

        The following methods are instrumented for both the sync and the async clients:

        - [`client.chat.completions.create`](https://platform.openai.com/docs/guides/text-generation/chat-completions-api) — with and without `stream=True`
        - [`client.completions.create`](https://platform.openai.com/docs/guides/text-generation/completions-api) — with and without `stream=True`
        - [`client.embeddings.create`](https://platform.openai.com/docs/guides/embeddings/how-to-get-embeddings)
        - [`client.images.generate`](https://platform.openai.com/docs/guides/images/generations)

        When `stream=True` a second span is created to instrument the streamed response.

        Example usage:

        ```python skip-run="true" skip-reason="external-connection"
        import openai

        import logfire

        client = openai.OpenAI()
        logfire.configure()
        logfire.instrument_openai(client)

        response = client.chat.completions.create(
            model='gpt-4',
            messages=[
                {'role': 'system', 'content': 'You are a helpful assistant.'},
                {'role': 'user', 'content': 'What is four plus five?'},
            ],
        )
        print('answer:', response.choices[0].message.content)
        ```

        Args:
            openai_client: The OpenAI client or class to instrument:

                - `None` (the default) to instrument both the `openai.OpenAI` and `openai.AsyncOpenAI` classes.
                - The `openai.OpenAI` class or a subclass
                - The `openai.AsyncOpenAI` class or a subclass
                - An instance of `openai.OpenAI`
                - An instance of `openai.AsyncOpenAI`

            suppress_other_instrumentation: If True, suppress any other OTEL instrumentation that may be otherwise
                enabled. In reality, this means the HTTPX instrumentation, which could otherwise be called since
                OpenAI uses HTTPX to make HTTP requests.

            version: The version(s) of the span attribute format to use:

                - `1` (the default): Uses `request_data` and `response_data` attributes.
                - `'latest'`: Uses OpenTelemetry Gen AI semantic convention attributes
                  (`gen_ai.input.messages`, `gen_ai.output.messages`, etc.) and omits the full
                  `response_data` attribute. A minimal `request_data` (e.g. `{"model": ...}`) is
                  still recorded for message template compatibility. This format may change between
                  releases.
                - `[1, 'latest']`: Emits both the full legacy attributes and the semantic convention
                  attributes simultaneously, useful for migration and testing.

        Returns:
            A context manager that will revert the instrumentation when exited.
                Use of this context manager is optional.
        """
        import openai

        from .integrations.llm_providers.llm_provider import instrument_llm_provider
        from .integrations.llm_providers.openai import get_endpoint_config, is_async_client, on_response
        from .integrations.llm_providers.semconv import normalize_versions

        normalized_versions = normalize_versions(version)
        self._warn_if_not_initialized_for_instrumentation()
        return instrument_llm_provider(
            self,
            openai_client or (openai.OpenAI, openai.AsyncOpenAI),
            suppress_other_instrumentation,
            'OpenAI',
            partial(get_endpoint_config, version=normalized_versions),
            partial(on_response, version=normalized_versions),
            is_async_client,
        )

    def instrument_openai_agents(self) -> None:
        """Instrument the [`agents`](https://github.com/openai/openai-agents-python) framework from OpenAI.

        For instrumentation of the standard OpenAI SDK package,
        see [`instrument_openai()`][logfire.Logfire.instrument_openai].
        """
        self._warn_if_not_initialized_for_instrumentation()

        from .integrations.openai_agents import LogfireTraceProviderWrapper

        LogfireTraceProviderWrapper.install(self)

    def instrument_anthropic(
        self,
        anthropic_client: (
            anthropic.Anthropic
            | anthropic.AsyncAnthropic
            | anthropic.AnthropicBedrock
            | anthropic.AsyncAnthropicBedrock
            | type[anthropic.Anthropic]
            | type[anthropic.AsyncAnthropic]
            | type[anthropic.AnthropicBedrock]
            | type[anthropic.AsyncAnthropicBedrock]
            | None
        ) = None,
        *,
        suppress_other_instrumentation: bool = True,
        version: SemconvVersion | Sequence[SemconvVersion] = 1,
    ) -> AbstractContextManager[None]:
        """Instrument an Anthropic client so that spans are automatically created for each request.

        The following methods are instrumented for both the sync and async clients:

        - [`client.messages.create`](https://docs.anthropic.com/en/api/messages)
        - [`client.messages.stream`](https://docs.anthropic.com/en/api/messages-streaming)
        - [`client.beta.tools.messages.create`](https://docs.anthropic.com/en/docs/tool-use)

        When `stream=True` a second span is created to instrument the streamed response.

        Example usage:

        ```python skip-run="true" skip-reason="external-connection"
        import anthropic

        import logfire

        client = anthropic.Anthropic()

        logfire.configure()
        logfire.instrument_anthropic(client)

        response = client.messages.create(
            model='claude-3-haiku-20240307',
            system='You are a helpful assistant.',
            messages=[
                {'role': 'user', 'content': 'What is four plus five?'},
            ],
        )
        print('answer:', response.content[0].text)
        ```

        Args:
            anthropic_client: The Anthropic client or class to instrument:
                - `None` (the default) to instrument all Anthropic client types
                - The `anthropic.Anthropic` or `anthropic.AnthropicBedrock` class or subclass
                - The `anthropic.AsyncAnthropic` or `anthropic.AsyncAnthropicBedrock` class or subclass
                - An instance of any of the above classes

            suppress_other_instrumentation: If True, suppress any other OTEL instrumentation that may be otherwise
                enabled. In reality, this means the HTTPX instrumentation, which could otherwise be called since
                OpenAI uses HTTPX to make HTTP requests.

            version: The version(s) of the span attribute format to use:

                - `1` (the default): Uses `request_data` and `response_data` attributes.
                - `'latest'`: Uses OpenTelemetry Gen AI semantic convention attributes
                  (`gen_ai.input.messages`, `gen_ai.output.messages`, etc.) and omits the full
                  `response_data` attribute. A minimal `request_data` (e.g. `{"model": ...}`) is
                  still recorded for message template compatibility. This format may change between
                  releases.
                - `[1, 'latest']`: Emits both the full legacy attributes and the semantic convention
                  attributes simultaneously, useful for migration and testing.

        Returns:
            A context manager that will revert the instrumentation when exited.
                Use of this context manager is optional.
        """
        import anthropic

        from .integrations.llm_providers.anthropic import get_endpoint_config, is_async_client, on_response
        from .integrations.llm_providers.llm_provider import instrument_llm_provider
        from .integrations.llm_providers.semconv import normalize_versions

        normalized_versions = normalize_versions(version)
        self._warn_if_not_initialized_for_instrumentation()
        return instrument_llm_provider(
            self,
            anthropic_client
            or (
                anthropic.Anthropic,
                anthropic.AsyncAnthropic,
                anthropic.AnthropicBedrock,
                anthropic.AsyncAnthropicBedrock,
            ),
            suppress_other_instrumentation,
            'Anthropic',
            partial(get_endpoint_config, version=normalized_versions),
            partial(on_response, version=normalized_versions),
            is_async_client,
        )

    def instrument_google_genai(self, **kwargs: Any):
        """Instrument the [Google Gen AI SDK (`google-genai`)](https://googleapis.github.io/python-genai/).

        !!! note
            To capture message contents (i.e. prompts and completions), set the environment variable
            `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT` to `true`.

        Uses the `GoogleGenAiSdkInstrumentor().instrument()` method of the
        [`opentelemetry-instrumentation-google-genai`](https://pypi.org/project/opentelemetry-instrumentation-google-genai/)
        package, to which it passes `**kwargs`.
        """
        from .integrations.google_genai import instrument_google_genai

        self._warn_if_not_initialized_for_instrumentation()
        instrument_google_genai(self, **kwargs)

    def instrument_litellm(self, **kwargs: Any):
        """Instrument the [LiteLLM](https://docs.litellm.ai/) Python SDK.

        !!! warning
            This currently works best if all arguments of instrumented methods are passed as keyword arguments,
            e.g. `litellm.completion(model=model, messages=messages)`.

        Uses the `LiteLLMInstrumentor().instrument()` method of the
        [`openinference-instrumentation-litellm`](https://pypi.org/project/openinference-instrumentation-litellm/)
        package, to which it passes `**kwargs`.
        """
        from .integrations.litellm import instrument_litellm

        self._warn_if_not_initialized_for_instrumentation()
        instrument_litellm(self, **kwargs)

    def instrument_dspy(self, **kwargs: Any):
        """Instrument [DSPy](https://dspy.ai/).

        Uses the `DSPyInstrumentor().instrument()` method of the
        [`openinference-instrumentation-dspy`](https://pypi.org/project/openinference-instrumentation-dspy/)
        package, to which it passes `**kwargs`.
        """
        from .integrations.dspy import instrument_dspy

        self._warn_if_not_initialized_for_instrumentation()
        instrument_dspy(self, **kwargs)

    def instrument_print(self) -> AbstractContextManager[None]:
        """Instrument the built-in `print` function so that calls to it are logged.

        If Logfire is configured with [`inspect_arguments=True`][logfire.configure(inspect_arguments)],
        the names of the arguments passed to `print` will be included in the log attributes
        and will be used for scrubbing.

        The fallback attribute name `logfire.print_args` will be used if:

         - `inspect_arguments` is `False`
         - Inspection fails for any reason
         - Multiple starred arguments are used (e.g. `print(*args1, *args2)`)
            in which case names can't be unambiguously determined.

        Returns:
            A context manager that will revert the instrumentation when exited.
                Use of this context manager is optional.
        """
        from .integrations.print import instrument_print

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_print(self)

    def instrument_asyncpg(self, **kwargs: Any) -> None:
        """Instrument the `asyncpg` module so that spans are automatically created for each query."""
        from .integrations.asyncpg import instrument_asyncpg

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_asyncpg(
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    @overload
    def instrument_httpx(
        self,
        client: httpx.Client,
        *,
        capture_all: bool = False,
        capture_headers: bool = False,
        capture_request_body: bool = False,
        capture_response_body: bool = False,
        request_hook: HttpxRequestHook | None = None,
        response_hook: HttpxResponseHook | None = None,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def instrument_httpx(
        self,
        client: httpx.AsyncClient,
        *,
        capture_all: bool = False,
        capture_headers: bool = False,
        capture_request_body: bool = False,
        capture_response_body: bool = False,
        request_hook: HttpxRequestHook | HttpxAsyncRequestHook | None = None,
        response_hook: HttpxResponseHook | HttpxAsyncResponseHook | None = None,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def instrument_httpx(
        self,
        client: None = None,
        *,
        capture_all: bool = False,
        capture_headers: bool = False,
        capture_request_body: bool = False,
        capture_response_body: bool = False,
        request_hook: HttpxRequestHook | None = None,
        response_hook: HttpxResponseHook | None = None,
        async_request_hook: HttpxAsyncRequestHook | None = None,
        async_response_hook: HttpxAsyncResponseHook | None = None,
        **kwargs: Any,
    ) -> None: ...

    def instrument_httpx(
        self,
        client: httpx.Client | httpx.AsyncClient | None = None,
        *,
        capture_all: bool | None = None,
        capture_headers: bool = False,
        capture_request_body: bool = False,
        capture_response_body: bool = False,
        request_hook: HttpxRequestHook | HttpxAsyncRequestHook | None = None,
        response_hook: HttpxResponseHook | HttpxAsyncResponseHook | None = None,
        async_request_hook: HttpxAsyncRequestHook | None = None,
        async_response_hook: HttpxAsyncResponseHook | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument the `httpx` module so that spans are automatically created for each request.

        Optionally, pass an `httpx.Client` instance to instrument only that client.

        Uses the
        [OpenTelemetry HTTPX Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/httpx/httpx.html)
        library, specifically `HTTPXClientInstrumentor().instrument()`, to which it passes `**kwargs`.

        Args:
            client: The `httpx.Client` or `httpx.AsyncClient` instance to instrument.
                If `None`, the default, all clients will be instrumented.
            capture_all: Set to `True` to capture all HTTP headers, request and response bodies.
                By default checks the environment variable `LOGFIRE_HTTPX_CAPTURE_ALL`.
            capture_headers: Set to `True` to capture all HTTP headers.

                If you don't want to capture all headers, you can customize the headers captured. See the
                [Capture Headers](https://logfire.pydantic.dev/docs/guides/advanced/capture_headers/) section for more info.
            capture_request_body: Set to `True` to capture the request body.
            capture_response_body: Set to `True` to capture the response body.
            request_hook: A function called right after a span is created for a request.
            response_hook: A function called right before a span is finished for the response.
            async_request_hook: A function called right after a span is created for an async request.
            async_response_hook: A function called right before a span is finished for an async response.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` method, for future compatibility.
        """
        from .integrations.httpx import instrument_httpx

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_httpx(
            self,
            client,
            capture_all=capture_all,
            capture_headers=capture_headers,
            capture_request_body=capture_request_body,
            capture_response_body=capture_response_body,
            request_hook=request_hook,
            response_hook=response_hook,
            async_request_hook=async_request_hook,
            async_response_hook=async_response_hook,
            **kwargs,
        )

    def instrument_celery(self, **kwargs: Any) -> None:
        """Instrument `celery` so that spans are automatically created for each task.

        Uses the
        [OpenTelemetry Celery Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/celery/celery.html)
        library.

        For distributed tracing to work correctly, this must be called in **both** the worker processes
        and the application that enqueues tasks (e.g., your Django or FastAPI web server).
        See the [distributed tracing guide](https://logfire.pydantic.dev/docs/how-to-guides/distributed-tracing/#integrations).

        See the [Celery guide](https://logfire.pydantic.dev/docs/integrations/celery/) for more details.

        Args:
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` method, for future compatibility.
        """
        from .integrations.celery import instrument_celery

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_celery(
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_django(
        self,
        capture_headers: bool = False,
        is_sql_commentor_enabled: bool | None = None,
        request_hook: Callable[[trace_api.Span, HttpRequest], None] | None = None,
        response_hook: Callable[[trace_api.Span, HttpRequest, HttpResponse], None] | None = None,
        excluded_urls: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument `django` so that spans are automatically created for each web request.

        Uses the
        [OpenTelemetry Django Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/django/django.html)
        library.

        Args:
            capture_headers: Set to `True` to capture all request and response headers.
            is_sql_commentor_enabled: Adds comments to SQL queries performed by Django,
                so that database logs have additional context.

                This does NOT create spans/logs for the queries themselves.
                For that you need to instrument the database driver, e.g. with `logfire.instrument_psycopg()`.

                To configure the SQL Commentor, see the OpenTelemetry documentation for the
                values that need to be added to `settings.py`.

            request_hook: A function called right after a span is created for a request.
                The function should accept two arguments: the span and the Django `Request` object.

            response_hook: A function called right before a span is finished for the response.
                The function should accept three arguments:
                the span, the Django `Request` object, and the Django `Response` object.

            excluded_urls: A string containing a comma-delimited list of regexes used to exclude URLs from tracking.

            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` method,
                for future compatibility.

        """
        from .integrations.django import instrument_django

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_django(
            capture_headers=capture_headers,
            is_sql_commentor_enabled=is_sql_commentor_enabled,
            request_hook=request_hook,
            response_hook=response_hook,
            excluded_urls=excluded_urls,
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_requests(
        self,
        excluded_urls: str | None = None,
        request_hook: Callable[[Span, requests.PreparedRequest], None] | None = None,
        response_hook: Callable[[Span, requests.PreparedRequest, requests.Response], None] | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument the `requests` module so that spans are automatically created for each request.

        Args:
            excluded_urls: A string containing a comma-delimited list of regexes used to exclude URLs from tracking
            request_hook: A function called right after a span is created for a request.
            response_hook: A function called right before a span is finished for the response.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` methods, for future compatibility.
        """
        from .integrations.requests import instrument_requests

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_requests(
            excluded_urls=excluded_urls,
            request_hook=request_hook,
            response_hook=response_hook,
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    @overload
    def instrument_psycopg(self, conn_or_module: PsycopgConnection | Psycopg2Connection, **kwargs: Any) -> None: ...

    @overload
    def instrument_psycopg(
        self,
        conn_or_module: None | Literal['psycopg', 'psycopg2'] | ModuleType = None,
        enable_commenter: bool = False,
        commenter_options: PsycopgCommenterOptions | None = None,
        **kwargs: Any,
    ) -> None: ...

    def instrument_psycopg(
        self,
        conn_or_module: Any = None,
        enable_commenter: bool = False,
        commenter_options: PsycopgCommenterOptions | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument a `psycopg` connection or module so that spans are automatically created for each query.

        Uses the OpenTelemetry instrumentation libraries for
        [`psycopg`](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/psycopg/psycopg.html)
        and
        [`psycopg2`](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/psycopg2/psycopg2.html).

        Args:
            conn_or_module: Can be:

                - The `psycopg` (version 3) or `psycopg2` module.
                - The string `'psycopg'` or `'psycopg2'` to instrument the module.
                - `None` (the default) to instrument whichever module(s) are installed.
                - A `psycopg` or `psycopg2` connection.

            enable_commenter: Adds comments to SQL queries performed by Psycopg, so that database logs have additional context.
            commenter_options: Configure the tags to be added to the SQL comments.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` methods,
                for future compatibility.
        """
        from .integrations.psycopg import instrument_psycopg

        self._warn_if_not_initialized_for_instrumentation()
        if enable_commenter:
            kwargs.update({'enable_commenter': True, 'commenter_options': commenter_options or {}})
        return instrument_psycopg(self, conn_or_module=conn_or_module, **kwargs)

    def instrument_flask(
        self,
        app: Flask,
        *,
        capture_headers: bool = False,
        enable_commenter: bool = True,
        commenter_options: FlaskCommenterOptions | None = None,
        excluded_urls: str | None = None,
        request_hook: FlaskRequestHook | None = None,
        response_hook: FlaskResponseHook | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument `app` so that spans are automatically created for each request.

        Uses the
        [OpenTelemetry Flask Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/flask/flask.html)
        library, specifically `FlaskInstrumentor().instrument_app()`, to which it passes `**kwargs`.

        Args:
            app: The Flask app to instrument.
            capture_headers: Set to `True` to capture all request and response headers.
            enable_commenter: Adds comments to SQL queries performed by Flask, so that database logs have additional context.
            commenter_options: Configure the tags to be added to the SQL comments.
                See more about it on the [SQLCommenter Configurations](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/flask/flask.html#sqlcommenter-configurations).
            excluded_urls: A string containing a comma-delimited list of regexes used to exclude URLs from tracking.
            request_hook: A function called right after a span is created for a request.
            response_hook: A function called right before a span is finished for the response.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry Flask instrumentation.
        """
        from .integrations.flask import instrument_flask

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_flask(
            app,
            capture_headers=capture_headers,
            enable_commenter=enable_commenter,
            commenter_options=commenter_options,
            excluded_urls=excluded_urls,
            request_hook=request_hook,
            response_hook=response_hook,
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_starlette(
        self,
        app: Starlette,
        *,
        capture_headers: bool = False,
        record_send_receive: bool = False,
        server_request_hook: ServerRequestHook | None = None,
        client_request_hook: ClientRequestHook | None = None,
        client_response_hook: ClientResponseHook | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument `app` so that spans are automatically created for each request.

        Uses the
        [OpenTelemetry Starlette Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/starlette/starlette.html)
        library, specifically `StarletteInstrumentor.instrument_app()`, to which it passes `**kwargs`.

        Args:
            app: The Starlette app to instrument.
            capture_headers: Set to `True` to capture all request and response headers.
            record_send_receive: Set to `True` to allow the OpenTelemetry ASGI middleware to create send/receive spans.

                These are disabled by default to reduce overhead and the number of spans created,
                since many can be created for a single request, and they are not often useful.
                If enabled, they will be set to debug level, meaning they will usually still be hidden in the UI.
            server_request_hook: A function that receives a server span and the ASGI scope for every incoming request.
            client_request_hook: A function that receives a span, the ASGI scope and the receive ASGI message for every ASGI receive event.
            client_response_hook: A function that receives a span, the ASGI scope and the send ASGI message for every ASGI send event.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry Starlette instrumentation.
        """
        from .integrations.starlette import instrument_starlette

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_starlette(
            self,
            app,
            record_send_receive=record_send_receive,
            capture_headers=capture_headers,
            server_request_hook=server_request_hook,
            client_request_hook=client_request_hook,
            client_response_hook=client_response_hook,
            **kwargs,
        )

    def instrument_asgi(
        self,
        app: ASGIApp,
        capture_headers: bool = False,
        record_send_receive: bool = False,
        **kwargs: Unpack[ASGIInstrumentKwargs],
    ) -> ASGIApp:
        """Instrument `app` so that spans are automatically created for each request.

        Uses the ASGI [`OpenTelemetryMiddleware`][opentelemetry.instrumentation.asgi.OpenTelemetryMiddleware] under
        the hood, to which it passes `**kwargs`.

        Warning:
            Instead of modifying the app in place, this method returns the instrumented ASGI application.

        Args:
            app: The ASGI application to instrument.
            capture_headers: Set to `True` to capture all request and response headers.
            record_send_receive: Set to `True` to allow the OpenTelemetry ASGI middleware to create send/receive spans.

                These are disabled by default to reduce overhead and the number of spans created,
                since many can be created for a single request, and they are not often useful.
                If enabled, they will be set to debug level, meaning they will usually still be hidden in the UI.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry ASGI middleware.

        Returns:
            The instrumented ASGI application.
        """
        from .integrations.asgi import instrument_asgi

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_asgi(
            self,
            app,
            record_send_receive=record_send_receive,
            capture_headers=capture_headers,
            **kwargs,
        )

    def instrument_wsgi(
        self,
        app: WSGIApplication,
        capture_headers: bool = False,
        request_hook: WSGIRequestHook | None = None,
        response_hook: WSGIResponseHook | None = None,
        **kwargs: Any,
    ) -> WSGIApplication:
        """Instrument `app` so that spans are automatically created for each request.

        Uses the WSGI [`OpenTelemetryMiddleware`][opentelemetry.instrumentation.wsgi.OpenTelemetryMiddleware] under
        the hood, to which it passes `**kwargs`.

        Warning:
            Instead of modifying the app in place, this method returns the instrumented WSGI application.

        Args:
            app: The WSGI application to instrument.
            capture_headers: Set to `True` to capture all request and response headers.
            request_hook: A function called right after a span is created for a request.
            response_hook: A function called right before a span is finished for the response.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry WSGI middleware.

        Returns:
            The instrumented WSGI application.
        """
        from .integrations.wsgi import instrument_wsgi

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_wsgi(
            app,
            capture_headers=capture_headers,
            request_hook=request_hook,
            response_hook=response_hook,
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_aiohttp_client(
        self,
        *,
        capture_all: bool | None = None,
        capture_headers: bool = False,
        capture_request_body: bool = False,
        capture_response_body: bool = False,
        request_hook: AiohttpClientRequestHook | None = None,
        response_hook: AiohttpClientResponseHook | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument the `aiohttp` module so that spans are automatically created for each client request.

        Uses the
        [OpenTelemetry aiohttp client Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/aiohttp_client/aiohttp_client.html)
        library, specifically `AioHttpClientInstrumentor().instrument()`, to which it passes `**kwargs`.
        """
        from .integrations.aiohttp_client import instrument_aiohttp_client

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_aiohttp_client(
            self,
            capture_all=capture_all,
            capture_request_body=capture_request_body,
            capture_response_body=capture_response_body,
            capture_headers=capture_headers,
            request_hook=request_hook,
            response_hook=response_hook,
            **kwargs,
        )

    def instrument_aiohttp_server(self, **kwargs: Any) -> None:
        """Instrument the `aiohttp` module so that spans are automatically created for each server request.

        Uses the
        [OpenTelemetry aiohttp server Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/aiohttp_server/aiohttp_server.html)
        library, specifically `AioHttpServerInstrumentor().instrument()`, to which it passes `**kwargs`.
        """
        from .integrations.aiohttp_server import instrument_aiohttp_server

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_aiohttp_server(**kwargs)

    def instrument_sqlalchemy(
        self,
        engine: AsyncEngine | Engine | None = None,
        engines: Iterable[AsyncEngine | Engine] | None = None,
        enable_commenter: bool = False,
        commenter_options: SQLAlchemyCommenterOptions | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument the `sqlalchemy` module so that spans are automatically created for each query.

        Uses the
        [OpenTelemetry SQLAlchemy Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/sqlalchemy/sqlalchemy.html)
        library, specifically `SQLAlchemyInstrumentor().instrument()`, to which it passes `**kwargs`.

        Args:
            engine: The `sqlalchemy` engine to instrument.
            engines: An iterable of `sqlalchemy` engines to instrument.
            enable_commenter: Adds comments to SQL queries performed by SQLAlchemy, so that database logs have additional context.
            commenter_options: Configure the tags to be added to the SQL comments.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` methods.
        """
        from .integrations.sqlalchemy import instrument_sqlalchemy

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_sqlalchemy(
            engine=engine,
            engines=engines,
            enable_commenter=enable_commenter,
            commenter_options=commenter_options or {},
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_sqlite3(self, conn: SQLite3Connection = None, **kwargs: Any) -> SQLite3Connection:
        """Instrument the `sqlite3` module or a specific connection so that spans are automatically created for each operation.

        Uses the
        [OpenTelemetry SQLite3 Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/sqlite3/sqlite3.html)
        library.

        Args:
            conn: The `sqlite3` connection to instrument, or `None` to instrument all connections.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` methods.

        Returns:
            If a connection is provided, returns the instrumented connection. If no connection is provided, returns `None`.
        """
        from .integrations.sqlite3 import instrument_sqlite3

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_sqlite3(conn=conn, **{'tracer_provider': self._config.get_tracer_provider(), **kwargs})

    def instrument_aws_lambda(
        self,
        lambda_handler: LambdaHandler,
        event_context_extractor: Callable[[LambdaEvent], Context] | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument AWS Lambda so that spans are automatically created for each invocation.

        Uses the
        [OpenTelemetry AWS Lambda Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/aws_lambda/aws_lambda.html)
        library, specifically `AwsLambdaInstrumentor().instrument()`, to which it passes `**kwargs`.

        Args:
            lambda_handler: The lambda handler function to instrument.
            event_context_extractor: A function that returns an OTel Trace Context given the Lambda Event the AWS.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` methods for future compatibility.
        """
        from .integrations.aws_lambda import instrument_aws_lambda

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_aws_lambda(
            lambda_handler=lambda_handler,
            event_context_extractor=event_context_extractor,
            **{  # type: ignore
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_pymongo(
        self,
        capture_statement: bool = False,
        request_hook: Callable[[Span, CommandStartedEvent], None] | None = None,
        response_hook: Callable[[Span, CommandSucceededEvent], None] | None = None,
        failed_hook: Callable[[Span, CommandFailedEvent], None] | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument the `pymongo` module so that spans are automatically created for each operation.

        Uses the
        [OpenTelemetry pymongo Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/pymongo/pymongo.html)
        library, specifically `PymongoInstrumentor().instrument()`, to which it passes `**kwargs`.

        Args:
            capture_statement: Set to `True` to capture the statement in the span attributes.
            request_hook: A function called when a command is sent to the server.
            response_hook: A function that is called when a command is successfully completed.
            failed_hook: A function that is called when a command fails.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` methods for future compatibility.
        """
        from .integrations.pymongo import instrument_pymongo

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_pymongo(
            capture_statement=capture_statement,
            request_hook=request_hook,
            response_hook=response_hook,
            failed_hook=failed_hook,
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_redis(
        self,
        capture_statement: bool = False,
        request_hook: RedisRequestHook | None = None,
        response_hook: RedisResponseHook | None = None,
        **kwargs: Any,
    ) -> None:
        """Instrument the `redis` module so that spans are automatically created for each operation.

        Uses the
        [OpenTelemetry Redis Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/redis/redis.html)
        library, specifically `RedisInstrumentor().instrument()`, to which it passes `**kwargs`.

        Args:
            capture_statement: Set to `True` to capture the statement in the span attributes.
            request_hook: A function that is called before performing the request.
            response_hook: A function that is called after receiving the response.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` methods for future compatibility.
        """
        from .integrations.redis import instrument_redis

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_redis(
            capture_statement=capture_statement,
            request_hook=request_hook,
            response_hook=response_hook,
            **{
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_mysql(self, conn: MySQLConnection = None, **kwargs: Any) -> MySQLConnection:
        """Instrument the `mysql` module or a specific MySQL connection so that spans are automatically created for each operation.

        Uses the
        [OpenTelemetry MySQL Instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/mysql/mysql.html)
        library.

        Args:
            conn: The `mysql` connection to instrument, or `None` to instrument all connections.
            **kwargs: Additional keyword arguments to pass to the OpenTelemetry `instrument` methods.

        Returns:
            If a connection is provided, returns the instrumented connection. If no connection is provided, returns None.
        """
        from .integrations.mysql import instrument_mysql

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_mysql(
            conn=conn,
            **{  # type: ignore
                'tracer_provider': self._config.get_tracer_provider(),
                'meter_provider': self._config.get_meter_provider(),
                **kwargs,
            },
        )

    def instrument_system_metrics(
        self, config: SystemMetricsConfig | None = None, base: SystemMetricsBase = 'basic'
    ) -> None:
        """Collect system metrics.

        See [the guide](https://logfire.pydantic.dev/docs/integrations/system-metrics/) for more information.

        Args:
            config: A dictionary where the keys are metric names
                and the values are optional further configuration for that metric.
            base: A string indicating the base config dictionary which `config` will be merged with,
                or `None` for an empty base config.
        """
        from .integrations.system_metrics import instrument_system_metrics

        self._warn_if_not_initialized_for_instrumentation()
        return instrument_system_metrics(self, config, base)

    def metric_counter(self, name: str, *, unit: str = '', description: str = '') -> Counter:
        """Create a counter metric.

        A counter is a cumulative metric that represents a single numerical value that only ever goes up.

        ```py
        import logfire

        logfire.configure()
        counter = logfire.metric_counter('exceptions', unit='1', description='Number of exceptions caught')

        try:
            raise Exception('oops')
        except Exception:
            counter.add(1)
        ```

        See the [Opentelemetry documentation](https://opentelemetry.io/docs/specs/otel/metrics/api/#counter) about
        counters.

        Args:
            name: The name of the metric.
            unit: The unit of the metric.
            description: The description of the metric.

        Returns:
            The counter metric.
        """
        return self._meter.create_counter(name, unit, description)

    def metric_histogram(self, name: str, *, unit: str = '', description: str = '') -> Histogram:
        """Create a histogram metric.

        A histogram is a metric that samples observations (usually things like request durations or response sizes).

        ```py
        import logfire

        logfire.configure()
        histogram = logfire.metric_histogram('bank.amount_transferred', unit='$', description='Amount transferred')


        def transfer(amount: int):
            histogram.record(amount)
        ```

        See the [Opentelemetry documentation](https://opentelemetry.io/docs/specs/otel/metrics/api/#histogram) about

        Args:
            name: The name of the metric.
            unit: The unit of the metric.
            description: The description of the metric.

        Returns:
            The histogram metric.
        """
        return self._meter.create_histogram(name, unit, description)

    def metric_gauge(self, name: str, *, unit: str = '', description: str = '') -> Gauge:
        """Create a gauge metric.

        Gauge is a synchronous instrument which can be used to record non-additive measurements.

        ```py
        import logfire

        logfire.configure()
        gauge = logfire.metric_gauge('system.cpu_usage', unit='%', description='CPU usage')


        def update_cpu_usage(cpu_percent):
            gauge.set(cpu_percent)
        ```

        See the [Opentelemetry documentation](https://opentelemetry.io/docs/specs/otel/metrics/api/#gauge) about gauges.

        Args:
            name: The name of the metric.
            unit: The unit of the metric.
            description: The description of the metric.

        Returns:
            The gauge metric.
        """
        return self._meter.create_gauge(name, unit, description)

    def metric_up_down_counter(self, name: str, *, unit: str = '', description: str = '') -> UpDownCounter:
        """Create an up-down counter metric.

        An up-down counter is a cumulative metric that represents a single numerical value that can be adjusted up or
        down.

        ```py
        import logfire

        logfire.configure()
        up_down_counter = logfire.metric_up_down_counter('users.logged_in', unit='1', description='Users logged in')


        def on_login(user):
            up_down_counter.add(1)


        def on_logout(user):
            up_down_counter.add(-1)
        ```

        See the [Opentelemetry documentation](https://opentelemetry.io/docs/specs/otel/metrics/api/#updowncounter) about
        up-down counters.

        Args:
            name: The name of the metric.
            unit: The unit of the metric.
            description: The description of the metric.

        Returns:
            The up-down counter metric.
        """
        return self._meter.create_up_down_counter(name, unit, description)

    def metric_counter_callback(
        self,
        name: str,
        *,
        callbacks: Sequence[CallbackT],
        unit: str = '',
        description: str = '',
    ) -> None:
        """Create a counter metric that uses a callback to collect observations.

        The callback is called every 60 seconds in a background thread.

        The counter metric is a cumulative metric that represents a single numerical value that only ever goes up.

        ```py
        import psutil
        from opentelemetry.metrics import CallbackOptions, Observation

        import logfire

        logfire.configure()


        def cpu_usage_callback(options: CallbackOptions):
            cpu_percents = psutil.cpu_percent(percpu=True)

            for i, cpu_percent in enumerate(cpu_percents):
                yield Observation(cpu_percent, {'cpu': i})


        cpu_usage_counter = logfire.metric_counter_callback(
            'system.cpu.usage',
            callbacks=[cpu_usage_callback],
            unit='%',
            description='CPU usage',
        )
        ```

        See the [Opentelemetry documentation](https://opentelemetry.io/docs/specs/otel/metrics/api/#asynchronous-counter)
        about asynchronous counter.

        Args:
            name: The name of the metric.
            callbacks: A sequence of callbacks that return an iterable of
                [Observation](https://opentelemetry-python.readthedocs.io/en/latest/api/metrics.html#opentelemetry.metrics.Observation).
            unit: The unit of the metric.
            description: The description of the metric.
        """
        self._meter.create_observable_counter(name, callbacks, unit, description)

    def metric_gauge_callback(
        self, name: str, callbacks: Sequence[CallbackT], *, unit: str = '', description: str = ''
    ) -> None:
        """Create a gauge metric that uses a callback to collect observations.

        The callback is called every 60 seconds in a background thread.

        The gauge metric is a metric that represents a single numerical value that can arbitrarily go up and down.

        ```py
        import threading

        from opentelemetry.metrics import CallbackOptions, Observation

        import logfire

        logfire.configure()


        def thread_count_callback(options: CallbackOptions):
            yield Observation(threading.active_count())


        logfire.metric_gauge_callback(
            'system.thread_count',
            callbacks=[thread_count_callback],
            unit='1',
            description='Number of threads',
        )
        ```

        See the [Opentelemetry documentation](https://opentelemetry.io/docs/specs/otel/metrics/api/#asynchronous-gauge)
        about asynchronous gauge.

        Args:
            name: The name of the metric.
            callbacks: A sequence of callbacks that return an iterable of
                [Observation](https://opentelemetry-python.readthedocs.io/en/latest/api/metrics.html#opentelemetry.metrics.Observation).
            unit: The unit of the metric.
            description: The description of the metric.
        """
        self._meter.create_observable_gauge(name, callbacks, unit, description)

    def metric_up_down_counter_callback(
        self, name: str, callbacks: Sequence[CallbackT], *, unit: str = '', description: str = ''
    ) -> None:
        """Create an up-down counter metric that uses a callback to collect observations.

        The callback is called every 60 seconds in a background thread.

        The up-down counter is a cumulative metric that represents a single numerical value that can be adjusted up or
        down.

        ```py
        from opentelemetry.metrics import CallbackOptions, Observation

        import logfire

        logfire.configure()

        items = []


        def inventory_callback(options: CallbackOptions):
            yield Observation(len(items))


        logfire.metric_up_down_counter_callback(
            name='store.inventory',
            description='Number of items in the inventory',
            callbacks=[inventory_callback],
        )
        ```

        See the [Opentelemetry documentation](https://opentelemetry.io/docs/specs/otel/metrics/api/#asynchronous-updowncounter)
        about asynchronous up-down counters.

        Args:
            name: The name of the metric.
            callbacks: A sequence of callbacks that return an iterable of
                [Observation](https://opentelemetry-python.readthedocs.io/en/latest/api/metrics.html#opentelemetry.metrics.Observation).
            unit: The unit of the metric.
            description: The description of the metric.
        """
        self._meter.create_observable_up_down_counter(name, callbacks, unit, description)

    def suppress_scopes(self, *scopes: str) -> None:
        """Prevent spans and metrics from being created for the given OpenTelemetry scope names.

        To get the scope name of a span/metric,
        check the value of the `otel_scope_name` column in the Logfire database.
        """
        self._config.suppress_scopes(*scopes)

    def shutdown(self, timeout_millis: int = 30_000, flush: bool = True) -> bool:  # pragma: no cover
        """Shut down all tracers and meters.

        This will clean up any resources used by the tracers and meters and flush any remaining spans and metrics.

        Args:
            timeout_millis: The timeout in milliseconds.
            flush: Whether to flush remaining spans and metrics before shutting down.

        Returns:
            `False` if the timeout was reached before the shutdown was completed, `True` otherwise.
        """
        start = time()

        def remaining_ms() -> int:
            return max(0, int(timeout_millis - (time() - start) * 1000))

        self.config.get_variable_provider().shutdown(timeout_millis=remaining_ms())
        remaining = remaining_ms()
        if not remaining:  # pragma: no cover
            return False

        if flush:  # pragma: no branch
            self._tracer_provider.force_flush(remaining)
            remaining = remaining_ms()
            if not remaining:  # pragma: no cover
                return False

        self._tracer_provider.shutdown()
        remaining = remaining_ms()
        if not remaining:  # pragma: no cover
            return False

        if flush:  # pragma: no branch
            self._meter_provider.force_flush(remaining)
            remaining = remaining_ms()
            if not remaining:  # pragma: no cover
                return False

        self._meter_provider.shutdown(remaining)
        remaining = remaining_ms()
        if not remaining:  # pragma: no cover
            return False

        return remaining_ms() > 0

    @overload
    def var(
        self,
        name: str,
        *,
        default: T,
        description: str | None = None,
    ) -> Variable[T]: ...

    @overload
    def var(
        self,
        name: str,
        *,
        type: type[T],
        default: T | ResolveFunction[T],
        description: str | None = None,
    ) -> Variable[T]: ...

    def var(
        self,
        name: str,
        *,
        type: type[T] | None = None,
        default: T | ResolveFunction[T],
        description: str | None = None,
    ) -> Variable[T]:
        """Define a managed variable.

        Managed variables let you externalize runtime configuration from your code,
        controlling values from the Logfire UI without redeploying. Use `.get()` on the
        returned `Variable` to resolve the current value.

        See the [managed variables guide](https://logfire.pydantic.dev/docs/reference/advanced/managed-variables/)
        for more details.

        ```py
        import logfire

        logfire.configure()

        # Simple primitive variable (type inferred from default)
        feature_enabled = logfire.var('feature_enabled', default=False)

        # Use the variable
        with feature_enabled.get(targeting_key='user-123') as resolved:
            if resolved.value:
                ...
        ```

        Args:
            name: Unique identifier for the variable. Must match the name configured in the
                Logfire UI when using remote variables.
            type: Expected type for validation and JSON schema generation. Can be a primitive
                type or a Pydantic model. If not provided, the type is inferred from `default`.
                Required when `default` is a resolve function.
            default: Default value used when no remote configuration is found.
                When `type` is not provided, the type is inferred from this value.
                Can also be a callable with `targeting_key` and `attributes` parameters
                (requires `type` to be set explicitly).
            description: Optional human-readable description of what the variable controls.
        """
        from logfire.variables.variable import Variable, is_resolve_function

        if type is None:
            if is_resolve_function(default):
                raise TypeError(
                    'When `default` is a resolve function (callable with targeting_key and attributes parameters), '
                    '`type` must be provided to specify the variable value type.'
                )
            tp = cast(Type[T], default.__class__)  # noqa UP006
        else:
            tp = type

        import re

        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(
                f"Invalid variable name '{name}'. "
                'Variable names must be valid Python identifiers (letters, digits, and underscores, '
                'not starting with a digit).'
            )

        if name in self._variables:
            raise ValueError(
                f"A variable with name '{name}' has already been registered. Each variable must have a unique name."
            )

        variable = Variable[T](name, default=default, type=tp, logfire_instance=self, description=description)
        self._variables[name] = variable

        return variable

    def variables_clear(self) -> None:
        """Clear all registered variables from this Logfire instance.

        This removes all variables previously registered via [`var()`][logfire.Logfire.var],
        allowing them to be re-registered. This is primarily intended for use in tests
        to ensure a clean state between test cases.
        """
        self._variables.clear()

    def variables_get(self) -> list[Variable[Any]]:
        """Get all variables registered with this Logfire instance."""
        return list(self._variables.values())

    def variables_push(
        self,
        variables: list[Variable[Any]] | None = None,
        *,
        dry_run: bool = False,
        yes: bool = False,
        strict: bool = False,
    ) -> bool:
        """Push variable definitions (metadata only) to the configured variable provider.

        This method syncs local variable definitions with the provider:
        - Creates new variables that don't exist in the provider
        - Updates JSON schemas for existing variables if they've changed
        - Warns about existing label values that are incompatible with new schemas

        The provider is determined by the Logfire configuration. For remote providers,
        this requires proper authentication (via VariablesOptions or LOGFIRE_API_KEY).

        Args:
            variables: Variable instances to push. If None, all variables
                registered with this Logfire instance will be pushed.
            dry_run: If True, only show what would change without applying.
            yes: If True, skip confirmation prompt.
            strict: If True, fail if any existing label values are incompatible with new schemas.

        Returns:
            True if changes were applied (or would be applied in dry_run mode), False otherwise.

        Example:
            ```python
            import logfire

            feature_enabled = logfire.var(name='feature_enabled', type=bool, default=False)
            max_retries = logfire.var(name='max_retries', type=int, default=3)

            if __name__ == '__main__':
                # Push all registered variables
                logfire.variables_push()

                # Or push specific variables only
                logfire.variables_push([feature_enabled])
            ```
        """
        if variables is None:
            variables = self.variables_get()  # pragma: no cover

        provider = self.config.get_variable_provider()
        return provider.push_variables(variables, dry_run=dry_run, yes=yes, strict=strict)

    def variables_push_types(
        self,
        types: Sequence[type[Any] | tuple[type[Any], str]],
        *,
        dry_run: bool = False,
        yes: bool = False,
        strict: bool = False,
    ) -> bool:
        """Push variable type definitions to the configured variable provider.

        Variable types are reusable schema definitions that can be referenced by variables.
        They help organize and standardize variable schemas across your project.

        This method syncs local Python types with the provider:
        - Creates new types that don't exist in the provider
        - Updates schemas for existing types if they've changed
        - Shows a diff of changes before applying
        - Checks if existing variable label values are compatible with the new schemas

        The provider is determined by the Logfire configuration. For remote providers,
        this requires proper authentication (via VariablesOptions or LOGFIRE_API_KEY).

        Args:
            types: Types to push. Items can be:
                - A type (name defaults to __name__ or str(type))
                - A tuple of (type, name) for explicit naming
            dry_run: If True, only show what would change without applying.
            yes: If True, skip confirmation prompt.
            strict: If True, abort when existing label values are incompatible with
                the new type schema.

        Returns:
            True if changes were applied (or would be applied in dry_run mode), False otherwise.

        Example:
            ```python
            import logfire
            from pydantic import BaseModel


            class FeatureConfig(BaseModel):
                enabled: bool = False
                max_retries: int = 3
                timeout_seconds: float = 30.0


            class UserSettings(BaseModel):
                theme: str = 'light'
                notifications_enabled: bool = True


            if __name__ == '__main__':
                # Push type definitions using their class names
                logfire.variables_push_types([FeatureConfig, UserSettings])

                # Or push with explicit names
                logfire.variables_push_types(
                    [
                        (FeatureConfig, 'my-feature-config'),
                        (UserSettings, 'my-user-settings'),
                    ]
                )
            ```
        """
        provider = self.config.get_variable_provider()
        return provider.push_variable_types(types, dry_run=dry_run, yes=yes, strict=strict)

    def variables_validate(
        self,
        variables: list[Variable[Any]] | None = None,
    ) -> ValidationReport:
        """Validate that provider-side variable label values match local type definitions.

        This method fetches the current variable configuration from the provider and
        validates that all label values can be deserialized to the expected types
        defined in the local Variable instances.

        Args:
            variables: Variable instances to validate. If None, all variables
                registered with this Logfire instance will be validated.

        Returns:
            A ValidationReport containing any errors found. Use `report.is_valid` to check
            if validation passed, and `report.format()` to get a human-readable summary.

        Example:
            ```python
            import logfire

            feature_enabled = logfire.var(name='feature_enabled', type=bool, default=False)
            max_retries = logfire.var(name='max_retries', type=int, default=3)

            if __name__ == '__main__':
                # Validate all registered variables
                logfire.variables_validate()

                # Or validate specific variables only
                report = logfire.variables_validate([feature_enabled])
                assert report.is_valid
            ```
        """
        if variables is None:
            variables = self.variables_get()  # pragma: no cover

        provider = self.config.get_variable_provider()
        return provider.validate_variables(variables)

    def variables_push_config(
        self,
        config: VariablesConfig,
        *,
        mode: Literal['merge', 'replace'] = 'merge',
        dry_run: bool = False,
        yes: bool = False,
    ) -> bool:  # pragma: no cover
        """Push a VariablesConfig to the configured provider.

        This method pushes a complete VariablesConfig (including labels and rollouts)
        to the provider. It's useful for:
        - Pushing configs generated or modified locally
        - Pushing configs read from files
        - Partial updates (merge mode) or full replacement (replace mode)

        Args:
            config: The VariablesConfig to sync.
            mode: 'merge' updates/creates only variables in config (leaves others unchanged).
                  'replace' makes the server match the config exactly (deletes missing variables).
            dry_run: If True, only show what would change without applying.
            yes: If True, skip confirmation prompt.

        Returns:
            True if changes were applied (or would be applied in dry_run mode), False otherwise.

        Example:
            ```python skip="true"
            import logfire
            from logfire.variables import VariablesConfig

            # Push config to server
            logfire.variables_push_config(config)

            # Or merge just a subset of variables
            logfire.variables_push_config(config, mode='merge')
            ```
        """
        provider = self.config.get_variable_provider()
        return provider.push_config(config, mode=mode, dry_run=dry_run, yes=yes)

    def variables_pull_config(self) -> VariablesConfig:  # pragma: no cover
        """Pull the current variable configuration from the provider.

        This method fetches the complete configuration from the provider,
        useful for generating local copies of the config that can be modified.

        Returns:
            The current VariablesConfig from the provider.

        Example:
            ```python skip="true"
            import logfire

            # Pull config from the provider
            config = logfire.variables_pull_config()
            print(config.model_dump_json(indent=2))
            ```
        """
        provider = self.config.get_variable_provider()
        return provider.pull_config()

    def variables_build_config(
        self,
        variables: list[Variable[Any]] | None = None,
    ) -> VariablesConfig:
        """Build a VariablesConfig from registered Variable instances.

        This creates a minimal config with just the name, schema, and example for each variable.
        No labels or versions are created - use this to build a template config that can be edited.

        Args:
            variables: Variable instances to include. If None, uses all registered variables.

        Returns:
            A VariablesConfig with minimal configs for each variable.

        Example:
            ```python skip="true"
            import logfire

            feature_enabled = logfire.var(name='feature_enabled', type=bool, default=False)
            max_retries = logfire.var(name='max_retries', type=int, default=3)

            # Build config from registered variables
            config = logfire.variables_build_config()
            print(config.model_dump_json(indent=2))
            ```
        """
        if variables is None:
            variables = self.variables_get()  # pragma: no cover

        from logfire.variables.config import VariablesConfig

        return VariablesConfig.from_variables(variables)


class FastLogfireSpan:
    """A simple version of `LogfireSpan` optimized for auto-tracing."""

    __slots__ = ('_span', '_token')

    def __init__(self, span: trace_api.Span) -> None:
        self._span = span
        self._token = context_api.attach(trace_api.set_span_in_context(self._span))

    def __enter__(self) -> FastLogfireSpan:
        return self

    @handle_internal_errors
    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: Any) -> None:
        context_api.detach(self._token)
        self._span.__exit__(exc_type, exc_value, traceback)


# Changes to this class may need to be reflected in `FastLogfireSpan` and `NoopSpan` as well.
class LogfireSpan(ReadableSpan):
    def __init__(
        self,
        span_name: str,
        otlp_attributes: dict[str, otel_types.AttributeValue],
        tracer: _ProxyTracer,
        json_schema_properties: JsonSchemaProperties,
        links: Sequence[tuple[SpanContext, otel_types.Attributes]],
        span_kind: SpanKind = SpanKind.INTERNAL,
    ) -> None:
        self._span_name = span_name
        self._otlp_attributes = otlp_attributes
        self._tracer = tracer
        self._json_schema_properties = json_schema_properties
        self._links = list(trace_api.Link(context=context, attributes=attributes) for context, attributes in links)
        self._span_kind = span_kind

        self._added_attributes = False
        self._token: None | Token[Context] = None
        self._span: None | _LogfireWrappedSpan = None

    if not TYPE_CHECKING:  # pragma: no branch

        def __getattr__(self, name: str) -> Any:
            return getattr(self._span, name)

    @handle_internal_errors
    def _start(self):
        if self._span is not None:
            return
        self._span = self._tracer.start_span(
            name=self._span_name,
            attributes=self._otlp_attributes,
            links=self._links,
            kind=self._span_kind,
        )

    @handle_internal_errors
    def _attach(self):
        if self._token is not None:
            return
        assert self._span is not None
        self._token = context_api.attach(trace_api.set_span_in_context(self._span))

    def __enter__(self) -> LogfireSpan:
        self._start()
        self._attach()
        return self

    @handle_internal_errors
    def _end(self):
        if not self._span or not self._span.is_recording():
            return
        if self._added_attributes:
            self._span.set_attribute(ATTRIBUTES_JSON_SCHEMA_KEY, attributes_json_schema(self._json_schema_properties))
        self._span.end()

    def _detach(self):
        if self._token is None:
            return
        context_api.detach(self._token)
        self._token = None

    @handle_internal_errors
    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: Any) -> None:
        self._detach()
        if self._span and self._span.is_recording() and isinstance(exc_value, BaseException):
            self._span.record_exception(exc_value, escaped=True)
        self._end()

    @property
    def message_template(self) -> str | None:  # pragma: no cover
        return self._get_attribute(ATTRIBUTES_MESSAGE_TEMPLATE_KEY, None)

    @property
    def tags(self) -> tuple[str, ...]:
        return self._get_attribute(ATTRIBUTES_TAGS_KEY, ())

    @tags.setter
    @handle_internal_errors
    def tags(self, new_tags: Sequence[str]) -> None:
        """Set or add tags to the span."""
        if isinstance(new_tags, str):
            new_tags = (new_tags,)
        self._set_attribute(ATTRIBUTES_TAGS_KEY, tuple(uniquify_sequence(new_tags)))

    @property
    def message(self) -> str:
        return self._get_attribute(ATTRIBUTES_MESSAGE_KEY, self._span_name)

    @message.setter
    def message(self, message: str):
        self._set_attribute(ATTRIBUTES_MESSAGE_KEY, message)

    @handle_internal_errors
    def set_attribute(self, key: str, value: Any) -> None:
        """Sets an attribute on the span.

        Args:
            key: The key of the attribute.
            value: The value of the attribute.
        """
        self._added_attributes = True
        self._json_schema_properties[key] = create_json_schema(value, set())
        otel_value = self._otlp_attributes[key] = prepare_otlp_attribute(value)
        if self._span is not None:  # pragma: no branch
            self._span.set_attribute(key, otel_value)

    def set_attributes(self, attributes: dict[str, Any]) -> None:
        """Sets the given attributes on the span."""
        for key, value in attributes.items():
            self.set_attribute(key, value)

    def add_link(self, context: SpanContext, attributes: otel_types.Attributes = None) -> None:
        if self._span is None:
            self._links += [trace_api.Link(context=context, attributes=attributes)]
        else:
            self._span.add_link(context, attributes)

    # TODO(Marcelo): We should add a test for `record_exception`.
    def record_exception(
        self,
        exception: BaseException,
        attributes: otel_types.Attributes = None,
        timestamp: int | None = None,
        escaped: bool = False,
    ) -> None:  # pragma: no cover
        """Records an exception as a span event.

        Delegates to the OpenTelemetry SDK `Span.record_exception` method.
        """
        if self._span is None:
            raise RuntimeError('Span has not been started')

        self._span.record_exception(
            exception,
            attributes=attributes,
            timestamp=timestamp,
            escaped=escaped,
        )

    def is_recording(self) -> bool:
        return self._span is not None and self._span.is_recording()

    @handle_internal_errors
    def set_level(self, level: LevelName | int):
        """Set the log level of this span."""
        attributes = log_level_attributes(level)
        if self._span is None:
            self._otlp_attributes.update(attributes)
        else:
            self._span.set_attributes(attributes)

    def _get_attribute(self, key: str, default: Any) -> Any:
        attributes = getattr(self._span, 'attributes', self._otlp_attributes)
        return attributes.get(key, default)

    def _set_attribute(self, key: str, value: Any) -> None:
        """Set an attribute on the span or in the _otlp_attributes if span is not yet created."""
        if self._span is None:
            self._otlp_attributes[key] = value
        else:
            self._span.set_attribute(key, value)


class NoopSpan:
    """Implements the same methods as `LogfireSpan` but does nothing.

    Used in place of `LogfireSpan` and `FastLogfireSpan` when an exception occurs during span creation.
    This way code like:

        with logfire.span(...) as span:
            span.set_attribute(...)

    doesn't raise an error even if `logfire.span` fails internally.
    If `logfire.span` just returned `None` then the `with` block and the `span.set_attribute` call would raise an error.

    TODO this should also be used when tracing is disabled, e.g. before `logfire.configure()` has been called.
    """

    def __init__(self, *_args: Any, **__kwargs: Any) -> None:
        pass

    def __getattr__(self, _name: str) -> Any:
        # Handle methods of LogfireSpan which return nothing
        return lambda *_args, **__kwargs: None  # type: ignore

    def __enter__(self) -> NoopSpan:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: Any) -> None:
        pass

    # Implement methods/properties that return something to get the type right.
    @property
    def message_template(self) -> str:  # pragma: no cover
        return ''

    @property
    def tags(self) -> tuple[str, ...]:
        return ()

    # This is required to make `span.tags = ` not raise an error.
    @tags.setter
    def tags(self, new_tags: Sequence[str]) -> None:
        pass

    @property
    def message(self) -> str:  # pragma: no cover
        return ''

    # This is required to make `span.message = ` not raise an error.
    @message.setter
    def message(self, message: str):
        pass

    def is_recording(self) -> bool:
        return False


AttributesValueType = TypeVar('AttributesValueType', bound=Union[Any, otel_types.AttributeValue])


def prepare_otlp_attributes(attributes: dict[str, Any]) -> dict[str, otel_types.AttributeValue]:
    """Prepare attributes for sending to OpenTelemetry.

    This will convert any non-OpenTelemetry compatible types to JSON.
    """
    return {key: prepare_otlp_attribute(value) for key, value in attributes.items()}


def prepare_otlp_attribute(value: Any) -> otel_types.AttributeValue:
    """Convert a user attribute to an OpenTelemetry compatible type."""
    if isinstance(value, Enum):
        return logfire_json_dumps(value)
    elif isinstance(value, int):
        if value > OTLP_MAX_INT_SIZE:
            warnings.warn(
                f'Integer value {value} is larger than the maximum OTLP integer size of {OTLP_MAX_INT_SIZE} (64-bits), '
                ' if you need support for sending larger integers, please open a feature request',
                UserWarning,
            )
            return str(value)
        else:
            return value
    elif isinstance(value, (str, bool, float)):
        return value
    else:
        return logfire_json_dumps(value)


def set_user_attributes_on_raw_span(span: Span, attributes: dict[str, Any]) -> None:
    if not span.is_recording():
        return

    otlp_attributes = prepare_otlp_attributes(attributes)
    if json_schema_properties := attributes_json_schema_properties(attributes):  # pragma: no branch
        existing_properties = JsonSchemaProperties({})
        existing_json_schema_str = (span.attributes or {}).get(ATTRIBUTES_JSON_SCHEMA_KEY)
        if existing_json_schema_str and isinstance(existing_json_schema_str, str):
            with contextlib.suppress(json.JSONDecodeError):
                existing_json_schema = json.loads(existing_json_schema_str)
                existing_properties = existing_json_schema.get('properties', {})
        existing_properties.update(json_schema_properties)
        otlp_attributes[ATTRIBUTES_JSON_SCHEMA_KEY] = attributes_json_schema(existing_properties)
    span.set_attributes(otlp_attributes)


P = ParamSpec('P')
R = TypeVar('R')

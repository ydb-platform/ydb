from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager, nullcontext
from typing import Any, ContextManager, Literal, TYPE_CHECKING, Sequence
from unittest.mock import MagicMock


try:
    logfire_module = importlib.import_module('logfire')
    sys.modules[__name__] = logfire_module

except ImportError:
    if not TYPE_CHECKING:  # pragma: no branch
        LevelName = Literal['trace', 'debug', 'info', 'notice', 'warn', 'warning', 'error', 'fatal']
        VERSION = '0.0.0'
        METRICS_PREFERRED_TEMPORALITY = {}

        def configure(*args, **kwargs): ...

        class LogfireSpan:
            def __getattr__(self, attr):
                return MagicMock()

            @property
            def name(self):
                return ''

            @property
            def attributes(self):
                return {}

            @property
            def events(self):
                return ()

            @property
            def links(self):
                return ()

            def __enter__(self):
                return self

            def __exit__(self, *args, **kwargs) -> None: ...

            @property
            def message_template(self) -> str:  # pragma: no cover
                return ''

            @property
            def tags(self) -> tuple[str, ...]:  # pragma: no cover
                return ()

            @tags.setter
            def tags(self, new_tags: Sequence[str]) -> None: ...  # pragma: no cover

            @property
            def message(self) -> str:  # pragma: no cover
                return ''

            @message.setter
            def message(self, message: str): ...  # pragma: no cover

            def is_recording(self) -> bool:  # pragma: no cover
                return False

            @property
            def context(self):
                return None

            @property
            def instrumentation_scope(self):
                return None

            @property
            def start_time(self):
                return None

            @property
            def end_time(self):
                return None

            @property
            def parent(self):
                return None

            def set_attribute(self, key: str, value: Any) -> None: ... # pragma: no cover

        class Logfire:
            def __getattr__(self, attr):
                return MagicMock()  # pragma: no cover

            def __init__(self, *args, **kwargs) -> None: ...

            def span(self, *args, **kwargs) -> LogfireSpan:
                return LogfireSpan()

            def log(self, *args, **kwargs) -> None: ...

            def trace(self, *args, **kwargs) -> None: ...

            def debug(self, *args, **kwargs) -> None: ...

            def notice(self, *args, **kwargs) -> None: ...

            def info(self, *args, **kwargs) -> None: ...

            def warning(self, *args, **kwargs) -> None: ...

            warn = warning

            def error(self, *args, **kwargs) -> None: ...

            def exception(self, *args, **kwargs) -> None: ...

            def fatal(self, *args, **kwargs) -> None: ...

            def suppress_scopes(self, *args, **kwargs) -> None: ...

            def with_tags(self, *args, **kwargs) -> Logfire:
                return self

            def with_settings(self, *args, **kwargs) -> Logfire:
                return self

            def force_flush(self, *args, **kwargs) -> None: ...

            def log_slow_async_callbacks(self, *args, **kwargs) -> None:  # pragma: no cover
                return nullcontext()

            def install_auto_tracing(self, *args, **kwargs) -> None: ...

            def instrument(self, *args, **kwargs):
                def decorator(func):
                    return func

                return decorator

            def instrument_asgi(self, app, *args, **kwargs):
                return app

            def instrument_wsgi(self, app, *args, **kwargs):
                return app

            def instrument_fastapi(self, *args, **kwargs) -> ContextManager[None]:
                return nullcontext()

            def instrument_pydantic(self, *args, **kwargs) -> None: ...

            def instrument_pydantic_ai(self, *args, **kwargs) -> None: ...

            def instrument_pymongo(self, *args, **kwargs) -> None: ...

            def instrument_sqlalchemy(self, *args, **kwargs) -> None: ...

            def instrument_sqlite3(self, *args, **kwargs) -> None: ...

            def instrument_aws_lambda(self, *args, **kwargs) -> None: ...

            def instrument_redis(self, *args, **kwargs) -> None: ...

            def instrument_flask(self, *args, **kwargs) -> None: ...

            def instrument_starlette(self, *args, **kwargs) -> None: ...

            def instrument_django(self, *args, **kwargs) -> None: ...

            def instrument_psycopg(self, *args, **kwargs) -> None: ...

            def instrument_surrealdb(self, *args, **kwargs) -> None: ...

            def instrument_requests(self, *args, **kwargs) -> None: ...

            def instrument_httpx(self, *args, **kwargs) -> None: ...

            def instrument_asyncpg(self, *args, **kwargs) -> None: ...

            def instrument_anthropic(self, *args, **kwargs) -> ContextManager[None]:
                return nullcontext()

            def instrument_openai(self, *args, **kwargs) -> ContextManager[None]:
                return nullcontext()

            def instrument_print(self, *args, **kwargs) -> ContextManager[None]:
                return nullcontext()

            def instrument_openai_agents(self, *args, **kwargs) -> None: ...

            def instrument_google_genai(self, *args, **kwargs) -> None: ...

            def instrument_litellm(self, *args, **kwargs) -> None: ...

            def instrument_dspy(self, *args, **kwargs) -> None: ...

            def instrument_aiohttp_client(self, *args, **kwargs) -> None: ...

            def instrument_aiohttp_server(self, *args, **kwargs) -> None: ...

            def instrument_system_metrics(self, *args, **kwargs) -> None: ...

            def instrument_mcp(self, *args, **kwargs) -> None: ...

            def shutdown(self, *args, **kwargs) -> None: ...


        DEFAULT_LOGFIRE_INSTANCE = Logfire()
        span = DEFAULT_LOGFIRE_INSTANCE.span
        log = DEFAULT_LOGFIRE_INSTANCE.log
        trace = DEFAULT_LOGFIRE_INSTANCE.trace
        debug = DEFAULT_LOGFIRE_INSTANCE.debug
        notice = DEFAULT_LOGFIRE_INSTANCE.notice
        info = DEFAULT_LOGFIRE_INSTANCE.info
        warn = DEFAULT_LOGFIRE_INSTANCE.warn
        warning = DEFAULT_LOGFIRE_INSTANCE.warning
        error = DEFAULT_LOGFIRE_INSTANCE.error
        exception = DEFAULT_LOGFIRE_INSTANCE.exception
        fatal = DEFAULT_LOGFIRE_INSTANCE.fatal
        with_tags = DEFAULT_LOGFIRE_INSTANCE.with_tags
        with_settings = DEFAULT_LOGFIRE_INSTANCE.with_settings
        force_flush = DEFAULT_LOGFIRE_INSTANCE.force_flush
        log_slow_async_callbacks = DEFAULT_LOGFIRE_INSTANCE.log_slow_async_callbacks
        install_auto_tracing = DEFAULT_LOGFIRE_INSTANCE.install_auto_tracing
        instrument = DEFAULT_LOGFIRE_INSTANCE.instrument
        instrument_asgi = DEFAULT_LOGFIRE_INSTANCE.instrument_asgi
        instrument_wsgi = DEFAULT_LOGFIRE_INSTANCE.instrument_wsgi
        instrument_pydantic = DEFAULT_LOGFIRE_INSTANCE.instrument_pydantic
        instrument_pydantic_ai = DEFAULT_LOGFIRE_INSTANCE.instrument_pydantic_ai
        instrument_fastapi = DEFAULT_LOGFIRE_INSTANCE.instrument_fastapi
        instrument_openai = DEFAULT_LOGFIRE_INSTANCE.instrument_openai
        instrument_openai_agents = DEFAULT_LOGFIRE_INSTANCE.instrument_openai_agents
        instrument_anthropic = DEFAULT_LOGFIRE_INSTANCE.instrument_anthropic
        instrument_google_genai = DEFAULT_LOGFIRE_INSTANCE.instrument_google_genai
        instrument_litellm = DEFAULT_LOGFIRE_INSTANCE.instrument_litellm
        instrument_dspy = DEFAULT_LOGFIRE_INSTANCE.instrument_dspy
        instrument_asyncpg = DEFAULT_LOGFIRE_INSTANCE.instrument_asyncpg
        instrument_print = DEFAULT_LOGFIRE_INSTANCE.instrument_print
        instrument_celery = DEFAULT_LOGFIRE_INSTANCE.instrument_celery
        instrument_httpx = DEFAULT_LOGFIRE_INSTANCE.instrument_httpx
        instrument_requests = DEFAULT_LOGFIRE_INSTANCE.instrument_requests
        instrument_surrealdb = DEFAULT_LOGFIRE_INSTANCE.instrument_surrealdb
        instrument_psycopg = DEFAULT_LOGFIRE_INSTANCE.instrument_psycopg
        instrument_django = DEFAULT_LOGFIRE_INSTANCE.instrument_django
        instrument_flask = DEFAULT_LOGFIRE_INSTANCE.instrument_flask
        instrument_starlette = DEFAULT_LOGFIRE_INSTANCE.instrument_starlette
        instrument_aiohttp_client = DEFAULT_LOGFIRE_INSTANCE.instrument_aiohttp_client
        instrument_aiohttp_server = DEFAULT_LOGFIRE_INSTANCE.instrument_aiohttp_server
        instrument_sqlalchemy = DEFAULT_LOGFIRE_INSTANCE.instrument_sqlalchemy
        instrument_sqlite3 = DEFAULT_LOGFIRE_INSTANCE.instrument_sqlite3
        instrument_aws_lambda = DEFAULT_LOGFIRE_INSTANCE.instrument_aws_lambda
        instrument_redis = DEFAULT_LOGFIRE_INSTANCE.instrument_redis
        instrument_pymongo = DEFAULT_LOGFIRE_INSTANCE.instrument_pymongo
        instrument_mysql = DEFAULT_LOGFIRE_INSTANCE.instrument_mysql
        instrument_system_metrics = DEFAULT_LOGFIRE_INSTANCE.instrument_system_metrics
        instrument_mcp = DEFAULT_LOGFIRE_INSTANCE.instrument_mcp
        shutdown = DEFAULT_LOGFIRE_INSTANCE.shutdown
        suppress_scopes = DEFAULT_LOGFIRE_INSTANCE.suppress_scopes

        def loguru_handler() -> dict[str, Any]:
            return {}

        def no_auto_trace(x):
            return x

        def add_non_user_code_prefix(*args, **kwargs) -> None: ...

        @contextmanager
        def suppress_instrumentation():
            yield

        class ConsoleOptions:
            def __init__(self, *args, **kwargs) -> None: ...

        class SamplingOptions:
            def __init__(self, *args, **kwargs) -> None: ...

        class CodeSource:
            def __init__(self, *args, **kwargs) -> None: ...

        class ScrubbingOptions:
            def __init__(self, *args, **kwargs) -> None: ...

        class AdvancedOptions:
            def __init__(self, *args, **kwargs) -> None: ...

        class MetricsOptions:
            def __init__(self, *args, **kwargs) -> None: ...


        class PydanticPlugin:
            def __init__(self, *args, **kwargs) -> None: ...

        class ScrubMatch:
            def __init__(self, *args, **kwargs) -> None: ...

        class AutoTraceModule:
            def __init__(self, *args, **kwargs) -> None: ...

        class StructlogProcessor:
            def __init__(self, *args, **kwargs) -> None: ...

        class LogfireLoggingHandler:
            def __init__(self, *args, **kwargs) -> None: ...

        def logfire_info() -> str:
            """Show versions of logfire, OS and related packages."""
            return 'logfire_info() is not implement by logfire-api'

        def get_baggage(*args, **kwargs) -> dict[str, str]:
            return {}

        def set_baggage(*args, **kwargs) -> ContextManager[None]:
            return nullcontext()

        def get_context(*args, **kwargs) -> dict[str, Any]:
            return {}

        def attach_context(*args, **kwargs)-> ContextManager[None]:
            return nullcontext()

"""**Logfire** is the observability tool focused on developer experience."""

from __future__ import annotations

from typing import Any

from logfire.propagate import attach_context, get_context
from logfire.sampling import SamplingOptions

from . import variables as variables
from ._internal.auto_trace import AutoTraceModule
from ._internal.auto_trace.rewrite_ast import no_auto_trace
from ._internal.baggage import get_baggage, set_baggage
from ._internal.cli import logfire_info
from ._internal.config import (
    AdvancedOptions,
    CodeSource,
    ConsoleOptions,
    LocalVariablesOptions,
    MetricsOptions,
    PydanticPlugin,
    VariablesOptions,
    configure,
)
from ._internal.constants import LevelName
from ._internal.main import Logfire, LogfireSpan
from ._internal.scrubbing import ScrubbingOptions, ScrubMatch
from ._internal.stack_info import add_non_user_code_prefix
from ._internal.utils import suppress_instrumentation
from .integrations.logging import LogfireLoggingHandler
from .integrations.structlog import LogfireProcessor as StructlogProcessor
from .version import VERSION

DEFAULT_LOGFIRE_INSTANCE: Logfire = Logfire()
span = DEFAULT_LOGFIRE_INSTANCE.span
instrument = DEFAULT_LOGFIRE_INSTANCE.instrument
force_flush = DEFAULT_LOGFIRE_INSTANCE.force_flush
log_slow_async_callbacks = DEFAULT_LOGFIRE_INSTANCE.log_slow_async_callbacks
install_auto_tracing = DEFAULT_LOGFIRE_INSTANCE.install_auto_tracing
instrument_pydantic = DEFAULT_LOGFIRE_INSTANCE.instrument_pydantic
instrument_pydantic_ai = DEFAULT_LOGFIRE_INSTANCE.instrument_pydantic_ai
instrument_asgi = DEFAULT_LOGFIRE_INSTANCE.instrument_asgi
instrument_wsgi = DEFAULT_LOGFIRE_INSTANCE.instrument_wsgi
instrument_fastapi = DEFAULT_LOGFIRE_INSTANCE.instrument_fastapi
instrument_openai = DEFAULT_LOGFIRE_INSTANCE.instrument_openai
instrument_openai_agents = DEFAULT_LOGFIRE_INSTANCE.instrument_openai_agents
instrument_anthropic = DEFAULT_LOGFIRE_INSTANCE.instrument_anthropic
instrument_google_genai = DEFAULT_LOGFIRE_INSTANCE.instrument_google_genai
instrument_litellm = DEFAULT_LOGFIRE_INSTANCE.instrument_litellm
instrument_dspy = DEFAULT_LOGFIRE_INSTANCE.instrument_dspy
instrument_print = DEFAULT_LOGFIRE_INSTANCE.instrument_print
instrument_asyncpg = DEFAULT_LOGFIRE_INSTANCE.instrument_asyncpg
instrument_httpx = DEFAULT_LOGFIRE_INSTANCE.instrument_httpx
instrument_celery = DEFAULT_LOGFIRE_INSTANCE.instrument_celery
instrument_requests = DEFAULT_LOGFIRE_INSTANCE.instrument_requests
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
instrument_surrealdb = DEFAULT_LOGFIRE_INSTANCE.instrument_surrealdb
instrument_system_metrics = DEFAULT_LOGFIRE_INSTANCE.instrument_system_metrics
instrument_mcp = DEFAULT_LOGFIRE_INSTANCE.instrument_mcp
suppress_scopes = DEFAULT_LOGFIRE_INSTANCE.suppress_scopes
shutdown = DEFAULT_LOGFIRE_INSTANCE.shutdown
with_tags = DEFAULT_LOGFIRE_INSTANCE.with_tags
# with_trace_sample_rate = DEFAULT_LOGFIRE_INSTANCE.with_trace_sample_rate
with_settings = DEFAULT_LOGFIRE_INSTANCE.with_settings

# Logging
log = DEFAULT_LOGFIRE_INSTANCE.log
trace = DEFAULT_LOGFIRE_INSTANCE.trace
debug = DEFAULT_LOGFIRE_INSTANCE.debug
info = DEFAULT_LOGFIRE_INSTANCE.info
notice = DEFAULT_LOGFIRE_INSTANCE.notice
warn = DEFAULT_LOGFIRE_INSTANCE.warn
warning = DEFAULT_LOGFIRE_INSTANCE.warning
error = DEFAULT_LOGFIRE_INSTANCE.error
fatal = DEFAULT_LOGFIRE_INSTANCE.fatal
exception = DEFAULT_LOGFIRE_INSTANCE.exception

# Metrics
metric_counter = DEFAULT_LOGFIRE_INSTANCE.metric_counter
metric_histogram = DEFAULT_LOGFIRE_INSTANCE.metric_histogram
metric_up_down_counter = DEFAULT_LOGFIRE_INSTANCE.metric_up_down_counter
metric_gauge = DEFAULT_LOGFIRE_INSTANCE.metric_gauge
metric_counter_callback = DEFAULT_LOGFIRE_INSTANCE.metric_counter_callback
metric_gauge_callback = DEFAULT_LOGFIRE_INSTANCE.metric_gauge_callback
metric_up_down_counter_callback = DEFAULT_LOGFIRE_INSTANCE.metric_up_down_counter_callback

# Variables
var = DEFAULT_LOGFIRE_INSTANCE.var
variables_clear = DEFAULT_LOGFIRE_INSTANCE.variables_clear
variables_get = DEFAULT_LOGFIRE_INSTANCE.variables_get
variables_push = DEFAULT_LOGFIRE_INSTANCE.variables_push
variables_push_types = DEFAULT_LOGFIRE_INSTANCE.variables_push_types
variables_validate = DEFAULT_LOGFIRE_INSTANCE.variables_validate
variables_push_config = DEFAULT_LOGFIRE_INSTANCE.variables_push_config
variables_pull_config = DEFAULT_LOGFIRE_INSTANCE.variables_pull_config
variables_build_config = DEFAULT_LOGFIRE_INSTANCE.variables_build_config


def loguru_handler() -> Any:
    """Create a **Logfire** handler for Loguru.

    Returns:
        A dictionary with the handler and format for Loguru.
    """
    from .integrations import loguru

    return {'sink': loguru.LogfireHandler(), 'format': '{message}'}


__version__ = VERSION

__all__ = (
    'Logfire',
    'LogfireSpan',
    'LevelName',
    'AdvancedOptions',
    'ConsoleOptions',
    'CodeSource',
    'PydanticPlugin',
    'configure',
    'span',
    'instrument',
    'log',
    'trace',
    'debug',
    'notice',
    'info',
    'warn',
    'warning',
    'error',
    'exception',
    'fatal',
    'force_flush',
    'log_slow_async_callbacks',
    'install_auto_tracing',
    'instrument_asgi',
    'instrument_wsgi',
    'instrument_pydantic',
    'instrument_pydantic_ai',
    'instrument_fastapi',
    'instrument_openai',
    'instrument_openai_agents',
    'instrument_anthropic',
    'instrument_google_genai',
    'instrument_litellm',
    'instrument_dspy',
    'instrument_print',
    'instrument_asyncpg',
    'instrument_httpx',
    'instrument_celery',
    'instrument_requests',
    'instrument_psycopg',
    'instrument_django',
    'instrument_flask',
    'instrument_starlette',
    'instrument_aiohttp_client',
    'instrument_aiohttp_server',
    'instrument_sqlalchemy',
    'instrument_sqlite3',
    'instrument_aws_lambda',
    'instrument_redis',
    'instrument_pymongo',
    'instrument_mysql',
    'instrument_surrealdb',
    'instrument_system_metrics',
    'instrument_mcp',
    'AutoTraceModule',
    'with_tags',
    'with_settings',
    # 'with_trace_sample_rate',
    'suppress_scopes',
    'shutdown',
    'no_auto_trace',
    'ScrubMatch',
    'ScrubbingOptions',
    'VERSION',
    'add_non_user_code_prefix',
    'suppress_instrumentation',
    'StructlogProcessor',
    'LogfireLoggingHandler',
    'loguru_handler',
    'SamplingOptions',
    'MetricsOptions',
    'VariablesOptions',
    'LocalVariablesOptions',
    'variables',
    'var',
    'variables_clear',
    'variables_get',
    'variables_push',
    'variables_push_types',
    'variables_validate',
    'variables_push_config',
    'variables_pull_config',
    'variables_build_config',
    'logfire_info',
    'get_baggage',
    'set_baggage',
    'get_context',
    'attach_context',
)

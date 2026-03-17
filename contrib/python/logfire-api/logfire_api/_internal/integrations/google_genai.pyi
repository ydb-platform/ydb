import logfire
from _typeshed import Incomplete
from logfire._internal.utils import handle_internal_errors as handle_internal_errors, safe_repr as safe_repr
from opentelemetry._logs import LogRecord, Logger, LoggerProvider
from typing import Any
from typing_extensions import TypeAlias

original_flatten_compound_value: Incomplete

def wrapped_flatten_compound_value(key: str, value: Any, *args: Any, **kwargs: Any): ...

original_to_dict: Any
ANY_ADAPTER: Incomplete

def wrapped_to_dict(obj: object) -> object: ...

Part: TypeAlias

def default_json(x: Any) -> str: ...

class SpanEventLogger(Logger):
    @handle_internal_errors
    def emit(self, record: LogRecord) -> None: ...

def transform_part(part: Part) -> Part: ...

class SpanEventLoggerProvider(LoggerProvider):
    def get_logger(self, *args: Any, **kwargs: Any) -> SpanEventLogger: ...

def instrument_google_genai(logfire_instance: logfire.Logfire, **kwargs: Any): ...

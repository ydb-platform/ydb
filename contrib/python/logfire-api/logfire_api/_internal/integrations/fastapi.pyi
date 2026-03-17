from ..constants import ONE_SECOND_IN_NANOSECONDS as ONE_SECOND_IN_NANOSECONDS
from ..main import Logfire as Logfire, NoopSpan as NoopSpan, set_user_attributes_on_raw_span as set_user_attributes_on_raw_span
from ..stack_info import StackInfo as StackInfo, get_code_object_info as get_code_object_info
from ..utils import handle_internal_errors as handle_internal_errors, maybe_capture_server_headers as maybe_capture_server_headers
from .asgi import tweak_asgi_spans_tracer_provider as tweak_asgi_spans_tracer_provider
from _typeshed import Incomplete
from collections.abc import Awaitable, Iterable
from contextlib import AbstractContextManager, contextmanager
from fastapi import FastAPI
from functools import lru_cache
from opentelemetry.trace import Span
from starlette.requests import Request
from starlette.websockets import WebSocket
from typing import Any, Callable

def find_mounted_apps(app: FastAPI) -> list[FastAPI]:
    """Fetch all sub-apps mounted to a FastAPI app, including nested sub-apps."""
def instrument_fastapi(logfire_instance: Logfire, app: FastAPI, *, capture_headers: bool = False, request_attributes_mapper: Callable[[Request | WebSocket, dict[str, Any]], dict[str, Any] | None] | None = None, excluded_urls: str | Iterable[str] | None = None, record_send_receive: bool = False, extra_spans: bool = False, **opentelemetry_kwargs: Any) -> AbstractContextManager[None]:
    """Instrument a FastAPI app so that spans and logs are automatically created for each request.

    See `Logfire.instrument_fastapi` for more details.
    """
@lru_cache
def patch_fastapi():
    """Globally monkeypatch fastapi functions and return a dictionary for recording instrumentation config per app."""

class FastAPIInstrumentation:
    logfire_instance: Incomplete
    timestamp_generator: Incomplete
    request_attributes_mapper: Incomplete
    extra_spans: Incomplete
    def __init__(self, logfire_instance: Logfire, request_attributes_mapper: Callable[[Request | WebSocket, dict[str, Any]], dict[str, Any] | None], extra_spans: bool) -> None: ...
    @contextmanager
    def pseudo_span(self, namespace: str, root_span: Span):
        """Record start and end timestamps in the root span, and possibly exceptions."""
    async def solve_dependencies(self, request: Request | WebSocket, original: Awaitable[Any]) -> Any: ...
    async def run_endpoint_function(self, original_run_endpoint_function: Any, request: Request, dependant: Any, values: dict[str, Any], **kwargs: Any) -> Any: ...

class _InstrumentedValues(dict):
    request: Request

LOGFIRE_SPAN_SCOPE_KEY: str

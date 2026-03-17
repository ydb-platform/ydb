from _typeshed import Incomplete
from aiohttp.client_reqrep import ClientResponse
from aiohttp.tracing import TraceRequestEndParams, TraceRequestExceptionParams, TraceRequestStartParams
from email.headerregistry import ContentTypeHeader
from logfire import Logfire as Logfire, LogfireSpan as LogfireSpan
from logfire._internal.config import GLOBAL_CONFIG as GLOBAL_CONFIG
from logfire._internal.main import set_user_attributes_on_raw_span as set_user_attributes_on_raw_span
from logfire._internal.stack_info import warn_at_user_stacklevel as warn_at_user_stacklevel
from logfire._internal.utils import handle_internal_errors as handle_internal_errors
from logfire.integrations.aiohttp_client import AioHttpRequestHeaders as AioHttpRequestHeaders, AioHttpResponseHeaders as AioHttpResponseHeaders, RequestHook as RequestHook, ResponseHook as ResponseHook
from opentelemetry.trace import Span
from typing import Any, Callable, Literal, ParamSpec
from yarl import URL

P = ParamSpec('P')

def instrument_aiohttp_client(logfire_instance: Logfire, capture_all: bool | None, capture_request_body: bool, capture_response_body: bool, capture_headers: bool, request_hook: RequestHook | None, response_hook: ResponseHook | None, **kwargs: Any) -> None:
    """Instrument the `aiohttp` module so that spans are automatically created for each client request.

    See the `Logfire.instrument_aiohttp_client` method for details.
    """

class LogfireClientInfoMixin:
    """Mixin providing content-type header parsing for charset detection."""
    headers: AioHttpRequestHeaders
    @property
    def content_type_header_object(self) -> ContentTypeHeader:
        """Parse the Content-Type header into a structured object."""
    @property
    def content_type_header_string(self) -> str:
        """Get the raw Content-Type header value."""
    @property
    def content_type_charset(self) -> str:
        """Extract charset from Content-Type header, defaulting to utf-8."""

class LogfireAioHttpRequestInfo(TraceRequestStartParams, LogfireClientInfoMixin):
    span: Span
    def capture_headers(self) -> None: ...
    @handle_internal_errors
    def capture_body_if_text(self, attr_name: str = 'http.request.body.text'): ...

class LogfireAioHttpResponseInfo(LogfireClientInfoMixin):
    span: Span
    method: str
    url: URL
    headers: AioHttpRequestHeaders
    response: ClientResponse | None
    exception: BaseException | None
    logfire_instance: Logfire
    body_captured: bool
    def capture_headers(self) -> None: ...
    def capture_body_if_text(self, attr_name: str = 'http.response.body.text') -> None: ...
    def capture_text_as_json(self, span: LogfireSpan, *, text: str, attr_name: str) -> None: ...
    @classmethod
    def create_from_trace_params(cls, span: Span, params: TraceRequestEndParams | TraceRequestExceptionParams, logfire_instance: Logfire) -> LogfireAioHttpResponseInfo: ...

def make_request_hook(hook: RequestHook | None, capture_headers: bool, capture_request_body: bool) -> RequestHook | None: ...
def make_response_hook(hook: ResponseHook | None, logfire_instance: Logfire, capture_headers: bool, capture_response_body: bool) -> ResponseHook | None: ...
def capture_request(span: Span, request: TraceRequestStartParams, capture_headers: bool, capture_request_body: bool) -> LogfireAioHttpRequestInfo: ...
def capture_response(span: Span, response: TraceRequestEndParams | TraceRequestExceptionParams, logfire_instance: Logfire, capture_headers: bool, capture_response_body: bool) -> LogfireAioHttpResponseInfo: ...
def run_hook(hook: Callable[P, Any] | None, *args: P.args, **kwargs: P.kwargs) -> None: ...
def capture_request_or_response_headers(span: Span, headers: AioHttpRequestHeaders | AioHttpResponseHeaders, request_or_response: Literal['request', 'response']) -> None: ...

CODES_FOR_METHODS_WITH_DATA_OR_JSON_PARAM: Incomplete

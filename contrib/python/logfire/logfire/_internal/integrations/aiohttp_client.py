from __future__ import annotations

import functools
import inspect
from email.headerregistry import ContentTypeHeader
from email.policy import EmailPolicy
from functools import cache
from typing import TYPE_CHECKING, Any, Callable, Literal, cast

import attr
from aiohttp.client import ClientSession
from aiohttp.client_reqrep import ClientResponse
from aiohttp.tracing import TraceRequestEndParams, TraceRequestExceptionParams, TraceRequestStartParams
from opentelemetry.trace import NonRecordingSpan, Span, use_span
from yarl import URL

from logfire._internal.config import GLOBAL_CONFIG
from logfire._internal.main import set_user_attributes_on_raw_span

try:
    from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_aiohttp_client()` requires the `opentelemetry-instrumentation-aiohttp-client` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[aiohttp-client]'"
    )

from logfire import Logfire, LogfireSpan
from logfire._internal.stack_info import warn_at_user_stacklevel
from logfire._internal.utils import handle_internal_errors
from logfire.integrations.aiohttp_client import AioHttpRequestHeaders, AioHttpResponseHeaders, RequestHook, ResponseHook

if TYPE_CHECKING:
    from typing import ParamSpec

    P = ParamSpec('P')


def instrument_aiohttp_client(
    logfire_instance: Logfire,
    capture_all: bool | None,
    capture_request_body: bool,
    capture_response_body: bool,
    capture_headers: bool,
    request_hook: RequestHook | None,
    response_hook: ResponseHook | None,
    **kwargs: Any,
) -> None:
    """Instrument the `aiohttp` module so that spans are automatically created for each client request.

    See the `Logfire.instrument_aiohttp_client` method for details.
    """
    if capture_all and (capture_headers or capture_request_body or capture_response_body):
        warn_at_user_stacklevel(
            'You should use either `capture_all` or the specific capture parameters, not both.', UserWarning
        )

    capture_all = cast(bool, GLOBAL_CONFIG.param_manager.load_param('aiohttp_client_capture_all', capture_all))

    should_capture_headers = capture_headers or capture_all
    should_capture_request_body = capture_request_body or capture_all
    should_capture_response_body = capture_response_body or capture_all

    logfire_instance = logfire_instance.with_settings(custom_scope_suffix='aiohttp_client')

    AioHttpClientInstrumentor().instrument(
        **{
            'tracer_provider': logfire_instance.config.get_tracer_provider(),
            'request_hook': make_request_hook(request_hook, should_capture_headers, should_capture_request_body),
            'response_hook': make_response_hook(
                response_hook,
                logfire_instance,
                should_capture_headers,
                should_capture_response_body,
            ),
            'meter_provider': logfire_instance.config.get_meter_provider(),
            **kwargs,
        },
    )


class LogfireClientInfoMixin:
    """Mixin providing content-type header parsing for charset detection."""

    headers: AioHttpRequestHeaders

    @property
    def content_type_header_object(self) -> ContentTypeHeader:
        """Parse the Content-Type header into a structured object."""
        return _content_type_header_from_string(self.content_type_header_string)

    @property
    def content_type_header_string(self) -> str:
        """Get the raw Content-Type header value."""
        return self.headers.get('content-type', '')

    @property
    def content_type_charset(self) -> str:
        """Extract charset from Content-Type header, defaulting to utf-8."""
        return self.content_type_header_object.params.get('charset', 'utf-8')


@attr.s(auto_attribs=True, frozen=True, slots=True)
class LogfireAioHttpRequestInfo(TraceRequestStartParams, LogfireClientInfoMixin):
    span: Span

    def capture_headers(self):
        capture_request_or_response_headers(self.span, self.headers, 'request')

    @handle_internal_errors
    def capture_body_if_text(self, attr_name: str = 'http.request.body.text'):
        frame = inspect.currentframe()
        try:
            while frame is not None:
                if frame.f_code in CODES_FOR_METHODS_WITH_DATA_OR_JSON_PARAM:
                    json_data = frame.f_locals.get('json')
                    data = frame.f_locals.get('data')

                    if json_data is not None:
                        self._set_complex_span_attributes({attr_name: json_data})
                        return

                    if data is not None:
                        self._capture_data_body(data, attr_name)
                        return

                frame = frame.f_back
        finally:
            del frame

    def _capture_data_body(self, data: Any, attr_name: str) -> None:
        if isinstance(data, bytes):
            try:
                text = data.decode(self.content_type_charset)
            except (UnicodeDecodeError, LookupError):
                return
            self._capture_text_as_json(attr_name, text)
        elif isinstance(data, str):
            self._capture_text_as_json(attr_name, data)
        elif isinstance(data, dict):
            self._set_complex_span_attributes({'http.request.body.form': data})

    def _capture_text_as_json(self, attr_name: str, text: str) -> None:
        self._set_complex_span_attributes({attr_name: {}})
        self.span.set_attribute(attr_name, text)

    def _set_complex_span_attributes(self, attributes: dict[str, Any]) -> None:
        set_user_attributes_on_raw_span(self.span, attributes)  # type: ignore


@attr.s(auto_attribs=True, slots=True)
class LogfireAioHttpResponseInfo(LogfireClientInfoMixin):
    span: Span
    method: str
    url: URL
    headers: AioHttpRequestHeaders
    response: ClientResponse | None
    exception: BaseException | None
    logfire_instance: Logfire
    body_captured: bool = False

    def capture_headers(self):
        if self.response:
            capture_request_or_response_headers(self.span, self.response.headers, 'response')

    def capture_body_if_text(self, attr_name: str = 'http.response.body.text') -> None:
        response = self.response
        if response is None:
            return

        original_read = response.read

        @functools.wraps(original_read)
        async def read() -> bytes:
            if self.body_captured:
                return await original_read()

            with (
                use_span(NonRecordingSpan(self.span.get_span_context())),
                self.logfire_instance.span('Reading response body') as span,
            ):
                body = await original_read()
                try:
                    encoding = response.get_encoding()
                    text = body.decode(encoding)
                except (UnicodeDecodeError, LookupError):
                    self.body_captured = True
                    return body
                self.capture_text_as_json(span, text=text, attr_name=attr_name)
                self.body_captured = True
                return body

        response.read = read

    def capture_text_as_json(self, span: LogfireSpan, *, text: str, attr_name: str) -> None:
        span.set_attribute(attr_name, {})
        span._span.set_attribute(attr_name, text)  # type: ignore

    @classmethod
    def create_from_trace_params(
        cls,
        span: Span,
        params: TraceRequestEndParams | TraceRequestExceptionParams,
        logfire_instance: Logfire,
    ) -> LogfireAioHttpResponseInfo:
        return cls(
            span=span,
            method=params.method,
            url=params.url,
            headers=params.headers,
            response=getattr(params, 'response', None),
            exception=getattr(params, 'exception', None),
            logfire_instance=logfire_instance,
        )


def make_request_hook(
    hook: RequestHook | None, capture_headers: bool, capture_request_body: bool
) -> RequestHook | None:
    if not (capture_headers or capture_request_body or hook):
        return None

    def new_hook(span: Span, request: TraceRequestStartParams) -> None:
        with handle_internal_errors:
            capture_request(span, request, capture_headers, capture_request_body)
            run_hook(hook, span, request)

    return new_hook


def make_response_hook(
    hook: ResponseHook | None,
    logfire_instance: Logfire,
    capture_headers: bool,
    capture_response_body: bool,
) -> ResponseHook | None:
    if not (capture_headers or capture_response_body or hook):
        return None

    def new_hook(span: Span, response: TraceRequestEndParams | TraceRequestExceptionParams) -> None:
        with handle_internal_errors:
            capture_response(
                span,
                response,
                logfire_instance,
                capture_headers,
                capture_response_body,
            )
            run_hook(hook, span, response)

    return new_hook


def capture_request(
    span: Span,
    request: TraceRequestStartParams,
    capture_headers: bool,
    capture_request_body: bool,
) -> LogfireAioHttpRequestInfo:
    request_info = LogfireAioHttpRequestInfo(method=request.method, url=request.url, headers=request.headers, span=span)

    if capture_headers:
        request_info.capture_headers()

    if capture_request_body:
        request_info.capture_body_if_text()

    return request_info


def capture_response(
    span: Span,
    response: TraceRequestEndParams | TraceRequestExceptionParams,
    logfire_instance: Logfire,
    capture_headers: bool,
    capture_response_body: bool,
) -> LogfireAioHttpResponseInfo:
    response_info = LogfireAioHttpResponseInfo.create_from_trace_params(
        span=span, params=response, logfire_instance=logfire_instance
    )

    if capture_headers:
        response_info.capture_headers()

    if capture_response_body:
        response_info.capture_body_if_text()

    return response_info


def run_hook(hook: Callable[P, Any] | None, *args: P.args, **kwargs: P.kwargs) -> None:
    if hook:
        hook(*args, **kwargs)


def capture_request_or_response_headers(
    span: Span,
    headers: AioHttpRequestHeaders | AioHttpResponseHeaders,
    request_or_response: Literal['request', 'response'],
) -> None:
    span.set_attributes(
        {
            f'http.{request_or_response}.header.{header_name}': headers.getall(header_name)
            for header_name in headers.keys()
        }
    )


CODES_FOR_METHODS_WITH_DATA_OR_JSON_PARAM = [
    inspect.unwrap(ClientSession._request).__code__,  # type: ignore
]


@cache
def _content_type_header_from_string(content_type: str) -> ContentTypeHeader:
    return EmailPolicy.header_factory('content-type', content_type)

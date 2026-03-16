from __future__ import annotations

import contextlib
import functools
import inspect
from collections.abc import Awaitable, Mapping
from email.headerregistry import ContentTypeHeader
from email.policy import EmailPolicy
from functools import cached_property, lru_cache
from typing import TYPE_CHECKING, Any, Callable, Literal, cast

import httpx
from opentelemetry.trace import NonRecordingSpan, Span, use_span

from logfire._internal.config import GLOBAL_CONFIG
from logfire._internal.stack_info import warn_at_user_stacklevel

try:
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    from logfire.integrations.httpx import (
        AsyncRequestHook,
        AsyncResponseHook,
        RequestHook,
        RequestInfo,
        ResponseHook,
        ResponseInfo,
    )
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_httpx()` requires the `opentelemetry-instrumentation-httpx` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[httpx]'"
    )

from logfire import Logfire, LogfireSpan
from logfire._internal.main import set_user_attributes_on_raw_span
from logfire._internal.utils import handle_internal_errors

if TYPE_CHECKING:
    from typing import ParamSpec

    P = ParamSpec('P')


def instrument_httpx(
    logfire_instance: Logfire,
    client: httpx.Client | httpx.AsyncClient | None,
    capture_all: bool | None,
    capture_headers: bool,
    capture_request_body: bool,
    capture_response_body: bool,
    request_hook: RequestHook | AsyncRequestHook | None,
    response_hook: ResponseHook | AsyncResponseHook | None,
    async_request_hook: AsyncRequestHook | None,
    async_response_hook: AsyncResponseHook | None,
    **kwargs: Any,
) -> None:
    """Instrument the `httpx` module so that spans are automatically created for each request.

    See the `Logfire.instrument_httpx` method for details.
    """
    if capture_all and (capture_headers or capture_request_body or capture_response_body):
        warn_at_user_stacklevel(
            'You should use either `capture_all` or the specific capture parameters, not both.', UserWarning
        )

    capture_request_headers = kwargs.get('capture_request_headers')
    capture_response_headers = kwargs.get('capture_response_headers')

    if capture_request_headers is not None:
        warn_at_user_stacklevel(
            'The `capture_request_headers` parameter is deprecated. Use `capture_headers` instead.', DeprecationWarning
        )
    if capture_response_headers is not None:
        warn_at_user_stacklevel(
            'The `capture_response_headers` parameter is deprecated. Use `capture_headers` instead.', DeprecationWarning
        )

    capture_all = cast(bool, GLOBAL_CONFIG.param_manager.load_param('httpx_capture_all', capture_all))

    should_capture_request_headers = capture_request_headers or capture_headers or capture_all
    should_capture_response_headers = capture_response_headers or capture_headers or capture_all
    should_capture_request_body = capture_request_body or capture_all
    should_capture_response_body = capture_response_body or capture_all

    del (  # Make sure these aren't used accidentally
        capture_all,
        capture_headers,
        capture_request_body,
        capture_response_body,
        capture_request_headers,
        capture_response_headers,
    )

    final_kwargs: dict[str, Any] = {
        'tracer_provider': logfire_instance.config.get_tracer_provider(),
        'meter_provider': logfire_instance.config.get_meter_provider(),
        **kwargs,
    }
    del kwargs  # make sure only final_kwargs is used

    instrumentor = HTTPXClientInstrumentor()
    logfire_instance = logfire_instance.with_settings(custom_scope_suffix='httpx')

    if client is None:
        request_hook = cast('RequestHook | None', request_hook)
        response_hook = cast('ResponseHook | None', response_hook)
        final_kwargs['request_hook'] = make_request_hook(
            request_hook, should_capture_request_headers, should_capture_request_body
        )
        final_kwargs['response_hook'] = make_response_hook(
            response_hook,
            should_capture_response_headers,
            should_capture_response_body,
            logfire_instance,
        )
        final_kwargs['async_request_hook'] = make_async_request_hook(
            async_request_hook, should_capture_request_headers, should_capture_request_body
        )
        final_kwargs['async_response_hook'] = make_async_response_hook(
            async_response_hook,
            should_capture_response_headers,
            should_capture_response_body,
            logfire_instance,
        )

        instrumentor.instrument(**final_kwargs)
    else:
        if isinstance(client, httpx.AsyncClient):
            request_hook = make_async_request_hook(
                request_hook,
                should_capture_request_headers,
                should_capture_request_body,
            )
            response_hook = make_async_response_hook(
                response_hook,
                should_capture_response_headers,
                should_capture_response_body,
                logfire_instance,
            )
        else:
            request_hook = cast('RequestHook | None', request_hook)
            response_hook = cast('ResponseHook | None', response_hook)

            request_hook = make_request_hook(request_hook, should_capture_request_headers, should_capture_request_body)
            response_hook = make_response_hook(
                response_hook,
                should_capture_response_headers,
                should_capture_response_body,
                logfire_instance,
            )

        tracer_provider = final_kwargs['tracer_provider']
        meter_provider = final_kwargs['meter_provider']
        client_kwargs = dict(
            tracer_provider=tracer_provider,
            request_hook=request_hook,
            response_hook=response_hook,
        )
        try:
            instrumentor.instrument_client(client, meter_provider=meter_provider, **client_kwargs)
        except TypeError:  # pragma: no cover
            # This is a fallback for older versions of opentelemetry-instrumentation-httpx
            instrumentor.instrument_client(client, **client_kwargs)


class LogfireHttpxInfoMixin:
    headers: httpx.Headers

    @property
    def content_type_header_object(self) -> ContentTypeHeader:
        return content_type_header_from_string(self.content_type_header_string)

    @property
    def content_type_header_string(self) -> str:
        return self.headers.get('content-type', '')


class LogfireHttpxRequestInfo(RequestInfo, LogfireHttpxInfoMixin):
    span: Span

    def capture_headers(self):
        capture_request_or_response_headers(self.span, self.headers, 'request')

    def capture_body(self):
        captured_form = self.capture_body_if_form()
        if not captured_form:
            self.capture_body_if_text()

    def capture_body_if_text(self, attr_name: str = 'http.request.body.text'):
        if not self.body_is_streaming:
            content = self.content
            if content:
                try:
                    text = self.content.decode(self.content_type_charset)
                except (UnicodeDecodeError, LookupError):
                    return
                self.capture_text_as_json(attr_name=attr_name, text=text)

    def capture_body_if_form(self, attr_name: str = 'http.request.body.form') -> bool:
        if not self.content_type_header_string == 'application/x-www-form-urlencoded':
            return False

        data = self.form_data
        if not (data and isinstance(data, Mapping)):  # pragma: no cover  # type: ignore
            return False
        self.set_complex_span_attributes({attr_name: data})
        return True

    def capture_text_as_json(self, attr_name: str, text: str):
        self.set_complex_span_attributes({attr_name: {}})  # Set the JSON schema
        self.span.set_attribute(attr_name, text)

    @property
    def body_is_streaming(self):
        return not isinstance(self.stream, httpx.ByteStream)

    @property
    def content_type_charset(self):
        return self.content_type_header_object.params.get('charset', 'utf-8')

    @property
    def content(self) -> bytes:
        if self.body_is_streaming:  # pragma: no cover
            raise ValueError('Cannot read content from a streaming body')
        return list(self.stream)[0]  # type: ignore

    @cached_property
    def form_data(self) -> Mapping[str, Any] | None:
        frame = inspect.currentframe().f_back.f_back.f_back  # type: ignore
        while frame:
            if frame.f_code in CODES_FOR_METHODS_WITH_DATA_PARAM:
                break
            frame = frame.f_back
        else:  # pragma: no cover
            return

        return frame.f_locals.get('data')

    def set_complex_span_attributes(self, attributes: dict[str, Any]):
        set_user_attributes_on_raw_span(self.span, attributes)  # type: ignore


class LogfireHttpxResponseInfo(ResponseInfo, LogfireHttpxInfoMixin):
    span: Span
    logfire_instance: Logfire
    is_async: bool

    def capture_headers(self):
        capture_request_or_response_headers(self.span, self.headers, 'response')

    def capture_body_if_text(self, attr_name: str = 'http.response.body.text'):
        def hook(span: LogfireSpan):
            try:
                # response.text uses errors='replace' under the hood.
                # We rely on decoding errors to guess when the response is not text.
                text = self.response.content.decode(self.response.encoding or 'utf-8')
            except (UnicodeDecodeError, LookupError):
                return
            self.capture_text_as_json(span, attr_name=attr_name, text=text)

        self.on_response_read(hook)

    @cached_property
    def response(self) -> httpx.Response:
        frame = inspect.currentframe().f_back.f_back  # type: ignore
        while frame:  # pragma: no branch
            response = frame.f_locals.get('response')
            frame = frame.f_back
            if isinstance(response, httpx.Response):
                return response
        raise RuntimeError('Could not find the response object')  # pragma: no cover

    def on_response_read(self, hook: Callable[[LogfireSpan], None]):
        if self.is_async:

            async def aread(original_aread: Callable[[], Awaitable[bytes]]) -> bytes:
                with self.attach_original_span_context(), self.logfire_instance.span('Reading response body') as span:
                    content = await original_aread()
                    hook(span)
                return content

            self.wrap_response_aread(aread)
        else:

            def read(original_read: Callable[[], bytes]) -> bytes:
                with self.attach_original_span_context(), self.logfire_instance.span('Reading response body') as span:
                    content = original_read()
                    hook(span)
                return content

            self.wrap_response_read(read)

    def wrap_response_read(self, hook: Callable[[Callable[[], bytes]], bytes]):
        response = self.response
        original_read = response.read

        @functools.wraps(original_read)
        def read() -> bytes:
            try:
                # Only log the body the first time it's read
                return response.content
            except httpx.ResponseNotRead:
                return hook(original_read)

        response.read = read

    def wrap_response_aread(self, hook: Callable[[Callable[[], Awaitable[bytes]]], Awaitable[bytes]]):
        response = self.response
        original_aread = response.aread

        @functools.wraps(original_aread)
        async def aread() -> bytes:
            try:
                # Only log the body the first time it's read
                return response.content
            except httpx.ResponseNotRead:
                return await hook(original_aread)

        response.aread = aread

    @contextlib.contextmanager
    def attach_original_span_context(self):
        with use_span(NonRecordingSpan(self.span.get_span_context())):
            yield

    def capture_text_as_json(self, span: LogfireSpan, *, text: str, attr_name: str):
        span.set_attribute(attr_name, {})  # Set the JSON schema
        # Set the attribute to the raw text so that the backend can parse it
        span._span.set_attribute(attr_name, text)  # type: ignore


def make_request_hook(hook: RequestHook | None, capture_headers: bool, capture_body: bool) -> RequestHook | None:
    if not (capture_headers or capture_body or hook):
        return None

    def new_hook(span: Span, request: RequestInfo) -> None:
        with handle_internal_errors:
            request = capture_request(span, request, capture_headers, capture_body)
            run_hook(hook, span, request)

    return new_hook


def make_async_request_hook(
    hook: AsyncRequestHook | RequestHook | None,
    should_capture_headers: bool,
    should_capture_body: bool,
) -> AsyncRequestHook | None:
    if not (should_capture_headers or should_capture_body or hook):
        return None

    async def new_hook(span: Span, request: RequestInfo) -> None:
        with handle_internal_errors:
            request = capture_request(span, request, should_capture_headers, should_capture_body)
            await run_async_hook(hook, span, request)

    return new_hook


def make_response_hook(
    hook: ResponseHook | None,
    capture_headers: bool,
    capture_body: bool,
    logfire_instance: Logfire,
) -> ResponseHook | None:
    if not (capture_headers or capture_body or hook):
        return None

    def new_hook(span: Span, request: RequestInfo, response: ResponseInfo) -> None:
        with handle_internal_errors:
            request, response = capture_response(
                span,
                request,
                response,
                logfire_instance,
                capture_headers,
                capture_body,
                is_async=False,
            )
            run_hook(hook, span, request, response)

    return new_hook


def make_async_response_hook(
    hook: ResponseHook | AsyncResponseHook | None,
    should_capture_headers: bool,
    should_capture_body: bool,
    logfire_instance: Logfire,
) -> AsyncResponseHook | None:
    if not (should_capture_headers or should_capture_body or hook):
        return None

    async def new_hook(span: Span, request: RequestInfo, response: ResponseInfo) -> None:
        with handle_internal_errors:
            request, response = capture_response(
                span,
                request,
                response,
                logfire_instance,
                should_capture_headers,
                should_capture_body,
                is_async=True,
            )
            await run_async_hook(hook, span, request, response)

    return new_hook


def capture_request(
    span: Span,
    request: RequestInfo,
    should_capture_headers: bool,
    should_capture_body: bool,
) -> LogfireHttpxRequestInfo:
    request = LogfireHttpxRequestInfo(*request)
    request.span = span

    if should_capture_headers:
        request.capture_headers()
    if should_capture_body:
        request.capture_body()

    return request


def capture_response(
    span: Span,
    request: RequestInfo,
    response: ResponseInfo,
    logfire_instance: Logfire,
    capture_headers: bool,
    capture_body: bool,
    *,
    is_async: bool,
) -> tuple[LogfireHttpxRequestInfo, LogfireHttpxResponseInfo]:
    request = LogfireHttpxRequestInfo(*request)
    request.span = span

    response = LogfireHttpxResponseInfo(*response)
    response.span = span
    response.logfire_instance = logfire_instance
    response.is_async = is_async

    if capture_headers:
        response.capture_headers()
    if capture_body:
        response.capture_body_if_text()

    return request, response


async def run_async_hook(hook: Callable[P, Any] | None, *args: P.args, **kwargs: P.kwargs) -> None:
    if hook:
        result = hook(*args, **kwargs)
        while inspect.isawaitable(result):
            result = await result


def run_hook(hook: Callable[P, Any] | None, *args: P.args, **kwargs: P.kwargs) -> None:
    if hook:
        hook(*args, **kwargs)


def capture_request_or_response_headers(
    span: Span, headers: httpx.Headers, request_or_response: Literal['request', 'response']
) -> None:
    span.set_attributes(
        {
            f'http.{request_or_response}.header.{header_name}': headers.get_list(header_name)
            for header_name in headers.keys()
        }
    )


CODES_FOR_METHODS_WITH_DATA_PARAM = [
    inspect.unwrap(method).__code__
    for method in [
        httpx.Client.request,
        httpx.Client.stream,
        httpx.AsyncClient.request,
        httpx.AsyncClient.stream,
    ]
]


@lru_cache
def content_type_header_from_string(content_type: str) -> ContentTypeHeader:
    return EmailPolicy.header_factory('content-type', content_type)


def content_type_subtypes(subtype: str) -> set[str]:
    if subtype.startswith('x-'):
        subtype = subtype[2:]
    return set(subtype.split('+'))


@lru_cache
def is_json_type(content_type: str) -> bool:
    header = content_type_header_from_string(content_type)
    return header.maintype == 'application' and 'json' in content_type_subtypes(header.subtype)

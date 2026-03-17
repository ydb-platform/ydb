# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# pylint: disable=too-many-lines
"""
Usage
-----

Instrumenting all clients
*************************

When using the instrumentor, all clients will automatically trace requests.

.. code-block:: python

    import httpx
    import asyncio
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    url = "https://example.com"
    HTTPXClientInstrumentor().instrument()

    with httpx.Client() as client:
        response = client.get(url)

    async def get(url):
        async with httpx.AsyncClient() as client:
            response = await client.get(url)

    asyncio.run(get(url))

Instrumenting single clients
****************************

If you only want to instrument requests for specific client instances, you can
use the `instrument_client` method.


.. code-block:: python

    import httpx
    import asyncio
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    url = "https://example.com"

    with httpx.Client() as client:
        HTTPXClientInstrumentor.instrument_client(client)
        response = client.get(url)

    async def get(url):
        async with httpx.AsyncClient() as client:
            HTTPXClientInstrumentor.instrument_client(client)
            response = await client.get(url)

    asyncio.run(get(url))

Uninstrument
************

If you need to uninstrument clients, there are two options available.

.. code-block:: python

    import httpx
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    HTTPXClientInstrumentor().instrument()
    client = httpx.Client()

    # Uninstrument a specific client
    HTTPXClientInstrumentor.uninstrument_client(client)

    # Uninstrument all clients
    HTTPXClientInstrumentor().uninstrument()


Using transports directly
*************************

If you don't want to use the instrumentor class, you can use the transport classes directly.


.. code-block:: python

    import httpx
    import asyncio
    from opentelemetry.instrumentation.httpx import (
        AsyncOpenTelemetryTransport,
        SyncOpenTelemetryTransport,
    )

    url = "https://example.com"
    transport = httpx.HTTPTransport()
    telemetry_transport = SyncOpenTelemetryTransport(transport)

    with httpx.Client(transport=telemetry_transport) as client:
        response = client.get(url)

    transport = httpx.AsyncHTTPTransport()
    telemetry_transport = AsyncOpenTelemetryTransport(transport)

    async def get(url):
        async with httpx.AsyncClient(transport=telemetry_transport) as client:
            response = await client.get(url)

    asyncio.run(get(url))

Request and response hooks
***************************

The instrumentation supports specifying request and response hooks. These are functions that get called back by the instrumentation right after a span is created for a request
and right before the span is finished while processing a response.

.. note::

    The request hook receives the raw arguments provided to the transport layer. The response hook receives the raw return values from the transport layer.

The hooks can be configured as follows:


.. code-block:: python

    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    def request_hook(span, request):
        # method, url, headers, stream, extensions = request
        pass

    def response_hook(span, request, response):
        # method, url, headers, stream, extensions = request
        # status_code, headers, stream, extensions = response
        pass

    async def async_request_hook(span, request):
        # method, url, headers, stream, extensions = request
        pass

    async def async_response_hook(span, request, response):
        # method, url, headers, stream, extensions = request
        # status_code, headers, stream, extensions = response
        pass

    HTTPXClientInstrumentor().instrument(
        request_hook=request_hook,
        response_hook=response_hook,
        async_request_hook=async_request_hook,
        async_response_hook=async_response_hook
    )


Or if you are using the transport classes directly:


.. code-block:: python

    import httpx
    from opentelemetry.instrumentation.httpx import SyncOpenTelemetryTransport, AsyncOpenTelemetryTransport

    def request_hook(span, request):
        # method, url, headers, stream, extensions = request
        pass

    def response_hook(span, request, response):
        # method, url, headers, stream, extensions = request
        # status_code, headers, stream, extensions = response
        pass

    async def async_request_hook(span, request):
        # method, url, headers, stream, extensions = request
        pass

    async def async_response_hook(span, request, response):
        # method, url, headers, stream, extensions = request
        # status_code, headers, stream, extensions = response
        pass

    transport = httpx.HTTPTransport()
    telemetry_transport = SyncOpenTelemetryTransport(
        transport,
        request_hook=request_hook,
        response_hook=response_hook
    )

    async_transport = httpx.AsyncHTTPTransport()
    async_telemetry_transport = AsyncOpenTelemetryTransport(
        async_transport,
        request_hook=async_request_hook,
        response_hook=async_response_hook
    )


Configuration
-------------

Exclude lists
*************
To exclude certain URLs from tracking, set the environment variable ``OTEL_PYTHON_HTTPX_EXCLUDED_URLS``
(or ``OTEL_PYTHON_EXCLUDED_URLS`` to cover all instrumentations) to a string of comma delimited regexes that match the
URLs.

For example,

::

    export OTEL_PYTHON_HTTPX_EXCLUDED_URLS="client/.*/info,healthcheck"

will exclude requests such as ``https://site/client/123/info`` and ``https://site/xyz/healthcheck``.

API
---
"""

from __future__ import annotations

import logging
import typing
from functools import partial
from inspect import iscoroutinefunction
from timeit import default_timer
from types import TracebackType

import httpx
from wrapt import wrap_function_wrapper

from opentelemetry.instrumentation._semconv import (
    HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
    HTTP_DURATION_HISTOGRAM_BUCKETS_OLD,
    _client_duration_attrs_new,
    _client_duration_attrs_old,
    _filter_semconv_duration_attrs,
    _get_schema_url,
    _OpenTelemetrySemanticConventionStability,
    _OpenTelemetryStabilitySignalType,
    _report_new,
    _report_old,
    _set_http_host_client,
    _set_http_method,
    _set_http_net_peer_name_client,
    _set_http_network_protocol_version,
    _set_http_peer_port_client,
    _set_http_scheme,
    _set_http_status_code,
    _set_http_url,
    _set_status,
    _StabilityMode,
)
from opentelemetry.instrumentation.httpx.package import _instruments
from opentelemetry.instrumentation.httpx.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import (
    http_status_to_status_code,
    is_http_instrumentation_enabled,
    unwrap,
)
from opentelemetry.metrics import Histogram, MeterProvider, get_meter
from opentelemetry.propagate import inject
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.network_attributes import (
    NETWORK_PEER_ADDRESS,
    NETWORK_PEER_PORT,
)
from opentelemetry.semconv.metrics import MetricInstruments
from opentelemetry.semconv.metrics.http_metrics import (
    HTTP_CLIENT_REQUEST_DURATION,
)
from opentelemetry.trace import SpanKind, Tracer, TracerProvider, get_tracer
from opentelemetry.trace.span import Span
from opentelemetry.trace.status import StatusCode
from opentelemetry.util.http import (
    ExcludeList,
    get_excluded_urls,
    redact_url,
    sanitize_method,
)

_logger = logging.getLogger(__name__)

RequestHook = typing.Callable[[Span, "RequestInfo"], None]
ResponseHook = typing.Callable[[Span, "RequestInfo", "ResponseInfo"], None]
AsyncRequestHook = typing.Callable[
    [Span, "RequestInfo"], typing.Awaitable[typing.Any]
]
AsyncResponseHook = typing.Callable[
    [Span, "RequestInfo", "ResponseInfo"], typing.Awaitable[typing.Any]
]


class RequestInfo(typing.NamedTuple):
    method: bytes
    url: httpx.URL
    headers: httpx.Headers | None
    stream: httpx.SyncByteStream | httpx.AsyncByteStream | None
    extensions: dict[str, typing.Any] | None


class ResponseInfo(typing.NamedTuple):
    status_code: int
    headers: httpx.Headers | None
    stream: httpx.SyncByteStream | httpx.AsyncByteStream
    extensions: dict[str, typing.Any] | None


def _get_default_span_name(method: str) -> str:
    method = sanitize_method(method.strip())
    if method == "_OTHER":
        method = "HTTP"

    return method


def _prepare_headers(headers: httpx.Headers | None) -> httpx.Headers:
    return httpx.Headers(headers)


def _extract_parameters(
    args: tuple[typing.Any, ...], kwargs: dict[str, typing.Any]
) -> tuple[
    bytes,
    httpx.URL | tuple[bytes, bytes, int | None, bytes],
    httpx.Headers | None,
    httpx.SyncByteStream | httpx.AsyncByteStream | None,
    dict[str, typing.Any],
]:
    if isinstance(args[0], httpx.Request):
        # In httpx >= 0.20.0, handle_request receives a Request object
        request: httpx.Request = args[0]
        method = request.method.encode()
        url = httpx.URL(str(request.url))
        headers = request.headers
        stream = request.stream
        extensions = request.extensions
    else:
        # In httpx < 0.20.0, handle_request receives the parameters separately
        method = args[0]
        url = args[1]
        headers = kwargs.get("headers", args[2] if len(args) > 2 else None)
        stream = kwargs.get("stream", args[3] if len(args) > 3 else None)
        extensions = kwargs.get(
            "extensions", args[4] if len(args) > 4 else None
        )

    return method, url, headers, stream, extensions


def _normalize_url(
    url: httpx.URL | tuple[bytes, bytes, int | None, bytes],
) -> str:
    if isinstance(url, tuple):
        scheme, host, port, path = [
            part.decode() if isinstance(part, bytes) else part for part in url
        ]
        return (
            f"{scheme}://{host}:{port}{path}"
            if port
            else f"{scheme}://{host}{path}"
        )

    return str(url)


def _inject_propagation_headers(headers, args, kwargs):
    _headers = _prepare_headers(headers)
    inject(_headers)
    if isinstance(args[0], httpx.Request):
        request: httpx.Request = args[0]
        request.headers = _headers
    else:
        kwargs["headers"] = _headers.raw


def _extract_response(
    response: httpx.Response
    | tuple[int, httpx.Headers, httpx.SyncByteStream, dict[str, typing.Any]],
) -> tuple[
    int,
    httpx.Headers,
    httpx.SyncByteStream | httpx.AsyncByteStream,
    dict[str, typing.Any],
    str,
]:
    if isinstance(response, httpx.Response):
        status_code = response.status_code
        headers = response.headers
        stream = response.stream
        extensions = response.extensions
        http_version = response.http_version
    else:
        status_code, headers, stream, extensions = response
        http_version = extensions.get("http_version", b"HTTP/1.1").decode(
            "ascii", errors="ignore"
        )

    return (status_code, headers, stream, extensions, http_version)


def _apply_request_client_attributes_to_span(
    span_attributes: dict[str, typing.Any],
    metric_attributes: dict[str, typing.Any],
    url: str | httpx.URL,
    method_original: str,
    semconv: _StabilityMode,
):
    url = httpx.URL(url)
    # http semconv transition: http.method -> http.request.method
    _set_http_method(
        span_attributes,
        method_original,
        sanitize_method(method_original),
        semconv,
    )

    # http semconv transition: http.url -> url.full
    _set_http_url(span_attributes, redact_url(str(url)), semconv)

    # Set HTTP method in metric labels
    _set_http_method(
        metric_attributes,
        method_original,
        sanitize_method(method_original),
        semconv,
    )

    if _report_old(semconv):
        # TODO: Support opt-in for url.scheme in new semconv
        _set_http_scheme(metric_attributes, url.scheme, semconv)

    if _report_new(semconv):
        if url.host:
            # http semconv transition: http.host -> server.address
            _set_http_host_client(span_attributes, url.host, semconv)
            # Add metric labels
            _set_http_host_client(metric_attributes, url.host, semconv)
            _set_http_net_peer_name_client(
                metric_attributes, url.host, semconv
            )
            # http semconv transition: net.sock.peer.addr -> network.peer.address
            span_attributes[NETWORK_PEER_ADDRESS] = url.host
        if url.port:
            # http semconv transition: net.sock.peer.port -> network.peer.port
            _set_http_peer_port_client(span_attributes, url.port, semconv)
            span_attributes[NETWORK_PEER_PORT] = url.port
            # Add metric labels
            _set_http_peer_port_client(metric_attributes, url.port, semconv)


def _apply_response_client_attributes_to_span(
    span: Span,
    status_code: int,
    http_version: str,
    semconv: _StabilityMode,
):
    # http semconv transition: http.status_code -> http.response.status_code
    # TODO: use _set_status when it's stable for http clients
    span_attributes = {}
    _set_http_status_code(
        span_attributes,
        status_code,
        semconv,
    )
    http_status_code = http_status_to_status_code(status_code)
    span.set_status(http_status_code)

    if http_status_code == StatusCode.ERROR and _report_new(semconv):
        # http semconv transition: new error.type
        span_attributes[ERROR_TYPE] = str(status_code)

    if http_version and _report_new(semconv):
        # http semconv transition: http.flavor -> network.protocol.version
        _set_http_network_protocol_version(
            span_attributes,
            http_version.replace("HTTP/", ""),
            semconv,
        )

    for key, val in span_attributes.items():
        span.set_attribute(key, val)


def _apply_response_client_attributes_to_metrics(
    span: Span | None,
    metric_attributes: dict[str, typing.Any],
    status_code: int,
    http_version: str,
    semconv: _StabilityMode,
) -> None:
    """Apply response attributes to metric attributes."""
    # Set HTTP status code in metric attributes
    _set_status(
        span,
        metric_attributes,
        status_code,
        str(status_code),
        server_span=False,
        sem_conv_opt_in_mode=semconv,
    )

    if http_version and _report_new(semconv):
        _set_http_network_protocol_version(
            metric_attributes,
            http_version.replace("HTTP/", ""),
            semconv,
        )


class SyncOpenTelemetryTransport(httpx.BaseTransport):
    """Sync transport class that will trace all requests made with a client.

    Args:
        transport: SyncHTTPTransport instance to wrap
        tracer_provider: Tracer provider to use
        meter_provider: Meter provider to use
        request_hook: A hook that receives the span and request that is called
            right after the span is created
        response_hook: A hook that receives the span, request, and response
            that is called right before the span ends
    """

    def __init__(
        self,
        transport: httpx.BaseTransport,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        request_hook: RequestHook | None = None,
        response_hook: ResponseHook | None = None,
    ):
        _OpenTelemetrySemanticConventionStability._initialize()
        self._sem_conv_opt_in_mode = _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
            _OpenTelemetryStabilitySignalType.HTTP,
        )
        schema_url = _get_schema_url(self._sem_conv_opt_in_mode)

        self._transport = transport
        self._tracer = get_tracer(
            __name__,
            instrumenting_library_version=__version__,
            tracer_provider=tracer_provider,
            schema_url=schema_url,
        )
        meter = get_meter(
            __name__,
            __version__,
            meter_provider,
            schema_url,
        )

        self._duration_histogram_old = None
        if _report_old(self._sem_conv_opt_in_mode):
            self._duration_histogram_old = meter.create_histogram(
                name=MetricInstruments.HTTP_CLIENT_DURATION,
                unit="ms",
                description="measures the duration of the outbound HTTP request",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_OLD,
            )
        self._duration_histogram_new = None
        if _report_new(self._sem_conv_opt_in_mode):
            self._duration_histogram_new = meter.create_histogram(
                name=HTTP_CLIENT_REQUEST_DURATION,
                unit="s",
                description="Duration of HTTP client requests.",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
            )
        self._request_hook = request_hook
        self._response_hook = response_hook
        self._excluded_urls = get_excluded_urls("HTTPX")

    def __enter__(self) -> SyncOpenTelemetryTransport:
        self._transport.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self._transport.__exit__(exc_type, exc_value, traceback)

    # pylint: disable=R0914
    def handle_request(
        self,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> (
        tuple[int, httpx.Headers, httpx.SyncByteStream, dict[str, typing.Any]]
        | httpx.Response
    ):
        """Add request info to span."""
        if not is_http_instrumentation_enabled():
            return self._transport.handle_request(*args, **kwargs)

        method, url, headers, stream, extensions = _extract_parameters(
            args, kwargs
        )

        if self._excluded_urls and self._excluded_urls.url_disabled(
            _normalize_url(url)
        ):
            return self._transport.handle_request(*args, **kwargs)

        method_original = method.decode()
        span_name = _get_default_span_name(method_original)
        span_attributes = {}
        metric_attributes = {}
        # apply http client response attributes according to semconv
        _apply_request_client_attributes_to_span(
            span_attributes,
            metric_attributes,
            url,
            method_original,
            self._sem_conv_opt_in_mode,
        )

        request_info = RequestInfo(method, url, headers, stream, extensions)

        with self._tracer.start_as_current_span(
            span_name, kind=SpanKind.CLIENT, attributes=span_attributes
        ) as span:
            exception = None
            if callable(self._request_hook):
                self._request_hook(span, request_info)

            _inject_propagation_headers(headers, args, kwargs)

            start_time = default_timer()

            try:
                response = self._transport.handle_request(*args, **kwargs)
            except Exception as exc:  # pylint: disable=W0703
                exception = exc
                response = getattr(exc, "response", None)
            finally:
                elapsed_time = max(default_timer() - start_time, 0)

            if isinstance(response, (httpx.Response, tuple)):
                status_code, headers, stream, extensions, http_version = (
                    _extract_response(response)
                )

                # Always apply response attributes to metrics
                _apply_response_client_attributes_to_metrics(
                    span,
                    metric_attributes,
                    status_code,
                    http_version,
                    self._sem_conv_opt_in_mode,
                )

                if span.is_recording():
                    # apply http client response attributes according to semconv
                    _apply_response_client_attributes_to_span(
                        span,
                        status_code,
                        http_version,
                        self._sem_conv_opt_in_mode,
                    )
                if callable(self._response_hook):
                    self._response_hook(
                        span,
                        request_info,
                        ResponseInfo(status_code, headers, stream, extensions),
                    )

            if exception:
                if span.is_recording() and _report_new(
                    self._sem_conv_opt_in_mode
                ):
                    span.set_attribute(
                        ERROR_TYPE, type(exception).__qualname__
                    )
                    metric_attributes[ERROR_TYPE] = type(
                        exception
                    ).__qualname__
                raise exception.with_traceback(exception.__traceback__)

            if self._duration_histogram_old is not None:
                duration_attrs_old = _filter_semconv_duration_attrs(
                    metric_attributes,
                    _client_duration_attrs_old,
                    _client_duration_attrs_new,
                    _StabilityMode.DEFAULT,
                )
                self._duration_histogram_old.record(
                    max(round(elapsed_time * 1000), 0),
                    attributes=duration_attrs_old,
                )
            if self._duration_histogram_new is not None:
                duration_attrs_new = _filter_semconv_duration_attrs(
                    metric_attributes,
                    _client_duration_attrs_old,
                    _client_duration_attrs_new,
                    _StabilityMode.HTTP,
                )
                self._duration_histogram_new.record(
                    elapsed_time, attributes=duration_attrs_new
                )

        return response

    def close(self) -> None:
        self._transport.close()


class AsyncOpenTelemetryTransport(httpx.AsyncBaseTransport):
    """Async transport class that will trace all requests made with a client.

    Args:
        transport: AsyncHTTPTransport instance to wrap
        tracer_provider: Tracer provider to use
        meter_provider: Meter provider to use
        request_hook: A hook that receives the span and request that is called
            right after the span is created
        response_hook: A hook that receives the span, request, and response
            that is called right before the span ends
    """

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        request_hook: AsyncRequestHook | None = None,
        response_hook: AsyncResponseHook | None = None,
    ):
        _OpenTelemetrySemanticConventionStability._initialize()
        self._sem_conv_opt_in_mode = _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
            _OpenTelemetryStabilitySignalType.HTTP,
        )
        schema_url = _get_schema_url(self._sem_conv_opt_in_mode)

        self._transport = transport
        self._tracer = get_tracer(
            __name__,
            instrumenting_library_version=__version__,
            tracer_provider=tracer_provider,
            schema_url=schema_url,
        )

        meter = get_meter(
            __name__,
            __version__,
            meter_provider,
            schema_url,
        )

        self._duration_histogram_old = None
        if _report_old(self._sem_conv_opt_in_mode):
            self._duration_histogram_old = meter.create_histogram(
                name=MetricInstruments.HTTP_CLIENT_DURATION,
                unit="ms",
                description="measures the duration of the outbound HTTP request",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_OLD,
            )
        self._duration_histogram_new = None
        if _report_new(self._sem_conv_opt_in_mode):
            self._duration_histogram_new = meter.create_histogram(
                name=HTTP_CLIENT_REQUEST_DURATION,
                unit="s",
                description="Duration of HTTP client requests.",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
            )

        self._request_hook = request_hook
        self._response_hook = response_hook
        self._excluded_urls = get_excluded_urls("HTTPX")

    async def __aenter__(self) -> "AsyncOpenTelemetryTransport":
        await self._transport.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self._transport.__aexit__(exc_type, exc_value, traceback)

    # pylint: disable=R0914
    async def handle_async_request(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> (
        tuple[int, httpx.Headers, httpx.AsyncByteStream, dict[str, typing.Any]]
        | httpx.Response
    ):
        """Add request info to span."""
        if not is_http_instrumentation_enabled():
            return await self._transport.handle_async_request(*args, **kwargs)

        method, url, headers, stream, extensions = _extract_parameters(
            args, kwargs
        )

        if self._excluded_urls and self._excluded_urls.url_disabled(
            _normalize_url(url)
        ):
            return await self._transport.handle_async_request(*args, **kwargs)

        method_original = method.decode()
        span_name = _get_default_span_name(method_original)
        span_attributes = {}
        metric_attributes = {}
        # apply http client response attributes according to semconv
        _apply_request_client_attributes_to_span(
            span_attributes,
            metric_attributes,
            url,
            method_original,
            self._sem_conv_opt_in_mode,
        )

        request_info = RequestInfo(method, url, headers, stream, extensions)

        with self._tracer.start_as_current_span(
            span_name, kind=SpanKind.CLIENT, attributes=span_attributes
        ) as span:
            exception = None
            if callable(self._request_hook):
                await self._request_hook(span, request_info)

            _inject_propagation_headers(headers, args, kwargs)

            start_time = default_timer()

            try:
                response = await self._transport.handle_async_request(
                    *args, **kwargs
                )
            except Exception as exc:  # pylint: disable=W0703
                exception = exc
                response = getattr(exc, "response", None)
            finally:
                elapsed_time = max(default_timer() - start_time, 0)

            if isinstance(response, (httpx.Response, tuple)):
                status_code, headers, stream, extensions, http_version = (
                    _extract_response(response)
                )

                # Always apply response attributes to metrics
                _apply_response_client_attributes_to_metrics(
                    span,
                    metric_attributes,
                    status_code,
                    http_version,
                    self._sem_conv_opt_in_mode,
                )

                if span.is_recording():
                    # apply http client response attributes according to semconv
                    _apply_response_client_attributes_to_span(
                        span,
                        status_code,
                        http_version,
                        self._sem_conv_opt_in_mode,
                    )

                if callable(self._response_hook):
                    await self._response_hook(
                        span,
                        request_info,
                        ResponseInfo(status_code, headers, stream, extensions),
                    )

            if exception:
                if span.is_recording() and _report_new(
                    self._sem_conv_opt_in_mode
                ):
                    span.set_attribute(
                        ERROR_TYPE, type(exception).__qualname__
                    )
                    metric_attributes[ERROR_TYPE] = type(
                        exception
                    ).__qualname__

                raise exception.with_traceback(exception.__traceback__)

            if self._duration_histogram_old is not None:
                duration_attrs_old = _filter_semconv_duration_attrs(
                    metric_attributes,
                    _client_duration_attrs_old,
                    _client_duration_attrs_new,
                    _StabilityMode.DEFAULT,
                )
                self._duration_histogram_old.record(
                    max(round(elapsed_time * 1000), 0),
                    attributes=duration_attrs_old,
                )
            if self._duration_histogram_new is not None:
                duration_attrs_new = _filter_semconv_duration_attrs(
                    metric_attributes,
                    _client_duration_attrs_old,
                    _client_duration_attrs_new,
                    _StabilityMode.HTTP,
                )
                self._duration_histogram_new.record(
                    elapsed_time, attributes=duration_attrs_new
                )

        return response

    async def aclose(self) -> None:
        await self._transport.aclose()


class HTTPXClientInstrumentor(BaseInstrumentor):
    # pylint: disable=protected-access,attribute-defined-outside-init
    """An instrumentor for httpx Client and AsyncClient

    See `BaseInstrumentor`
    """

    def instrumentation_dependencies(self) -> typing.Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: typing.Any):
        """Instruments httpx Client and AsyncClient

        Args:
            **kwargs: Optional arguments
                ``tracer_provider``: a TracerProvider, defaults to global
                ``meter_provider``: a MeterProvider, defaults to global
                ``request_hook``: A ``httpx.Client`` hook that receives the span and request
                    that is called right after the span is created
                ``response_hook``: A ``httpx.Client`` hook that receives the span, request,
                    and response that is called right before the span ends
                ``async_request_hook``: Async ``request_hook`` for ``httpx.AsyncClient``
                ``async_response_hook``: Async``response_hook`` for ``httpx.AsyncClient``
        """
        tracer_provider = kwargs.get("tracer_provider")
        meter_provider = kwargs.get("meter_provider")
        request_hook = kwargs.get("request_hook")
        response_hook = kwargs.get("response_hook")
        async_request_hook = kwargs.get("async_request_hook")
        async_request_hook = (
            async_request_hook
            if iscoroutinefunction(async_request_hook)
            else None
        )
        async_response_hook = kwargs.get("async_response_hook")
        async_response_hook = (
            async_response_hook
            if iscoroutinefunction(async_response_hook)
            else None
        )
        excluded_urls = get_excluded_urls("HTTPX")

        _OpenTelemetrySemanticConventionStability._initialize()
        sem_conv_opt_in_mode = _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
            _OpenTelemetryStabilitySignalType.HTTP,
        )
        schema_url = _get_schema_url(sem_conv_opt_in_mode)

        tracer = get_tracer(
            __name__,
            instrumenting_library_version=__version__,
            tracer_provider=tracer_provider,
            schema_url=schema_url,
        )

        meter = get_meter(
            __name__,
            __version__,
            meter_provider,
            schema_url,
        )
        duration_histogram_old = None
        if _report_old(sem_conv_opt_in_mode):
            duration_histogram_old = meter.create_histogram(
                name=MetricInstruments.HTTP_CLIENT_DURATION,
                unit="ms",
                description="measures the duration of the outbound HTTP request",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_OLD,
            )
        duration_histogram_new = None
        if _report_new(sem_conv_opt_in_mode):
            duration_histogram_new = meter.create_histogram(
                name=HTTP_CLIENT_REQUEST_DURATION,
                unit="s",
                description="Duration of HTTP client requests.",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
            )

        wrap_function_wrapper(
            "httpx",
            "HTTPTransport.handle_request",
            partial(
                self._handle_request_wrapper,
                tracer=tracer,
                duration_histogram_old=duration_histogram_old,
                duration_histogram_new=duration_histogram_new,
                sem_conv_opt_in_mode=sem_conv_opt_in_mode,
                request_hook=request_hook,
                response_hook=response_hook,
                excluded_urls=excluded_urls,
            ),
        )
        wrap_function_wrapper(
            "httpx",
            "AsyncHTTPTransport.handle_async_request",
            partial(
                self._handle_async_request_wrapper,
                tracer=tracer,
                duration_histogram_old=duration_histogram_old,
                duration_histogram_new=duration_histogram_new,
                sem_conv_opt_in_mode=sem_conv_opt_in_mode,
                async_request_hook=async_request_hook,
                async_response_hook=async_response_hook,
                excluded_urls=excluded_urls,
            ),
        )

    def _uninstrument(self, **kwargs: typing.Any):
        unwrap(httpx.HTTPTransport, "handle_request")
        unwrap(httpx.AsyncHTTPTransport, "handle_async_request")

    @staticmethod
    def _handle_request_wrapper(  # pylint: disable=too-many-locals
        wrapped: typing.Callable[..., typing.Any],
        instance: httpx.HTTPTransport,
        args: tuple[typing.Any, ...],
        kwargs: dict[str, typing.Any],
        tracer: Tracer,
        duration_histogram_old: Histogram,
        duration_histogram_new: Histogram,
        sem_conv_opt_in_mode: _StabilityMode,
        request_hook: RequestHook,
        response_hook: ResponseHook,
        excluded_urls: ExcludeList | None,
    ):
        if not is_http_instrumentation_enabled():
            return wrapped(*args, **kwargs)

        method, url, headers, stream, extensions = _extract_parameters(
            args, kwargs
        )

        if excluded_urls and excluded_urls.url_disabled(_normalize_url(url)):
            return wrapped(*args, **kwargs)

        method_original = method.decode()
        span_name = _get_default_span_name(method_original)
        span_attributes = {}
        metric_attributes = {}
        # apply http client response attributes according to semconv
        _apply_request_client_attributes_to_span(
            span_attributes,
            metric_attributes,
            url,
            method_original,
            sem_conv_opt_in_mode,
        )

        request_info = RequestInfo(method, url, headers, stream, extensions)

        with tracer.start_as_current_span(
            span_name, kind=SpanKind.CLIENT, attributes=span_attributes
        ) as span:
            exception = None
            if callable(request_hook):
                request_hook(span, request_info)

            _inject_propagation_headers(headers, args, kwargs)

            start_time = default_timer()

            try:
                response = wrapped(*args, **kwargs)
            except Exception as exc:  # pylint: disable=W0703
                exception = exc
                response = getattr(exc, "response", None)
            finally:
                elapsed_time = max(default_timer() - start_time, 0)

            if isinstance(response, (httpx.Response, tuple)):
                status_code, headers, stream, extensions, http_version = (
                    _extract_response(response)
                )

                # Always apply response attributes to metrics
                _apply_response_client_attributes_to_metrics(
                    span,
                    metric_attributes,
                    status_code,
                    http_version,
                    sem_conv_opt_in_mode,
                )

                if span.is_recording():
                    # apply http client response attributes according to semconv
                    _apply_response_client_attributes_to_span(
                        span,
                        status_code,
                        http_version,
                        sem_conv_opt_in_mode,
                    )
                if callable(response_hook):
                    response_hook(
                        span,
                        request_info,
                        ResponseInfo(status_code, headers, stream, extensions),
                    )

            if exception:
                if span.is_recording() and _report_new(sem_conv_opt_in_mode):
                    span.set_attribute(
                        ERROR_TYPE, type(exception).__qualname__
                    )
                    metric_attributes[ERROR_TYPE] = type(
                        exception
                    ).__qualname__
                raise exception.with_traceback(exception.__traceback__)

            if duration_histogram_old is not None:
                duration_attrs_old = _filter_semconv_duration_attrs(
                    metric_attributes,
                    _client_duration_attrs_old,
                    _client_duration_attrs_new,
                    _StabilityMode.DEFAULT,
                )
                duration_histogram_old.record(
                    max(round(elapsed_time * 1000), 0),
                    attributes=duration_attrs_old,
                )
            if duration_histogram_new is not None:
                duration_attrs_new = _filter_semconv_duration_attrs(
                    metric_attributes,
                    _client_duration_attrs_old,
                    _client_duration_attrs_new,
                    _StabilityMode.HTTP,
                )
                duration_histogram_new.record(
                    elapsed_time, attributes=duration_attrs_new
                )

        return response

    @staticmethod
    async def _handle_async_request_wrapper(  # pylint: disable=too-many-locals
        wrapped: typing.Callable[..., typing.Awaitable[typing.Any]],
        instance: httpx.AsyncHTTPTransport,
        args: tuple[typing.Any, ...],
        kwargs: dict[str, typing.Any],
        tracer: Tracer,
        duration_histogram_old: Histogram,
        duration_histogram_new: Histogram,
        sem_conv_opt_in_mode: _StabilityMode,
        async_request_hook: AsyncRequestHook,
        async_response_hook: AsyncResponseHook,
        excluded_urls: ExcludeList | None,
    ):
        if not is_http_instrumentation_enabled():
            return await wrapped(*args, **kwargs)

        method, url, headers, stream, extensions = _extract_parameters(
            args, kwargs
        )

        if excluded_urls and excluded_urls.url_disabled(_normalize_url(url)):
            return await wrapped(*args, **kwargs)

        method_original = method.decode()
        span_name = _get_default_span_name(method_original)
        span_attributes = {}
        metric_attributes = {}
        # apply http client response attributes according to semconv
        _apply_request_client_attributes_to_span(
            span_attributes,
            metric_attributes,
            url,
            method_original,
            sem_conv_opt_in_mode,
        )

        request_info = RequestInfo(method, url, headers, stream, extensions)

        with tracer.start_as_current_span(
            span_name, kind=SpanKind.CLIENT, attributes=span_attributes
        ) as span:
            exception = None
            if callable(async_request_hook):
                await async_request_hook(span, request_info)

            _inject_propagation_headers(headers, args, kwargs)

            start_time = default_timer()

            try:
                response = await wrapped(*args, **kwargs)
            except Exception as exc:  # pylint: disable=W0703
                exception = exc
                response = getattr(exc, "response", None)
            finally:
                elapsed_time = max(default_timer() - start_time, 0)

            if isinstance(response, (httpx.Response, tuple)):
                status_code, headers, stream, extensions, http_version = (
                    _extract_response(response)
                )

                # Always apply response attributes to metrics
                _apply_response_client_attributes_to_metrics(
                    span,
                    metric_attributes,
                    status_code,
                    http_version,
                    sem_conv_opt_in_mode,
                )

                if span.is_recording():
                    # apply http client response attributes according to semconv
                    _apply_response_client_attributes_to_span(
                        span,
                        status_code,
                        http_version,
                        sem_conv_opt_in_mode,
                    )

                if callable(async_response_hook):
                    await async_response_hook(
                        span,
                        request_info,
                        ResponseInfo(status_code, headers, stream, extensions),
                    )

            if exception:
                if span.is_recording() and _report_new(sem_conv_opt_in_mode):
                    span.set_attribute(
                        ERROR_TYPE, type(exception).__qualname__
                    )
                raise exception.with_traceback(exception.__traceback__)

            if duration_histogram_old is not None:
                duration_attrs_old = _filter_semconv_duration_attrs(
                    metric_attributes,
                    _client_duration_attrs_old,
                    _client_duration_attrs_new,
                    _StabilityMode.DEFAULT,
                )
                duration_histogram_old.record(
                    max(round(elapsed_time * 1000), 0),
                    attributes=duration_attrs_old,
                )
            if duration_histogram_new is not None:
                duration_attrs_new = _filter_semconv_duration_attrs(
                    metric_attributes,
                    _client_duration_attrs_old,
                    _client_duration_attrs_new,
                    _StabilityMode.HTTP,
                )
                duration_histogram_new.record(
                    elapsed_time, attributes=duration_attrs_new
                )

        return response

    # pylint: disable=too-many-branches,too-many-locals
    @classmethod
    def instrument_client(
        cls,
        client: httpx.Client | httpx.AsyncClient,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        request_hook: RequestHook | AsyncRequestHook | None = None,
        response_hook: ResponseHook | AsyncResponseHook | None = None,
    ) -> None:
        """Instrument httpx Client or AsyncClient

        Args:
            client: The httpx Client or AsyncClient instance
            tracer_provider: A TracerProvider, defaults to global
            meter_provider: A MeterProvider, defaults to global
            request_hook: A hook that receives the span and request that is called
                right after the span is created
            response_hook: A hook that receives the span, request, and response
                that is called right before the span ends
        """

        if getattr(client, "_is_instrumented_by_opentelemetry", False):
            _logger.warning(
                "Attempting to instrument Httpx client while already instrumented"
            )
            return

        _OpenTelemetrySemanticConventionStability._initialize()
        sem_conv_opt_in_mode = _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
            _OpenTelemetryStabilitySignalType.HTTP,
        )
        schema_url = _get_schema_url(sem_conv_opt_in_mode)
        tracer = get_tracer(
            __name__,
            instrumenting_library_version=__version__,
            tracer_provider=tracer_provider,
            schema_url=schema_url,
        )
        meter = get_meter(
            __name__,
            __version__,
            meter_provider,
            schema_url,
        )
        duration_histogram_old = None
        if _report_old(sem_conv_opt_in_mode):
            duration_histogram_old = meter.create_histogram(
                name=MetricInstruments.HTTP_CLIENT_DURATION,
                unit="ms",
                description="measures the duration of the outbound HTTP request",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_OLD,
            )
        duration_histogram_new = None
        if _report_new(sem_conv_opt_in_mode):
            duration_histogram_new = meter.create_histogram(
                name=HTTP_CLIENT_REQUEST_DURATION,
                unit="s",
                description="Duration of HTTP client requests.",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
            )

        if iscoroutinefunction(request_hook):
            async_request_hook = request_hook
            request_hook = None
        else:
            # request_hook already set
            async_request_hook = None

        if iscoroutinefunction(response_hook):
            async_response_hook = response_hook
            response_hook = None
        else:
            # response_hook already set
            async_response_hook = None

        excluded_urls = get_excluded_urls("HTTPX")

        if hasattr(client._transport, "handle_request"):
            wrap_function_wrapper(
                client._transport,
                "handle_request",
                partial(
                    cls._handle_request_wrapper,
                    tracer=tracer,
                    duration_histogram_old=duration_histogram_old,
                    duration_histogram_new=duration_histogram_new,
                    sem_conv_opt_in_mode=sem_conv_opt_in_mode,
                    request_hook=request_hook,
                    response_hook=response_hook,
                    excluded_urls=excluded_urls,
                ),
            )
            for transport in client._mounts.values():
                if hasattr(transport, "handle_request"):
                    wrap_function_wrapper(
                        transport,
                        "handle_request",
                        partial(
                            cls._handle_request_wrapper,
                            tracer=tracer,
                            duration_histogram_old=duration_histogram_old,
                            duration_histogram_new=duration_histogram_new,
                            sem_conv_opt_in_mode=sem_conv_opt_in_mode,
                            request_hook=request_hook,
                            response_hook=response_hook,
                            excluded_urls=excluded_urls,
                        ),
                    )
            client._is_instrumented_by_opentelemetry = True
        if hasattr(client._transport, "handle_async_request"):
            wrap_function_wrapper(
                client._transport,
                "handle_async_request",
                partial(
                    cls._handle_async_request_wrapper,
                    tracer=tracer,
                    duration_histogram_old=duration_histogram_old,
                    duration_histogram_new=duration_histogram_new,
                    sem_conv_opt_in_mode=sem_conv_opt_in_mode,
                    async_request_hook=async_request_hook,
                    async_response_hook=async_response_hook,
                    excluded_urls=excluded_urls,
                ),
            )
            for transport in client._mounts.values():
                if hasattr(transport, "handle_async_request"):
                    wrap_function_wrapper(
                        transport,
                        "handle_async_request",
                        partial(
                            cls._handle_async_request_wrapper,
                            tracer=tracer,
                            duration_histogram_old=duration_histogram_old,
                            duration_histogram_new=duration_histogram_new,
                            sem_conv_opt_in_mode=sem_conv_opt_in_mode,
                            async_request_hook=async_request_hook,
                            async_response_hook=async_response_hook,
                            excluded_urls=excluded_urls,
                        ),
                    )
            client._is_instrumented_by_opentelemetry = True

    @staticmethod
    def uninstrument_client(client: httpx.Client | httpx.AsyncClient) -> None:
        """Disables instrumentation for the given client instance

        Args:
            client: The httpx Client or AsyncClient instance
        """
        if hasattr(client._transport, "handle_request"):
            unwrap(client._transport, "handle_request")
            for transport in client._mounts.values():
                unwrap(transport, "handle_request")
            client._is_instrumented_by_opentelemetry = False
        elif hasattr(client._transport, "handle_async_request"):
            unwrap(client._transport, "handle_async_request")
            for transport in client._mounts.values():
                unwrap(transport, "handle_async_request")
            client._is_instrumented_by_opentelemetry = False

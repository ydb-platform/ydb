# Copyright 2020, OpenTelemetry Authors
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

"""
The opentelemetry-instrumentation-aiohttp-server package allows tracing HTTP
requests made by the aiohttp server library.

Usage
-----

.. code:: python

    from aiohttp import web
    from opentelemetry.instrumentation.aiohttp_server import (
        AioHttpServerInstrumentor
    )

    AioHttpServerInstrumentor().instrument()

    async def hello(request):
        return web.Response(text="Hello, world")

    app = web.Application()
    app.add_routes([web.get('/', hello)])

    web.run_app(app)


Configuration
-------------

Exclude lists
*************
To exclude certain URLs from tracking, set the environment variable ``OTEL_PYTHON_AIOHTTP_SERVER_EXCLUDED_URLS``
(or ``OTEL_PYTHON_EXCLUDED_URLS`` to cover all instrumentations) to a string of comma delimited regexes that match the
URLs.

For example,

::

    export OTEL_PYTHON_AIOHTTP_SERVER_EXCLUDED_URLS="client/.*/info,healthcheck"

will exclude requests such as ``https://site/client/123/info`` and ``https://site/xyz/healthcheck``.

Capture HTTP request and response headers
*****************************************
You can configure the agent to capture specified HTTP headers as span attributes, according to the
`semantic conventions <https://github.com/open-telemetry/semantic-conventions/blob/main/docs/http/http-spans.md#http-server-span>`_.

Request headers
***************
To capture HTTP request headers as span attributes, set the environment variable
``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`` to a comma delimited list of HTTP header names.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST="content-type,custom_request_header"

will extract ``content-type`` and ``custom_request_header`` from the request headers and add them as span attributes.

Request header names in aiohttp are case-insensitive. So, giving the header name as ``CUStom-Header`` in the environment
variable will capture the header named ``custom-header``.

Regular expressions may also be used to match multiple headers that correspond to the given pattern.  For example:
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST="Accept.*,X-.*"

Would match all request headers that start with ``Accept`` and ``X-``.

To capture all request headers, set ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`` to ``".*"``.
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST=".*"

The name of the added span attribute will follow the format ``http.request.header.<header_name>`` where ``<header_name>``
is the normalized HTTP header name (lowercase, with ``-`` replaced by ``_``). The value of the attribute will be a
list containing the header values.

For example:
``http.request.header.custom_request_header = ["<value1>, <value2>"]``

Response headers
****************
To capture HTTP response headers as span attributes, set the environment variable
``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE`` to a comma delimited list of HTTP header names.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE="content-type,custom_response_header"

will extract ``content-type`` and ``custom_response_header`` from the response headers and add them as span attributes.

Response header names in aiohttp are case-insensitive. So, giving the header name as ``CUStom-Header`` in the environment
variable will capture the header named ``custom-header``.

Regular expressions may also be used to match multiple headers that correspond to the given pattern.  For example:
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE="Content.*,X-.*"

Would match all response headers that start with ``Content`` and ``X-``.

To capture all response headers, set ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE`` to ``".*"``.
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE=".*"

The name of the added span attribute will follow the format ``http.response.header.<header_name>`` where ``<header_name>``
is the normalized HTTP header name (lowercase, with ``-`` replaced by ``_``). The value of the attribute will be a
list containing the header values.

For example:
``http.response.header.custom_response_header = ["<value1>, <value2>"]``

Sanitizing headers
******************
In order to prevent storing sensitive data such as personally identifiable information (PII), session keys, passwords,
etc, set the environment variable ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS``
to a comma delimited list of HTTP header names to be sanitized.  Regexes may be used, and all header names will be
matched in a case-insensitive manner.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS=".*session.*,set-cookie"

will replace the value of headers such as ``session-id`` and ``set-cookie`` with ``[REDACTED]`` in the span.

Note:
    The environment variable names used to capture HTTP headers are still experimental, and thus are subject to change.

API
---
"""

from __future__ import annotations

import urllib
from timeit import default_timer

from aiohttp import web
from multidict import CIMultiDictProxy

from opentelemetry import metrics, trace
from opentelemetry.instrumentation.aiohttp_server.package import _instruments
from opentelemetry.instrumentation.aiohttp_server.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import (
    http_status_to_status_code,
    is_http_instrumentation_enabled,
)
from opentelemetry.propagate import extract
from opentelemetry.propagators.textmap import Getter
from opentelemetry.semconv._incubating.attributes.http_attributes import (
    HTTP_FLAVOR,
    HTTP_HOST,
    HTTP_METHOD,
    HTTP_ROUTE,
    HTTP_SCHEME,
    HTTP_SERVER_NAME,
    HTTP_STATUS_CODE,
    HTTP_TARGET,
    HTTP_URL,
    HTTP_USER_AGENT,
)
from opentelemetry.semconv._incubating.attributes.net_attributes import (
    NET_HOST_NAME,
    NET_HOST_PORT,
)
from opentelemetry.semconv.metrics import MetricInstruments
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util.http import (
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS,
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST,
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE,
    SanitizeValue,
    get_custom_headers,
    get_excluded_urls,
    normalise_request_header_name,
    normalise_response_header_name,
    redact_url,
)

_duration_attrs = [
    HTTP_METHOD,
    HTTP_HOST,
    HTTP_SCHEME,
    HTTP_STATUS_CODE,
    HTTP_FLAVOR,
    HTTP_SERVER_NAME,
    NET_HOST_NAME,
    NET_HOST_PORT,
    HTTP_ROUTE,
]

_active_requests_count_attrs = [
    HTTP_METHOD,
    HTTP_HOST,
    HTTP_SCHEME,
    HTTP_FLAVOR,
    HTTP_SERVER_NAME,
]

tracer = None
meter = None
_excluded_urls = None


def _parse_duration_attrs(req_attrs):
    duration_attrs = {}
    for attr_key in _duration_attrs:
        if req_attrs.get(attr_key) is not None:
            duration_attrs[attr_key] = req_attrs[attr_key]
    return duration_attrs


def _parse_active_request_count_attrs(req_attrs):
    active_requests_count_attrs = {}
    for attr_key in _active_requests_count_attrs:
        if req_attrs.get(attr_key) is not None:
            active_requests_count_attrs[attr_key] = req_attrs[attr_key]
    return active_requests_count_attrs


def get_default_span_name(request: web.Request) -> str:
    """Default implementation for get_default_span_details
    Args:
        request: the request object itself.
    Returns:
        The span name.
    """
    span_name = request.path.strip() or f"HTTP {request.method}"
    return span_name


def _get_view_func(request: web.Request) -> str:
    """Returns the name of the request handler.
    Args:
        request: the request object itself.
    Returns:
        a string containing the name of the handler function
    """
    try:
        return request.match_info.handler.__name__
    except AttributeError:
        return "unknown"


def collect_request_attributes(request: web.Request) -> dict:
    """Collects HTTP request attributes from the ASGI scope and returns a
    dictionary to be used as span creation attributes."""

    server_host, port, http_url = (
        request.url.host,
        request.url.port,
        str(request.url),
    )

    query_string = request.query_string
    if query_string and http_url:
        if isinstance(query_string, bytes):
            query_string = query_string.decode("utf8")
        http_url += "?" + urllib.parse.unquote(query_string)

    result = {
        HTTP_SCHEME: request.scheme,
        HTTP_HOST: server_host,
        NET_HOST_PORT: port,
        HTTP_ROUTE: _get_view_func(request),
        HTTP_FLAVOR: f"{request.version.major}.{request.version.minor}",
        HTTP_TARGET: request.path,
        HTTP_URL: redact_url(http_url),
    }

    http_method = request.method
    if http_method:
        result[HTTP_METHOD] = http_method

    http_host_value_list = (
        [request.host] if not isinstance(request.host, list) else request.host
    )
    if http_host_value_list:
        result[HTTP_SERVER_NAME] = ",".join(http_host_value_list)
    http_user_agent = request.headers.get("user-agent")
    if http_user_agent:
        result[HTTP_USER_AGENT] = http_user_agent

    # remove None values
    result = {k: v for k, v in result.items() if v is not None}

    return result


def collect_request_headers_attributes(
    request: web.Request,
) -> dict[str, list[str]]:
    sanitize = SanitizeValue(
        get_custom_headers(
            OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS
        )
    )

    return sanitize.sanitize_header_values(
        request.headers,
        get_custom_headers(
            OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST
        ),
        normalise_request_header_name,
    )


def collect_response_headers_attributes(
    response: web.Response,
) -> dict[str, list[str]]:
    sanitize = SanitizeValue(
        get_custom_headers(
            OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS
        )
    )

    return sanitize.sanitize_header_values(
        response.headers,
        get_custom_headers(
            OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE
        ),
        normalise_response_header_name,
    )


def set_status_code(span, status_code: int) -> None:
    """Adds HTTP response attributes to span using the status_code argument."""

    try:
        status_code = int(status_code)
    except ValueError:
        span.set_status(
            Status(
                StatusCode.ERROR,
                "Non-integer HTTP status: " + repr(status_code),
            )
        )
    else:
        span.set_attribute(HTTP_STATUS_CODE, status_code)
        span.set_status(
            Status(http_status_to_status_code(status_code, server_span=True))
        )


class AiohttpGetter(Getter):
    """Extract current trace from headers"""

    def get(self, carrier, key: str) -> list | None:
        """Getter implementation to retrieve an HTTP header value from the ASGI
        scope.

        Args:
            carrier: ASGI scope object
            key: header name in scope
        Returns:
            A list of all header values matching the key, or None if the key
            does not match any header.
        """
        headers: CIMultiDictProxy = carrier.headers
        if not headers:
            return None
        return headers.getall(key, None)

    def keys(self, carrier: dict) -> list:
        return list(carrier.keys())


getter = AiohttpGetter()


@web.middleware
async def middleware(request, handler):
    """Middleware for aiohttp implementing tracing logic"""
    if not is_http_instrumentation_enabled() or _excluded_urls.url_disabled(
        request.url.path
    ):
        return await handler(request)

    span_name = get_default_span_name(request)

    request_attrs = collect_request_attributes(request)
    duration_attrs = _parse_duration_attrs(request_attrs)
    active_requests_count_attrs = _parse_active_request_count_attrs(
        request_attrs
    )

    duration_histogram = meter.create_histogram(
        name=MetricInstruments.HTTP_SERVER_DURATION,
        unit="ms",
        description="Measures the duration of inbound HTTP requests.",
    )

    active_requests_counter = meter.create_up_down_counter(
        name=MetricInstruments.HTTP_SERVER_ACTIVE_REQUESTS,
        unit="requests",
        description="measures the number of concurrent HTTP requests those are currently in flight",
    )

    with tracer.start_as_current_span(
        span_name,
        context=extract(request, getter=getter),
        kind=trace.SpanKind.SERVER,
    ) as span:
        if span.is_recording():
            request_headers_attributes = collect_request_headers_attributes(
                request
            )
            request_attrs.update(request_headers_attributes)
            span.set_attributes(request_attrs)
        start = default_timer()
        active_requests_counter.add(1, active_requests_count_attrs)
        try:
            resp = await handler(request)
            set_status_code(span, resp.status)
            if span.is_recording():
                response_headers_attributes = (
                    collect_response_headers_attributes(resp)
                )
                span.set_attributes(response_headers_attributes)
        except web.HTTPException as ex:
            set_status_code(span, ex.status_code)
            raise
        finally:
            duration = max((default_timer() - start) * 1000, 0)
            duration_histogram.record(duration, duration_attrs)
            active_requests_counter.add(-1, active_requests_count_attrs)
        return resp


class _InstrumentedApplication(web.Application):
    """Insert tracing middleware"""

    def __init__(self, *args, **kwargs):
        middlewares = kwargs.pop("middlewares", [])
        middlewares.insert(0, middleware)
        kwargs["middlewares"] = middlewares
        super().__init__(*args, **kwargs)


class AioHttpServerInstrumentor(BaseInstrumentor):
    # pylint: disable=protected-access,attribute-defined-outside-init
    """An instrumentor for aiohttp.web.Application

    See `BaseInstrumentor`
    """

    def _instrument(self, **kwargs):
        # update global values at instrument time so we can test them
        global _excluded_urls  # pylint: disable=global-statement
        _excluded_urls = get_excluded_urls("AIOHTTP_SERVER")

        global tracer  # pylint: disable=global-statement
        tracer = trace.get_tracer(__name__)

        global meter  # pylint: disable=global-statement
        meter = metrics.get_meter(__name__, __version__)

        self._original_app = web.Application
        setattr(web, "Application", _InstrumentedApplication)

    def _uninstrument(self, **kwargs):
        setattr(web, "Application", self._original_app)

    def instrumentation_dependencies(self):
        return _instruments

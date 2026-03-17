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
The opentelemetry-instrumentation-aiohttp-client package allows tracing HTTP
requests made by the aiohttp client library.

Usage
-----
Explicitly instrumenting a single client session:

.. code:: python

    import asyncio
    import aiohttp
    from opentelemetry.instrumentation.aiohttp_client import create_trace_config
    import yarl

    def strip_query_params(url: yarl.URL) -> str:
        return str(url.with_query(None))

    async def get(url):
        async with aiohttp.ClientSession(trace_configs=[create_trace_config(
            # Remove all query params from the URL attribute on the span.
            url_filter=strip_query_params,
        )]) as session:
            async with session.get(url) as response:
                await response.text()

    asyncio.run(get("https://example.com"))

Instrumenting all client sessions:

.. code:: python

    import asyncio
    import aiohttp
    from opentelemetry.instrumentation.aiohttp_client import (
        AioHttpClientInstrumentor
    )

    # Enable instrumentation
    AioHttpClientInstrumentor().instrument()

    # Create a session and make an HTTP get request
    async def get(url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                await response.text()

    asyncio.run(get("https://example.com"))

Configuration
-------------

Request/Response hooks
**********************

Utilize request/response hooks to execute custom logic to be performed before/after performing a request.

.. code-block:: python

   def request_hook(span: Span, params: aiohttp.TraceRequestStartParams):
      if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_request_hook", "some-value")

   def response_hook(span: Span, params: typing.Union[
                aiohttp.TraceRequestEndParams,
                aiohttp.TraceRequestExceptionParams,
            ]):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_response_hook", "some-value")

   AioHttpClientInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook)

Exclude lists
*************
To exclude certain URLs from tracking, set the environment variable ``OTEL_PYTHON_AIOHTTP_CLIENT_EXCLUDED_URLS``
(or ``OTEL_PYTHON_EXCLUDED_URLS`` to cover all instrumentations) to a string of comma delimited regexes that match the
URLs.

For example,

::

    export OTEL_PYTHON_AIOHTTP_CLIENT_EXCLUDED_URLS="client/.*/info,healthcheck"

will exclude requests such as ``https://site/client/123/info`` and ``https://site/xyz/healthcheck``.

API
---
"""

import types
import typing
from timeit import default_timer
from typing import Collection
from urllib.parse import urlparse

import aiohttp
import wrapt
import yarl

from opentelemetry import context as context_api
from opentelemetry import trace
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
    _set_http_peer_port_client,
    _set_http_url,
    _set_status,
    _StabilityMode,
)
from opentelemetry.instrumentation.aiohttp_client.package import _instruments
from opentelemetry.instrumentation.aiohttp_client.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import (
    is_http_instrumentation_enabled,
    unwrap,
)
from opentelemetry.metrics import MeterProvider, get_meter
from opentelemetry.propagate import inject
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.metrics import MetricInstruments
from opentelemetry.semconv.metrics.http_metrics import (
    HTTP_CLIENT_REQUEST_DURATION,
)
from opentelemetry.trace import Span, SpanKind, TracerProvider, get_tracer
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util.http import (
    get_excluded_urls,
    redact_url,
    sanitize_method,
)

_UrlFilterT = typing.Optional[typing.Callable[[yarl.URL], str]]
_RequestHookT = typing.Optional[
    typing.Callable[[Span, aiohttp.TraceRequestStartParams], None]
]
_ResponseHookT = typing.Optional[
    typing.Callable[
        [
            Span,
            typing.Union[
                aiohttp.TraceRequestEndParams,
                aiohttp.TraceRequestExceptionParams,
            ],
        ],
        None,
    ]
]


def _get_span_name(method: str) -> str:
    method = sanitize_method(method.strip())
    if method == "_OTHER":
        method = "HTTP"
    return method


def _set_http_status_code_attribute(
    span,
    status_code,
    metric_attributes=None,
    sem_conv_opt_in_mode=_StabilityMode.DEFAULT,
):
    status_code_str = str(status_code)
    try:
        status_code = int(status_code)
    except ValueError:
        status_code = -1
    if metric_attributes is None:
        metric_attributes = {}
    # When we have durations we should set metrics only once
    # Also the decision to include status code on a histogram should
    # not be dependent on tracing decisions.
    _set_status(
        span,
        metric_attributes,
        status_code,
        status_code_str,
        server_span=False,
        sem_conv_opt_in_mode=sem_conv_opt_in_mode,
    )


# pylint: disable=too-many-locals
# pylint: disable=too-many-statements
def create_trace_config(
    url_filter: _UrlFilterT = None,
    request_hook: _RequestHookT = None,
    response_hook: _ResponseHookT = None,
    tracer_provider: TracerProvider = None,
    meter_provider: MeterProvider = None,
    sem_conv_opt_in_mode: _StabilityMode = _StabilityMode.DEFAULT,
) -> aiohttp.TraceConfig:
    """Create an aiohttp-compatible trace configuration.

    One span is created for the entire HTTP request, including initial
    TCP/TLS setup if the connection doesn't exist.

    By default the span name is set to the HTTP request method.

    Example usage:

    .. code:: python

        import aiohttp
        from opentelemetry.instrumentation.aiohttp_client import create_trace_config

        async with aiohttp.ClientSession(trace_configs=[create_trace_config()]) as session:
            async with session.get(url) as response:
                await response.text()


    :param url_filter: A callback to process the requested URL prior to adding
        it as a span attribute. This can be useful to remove sensitive data
        such as API keys or user personal information.

    :param Callable request_hook: Optional callback that can modify span name and request params.
    :param Callable response_hook: Optional callback that can modify span name and response params.
    :param tracer_provider: optional TracerProvider from which to get a Tracer
    :param meter_provider: optional Meter provider to use

    :return: An object suitable for use with :py:class:`aiohttp.ClientSession`.
    :rtype: :py:class:`aiohttp.TraceConfig`
    """
    # `aiohttp.TraceRequestStartParams` resolves to `aiohttp.tracing.TraceRequestStartParams`
    # which doesn't exist in the aiohttp intersphinx inventory.
    # Explicitly specify the type for the `request_hook` and `response_hook` param and rtype to work
    # around this issue.

    schema_url = _get_schema_url(sem_conv_opt_in_mode)

    tracer = get_tracer(
        __name__,
        __version__,
        tracer_provider,
        schema_url=schema_url,
    )

    meter = get_meter(
        __name__,
        __version__,
        meter_provider,
        schema_url,
    )

    start_time = 0

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

    excluded_urls = get_excluded_urls("AIOHTTP_CLIENT")

    def _end_trace(trace_config_ctx: types.SimpleNamespace):
        elapsed_time = max(default_timer() - trace_config_ctx.start_time, 0)
        if trace_config_ctx.token:
            context_api.detach(trace_config_ctx.token)
        trace_config_ctx.span.end()

        if trace_config_ctx.duration_histogram_old is not None:
            duration_attrs_old = _filter_semconv_duration_attrs(
                trace_config_ctx.metric_attributes,
                _client_duration_attrs_old,
                _client_duration_attrs_new,
                _StabilityMode.DEFAULT,
            )
            trace_config_ctx.duration_histogram_old.record(
                max(round(elapsed_time * 1000), 0),
                attributes=duration_attrs_old,
            )
        if trace_config_ctx.duration_histogram_new is not None:
            duration_attrs_new = _filter_semconv_duration_attrs(
                trace_config_ctx.metric_attributes,
                _client_duration_attrs_old,
                _client_duration_attrs_new,
                _StabilityMode.HTTP,
            )
            trace_config_ctx.duration_histogram_new.record(
                elapsed_time, attributes=duration_attrs_new
            )

    async def on_request_start(
        unused_session: aiohttp.ClientSession,
        trace_config_ctx: types.SimpleNamespace,
        params: aiohttp.TraceRequestStartParams,
    ):
        if (
            not is_http_instrumentation_enabled()
            or trace_config_ctx.excluded_urls.url_disabled(str(params.url))
        ):
            trace_config_ctx.span = None
            return

        trace_config_ctx.start_time = default_timer()
        method = params.method
        request_span_name = _get_span_name(method)
        request_url = (
            redact_url(trace_config_ctx.url_filter(params.url))
            if callable(trace_config_ctx.url_filter)
            else redact_url(str(params.url))
        )

        span_attributes = {}
        _set_http_method(
            span_attributes,
            method,
            sanitize_method(method),
            sem_conv_opt_in_mode,
        )
        _set_http_method(
            trace_config_ctx.metric_attributes,
            method,
            sanitize_method(method),
            sem_conv_opt_in_mode,
        )
        _set_http_url(span_attributes, request_url, sem_conv_opt_in_mode)

        try:
            parsed_url = urlparse(request_url)
            if parsed_url.hostname:
                _set_http_host_client(
                    trace_config_ctx.metric_attributes,
                    parsed_url.hostname,
                    sem_conv_opt_in_mode,
                )
                _set_http_net_peer_name_client(
                    trace_config_ctx.metric_attributes,
                    parsed_url.hostname,
                    sem_conv_opt_in_mode,
                )
                if _report_new(sem_conv_opt_in_mode):
                    _set_http_host_client(
                        span_attributes,
                        parsed_url.hostname,
                        sem_conv_opt_in_mode,
                    )
            if parsed_url.port:
                _set_http_peer_port_client(
                    trace_config_ctx.metric_attributes,
                    parsed_url.port,
                    sem_conv_opt_in_mode,
                )
                if _report_new(sem_conv_opt_in_mode):
                    _set_http_peer_port_client(
                        span_attributes, parsed_url.port, sem_conv_opt_in_mode
                    )
        except ValueError:
            pass

        trace_config_ctx.span = trace_config_ctx.tracer.start_span(
            request_span_name, kind=SpanKind.CLIENT, attributes=span_attributes
        )

        if callable(request_hook):
            request_hook(trace_config_ctx.span, params)

        trace_config_ctx.token = context_api.attach(
            trace.set_span_in_context(trace_config_ctx.span)
        )

        inject(params.headers)

    async def on_request_end(
        unused_session: aiohttp.ClientSession,
        trace_config_ctx: types.SimpleNamespace,
        params: aiohttp.TraceRequestEndParams,
    ):
        if trace_config_ctx.span is None:
            return

        if callable(response_hook):
            response_hook(trace_config_ctx.span, params)
        _set_http_status_code_attribute(
            trace_config_ctx.span,
            params.response.status,
            trace_config_ctx.metric_attributes,
            sem_conv_opt_in_mode,
        )

        _end_trace(trace_config_ctx)

    async def on_request_exception(
        unused_session: aiohttp.ClientSession,
        trace_config_ctx: types.SimpleNamespace,
        params: aiohttp.TraceRequestExceptionParams,
    ):
        if trace_config_ctx.span is None:
            return

        if trace_config_ctx.span.is_recording() and params.exception:
            exc_type = type(params.exception).__qualname__
            if _report_new(sem_conv_opt_in_mode):
                trace_config_ctx.span.set_attribute(ERROR_TYPE, exc_type)
                trace_config_ctx.metric_attributes[ERROR_TYPE] = exc_type

            trace_config_ctx.span.set_status(
                Status(StatusCode.ERROR, exc_type)
            )
            trace_config_ctx.span.record_exception(params.exception)

        if callable(response_hook):
            response_hook(trace_config_ctx.span, params)

        _end_trace(trace_config_ctx)

    def _trace_config_ctx_factory(**kwargs):
        kwargs.setdefault("trace_request_ctx", {})
        return types.SimpleNamespace(
            tracer=tracer,
            url_filter=url_filter,
            start_time=start_time,
            duration_histogram_old=duration_histogram_old,
            duration_histogram_new=duration_histogram_new,
            excluded_urls=excluded_urls,
            metric_attributes={},
            **kwargs,
        )

    trace_config = aiohttp.TraceConfig(
        trace_config_ctx_factory=_trace_config_ctx_factory
    )

    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_end.append(on_request_end)
    trace_config.on_request_exception.append(on_request_exception)

    return trace_config


def _instrument(
    tracer_provider: TracerProvider = None,
    meter_provider: MeterProvider = None,
    url_filter: _UrlFilterT = None,
    request_hook: _RequestHookT = None,
    response_hook: _ResponseHookT = None,
    trace_configs: typing.Optional[
        typing.Sequence[aiohttp.TraceConfig]
    ] = None,
    sem_conv_opt_in_mode: _StabilityMode = _StabilityMode.DEFAULT,
):
    """Enables tracing of all ClientSessions

    When a ClientSession gets created a TraceConfig is automatically added to
    the session's trace_configs.
    """

    trace_configs = trace_configs or ()

    # pylint:disable=unused-argument
    def instrumented_init(wrapped, instance, args, kwargs):
        client_trace_configs = list(kwargs.get("trace_configs") or [])
        client_trace_configs.extend(trace_configs)

        trace_config = create_trace_config(
            url_filter=url_filter,
            request_hook=request_hook,
            response_hook=response_hook,
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            sem_conv_opt_in_mode=sem_conv_opt_in_mode,
        )
        trace_config._is_instrumented_by_opentelemetry = True
        client_trace_configs.append(trace_config)

        kwargs["trace_configs"] = client_trace_configs
        return wrapped(*args, **kwargs)

    wrapt.wrap_function_wrapper(
        aiohttp.ClientSession, "__init__", instrumented_init
    )


def _uninstrument():
    """Disables instrumenting for all newly created ClientSessions"""
    unwrap(aiohttp.ClientSession, "__init__")


def _uninstrument_session(client_session: aiohttp.ClientSession):
    """Disables instrumentation for the given ClientSession"""
    # pylint: disable=protected-access
    trace_configs = client_session._trace_configs
    client_session._trace_configs = [
        trace_config
        for trace_config in trace_configs
        if not hasattr(trace_config, "_is_instrumented_by_opentelemetry")
    ]


class AioHttpClientInstrumentor(BaseInstrumentor):
    """An instrumentor for aiohttp client sessions

    See `BaseInstrumentor`
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        """Instruments aiohttp ClientSession

        Args:
            **kwargs: Optional arguments
                ``tracer_provider``: a TracerProvider, defaults to global
                ``meter_provider``: a MeterProvider, defaults to global
                ``url_filter``: A callback to process the requested URL prior to adding
                    it as a span attribute. This can be useful to remove sensitive data
                    such as API keys or user personal information.
                ``request_hook``: An optional callback that is invoked right after a span is created.
                ``response_hook``: An optional callback which is invoked right before the span is finished processing a response.
                ``trace_configs``: An optional list of aiohttp.TraceConfig items, allowing customize enrichment of spans
                 based on aiohttp events (see specification: https://docs.aiohttp.org/en/stable/tracing_reference.html)
        """
        _OpenTelemetrySemanticConventionStability._initialize()
        _sem_conv_opt_in_mode = _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
            _OpenTelemetryStabilitySignalType.HTTP,
        )
        _instrument(
            tracer_provider=kwargs.get("tracer_provider"),
            meter_provider=kwargs.get("meter_provider"),
            url_filter=kwargs.get("url_filter"),
            request_hook=kwargs.get("request_hook"),
            response_hook=kwargs.get("response_hook"),
            trace_configs=kwargs.get("trace_configs"),
            sem_conv_opt_in_mode=_sem_conv_opt_in_mode,
        )

    def _uninstrument(self, **kwargs):
        _uninstrument()

    @staticmethod
    def uninstrument_session(client_session: aiohttp.ClientSession):
        """Disables instrumentation for the given session"""
        _uninstrument_session(client_session)

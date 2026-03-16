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

"""
Instrument `Botocore`_ to trace service requests.

There are two options for instrumenting code. The first option is to use the
``opentelemetry-instrument`` executable which will automatically
instrument your Botocore client. The second is to programmatically enable
instrumentation via the following code:

.. _Botocore: https://pypi.org/project/botocore/

Usage
-----

.. code:: python

    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    import botocore.session


    # Instrument Botocore
    BotocoreInstrumentor().instrument()

    # This will create a span with Botocore-specific attributes
    session = botocore.session.get_session()
    session.set_credentials(
        access_key="access-key", secret_key="secret-key"
    )
    ec2 = session.create_client("ec2", region_name="us-west-2")
    ec2.describe_instances()

API
---

The `instrument` method accepts the following keyword args:

tracer_provider (TracerProvider) - an optional tracer provider
request_hook (Callable) - a function with extra user-defined logic to be performed before performing the request
this function signature is:  def request_hook(span: Span, service_name: str, operation_name: str, api_params: dict) -> None
response_hook (Callable) - a function with extra user-defined logic to be performed after performing the request
this function signature is:  def response_hook(span: Span, service_name: str, operation_name: str, result: dict) -> None

for example:

.. code: python

    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    import botocore.session

    def request_hook(span, service_name, operation_name, api_params):
        # request hook logic
        pass

    def response_hook(span, service_name, operation_name, result):
        # response hook logic
        pass

    # Instrument Botocore with hooks
    BotocoreInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook)

    # This will create a span with Botocore-specific attributes, including custom attributes added from the hooks
    session = botocore.session.get_session()
    session.set_credentials(
        access_key="access-key", secret_key="secret-key"
    )
    ec2 = session.create_client("ec2", region_name="us-west-2")
    ec2.describe_instances()
"""

import logging
from typing import Any, Callable, Collection, Dict, Optional, Tuple

from botocore.client import BaseClient
from botocore.endpoint import Endpoint
from botocore.exceptions import ClientError
from wrapt import wrap_function_wrapper

from opentelemetry._logs import get_logger
from opentelemetry.instrumentation.botocore.extensions import (
    _find_extension,
    _has_extension,
)
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AwsSdkCallContext,
    _AwsSdkExtension,
    _BotocoreInstrumentorContext,
)
from opentelemetry.instrumentation.botocore.package import _instruments
from opentelemetry.instrumentation.botocore.utils import get_server_attributes
from opentelemetry.instrumentation.botocore.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import (
    is_instrumentation_enabled,
    suppress_http_instrumentation,
    unwrap,
)
from opentelemetry.metrics import Instrument, Meter, get_meter
from opentelemetry.propagators.aws.aws_xray_propagator import AwsXRayPropagator
from opentelemetry.semconv._incubating.attributes.cloud_attributes import (
    CLOUD_REGION,
)
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import get_tracer
from opentelemetry.trace.span import Span

logger = logging.getLogger(__name__)


class BotocoreInstrumentor(BaseInstrumentor):
    """An instrumentor for Botocore.

    See `BaseInstrumentor`
    """

    def __init__(self):
        super().__init__()
        self.request_hook = None
        self.response_hook = None
        self.propagator = AwsXRayPropagator()

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        # pylint: disable=attribute-defined-outside-init

        # tracers are lazy initialized per-extension in _get_tracer
        self._tracers = {}
        # loggers are lazy initialized per-extension in _get_logger
        self._loggers = {}
        # meters are lazy initialized per-extension in _get_meter
        self._meters = {}
        # metrics are lazy initialized per-extension in _get_metrics
        self._metrics: Dict[str, Dict[str, Instrument]] = {}

        self.request_hook = kwargs.get("request_hook")
        self.response_hook = kwargs.get("response_hook")

        propagator = kwargs.get("propagator")
        if propagator is not None:
            self.propagator = propagator

        self.tracer_provider = kwargs.get("tracer_provider")
        self.logger_provider = kwargs.get("logger_provider")
        self.meter_provider = kwargs.get("meter_provider")

        wrap_function_wrapper(
            "botocore.client",
            "BaseClient._make_api_call",
            self._patched_api_call,
        )

        wrap_function_wrapper(
            "botocore.endpoint",
            "Endpoint.prepare_request",
            self._patched_endpoint_prepare_request,
        )

    @staticmethod
    def _get_instrumentation_name(extension: _AwsSdkExtension) -> str:
        has_extension = _has_extension(extension._call_context)
        return (
            f"{__name__}.{extension._call_context.service}"
            if has_extension
            else __name__
        )

    def _get_tracer(self, extension: _AwsSdkExtension):
        """This is a multiplexer in order to have a tracer per extension"""

        instrumentation_name = self._get_instrumentation_name(extension)
        tracer = self._tracers.get(instrumentation_name)
        if tracer:
            return tracer

        schema_version = extension.tracer_schema_version()
        self._tracers[instrumentation_name] = get_tracer(
            instrumentation_name,
            __version__,
            self.tracer_provider,
            schema_url=f"https://opentelemetry.io/schemas/{schema_version}",
        )
        return self._tracers[instrumentation_name]

    def _get_logger(self, extension: _AwsSdkExtension):
        """This is a multiplexer in order to have a logger per extension"""

        instrumentation_name = self._get_instrumentation_name(extension)
        instrumentation_logger = self._loggers.get(instrumentation_name)
        if instrumentation_logger:
            return instrumentation_logger

        schema_version = extension.event_logger_schema_version()
        self._loggers[instrumentation_name] = get_logger(
            instrumentation_name,
            "",
            schema_url=f"https://opentelemetry.io/schemas/{schema_version}",
            logger_provider=self.logger_provider,
        )

        return self._loggers[instrumentation_name]

    def _get_meter(self, extension: _AwsSdkExtension):
        """This is a multiplexer in order to have a meter per extension"""

        instrumentation_name = self._get_instrumentation_name(extension)
        meter = self._meters.get(instrumentation_name)
        if meter:
            return meter

        schema_version = extension.meter_schema_version()
        self._meters[instrumentation_name] = get_meter(
            instrumentation_name,
            "",
            schema_url=f"https://opentelemetry.io/schemas/{schema_version}",
            meter_provider=self.meter_provider,
        )

        return self._meters[instrumentation_name]

    def _get_metrics(
        self, extension: _AwsSdkExtension, meter: Meter
    ) -> Dict[str, Instrument]:
        """This is a multiplexer for lazy initialization of metrics required by extensions"""
        instrumentation_name = self._get_instrumentation_name(extension)
        metrics = self._metrics.get(instrumentation_name)
        if metrics is not None:
            return metrics

        self._metrics.setdefault(instrumentation_name, {})
        metrics = self._metrics[instrumentation_name]
        _safe_invoke(extension.setup_metrics, meter, metrics)
        return metrics

    def _uninstrument(self, **kwargs):
        unwrap(BaseClient, "_make_api_call")
        unwrap(Endpoint, "prepare_request")

    # pylint: disable=unused-argument
    def _patched_endpoint_prepare_request(
        self, wrapped, instance, args, kwargs
    ):
        request = args[0]
        headers = request.headers

        # Only the x-ray header is propagated by AWS services. Using any
        # other propagator will lose the trace context.
        self.propagator.inject(headers)

        return wrapped(*args, **kwargs)

    # pylint: disable=too-many-branches
    def _patched_api_call(self, original_func, instance, args, kwargs):
        if not is_instrumentation_enabled():
            return original_func(*args, **kwargs)

        call_context = _determine_call_context(instance, args)
        if call_context is None:
            return original_func(*args, **kwargs)

        extension = _find_extension(call_context)
        if not extension.should_trace_service_call():
            return original_func(*args, **kwargs)

        attributes = {
            SpanAttributes.RPC_SYSTEM: "aws-api",
            SpanAttributes.RPC_SERVICE: call_context.service_id,
            SpanAttributes.RPC_METHOD: call_context.operation,
            CLOUD_REGION: call_context.region,
            **get_server_attributes(call_context.endpoint_url),
        }

        _safe_invoke(extension.extract_attributes, attributes)
        end_span_on_exit = extension.should_end_span_on_exit()

        tracer = self._get_tracer(extension)
        meter = self._get_meter(extension)
        metrics = self._get_metrics(extension, meter)
        instrumentor_ctx = _BotocoreInstrumentorContext(
            logger=self._get_logger(extension),
            metrics=metrics,
        )
        with tracer.start_as_current_span(
            call_context.span_name,
            kind=call_context.span_kind,
            attributes=attributes,
            # tracing streaming services require to close the span manually
            # at a later time after the stream has been consumed
            end_on_exit=end_span_on_exit,
        ) as span:
            _safe_invoke(extension.before_service_call, span, instrumentor_ctx)
            self._call_request_hook(span, call_context)

            try:
                with suppress_http_instrumentation():
                    result = None
                    try:
                        result = original_func(*args, **kwargs)
                    except ClientError as error:
                        result = getattr(error, "response", None)
                        _apply_response_attributes(span, result)
                        _safe_invoke(
                            extension.on_error, span, error, instrumentor_ctx
                        )
                        raise
                    _apply_response_attributes(span, result)
                    _safe_invoke(
                        extension.on_success, span, result, instrumentor_ctx
                    )
            finally:
                _safe_invoke(extension.after_service_call, instrumentor_ctx)
                self._call_response_hook(span, call_context, result)

            return result

    def _call_request_hook(self, span: Span, call_context: _AwsSdkCallContext):
        if not callable(self.request_hook):
            return
        self.request_hook(
            span,
            call_context.service,
            call_context.operation,
            call_context.params,
        )

    def _call_response_hook(
        self, span: Span, call_context: _AwsSdkCallContext, result
    ):
        if not callable(self.response_hook):
            return
        self.response_hook(
            span, call_context.service, call_context.operation, result
        )


def _apply_response_attributes(span: Span, result):
    if result is None or not span.is_recording():
        return

    metadata = result.get("ResponseMetadata")
    if metadata is None:
        return

    request_id = metadata.get("RequestId")
    if request_id is None:
        headers = metadata.get("HTTPHeaders")
        if headers is not None:
            request_id = (
                headers.get("x-amzn-RequestId")
                or headers.get("x-amz-request-id")
                or headers.get("x-amz-id-2")
            )
    if request_id:
        # TODO: update when semantic conventions exist
        span.set_attribute("aws.request_id", request_id)

    retry_attempts = metadata.get("RetryAttempts")
    if retry_attempts is not None:
        # TODO: update when semantic conventions exists
        span.set_attribute("retry_attempts", retry_attempts)

    status_code = metadata.get("HTTPStatusCode")
    if status_code is not None:
        span.set_attribute(SpanAttributes.HTTP_STATUS_CODE, status_code)


def _determine_call_context(
    client: BaseClient, args: Tuple[str, Dict[str, Any]]
) -> Optional[_AwsSdkCallContext]:
    try:
        call_context = _AwsSdkCallContext(client, args)

        logger.debug(
            "AWS SDK invocation: %s %s",
            call_context.service,
            call_context.operation,
        )

        return call_context
    except Exception as ex:  # pylint:disable=broad-except
        # this shouldn't happen actually unless internals of botocore changed and
        # extracting essential attributes ('service' and 'operation') failed.
        logger.error("Error when initializing call context", exc_info=ex)
        return None


def _safe_invoke(function: Callable, *args):
    function_name = "<unknown>"
    try:
        function_name = function.__name__
        function(*args)
    except Exception as ex:  # pylint:disable=broad-except
        logger.error(
            "Error when invoking function '%s'", function_name, exc_info=ex
        )

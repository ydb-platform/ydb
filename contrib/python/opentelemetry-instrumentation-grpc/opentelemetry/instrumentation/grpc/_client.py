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

# pylint:disable=relative-beyond-top-level
# pylint:disable=arguments-differ
# pylint:disable=no-member
# pylint:disable=signature-differs

"""Implementation of the invocation-side open-telemetry interceptor."""

import logging
from collections import OrderedDict
from typing import Callable, MutableMapping

import grpc

from opentelemetry import trace
from opentelemetry.instrumentation.grpc import grpcext
from opentelemetry.instrumentation.grpc._utilities import RpcInfo
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.propagate import inject
from opentelemetry.propagators.textmap import Setter
from opentelemetry.semconv._incubating.attributes.rpc_attributes import (
    RPC_GRPC_STATUS_CODE,
    RPC_METHOD,
    RPC_SERVICE,
    RPC_SYSTEM,
)
from opentelemetry.trace.status import Status, StatusCode

logger = logging.getLogger(__name__)


class _CarrierSetter(Setter):
    """We use a custom setter in order to be able to lower case
    keys as is required by grpc.
    """

    def set(self, carrier: MutableMapping[str, str], key: str, value: str):
        carrier[key.lower()] = value


_carrier_setter = _CarrierSetter()


def _make_future_done_callback(span, rpc_info):
    def callback(response_future):
        with trace.use_span(span, end_on_exit=True):
            code = response_future.code()
            if code != grpc.StatusCode.OK:
                rpc_info.error = code
                return
            response = response_future.result()
            rpc_info.response = response

    return callback


def _safe_invoke(function: Callable, *args):
    function_name = "<unknown>"
    try:
        function_name = function.__name__
        function(*args)
    except Exception as ex:  # pylint:disable=broad-except
        logger.error(
            "Error when invoking function '%s'", function_name, exc_info=ex
        )


class OpenTelemetryClientInterceptor(
    grpcext.UnaryClientInterceptor, grpcext.StreamClientInterceptor
):
    def __init__(
        self, tracer, filter_=None, request_hook=None, response_hook=None
    ):
        self._tracer = tracer
        self._filter = filter_
        self._request_hook = request_hook
        self._response_hook = response_hook

    def _start_span(self, method, **kwargs):
        service, meth = method.lstrip("/").split("/", 1)
        attributes = {
            RPC_SYSTEM: "grpc",
            RPC_GRPC_STATUS_CODE: grpc.StatusCode.OK.value[0],
            RPC_METHOD: meth,
            RPC_SERVICE: service,
        }

        return self._tracer.start_as_current_span(
            name=method,
            kind=trace.SpanKind.CLIENT,
            attributes=attributes,
            **kwargs,
        )

    # pylint:disable=no-self-use
    def _trace_result(self, span, rpc_info, result):
        # If the RPC is called asynchronously, add a callback to end the span
        # when the future is done, else end the span immediately
        if isinstance(result, grpc.Future):
            result.add_done_callback(
                _make_future_done_callback(span, rpc_info)
            )
            return result
        response = result
        # Handle the case when the RPC is initiated via the with_call
        # method and the result is a tuple with the first element as the
        # response.
        # http://www.grpc.io/grpc/python/grpc.html#grpc.UnaryUnaryMultiCallable.with_call
        if isinstance(result, tuple):
            response = result[0]
        rpc_info.response = response
        if self._response_hook:
            self._call_response_hook(span, response)
        span.end()
        return result

    def _intercept(self, request, metadata, client_info, invoker):
        if not is_instrumentation_enabled():
            return invoker(request, metadata)

        if not metadata:
            mutable_metadata = OrderedDict()
        else:
            mutable_metadata = OrderedDict(metadata)
        with self._start_span(
            client_info.full_method,
            end_on_exit=False,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            result = None
            try:
                inject(mutable_metadata, setter=_carrier_setter)
                metadata = tuple(mutable_metadata.items())

                rpc_info = RpcInfo(
                    full_method=client_info.full_method,
                    metadata=metadata,
                    timeout=client_info.timeout,
                    request=request,
                )
                if self._request_hook:
                    self._call_request_hook(span, request)
                result = invoker(request, metadata)
            except Exception as exc:
                if isinstance(exc, grpc.RpcError):
                    span.set_attribute(
                        RPC_GRPC_STATUS_CODE,
                        exc.code().value[0],
                    )
                span.set_status(
                    Status(
                        status_code=StatusCode.ERROR,
                        description=f"{type(exc).__name__}: {exc}",
                    )
                )
                span.record_exception(exc)
                raise exc
            finally:
                if result is None:
                    span.end()
        return self._trace_result(span, rpc_info, result)

    def _call_request_hook(self, span, request):
        if not callable(self._request_hook):
            return
        _safe_invoke(self._request_hook, span, request)

    def _call_response_hook(self, span, response):
        if not callable(self._response_hook):
            return
        _safe_invoke(self._response_hook, span, response)

    def intercept_unary(self, request, metadata, client_info, invoker):
        if self._filter is not None and not self._filter(client_info):
            return invoker(request, metadata)
        return self._intercept(request, metadata, client_info, invoker)

    # For RPCs that stream responses, the result can be a generator. To record
    # the span across the generated responses and detect any errors, we wrap
    # the result in a new generator that yields the response values.
    def _intercept_server_stream(
        self, request_or_iterator, metadata, client_info, invoker
    ):
        if not metadata:
            mutable_metadata = OrderedDict()
        else:
            mutable_metadata = OrderedDict(metadata)

        with self._start_span(client_info.full_method) as span:
            inject(mutable_metadata, setter=_carrier_setter)
            metadata = tuple(mutable_metadata.items())
            rpc_info = RpcInfo(
                full_method=client_info.full_method,
                metadata=metadata,
                timeout=client_info.timeout,
            )

            if client_info.is_client_stream:
                rpc_info.request = request_or_iterator

            try:
                yield from invoker(request_or_iterator, metadata)
            except grpc.RpcError as err:
                span.set_status(Status(StatusCode.ERROR))
                span.set_attribute(RPC_GRPC_STATUS_CODE, err.code().value[0])
                raise err

    def intercept_stream(
        self, request_or_iterator, metadata, client_info, invoker
    ):
        if not is_instrumentation_enabled():
            return invoker(request_or_iterator, metadata)

        if self._filter is not None and not self._filter(client_info):
            return invoker(request_or_iterator, metadata)

        if client_info.is_server_stream:
            return self._intercept_server_stream(
                request_or_iterator, metadata, client_info, invoker
            )

        return self._intercept(
            request_or_iterator, metadata, client_info, invoker
        )

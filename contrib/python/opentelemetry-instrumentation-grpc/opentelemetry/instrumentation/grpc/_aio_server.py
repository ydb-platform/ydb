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

import grpc
import grpc.aio
import wrapt

from opentelemetry.semconv._incubating.attributes.rpc_attributes import (
    RPC_GRPC_STATUS_CODE,
)

from ._server import OpenTelemetryServerInterceptor, _wrap_rpc_behavior
from ._utilities import _server_status


# pylint:disable=abstract-method
class _OpenTelemetryAioServicerContext(wrapt.ObjectProxy):
    def __init__(self, servicer_context, active_span):
        super().__init__(servicer_context)
        self._self_active_span = active_span
        self._self_code = grpc.StatusCode.OK
        self._self_details = None

    async def abort(self, code, details="", trailing_metadata=tuple()):
        self._self_code = code
        self._self_details = details
        self._self_active_span.set_attribute(
            RPC_GRPC_STATUS_CODE, code.value[0]
        )
        status = _server_status(code, details)
        self._self_active_span.set_status(status)
        return await self.__wrapped__.abort(code, details, trailing_metadata)

    def set_code(self, code):
        self._self_code = code
        details = self._self_details or code.value[1]
        self._self_active_span.set_attribute(
            RPC_GRPC_STATUS_CODE, code.value[0]
        )
        if code != grpc.StatusCode.OK:
            status = _server_status(code, details)
            self._self_active_span.set_status(status)
        return self.__wrapped__.set_code(code)

    def set_details(self, details):
        self._self_details = details
        if self._self_code != grpc.StatusCode.OK:
            status = _server_status(self._self_code, details)
            self._self_active_span.set_status(status)
        return self.__wrapped__.set_details(details)


class OpenTelemetryAioServerInterceptor(
    grpc.aio.ServerInterceptor, OpenTelemetryServerInterceptor
):
    """
    An AsyncIO gRPC server interceptor, to add OpenTelemetry.
    Usage::
        tracer = some OpenTelemetry tracer
        interceptors = [
            AsyncOpenTelemetryServerInterceptor(tracer),
        ]
        server = aio.server(
            futures.ThreadPoolExecutor(max_workers=concurrency),
            interceptors = (interceptors,))
    """

    async def intercept_service(self, continuation, handler_call_details):
        if self._filter is not None and not self._filter(handler_call_details):
            return await continuation(handler_call_details)

        def telemetry_wrapper(behavior, request_streaming, response_streaming):
            # handle streaming responses specially
            if response_streaming:
                return self._intercept_aio_server_stream(
                    behavior,
                    handler_call_details,
                )

            return self._intercept_aio_server_unary(
                behavior,
                handler_call_details,
            )

        next_handler = await continuation(handler_call_details)

        return _wrap_rpc_behavior(next_handler, telemetry_wrapper)

    def _intercept_aio_server_unary(self, behavior, handler_call_details):
        async def _unary_interceptor(request_or_iterator, context):
            with self._set_remote_context(context):
                with self._start_span(
                    handler_call_details,
                    context,
                    set_status_on_exception=False,
                ) as span:
                    # wrap the context
                    context = _OpenTelemetryAioServicerContext(context, span)

                    # And now we run the actual RPC.
                    try:
                        return await behavior(request_or_iterator, context)

                    except Exception as error:
                        # Bare exceptions are likely to be gRPC aborts, which
                        # we handle in our context wrapper.
                        # Here, we're interested in uncaught exceptions.
                        # pylint:disable=unidiomatic-typecheck
                        if type(error) != Exception:  # noqa: E721
                            span.record_exception(error)
                        raise error

        return _unary_interceptor

    def _intercept_aio_server_stream(self, behavior, handler_call_details):
        async def _stream_interceptor(request_or_iterator, context):
            with self._set_remote_context(context):
                with self._start_span(
                    handler_call_details,
                    context,
                    set_status_on_exception=False,
                ) as span:
                    context = _OpenTelemetryAioServicerContext(context, span)

                    try:
                        async for response in behavior(
                            request_or_iterator, context
                        ):
                            yield response

                    except Exception as error:
                        # pylint:disable=unidiomatic-typecheck
                        if type(error) != Exception:  # noqa: E721
                            span.record_exception(error)
                        raise error

        return _stream_interceptor

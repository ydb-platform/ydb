# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A gRPC Interceptor that is responsible for handling Google Ads API errors.

This class is initialized in the GoogleAdsClient and passed into a grpc
intercept_channel whenever a new service is initialized. It intercepts requests
to determine if a non-retryable Google Ads API error has been encountered. If
so it translates the error to a GoogleAdsFailure instance and raises it.
"""

from typing import Optional, Union

from google.protobuf.message import Message
import grpc

from google.ads.googleads import util
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.interceptors import Interceptor, ContinuationType
from google.ads.googleads.interceptors.response_wrappers import _UnaryStreamWrapper, _UnaryUnaryWrapper


class ExceptionInterceptor(
    Interceptor, grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor
):
    """An interceptor that wraps rpc exceptions."""

    def __init__(self, api_version: str, use_proto_plus: bool = False):
        """Initializes the ExceptionInterceptor.

        Args:
            api_version: a str of the API version of the request.
            use_proto_plus: a boolean of whether returned messages should be
                proto_plus or protobuf.
        """
        super().__init__(api_version)
        self._api_version = api_version
        self._use_proto_plus = use_proto_plus

    def _handle_grpc_failure(self, response: Union[grpc.Call, grpc.Future]):
        """Attempts to convert failed responses to a GoogleAdsException object.

        Handles failed gRPC responses of by attempting to convert them
        to a more readable GoogleAdsException. Certain types of exceptions are
        not converted; if the object's trailing metadata does not indicate that
        it is a GoogleAdsException, or if it falls under a certain category of
        status code, (INTERNAL or RESOURCE_EXHAUSTED). See documentation for
        more information about gRPC status codes:
        https://github.com/grpc/grpc/blob/master/doc/statuscodes.md

        Args:
            response: a grpc.Call/grpc.Future instance.

        Raises:
            GoogleAdsException: If the exception's trailing metadata
                indicates that it is a GoogleAdsException.
            RpcError: If the exception's is a gRPC exception but the trailing
                metadata is empty or is not indicative of a GoogleAdsException,
                or if the exception has a status code of INTERNAL or
                RESOURCE_EXHAUSTED.
            Exception: If not a GoogleAdsException or RpcException the error
                will be raised as-is.
        """
        raise self._get_error_from_response(response)

    def intercept_unary_unary(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: Message,
    ):
        """Intercepts and wraps exceptions in the rpc response.

        Overrides abstract method defined in grpc.UnaryUnaryClientInterceptor.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call instance representing a service response.

        Raises:
            GoogleAdsException: If the exception's trailing metadata
                indicates that it is a GoogleAdsException.
            RpcError: If the exception's trailing metadata is empty or is not
                indicative of a GoogleAdsException, or if the exception has a
                status code of INTERNAL or RESOURCE_EXHAUSTED.
        """
        response: grpc.Call = continuation(client_call_details, request)
        exception: Optional[grpc.RpcError] = response.exception()

        if exception:
            self._handle_grpc_failure(response)
        else:
            return _UnaryUnaryWrapper(
                response, use_proto_plus=self._use_proto_plus
            )

    def intercept_unary_stream(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: Message,
    ):
        """Intercepts and wraps exceptions in the rpc response.

        Overrides abstract method defined in grpc.UnaryStreamClientInterceptor.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call instance representing a service response.

        Raises:
            GoogleAdsException: If the exception's trailing metadata
                indicates that it is a GoogleAdsException.
            RpcError: If the exception's trailing metadata is empty or is not
                indicative of a GoogleAdsException, or if the exception has a
                status code of INTERNAL or RESOURCE_EXHAUSTED.
        """
        response: grpc.Call = continuation(client_call_details, request)
        return _UnaryStreamWrapper(
            response,
            self._handle_grpc_failure,
            use_proto_plus=self._use_proto_plus,
        )

class _AsyncUnaryUnaryCallWrapper(grpc.aio.UnaryUnaryCall):
    def __init__(self, call, interceptor, use_proto_plus: bool = False):
        self._call = call
        self._interceptor = interceptor
        self._use_proto_plus = use_proto_plus

    def __await__(self):
        try:
            response = yield from self._call.__await__()

            if self._use_proto_plus is True:
                # By default this message is wrapped by proto-plus
                return response
            else:
                return util.convert_proto_plus_to_protobuf(response)
        except grpc.RpcError:
            yield from self._interceptor._handle_grpc_failure_async(self._call).__await__()
            raise

    def cancel(self):
        return self._call.cancel()

    def cancelled(self):
        return self._call.cancelled()

    def done(self):
        return self._call.done()

    def add_done_callback(self, callback):
        self._call.add_done_callback(callback)

    def code(self):
        return self._call.code()

    def details(self):
        return self._call.details()

    def initial_metadata(self):
        return self._call.initial_metadata()

    def trailing_metadata(self):
        return self._call.trailing_metadata()

    def time_remaining(self):
        return self._call.time_remaining()

    async def wait_for_connection(self):
        return await self._call.wait_for_connection()


class _AsyncUnaryStreamCallWrapper(grpc.aio.UnaryStreamCall):
    def __init__(self, call, interceptor, use_proto_plus: bool = False):
        self._call = call
        self._interceptor = interceptor
        self._use_proto_plus = use_proto_plus

    def __aiter__(self):
        async def _wrapped_aiter():
            try:
                async for response in self._call:
                    if self._use_proto_plus is True:
                        # By default this message is wrapped by proto-plus
                        yield response
                    else:
                        yield util.convert_proto_plus_to_protobuf(response)
            except grpc.RpcError:
                await self._interceptor._handle_grpc_failure_async(self._call)
                raise
        return _wrapped_aiter()

    def cancel(self):
        return self._call.cancel()

    def cancelled(self):
        return self._call.cancelled()

    def done(self):
        return self._call.done()

    def add_done_callback(self, callback):
        self._call.add_done_callback(callback)

    def code(self):
        return self._call.code()

    def details(self):
        return self._call.details()

    def initial_metadata(self):
        return self._call.initial_metadata()

    def trailing_metadata(self):
        return self._call.trailing_metadata()

    def time_remaining(self):
        return self._call.time_remaining()

    async def wait_for_connection(self):
        return await self._call.wait_for_connection()

    async def read(self):
        try:
            response =  await self._call.read()
            if self._use_proto_plus is True:
                # By default this message is wrapped by proto-plus
                return response
            else:
                return util.convert_proto_plus_to_protobuf(response)
        except grpc.RpcError:
            await self._interceptor._handle_grpc_failure_async(self._call)
            raise

class _AsyncExceptionInterceptor(
    ExceptionInterceptor,
):
    """An interceptor that wraps rpc exceptions."""

    async def _handle_grpc_failure_async(self, response: grpc.aio.Call):
        """Async version of _handle_grpc_failure."""
        status_code = response.code()
        response_exception = response.exception()

        # We need to access _RETRY_STATUS_CODES from interceptor module?
        # It's imported in interceptor.py but not exposed in ExceptionInterceptor?
        # It is in interceptor.py as _RETRY_STATUS_CODES.
        # We need to import it or access it.
        # It is NOT imported in exception_interceptor.py?
        # Let's check imports.

        # exception_interceptor.py imports:
        # from google.ads.googleads.interceptors import Interceptor, ContinuationType

        # _RETRY_STATUS_CODES is defined in interceptor.py but not exported in __all__?
        # We can access it via Interceptor._RETRY_STATUS_CODES if we added it?
        # interceptor.py has `_RETRY_STATUS_CODES` global.
        # It is NOT in Interceptor class.

        # We can import it if we modify imports or just redefine it.
        # Redefining is safer/easier.
        RETRY_STATUS_CODES = (grpc.StatusCode.INTERNAL, grpc.StatusCode.RESOURCE_EXHAUSTED)

        if status_code not in RETRY_STATUS_CODES:
            trailing_metadata = await response.trailing_metadata()
            google_ads_failure = self._get_google_ads_failure(trailing_metadata)

            if google_ads_failure and response_exception:
                request_id = self.get_request_id_from_metadata(trailing_metadata)
                raise GoogleAdsException(
                    response_exception, response, google_ads_failure, request_id
                )
            elif response_exception:
                raise response_exception
        elif response_exception:
            raise response_exception

        # If we got here, maybe no exception? But we only call this on error.
        raise response.exception()

    async def intercept_unary_unary(
        self,
        continuation,
        client_call_details,
        request,
    ):
        call = await continuation(client_call_details, request)
        return _AsyncUnaryUnaryCallWrapper(call, self, use_proto_plus=self._use_proto_plus)

    async def intercept_unary_stream(
        self,
        continuation,
        client_call_details,
        request,
    ):
        call = await continuation(client_call_details, request)
        return _AsyncUnaryStreamCallWrapper(call, self, use_proto_plus=self._use_proto_plus)

class AsyncUnaryUnaryExceptionInterceptor(
    _AsyncExceptionInterceptor,
    grpc.aio.UnaryUnaryClientInterceptor,
):
    """An interceptor that appends custom metadata to Unary-Unary requests."""

class AsyncUnaryStreamExceptionInterceptor(
    _AsyncExceptionInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
):
    """An interceptor that appends custom metadata to Unary-Stream requests."""

__all__ = [
    "ExceptionInterceptor",
    "AsyncUnaryUnaryExceptionInterceptor",
    "AsyncUnaryStreamExceptionInterceptor",
]
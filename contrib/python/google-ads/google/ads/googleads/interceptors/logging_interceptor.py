# Copyright 2022 Google LLC
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
"""A gRPC Interceptor that is responsible for logging requests and responses.

This class is initialized in the GoogleAdsClient and passed into a grpc
intercept_channel whenever a new service is initialized. It intercepts requests
and responses, parses them into a human readable structure and logs them using
the passed in logger instance.
"""

import json
import logging
from typing import Optional, Union

from google.protobuf.message import Message as ProtobufMessageType
import grpc

from google.ads.googleads.interceptors import (
    Interceptor, MetadataType, ContinuationType, mask_message
)


class LoggingInterceptor(
    Interceptor,
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor
):
    """An interceptor that logs rpc requests and responses."""

    _FULL_REQUEST_LOG_LINE: str = (
        "Request\n-------\nMethod: {}\nHost: {}\n"
        "Headers: {}\nRequest: {}\n\nResponse\n-------\n"
        "Headers: {}\nResponse: {}\n"
    )
    _FULL_FAULT_LOG_LINE: str = (
        "Request\n-------\nMethod: {}\nHost: {}\n"
        "Headers: {}\nRequest: {}\n\nResponse\n-------\n"
        "Headers: {}\nFault: {}\n"
    )
    _SUMMARY_LOG_LINE: str = (
        "Request made: ClientCustomerId: {}, Host: {}, "
        "Method: {}, RequestId: {}, IsFault: {}, "
        "FaultMessage: {}"
    )

    def __init__(
        self,
        logger: logging.Logger,
        api_version: str,
        endpoint: Optional[str] = None,
    ):
        """Initializer for the LoggingInterceptor.

        Args:
            logger: An instance of logging.Logger.
            api_version: a str of the API version of the request.
            endpoint: a str specifying the endpoint for requests.
        """
        super().__init__(api_version)
        self.endpoint = endpoint
        self.logger = logger
        self._cache = None

    def _get_trailing_metadata(self, response: Union[grpc.Call, grpc.Future]):
        """Retrieves trailing metadata from a response object.

        If the exception is a GoogleAdsException the trailing metadata will be
        on its error object, otherwise it will be on the response object.

        Returns:
            A tuple of metadatum representing response header key value pairs.

        Args:
            response: A grpc.Call/grpc.Future instance.
        """
        try:
            trailing_metadata: MetadataType = response.trailing_metadata()

            if not trailing_metadata:
                return self.get_trailing_metadata_from_interceptor_exception(
                    response.exception()
                )

            return trailing_metadata
        except AttributeError:
            return self.get_trailing_metadata_from_interceptor_exception(
                response.exception()
            )

    def _get_initial_metadata(
        self, client_call_details: grpc.ClientCallDetails
    ) -> MetadataType:
        """Retrieves the initial metadata from client_call_details.

        Returns an empty tuple if metadata isn't present on the
        client_call_details object.

        Returns:
            A tuple of metadatum representing request header key value pairs.

        Args:
            client_call_details: An instance of grpc.ClientCallDetails.
        """
        return getattr(client_call_details, "metadata", tuple())

    def _get_call_method(
        self, client_call_details: grpc.ClientCallDetails
    ) -> Optional[str]:
        """Retrieves the call method from client_call_details.

        Returns None if the method is not present on the client_call_details
        object.

        Returns:
            A str with the call method or None if it isn't present.

        Args:
            client_call_details: An instance of grpc.ClientCallDetails.
        """
        return getattr(client_call_details, "method", None)

    def _get_customer_id(self, request: ProtobufMessageType) -> Optional[str]:
        """Retrieves the customer_id from the grpc request.

        Returns None if a customer_id is not present on the request object.

        Returns:
            A str with the customer id from the request or None if it isn't
            present.

        Args:
            request: An instance of a request proto message.
        """
        if hasattr(request, "customer_id"):
            return getattr(request, "customer_id")
        elif hasattr(request, "resource_name"):
            resource_name: str = getattr(request, "resource_name")
            segments: str = resource_name.split("/")
            if segments[0] == "customers":
                return segments[1]
        else:
            return None

    def _parse_exception_to_str(self, exception: grpc.Call) -> str:
        """Parses response exception object to str for logging.

        Returns:
            A str representing a exception from the API.

        Args:
            exception: A grpc.Call instance.
        """
        try:
            # If the exception is from the Google Ads API then the failure
            # attribute will be an instance of GoogleAdsFailure. We convert it
            # to str here so the return type is consistent, rather than casting
            # by concatenating in the logs.
            return str(exception.failure)
        except AttributeError:
            try:
                # if exception.failure isn't present then it's likely this is a
                # transport error with a .debug_error_string method and the
                # returned JSON string will need to be formatted.
                return self.format_json_object(
                    json.loads(exception.debug_error_string())
                )
            except (AttributeError, ValueError):
                # if both attempts to retrieve serializable error data fail
                # then simply return an empty JSON string
                return "{}"

    def _get_fault_message(self, exception: grpc.Call) -> Optional[str]:
        """Retrieves a fault/error message from an exception object.

        Returns None if no error message can be found on the exception.

        Returns:
            A str with an error message or None if one cannot be found.

        Args:
            response: A grpc.Call/grpc.Future instance.
            exception: A grpc.Call instance.
        """
        try:
            return exception.failure.errors[0].message
        except AttributeError:
            try:
                return exception.details()
            except AttributeError:
                return None

    def log_successful_request(
        self,
        method: str,
        customer_id: str,
        metadata_json: str,
        request_id: str,
        request: ProtobufMessageType,
        trailing_metadata_json: str,
        response: Union[grpc.Call, grpc.Future],
    ) -> None:
        """Handles logging of a successful request.

        Args:
            method: The method of the request.
            customer_id: The customer ID associated with the request.
            metadata_json: A JSON str of initial_metadata.
            request_id: A unique ID for the request provided in the response.
            request: An instance of a request proto message.
            trailing_metadata_json: A JSON str of trailing_metadata.
            response: A grpc.Call/grpc.Future instance.
        """
        result: ProtobufMessageType = self.retrieve_and_mask_result(response)

        # Check log level here to avoid calling .format() unless necessary.
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(
                self._FULL_REQUEST_LOG_LINE.format(
                    method,
                    self.endpoint,
                    metadata_json,
                    request,
                    trailing_metadata_json,
                    result,
                )
            )

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(
                self._SUMMARY_LOG_LINE.format(
                    customer_id, self.endpoint, method, request_id, False, None
                )
            )

    def log_failed_request(
        self,
        method: str,
        customer_id: str,
        metadata_json: str,
        request_id: str,
        request: ProtobufMessageType,
        trailing_metadata_json: str,
        response: Union[grpc.Call, grpc.Future],
    ) -> None:
        """Handles logging of a failed request.

        Args:
            method: The method of the request.
            customer_id: The customer ID associated with the request.
            metadata_json: A JSON str of initial_metadata.
            request_id: A unique ID for the request provided in the response.
            request: An instance of a request proto message.
            trailing_metadata_json: A JSON str of trailing_metadata.
            response: A JSON str of the response message.
        """
        exception: grpc.Call = self._get_error_from_response(response)
        exception_str: str = self._parse_exception_to_str(exception)
        fault_message: str = self._get_fault_message(exception)

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(
                self._FULL_FAULT_LOG_LINE.format(
                    method,
                    self.endpoint,
                    metadata_json,
                    request,
                    trailing_metadata_json,
                    exception_str,
                )
            )

        self.logger.warning(
            self._SUMMARY_LOG_LINE.format(
                customer_id,
                self.endpoint,
                method,
                request_id,
                True,
                fault_message,
            )
        )

    def log_request(
        self,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
        response: Union[grpc.Call, grpc.Future],
    ) -> None:
        """Handles logging all requests.

        Args:
            client_call_details: An instance of grpc.ClientCallDetails.
            request: An instance of a request proto message.
            response: A grpc.Call/grpc.Future instance.
        """
        method: str = self._get_call_method(client_call_details)
        customer_id: str = self._get_customer_id(request)
        initial_metadata: MetadataType = self._get_initial_metadata(
            client_call_details
        )
        initial_metadata_json: str = self.parse_metadata_to_json(initial_metadata)
        trailing_metadata: MetadataType = self._get_trailing_metadata(response)
        request_id: str = self.get_request_id_from_metadata(
            trailing_metadata
        )
        trailing_metadata_json: str = self.parse_metadata_to_json(trailing_metadata)
        request: ProtobufMessageType = mask_message(request, self._SENSITIVE_INFO_MASK)

        if response.exception():
            self.log_failed_request(
                method,
                customer_id,
                initial_metadata_json,
                request_id,
                request,
                trailing_metadata_json,
                response,
            )
        else:
            self.log_successful_request(
                method,
                customer_id,
                initial_metadata_json,
                request_id,
                request,
                trailing_metadata_json,
                response,
            )

    def intercept_unary_unary(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        """Intercepts and logs API interactions.

        Overrides abstract method defined in grpc.UnaryUnaryClientInterceptor.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call/grpc.Future instance representing a service response.
        """
        response: grpc.Call = continuation(client_call_details, request)

        if self.logger.isEnabledFor(logging.WARNING):
            self.log_request(client_call_details, request, response)

        return response

    def intercept_unary_stream(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        """Intercepts and logs API interactions for Unary-Stream requests.

        Overrides abstract method defined in grpc.UnaryStreamClientInterceptor.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call/grpc.Future instance representing a service response.
        """
        def on_rpc_complete(response_future: grpc.Future) -> None:
            if self.logger.isEnabledFor(logging.WARNING):
                self.log_request(client_call_details, request, response_future)

        response: grpc.Call = continuation(client_call_details, request)

        response.add_done_callback(on_rpc_complete)
        self._cache = response.get_cache()
        return response

    def retrieve_and_mask_result(
        self, response: Union[grpc.Call, grpc.Future]
    ) -> ProtobufMessageType:
        """If the cache is populated, mask the cached stream response object.
        Otherwise, mask the non-streaming response result.

        Args:
            response: A grpc.Call/grpc.Future instance.

        Returns:
            A masked response message.
        """
        if self._cache and self._cache.initial_response_object is not None:
            # Get the first streaming response object from the cache.
            result: ProtobufMessageType = self._cache.initial_response_object
        else:
            result: ProtobufMessageType = response.result()
        # Mask the response object if debug level logging is enabled.
        if type(self.logger) is logging.Logger and self.logger.isEnabledFor(
            logging.DEBUG
        ):
            result: ProtobufMessageType = (
                mask_message(result, self._SENSITIVE_INFO_MASK)
            )

        return result


class _AsyncLoggingInterceptor(
    LoggingInterceptor,
):
    """An interceptor that logs rpc requests and responses."""

    async def _log_request_async(
        self,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
        response: grpc.aio.Call,
    ) -> None:
        """Handles logging all requests asynchronously.

        Args:
            client_call_details: An instance of grpc.ClientCallDetails.
            request: An instance of a request proto message.
            response: A grpc.aio.Call instance.
        """
        method: str = self._get_call_method(client_call_details)
        customer_id: str = self._get_customer_id(request)
        initial_metadata: MetadataType = self._get_initial_metadata(
            client_call_details
        )
        initial_metadata_json: str = self.parse_metadata_to_json(
            initial_metadata
        )

        # Await metadata
        trailing_metadata = await response.trailing_metadata()

        request_id: str = self.get_request_id_from_metadata(trailing_metadata)
        trailing_metadata_json: str = self.parse_metadata_to_json(
            trailing_metadata
        )
        request: ProtobufMessageType = mask_message(
            request, self._SENSITIVE_INFO_MASK
        )

        # Check for exception
        # response is a Call object (future).
        # We can check if it has exception.
        # But for async call, we might need to use done() and exception()?
        # Since this is called in on_done, it is done.

        try:
            # This might raise if cancelled?
            exception = response.exception()
        except Exception:
             exception = None

        if exception:
            # We need to adapt exception logging for async exception?
            # _parse_exception_to_str expects grpc.Call (sync).
            # It accesses exception.failure or exception.debug_error_string().
            # grpc.aio.RpcError has debug_error_string().
            # It might have failure if it's GoogleAdsException?
            # But ExceptionInterceptor wraps it.
            # If ExceptionInterceptor ran BEFORE this, the exception might be GoogleAdsException.
            # If we chain interceptors correctly.

            # We need to construct a dummy object that looks like sync response for log_failed_request?
            # Or just update log_failed_request to handle it.
            # But log_failed_request calls _get_error_from_response(response).
            # I'll just pass the exception as response? No.

            # I'll implement a helper to log failed request from exception.
            self.log_failed_request_async(
                method,
                customer_id,
                initial_metadata_json,
                request_id,
                request,
                trailing_metadata_json,
                response, # Pass the call object
                exception
            )
        else:
            result = None
            if not hasattr(response, "read"):
                result = await response

            self.log_successful_request_async(
                method,
                customer_id,
                initial_metadata_json,
                request_id,
                request,
                trailing_metadata_json,
                response,
                result,
            )

    def log_failed_request_async(
        self,
        method: str,
        customer_id: str,
        metadata_json: str,
        request_id: str,
        request: ProtobufMessageType,
        trailing_metadata_json: str,
        response: grpc.aio.Call,
        exception: Exception,
    ) -> None:
        # Adapted from log_failed_request
        # We need to handle exception object.

        # _get_error_from_response expects response object.
        # But here we have exception.
        # We can just use the exception directly.

        exception_str: str = self._parse_exception_to_str_async(exception)
        fault_message: str = self._get_fault_message_async(exception)

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(
                self._FULL_FAULT_LOG_LINE.format(
                    method,
                    self.endpoint,
                    metadata_json,
                    request,
                    trailing_metadata_json,
                    exception_str,
                )
            )

        self.logger.warning(
            self._SUMMARY_LOG_LINE.format(
                customer_id,
                self.endpoint,
                method,
                request_id,
                True,
                fault_message,
            )
        )

    def _parse_exception_to_str_async(self, exception: Exception) -> str:
        try:
            return str(exception.failure)
        except AttributeError:
            try:
                if hasattr(exception, "debug_error_string"):
                     return self.format_json_object(
                        json.loads(exception.debug_error_string())
                    )
                return str(exception)
            except (AttributeError, ValueError):
                return "{}"

    def _get_fault_message_async(self, exception: Exception) -> Optional[str]:
        try:
            return exception.failure.errors[0].message
        except AttributeError:
            try:
                return exception.details()
            except AttributeError:
                return None

    def log_successful_request_async(
        self,
        method: str,
        customer_id: str,
        metadata_json: str,
        request_id: str,
        request: ProtobufMessageType,
        trailing_metadata_json: str,
        response: grpc.aio.Call,
        result: ProtobufMessageType = None,
    ) -> None:
        # Result is already available
        if result is None and hasattr(response, "result"):
             result = response.result()

        if type(self.logger) is logging.Logger and self.logger.isEnabledFor(
            logging.DEBUG
        ):
            result = mask_message(result, self._SENSITIVE_INFO_MASK)

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(
                self._FULL_REQUEST_LOG_LINE.format(
                    method,
                    self.endpoint,
                    metadata_json,
                    request,
                    trailing_metadata_json,
                    result,
                )
            )

        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(
                self._SUMMARY_LOG_LINE.format(
                    customer_id, self.endpoint, method, request_id, False, None
                )
            )

    async def intercept_unary_unary(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        call = await continuation(client_call_details, request)

        def on_done(future):
            if self.logger.isEnabledFor(logging.WARNING):
                import asyncio
                # We need a running loop.
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._log_request_async(client_call_details, request, future))
                except RuntimeError:
                    pass

        call.add_done_callback(on_done)
        return call

    async def intercept_unary_stream(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        # For stream, we can't easily log result payload as it is a stream.
        # But we can log request.
        # And log failure/completion.

        call = await continuation(client_call_details, request)

        # We can add done callback to the call object?
        # grpc.aio.UnaryStreamCall is a Call.

        def on_done(future):
            if self.logger.isEnabledFor(logging.WARNING):
                import asyncio
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._log_request_async(client_call_details, request, future))
                except RuntimeError:
                    pass

        call.add_done_callback(on_done)
        return call

class AsyncUnaryUnaryLoggingInterceptor(
    _AsyncLoggingInterceptor,
    grpc.aio.UnaryUnaryClientInterceptor,
):
    """An interceptor that logs payloads in Unary-Unary requests."""

class AsyncUnaryStreamLoggingInterceptor(
    _AsyncLoggingInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
):
    """An interceptor that logs payloads in Unary-Stream requests."""

__all__ = [
    "MetadataInterceptor",
    "AsyncUnaryUnaryLoggingInterceptor",
    "AsyncUnaryStreamLoggingInterceptor",
]
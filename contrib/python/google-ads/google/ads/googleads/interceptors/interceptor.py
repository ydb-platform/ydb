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
"""A mixin class to store shared functionality for all the gRPC Interceptors.

This mixin class centralizes sets of functionality that are common across all
Interceptors, including retrieving data from gRPC metadata and initializing
instances of grpc.ClientCallDetails.
"""

from dataclasses import dataclass
from importlib import import_module
import json
from types import ModuleType
from typing import (
    Any,
    AnyStr,
    Callable,
    Dict,
    Optional,
    Sequence,
    Tuple
)

from google.protobuf.message import DecodeError, Message
import grpc
# from grpc import Call, ClientCallDetails, StatusCode, CallCredentials, RpcError

from google.ads.googleads.errors import GoogleAdsException


_REQUEST_ID_KEY: str = "request-id"
# Codes that are retried upon by google.api_core.
_RETRY_STATUS_CODES: Tuple[grpc.StatusCode]= (grpc.StatusCode.INTERNAL, grpc.StatusCode.RESOURCE_EXHAUSTED)

MetadataType = Sequence[Tuple[str, AnyStr]]
ContinuationType = Callable[[grpc.ClientCallDetails, Message], grpc.Call]

class Interceptor:
    _SENSITIVE_INFO_MASK: str = "REDACTED"

    @dataclass
    class _ClientCallDetails(grpc.ClientCallDetails):
        """Wrapper class for initializing a new ClientCallDetails instance."""

        method: str
        timeout: Optional[float]
        metadata: Optional[MetadataType]
        credentials: Optional[grpc.CallCredentials]
        wait_for_ready: Optional[bool] = None

    @classmethod
    def get_request_id_from_metadata(
        cls, trailing_metadata: MetadataType
    ) -> Optional[str]:
        """Gets the request ID for the Google Ads API request.

        Args:
            trailing_metadata: a tuple of metadatum from the service response.

        Returns:
            A str request ID associated with the Google Ads API request, or None
            if it doesn't exist.
        """
        for kv in trailing_metadata:
            if kv[0] == _REQUEST_ID_KEY:
                return kv[1]

        return None

    @classmethod
    def parse_metadata_to_json(cls, metadata: Optional[MetadataType]) -> str:
        """Parses metadata from gRPC request and response messages to a JSON str.

        Obscures the value for "developer-token".

        Args:
            metadata: a tuple of metadatum.

        Returns:
            A str of metadata formatted as JSON key/value pairs.
        """
        metadata_dict: Dict = {}

        if metadata is None:
            return "{}"

        for datum in metadata:
            key: str = datum[0]
            if key == "developer-token":
                metadata_dict[key] = cls._SENSITIVE_INFO_MASK
            else:
                value: AnyStr = datum[1]
                metadata_dict[key] = value

        return cls.format_json_object(metadata_dict)

    @classmethod
    def format_json_object(cls, obj: Dict) -> str:
        """Parses a serializable object into a consistently formatted JSON string.

        Returns:
            A str of formatted JSON serialized from the given object.

        Args:
            obj: an object or dict.

        Returns:
            A str of metadata formatted as JSON key/value pairs.
        """
        def default_serializer(value: Any) -> Optional[str]:
            if isinstance(value, bytes):
                return value.decode(errors="ignore")
            else:
                return None

        return str(
            json.dumps(
                obj,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
                default=default_serializer,
                separators=(",", ": "),
            )
        )

    @classmethod
    def get_trailing_metadata_from_interceptor_exception(
        cls, exception: grpc.RpcError
    ) -> MetadataType:
        """Retrieves trailing metadata from an exception object.

        Args:
            exception: an instance of grpc.Call.

        Returns:
            A tuple of trailing metadata key value pairs.
        """
        try:
            # GoogleAdsFailure exceptions will contain trailing metadata on the
            # error attribute.
            return exception.error.trailing_metadata()
        except AttributeError:
            try:
                # Transport failures, i.e. issues at the gRPC layer, will contain
                # trailing metadata on the exception itself.
                return exception.trailing_metadata()
            except AttributeError:
                # if trailing metadata is not found in either location then
                # return an empty tuple
                return tuple()

    @classmethod
    def get_client_call_details_instance(
        cls,
        method: str,
        timeout: float,
        metadata: MetadataType,
        credentials: Optional[grpc.CallCredentials] = None,
        wait_for_ready: Optional[bool] = None,
    ) -> grpc.ClientCallDetails:
        """Initializes an instance of the ClientCallDetails with the given data.

        Args:
            method: A str of the service method being invoked.
            timeout: A float of the request timeout
            metadata: A list of metadata tuples
            credentials: An optional grpc.CallCredentials instance for the RPC
            wait_for_ready: An optional bool indicating if the RPC should wait
                for the channel to be ready.

        Returns:
            An instance of _ClientCallDetails that wraps grpc.ClientCallDetails.
        """
        return cls._ClientCallDetails(
            method, timeout, metadata, credentials, wait_for_ready
        )

    def __init__(self, api_version: str):
        self._error_protos: Optional[ModuleType] = None
        self._failure_key: str = (
            f"google.ads.googleads.{api_version}.errors.googleadsfailure-bin"
        )
        self._api_version: str = api_version

    def _get_error_from_response(self, response: grpc.Call) -> grpc.Call:
        """Attempts to wrap failed responses as GoogleAdsException instances.

        Handles failed gRPC responses of by attempting to convert them
        to a more readable GoogleAdsException. Certain types of exceptions are
        not converted; if the object's trailing metadata does not indicate that
        it is a GoogleAdsException, or if it falls under a certain category of
        status code, (INTERNAL or RESOURCE_EXHAUSTED). See documentation for
        more information about gRPC status codes:
        https://github.com/grpc/grpc/blob/master/doc/statuscodes.md

        Args:
            response: a grpc.Call/grpc.Future instance.

        Returns:
            GoogleAdsException: If the exception's trailing metadata
                indicates that it is a GoogleAdsException.
            RpcError: If the exception's is a gRPC exception but the trailing
                metadata is empty or is not indicative of a GoogleAdsException,
                or if the exception has a status code of INTERNAL or
                RESOURCE_EXHAUSTED.
            Exception: If not a GoogleAdsException or RpcException the error
                will be raised as-is.
        """
        status_code: grpc.StatusCode = response.code()
        response_exception: grpc.Call = response.exception()

        if status_code not in _RETRY_STATUS_CODES:
            trailing_metadata: MetadataType = response.trailing_metadata()
            google_ads_failure: Optional[Message] = self._get_google_ads_failure(
                trailing_metadata
            )

            if google_ads_failure and response_exception:
                request_id: Optional[str] = self.get_request_id_from_metadata(
                    trailing_metadata
                )

                # If exception is a GoogleAdsFailure then it gets wrapped in a
                # library-specific Error type for easy handling. These errors
                # originate from the Google Ads API and are often caused by
                # invalid requests.
                return GoogleAdsException(
                    response_exception, response, google_ads_failure, request_id
                )
            elif response_exception:
                # Raise the original exception if not a GoogleAdsFailure. This
                # type of error is generally caused by problems at the request
                # level, such as when an invalid endpoint is given.
                return response_exception
        elif response_exception:
            # Raise the original exception if error has status code
            # INTERNAL or RESOURCE_EXHAUSTED, meaning that
            return response_exception
        raise response.exception()

    def _get_google_ads_failure(self, trailing_metadata: MetadataType) -> Optional[Message]:
        """Gets the Google Ads failure details if they exist.

        Args:
            trailing_metadata: a tuple of metadatum from the service response.

        Returns:
            A GoogleAdsFailure that describes how a GoogleAds API call failed.
            Returns None if either the trailing metadata of the request did not
            return the failure details, or if the GoogleAdsFailure fails to
            parse.
        """
        if trailing_metadata is not None:
            for kv in trailing_metadata:
                if kv[0] == self._failure_key:
                    try:
                        if not self._error_protos:
                            self._error_protos = import_module(
                                f"google.ads.googleads.{self._api_version}.errors."
                                "types.errors"
                            )

                        return self._error_protos.GoogleAdsFailure.deserialize(
                            kv[1]
                        )
                    except DecodeError:
                        return None

        return None

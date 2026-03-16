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
"""A gRPC Interceptor that is responsible to augmenting request metadata.

This class is initialized in the GoogleAdsClient and passed into a grpc
intercept_channel whenever a new service is initialized. It intercepts requests
and updates the metadata in order to insert the developer token and
login-customer-id values.
"""

# TODO: Explicitly importing the protobuf package version here should be removed
# once the below issue is resolved, and the protobuf version is added to the
# request user-agent directly by the google-api-core package:
# https://github.com/googleapis/python-api-core/issues/416
from importlib import metadata
from typing import List, Optional, Tuple, Union

from google.protobuf.internal import api_implementation
from google.protobuf.message import Message as ProtobufMessageType
import grpc

from google.ads.googleads.interceptors import Interceptor, MetadataType, ContinuationType


# Determine which version of the package is installed.
try:
    _PROTOBUF_VERSION = metadata.version("protobuf")
except metadata.PackageNotFoundError:
    _PROTOBUF_VERSION = None

# Determine which protobuf implementation is being used.
if api_implementation.Type() == "cpp":
    _PB_IMPL_HEADER = "+c"
elif api_implementation.Type() == "python":
    _PB_IMPL_HEADER = "+n"
else:
    _PB_IMPL_HEADER = ""

class MetadataInterceptor(
    Interceptor,
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor
):
    """An interceptor that appends custom metadata to requests."""

    def __init__(
        self,
        developer_token: str,
        login_customer_id: Optional[str]= None,
        linked_customer_id: Optional[str] = None,
        use_cloud_org_for_api_access: Optional[bool] = None,
        gaada: Optional[str] = None,
    ):
        """Initialization method for this class.

        Args:
            developer_token: a str developer token.
            login_customer_id: a str specifying a login customer ID.
            linked_customer_id: a str specifying a linked customer ID.
            use_cloud_org_for_api_access: a str specifying whether to use the
                Google Cloud Organization of your Google Cloud project instead
                of developer token to determine your Google Ads API access
                levels. Use this flag only if you are enrolled into a limited
                pilot that supports this configuration
            gaada: a str specifying the Google Ads API Assistant version.
        """
        self.developer_token_meta: Tuple[str, str] = (
            "developer-token",
            developer_token,
        )
        self.login_customer_id_meta: Optional[Tuple[str, str]] = (
            ("login-customer-id", login_customer_id)
            if login_customer_id
            else None
        )
        self.linked_customer_id_meta: Optional[Tuple[str, str]] = (
            ("linked-customer-id", linked_customer_id)
            if linked_customer_id
            else None
        )
        self.gaada: Optional[str] = gaada
        self.use_cloud_org_for_api_access: Optional[
            bool
        ] = use_cloud_org_for_api_access

    def _update_client_call_details_metadata(
        self, client_call_details: grpc.ClientCallDetails, metadata: MetadataType
    ):
        """Updates the client call details with additional metadata.

        Args:
            client_call_details: An instance of grpc.ClientCallDetails.
            metadata: Additional metadata defined by GoogleAdsClient.

        Returns:
            An new instance of grpc.ClientCallDetails with additional metadata
            from the GoogleAdsClient.
        """
        client_call_details: grpc.ClientCallDetails = self.get_client_call_details_instance(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            getattr(client_call_details, "wait_for_ready", None),
        )

        return client_call_details

    def _intercept(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        """Generic interceptor used for Unary-Unary and Unary-Stream requests.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call/grpc.Future instance representing a service response.
        """
        if client_call_details.metadata is None:
            metadata: MetadataType = []
        else:
            metadata: MetadataType = list(client_call_details.metadata)

        # If self.use_cloud_org_for_api_access is not True, add the developer
        # token to the request's metadata
        if not self.use_cloud_org_for_api_access:
            metadata.append(self.developer_token_meta)

        if self.login_customer_id_meta:
            metadata.append(self.login_customer_id_meta)

        if self.linked_customer_id_meta:
            metadata.append(self.linked_customer_id_meta)

        # TODO: This logic should be updated or removed once the following is
        # fixed: https://github.com/googleapis/python-api-core/issues/416
        for i, metadatum_tuple in enumerate(metadata):
            # Check if the user agent header key is in the current metadatum
            if "x-goog-api-client" in metadatum_tuple:
                metadatum: List[str] = list(metadatum_tuple)

                if self.gaada:
                    metadatum[1] += f" gaada/{self.gaada}"

                if _PROTOBUF_VERSION:
                    # Convert the tuple to a list so it can be modified.
                    # Check that "pb" isn't already included in the user agent.
                    if "pb" not in metadatum[1]:
                        # Append the protobuf version key value pair to the end of
                        # the string.
                        metadatum[1] += f" pb/{_PROTOBUF_VERSION}{_PB_IMPL_HEADER}"
                        # Convert the metadatum back to a tuple.
                        metadatum_tuple: Tuple[str, str] = tuple(metadatum)

                # Splice the metadatum back in its original position in
                # order to preserve the order of the metadata list.
                metadata[i] = metadatum_tuple
                # Exit the loop since we already found the user agent.
                break


        client_call_details: grpc.ClientCallDetails = self._update_client_call_details_metadata(
            client_call_details, metadata
        )

        return continuation(client_call_details, request)

    def intercept_unary_unary(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        """Intercepts and appends custom metadata for Unary-Unary requests.

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
        return self._intercept(continuation, client_call_details, request)

    def intercept_unary_stream(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        """Intercepts and appends custom metadata to Unary-Stream requests.

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
        return self._intercept(continuation, client_call_details, request)


class _AsyncMetadataInterceptor(
    MetadataInterceptor
):
    """An interceptor that appends custom metadata to requests."""

    async def _intercept(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        """Generic interceptor used for Unary-Unary and Unary-Stream requests.

        Args:
            continuation: a function to continue the request process.
            client_call_details: a grpc._interceptor._ClientCallDetails
                instance containing request metadata.
            request: a SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest
                message class instance.

        Returns:
            A grpc.Call/grpc.Future instance representing a service response.
        """
        if client_call_details.metadata is None:
            metadata: MetadataType = []
        else:
            metadata: MetadataType = list(client_call_details.metadata)

        # If self.use_cloud_org_for_api_access is not True, add the developer
        # token to the request's metadata
        if not self.use_cloud_org_for_api_access:
            metadata.append(self.developer_token_meta)

        if self.login_customer_id_meta:
            metadata.append(self.login_customer_id_meta)

        if self.linked_customer_id_meta:
            metadata.append(self.linked_customer_id_meta)

        # TODO: This logic should be updated or removed once the following is
        # fixed: https://github.com/googleapis/python-api-core/issues/416
        for i, metadatum_tuple in enumerate(metadata):
            # Check if the user agent header key is in the current metadatum
            if "x-goog-api-client" in metadatum_tuple and _PROTOBUF_VERSION:
                # Convert the tuple to a list so it can be modified.
                metadatum: List[str] = list(metadatum_tuple)
                # Check that "pb" isn't already included in the user agent.
                if "pb" not in metadatum[1]:
                    # Append the protobuf version key value pair to the end of
                    # the string.
                    metadatum[1] += f" pb/{_PROTOBUF_VERSION}{_PB_IMPL_HEADER}"
                    # Convert the metadatum back to a tuple.
                    metadatum_tuple: Tuple[str, str] = tuple(metadatum)
                    # Splice the metadatum back in its original position in
                    # order to preserve the order of the metadata list.
                    metadata[i] = metadatum_tuple
                    # Exit the loop since we already found the user agent.
                    break

        client_call_details: grpc.ClientCallDetails = (
            self._update_client_call_details_metadata(
                client_call_details, metadata
            )
        )

        return await continuation(client_call_details, request)

    async def intercept_unary_unary(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        """Intercepts and appends custom metadata for Unary-Unary requests.

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
        return await self._intercept(continuation, client_call_details, request)

    async def intercept_unary_stream(
        self,
        continuation: ContinuationType,
        client_call_details: grpc.ClientCallDetails,
        request: ProtobufMessageType,
    ) -> Union[grpc.Call, grpc.Future]:
        """Intercepts and appends custom metadata to Unary-Stream requests.

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
        return await self._intercept(continuation, client_call_details, request)

class AsyncUnaryUnaryMetadataInterceptor(
    _AsyncMetadataInterceptor,
    grpc.aio.UnaryUnaryClientInterceptor,
):
    """An interceptor that appends custom metadata to Unary-Unary requests."""

class AsyncUnaryStreamMetadataInterceptor(
    _AsyncMetadataInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
):
    """An interceptor that appends custom metadata to Unary-Stream requests."""

__all__ = [
    "MetadataInterceptor",
    "AsyncUnaryUnaryMetadataInterceptor",
    "AsyncUnaryStreamMetadataInterceptor",
]
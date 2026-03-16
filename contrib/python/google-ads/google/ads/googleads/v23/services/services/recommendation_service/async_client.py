# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
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
#
import logging as std_logging
from typing import Callable, MutableSequence, Optional, Sequence, Tuple, Union

from google.ads.googleads.v23 import gapic_version as package_version

from google.api_core.client_options import ClientOptions
from google.api_core import gapic_v1
from google.api_core import retry_async as retries
from google.auth import credentials as ga_credentials  # type: ignore
from google.oauth2 import service_account  # type: ignore
import google.protobuf


try:
    OptionalRetry = Union[
        retries.AsyncRetry, gapic_v1.method._MethodDefault, None
    ]
except AttributeError:  # pragma: NO COVER
    OptionalRetry = Union[retries.AsyncRetry, object, None]  # type: ignore

from google.ads.googleads.v23.enums.types import (
    advertising_channel_type as gage_advertising_channel_type,
)
from google.ads.googleads.v23.enums.types import recommendation_type
from google.ads.googleads.v23.services.types import recommendation_service
from google.rpc import status_pb2  # type: ignore
from .transports.base import RecommendationServiceTransport, DEFAULT_CLIENT_INFO
from .client import RecommendationServiceClient

try:
    from google.api_core import client_logging  # type: ignore

    CLIENT_LOGGING_SUPPORTED = True  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    CLIENT_LOGGING_SUPPORTED = False

_LOGGER = std_logging.getLogger(__name__)


class RecommendationServiceAsyncClient:
    """Service to manage recommendations."""

    _client: RecommendationServiceClient

    # Copy defaults from the synchronous client for use here.
    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = RecommendationServiceClient.DEFAULT_ENDPOINT
    DEFAULT_MTLS_ENDPOINT = RecommendationServiceClient.DEFAULT_MTLS_ENDPOINT
    _DEFAULT_ENDPOINT_TEMPLATE = (
        RecommendationServiceClient._DEFAULT_ENDPOINT_TEMPLATE
    )
    _DEFAULT_UNIVERSE = RecommendationServiceClient._DEFAULT_UNIVERSE

    ad_path = staticmethod(RecommendationServiceClient.ad_path)
    parse_ad_path = staticmethod(RecommendationServiceClient.parse_ad_path)
    ad_group_path = staticmethod(RecommendationServiceClient.ad_group_path)
    parse_ad_group_path = staticmethod(
        RecommendationServiceClient.parse_ad_group_path
    )
    asset_path = staticmethod(RecommendationServiceClient.asset_path)
    parse_asset_path = staticmethod(
        RecommendationServiceClient.parse_asset_path
    )
    campaign_path = staticmethod(RecommendationServiceClient.campaign_path)
    parse_campaign_path = staticmethod(
        RecommendationServiceClient.parse_campaign_path
    )
    campaign_budget_path = staticmethod(
        RecommendationServiceClient.campaign_budget_path
    )
    parse_campaign_budget_path = staticmethod(
        RecommendationServiceClient.parse_campaign_budget_path
    )
    conversion_action_path = staticmethod(
        RecommendationServiceClient.conversion_action_path
    )
    parse_conversion_action_path = staticmethod(
        RecommendationServiceClient.parse_conversion_action_path
    )
    recommendation_path = staticmethod(
        RecommendationServiceClient.recommendation_path
    )
    parse_recommendation_path = staticmethod(
        RecommendationServiceClient.parse_recommendation_path
    )
    common_billing_account_path = staticmethod(
        RecommendationServiceClient.common_billing_account_path
    )
    parse_common_billing_account_path = staticmethod(
        RecommendationServiceClient.parse_common_billing_account_path
    )
    common_folder_path = staticmethod(
        RecommendationServiceClient.common_folder_path
    )
    parse_common_folder_path = staticmethod(
        RecommendationServiceClient.parse_common_folder_path
    )
    common_organization_path = staticmethod(
        RecommendationServiceClient.common_organization_path
    )
    parse_common_organization_path = staticmethod(
        RecommendationServiceClient.parse_common_organization_path
    )
    common_project_path = staticmethod(
        RecommendationServiceClient.common_project_path
    )
    parse_common_project_path = staticmethod(
        RecommendationServiceClient.parse_common_project_path
    )
    common_location_path = staticmethod(
        RecommendationServiceClient.common_location_path
    )
    parse_common_location_path = staticmethod(
        RecommendationServiceClient.parse_common_location_path
    )

    @classmethod
    def from_service_account_info(cls, info: dict, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
            info.

        Args:
            info (dict): The service account private key info.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            RecommendationServiceAsyncClient: The constructed client.
        """
        return RecommendationServiceClient.from_service_account_info.__func__(RecommendationServiceAsyncClient, info, *args, **kwargs)  # type: ignore

    @classmethod
    def from_service_account_file(cls, filename: str, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
            file.

        Args:
            filename (str): The path to the service account private key json
                file.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            RecommendationServiceAsyncClient: The constructed client.
        """
        return RecommendationServiceClient.from_service_account_file.__func__(RecommendationServiceAsyncClient, filename, *args, **kwargs)  # type: ignore

    from_service_account_json = from_service_account_file

    @classmethod
    def get_mtls_endpoint_and_cert_source(
        cls, client_options: Optional[ClientOptions] = None
    ):
        """Return the API endpoint and client cert source for mutual TLS.

        The client cert source is determined in the following order:
        (1) if `GOOGLE_API_USE_CLIENT_CERTIFICATE` environment variable is not "true", the
        client cert source is None.
        (2) if `client_options.client_cert_source` is provided, use the provided one; if the
        default client cert source exists, use the default one; otherwise the client cert
        source is None.

        The API endpoint is determined in the following order:
        (1) if `client_options.api_endpoint` if provided, use the provided one.
        (2) if `GOOGLE_API_USE_CLIENT_CERTIFICATE` environment variable is "always", use the
        default mTLS endpoint; if the environment variable is "never", use the default API
        endpoint; otherwise if client cert source exists, use the default mTLS endpoint, otherwise
        use the default API endpoint.

        More details can be found at https://google.aip.dev/auth/4114.

        Args:
            client_options (google.api_core.client_options.ClientOptions): Custom options for the
                client. Only the `api_endpoint` and `client_cert_source` properties may be used
                in this method.

        Returns:
            Tuple[str, Callable[[], Tuple[bytes, bytes]]]: returns the API endpoint and the
                client cert source to use.

        Raises:
            google.auth.exceptions.MutualTLSChannelError: If any errors happen.
        """
        return RecommendationServiceClient.get_mtls_endpoint_and_cert_source(client_options)  # type: ignore

    @property
    def transport(self) -> RecommendationServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            RecommendationServiceTransport: The transport used by the client instance.
        """
        return self._client.transport

    @property
    def api_endpoint(self):
        """Return the API endpoint used by the client instance.

        Returns:
            str: The API endpoint used by the client instance.
        """
        return self._client._api_endpoint

    @property
    def universe_domain(self) -> str:
        """Return the universe domain used by the client instance.

        Returns:
            str: The universe domain used
                by the client instance.
        """
        return self._client._universe_domain

    get_transport_class = RecommendationServiceClient.get_transport_class

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[
                str,
                RecommendationServiceTransport,
                Callable[..., RecommendationServiceTransport],
            ]
        ] = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the recommendation service async client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,RecommendationServiceTransport,Callable[..., RecommendationServiceTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport to use.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the RecommendationServiceTransport constructor.
                If set to None, a transport is chosen automatically.
            client_options (Optional[Union[google.api_core.client_options.ClientOptions, dict]]):
                Custom options for the client.

                1. The ``api_endpoint`` property can be used to override the
                default endpoint provided by the client when ``transport`` is
                not explicitly provided. Only if this property is not set and
                ``transport`` was not explicitly provided, the endpoint is
                determined by the GOOGLE_API_USE_MTLS_ENDPOINT environment
                variable, which have one of the following values:
                "always" (always use the default mTLS endpoint), "never" (always
                use the default regular endpoint) and "auto" (auto-switch to the
                default mTLS endpoint if client certificate is present; this is
                the default value).

                2. If the GOOGLE_API_USE_CLIENT_CERTIFICATE environment variable
                is "true", then the ``client_cert_source`` property can be used
                to provide a client certificate for mTLS transport. If
                not provided, the default SSL client certificate will be used if
                present. If GOOGLE_API_USE_CLIENT_CERTIFICATE is "false" or not
                set, no client certificate will be used.

                3. The ``universe_domain`` property can be used to override the
                default "googleapis.com" universe. Note that ``api_endpoint``
                property still takes precedence; and ``universe_domain`` is
                currently not supported for mTLS.

            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you're developing
                your own client library.

        Raises:
            google.auth.exceptions.MutualTlsChannelError: If mutual TLS transport
                creation failed for any reason.
        """
        self._client = RecommendationServiceClient(
            credentials=credentials,
            transport=transport,
            client_options=client_options,
            client_info=client_info,
        )

        if CLIENT_LOGGING_SUPPORTED and _LOGGER.isEnabledFor(
            std_logging.DEBUG
        ):  # pragma: NO COVER
            _LOGGER.debug(
                "Created client `google.ads.googleads.v23.services.RecommendationServiceAsyncClient`.",
                extra=(
                    {
                        "serviceName": "google.ads.googleads.v23.services.RecommendationService",
                        "universeDomain": getattr(
                            self._client._transport._credentials,
                            "universe_domain",
                            "",
                        ),
                        "credentialsType": f"{type(self._client._transport._credentials).__module__}.{type(self._client._transport._credentials).__qualname__}",
                        "credentialsInfo": getattr(
                            self.transport._credentials,
                            "get_cred_info",
                            lambda: None,
                        )(),
                    }
                    if hasattr(self._client._transport, "_credentials")
                    else {
                        "serviceName": "google.ads.googleads.v23.services.RecommendationService",
                        "credentialsType": None,
                    }
                ),
            )

    async def apply_recommendation(
        self,
        request: Optional[
            Union[recommendation_service.ApplyRecommendationRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        operations: Optional[
            MutableSequence[recommendation_service.ApplyRecommendationOperation]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> recommendation_service.ApplyRecommendationResponse:
        r"""Applies given recommendations with corresponding apply
        parameters.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `DatabaseError <>`__ `FieldError <>`__
        `HeaderError <>`__ `InternalError <>`__ `MutateError <>`__
        `QuotaError <>`__ `RecommendationError <>`__ `RequestError <>`__
        `UrlFieldError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ApplyRecommendationRequest, dict]]):
                The request object. Request message for
                [RecommendationService.ApplyRecommendation][google.ads.googleads.v23.services.RecommendationService.ApplyRecommendation].
            customer_id (:class:`str`):
                Required. The ID of the customer with
                the recommendation.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            operations (:class:`MutableSequence[google.ads.googleads.v23.services.types.ApplyRecommendationOperation]`):
                Required. The list of operations to apply
                recommendations. If partial_failure=false all
                recommendations should be of the same type There is a
                limit of 100 operations per request.

                This corresponds to the ``operations`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.ApplyRecommendationResponse:
                Response message for
                   [RecommendationService.ApplyRecommendation][google.ads.googleads.v23.services.RecommendationService.ApplyRecommendation].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, operations]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, recommendation_service.ApplyRecommendationRequest
        ):
            request = recommendation_service.ApplyRecommendationRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if operations:
            request.operations.extend(operations)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.apply_recommendation
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._client._validate_universe_domain()

        # Send the request.
        response = await rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    async def dismiss_recommendation(
        self,
        request: Optional[
            Union[recommendation_service.DismissRecommendationRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        operations: Optional[
            MutableSequence[
                recommendation_service.DismissRecommendationRequest.DismissRecommendationOperation
            ]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> recommendation_service.DismissRecommendationResponse:
        r"""Dismisses given recommendations.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__
        `RecommendationError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.DismissRecommendationRequest, dict]]):
                The request object. Request message for
                [RecommendationService.DismissRecommendation][google.ads.googleads.v23.services.RecommendationService.DismissRecommendation].
            customer_id (:class:`str`):
                Required. The ID of the customer with
                the recommendation.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            operations (:class:`MutableSequence[google.ads.googleads.v23.services.types.DismissRecommendationRequest.DismissRecommendationOperation]`):
                Required. The list of operations to dismiss
                recommendations. If partial_failure=false all
                recommendations should be of the same type There is a
                limit of 100 operations per request.

                This corresponds to the ``operations`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.DismissRecommendationResponse:
                Response message for
                   [RecommendationService.DismissRecommendation][google.ads.googleads.v23.services.RecommendationService.DismissRecommendation].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, operations]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, recommendation_service.DismissRecommendationRequest
        ):
            request = recommendation_service.DismissRecommendationRequest(
                request
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if operations:
            request.operations.extend(operations)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.dismiss_recommendation
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._client._validate_universe_domain()

        # Send the request.
        response = await rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    async def generate_recommendations(
        self,
        request: Optional[
            Union[recommendation_service.GenerateRecommendationsRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        recommendation_types: Optional[
            MutableSequence[
                recommendation_type.RecommendationTypeEnum.RecommendationType
            ]
        ] = None,
        advertising_channel_type: Optional[
            gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> recommendation_service.GenerateRecommendationsResponse:
        r"""Generates Recommendations based off the requested
        recommendation_types.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__
        `RecommendationError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GenerateRecommendationsRequest, dict]]):
                The request object. Request message for
                [RecommendationService.GenerateRecommendations][google.ads.googleads.v23.services.RecommendationService.GenerateRecommendations].
            customer_id (:class:`str`):
                Required. The ID of the customer
                generating recommendations.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            recommendation_types (:class:`MutableSequence[google.ads.googleads.v23.enums.types.RecommendationTypeEnum.RecommendationType]`):
                Required. List of eligible recommendation_types to
                generate. If the uploaded criteria isn't sufficient to
                make a recommendation, or the campaign is already in the
                recommended state, no recommendation will be returned
                for that type. Generally, a recommendation is returned
                if all required fields for that recommendation_type are
                uploaded, but there are cases where this is still not
                sufficient.

                The following recommendation_types are supported for
                recommendation generation: CAMPAIGN_BUDGET, KEYWORD,
                MAXIMIZE_CLICKS_OPT_IN, MAXIMIZE_CONVERSIONS_OPT_IN,
                MAXIMIZE_CONVERSION_VALUE_OPT_IN, SET_TARGET_CPA,
                SET_TARGET_ROAS, SITELINK_ASSET, TARGET_CPA_OPT_IN,
                TARGET_ROAS_OPT_IN

                This corresponds to the ``recommendation_types`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            advertising_channel_type (:class:`google.ads.googleads.v23.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType`):
                Required. Advertising channel type of the campaign. The
                following advertising_channel_types are supported for
                recommendation generation: PERFORMANCE_MAX and SEARCH

                This corresponds to the ``advertising_channel_type`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.GenerateRecommendationsResponse:
                Response message for
                   [RecommendationService.GenerateRecommendations][google.ads.googleads.v23.services.RecommendationService.GenerateRecommendations].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [
            customer_id,
            recommendation_types,
            advertising_channel_type,
        ]
        has_flattened_params = (
            len([param for param in flattened_params if param is not None]) > 0
        )
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, recommendation_service.GenerateRecommendationsRequest
        ):
            request = recommendation_service.GenerateRecommendationsRequest(
                request
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if advertising_channel_type is not None:
            request.advertising_channel_type = advertising_channel_type
        if recommendation_types:
            request.recommendation_types.extend(recommendation_types)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.generate_recommendations
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("customer_id", request.customer_id),)
            ),
        )

        # Validate the universe domain.
        self._client._validate_universe_domain()

        # Send the request.
        response = await rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    async def __aenter__(self) -> "RecommendationServiceAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__


__all__ = ("RecommendationServiceAsyncClient",)

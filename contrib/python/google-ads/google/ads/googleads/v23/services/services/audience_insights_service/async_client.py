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

from google.ads.googleads.v23.common.types import audience_insights_attribute
from google.ads.googleads.v23.common.types import criteria
from google.ads.googleads.v23.enums.types import audience_insights_dimension
from google.ads.googleads.v23.services.types import audience_insights_service
from .transports.base import (
    AudienceInsightsServiceTransport,
    DEFAULT_CLIENT_INFO,
)
from .client import AudienceInsightsServiceClient

try:
    from google.api_core import client_logging  # type: ignore

    CLIENT_LOGGING_SUPPORTED = True  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    CLIENT_LOGGING_SUPPORTED = False

_LOGGER = std_logging.getLogger(__name__)


class AudienceInsightsServiceAsyncClient:
    """Audience Insights Service helps users find information about
    groups of people and how they can be reached with Google Ads.
    Accessible to allowlisted customers only.
    """

    _client: AudienceInsightsServiceClient

    # Copy defaults from the synchronous client for use here.
    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = AudienceInsightsServiceClient.DEFAULT_ENDPOINT
    DEFAULT_MTLS_ENDPOINT = AudienceInsightsServiceClient.DEFAULT_MTLS_ENDPOINT
    _DEFAULT_ENDPOINT_TEMPLATE = (
        AudienceInsightsServiceClient._DEFAULT_ENDPOINT_TEMPLATE
    )
    _DEFAULT_UNIVERSE = AudienceInsightsServiceClient._DEFAULT_UNIVERSE

    common_billing_account_path = staticmethod(
        AudienceInsightsServiceClient.common_billing_account_path
    )
    parse_common_billing_account_path = staticmethod(
        AudienceInsightsServiceClient.parse_common_billing_account_path
    )
    common_folder_path = staticmethod(
        AudienceInsightsServiceClient.common_folder_path
    )
    parse_common_folder_path = staticmethod(
        AudienceInsightsServiceClient.parse_common_folder_path
    )
    common_organization_path = staticmethod(
        AudienceInsightsServiceClient.common_organization_path
    )
    parse_common_organization_path = staticmethod(
        AudienceInsightsServiceClient.parse_common_organization_path
    )
    common_project_path = staticmethod(
        AudienceInsightsServiceClient.common_project_path
    )
    parse_common_project_path = staticmethod(
        AudienceInsightsServiceClient.parse_common_project_path
    )
    common_location_path = staticmethod(
        AudienceInsightsServiceClient.common_location_path
    )
    parse_common_location_path = staticmethod(
        AudienceInsightsServiceClient.parse_common_location_path
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
            AudienceInsightsServiceAsyncClient: The constructed client.
        """
        return AudienceInsightsServiceClient.from_service_account_info.__func__(AudienceInsightsServiceAsyncClient, info, *args, **kwargs)  # type: ignore

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
            AudienceInsightsServiceAsyncClient: The constructed client.
        """
        return AudienceInsightsServiceClient.from_service_account_file.__func__(AudienceInsightsServiceAsyncClient, filename, *args, **kwargs)  # type: ignore

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
        return AudienceInsightsServiceClient.get_mtls_endpoint_and_cert_source(client_options)  # type: ignore

    @property
    def transport(self) -> AudienceInsightsServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            AudienceInsightsServiceTransport: The transport used by the client instance.
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

    get_transport_class = AudienceInsightsServiceClient.get_transport_class

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[
                str,
                AudienceInsightsServiceTransport,
                Callable[..., AudienceInsightsServiceTransport],
            ]
        ] = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the audience insights service async client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,AudienceInsightsServiceTransport,Callable[..., AudienceInsightsServiceTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport to use.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the AudienceInsightsServiceTransport constructor.
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
        self._client = AudienceInsightsServiceClient(
            credentials=credentials,
            transport=transport,
            client_options=client_options,
            client_info=client_info,
        )

        if CLIENT_LOGGING_SUPPORTED and _LOGGER.isEnabledFor(
            std_logging.DEBUG
        ):  # pragma: NO COVER
            _LOGGER.debug(
                "Created client `google.ads.googleads.v23.services.AudienceInsightsServiceAsyncClient`.",
                extra=(
                    {
                        "serviceName": "google.ads.googleads.v23.services.AudienceInsightsService",
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
                        "serviceName": "google.ads.googleads.v23.services.AudienceInsightsService",
                        "credentialsType": None,
                    }
                ),
            )

    async def generate_insights_finder_report(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateInsightsFinderReportRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        baseline_audience: Optional[
            audience_insights_service.InsightsAudience
        ] = None,
        specific_audience: Optional[
            audience_insights_service.InsightsAudience
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateInsightsFinderReportResponse:
        r"""Creates a saved report that can be viewed in the Insights Finder
        tool.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GenerateInsightsFinderReportRequest, dict]]):
                The request object. Request message for
                [AudienceInsightsService.GenerateInsightsFinderReport][google.ads.googleads.v23.services.AudienceInsightsService.GenerateInsightsFinderReport].
            customer_id (:class:`str`):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            baseline_audience (:class:`google.ads.googleads.v23.services.types.InsightsAudience`):
                Required. A baseline audience for
                this report, typically all people in a
                region.

                This corresponds to the ``baseline_audience`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            specific_audience (:class:`google.ads.googleads.v23.services.types.InsightsAudience`):
                Required. The specific audience of
                interest for this report.  The insights
                in the report will be based on
                attributes more prevalent in this
                audience than in the report's baseline
                audience.

                This corresponds to the ``specific_audience`` field
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
            google.ads.googleads.v23.services.types.GenerateInsightsFinderReportResponse:
                The response message for
                   [AudienceInsightsService.GenerateInsightsFinderReport][google.ads.googleads.v23.services.AudienceInsightsService.GenerateInsightsFinderReport],
                   containing the shareable URL for the report.

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, baseline_audience, specific_audience]
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
            request,
            audience_insights_service.GenerateInsightsFinderReportRequest,
        ):
            request = (
                audience_insights_service.GenerateInsightsFinderReportRequest(
                    request
                )
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if baseline_audience is not None:
            request.baseline_audience = baseline_audience
        if specific_audience is not None:
            request.specific_audience = specific_audience

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.generate_insights_finder_report
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

    async def list_audience_insights_attributes(
        self,
        request: Optional[
            Union[
                audience_insights_service.ListAudienceInsightsAttributesRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        dimensions: Optional[
            MutableSequence[
                audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
            ]
        ] = None,
        query_text: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.ListAudienceInsightsAttributesResponse:
        r"""Searches for audience attributes that can be used to generate
        insights.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ListAudienceInsightsAttributesRequest, dict]]):
                The request object. Request message for
                [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v23.services.AudienceInsightsService.ListAudienceInsightsAttributes].
            customer_id (:class:`str`):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            dimensions (:class:`MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]`):
                Required. The types of attributes to be returned.
                Supported dimensions are CATEGORY, KNOWLEDGE_GRAPH,
                DEVICE, GEO_TARGET_COUNTRY, SUB_COUNTRY_LOCATION,
                YOUTUBE_LINEUP, AFFINITY_USER_INTEREST,
                IN_MARKET_USER_INTEREST, LIFE_EVENT_USER_INTEREST,
                PARENTAL_STATUS, INCOME_RANGE, AGE_RANGE, and GENDER.

                This corresponds to the ``dimensions`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            query_text (:class:`str`):
                Required. A free text query. If the requested dimensions
                include Attributes CATEGORY or KNOWLEDGE_GRAPH, then the
                attributes returned for those dimensions will match or
                be related to this string. For other dimensions, this
                field is ignored and all available attributes are
                returned.

                This corresponds to the ``query_text`` field
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
            google.ads.googleads.v23.services.types.ListAudienceInsightsAttributesResponse:
                Response message for
                   [AudienceInsightsService.ListAudienceInsightsAttributes][google.ads.googleads.v23.services.AudienceInsightsService.ListAudienceInsightsAttributes].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, dimensions, query_text]
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
            request,
            audience_insights_service.ListAudienceInsightsAttributesRequest,
        ):
            request = (
                audience_insights_service.ListAudienceInsightsAttributesRequest(
                    request
                )
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if query_text is not None:
            request.query_text = query_text
        if dimensions:
            request.dimensions.extend(dimensions)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.list_audience_insights_attributes
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

    async def list_insights_eligible_dates(
        self,
        request: Optional[
            Union[
                audience_insights_service.ListInsightsEligibleDatesRequest, dict
            ]
        ] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.ListInsightsEligibleDatesResponse:
        r"""Lists date ranges for which audience insights data can be
        requested.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ListInsightsEligibleDatesRequest, dict]]):
                The request object. Request message for
                [AudienceInsightsService.ListInsightsEligibleDates][google.ads.googleads.v23.services.AudienceInsightsService.ListInsightsEligibleDates].
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.ListInsightsEligibleDatesResponse:
                Response message for
                   [AudienceInsightsService.ListInsightsEligibleDates][google.ads.googleads.v23.services.AudienceInsightsService.ListInsightsEligibleDates].

        """
        # Create or coerce a protobuf request object.
        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, audience_insights_service.ListInsightsEligibleDatesRequest
        ):
            request = (
                audience_insights_service.ListInsightsEligibleDatesRequest(
                    request
                )
            )

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.list_insights_eligible_dates
        ]

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

    async def generate_audience_composition_insights(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateAudienceCompositionInsightsRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        audience: Optional[audience_insights_service.InsightsAudience] = None,
        dimensions: Optional[
            MutableSequence[
                audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
            ]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateAudienceCompositionInsightsResponse:
        r"""Returns a collection of attributes that are represented in an
        audience of interest, with metrics that compare each attribute's
        share of the audience with its share of a baseline audience.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GenerateAudienceCompositionInsightsRequest, dict]]):
                The request object. Request message for
                [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceCompositionInsights].
            customer_id (:class:`str`):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            audience (:class:`google.ads.googleads.v23.services.types.InsightsAudience`):
                Required. The audience of interest
                for which insights are being requested.

                This corresponds to the ``audience`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            dimensions (:class:`MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]`):
                Required. The audience dimensions for which composition
                insights should be returned. Supported dimensions are
                KNOWLEDGE_GRAPH, GEO_TARGET_COUNTRY,
                SUB_COUNTRY_LOCATION, YOUTUBE_CHANNEL, YOUTUBE_LINEUP,
                AFFINITY_USER_INTEREST, IN_MARKET_USER_INTEREST,
                LIFE_EVENT_USER_INTEREST, PARENTAL_STATUS, INCOME_RANGE,
                AGE_RANGE, and GENDER.

                This corresponds to the ``dimensions`` field
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
            google.ads.googleads.v23.services.types.GenerateAudienceCompositionInsightsResponse:
                Response message for
                   [AudienceInsightsService.GenerateAudienceCompositionInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceCompositionInsights].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, audience, dimensions]
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
            request,
            audience_insights_service.GenerateAudienceCompositionInsightsRequest,
        ):
            request = audience_insights_service.GenerateAudienceCompositionInsightsRequest(
                request
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if audience is not None:
            request.audience = audience
        if dimensions:
            request.dimensions.extend(dimensions)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.generate_audience_composition_insights
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

    async def generate_audience_definition(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateAudienceDefinitionRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        audience_description: Optional[
            audience_insights_service.InsightsAudienceDescription
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateAudienceDefinitionResponse:
        r"""Returns a collection of audience attributes using generative AI
        based on the provided audience description.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GenerateAudienceDefinitionRequest, dict]]):
                The request object. Request message for
                [AudienceInsightsService.GenerateAudienceDefinition][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceDefinition].
            customer_id (:class:`str`):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            audience_description (:class:`google.ads.googleads.v23.services.types.InsightsAudienceDescription`):
                Required. Provide a text description of an audience to
                get AI-generated structured suggestions. This can take
                around 5 or more seconds to complete Supported marketing
                objectives are: AWARENESS, CONSIDERATION and RESEARCH.
                Supported dimensions are: AGE_RANGE, GENDER,
                PARENTAL_STATUS, AFFINITY_USER_INTEREST,
                IN_MARKET_USER_INTEREST, LIFE_EVENT_USER_INTEREST,
                CATEGORY and KNOWLEDGE_GRAPH.

                This corresponds to the ``audience_description`` field
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
            google.ads.googleads.v23.services.types.GenerateAudienceDefinitionResponse:
                Response message for
                   [AudienceInsightsService.GenerateAudienceDefinition][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceDefinition].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, audience_description]
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
            request, audience_insights_service.GenerateAudienceDefinitionRequest
        ):
            request = (
                audience_insights_service.GenerateAudienceDefinitionRequest(
                    request
                )
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if audience_description is not None:
            request.audience_description = audience_description

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.generate_audience_definition
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

    async def generate_suggested_targeting_insights(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateSuggestedTargetingInsightsRequest,
                dict,
            ]
        ] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateSuggestedTargetingInsightsResponse:
        r"""Returns a collection of targeting insights (e.g. targetable
        audiences) that are relevant to the requested audience.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GenerateSuggestedTargetingInsightsRequest, dict]]):
                The request object. Request message for
                [AudienceInsightsService.GenerateSuggestedTargetingInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateSuggestedTargetingInsights].
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.GenerateSuggestedTargetingInsightsResponse:
                Response message for
                   [AudienceInsightsService.GenerateSuggestedTargetingInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateSuggestedTargetingInsights].

        """
        # Create or coerce a protobuf request object.
        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request,
            audience_insights_service.GenerateSuggestedTargetingInsightsRequest,
        ):
            request = audience_insights_service.GenerateSuggestedTargetingInsightsRequest(
                request
            )

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.generate_suggested_targeting_insights
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

    async def generate_audience_overlap_insights(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateAudienceOverlapInsightsRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        country_location: Optional[criteria.LocationInfo] = None,
        primary_attribute: Optional[
            audience_insights_attribute.AudienceInsightsAttribute
        ] = None,
        dimensions: Optional[
            MutableSequence[
                audience_insights_dimension.AudienceInsightsDimensionEnum.AudienceInsightsDimension
            ]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateAudienceOverlapInsightsResponse:
        r"""Returns a collection of audience attributes along with estimates
        of the overlap between their potential YouTube reach and that of
        a given input attribute.

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GenerateAudienceOverlapInsightsRequest, dict]]):
                The request object. Request message for
                [AudienceInsightsService.GenerateAudienceOverlapInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceOverlapInsights].
            customer_id (:class:`str`):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            country_location (:class:`google.ads.googleads.v23.common.types.LocationInfo`):
                Required. The country in which to
                calculate the sizes and overlaps of
                audiences.

                This corresponds to the ``country_location`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            primary_attribute (:class:`google.ads.googleads.v23.common.types.AudienceInsightsAttribute`):
                Required. The audience attribute that
                should be intersected with all other
                eligible audiences.  This must be an
                Affinity or In-Market UserInterest, an
                AgeRange or a Gender.

                This corresponds to the ``primary_attribute`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            dimensions (:class:`MutableSequence[google.ads.googleads.v23.enums.types.AudienceInsightsDimensionEnum.AudienceInsightsDimension]`):
                Required. The types of attributes of which to calculate
                the overlap with the primary_attribute. The values must
                be a subset of AFFINITY_USER_INTEREST,
                IN_MARKET_USER_INTEREST, AGE_RANGE and GENDER.

                This corresponds to the ``dimensions`` field
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
            google.ads.googleads.v23.services.types.GenerateAudienceOverlapInsightsResponse:
                Response message for
                   [AudienceInsightsService.GenerateAudienceOverlapInsights][google.ads.googleads.v23.services.AudienceInsightsService.GenerateAudienceOverlapInsights].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [
            customer_id,
            country_location,
            primary_attribute,
            dimensions,
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
            request,
            audience_insights_service.GenerateAudienceOverlapInsightsRequest,
        ):
            request = audience_insights_service.GenerateAudienceOverlapInsightsRequest(
                request
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if country_location is not None:
            request.country_location = country_location
        if primary_attribute is not None:
            request.primary_attribute = primary_attribute
        if dimensions:
            request.dimensions.extend(dimensions)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.generate_audience_overlap_insights
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

    async def generate_targeting_suggestion_metrics(
        self,
        request: Optional[
            Union[
                audience_insights_service.GenerateTargetingSuggestionMetricsRequest,
                dict,
            ]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        audiences: Optional[
            MutableSequence[audience_insights_service.InsightsAudience]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> audience_insights_service.GenerateTargetingSuggestionMetricsResponse:
        r"""Returns potential reach metrics for targetable audiences.

        This method helps answer questions like "How many Men aged 18+
        interested in Camping can be reached on YouTube?"

        List of thrown errors: `AudienceInsightsError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GenerateTargetingSuggestionMetricsRequest, dict]]):
                The request object. Request message for
                [AudienceInsightsService.GenerateTargetingSuggestionMetrics][google.ads.googleads.v23.services.AudienceInsightsService.GenerateTargetingSuggestionMetrics].
            customer_id (:class:`str`):
                Required. The ID of the customer.
                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            audiences (:class:`MutableSequence[google.ads.googleads.v23.services.types.InsightsAudience]`):
                Required. Audiences to request
                metrics for.

                This corresponds to the ``audiences`` field
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
            google.ads.googleads.v23.services.types.GenerateTargetingSuggestionMetricsResponse:
                Response message for
                   [AudienceInsightsService.GenerateTargetingSuggestionMetrics][google.ads.googleads.v23.services.AudienceInsightsService.GenerateTargetingSuggestionMetrics].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, audiences]
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
            request,
            audience_insights_service.GenerateTargetingSuggestionMetricsRequest,
        ):
            request = audience_insights_service.GenerateTargetingSuggestionMetricsRequest(
                request
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if audiences:
            request.audiences.extend(audiences)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.generate_targeting_suggestion_metrics
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

    async def __aenter__(self) -> "AudienceInsightsServiceAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__


__all__ = ("AudienceInsightsServiceAsyncClient",)

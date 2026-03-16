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

from google.ads.googleads.v23.common.types import criteria
from google.ads.googleads.v23.enums.types import benchmarks_source_type
from google.ads.googleads.v23.services.types import benchmarks_service
from .transports.base import BenchmarksServiceTransport, DEFAULT_CLIENT_INFO
from .client import BenchmarksServiceClient

try:
    from google.api_core import client_logging  # type: ignore

    CLIENT_LOGGING_SUPPORTED = True  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    CLIENT_LOGGING_SUPPORTED = False

_LOGGER = std_logging.getLogger(__name__)


class BenchmarksServiceAsyncClient:
    """BenchmarksService helps users compare YouTube advertisement
    data against industry benchmarks. Accessible to allowlisted
    customers only.
    """

    _client: BenchmarksServiceClient

    # Copy defaults from the synchronous client for use here.
    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = BenchmarksServiceClient.DEFAULT_ENDPOINT
    DEFAULT_MTLS_ENDPOINT = BenchmarksServiceClient.DEFAULT_MTLS_ENDPOINT
    _DEFAULT_ENDPOINT_TEMPLATE = (
        BenchmarksServiceClient._DEFAULT_ENDPOINT_TEMPLATE
    )
    _DEFAULT_UNIVERSE = BenchmarksServiceClient._DEFAULT_UNIVERSE

    common_billing_account_path = staticmethod(
        BenchmarksServiceClient.common_billing_account_path
    )
    parse_common_billing_account_path = staticmethod(
        BenchmarksServiceClient.parse_common_billing_account_path
    )
    common_folder_path = staticmethod(
        BenchmarksServiceClient.common_folder_path
    )
    parse_common_folder_path = staticmethod(
        BenchmarksServiceClient.parse_common_folder_path
    )
    common_organization_path = staticmethod(
        BenchmarksServiceClient.common_organization_path
    )
    parse_common_organization_path = staticmethod(
        BenchmarksServiceClient.parse_common_organization_path
    )
    common_project_path = staticmethod(
        BenchmarksServiceClient.common_project_path
    )
    parse_common_project_path = staticmethod(
        BenchmarksServiceClient.parse_common_project_path
    )
    common_location_path = staticmethod(
        BenchmarksServiceClient.common_location_path
    )
    parse_common_location_path = staticmethod(
        BenchmarksServiceClient.parse_common_location_path
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
            BenchmarksServiceAsyncClient: The constructed client.
        """
        return BenchmarksServiceClient.from_service_account_info.__func__(BenchmarksServiceAsyncClient, info, *args, **kwargs)  # type: ignore

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
            BenchmarksServiceAsyncClient: The constructed client.
        """
        return BenchmarksServiceClient.from_service_account_file.__func__(BenchmarksServiceAsyncClient, filename, *args, **kwargs)  # type: ignore

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
        return BenchmarksServiceClient.get_mtls_endpoint_and_cert_source(client_options)  # type: ignore

    @property
    def transport(self) -> BenchmarksServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            BenchmarksServiceTransport: The transport used by the client instance.
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

    get_transport_class = BenchmarksServiceClient.get_transport_class

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[
                str,
                BenchmarksServiceTransport,
                Callable[..., BenchmarksServiceTransport],
            ]
        ] = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the benchmarks service async client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,BenchmarksServiceTransport,Callable[..., BenchmarksServiceTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport to use.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the BenchmarksServiceTransport constructor.
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
        self._client = BenchmarksServiceClient(
            credentials=credentials,
            transport=transport,
            client_options=client_options,
            client_info=client_info,
        )

        if CLIENT_LOGGING_SUPPORTED and _LOGGER.isEnabledFor(
            std_logging.DEBUG
        ):  # pragma: NO COVER
            _LOGGER.debug(
                "Created client `google.ads.googleads.v23.services.BenchmarksServiceAsyncClient`.",
                extra=(
                    {
                        "serviceName": "google.ads.googleads.v23.services.BenchmarksService",
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
                        "serviceName": "google.ads.googleads.v23.services.BenchmarksService",
                        "credentialsType": None,
                    }
                ),
            )

    async def list_benchmarks_available_dates(
        self,
        request: Optional[
            Union[benchmarks_service.ListBenchmarksAvailableDatesRequest, dict]
        ] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> benchmarks_service.ListBenchmarksAvailableDatesResponse:
        r"""Returns a date range that supports benchmarks.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ListBenchmarksAvailableDatesRequest, dict]]):
                The request object. Request message for
                [BenchmarksService.ListBenchmarksAvailableDates][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksAvailableDates].
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.ListBenchmarksAvailableDatesResponse:
                Response message for
                   [BenchmarksService.ListBenchmarksAvailableDates][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksAvailableDates].

        """
        # Create or coerce a protobuf request object.
        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, benchmarks_service.ListBenchmarksAvailableDatesRequest
        ):
            request = benchmarks_service.ListBenchmarksAvailableDatesRequest(
                request
            )

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.list_benchmarks_available_dates
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

    async def list_benchmarks_locations(
        self,
        request: Optional[
            Union[benchmarks_service.ListBenchmarksLocationsRequest, dict]
        ] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> benchmarks_service.ListBenchmarksLocationsResponse:
        r"""Returns the list of locations that support benchmarks (for
        example, countries).

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ListBenchmarksLocationsRequest, dict]]):
                The request object. Request message for
                [BenchmarksService.ListBenchmarksLocations][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksLocations].
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.ListBenchmarksLocationsResponse:
                Response message for
                   [BenchmarksService.ListBenchmarksLocations][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksLocations].

        """
        # Create or coerce a protobuf request object.
        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, benchmarks_service.ListBenchmarksLocationsRequest
        ):
            request = benchmarks_service.ListBenchmarksLocationsRequest(request)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.list_benchmarks_locations
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

    async def list_benchmarks_products(
        self,
        request: Optional[
            Union[benchmarks_service.ListBenchmarksProductsRequest, dict]
        ] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> benchmarks_service.ListBenchmarksProductsResponse:
        r"""Returns the list of products that supports benchmarks.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ListBenchmarksProductsRequest, dict]]):
                The request object. Request message for
                [BenchmarksService.ListBenchmarksProducts][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksProducts].
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.ads.googleads.v23.services.types.ListBenchmarksProductsResponse:
                Response message for
                   [BenchmarksService.ListBenchmarksProducts][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksProducts].

        """
        # Create or coerce a protobuf request object.
        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(
            request, benchmarks_service.ListBenchmarksProductsRequest
        ):
            request = benchmarks_service.ListBenchmarksProductsRequest(request)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.list_benchmarks_products
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

    async def list_benchmarks_sources(
        self,
        request: Optional[
            Union[benchmarks_service.ListBenchmarksSourcesRequest, dict]
        ] = None,
        *,
        benchmarks_sources: Optional[
            MutableSequence[
                benchmarks_source_type.BenchmarksSourceTypeEnum.BenchmarksSourceType
            ]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> benchmarks_service.ListBenchmarksSourcesResponse:
        r"""Returns the list of benchmarks sources (for example, Industry
        Verticals).

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `FieldError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ListBenchmarksSourcesRequest, dict]]):
                The request object. Request message for
                [BenchmarksService.ListBenchmarksSources][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksSources].
            benchmarks_sources (:class:`MutableSequence[google.ads.googleads.v23.enums.types.BenchmarksSourceTypeEnum.BenchmarksSourceType]`):
                Required. The types of benchmarks sources to be returned
                (for example, INDUSTRY_VERTICAL).

                This corresponds to the ``benchmarks_sources`` field
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
            google.ads.googleads.v23.services.types.ListBenchmarksSourcesResponse:
                Response message for
                   [BenchmarksService.ListBenchmarksSources][google.ads.googleads.v23.services.BenchmarksService.ListBenchmarksSources].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [benchmarks_sources]
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
            request, benchmarks_service.ListBenchmarksSourcesRequest
        ):
            request = benchmarks_service.ListBenchmarksSourcesRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if benchmarks_sources:
            request.benchmarks_sources.extend(benchmarks_sources)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.list_benchmarks_sources
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

    async def generate_benchmarks_metrics(
        self,
        request: Optional[
            Union[benchmarks_service.GenerateBenchmarksMetricsRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        location: Optional[criteria.LocationInfo] = None,
        benchmarks_source: Optional[benchmarks_service.BenchmarksSource] = None,
        product_filter: Optional[benchmarks_service.ProductFilter] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> benchmarks_service.GenerateBenchmarksMetricsResponse:
        r"""Returns YouTube advertisement metrics for the given client
        against industry benchmarks.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `BenchmarksError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GenerateBenchmarksMetricsRequest, dict]]):
                The request object. Request message for
                [BenchmarksService.GenerateBenchmarksMetrics][google.ads.googleads.v23.services.BenchmarksService.GenerateBenchmarksMetrics].
            customer_id (:class:`str`):
                Required. The ID of the customer.
                Supply a client customer ID to generate
                metrics for the customer. A manager
                account customer ID will not return
                customer metrics since it does not have
                any associated direct ad campaigns.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            location (:class:`google.ads.googleads.v23.common.types.LocationInfo`):
                Required. The location to generate
                benchmarks metrics for.

                This corresponds to the ``location`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            benchmarks_source (:class:`google.ads.googleads.v23.services.types.BenchmarksSource`):
                Required. The source used to generate
                benchmarks metrics for.

                This corresponds to the ``benchmarks_source`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            product_filter (:class:`google.ads.googleads.v23.services.types.ProductFilter`):
                Required. The products to aggregate
                metrics over. Product filter settings
                support a list of product IDs or a list
                of marketing objectives.

                This corresponds to the ``product_filter`` field
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
            google.ads.googleads.v23.services.types.GenerateBenchmarksMetricsResponse:
                Response message for
                   [BenchmarksService.GenerateBenchmarksMetrics][google.ads.googleads.v23.services.BenchmarksService.GenerateBenchmarksMetrics].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [
            customer_id,
            location,
            benchmarks_source,
            product_filter,
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
            request, benchmarks_service.GenerateBenchmarksMetricsRequest
        ):
            request = benchmarks_service.GenerateBenchmarksMetricsRequest(
                request
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if location is not None:
            request.location = location
        if benchmarks_source is not None:
            request.benchmarks_source = benchmarks_source
        if product_filter is not None:
            request.product_filter = product_filter

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.generate_benchmarks_metrics
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

    async def __aenter__(self) -> "BenchmarksServiceAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__


__all__ = ("BenchmarksServiceAsyncClient",)

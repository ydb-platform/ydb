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

from google.ads.googleads.v23.services.types import ad_group_ad_service
from google.rpc import status_pb2  # type: ignore
from .transports.base import AdGroupAdServiceTransport, DEFAULT_CLIENT_INFO
from .client import AdGroupAdServiceClient

try:
    from google.api_core import client_logging  # type: ignore

    CLIENT_LOGGING_SUPPORTED = True  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    CLIENT_LOGGING_SUPPORTED = False

_LOGGER = std_logging.getLogger(__name__)


class AdGroupAdServiceAsyncClient:
    """Service to manage ads in an ad group."""

    _client: AdGroupAdServiceClient

    # Copy defaults from the synchronous client for use here.
    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = AdGroupAdServiceClient.DEFAULT_ENDPOINT
    DEFAULT_MTLS_ENDPOINT = AdGroupAdServiceClient.DEFAULT_MTLS_ENDPOINT
    _DEFAULT_ENDPOINT_TEMPLATE = (
        AdGroupAdServiceClient._DEFAULT_ENDPOINT_TEMPLATE
    )
    _DEFAULT_UNIVERSE = AdGroupAdServiceClient._DEFAULT_UNIVERSE

    ad_path = staticmethod(AdGroupAdServiceClient.ad_path)
    parse_ad_path = staticmethod(AdGroupAdServiceClient.parse_ad_path)
    ad_group_path = staticmethod(AdGroupAdServiceClient.ad_group_path)
    parse_ad_group_path = staticmethod(
        AdGroupAdServiceClient.parse_ad_group_path
    )
    ad_group_ad_path = staticmethod(AdGroupAdServiceClient.ad_group_ad_path)
    parse_ad_group_ad_path = staticmethod(
        AdGroupAdServiceClient.parse_ad_group_ad_path
    )
    ad_group_ad_label_path = staticmethod(
        AdGroupAdServiceClient.ad_group_ad_label_path
    )
    parse_ad_group_ad_label_path = staticmethod(
        AdGroupAdServiceClient.parse_ad_group_ad_label_path
    )
    asset_path = staticmethod(AdGroupAdServiceClient.asset_path)
    parse_asset_path = staticmethod(AdGroupAdServiceClient.parse_asset_path)
    common_billing_account_path = staticmethod(
        AdGroupAdServiceClient.common_billing_account_path
    )
    parse_common_billing_account_path = staticmethod(
        AdGroupAdServiceClient.parse_common_billing_account_path
    )
    common_folder_path = staticmethod(AdGroupAdServiceClient.common_folder_path)
    parse_common_folder_path = staticmethod(
        AdGroupAdServiceClient.parse_common_folder_path
    )
    common_organization_path = staticmethod(
        AdGroupAdServiceClient.common_organization_path
    )
    parse_common_organization_path = staticmethod(
        AdGroupAdServiceClient.parse_common_organization_path
    )
    common_project_path = staticmethod(
        AdGroupAdServiceClient.common_project_path
    )
    parse_common_project_path = staticmethod(
        AdGroupAdServiceClient.parse_common_project_path
    )
    common_location_path = staticmethod(
        AdGroupAdServiceClient.common_location_path
    )
    parse_common_location_path = staticmethod(
        AdGroupAdServiceClient.parse_common_location_path
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
            AdGroupAdServiceAsyncClient: The constructed client.
        """
        return AdGroupAdServiceClient.from_service_account_info.__func__(AdGroupAdServiceAsyncClient, info, *args, **kwargs)  # type: ignore

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
            AdGroupAdServiceAsyncClient: The constructed client.
        """
        return AdGroupAdServiceClient.from_service_account_file.__func__(AdGroupAdServiceAsyncClient, filename, *args, **kwargs)  # type: ignore

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
        return AdGroupAdServiceClient.get_mtls_endpoint_and_cert_source(client_options)  # type: ignore

    @property
    def transport(self) -> AdGroupAdServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            AdGroupAdServiceTransport: The transport used by the client instance.
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

    get_transport_class = AdGroupAdServiceClient.get_transport_class

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[
                str,
                AdGroupAdServiceTransport,
                Callable[..., AdGroupAdServiceTransport],
            ]
        ] = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the ad group ad service async client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,AdGroupAdServiceTransport,Callable[..., AdGroupAdServiceTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport to use.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the AdGroupAdServiceTransport constructor.
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
        self._client = AdGroupAdServiceClient(
            credentials=credentials,
            transport=transport,
            client_options=client_options,
            client_info=client_info,
        )

        if CLIENT_LOGGING_SUPPORTED and _LOGGER.isEnabledFor(
            std_logging.DEBUG
        ):  # pragma: NO COVER
            _LOGGER.debug(
                "Created client `google.ads.googleads.v23.services.AdGroupAdServiceAsyncClient`.",
                extra=(
                    {
                        "serviceName": "google.ads.googleads.v23.services.AdGroupAdService",
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
                        "serviceName": "google.ads.googleads.v23.services.AdGroupAdService",
                        "credentialsType": None,
                    }
                ),
            )

    async def mutate_ad_group_ads(
        self,
        request: Optional[
            Union[ad_group_ad_service.MutateAdGroupAdsRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        operations: Optional[
            MutableSequence[ad_group_ad_service.AdGroupAdOperation]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> ad_group_ad_service.MutateAdGroupAdsResponse:
        r"""Creates, updates, or removes ads. Operation statuses are
        returned.

        List of thrown errors: `AdCustomizerError <>`__ `AdError <>`__
        `AdGroupAdError <>`__ `AdSharingError <>`__ `AdxError <>`__
        `AssetError <>`__ `AssetLinkError <>`__
        `AuthenticationError <>`__ `AuthorizationError <>`__
        `CollectionSizeError <>`__ `ContextError <>`__
        `DatabaseError <>`__ `DateError <>`__ `DistinctError <>`__
        `FeedAttributeReferenceError <>`__ `FieldError <>`__
        `FieldMaskError <>`__ `FunctionError <>`__
        `FunctionParsingError <>`__ `HeaderError <>`__ `IdError <>`__
        `ImageError <>`__ `InternalError <>`__ `ListOperationError <>`__
        `MediaBundleError <>`__ `MediaFileError <>`__ `MutateError <>`__
        `NewResourceCreationError <>`__ `NotEmptyError <>`__
        `NullError <>`__ `OperationAccessDeniedError <>`__
        `OperatorError <>`__ `PolicyFindingError <>`__
        `PolicyValidationParameterError <>`__
        `PolicyViolationError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__ `ResourceCountLimitExceededError <>`__
        `SizeLimitError <>`__ `StringFormatError <>`__
        `StringLengthError <>`__ `UrlFieldError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.MutateAdGroupAdsRequest, dict]]):
                The request object. Request message for
                [AdGroupAdService.MutateAdGroupAds][google.ads.googleads.v23.services.AdGroupAdService.MutateAdGroupAds].
            customer_id (:class:`str`):
                Required. The ID of the customer
                whose ads are being modified.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            operations (:class:`MutableSequence[google.ads.googleads.v23.services.types.AdGroupAdOperation]`):
                Required. The list of operations to
                perform on individual ads.

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
            google.ads.googleads.v23.services.types.MutateAdGroupAdsResponse:
                Response message for an ad group ad
                mutate.

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
        if not isinstance(request, ad_group_ad_service.MutateAdGroupAdsRequest):
            request = ad_group_ad_service.MutateAdGroupAdsRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if operations:
            request.operations.extend(operations)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.mutate_ad_group_ads
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

    async def remove_automatically_created_assets(
        self,
        request: Optional[
            Union[
                ad_group_ad_service.RemoveAutomaticallyCreatedAssetsRequest,
                dict,
            ]
        ] = None,
        *,
        ad_group_ad: Optional[str] = None,
        assets_with_field_type: Optional[
            MutableSequence[ad_group_ad_service.AssetsWithFieldType]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> None:
        r"""Remove automatically created assets from an ad.

        List of thrown errors: `AdError <>`__ `AuthenticationError <>`__
        `AuthorizationError <>`__
        `AutomaticallyCreatedAssetRemovalError <>`__ `HeaderError <>`__
        `InternalError <>`__ `MutateError <>`__ `QuotaError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.RemoveAutomaticallyCreatedAssetsRequest, dict]]):
                The request object. Request message for
                [AdGroupAdService.RemoveAutomaticallyCreatedAssets][google.ads.googleads.v23.services.AdGroupAdService.RemoveAutomaticallyCreatedAssets].
            ad_group_ad (:class:`str`):
                Required. The resource name of the
                AdGroupAd from which to remove
                automatically created assets.

                This corresponds to the ``ad_group_ad`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            assets_with_field_type (:class:`MutableSequence[google.ads.googleads.v23.services.types.AssetsWithFieldType]`):
                Required. List of assets with field
                type to be removed from the AdGroupAd.

                This corresponds to the ``assets_with_field_type`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.
        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [ad_group_ad, assets_with_field_type]
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
            request, ad_group_ad_service.RemoveAutomaticallyCreatedAssetsRequest
        ):
            request = (
                ad_group_ad_service.RemoveAutomaticallyCreatedAssetsRequest(
                    request
                )
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if ad_group_ad is not None:
            request.ad_group_ad = ad_group_ad
        if assets_with_field_type:
            request.assets_with_field_type.extend(assets_with_field_type)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.remove_automatically_created_assets
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("ad_group_ad", request.ad_group_ad),)
            ),
        )

        # Validate the universe domain.
        self._client._validate_universe_domain()

        # Send the request.
        await rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    async def __aenter__(self) -> "AdGroupAdServiceAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__


__all__ = ("AdGroupAdServiceAsyncClient",)

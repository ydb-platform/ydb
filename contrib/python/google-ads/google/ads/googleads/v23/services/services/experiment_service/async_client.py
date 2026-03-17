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

from google.ads.googleads.v23.services.services.experiment_service import pagers
from google.ads.googleads.v23.services.types import experiment_service
from google.api_core import operation  # type: ignore
from google.api_core import operation_async  # type: ignore
from google.protobuf import empty_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore
from .transports.base import ExperimentServiceTransport, DEFAULT_CLIENT_INFO
from .client import ExperimentServiceClient

try:
    from google.api_core import client_logging  # type: ignore

    CLIENT_LOGGING_SUPPORTED = True  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    CLIENT_LOGGING_SUPPORTED = False

_LOGGER = std_logging.getLogger(__name__)


class ExperimentServiceAsyncClient:
    """Service to manage experiments."""

    _client: ExperimentServiceClient

    # Copy defaults from the synchronous client for use here.
    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = ExperimentServiceClient.DEFAULT_ENDPOINT
    DEFAULT_MTLS_ENDPOINT = ExperimentServiceClient.DEFAULT_MTLS_ENDPOINT
    _DEFAULT_ENDPOINT_TEMPLATE = (
        ExperimentServiceClient._DEFAULT_ENDPOINT_TEMPLATE
    )
    _DEFAULT_UNIVERSE = ExperimentServiceClient._DEFAULT_UNIVERSE

    campaign_path = staticmethod(ExperimentServiceClient.campaign_path)
    parse_campaign_path = staticmethod(
        ExperimentServiceClient.parse_campaign_path
    )
    campaign_budget_path = staticmethod(
        ExperimentServiceClient.campaign_budget_path
    )
    parse_campaign_budget_path = staticmethod(
        ExperimentServiceClient.parse_campaign_budget_path
    )
    experiment_path = staticmethod(ExperimentServiceClient.experiment_path)
    parse_experiment_path = staticmethod(
        ExperimentServiceClient.parse_experiment_path
    )
    common_billing_account_path = staticmethod(
        ExperimentServiceClient.common_billing_account_path
    )
    parse_common_billing_account_path = staticmethod(
        ExperimentServiceClient.parse_common_billing_account_path
    )
    common_folder_path = staticmethod(
        ExperimentServiceClient.common_folder_path
    )
    parse_common_folder_path = staticmethod(
        ExperimentServiceClient.parse_common_folder_path
    )
    common_organization_path = staticmethod(
        ExperimentServiceClient.common_organization_path
    )
    parse_common_organization_path = staticmethod(
        ExperimentServiceClient.parse_common_organization_path
    )
    common_project_path = staticmethod(
        ExperimentServiceClient.common_project_path
    )
    parse_common_project_path = staticmethod(
        ExperimentServiceClient.parse_common_project_path
    )
    common_location_path = staticmethod(
        ExperimentServiceClient.common_location_path
    )
    parse_common_location_path = staticmethod(
        ExperimentServiceClient.parse_common_location_path
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
            ExperimentServiceAsyncClient: The constructed client.
        """
        return ExperimentServiceClient.from_service_account_info.__func__(ExperimentServiceAsyncClient, info, *args, **kwargs)  # type: ignore

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
            ExperimentServiceAsyncClient: The constructed client.
        """
        return ExperimentServiceClient.from_service_account_file.__func__(ExperimentServiceAsyncClient, filename, *args, **kwargs)  # type: ignore

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
        return ExperimentServiceClient.get_mtls_endpoint_and_cert_source(client_options)  # type: ignore

    @property
    def transport(self) -> ExperimentServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            ExperimentServiceTransport: The transport used by the client instance.
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

    get_transport_class = ExperimentServiceClient.get_transport_class

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[
                str,
                ExperimentServiceTransport,
                Callable[..., ExperimentServiceTransport],
            ]
        ] = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the experiment service async client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,ExperimentServiceTransport,Callable[..., ExperimentServiceTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport to use.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the ExperimentServiceTransport constructor.
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
        self._client = ExperimentServiceClient(
            credentials=credentials,
            transport=transport,
            client_options=client_options,
            client_info=client_info,
        )

        if CLIENT_LOGGING_SUPPORTED and _LOGGER.isEnabledFor(
            std_logging.DEBUG
        ):  # pragma: NO COVER
            _LOGGER.debug(
                "Created client `google.ads.googleads.v23.services.ExperimentServiceAsyncClient`.",
                extra=(
                    {
                        "serviceName": "google.ads.googleads.v23.services.ExperimentService",
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
                        "serviceName": "google.ads.googleads.v23.services.ExperimentService",
                        "credentialsType": None,
                    }
                ),
            )

    async def mutate_experiments(
        self,
        request: Optional[
            Union[experiment_service.MutateExperimentsRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        operations: Optional[
            MutableSequence[experiment_service.ExperimentOperation]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> experiment_service.MutateExperimentsResponse:
        r"""Creates, updates, or removes experiments. Operation statuses are
        returned.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `ExperimentError <>`__
        `HeaderError <>`__ `InternalError <>`__ `QuotaError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.MutateExperimentsRequest, dict]]):
                The request object. Request message for
                [ExperimentService.MutateExperiments][google.ads.googleads.v23.services.ExperimentService.MutateExperiments].
            customer_id (:class:`str`):
                Required. The ID of the customer
                whose experiments are being modified.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            operations (:class:`MutableSequence[google.ads.googleads.v23.services.types.ExperimentOperation]`):
                Required. The list of operations to
                perform on individual experiments.

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
            google.ads.googleads.v23.services.types.MutateExperimentsResponse:
                Response message for experiment
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
        if not isinstance(request, experiment_service.MutateExperimentsRequest):
            request = experiment_service.MutateExperimentsRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if operations:
            request.operations.extend(operations)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.mutate_experiments
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

    async def end_experiment(
        self,
        request: Optional[
            Union[experiment_service.EndExperimentRequest, dict]
        ] = None,
        *,
        experiment: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> None:
        r"""Immediately ends an experiment, changing the experiment's
        scheduled end date and without waiting for end of day. End date
        is updated to be the time of the request.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `ExperimentError <>`__
        `HeaderError <>`__ `InternalError <>`__ `QuotaError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.EndExperimentRequest, dict]]):
                The request object. Request message for
                [ExperimentService.EndExperiment][google.ads.googleads.v23.services.ExperimentService.EndExperiment].
            experiment (:class:`str`):
                Required. The resource name of the
                campaign experiment to end.

                This corresponds to the ``experiment`` field
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
        flattened_params = [experiment]
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
        if not isinstance(request, experiment_service.EndExperimentRequest):
            request = experiment_service.EndExperimentRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if experiment is not None:
            request.experiment = experiment

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.end_experiment
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("experiment", request.experiment),)
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

    async def list_experiment_async_errors(
        self,
        request: Optional[
            Union[experiment_service.ListExperimentAsyncErrorsRequest, dict]
        ] = None,
        *,
        resource_name: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> pagers.ListExperimentAsyncErrorsAsyncPager:
        r"""Returns all errors that occurred during the last Experiment
        update (either scheduling or promotion). Supports standard list
        paging.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ListExperimentAsyncErrorsRequest, dict]]):
                The request object. Request message for
                [ExperimentService.ListExperimentAsyncErrors][google.ads.googleads.v23.services.ExperimentService.ListExperimentAsyncErrors].
            resource_name (:class:`str`):
                Required. The name of the experiment
                from which to retrieve the async errors.

                This corresponds to the ``resource_name`` field
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
            google.ads.googleads.v23.services.services.experiment_service.pagers.ListExperimentAsyncErrorsAsyncPager:
                Response message for
                   [ExperimentService.ListExperimentAsyncErrors][google.ads.googleads.v23.services.ExperimentService.ListExperimentAsyncErrors].

                Iterating over this object will yield results and
                resolve additional pages automatically.

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [resource_name]
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
            request, experiment_service.ListExperimentAsyncErrorsRequest
        ):
            request = experiment_service.ListExperimentAsyncErrorsRequest(
                request
            )

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if resource_name is not None:
            request.resource_name = resource_name

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.list_experiment_async_errors
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("resource_name", request.resource_name),)
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

        # This method is paged; wrap the response in a pager, which provides
        # an `__aiter__` convenience method.
        response = pagers.ListExperimentAsyncErrorsAsyncPager(
            method=rpc,
            request=request,
            response=response,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    async def graduate_experiment(
        self,
        request: Optional[
            Union[experiment_service.GraduateExperimentRequest, dict]
        ] = None,
        *,
        experiment: Optional[str] = None,
        campaign_budget_mappings: Optional[
            MutableSequence[experiment_service.CampaignBudgetMapping]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> None:
        r"""Graduates an experiment to a full campaign.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `ExperimentError <>`__
        `HeaderError <>`__ `InternalError <>`__ `MutateError <>`__
        `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.GraduateExperimentRequest, dict]]):
                The request object. Request message for
                [ExperimentService.GraduateExperiment][google.ads.googleads.v23.services.ExperimentService.GraduateExperiment].
            experiment (:class:`str`):
                Required. The experiment to be
                graduated.

                This corresponds to the ``experiment`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            campaign_budget_mappings (:class:`MutableSequence[google.ads.googleads.v23.services.types.CampaignBudgetMapping]`):
                Required. List of campaign budget
                mappings for graduation. Each campaign
                that appears here will graduate, and
                will be assigned a new budget that is
                paired with it in the mapping. The
                maximum size is one.

                This corresponds to the ``campaign_budget_mappings`` field
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
        flattened_params = [experiment, campaign_budget_mappings]
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
            request, experiment_service.GraduateExperimentRequest
        ):
            request = experiment_service.GraduateExperimentRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if experiment is not None:
            request.experiment = experiment
        if campaign_budget_mappings:
            request.campaign_budget_mappings.extend(campaign_budget_mappings)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.graduate_experiment
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("experiment", request.experiment),)
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

    async def schedule_experiment(
        self,
        request: Optional[
            Union[experiment_service.ScheduleExperimentRequest, dict]
        ] = None,
        *,
        resource_name: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> operation_async.AsyncOperation:
        r"""Schedule an experiment. The in design campaign will be converted
        into a real campaign (called the experiment campaign) that will
        begin serving ads if successfully created.

        The experiment is scheduled immediately with status
        INITIALIZING. This method returns a long running operation that
        tracks the forking of the in design campaign. If the forking
        fails, a list of errors can be retrieved using the
        ListExperimentAsyncErrors method. The operation's metadata will
        be a string containing the resource name of the created
        experiment.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `ExperimentError <>`__
        `DatabaseError <>`__ `DateError <>`__ `DateRangeError <>`__
        `FieldError <>`__ `HeaderError <>`__ `InternalError <>`__
        `QuotaError <>`__ `RangeError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ScheduleExperimentRequest, dict]]):
                The request object. Request message for
                [ExperimentService.ScheduleExperiment][google.ads.googleads.v23.services.ExperimentService.ScheduleExperiment].
            resource_name (:class:`str`):
                Required. The scheduled experiment.
                This corresponds to the ``resource_name`` field
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
            google.api_core.operation_async.AsyncOperation:
                An object representing a long-running operation.

                The result type for the operation will be :class:`google.protobuf.empty_pb2.Empty` A generic empty message that you can re-use to avoid defining duplicated
                   empty messages in your APIs. A typical example is to
                   use it as the request or the response type of an API
                   method. For instance:

                      service Foo {
                         rpc Bar(google.protobuf.Empty) returns
                         (google.protobuf.Empty);

                      }

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [resource_name]
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
            request, experiment_service.ScheduleExperimentRequest
        ):
            request = experiment_service.ScheduleExperimentRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if resource_name is not None:
            request.resource_name = resource_name

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.schedule_experiment
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("resource_name", request.resource_name),)
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

        # Wrap the response in an operation future.
        response = operation_async.from_gapic(
            response,
            self._client._transport.operations_client,
            empty_pb2.Empty,
            metadata_type=experiment_service.ScheduleExperimentMetadata,
        )

        # Done; return the response.
        return response

    async def promote_experiment(
        self,
        request: Optional[
            Union[experiment_service.PromoteExperimentRequest, dict]
        ] = None,
        *,
        resource_name: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> operation_async.AsyncOperation:
        r"""Promotes the trial campaign thus applying changes in the trial
        campaign to the base campaign. This method returns a long
        running operation that tracks the promotion of the experiment
        campaign. If it fails, a list of errors can be retrieved using
        the ListExperimentAsyncErrors method. The operation's metadata
        will be a string containing the resource name of the created
        experiment.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `ExperimentError <>`__
        `HeaderError <>`__ `InternalError <>`__ `QuotaError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.PromoteExperimentRequest, dict]]):
                The request object. Request message for
                [ExperimentService.PromoteExperiment][google.ads.googleads.v23.services.ExperimentService.PromoteExperiment].
            resource_name (:class:`str`):
                Required. The resource name of the
                experiment to promote.

                This corresponds to the ``resource_name`` field
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
            google.api_core.operation_async.AsyncOperation:
                An object representing a long-running operation.

                The result type for the operation will be :class:`google.protobuf.empty_pb2.Empty` A generic empty message that you can re-use to avoid defining duplicated
                   empty messages in your APIs. A typical example is to
                   use it as the request or the response type of an API
                   method. For instance:

                      service Foo {
                         rpc Bar(google.protobuf.Empty) returns
                         (google.protobuf.Empty);

                      }

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [resource_name]
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
        if not isinstance(request, experiment_service.PromoteExperimentRequest):
            request = experiment_service.PromoteExperimentRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if resource_name is not None:
            request.resource_name = resource_name

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.promote_experiment
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata) + (
            gapic_v1.routing_header.to_grpc_metadata(
                (("resource_name", request.resource_name),)
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

        # Wrap the response in an operation future.
        response = operation_async.from_gapic(
            response,
            self._client._transport.operations_client,
            empty_pb2.Empty,
            metadata_type=experiment_service.PromoteExperimentMetadata,
        )

        # Done; return the response.
        return response

    async def __aenter__(self) -> "ExperimentServiceAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__


__all__ = ("ExperimentServiceAsyncClient",)

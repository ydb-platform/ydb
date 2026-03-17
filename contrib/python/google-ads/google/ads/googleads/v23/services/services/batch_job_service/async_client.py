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

from google.ads.googleads.v23.resources.types import batch_job
from google.ads.googleads.v23.services.services.batch_job_service import pagers
from google.ads.googleads.v23.services.types import batch_job_service
from google.ads.googleads.v23.services.types import google_ads_service
from google.api_core import operation  # type: ignore
from google.api_core import operation_async  # type: ignore
from google.protobuf import empty_pb2  # type: ignore
from .transports.base import BatchJobServiceTransport, DEFAULT_CLIENT_INFO
from .client import BatchJobServiceClient

try:
    from google.api_core import client_logging  # type: ignore

    CLIENT_LOGGING_SUPPORTED = True  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    CLIENT_LOGGING_SUPPORTED = False

_LOGGER = std_logging.getLogger(__name__)


class BatchJobServiceAsyncClient:
    """Service to manage batch jobs."""

    _client: BatchJobServiceClient

    # Copy defaults from the synchronous client for use here.
    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = BatchJobServiceClient.DEFAULT_ENDPOINT
    DEFAULT_MTLS_ENDPOINT = BatchJobServiceClient.DEFAULT_MTLS_ENDPOINT
    _DEFAULT_ENDPOINT_TEMPLATE = (
        BatchJobServiceClient._DEFAULT_ENDPOINT_TEMPLATE
    )
    _DEFAULT_UNIVERSE = BatchJobServiceClient._DEFAULT_UNIVERSE

    accessible_bidding_strategy_path = staticmethod(
        BatchJobServiceClient.accessible_bidding_strategy_path
    )
    parse_accessible_bidding_strategy_path = staticmethod(
        BatchJobServiceClient.parse_accessible_bidding_strategy_path
    )
    ad_path = staticmethod(BatchJobServiceClient.ad_path)
    parse_ad_path = staticmethod(BatchJobServiceClient.parse_ad_path)
    ad_group_path = staticmethod(BatchJobServiceClient.ad_group_path)
    parse_ad_group_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_path
    )
    ad_group_ad_path = staticmethod(BatchJobServiceClient.ad_group_ad_path)
    parse_ad_group_ad_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_ad_path
    )
    ad_group_ad_label_path = staticmethod(
        BatchJobServiceClient.ad_group_ad_label_path
    )
    parse_ad_group_ad_label_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_ad_label_path
    )
    ad_group_asset_path = staticmethod(
        BatchJobServiceClient.ad_group_asset_path
    )
    parse_ad_group_asset_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_asset_path
    )
    ad_group_bid_modifier_path = staticmethod(
        BatchJobServiceClient.ad_group_bid_modifier_path
    )
    parse_ad_group_bid_modifier_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_bid_modifier_path
    )
    ad_group_criterion_path = staticmethod(
        BatchJobServiceClient.ad_group_criterion_path
    )
    parse_ad_group_criterion_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_criterion_path
    )
    ad_group_criterion_customizer_path = staticmethod(
        BatchJobServiceClient.ad_group_criterion_customizer_path
    )
    parse_ad_group_criterion_customizer_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_criterion_customizer_path
    )
    ad_group_criterion_label_path = staticmethod(
        BatchJobServiceClient.ad_group_criterion_label_path
    )
    parse_ad_group_criterion_label_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_criterion_label_path
    )
    ad_group_customizer_path = staticmethod(
        BatchJobServiceClient.ad_group_customizer_path
    )
    parse_ad_group_customizer_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_customizer_path
    )
    ad_group_label_path = staticmethod(
        BatchJobServiceClient.ad_group_label_path
    )
    parse_ad_group_label_path = staticmethod(
        BatchJobServiceClient.parse_ad_group_label_path
    )
    ad_parameter_path = staticmethod(BatchJobServiceClient.ad_parameter_path)
    parse_ad_parameter_path = staticmethod(
        BatchJobServiceClient.parse_ad_parameter_path
    )
    asset_path = staticmethod(BatchJobServiceClient.asset_path)
    parse_asset_path = staticmethod(BatchJobServiceClient.parse_asset_path)
    asset_group_path = staticmethod(BatchJobServiceClient.asset_group_path)
    parse_asset_group_path = staticmethod(
        BatchJobServiceClient.parse_asset_group_path
    )
    asset_group_asset_path = staticmethod(
        BatchJobServiceClient.asset_group_asset_path
    )
    parse_asset_group_asset_path = staticmethod(
        BatchJobServiceClient.parse_asset_group_asset_path
    )
    asset_group_listing_group_filter_path = staticmethod(
        BatchJobServiceClient.asset_group_listing_group_filter_path
    )
    parse_asset_group_listing_group_filter_path = staticmethod(
        BatchJobServiceClient.parse_asset_group_listing_group_filter_path
    )
    asset_group_signal_path = staticmethod(
        BatchJobServiceClient.asset_group_signal_path
    )
    parse_asset_group_signal_path = staticmethod(
        BatchJobServiceClient.parse_asset_group_signal_path
    )
    asset_set_path = staticmethod(BatchJobServiceClient.asset_set_path)
    parse_asset_set_path = staticmethod(
        BatchJobServiceClient.parse_asset_set_path
    )
    asset_set_asset_path = staticmethod(
        BatchJobServiceClient.asset_set_asset_path
    )
    parse_asset_set_asset_path = staticmethod(
        BatchJobServiceClient.parse_asset_set_asset_path
    )
    audience_path = staticmethod(BatchJobServiceClient.audience_path)
    parse_audience_path = staticmethod(
        BatchJobServiceClient.parse_audience_path
    )
    batch_job_path = staticmethod(BatchJobServiceClient.batch_job_path)
    parse_batch_job_path = staticmethod(
        BatchJobServiceClient.parse_batch_job_path
    )
    bidding_data_exclusion_path = staticmethod(
        BatchJobServiceClient.bidding_data_exclusion_path
    )
    parse_bidding_data_exclusion_path = staticmethod(
        BatchJobServiceClient.parse_bidding_data_exclusion_path
    )
    bidding_seasonality_adjustment_path = staticmethod(
        BatchJobServiceClient.bidding_seasonality_adjustment_path
    )
    parse_bidding_seasonality_adjustment_path = staticmethod(
        BatchJobServiceClient.parse_bidding_seasonality_adjustment_path
    )
    bidding_strategy_path = staticmethod(
        BatchJobServiceClient.bidding_strategy_path
    )
    parse_bidding_strategy_path = staticmethod(
        BatchJobServiceClient.parse_bidding_strategy_path
    )
    campaign_path = staticmethod(BatchJobServiceClient.campaign_path)
    parse_campaign_path = staticmethod(
        BatchJobServiceClient.parse_campaign_path
    )
    campaign_asset_path = staticmethod(
        BatchJobServiceClient.campaign_asset_path
    )
    parse_campaign_asset_path = staticmethod(
        BatchJobServiceClient.parse_campaign_asset_path
    )
    campaign_asset_set_path = staticmethod(
        BatchJobServiceClient.campaign_asset_set_path
    )
    parse_campaign_asset_set_path = staticmethod(
        BatchJobServiceClient.parse_campaign_asset_set_path
    )
    campaign_bid_modifier_path = staticmethod(
        BatchJobServiceClient.campaign_bid_modifier_path
    )
    parse_campaign_bid_modifier_path = staticmethod(
        BatchJobServiceClient.parse_campaign_bid_modifier_path
    )
    campaign_budget_path = staticmethod(
        BatchJobServiceClient.campaign_budget_path
    )
    parse_campaign_budget_path = staticmethod(
        BatchJobServiceClient.parse_campaign_budget_path
    )
    campaign_conversion_goal_path = staticmethod(
        BatchJobServiceClient.campaign_conversion_goal_path
    )
    parse_campaign_conversion_goal_path = staticmethod(
        BatchJobServiceClient.parse_campaign_conversion_goal_path
    )
    campaign_criterion_path = staticmethod(
        BatchJobServiceClient.campaign_criterion_path
    )
    parse_campaign_criterion_path = staticmethod(
        BatchJobServiceClient.parse_campaign_criterion_path
    )
    campaign_customizer_path = staticmethod(
        BatchJobServiceClient.campaign_customizer_path
    )
    parse_campaign_customizer_path = staticmethod(
        BatchJobServiceClient.parse_campaign_customizer_path
    )
    campaign_draft_path = staticmethod(
        BatchJobServiceClient.campaign_draft_path
    )
    parse_campaign_draft_path = staticmethod(
        BatchJobServiceClient.parse_campaign_draft_path
    )
    campaign_group_path = staticmethod(
        BatchJobServiceClient.campaign_group_path
    )
    parse_campaign_group_path = staticmethod(
        BatchJobServiceClient.parse_campaign_group_path
    )
    campaign_label_path = staticmethod(
        BatchJobServiceClient.campaign_label_path
    )
    parse_campaign_label_path = staticmethod(
        BatchJobServiceClient.parse_campaign_label_path
    )
    campaign_shared_set_path = staticmethod(
        BatchJobServiceClient.campaign_shared_set_path
    )
    parse_campaign_shared_set_path = staticmethod(
        BatchJobServiceClient.parse_campaign_shared_set_path
    )
    carrier_constant_path = staticmethod(
        BatchJobServiceClient.carrier_constant_path
    )
    parse_carrier_constant_path = staticmethod(
        BatchJobServiceClient.parse_carrier_constant_path
    )
    combined_audience_path = staticmethod(
        BatchJobServiceClient.combined_audience_path
    )
    parse_combined_audience_path = staticmethod(
        BatchJobServiceClient.parse_combined_audience_path
    )
    conversion_action_path = staticmethod(
        BatchJobServiceClient.conversion_action_path
    )
    parse_conversion_action_path = staticmethod(
        BatchJobServiceClient.parse_conversion_action_path
    )
    conversion_custom_variable_path = staticmethod(
        BatchJobServiceClient.conversion_custom_variable_path
    )
    parse_conversion_custom_variable_path = staticmethod(
        BatchJobServiceClient.parse_conversion_custom_variable_path
    )
    conversion_goal_campaign_config_path = staticmethod(
        BatchJobServiceClient.conversion_goal_campaign_config_path
    )
    parse_conversion_goal_campaign_config_path = staticmethod(
        BatchJobServiceClient.parse_conversion_goal_campaign_config_path
    )
    conversion_value_rule_path = staticmethod(
        BatchJobServiceClient.conversion_value_rule_path
    )
    parse_conversion_value_rule_path = staticmethod(
        BatchJobServiceClient.parse_conversion_value_rule_path
    )
    conversion_value_rule_set_path = staticmethod(
        BatchJobServiceClient.conversion_value_rule_set_path
    )
    parse_conversion_value_rule_set_path = staticmethod(
        BatchJobServiceClient.parse_conversion_value_rule_set_path
    )
    custom_conversion_goal_path = staticmethod(
        BatchJobServiceClient.custom_conversion_goal_path
    )
    parse_custom_conversion_goal_path = staticmethod(
        BatchJobServiceClient.parse_custom_conversion_goal_path
    )
    customer_path = staticmethod(BatchJobServiceClient.customer_path)
    parse_customer_path = staticmethod(
        BatchJobServiceClient.parse_customer_path
    )
    customer_asset_path = staticmethod(
        BatchJobServiceClient.customer_asset_path
    )
    parse_customer_asset_path = staticmethod(
        BatchJobServiceClient.parse_customer_asset_path
    )
    customer_conversion_goal_path = staticmethod(
        BatchJobServiceClient.customer_conversion_goal_path
    )
    parse_customer_conversion_goal_path = staticmethod(
        BatchJobServiceClient.parse_customer_conversion_goal_path
    )
    customer_customizer_path = staticmethod(
        BatchJobServiceClient.customer_customizer_path
    )
    parse_customer_customizer_path = staticmethod(
        BatchJobServiceClient.parse_customer_customizer_path
    )
    customer_label_path = staticmethod(
        BatchJobServiceClient.customer_label_path
    )
    parse_customer_label_path = staticmethod(
        BatchJobServiceClient.parse_customer_label_path
    )
    customer_negative_criterion_path = staticmethod(
        BatchJobServiceClient.customer_negative_criterion_path
    )
    parse_customer_negative_criterion_path = staticmethod(
        BatchJobServiceClient.parse_customer_negative_criterion_path
    )
    customizer_attribute_path = staticmethod(
        BatchJobServiceClient.customizer_attribute_path
    )
    parse_customizer_attribute_path = staticmethod(
        BatchJobServiceClient.parse_customizer_attribute_path
    )
    detailed_demographic_path = staticmethod(
        BatchJobServiceClient.detailed_demographic_path
    )
    parse_detailed_demographic_path = staticmethod(
        BatchJobServiceClient.parse_detailed_demographic_path
    )
    experiment_path = staticmethod(BatchJobServiceClient.experiment_path)
    parse_experiment_path = staticmethod(
        BatchJobServiceClient.parse_experiment_path
    )
    experiment_arm_path = staticmethod(
        BatchJobServiceClient.experiment_arm_path
    )
    parse_experiment_arm_path = staticmethod(
        BatchJobServiceClient.parse_experiment_arm_path
    )
    geo_target_constant_path = staticmethod(
        BatchJobServiceClient.geo_target_constant_path
    )
    parse_geo_target_constant_path = staticmethod(
        BatchJobServiceClient.parse_geo_target_constant_path
    )
    keyword_plan_path = staticmethod(BatchJobServiceClient.keyword_plan_path)
    parse_keyword_plan_path = staticmethod(
        BatchJobServiceClient.parse_keyword_plan_path
    )
    keyword_plan_ad_group_path = staticmethod(
        BatchJobServiceClient.keyword_plan_ad_group_path
    )
    parse_keyword_plan_ad_group_path = staticmethod(
        BatchJobServiceClient.parse_keyword_plan_ad_group_path
    )
    keyword_plan_ad_group_keyword_path = staticmethod(
        BatchJobServiceClient.keyword_plan_ad_group_keyword_path
    )
    parse_keyword_plan_ad_group_keyword_path = staticmethod(
        BatchJobServiceClient.parse_keyword_plan_ad_group_keyword_path
    )
    keyword_plan_campaign_path = staticmethod(
        BatchJobServiceClient.keyword_plan_campaign_path
    )
    parse_keyword_plan_campaign_path = staticmethod(
        BatchJobServiceClient.parse_keyword_plan_campaign_path
    )
    keyword_plan_campaign_keyword_path = staticmethod(
        BatchJobServiceClient.keyword_plan_campaign_keyword_path
    )
    parse_keyword_plan_campaign_keyword_path = staticmethod(
        BatchJobServiceClient.parse_keyword_plan_campaign_keyword_path
    )
    keyword_theme_constant_path = staticmethod(
        BatchJobServiceClient.keyword_theme_constant_path
    )
    parse_keyword_theme_constant_path = staticmethod(
        BatchJobServiceClient.parse_keyword_theme_constant_path
    )
    label_path = staticmethod(BatchJobServiceClient.label_path)
    parse_label_path = staticmethod(BatchJobServiceClient.parse_label_path)
    language_constant_path = staticmethod(
        BatchJobServiceClient.language_constant_path
    )
    parse_language_constant_path = staticmethod(
        BatchJobServiceClient.parse_language_constant_path
    )
    life_event_path = staticmethod(BatchJobServiceClient.life_event_path)
    parse_life_event_path = staticmethod(
        BatchJobServiceClient.parse_life_event_path
    )
    mobile_app_category_constant_path = staticmethod(
        BatchJobServiceClient.mobile_app_category_constant_path
    )
    parse_mobile_app_category_constant_path = staticmethod(
        BatchJobServiceClient.parse_mobile_app_category_constant_path
    )
    mobile_device_constant_path = staticmethod(
        BatchJobServiceClient.mobile_device_constant_path
    )
    parse_mobile_device_constant_path = staticmethod(
        BatchJobServiceClient.parse_mobile_device_constant_path
    )
    operating_system_version_constant_path = staticmethod(
        BatchJobServiceClient.operating_system_version_constant_path
    )
    parse_operating_system_version_constant_path = staticmethod(
        BatchJobServiceClient.parse_operating_system_version_constant_path
    )
    recommendation_subscription_path = staticmethod(
        BatchJobServiceClient.recommendation_subscription_path
    )
    parse_recommendation_subscription_path = staticmethod(
        BatchJobServiceClient.parse_recommendation_subscription_path
    )
    remarketing_action_path = staticmethod(
        BatchJobServiceClient.remarketing_action_path
    )
    parse_remarketing_action_path = staticmethod(
        BatchJobServiceClient.parse_remarketing_action_path
    )
    shared_criterion_path = staticmethod(
        BatchJobServiceClient.shared_criterion_path
    )
    parse_shared_criterion_path = staticmethod(
        BatchJobServiceClient.parse_shared_criterion_path
    )
    shared_set_path = staticmethod(BatchJobServiceClient.shared_set_path)
    parse_shared_set_path = staticmethod(
        BatchJobServiceClient.parse_shared_set_path
    )
    smart_campaign_setting_path = staticmethod(
        BatchJobServiceClient.smart_campaign_setting_path
    )
    parse_smart_campaign_setting_path = staticmethod(
        BatchJobServiceClient.parse_smart_campaign_setting_path
    )
    topic_constant_path = staticmethod(
        BatchJobServiceClient.topic_constant_path
    )
    parse_topic_constant_path = staticmethod(
        BatchJobServiceClient.parse_topic_constant_path
    )
    user_interest_path = staticmethod(BatchJobServiceClient.user_interest_path)
    parse_user_interest_path = staticmethod(
        BatchJobServiceClient.parse_user_interest_path
    )
    user_list_path = staticmethod(BatchJobServiceClient.user_list_path)
    parse_user_list_path = staticmethod(
        BatchJobServiceClient.parse_user_list_path
    )
    common_billing_account_path = staticmethod(
        BatchJobServiceClient.common_billing_account_path
    )
    parse_common_billing_account_path = staticmethod(
        BatchJobServiceClient.parse_common_billing_account_path
    )
    common_folder_path = staticmethod(BatchJobServiceClient.common_folder_path)
    parse_common_folder_path = staticmethod(
        BatchJobServiceClient.parse_common_folder_path
    )
    common_organization_path = staticmethod(
        BatchJobServiceClient.common_organization_path
    )
    parse_common_organization_path = staticmethod(
        BatchJobServiceClient.parse_common_organization_path
    )
    common_project_path = staticmethod(
        BatchJobServiceClient.common_project_path
    )
    parse_common_project_path = staticmethod(
        BatchJobServiceClient.parse_common_project_path
    )
    common_location_path = staticmethod(
        BatchJobServiceClient.common_location_path
    )
    parse_common_location_path = staticmethod(
        BatchJobServiceClient.parse_common_location_path
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
            BatchJobServiceAsyncClient: The constructed client.
        """
        return BatchJobServiceClient.from_service_account_info.__func__(BatchJobServiceAsyncClient, info, *args, **kwargs)  # type: ignore

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
            BatchJobServiceAsyncClient: The constructed client.
        """
        return BatchJobServiceClient.from_service_account_file.__func__(BatchJobServiceAsyncClient, filename, *args, **kwargs)  # type: ignore

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
        return BatchJobServiceClient.get_mtls_endpoint_and_cert_source(client_options)  # type: ignore

    @property
    def transport(self) -> BatchJobServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            BatchJobServiceTransport: The transport used by the client instance.
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

    get_transport_class = BatchJobServiceClient.get_transport_class

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[
                str,
                BatchJobServiceTransport,
                Callable[..., BatchJobServiceTransport],
            ]
        ] = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the batch job service async client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,BatchJobServiceTransport,Callable[..., BatchJobServiceTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport to use.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the BatchJobServiceTransport constructor.
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
        self._client = BatchJobServiceClient(
            credentials=credentials,
            transport=transport,
            client_options=client_options,
            client_info=client_info,
        )

        if CLIENT_LOGGING_SUPPORTED and _LOGGER.isEnabledFor(
            std_logging.DEBUG
        ):  # pragma: NO COVER
            _LOGGER.debug(
                "Created client `google.ads.googleads.v23.services.BatchJobServiceAsyncClient`.",
                extra=(
                    {
                        "serviceName": "google.ads.googleads.v23.services.BatchJobService",
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
                        "serviceName": "google.ads.googleads.v23.services.BatchJobService",
                        "credentialsType": None,
                    }
                ),
            )

    async def mutate_batch_job(
        self,
        request: Optional[
            Union[batch_job_service.MutateBatchJobRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        operation: Optional[batch_job_service.BatchJobOperation] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> batch_job_service.MutateBatchJobResponse:
        r"""Mutates a batch job.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `HeaderError <>`__
        `InternalError <>`__ `QuotaError <>`__ `RequestError <>`__
        `ResourceCountLimitExceededError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.MutateBatchJobRequest, dict]]):
                The request object. Request message for
                [BatchJobService.MutateBatchJob][google.ads.googleads.v23.services.BatchJobService.MutateBatchJob].
            customer_id (:class:`str`):
                Required. The ID of the customer for
                which to create a batch job.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            operation (:class:`google.ads.googleads.v23.services.types.BatchJobOperation`):
                Required. The operation to perform on
                an individual batch job.

                This corresponds to the ``operation`` field
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
            google.ads.googleads.v23.services.types.MutateBatchJobResponse:
                Response message for
                   [BatchJobService.MutateBatchJob][google.ads.googleads.v23.services.BatchJobService.MutateBatchJob].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, operation]
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
        if not isinstance(request, batch_job_service.MutateBatchJobRequest):
            request = batch_job_service.MutateBatchJobRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if operation is not None:
            request.operation = operation

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.mutate_batch_job
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

    async def list_batch_job_results(
        self,
        request: Optional[
            Union[batch_job_service.ListBatchJobResultsRequest, dict]
        ] = None,
        *,
        resource_name: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> pagers.ListBatchJobResultsAsyncPager:
        r"""Returns the results of the batch job. The job must be done.
        Supports standard list paging.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `BatchJobError <>`__
        `HeaderError <>`__ `InternalError <>`__ `QuotaError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.ListBatchJobResultsRequest, dict]]):
                The request object. Request message for
                [BatchJobService.ListBatchJobResults][google.ads.googleads.v23.services.BatchJobService.ListBatchJobResults].
            resource_name (:class:`str`):
                Required. The resource name of the
                batch job whose results are being
                listed.

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
            google.ads.googleads.v23.services.services.batch_job_service.pagers.ListBatchJobResultsAsyncPager:
                Response message for
                   [BatchJobService.ListBatchJobResults][google.ads.googleads.v23.services.BatchJobService.ListBatchJobResults].

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
            request, batch_job_service.ListBatchJobResultsRequest
        ):
            request = batch_job_service.ListBatchJobResultsRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if resource_name is not None:
            request.resource_name = resource_name

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.list_batch_job_results
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
        response = pagers.ListBatchJobResultsAsyncPager(
            method=rpc,
            request=request,
            response=response,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    async def run_batch_job(
        self,
        request: Optional[
            Union[batch_job_service.RunBatchJobRequest, dict]
        ] = None,
        *,
        resource_name: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> operation_async.AsyncOperation:
        r"""Runs the batch job.

        The Operation.metadata field type is BatchJobMetadata. When
        finished, the long running operation will not contain errors or
        a response. Instead, use ListBatchJobResults to get the results
        of the job.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `BatchJobError <>`__
        `HeaderError <>`__ `InternalError <>`__ `QuotaError <>`__
        `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.RunBatchJobRequest, dict]]):
                The request object. Request message for
                [BatchJobService.RunBatchJob][google.ads.googleads.v23.services.BatchJobService.RunBatchJob].
            resource_name (:class:`str`):
                Required. The resource name of the
                BatchJob to run.

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
        if not isinstance(request, batch_job_service.RunBatchJobRequest):
            request = batch_job_service.RunBatchJobRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if resource_name is not None:
            request.resource_name = resource_name

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.run_batch_job
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
            metadata_type=batch_job.BatchJob.BatchJobMetadata,
        )

        # Done; return the response.
        return response

    async def add_batch_job_operations(
        self,
        request: Optional[
            Union[batch_job_service.AddBatchJobOperationsRequest, dict]
        ] = None,
        *,
        resource_name: Optional[str] = None,
        sequence_token: Optional[str] = None,
        mutate_operations: Optional[
            MutableSequence[google_ads_service.MutateOperation]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> batch_job_service.AddBatchJobOperationsResponse:
        r"""Add operations to the batch job.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `BatchJobError <>`__
        `HeaderError <>`__ `InternalError <>`__ `QuotaError <>`__
        `RequestError <>`__ `ResourceCountLimitExceededError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.AddBatchJobOperationsRequest, dict]]):
                The request object. Request message for
                [BatchJobService.AddBatchJobOperations][google.ads.googleads.v23.services.BatchJobService.AddBatchJobOperations].
            resource_name (:class:`str`):
                Required. The resource name of the
                batch job.

                This corresponds to the ``resource_name`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            sequence_token (:class:`str`):
                A token used to enforce sequencing.

                The first AddBatchJobOperations request for a batch job
                should not set sequence_token. Subsequent requests must
                set sequence_token to the value of next_sequence_token
                received in the previous AddBatchJobOperations response.

                This corresponds to the ``sequence_token`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            mutate_operations (:class:`MutableSequence[google.ads.googleads.v23.services.types.MutateOperation]`):
                Required. The list of mutates being
                added.
                Operations can use negative integers as
                temp ids to signify dependencies between
                entities created in this batch job. For
                example, a customer with id = 1234 can
                create a campaign and an ad group in
                that same campaign by creating a
                campaign in the first operation with the
                resource name explicitly set to
                "customers/1234/campaigns/-1", and
                creating an ad group in the second
                operation with the campaign field also
                set to "customers/1234/campaigns/-1".

                This corresponds to the ``mutate_operations`` field
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
            google.ads.googleads.v23.services.types.AddBatchJobOperationsResponse:
                Response message for
                   [BatchJobService.AddBatchJobOperations][google.ads.googleads.v23.services.BatchJobService.AddBatchJobOperations].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [resource_name, sequence_token, mutate_operations]
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
            request, batch_job_service.AddBatchJobOperationsRequest
        ):
            request = batch_job_service.AddBatchJobOperationsRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if resource_name is not None:
            request.resource_name = resource_name
        if sequence_token is not None:
            request.sequence_token = sequence_token
        if mutate_operations:
            request.mutate_operations.extend(mutate_operations)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.add_batch_job_operations
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

        # Done; return the response.
        return response

    async def __aenter__(self) -> "BatchJobServiceAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__


__all__ = ("BatchJobServiceAsyncClient",)

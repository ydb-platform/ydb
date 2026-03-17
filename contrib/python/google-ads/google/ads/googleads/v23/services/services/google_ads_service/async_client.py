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
from typing import (
    Callable,
    MutableSequence,
    Optional,
    AsyncIterable,
    Awaitable,
    Sequence,
    Tuple,
    Union,
)

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

from google.ads.googleads.v23.services.services.google_ads_service import pagers
from google.ads.googleads.v23.services.types import google_ads_service
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore
from .transports.base import GoogleAdsServiceTransport, DEFAULT_CLIENT_INFO
from .client import GoogleAdsServiceClient

try:
    from google.api_core import client_logging  # type: ignore

    CLIENT_LOGGING_SUPPORTED = True  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    CLIENT_LOGGING_SUPPORTED = False

_LOGGER = std_logging.getLogger(__name__)


class GoogleAdsServiceAsyncClient:
    """Service to fetch data and metrics across resources."""

    _client: GoogleAdsServiceClient

    # Copy defaults from the synchronous client for use here.
    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = GoogleAdsServiceClient.DEFAULT_ENDPOINT
    DEFAULT_MTLS_ENDPOINT = GoogleAdsServiceClient.DEFAULT_MTLS_ENDPOINT
    _DEFAULT_ENDPOINT_TEMPLATE = (
        GoogleAdsServiceClient._DEFAULT_ENDPOINT_TEMPLATE
    )
    _DEFAULT_UNIVERSE = GoogleAdsServiceClient._DEFAULT_UNIVERSE

    accessible_bidding_strategy_path = staticmethod(
        GoogleAdsServiceClient.accessible_bidding_strategy_path
    )
    parse_accessible_bidding_strategy_path = staticmethod(
        GoogleAdsServiceClient.parse_accessible_bidding_strategy_path
    )
    account_budget_path = staticmethod(
        GoogleAdsServiceClient.account_budget_path
    )
    parse_account_budget_path = staticmethod(
        GoogleAdsServiceClient.parse_account_budget_path
    )
    account_budget_proposal_path = staticmethod(
        GoogleAdsServiceClient.account_budget_proposal_path
    )
    parse_account_budget_proposal_path = staticmethod(
        GoogleAdsServiceClient.parse_account_budget_proposal_path
    )
    account_link_path = staticmethod(GoogleAdsServiceClient.account_link_path)
    parse_account_link_path = staticmethod(
        GoogleAdsServiceClient.parse_account_link_path
    )
    ad_path = staticmethod(GoogleAdsServiceClient.ad_path)
    parse_ad_path = staticmethod(GoogleAdsServiceClient.parse_ad_path)
    ad_group_path = staticmethod(GoogleAdsServiceClient.ad_group_path)
    parse_ad_group_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_path
    )
    ad_group_ad_path = staticmethod(GoogleAdsServiceClient.ad_group_ad_path)
    parse_ad_group_ad_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_ad_path
    )
    ad_group_ad_asset_combination_view_path = staticmethod(
        GoogleAdsServiceClient.ad_group_ad_asset_combination_view_path
    )
    parse_ad_group_ad_asset_combination_view_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_ad_asset_combination_view_path
    )
    ad_group_ad_asset_view_path = staticmethod(
        GoogleAdsServiceClient.ad_group_ad_asset_view_path
    )
    parse_ad_group_ad_asset_view_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_ad_asset_view_path
    )
    ad_group_ad_label_path = staticmethod(
        GoogleAdsServiceClient.ad_group_ad_label_path
    )
    parse_ad_group_ad_label_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_ad_label_path
    )
    ad_group_asset_path = staticmethod(
        GoogleAdsServiceClient.ad_group_asset_path
    )
    parse_ad_group_asset_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_asset_path
    )
    ad_group_asset_set_path = staticmethod(
        GoogleAdsServiceClient.ad_group_asset_set_path
    )
    parse_ad_group_asset_set_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_asset_set_path
    )
    ad_group_audience_view_path = staticmethod(
        GoogleAdsServiceClient.ad_group_audience_view_path
    )
    parse_ad_group_audience_view_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_audience_view_path
    )
    ad_group_bid_modifier_path = staticmethod(
        GoogleAdsServiceClient.ad_group_bid_modifier_path
    )
    parse_ad_group_bid_modifier_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_bid_modifier_path
    )
    ad_group_criterion_path = staticmethod(
        GoogleAdsServiceClient.ad_group_criterion_path
    )
    parse_ad_group_criterion_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_criterion_path
    )
    ad_group_criterion_customizer_path = staticmethod(
        GoogleAdsServiceClient.ad_group_criterion_customizer_path
    )
    parse_ad_group_criterion_customizer_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_criterion_customizer_path
    )
    ad_group_criterion_label_path = staticmethod(
        GoogleAdsServiceClient.ad_group_criterion_label_path
    )
    parse_ad_group_criterion_label_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_criterion_label_path
    )
    ad_group_criterion_simulation_path = staticmethod(
        GoogleAdsServiceClient.ad_group_criterion_simulation_path
    )
    parse_ad_group_criterion_simulation_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_criterion_simulation_path
    )
    ad_group_customizer_path = staticmethod(
        GoogleAdsServiceClient.ad_group_customizer_path
    )
    parse_ad_group_customizer_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_customizer_path
    )
    ad_group_label_path = staticmethod(
        GoogleAdsServiceClient.ad_group_label_path
    )
    parse_ad_group_label_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_label_path
    )
    ad_group_simulation_path = staticmethod(
        GoogleAdsServiceClient.ad_group_simulation_path
    )
    parse_ad_group_simulation_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_group_simulation_path
    )
    ad_parameter_path = staticmethod(GoogleAdsServiceClient.ad_parameter_path)
    parse_ad_parameter_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_parameter_path
    )
    ad_schedule_view_path = staticmethod(
        GoogleAdsServiceClient.ad_schedule_view_path
    )
    parse_ad_schedule_view_path = staticmethod(
        GoogleAdsServiceClient.parse_ad_schedule_view_path
    )
    age_range_view_path = staticmethod(
        GoogleAdsServiceClient.age_range_view_path
    )
    parse_age_range_view_path = staticmethod(
        GoogleAdsServiceClient.parse_age_range_view_path
    )
    ai_max_search_term_ad_combination_view_path = staticmethod(
        GoogleAdsServiceClient.ai_max_search_term_ad_combination_view_path
    )
    parse_ai_max_search_term_ad_combination_view_path = staticmethod(
        GoogleAdsServiceClient.parse_ai_max_search_term_ad_combination_view_path
    )
    android_privacy_shared_key_google_ad_group_path = staticmethod(
        GoogleAdsServiceClient.android_privacy_shared_key_google_ad_group_path
    )
    parse_android_privacy_shared_key_google_ad_group_path = staticmethod(
        GoogleAdsServiceClient.parse_android_privacy_shared_key_google_ad_group_path
    )
    android_privacy_shared_key_google_campaign_path = staticmethod(
        GoogleAdsServiceClient.android_privacy_shared_key_google_campaign_path
    )
    parse_android_privacy_shared_key_google_campaign_path = staticmethod(
        GoogleAdsServiceClient.parse_android_privacy_shared_key_google_campaign_path
    )
    android_privacy_shared_key_google_network_type_path = staticmethod(
        GoogleAdsServiceClient.android_privacy_shared_key_google_network_type_path
    )
    parse_android_privacy_shared_key_google_network_type_path = staticmethod(
        GoogleAdsServiceClient.parse_android_privacy_shared_key_google_network_type_path
    )
    applied_incentive_path = staticmethod(
        GoogleAdsServiceClient.applied_incentive_path
    )
    parse_applied_incentive_path = staticmethod(
        GoogleAdsServiceClient.parse_applied_incentive_path
    )
    asset_path = staticmethod(GoogleAdsServiceClient.asset_path)
    parse_asset_path = staticmethod(GoogleAdsServiceClient.parse_asset_path)
    asset_field_type_view_path = staticmethod(
        GoogleAdsServiceClient.asset_field_type_view_path
    )
    parse_asset_field_type_view_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_field_type_view_path
    )
    asset_group_path = staticmethod(GoogleAdsServiceClient.asset_group_path)
    parse_asset_group_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_group_path
    )
    asset_group_asset_path = staticmethod(
        GoogleAdsServiceClient.asset_group_asset_path
    )
    parse_asset_group_asset_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_group_asset_path
    )
    asset_group_listing_group_filter_path = staticmethod(
        GoogleAdsServiceClient.asset_group_listing_group_filter_path
    )
    parse_asset_group_listing_group_filter_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_group_listing_group_filter_path
    )
    asset_group_product_group_view_path = staticmethod(
        GoogleAdsServiceClient.asset_group_product_group_view_path
    )
    parse_asset_group_product_group_view_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_group_product_group_view_path
    )
    asset_group_signal_path = staticmethod(
        GoogleAdsServiceClient.asset_group_signal_path
    )
    parse_asset_group_signal_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_group_signal_path
    )
    asset_group_top_combination_view_path = staticmethod(
        GoogleAdsServiceClient.asset_group_top_combination_view_path
    )
    parse_asset_group_top_combination_view_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_group_top_combination_view_path
    )
    asset_set_path = staticmethod(GoogleAdsServiceClient.asset_set_path)
    parse_asset_set_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_set_path
    )
    asset_set_asset_path = staticmethod(
        GoogleAdsServiceClient.asset_set_asset_path
    )
    parse_asset_set_asset_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_set_asset_path
    )
    asset_set_type_view_path = staticmethod(
        GoogleAdsServiceClient.asset_set_type_view_path
    )
    parse_asset_set_type_view_path = staticmethod(
        GoogleAdsServiceClient.parse_asset_set_type_view_path
    )
    audience_path = staticmethod(GoogleAdsServiceClient.audience_path)
    parse_audience_path = staticmethod(
        GoogleAdsServiceClient.parse_audience_path
    )
    batch_job_path = staticmethod(GoogleAdsServiceClient.batch_job_path)
    parse_batch_job_path = staticmethod(
        GoogleAdsServiceClient.parse_batch_job_path
    )
    bidding_data_exclusion_path = staticmethod(
        GoogleAdsServiceClient.bidding_data_exclusion_path
    )
    parse_bidding_data_exclusion_path = staticmethod(
        GoogleAdsServiceClient.parse_bidding_data_exclusion_path
    )
    bidding_seasonality_adjustment_path = staticmethod(
        GoogleAdsServiceClient.bidding_seasonality_adjustment_path
    )
    parse_bidding_seasonality_adjustment_path = staticmethod(
        GoogleAdsServiceClient.parse_bidding_seasonality_adjustment_path
    )
    bidding_strategy_path = staticmethod(
        GoogleAdsServiceClient.bidding_strategy_path
    )
    parse_bidding_strategy_path = staticmethod(
        GoogleAdsServiceClient.parse_bidding_strategy_path
    )
    bidding_strategy_simulation_path = staticmethod(
        GoogleAdsServiceClient.bidding_strategy_simulation_path
    )
    parse_bidding_strategy_simulation_path = staticmethod(
        GoogleAdsServiceClient.parse_bidding_strategy_simulation_path
    )
    billing_setup_path = staticmethod(GoogleAdsServiceClient.billing_setup_path)
    parse_billing_setup_path = staticmethod(
        GoogleAdsServiceClient.parse_billing_setup_path
    )
    call_view_path = staticmethod(GoogleAdsServiceClient.call_view_path)
    parse_call_view_path = staticmethod(
        GoogleAdsServiceClient.parse_call_view_path
    )
    campaign_path = staticmethod(GoogleAdsServiceClient.campaign_path)
    parse_campaign_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_path
    )
    campaign_aggregate_asset_view_path = staticmethod(
        GoogleAdsServiceClient.campaign_aggregate_asset_view_path
    )
    parse_campaign_aggregate_asset_view_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_aggregate_asset_view_path
    )
    campaign_asset_path = staticmethod(
        GoogleAdsServiceClient.campaign_asset_path
    )
    parse_campaign_asset_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_asset_path
    )
    campaign_asset_set_path = staticmethod(
        GoogleAdsServiceClient.campaign_asset_set_path
    )
    parse_campaign_asset_set_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_asset_set_path
    )
    campaign_audience_view_path = staticmethod(
        GoogleAdsServiceClient.campaign_audience_view_path
    )
    parse_campaign_audience_view_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_audience_view_path
    )
    campaign_bid_modifier_path = staticmethod(
        GoogleAdsServiceClient.campaign_bid_modifier_path
    )
    parse_campaign_bid_modifier_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_bid_modifier_path
    )
    campaign_budget_path = staticmethod(
        GoogleAdsServiceClient.campaign_budget_path
    )
    parse_campaign_budget_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_budget_path
    )
    campaign_conversion_goal_path = staticmethod(
        GoogleAdsServiceClient.campaign_conversion_goal_path
    )
    parse_campaign_conversion_goal_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_conversion_goal_path
    )
    campaign_criterion_path = staticmethod(
        GoogleAdsServiceClient.campaign_criterion_path
    )
    parse_campaign_criterion_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_criterion_path
    )
    campaign_customizer_path = staticmethod(
        GoogleAdsServiceClient.campaign_customizer_path
    )
    parse_campaign_customizer_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_customizer_path
    )
    campaign_draft_path = staticmethod(
        GoogleAdsServiceClient.campaign_draft_path
    )
    parse_campaign_draft_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_draft_path
    )
    campaign_goal_config_path = staticmethod(
        GoogleAdsServiceClient.campaign_goal_config_path
    )
    parse_campaign_goal_config_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_goal_config_path
    )
    campaign_group_path = staticmethod(
        GoogleAdsServiceClient.campaign_group_path
    )
    parse_campaign_group_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_group_path
    )
    campaign_label_path = staticmethod(
        GoogleAdsServiceClient.campaign_label_path
    )
    parse_campaign_label_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_label_path
    )
    campaign_lifecycle_goal_path = staticmethod(
        GoogleAdsServiceClient.campaign_lifecycle_goal_path
    )
    parse_campaign_lifecycle_goal_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_lifecycle_goal_path
    )
    campaign_search_term_insight_path = staticmethod(
        GoogleAdsServiceClient.campaign_search_term_insight_path
    )
    parse_campaign_search_term_insight_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_search_term_insight_path
    )
    campaign_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.campaign_search_term_view_path
    )
    parse_campaign_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_search_term_view_path
    )
    campaign_shared_set_path = staticmethod(
        GoogleAdsServiceClient.campaign_shared_set_path
    )
    parse_campaign_shared_set_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_shared_set_path
    )
    campaign_simulation_path = staticmethod(
        GoogleAdsServiceClient.campaign_simulation_path
    )
    parse_campaign_simulation_path = staticmethod(
        GoogleAdsServiceClient.parse_campaign_simulation_path
    )
    carrier_constant_path = staticmethod(
        GoogleAdsServiceClient.carrier_constant_path
    )
    parse_carrier_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_carrier_constant_path
    )
    change_event_path = staticmethod(GoogleAdsServiceClient.change_event_path)
    parse_change_event_path = staticmethod(
        GoogleAdsServiceClient.parse_change_event_path
    )
    change_status_path = staticmethod(GoogleAdsServiceClient.change_status_path)
    parse_change_status_path = staticmethod(
        GoogleAdsServiceClient.parse_change_status_path
    )
    channel_aggregate_asset_view_path = staticmethod(
        GoogleAdsServiceClient.channel_aggregate_asset_view_path
    )
    parse_channel_aggregate_asset_view_path = staticmethod(
        GoogleAdsServiceClient.parse_channel_aggregate_asset_view_path
    )
    click_view_path = staticmethod(GoogleAdsServiceClient.click_view_path)
    parse_click_view_path = staticmethod(
        GoogleAdsServiceClient.parse_click_view_path
    )
    combined_audience_path = staticmethod(
        GoogleAdsServiceClient.combined_audience_path
    )
    parse_combined_audience_path = staticmethod(
        GoogleAdsServiceClient.parse_combined_audience_path
    )
    content_criterion_view_path = staticmethod(
        GoogleAdsServiceClient.content_criterion_view_path
    )
    parse_content_criterion_view_path = staticmethod(
        GoogleAdsServiceClient.parse_content_criterion_view_path
    )
    conversion_action_path = staticmethod(
        GoogleAdsServiceClient.conversion_action_path
    )
    parse_conversion_action_path = staticmethod(
        GoogleAdsServiceClient.parse_conversion_action_path
    )
    conversion_custom_variable_path = staticmethod(
        GoogleAdsServiceClient.conversion_custom_variable_path
    )
    parse_conversion_custom_variable_path = staticmethod(
        GoogleAdsServiceClient.parse_conversion_custom_variable_path
    )
    conversion_goal_campaign_config_path = staticmethod(
        GoogleAdsServiceClient.conversion_goal_campaign_config_path
    )
    parse_conversion_goal_campaign_config_path = staticmethod(
        GoogleAdsServiceClient.parse_conversion_goal_campaign_config_path
    )
    conversion_value_rule_path = staticmethod(
        GoogleAdsServiceClient.conversion_value_rule_path
    )
    parse_conversion_value_rule_path = staticmethod(
        GoogleAdsServiceClient.parse_conversion_value_rule_path
    )
    conversion_value_rule_set_path = staticmethod(
        GoogleAdsServiceClient.conversion_value_rule_set_path
    )
    parse_conversion_value_rule_set_path = staticmethod(
        GoogleAdsServiceClient.parse_conversion_value_rule_set_path
    )
    currency_constant_path = staticmethod(
        GoogleAdsServiceClient.currency_constant_path
    )
    parse_currency_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_currency_constant_path
    )
    custom_audience_path = staticmethod(
        GoogleAdsServiceClient.custom_audience_path
    )
    parse_custom_audience_path = staticmethod(
        GoogleAdsServiceClient.parse_custom_audience_path
    )
    custom_conversion_goal_path = staticmethod(
        GoogleAdsServiceClient.custom_conversion_goal_path
    )
    parse_custom_conversion_goal_path = staticmethod(
        GoogleAdsServiceClient.parse_custom_conversion_goal_path
    )
    customer_path = staticmethod(GoogleAdsServiceClient.customer_path)
    parse_customer_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_path
    )
    customer_asset_path = staticmethod(
        GoogleAdsServiceClient.customer_asset_path
    )
    parse_customer_asset_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_asset_path
    )
    customer_asset_set_path = staticmethod(
        GoogleAdsServiceClient.customer_asset_set_path
    )
    parse_customer_asset_set_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_asset_set_path
    )
    customer_client_path = staticmethod(
        GoogleAdsServiceClient.customer_client_path
    )
    parse_customer_client_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_client_path
    )
    customer_client_link_path = staticmethod(
        GoogleAdsServiceClient.customer_client_link_path
    )
    parse_customer_client_link_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_client_link_path
    )
    customer_conversion_goal_path = staticmethod(
        GoogleAdsServiceClient.customer_conversion_goal_path
    )
    parse_customer_conversion_goal_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_conversion_goal_path
    )
    customer_customizer_path = staticmethod(
        GoogleAdsServiceClient.customer_customizer_path
    )
    parse_customer_customizer_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_customizer_path
    )
    customer_label_path = staticmethod(
        GoogleAdsServiceClient.customer_label_path
    )
    parse_customer_label_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_label_path
    )
    customer_lifecycle_goal_path = staticmethod(
        GoogleAdsServiceClient.customer_lifecycle_goal_path
    )
    parse_customer_lifecycle_goal_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_lifecycle_goal_path
    )
    customer_manager_link_path = staticmethod(
        GoogleAdsServiceClient.customer_manager_link_path
    )
    parse_customer_manager_link_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_manager_link_path
    )
    customer_negative_criterion_path = staticmethod(
        GoogleAdsServiceClient.customer_negative_criterion_path
    )
    parse_customer_negative_criterion_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_negative_criterion_path
    )
    customer_search_term_insight_path = staticmethod(
        GoogleAdsServiceClient.customer_search_term_insight_path
    )
    parse_customer_search_term_insight_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_search_term_insight_path
    )
    customer_user_access_path = staticmethod(
        GoogleAdsServiceClient.customer_user_access_path
    )
    parse_customer_user_access_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_user_access_path
    )
    customer_user_access_invitation_path = staticmethod(
        GoogleAdsServiceClient.customer_user_access_invitation_path
    )
    parse_customer_user_access_invitation_path = staticmethod(
        GoogleAdsServiceClient.parse_customer_user_access_invitation_path
    )
    custom_interest_path = staticmethod(
        GoogleAdsServiceClient.custom_interest_path
    )
    parse_custom_interest_path = staticmethod(
        GoogleAdsServiceClient.parse_custom_interest_path
    )
    customizer_attribute_path = staticmethod(
        GoogleAdsServiceClient.customizer_attribute_path
    )
    parse_customizer_attribute_path = staticmethod(
        GoogleAdsServiceClient.parse_customizer_attribute_path
    )
    data_link_path = staticmethod(GoogleAdsServiceClient.data_link_path)
    parse_data_link_path = staticmethod(
        GoogleAdsServiceClient.parse_data_link_path
    )
    detail_content_suitability_placement_view_path = staticmethod(
        GoogleAdsServiceClient.detail_content_suitability_placement_view_path
    )
    parse_detail_content_suitability_placement_view_path = staticmethod(
        GoogleAdsServiceClient.parse_detail_content_suitability_placement_view_path
    )
    detailed_demographic_path = staticmethod(
        GoogleAdsServiceClient.detailed_demographic_path
    )
    parse_detailed_demographic_path = staticmethod(
        GoogleAdsServiceClient.parse_detailed_demographic_path
    )
    detail_placement_view_path = staticmethod(
        GoogleAdsServiceClient.detail_placement_view_path
    )
    parse_detail_placement_view_path = staticmethod(
        GoogleAdsServiceClient.parse_detail_placement_view_path
    )
    display_keyword_view_path = staticmethod(
        GoogleAdsServiceClient.display_keyword_view_path
    )
    parse_display_keyword_view_path = staticmethod(
        GoogleAdsServiceClient.parse_display_keyword_view_path
    )
    distance_view_path = staticmethod(GoogleAdsServiceClient.distance_view_path)
    parse_distance_view_path = staticmethod(
        GoogleAdsServiceClient.parse_distance_view_path
    )
    domain_category_path = staticmethod(
        GoogleAdsServiceClient.domain_category_path
    )
    parse_domain_category_path = staticmethod(
        GoogleAdsServiceClient.parse_domain_category_path
    )
    dynamic_search_ads_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.dynamic_search_ads_search_term_view_path
    )
    parse_dynamic_search_ads_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.parse_dynamic_search_ads_search_term_view_path
    )
    expanded_landing_page_view_path = staticmethod(
        GoogleAdsServiceClient.expanded_landing_page_view_path
    )
    parse_expanded_landing_page_view_path = staticmethod(
        GoogleAdsServiceClient.parse_expanded_landing_page_view_path
    )
    experiment_path = staticmethod(GoogleAdsServiceClient.experiment_path)
    parse_experiment_path = staticmethod(
        GoogleAdsServiceClient.parse_experiment_path
    )
    experiment_arm_path = staticmethod(
        GoogleAdsServiceClient.experiment_arm_path
    )
    parse_experiment_arm_path = staticmethod(
        GoogleAdsServiceClient.parse_experiment_arm_path
    )
    final_url_expansion_asset_view_path = staticmethod(
        GoogleAdsServiceClient.final_url_expansion_asset_view_path
    )
    parse_final_url_expansion_asset_view_path = staticmethod(
        GoogleAdsServiceClient.parse_final_url_expansion_asset_view_path
    )
    gender_view_path = staticmethod(GoogleAdsServiceClient.gender_view_path)
    parse_gender_view_path = staticmethod(
        GoogleAdsServiceClient.parse_gender_view_path
    )
    geographic_view_path = staticmethod(
        GoogleAdsServiceClient.geographic_view_path
    )
    parse_geographic_view_path = staticmethod(
        GoogleAdsServiceClient.parse_geographic_view_path
    )
    geo_target_constant_path = staticmethod(
        GoogleAdsServiceClient.geo_target_constant_path
    )
    parse_geo_target_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_geo_target_constant_path
    )
    goal_path = staticmethod(GoogleAdsServiceClient.goal_path)
    parse_goal_path = staticmethod(GoogleAdsServiceClient.parse_goal_path)
    group_content_suitability_placement_view_path = staticmethod(
        GoogleAdsServiceClient.group_content_suitability_placement_view_path
    )
    parse_group_content_suitability_placement_view_path = staticmethod(
        GoogleAdsServiceClient.parse_group_content_suitability_placement_view_path
    )
    group_placement_view_path = staticmethod(
        GoogleAdsServiceClient.group_placement_view_path
    )
    parse_group_placement_view_path = staticmethod(
        GoogleAdsServiceClient.parse_group_placement_view_path
    )
    hotel_group_view_path = staticmethod(
        GoogleAdsServiceClient.hotel_group_view_path
    )
    parse_hotel_group_view_path = staticmethod(
        GoogleAdsServiceClient.parse_hotel_group_view_path
    )
    hotel_performance_view_path = staticmethod(
        GoogleAdsServiceClient.hotel_performance_view_path
    )
    parse_hotel_performance_view_path = staticmethod(
        GoogleAdsServiceClient.parse_hotel_performance_view_path
    )
    hotel_reconciliation_path = staticmethod(
        GoogleAdsServiceClient.hotel_reconciliation_path
    )
    parse_hotel_reconciliation_path = staticmethod(
        GoogleAdsServiceClient.parse_hotel_reconciliation_path
    )
    income_range_view_path = staticmethod(
        GoogleAdsServiceClient.income_range_view_path
    )
    parse_income_range_view_path = staticmethod(
        GoogleAdsServiceClient.parse_income_range_view_path
    )
    keyword_plan_path = staticmethod(GoogleAdsServiceClient.keyword_plan_path)
    parse_keyword_plan_path = staticmethod(
        GoogleAdsServiceClient.parse_keyword_plan_path
    )
    keyword_plan_ad_group_path = staticmethod(
        GoogleAdsServiceClient.keyword_plan_ad_group_path
    )
    parse_keyword_plan_ad_group_path = staticmethod(
        GoogleAdsServiceClient.parse_keyword_plan_ad_group_path
    )
    keyword_plan_ad_group_keyword_path = staticmethod(
        GoogleAdsServiceClient.keyword_plan_ad_group_keyword_path
    )
    parse_keyword_plan_ad_group_keyword_path = staticmethod(
        GoogleAdsServiceClient.parse_keyword_plan_ad_group_keyword_path
    )
    keyword_plan_campaign_path = staticmethod(
        GoogleAdsServiceClient.keyword_plan_campaign_path
    )
    parse_keyword_plan_campaign_path = staticmethod(
        GoogleAdsServiceClient.parse_keyword_plan_campaign_path
    )
    keyword_plan_campaign_keyword_path = staticmethod(
        GoogleAdsServiceClient.keyword_plan_campaign_keyword_path
    )
    parse_keyword_plan_campaign_keyword_path = staticmethod(
        GoogleAdsServiceClient.parse_keyword_plan_campaign_keyword_path
    )
    keyword_theme_constant_path = staticmethod(
        GoogleAdsServiceClient.keyword_theme_constant_path
    )
    parse_keyword_theme_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_keyword_theme_constant_path
    )
    keyword_view_path = staticmethod(GoogleAdsServiceClient.keyword_view_path)
    parse_keyword_view_path = staticmethod(
        GoogleAdsServiceClient.parse_keyword_view_path
    )
    label_path = staticmethod(GoogleAdsServiceClient.label_path)
    parse_label_path = staticmethod(GoogleAdsServiceClient.parse_label_path)
    landing_page_view_path = staticmethod(
        GoogleAdsServiceClient.landing_page_view_path
    )
    parse_landing_page_view_path = staticmethod(
        GoogleAdsServiceClient.parse_landing_page_view_path
    )
    language_constant_path = staticmethod(
        GoogleAdsServiceClient.language_constant_path
    )
    parse_language_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_language_constant_path
    )
    lead_form_submission_data_path = staticmethod(
        GoogleAdsServiceClient.lead_form_submission_data_path
    )
    parse_lead_form_submission_data_path = staticmethod(
        GoogleAdsServiceClient.parse_lead_form_submission_data_path
    )
    life_event_path = staticmethod(GoogleAdsServiceClient.life_event_path)
    parse_life_event_path = staticmethod(
        GoogleAdsServiceClient.parse_life_event_path
    )
    local_services_employee_path = staticmethod(
        GoogleAdsServiceClient.local_services_employee_path
    )
    parse_local_services_employee_path = staticmethod(
        GoogleAdsServiceClient.parse_local_services_employee_path
    )
    local_services_lead_path = staticmethod(
        GoogleAdsServiceClient.local_services_lead_path
    )
    parse_local_services_lead_path = staticmethod(
        GoogleAdsServiceClient.parse_local_services_lead_path
    )
    local_services_lead_conversation_path = staticmethod(
        GoogleAdsServiceClient.local_services_lead_conversation_path
    )
    parse_local_services_lead_conversation_path = staticmethod(
        GoogleAdsServiceClient.parse_local_services_lead_conversation_path
    )
    local_services_verification_artifact_path = staticmethod(
        GoogleAdsServiceClient.local_services_verification_artifact_path
    )
    parse_local_services_verification_artifact_path = staticmethod(
        GoogleAdsServiceClient.parse_local_services_verification_artifact_path
    )
    location_interest_view_path = staticmethod(
        GoogleAdsServiceClient.location_interest_view_path
    )
    parse_location_interest_view_path = staticmethod(
        GoogleAdsServiceClient.parse_location_interest_view_path
    )
    location_view_path = staticmethod(GoogleAdsServiceClient.location_view_path)
    parse_location_view_path = staticmethod(
        GoogleAdsServiceClient.parse_location_view_path
    )
    managed_placement_view_path = staticmethod(
        GoogleAdsServiceClient.managed_placement_view_path
    )
    parse_managed_placement_view_path = staticmethod(
        GoogleAdsServiceClient.parse_managed_placement_view_path
    )
    matched_location_interest_view_path = staticmethod(
        GoogleAdsServiceClient.matched_location_interest_view_path
    )
    parse_matched_location_interest_view_path = staticmethod(
        GoogleAdsServiceClient.parse_matched_location_interest_view_path
    )
    media_file_path = staticmethod(GoogleAdsServiceClient.media_file_path)
    parse_media_file_path = staticmethod(
        GoogleAdsServiceClient.parse_media_file_path
    )
    mobile_app_category_constant_path = staticmethod(
        GoogleAdsServiceClient.mobile_app_category_constant_path
    )
    parse_mobile_app_category_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_mobile_app_category_constant_path
    )
    mobile_device_constant_path = staticmethod(
        GoogleAdsServiceClient.mobile_device_constant_path
    )
    parse_mobile_device_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_mobile_device_constant_path
    )
    offline_conversion_upload_client_summary_path = staticmethod(
        GoogleAdsServiceClient.offline_conversion_upload_client_summary_path
    )
    parse_offline_conversion_upload_client_summary_path = staticmethod(
        GoogleAdsServiceClient.parse_offline_conversion_upload_client_summary_path
    )
    offline_conversion_upload_conversion_action_summary_path = staticmethod(
        GoogleAdsServiceClient.offline_conversion_upload_conversion_action_summary_path
    )
    parse_offline_conversion_upload_conversion_action_summary_path = staticmethod(
        GoogleAdsServiceClient.parse_offline_conversion_upload_conversion_action_summary_path
    )
    offline_user_data_job_path = staticmethod(
        GoogleAdsServiceClient.offline_user_data_job_path
    )
    parse_offline_user_data_job_path = staticmethod(
        GoogleAdsServiceClient.parse_offline_user_data_job_path
    )
    operating_system_version_constant_path = staticmethod(
        GoogleAdsServiceClient.operating_system_version_constant_path
    )
    parse_operating_system_version_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_operating_system_version_constant_path
    )
    paid_organic_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.paid_organic_search_term_view_path
    )
    parse_paid_organic_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.parse_paid_organic_search_term_view_path
    )
    parental_status_view_path = staticmethod(
        GoogleAdsServiceClient.parental_status_view_path
    )
    parse_parental_status_view_path = staticmethod(
        GoogleAdsServiceClient.parse_parental_status_view_path
    )
    payments_account_path = staticmethod(
        GoogleAdsServiceClient.payments_account_path
    )
    parse_payments_account_path = staticmethod(
        GoogleAdsServiceClient.parse_payments_account_path
    )
    performance_max_placement_view_path = staticmethod(
        GoogleAdsServiceClient.performance_max_placement_view_path
    )
    parse_performance_max_placement_view_path = staticmethod(
        GoogleAdsServiceClient.parse_performance_max_placement_view_path
    )
    per_store_view_path = staticmethod(
        GoogleAdsServiceClient.per_store_view_path
    )
    parse_per_store_view_path = staticmethod(
        GoogleAdsServiceClient.parse_per_store_view_path
    )
    product_category_constant_path = staticmethod(
        GoogleAdsServiceClient.product_category_constant_path
    )
    parse_product_category_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_product_category_constant_path
    )
    product_group_view_path = staticmethod(
        GoogleAdsServiceClient.product_group_view_path
    )
    parse_product_group_view_path = staticmethod(
        GoogleAdsServiceClient.parse_product_group_view_path
    )
    product_link_path = staticmethod(GoogleAdsServiceClient.product_link_path)
    parse_product_link_path = staticmethod(
        GoogleAdsServiceClient.parse_product_link_path
    )
    product_link_invitation_path = staticmethod(
        GoogleAdsServiceClient.product_link_invitation_path
    )
    parse_product_link_invitation_path = staticmethod(
        GoogleAdsServiceClient.parse_product_link_invitation_path
    )
    qualifying_question_path = staticmethod(
        GoogleAdsServiceClient.qualifying_question_path
    )
    parse_qualifying_question_path = staticmethod(
        GoogleAdsServiceClient.parse_qualifying_question_path
    )
    recommendation_path = staticmethod(
        GoogleAdsServiceClient.recommendation_path
    )
    parse_recommendation_path = staticmethod(
        GoogleAdsServiceClient.parse_recommendation_path
    )
    recommendation_subscription_path = staticmethod(
        GoogleAdsServiceClient.recommendation_subscription_path
    )
    parse_recommendation_subscription_path = staticmethod(
        GoogleAdsServiceClient.parse_recommendation_subscription_path
    )
    remarketing_action_path = staticmethod(
        GoogleAdsServiceClient.remarketing_action_path
    )
    parse_remarketing_action_path = staticmethod(
        GoogleAdsServiceClient.parse_remarketing_action_path
    )
    search_term_view_path = staticmethod(
        GoogleAdsServiceClient.search_term_view_path
    )
    parse_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.parse_search_term_view_path
    )
    shared_criterion_path = staticmethod(
        GoogleAdsServiceClient.shared_criterion_path
    )
    parse_shared_criterion_path = staticmethod(
        GoogleAdsServiceClient.parse_shared_criterion_path
    )
    shared_set_path = staticmethod(GoogleAdsServiceClient.shared_set_path)
    parse_shared_set_path = staticmethod(
        GoogleAdsServiceClient.parse_shared_set_path
    )
    shopping_performance_view_path = staticmethod(
        GoogleAdsServiceClient.shopping_performance_view_path
    )
    parse_shopping_performance_view_path = staticmethod(
        GoogleAdsServiceClient.parse_shopping_performance_view_path
    )
    shopping_product_path = staticmethod(
        GoogleAdsServiceClient.shopping_product_path
    )
    parse_shopping_product_path = staticmethod(
        GoogleAdsServiceClient.parse_shopping_product_path
    )
    smart_campaign_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.smart_campaign_search_term_view_path
    )
    parse_smart_campaign_search_term_view_path = staticmethod(
        GoogleAdsServiceClient.parse_smart_campaign_search_term_view_path
    )
    smart_campaign_setting_path = staticmethod(
        GoogleAdsServiceClient.smart_campaign_setting_path
    )
    parse_smart_campaign_setting_path = staticmethod(
        GoogleAdsServiceClient.parse_smart_campaign_setting_path
    )
    targeting_expansion_view_path = staticmethod(
        GoogleAdsServiceClient.targeting_expansion_view_path
    )
    parse_targeting_expansion_view_path = staticmethod(
        GoogleAdsServiceClient.parse_targeting_expansion_view_path
    )
    third_party_app_analytics_link_path = staticmethod(
        GoogleAdsServiceClient.third_party_app_analytics_link_path
    )
    parse_third_party_app_analytics_link_path = staticmethod(
        GoogleAdsServiceClient.parse_third_party_app_analytics_link_path
    )
    topic_constant_path = staticmethod(
        GoogleAdsServiceClient.topic_constant_path
    )
    parse_topic_constant_path = staticmethod(
        GoogleAdsServiceClient.parse_topic_constant_path
    )
    topic_view_path = staticmethod(GoogleAdsServiceClient.topic_view_path)
    parse_topic_view_path = staticmethod(
        GoogleAdsServiceClient.parse_topic_view_path
    )
    travel_activity_group_view_path = staticmethod(
        GoogleAdsServiceClient.travel_activity_group_view_path
    )
    parse_travel_activity_group_view_path = staticmethod(
        GoogleAdsServiceClient.parse_travel_activity_group_view_path
    )
    travel_activity_performance_view_path = staticmethod(
        GoogleAdsServiceClient.travel_activity_performance_view_path
    )
    parse_travel_activity_performance_view_path = staticmethod(
        GoogleAdsServiceClient.parse_travel_activity_performance_view_path
    )
    user_interest_path = staticmethod(GoogleAdsServiceClient.user_interest_path)
    parse_user_interest_path = staticmethod(
        GoogleAdsServiceClient.parse_user_interest_path
    )
    user_list_path = staticmethod(GoogleAdsServiceClient.user_list_path)
    parse_user_list_path = staticmethod(
        GoogleAdsServiceClient.parse_user_list_path
    )
    user_list_customer_type_path = staticmethod(
        GoogleAdsServiceClient.user_list_customer_type_path
    )
    parse_user_list_customer_type_path = staticmethod(
        GoogleAdsServiceClient.parse_user_list_customer_type_path
    )
    user_location_view_path = staticmethod(
        GoogleAdsServiceClient.user_location_view_path
    )
    parse_user_location_view_path = staticmethod(
        GoogleAdsServiceClient.parse_user_location_view_path
    )
    video_path = staticmethod(GoogleAdsServiceClient.video_path)
    parse_video_path = staticmethod(GoogleAdsServiceClient.parse_video_path)
    webpage_view_path = staticmethod(GoogleAdsServiceClient.webpage_view_path)
    parse_webpage_view_path = staticmethod(
        GoogleAdsServiceClient.parse_webpage_view_path
    )
    you_tube_video_upload_path = staticmethod(
        GoogleAdsServiceClient.you_tube_video_upload_path
    )
    parse_you_tube_video_upload_path = staticmethod(
        GoogleAdsServiceClient.parse_you_tube_video_upload_path
    )
    common_billing_account_path = staticmethod(
        GoogleAdsServiceClient.common_billing_account_path
    )
    parse_common_billing_account_path = staticmethod(
        GoogleAdsServiceClient.parse_common_billing_account_path
    )
    common_folder_path = staticmethod(GoogleAdsServiceClient.common_folder_path)
    parse_common_folder_path = staticmethod(
        GoogleAdsServiceClient.parse_common_folder_path
    )
    common_organization_path = staticmethod(
        GoogleAdsServiceClient.common_organization_path
    )
    parse_common_organization_path = staticmethod(
        GoogleAdsServiceClient.parse_common_organization_path
    )
    common_project_path = staticmethod(
        GoogleAdsServiceClient.common_project_path
    )
    parse_common_project_path = staticmethod(
        GoogleAdsServiceClient.parse_common_project_path
    )
    common_location_path = staticmethod(
        GoogleAdsServiceClient.common_location_path
    )
    parse_common_location_path = staticmethod(
        GoogleAdsServiceClient.parse_common_location_path
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
            GoogleAdsServiceAsyncClient: The constructed client.
        """
        return GoogleAdsServiceClient.from_service_account_info.__func__(GoogleAdsServiceAsyncClient, info, *args, **kwargs)  # type: ignore

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
            GoogleAdsServiceAsyncClient: The constructed client.
        """
        return GoogleAdsServiceClient.from_service_account_file.__func__(GoogleAdsServiceAsyncClient, filename, *args, **kwargs)  # type: ignore

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
        return GoogleAdsServiceClient.get_mtls_endpoint_and_cert_source(client_options)  # type: ignore

    @property
    def transport(self) -> GoogleAdsServiceTransport:
        """Returns the transport used by the client instance.

        Returns:
            GoogleAdsServiceTransport: The transport used by the client instance.
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

    get_transport_class = GoogleAdsServiceClient.get_transport_class

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[
                str,
                GoogleAdsServiceTransport,
                Callable[..., GoogleAdsServiceTransport],
            ]
        ] = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the google ads service async client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,GoogleAdsServiceTransport,Callable[..., GoogleAdsServiceTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport to use.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the GoogleAdsServiceTransport constructor.
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
        self._client = GoogleAdsServiceClient(
            credentials=credentials,
            transport=transport,
            client_options=client_options,
            client_info=client_info,
        )

        if CLIENT_LOGGING_SUPPORTED and _LOGGER.isEnabledFor(
            std_logging.DEBUG
        ):  # pragma: NO COVER
            _LOGGER.debug(
                "Created client `google.ads.googleads.v23.services.GoogleAdsServiceAsyncClient`.",
                extra=(
                    {
                        "serviceName": "google.ads.googleads.v23.services.GoogleAdsService",
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
                        "serviceName": "google.ads.googleads.v23.services.GoogleAdsService",
                        "credentialsType": None,
                    }
                ),
            )

    async def search(
        self,
        request: Optional[
            Union[google_ads_service.SearchGoogleAdsRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        query: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> pagers.SearchAsyncPager:
        r"""Returns all rows that match the search query.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `ChangeEventError <>`__
        `ChangeStatusError <>`__ `ClickViewError <>`__
        `HeaderError <>`__ `InternalError <>`__ `QueryError <>`__
        `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.SearchGoogleAdsRequest, dict]]):
                The request object. Request message for
                [GoogleAdsService.Search][google.ads.googleads.v23.services.GoogleAdsService.Search].
            customer_id (:class:`str`):
                Required. The ID of the customer
                being queried.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            query (:class:`str`):
                Required. The query string.
                This corresponds to the ``query`` field
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
            google.ads.googleads.v23.services.services.google_ads_service.pagers.SearchAsyncPager:
                Response message for
                   [GoogleAdsService.Search][google.ads.googleads.v23.services.GoogleAdsService.Search].

                Iterating over this object will yield results and
                resolve additional pages automatically.

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, query]
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
        if not isinstance(request, google_ads_service.SearchGoogleAdsRequest):
            request = google_ads_service.SearchGoogleAdsRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if query is not None:
            request.query = query

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.search
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

        # This method is paged; wrap the response in a pager, which provides
        # an `__aiter__` convenience method.
        response = pagers.SearchAsyncPager(
            method=rpc,
            request=request,
            response=response,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    def search_stream(
        self,
        request: Optional[
            Union[google_ads_service.SearchGoogleAdsStreamRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        query: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> Awaitable[
        AsyncIterable[google_ads_service.SearchGoogleAdsStreamResponse]
    ]:
        r"""Returns all rows that match the search stream query.

        List of thrown errors: `AuthenticationError <>`__
        `AuthorizationError <>`__ `ChangeEventError <>`__
        `ChangeStatusError <>`__ `ClickViewError <>`__
        `HeaderError <>`__ `InternalError <>`__ `QueryError <>`__
        `QuotaError <>`__ `RequestError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.SearchGoogleAdsStreamRequest, dict]]):
                The request object. Request message for
                [GoogleAdsService.SearchStream][google.ads.googleads.v23.services.GoogleAdsService.SearchStream].
            customer_id (:class:`str`):
                Required. The ID of the customer
                being queried.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            query (:class:`str`):
                Required. The query string.
                This corresponds to the ``query`` field
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
            AsyncIterable[google.ads.googleads.v23.services.types.SearchGoogleAdsStreamResponse]:
                Response message for
                   [GoogleAdsService.SearchStream][google.ads.googleads.v23.services.GoogleAdsService.SearchStream].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, query]
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
            request, google_ads_service.SearchGoogleAdsStreamRequest
        ):
            request = google_ads_service.SearchGoogleAdsStreamRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if query is not None:
            request.query = query

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.search_stream
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
        response = rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    async def mutate(
        self,
        request: Optional[
            Union[google_ads_service.MutateGoogleAdsRequest, dict]
        ] = None,
        *,
        customer_id: Optional[str] = None,
        mutate_operations: Optional[
            MutableSequence[google_ads_service.MutateOperation]
        ] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> google_ads_service.MutateGoogleAdsResponse:
        r"""Creates, updates, or removes resources. This method supports
        atomic transactions with multiple types of resources. For
        example, you can atomically create a campaign and a campaign
        budget, or perform up to thousands of mutates atomically.

        This method is essentially a wrapper around a series of mutate
        methods. The only features it offers over calling those methods
        directly are:

        - Atomic transactions
        - Temp resource names (described below)
        - Somewhat reduced latency over making a series of mutate calls

        Note: Only resources that support atomic transactions are
        included, so this method can't replace all calls to individual
        services.

        Atomic Transaction Benefits
        ---------------------------

        Atomicity makes error handling much easier. If you're making a
        series of changes and one fails, it can leave your account in an
        inconsistent state. With atomicity, you either reach the chosen
        state directly, or the request fails and you can retry.

        Temp Resource Names
        -------------------

        Temp resource names are a special type of resource name used to
        create a resource and reference that resource in the same
        request. For example, if a campaign budget is created with
        ``resource_name`` equal to ``customers/123/campaignBudgets/-1``,
        that resource name can be reused in the ``Campaign.budget``
        field in the same request. That way, the two resources are
        created and linked atomically.

        To create a temp resource name, put a negative number in the
        part of the name that the server would normally allocate.

        Note:

        - Resources must be created with a temp name before the name can
          be reused. For example, the previous CampaignBudget+Campaign
          example would fail if the mutate order was reversed.
        - Temp names are not remembered across requests.
        - There's no limit to the number of temp names in a request.
        - Each temp name must use a unique negative number, even if the
          resource types differ.

        Latency
        -------

        It's important to group mutates by resource type or the request
        may time out and fail. Latency is roughly equal to a series of
        calls to individual mutate methods, where each change in
        resource type is a new call. For example, mutating 10 campaigns
        then 10 ad groups is like 2 calls, while mutating 1 campaign, 1
        ad group, 1 campaign, 1 ad group is like 4 calls.

        List of thrown errors: `AdCustomizerError <>`__ `AdError <>`__
        `AdGroupAdError <>`__ `AdGroupCriterionError <>`__
        `AdGroupError <>`__ `AssetError <>`__ `AuthenticationError <>`__
        `AuthorizationError <>`__ `BiddingError <>`__
        `CampaignBudgetError <>`__ `CampaignCriterionError <>`__
        `CampaignError <>`__ `CampaignExperimentError <>`__
        `CampaignSharedSetError <>`__ `CollectionSizeError <>`__
        `ContextError <>`__ `ConversionActionError <>`__
        `CriterionError <>`__ `CustomerFeedError <>`__
        `DatabaseError <>`__ `DateError <>`__ `DateRangeError <>`__
        `DistinctError <>`__ `ExtensionFeedItemError <>`__
        `ExtensionSettingError <>`__ `FeedAttributeReferenceError <>`__
        `FeedError <>`__ `FeedItemError <>`__ `FeedItemSetError <>`__
        `FieldError <>`__ `FieldMaskError <>`__
        `FunctionParsingError <>`__ `HeaderError <>`__ `ImageError <>`__
        `InternalError <>`__ `KeywordPlanAdGroupKeywordError <>`__
        `KeywordPlanCampaignError <>`__ `KeywordPlanError <>`__
        `LabelError <>`__ `ListOperationError <>`__
        `MediaUploadError <>`__ `MutateError <>`__
        `NewResourceCreationError <>`__ `NullError <>`__
        `OperationAccessDeniedError <>`__ `PolicyFindingError <>`__
        `PolicyViolationError <>`__ `QuotaError <>`__ `RangeError <>`__
        `RequestError <>`__ `ResourceCountLimitExceededError <>`__
        `SettingError <>`__ `SharedSetError <>`__ `SizeLimitError <>`__
        `StringFormatError <>`__ `StringLengthError <>`__
        `UrlFieldError <>`__ `UserListError <>`__
        `YoutubeVideoRegistrationError <>`__

        Args:
            request (Optional[Union[google.ads.googleads.v23.services.types.MutateGoogleAdsRequest, dict]]):
                The request object. Request message for
                [GoogleAdsService.Mutate][google.ads.googleads.v23.services.GoogleAdsService.Mutate].
            customer_id (:class:`str`):
                Required. The ID of the customer
                whose resources are being modified.

                This corresponds to the ``customer_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            mutate_operations (:class:`MutableSequence[google.ads.googleads.v23.services.types.MutateOperation]`):
                Required. The list of operations to
                perform on individual resources.

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
            google.ads.googleads.v23.services.types.MutateGoogleAdsResponse:
                Response message for
                   [GoogleAdsService.Mutate][google.ads.googleads.v23.services.GoogleAdsService.Mutate].

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        flattened_params = [customer_id, mutate_operations]
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
        if not isinstance(request, google_ads_service.MutateGoogleAdsRequest):
            request = google_ads_service.MutateGoogleAdsRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if customer_id is not None:
            request.customer_id = customer_id
        if mutate_operations:
            request.mutate_operations.extend(mutate_operations)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.mutate
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

    async def __aenter__(self) -> "GoogleAdsServiceAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)

if hasattr(DEFAULT_CLIENT_INFO, "protobuf_runtime_version"):  # pragma: NO COVER
    DEFAULT_CLIENT_INFO.protobuf_runtime_version = google.protobuf.__version__


__all__ = ("GoogleAdsServiceAsyncClient",)

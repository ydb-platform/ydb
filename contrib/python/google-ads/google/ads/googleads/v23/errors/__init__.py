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
from google.ads.googleads.v23 import gapic_version as package_version

import google.api_core as api_core
import sys

__version__ = package_version.__version__

if sys.version_info >= (3, 8):  # pragma: NO COVER
    from importlib import metadata
else:  # pragma: NO COVER
    # TODO(https://github.com/googleapis/python-api-core/issues/835): Remove
    # this code path once we drop support for Python 3.7
    import importlib_metadata as metadata


from .types.access_invitation_error import AccessInvitationErrorEnum
from .types.account_budget_proposal_error import AccountBudgetProposalErrorEnum
from .types.account_link_error import AccountLinkErrorEnum
from .types.ad_customizer_error import AdCustomizerErrorEnum
from .types.ad_error import AdErrorEnum
from .types.ad_group_ad_error import AdGroupAdErrorEnum
from .types.ad_group_bid_modifier_error import AdGroupBidModifierErrorEnum
from .types.ad_group_criterion_customizer_error import (
    AdGroupCriterionCustomizerErrorEnum,
)
from .types.ad_group_criterion_error import AdGroupCriterionErrorEnum
from .types.ad_group_customizer_error import AdGroupCustomizerErrorEnum
from .types.ad_group_error import AdGroupErrorEnum
from .types.ad_group_feed_error import AdGroupFeedErrorEnum
from .types.ad_parameter_error import AdParameterErrorEnum
from .types.ad_sharing_error import AdSharingErrorEnum
from .types.adx_error import AdxErrorEnum
from .types.asset_error import AssetErrorEnum
from .types.asset_generation_error import AssetGenerationErrorEnum
from .types.asset_group_asset_error import AssetGroupAssetErrorEnum
from .types.asset_group_error import AssetGroupErrorEnum
from .types.asset_group_listing_group_filter_error import (
    AssetGroupListingGroupFilterErrorEnum,
)
from .types.asset_group_signal_error import AssetGroupSignalErrorEnum
from .types.asset_link_error import AssetLinkErrorEnum
from .types.asset_set_asset_error import AssetSetAssetErrorEnum
from .types.asset_set_error import AssetSetErrorEnum
from .types.asset_set_link_error import AssetSetLinkErrorEnum
from .types.audience_error import AudienceErrorEnum
from .types.audience_insights_error import AudienceInsightsErrorEnum
from .types.authentication_error import AuthenticationErrorEnum
from .types.authorization_error import AuthorizationErrorEnum
from .types.automatically_created_asset_removal_error import (
    AutomaticallyCreatedAssetRemovalErrorEnum,
)
from .types.batch_job_error import BatchJobErrorEnum
from .types.benchmarks_error import BenchmarksErrorEnum
from .types.bidding_error import BiddingErrorEnum
from .types.bidding_strategy_error import BiddingStrategyErrorEnum
from .types.billing_setup_error import BillingSetupErrorEnum
from .types.brand_guidelines_migration_error import (
    BrandGuidelinesMigrationErrorEnum,
)
from .types.campaign_budget_error import CampaignBudgetErrorEnum
from .types.campaign_conversion_goal_error import (
    CampaignConversionGoalErrorEnum,
)
from .types.campaign_criterion_error import CampaignCriterionErrorEnum
from .types.campaign_customizer_error import CampaignCustomizerErrorEnum
from .types.campaign_draft_error import CampaignDraftErrorEnum
from .types.campaign_error import CampaignErrorEnum
from .types.campaign_experiment_error import CampaignExperimentErrorEnum
from .types.campaign_feed_error import CampaignFeedErrorEnum
from .types.campaign_goal_config_error import CampaignGoalConfigErrorEnum
from .types.campaign_lifecycle_goal_error import CampaignLifecycleGoalErrorEnum
from .types.campaign_shared_set_error import CampaignSharedSetErrorEnum
from .types.change_event_error import ChangeEventErrorEnum
from .types.change_status_error import ChangeStatusErrorEnum
from .types.click_view_error import ClickViewErrorEnum
from .types.collection_size_error import CollectionSizeErrorEnum
from .types.context_error import ContextErrorEnum
from .types.conversion_action_error import ConversionActionErrorEnum
from .types.conversion_adjustment_upload_error import (
    ConversionAdjustmentUploadErrorEnum,
)
from .types.conversion_custom_variable_error import (
    ConversionCustomVariableErrorEnum,
)
from .types.conversion_goal_campaign_config_error import (
    ConversionGoalCampaignConfigErrorEnum,
)
from .types.conversion_upload_error import ConversionUploadErrorEnum
from .types.conversion_value_rule_error import ConversionValueRuleErrorEnum
from .types.conversion_value_rule_set_error import (
    ConversionValueRuleSetErrorEnum,
)
from .types.country_code_error import CountryCodeErrorEnum
from .types.criterion_error import CriterionErrorEnum
from .types.currency_code_error import CurrencyCodeErrorEnum
from .types.currency_error import CurrencyErrorEnum
from .types.custom_audience_error import CustomAudienceErrorEnum
from .types.custom_conversion_goal_error import CustomConversionGoalErrorEnum
from .types.custom_interest_error import CustomInterestErrorEnum
from .types.customer_client_link_error import CustomerClientLinkErrorEnum
from .types.customer_customizer_error import CustomerCustomizerErrorEnum
from .types.customer_error import CustomerErrorEnum
from .types.customer_feed_error import CustomerFeedErrorEnum
from .types.customer_lifecycle_goal_error import CustomerLifecycleGoalErrorEnum
from .types.customer_manager_link_error import CustomerManagerLinkErrorEnum
from .types.customer_sk_ad_network_conversion_value_schema_error import (
    CustomerSkAdNetworkConversionValueSchemaErrorEnum,
)
from .types.customer_user_access_error import CustomerUserAccessErrorEnum
from .types.customizer_attribute_error import CustomizerAttributeErrorEnum
from .types.data_link_error import DataLinkErrorEnum
from .types.database_error import DatabaseErrorEnum
from .types.date_error import DateErrorEnum
from .types.date_range_error import DateRangeErrorEnum
from .types.distinct_error import DistinctErrorEnum
from .types.enum_error import EnumErrorEnum
from .types.errors import BudgetPerDayMinimumErrorDetails
from .types.errors import ErrorCode
from .types.errors import ErrorDetails
from .types.errors import ErrorLocation
from .types.errors import GoogleAdsError
from .types.errors import GoogleAdsFailure
from .types.errors import PolicyFindingDetails
from .types.errors import PolicyViolationDetails
from .types.errors import QuotaErrorDetails
from .types.errors import ResourceCountDetails
from .types.experiment_arm_error import ExperimentArmErrorEnum
from .types.experiment_error import ExperimentErrorEnum
from .types.extension_feed_item_error import ExtensionFeedItemErrorEnum
from .types.extension_setting_error import ExtensionSettingErrorEnum
from .types.feed_attribute_reference_error import (
    FeedAttributeReferenceErrorEnum,
)
from .types.feed_error import FeedErrorEnum
from .types.feed_item_error import FeedItemErrorEnum
from .types.feed_item_set_error import FeedItemSetErrorEnum
from .types.feed_item_set_link_error import FeedItemSetLinkErrorEnum
from .types.feed_item_target_error import FeedItemTargetErrorEnum
from .types.feed_item_validation_error import FeedItemValidationErrorEnum
from .types.feed_mapping_error import FeedMappingErrorEnum
from .types.field_error import FieldErrorEnum
from .types.field_mask_error import FieldMaskErrorEnum
from .types.final_url_expansion_asset_view_error import (
    FinalUrlExpansionAssetViewErrorEnum,
)
from .types.function_error import FunctionErrorEnum
from .types.function_parsing_error import FunctionParsingErrorEnum
from .types.geo_target_constant_suggestion_error import (
    GeoTargetConstantSuggestionErrorEnum,
)
from .types.goal_error import GoalErrorEnum
from .types.header_error import HeaderErrorEnum
from .types.id_error import IdErrorEnum
from .types.identity_verification_error import IdentityVerificationErrorEnum
from .types.image_error import ImageErrorEnum
from .types.incentive_error import IncentiveErrorEnum
from .types.internal_error import InternalErrorEnum
from .types.invoice_error import InvoiceErrorEnum
from .types.keyword_plan_ad_group_error import KeywordPlanAdGroupErrorEnum
from .types.keyword_plan_ad_group_keyword_error import (
    KeywordPlanAdGroupKeywordErrorEnum,
)
from .types.keyword_plan_campaign_error import KeywordPlanCampaignErrorEnum
from .types.keyword_plan_campaign_keyword_error import (
    KeywordPlanCampaignKeywordErrorEnum,
)
from .types.keyword_plan_error import KeywordPlanErrorEnum
from .types.keyword_plan_idea_error import KeywordPlanIdeaErrorEnum
from .types.label_error import LabelErrorEnum
from .types.language_code_error import LanguageCodeErrorEnum
from .types.list_operation_error import ListOperationErrorEnum
from .types.manager_link_error import ManagerLinkErrorEnum
from .types.media_bundle_error import MediaBundleErrorEnum
from .types.media_file_error import MediaFileErrorEnum
from .types.media_upload_error import MediaUploadErrorEnum
from .types.merchant_center_error import MerchantCenterErrorEnum
from .types.multiplier_error import MultiplierErrorEnum
from .types.mutate_error import MutateErrorEnum
from .types.new_resource_creation_error import NewResourceCreationErrorEnum
from .types.not_allowlisted_error import NotAllowlistedErrorEnum
from .types.not_empty_error import NotEmptyErrorEnum
from .types.null_error import NullErrorEnum
from .types.offline_user_data_job_error import OfflineUserDataJobErrorEnum
from .types.operation_access_denied_error import OperationAccessDeniedErrorEnum
from .types.operator_error import OperatorErrorEnum
from .types.partial_failure_error import PartialFailureErrorEnum
from .types.payments_account_error import PaymentsAccountErrorEnum
from .types.policy_finding_error import PolicyFindingErrorEnum
from .types.policy_validation_parameter_error import (
    PolicyValidationParameterErrorEnum,
)
from .types.policy_violation_error import PolicyViolationErrorEnum
from .types.product_link_error import ProductLinkErrorEnum
from .types.product_link_invitation_error import ProductLinkInvitationErrorEnum
from .types.query_error import QueryErrorEnum
from .types.quota_error import QuotaErrorEnum
from .types.range_error import RangeErrorEnum
from .types.reach_plan_error import ReachPlanErrorEnum
from .types.recommendation_error import RecommendationErrorEnum
from .types.recommendation_subscription_error import (
    RecommendationSubscriptionErrorEnum,
)
from .types.region_code_error import RegionCodeErrorEnum
from .types.request_error import RequestErrorEnum
from .types.resource_access_denied_error import ResourceAccessDeniedErrorEnum
from .types.resource_count_limit_exceeded_error import (
    ResourceCountLimitExceededErrorEnum,
)
from .types.search_term_insight_error import SearchTermInsightErrorEnum
from .types.setting_error import SettingErrorEnum
from .types.shareable_preview_error import ShareablePreviewErrorEnum
from .types.shared_criterion_error import SharedCriterionErrorEnum
from .types.shared_set_error import SharedSetErrorEnum
from .types.shopping_product_error import ShoppingProductErrorEnum
from .types.size_limit_error import SizeLimitErrorEnum
from .types.smart_campaign_error import SmartCampaignErrorEnum
from .types.string_format_error import StringFormatErrorEnum
from .types.string_length_error import StringLengthErrorEnum
from .types.third_party_app_analytics_link_error import (
    ThirdPartyAppAnalyticsLinkErrorEnum,
)
from .types.time_zone_error import TimeZoneErrorEnum
from .types.url_field_error import UrlFieldErrorEnum
from .types.user_data_error import UserDataErrorEnum
from .types.user_list_customer_type_error import UserListCustomerTypeErrorEnum
from .types.user_list_error import UserListErrorEnum
from .types.video_campaign_error import VideoCampaignErrorEnum
from .types.youtube_video_registration_error import (
    YoutubeVideoRegistrationErrorEnum,
)

if hasattr(api_core, "check_python_version") and hasattr(
    api_core, "check_dependency_versions"
):  # pragma: NO COVER
    api_core.check_python_version("google.ads.googleads.v23")  # type: ignore
    api_core.check_dependency_versions("google.ads.googleads.v23")  # type: ignore
else:  # pragma: NO COVER
    # An older version of api_core is installed which does not define the
    # functions above. We do equivalent checks manually.
    try:
        import warnings
        import sys

        _py_version_str = sys.version.split()[0]
        _package_label = "google.ads.googleads.v23"
        if sys.version_info < (3, 9):
            warnings.warn(
                "You are using a non-supported Python version "
                + f"({_py_version_str}).  Google will not post any further "
                + f"updates to {_package_label} supporting this Python version. "
                + "Please upgrade to the latest Python version, or at "
                + f"least to Python 3.9, and then update {_package_label}.",
                FutureWarning,
            )
        if sys.version_info[:2] == (3, 9):
            warnings.warn(
                f"You are using a Python version ({_py_version_str}) "
                + f"which Google will stop supporting in {_package_label} in "
                + "January 2026. Please "
                + "upgrade to the latest Python version, or at "
                + "least to Python 3.10, before then, and "
                + f"then update {_package_label}.",
                FutureWarning,
            )

        def parse_version_to_tuple(version_string: str):
            """Safely converts a semantic version string to a comparable tuple of integers.
            Example: "4.25.8" -> (4, 25, 8)
            Ignores non-numeric parts and handles common version formats.
            Args:
                version_string: Version string in the format "x.y.z" or "x.y.z<suffix>"
            Returns:
                Tuple of integers for the parsed version string.
            """
            parts = []
            for part in version_string.split("."):
                try:
                    parts.append(int(part))
                except ValueError:
                    # If it's a non-numeric part (e.g., '1.0.0b1' -> 'b1'), stop here.
                    # This is a simplification compared to 'packaging.parse_version', but sufficient
                    # for comparing strictly numeric semantic versions.
                    break
            return tuple(parts)

        def _get_version(dependency_name):
            try:
                version_string: str = metadata.version(dependency_name)
                parsed_version = parse_version_to_tuple(version_string)
                return (parsed_version, version_string)
            except Exception:
                # Catch exceptions from metadata.version() (e.g., PackageNotFoundError)
                # or errors during parse_version_to_tuple
                return (None, "--")

        _dependency_package = "google.protobuf"
        _next_supported_version = "4.25.8"
        _next_supported_version_tuple = (4, 25, 8)
        _recommendation = " (we recommend 6.x)"
        (_version_used, _version_used_string) = _get_version(
            _dependency_package
        )
        if _version_used and _version_used < _next_supported_version_tuple:
            warnings.warn(
                f"Package {_package_label} depends on "
                + f"{_dependency_package}, currently installed at version "
                + f"{_version_used_string}. Future updates to "
                + f"{_package_label} will require {_dependency_package} at "
                + f"version {_next_supported_version} or higher{_recommendation}."
                + " Please ensure "
                + "that either (a) your Python environment doesn't pin the "
                + f"version of {_dependency_package}, so that updates to "
                + f"{_package_label} can require the higher version, or "
                + "(b) you manually update your Python environment to use at "
                + f"least version {_next_supported_version} of "
                + f"{_dependency_package}.",
                FutureWarning,
            )
    except Exception:
        warnings.warn(
            "Could not determine the version of Python "
            + "currently being used. To continue receiving "
            + "updates for {_package_label}, ensure you are "
            + "using a supported version of Python; see "
            + "https://devguide.python.org/versions/"
        )

__all__ = (
    "AccessInvitationErrorEnum",
    "AccountBudgetProposalErrorEnum",
    "AccountLinkErrorEnum",
    "AdCustomizerErrorEnum",
    "AdErrorEnum",
    "AdGroupAdErrorEnum",
    "AdGroupBidModifierErrorEnum",
    "AdGroupCriterionCustomizerErrorEnum",
    "AdGroupCriterionErrorEnum",
    "AdGroupCustomizerErrorEnum",
    "AdGroupErrorEnum",
    "AdGroupFeedErrorEnum",
    "AdParameterErrorEnum",
    "AdSharingErrorEnum",
    "AdxErrorEnum",
    "AssetErrorEnum",
    "AssetGenerationErrorEnum",
    "AssetGroupAssetErrorEnum",
    "AssetGroupErrorEnum",
    "AssetGroupListingGroupFilterErrorEnum",
    "AssetGroupSignalErrorEnum",
    "AssetLinkErrorEnum",
    "AssetSetAssetErrorEnum",
    "AssetSetErrorEnum",
    "AssetSetLinkErrorEnum",
    "AudienceErrorEnum",
    "AudienceInsightsErrorEnum",
    "AuthenticationErrorEnum",
    "AuthorizationErrorEnum",
    "AutomaticallyCreatedAssetRemovalErrorEnum",
    "BatchJobErrorEnum",
    "BenchmarksErrorEnum",
    "BiddingErrorEnum",
    "BiddingStrategyErrorEnum",
    "BillingSetupErrorEnum",
    "BrandGuidelinesMigrationErrorEnum",
    "BudgetPerDayMinimumErrorDetails",
    "CampaignBudgetErrorEnum",
    "CampaignConversionGoalErrorEnum",
    "CampaignCriterionErrorEnum",
    "CampaignCustomizerErrorEnum",
    "CampaignDraftErrorEnum",
    "CampaignErrorEnum",
    "CampaignExperimentErrorEnum",
    "CampaignFeedErrorEnum",
    "CampaignGoalConfigErrorEnum",
    "CampaignLifecycleGoalErrorEnum",
    "CampaignSharedSetErrorEnum",
    "ChangeEventErrorEnum",
    "ChangeStatusErrorEnum",
    "ClickViewErrorEnum",
    "CollectionSizeErrorEnum",
    "ContextErrorEnum",
    "ConversionActionErrorEnum",
    "ConversionAdjustmentUploadErrorEnum",
    "ConversionCustomVariableErrorEnum",
    "ConversionGoalCampaignConfigErrorEnum",
    "ConversionUploadErrorEnum",
    "ConversionValueRuleErrorEnum",
    "ConversionValueRuleSetErrorEnum",
    "CountryCodeErrorEnum",
    "CriterionErrorEnum",
    "CurrencyCodeErrorEnum",
    "CurrencyErrorEnum",
    "CustomAudienceErrorEnum",
    "CustomConversionGoalErrorEnum",
    "CustomInterestErrorEnum",
    "CustomerClientLinkErrorEnum",
    "CustomerCustomizerErrorEnum",
    "CustomerErrorEnum",
    "CustomerFeedErrorEnum",
    "CustomerLifecycleGoalErrorEnum",
    "CustomerManagerLinkErrorEnum",
    "CustomerSkAdNetworkConversionValueSchemaErrorEnum",
    "CustomerUserAccessErrorEnum",
    "CustomizerAttributeErrorEnum",
    "DataLinkErrorEnum",
    "DatabaseErrorEnum",
    "DateErrorEnum",
    "DateRangeErrorEnum",
    "DistinctErrorEnum",
    "EnumErrorEnum",
    "ErrorCode",
    "ErrorDetails",
    "ErrorLocation",
    "ExperimentArmErrorEnum",
    "ExperimentErrorEnum",
    "ExtensionFeedItemErrorEnum",
    "ExtensionSettingErrorEnum",
    "FeedAttributeReferenceErrorEnum",
    "FeedErrorEnum",
    "FeedItemErrorEnum",
    "FeedItemSetErrorEnum",
    "FeedItemSetLinkErrorEnum",
    "FeedItemTargetErrorEnum",
    "FeedItemValidationErrorEnum",
    "FeedMappingErrorEnum",
    "FieldErrorEnum",
    "FieldMaskErrorEnum",
    "FinalUrlExpansionAssetViewErrorEnum",
    "FunctionErrorEnum",
    "FunctionParsingErrorEnum",
    "GeoTargetConstantSuggestionErrorEnum",
    "GoalErrorEnum",
    "GoogleAdsError",
    "GoogleAdsFailure",
    "HeaderErrorEnum",
    "IdErrorEnum",
    "IdentityVerificationErrorEnum",
    "ImageErrorEnum",
    "IncentiveErrorEnum",
    "InternalErrorEnum",
    "InvoiceErrorEnum",
    "KeywordPlanAdGroupErrorEnum",
    "KeywordPlanAdGroupKeywordErrorEnum",
    "KeywordPlanCampaignErrorEnum",
    "KeywordPlanCampaignKeywordErrorEnum",
    "KeywordPlanErrorEnum",
    "KeywordPlanIdeaErrorEnum",
    "LabelErrorEnum",
    "LanguageCodeErrorEnum",
    "ListOperationErrorEnum",
    "ManagerLinkErrorEnum",
    "MediaBundleErrorEnum",
    "MediaFileErrorEnum",
    "MediaUploadErrorEnum",
    "MerchantCenterErrorEnum",
    "MultiplierErrorEnum",
    "MutateErrorEnum",
    "NewResourceCreationErrorEnum",
    "NotAllowlistedErrorEnum",
    "NotEmptyErrorEnum",
    "NullErrorEnum",
    "OfflineUserDataJobErrorEnum",
    "OperationAccessDeniedErrorEnum",
    "OperatorErrorEnum",
    "PartialFailureErrorEnum",
    "PaymentsAccountErrorEnum",
    "PolicyFindingDetails",
    "PolicyFindingErrorEnum",
    "PolicyValidationParameterErrorEnum",
    "PolicyViolationDetails",
    "PolicyViolationErrorEnum",
    "ProductLinkErrorEnum",
    "ProductLinkInvitationErrorEnum",
    "QueryErrorEnum",
    "QuotaErrorDetails",
    "QuotaErrorEnum",
    "RangeErrorEnum",
    "ReachPlanErrorEnum",
    "RecommendationErrorEnum",
    "RecommendationSubscriptionErrorEnum",
    "RegionCodeErrorEnum",
    "RequestErrorEnum",
    "ResourceAccessDeniedErrorEnum",
    "ResourceCountDetails",
    "ResourceCountLimitExceededErrorEnum",
    "SearchTermInsightErrorEnum",
    "SettingErrorEnum",
    "ShareablePreviewErrorEnum",
    "SharedCriterionErrorEnum",
    "SharedSetErrorEnum",
    "ShoppingProductErrorEnum",
    "SizeLimitErrorEnum",
    "SmartCampaignErrorEnum",
    "StringFormatErrorEnum",
    "StringLengthErrorEnum",
    "ThirdPartyAppAnalyticsLinkErrorEnum",
    "TimeZoneErrorEnum",
    "UrlFieldErrorEnum",
    "UserDataErrorEnum",
    "UserListCustomerTypeErrorEnum",
    "UserListErrorEnum",
    "VideoCampaignErrorEnum",
    "YoutubeVideoRegistrationErrorEnum",
)

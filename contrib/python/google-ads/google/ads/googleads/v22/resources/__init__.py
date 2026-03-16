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
from google.ads.googleads.v22 import gapic_version as package_version

import google.api_core as api_core
import sys

__version__ = package_version.__version__

if sys.version_info >= (3, 8):  # pragma: NO COVER
    from importlib import metadata
else:  # pragma: NO COVER
    # TODO(https://github.com/googleapis/python-api-core/issues/835): Remove
    # this code path once we drop support for Python 3.7
    import importlib_metadata as metadata


from .types.accessible_bidding_strategy import AccessibleBiddingStrategy
from .types.account_budget import AccountBudget
from .types.account_budget_proposal import AccountBudgetProposal
from .types.account_link import AccountLink
from .types.account_link import ThirdPartyAppAnalyticsLinkIdentifier
from .types.ad import Ad
from .types.ad_group import AdGroup
from .types.ad_group_ad import AdGroupAd
from .types.ad_group_ad import AdGroupAdAssetAutomationSetting
from .types.ad_group_ad import AdGroupAdPolicySummary
from .types.ad_group_ad_asset_combination_view import (
    AdGroupAdAssetCombinationView,
)
from .types.ad_group_ad_asset_view import AdGroupAdAssetPolicySummary
from .types.ad_group_ad_asset_view import AdGroupAdAssetView
from .types.ad_group_ad_label import AdGroupAdLabel
from .types.ad_group_asset import AdGroupAsset
from .types.ad_group_asset_set import AdGroupAssetSet
from .types.ad_group_audience_view import AdGroupAudienceView
from .types.ad_group_bid_modifier import AdGroupBidModifier
from .types.ad_group_criterion import AdGroupCriterion
from .types.ad_group_criterion_customizer import AdGroupCriterionCustomizer
from .types.ad_group_criterion_label import AdGroupCriterionLabel
from .types.ad_group_criterion_simulation import AdGroupCriterionSimulation
from .types.ad_group_customizer import AdGroupCustomizer
from .types.ad_group_label import AdGroupLabel
from .types.ad_group_simulation import AdGroupSimulation
from .types.ad_parameter import AdParameter
from .types.ad_schedule_view import AdScheduleView
from .types.age_range_view import AgeRangeView
from .types.ai_max_search_term_ad_combination_view import (
    AiMaxSearchTermAdCombinationView,
)
from .types.android_privacy_shared_key_google_ad_group import (
    AndroidPrivacySharedKeyGoogleAdGroup,
)
from .types.android_privacy_shared_key_google_campaign import (
    AndroidPrivacySharedKeyGoogleCampaign,
)
from .types.android_privacy_shared_key_google_network_type import (
    AndroidPrivacySharedKeyGoogleNetworkType,
)
from .types.asset import Asset
from .types.asset import AssetFieldTypePolicySummary
from .types.asset import AssetPolicySummary
from .types.asset_field_type_view import AssetFieldTypeView
from .types.asset_group import AdStrengthActionItem
from .types.asset_group import AssetCoverage
from .types.asset_group import AssetGroup
from .types.asset_group_asset import AssetGroupAsset
from .types.asset_group_listing_group_filter import AssetGroupListingGroupFilter
from .types.asset_group_listing_group_filter import ListingGroupFilterDimension
from .types.asset_group_listing_group_filter import (
    ListingGroupFilterDimensionPath,
)
from .types.asset_group_product_group_view import AssetGroupProductGroupView
from .types.asset_group_signal import AssetGroupSignal
from .types.asset_group_top_combination_view import (
    AssetGroupAssetCombinationData,
)
from .types.asset_group_top_combination_view import AssetGroupTopCombinationView
from .types.asset_set import AssetSet
from .types.asset_set_asset import AssetSetAsset
from .types.asset_set_type_view import AssetSetTypeView
from .types.audience import Audience
from .types.batch_job import BatchJob
from .types.bidding_data_exclusion import BiddingDataExclusion
from .types.bidding_seasonality_adjustment import BiddingSeasonalityAdjustment
from .types.bidding_strategy import BiddingStrategy
from .types.bidding_strategy_simulation import BiddingStrategySimulation
from .types.billing_setup import BillingSetup
from .types.call_view import CallView
from .types.campaign import Campaign
from .types.campaign_aggregate_asset_view import CampaignAggregateAssetView
from .types.campaign_asset import CampaignAsset
from .types.campaign_asset_set import CampaignAssetSet
from .types.campaign_audience_view import CampaignAudienceView
from .types.campaign_bid_modifier import CampaignBidModifier
from .types.campaign_budget import CampaignBudget
from .types.campaign_conversion_goal import CampaignConversionGoal
from .types.campaign_criterion import CampaignCriterion
from .types.campaign_customizer import CampaignCustomizer
from .types.campaign_draft import CampaignDraft
from .types.campaign_goal_config import CampaignGoalConfig
from .types.campaign_group import CampaignGroup
from .types.campaign_label import CampaignLabel
from .types.campaign_lifecycle_goal import CampaignLifecycleGoal
from .types.campaign_lifecycle_goal import CustomerAcquisitionGoalSettings
from .types.campaign_search_term_insight import CampaignSearchTermInsight
from .types.campaign_search_term_view import CampaignSearchTermView
from .types.campaign_shared_set import CampaignSharedSet
from .types.campaign_simulation import CampaignSimulation
from .types.carrier_constant import CarrierConstant
from .types.change_event import ChangeEvent
from .types.change_status import ChangeStatus
from .types.channel_aggregate_asset_view import ChannelAggregateAssetView
from .types.click_view import ClickView
from .types.combined_audience import CombinedAudience
from .types.content_criterion_view import ContentCriterionView
from .types.conversion_action import ConversionAction
from .types.conversion_custom_variable import ConversionCustomVariable
from .types.conversion_goal_campaign_config import ConversionGoalCampaignConfig
from .types.conversion_value_rule import ConversionValueRule
from .types.conversion_value_rule_set import ConversionValueRuleSet
from .types.currency_constant import CurrencyConstant
from .types.custom_audience import CustomAudience
from .types.custom_audience import CustomAudienceMember
from .types.custom_conversion_goal import CustomConversionGoal
from .types.custom_interest import CustomInterest
from .types.custom_interest import CustomInterestMember
from .types.customer import CallReportingSetting
from .types.customer import ConversionTrackingSetting
from .types.customer import Customer
from .types.customer import CustomerAgreementSetting
from .types.customer import GranularInsuranceStatus
from .types.customer import GranularLicenseStatus
from .types.customer import LocalServicesSettings
from .types.customer import RemarketingSetting
from .types.customer import VideoCustomer
from .types.customer_asset import CustomerAsset
from .types.customer_asset_set import CustomerAssetSet
from .types.customer_client import CustomerClient
from .types.customer_client_link import CustomerClientLink
from .types.customer_conversion_goal import CustomerConversionGoal
from .types.customer_customizer import CustomerCustomizer
from .types.customer_label import CustomerLabel
from .types.customer_lifecycle_goal import CustomerLifecycleGoal
from .types.customer_manager_link import CustomerManagerLink
from .types.customer_negative_criterion import CustomerNegativeCriterion
from .types.customer_search_term_insight import CustomerSearchTermInsight
from .types.customer_sk_ad_network_conversion_value_schema import (
    CustomerSkAdNetworkConversionValueSchema,
)
from .types.customer_user_access import CustomerUserAccess
from .types.customer_user_access_invitation import CustomerUserAccessInvitation
from .types.customizer_attribute import CustomizerAttribute
from .types.data_link import DataLink
from .types.data_link import YoutubeVideoIdentifier
from .types.detail_content_suitability_placement_view import (
    DetailContentSuitabilityPlacementView,
)
from .types.detail_placement_view import DetailPlacementView
from .types.detailed_demographic import DetailedDemographic
from .types.display_keyword_view import DisplayKeywordView
from .types.distance_view import DistanceView
from .types.domain_category import DomainCategory
from .types.dynamic_search_ads_search_term_view import (
    DynamicSearchAdsSearchTermView,
)
from .types.expanded_landing_page_view import ExpandedLandingPageView
from .types.experiment import Experiment
from .types.experiment_arm import ExperimentArm
from .types.final_url_expansion_asset_view import FinalUrlExpansionAssetView
from .types.gender_view import GenderView
from .types.geo_target_constant import GeoTargetConstant
from .types.geographic_view import GeographicView
from .types.goal import Goal
from .types.google_ads_field import GoogleAdsField
from .types.group_content_suitability_placement_view import (
    GroupContentSuitabilityPlacementView,
)
from .types.group_placement_view import GroupPlacementView
from .types.hotel_group_view import HotelGroupView
from .types.hotel_performance_view import HotelPerformanceView
from .types.hotel_reconciliation import HotelReconciliation
from .types.income_range_view import IncomeRangeView
from .types.invoice import Invoice
from .types.keyword_plan import KeywordPlan
from .types.keyword_plan import KeywordPlanForecastPeriod
from .types.keyword_plan_ad_group import KeywordPlanAdGroup
from .types.keyword_plan_ad_group_keyword import KeywordPlanAdGroupKeyword
from .types.keyword_plan_campaign import KeywordPlanCampaign
from .types.keyword_plan_campaign import KeywordPlanGeoTarget
from .types.keyword_plan_campaign_keyword import KeywordPlanCampaignKeyword
from .types.keyword_theme_constant import KeywordThemeConstant
from .types.keyword_view import KeywordView
from .types.label import Label
from .types.landing_page_view import LandingPageView
from .types.language_constant import LanguageConstant
from .types.lead_form_submission_data import CustomLeadFormSubmissionField
from .types.lead_form_submission_data import LeadFormSubmissionData
from .types.lead_form_submission_data import LeadFormSubmissionField
from .types.life_event import LifeEvent
from .types.local_services_employee import Fellowship
from .types.local_services_employee import LocalServicesEmployee
from .types.local_services_employee import Residency
from .types.local_services_employee import UniversityDegree
from .types.local_services_lead import ContactDetails
from .types.local_services_lead import CreditDetails
from .types.local_services_lead import LocalServicesLead
from .types.local_services_lead import Note
from .types.local_services_lead_conversation import (
    LocalServicesLeadConversation,
)
from .types.local_services_lead_conversation import MessageDetails
from .types.local_services_lead_conversation import PhoneCallDetails
from .types.local_services_verification_artifact import (
    BackgroundCheckVerificationArtifact,
)
from .types.local_services_verification_artifact import (
    BusinessRegistrationCheckVerificationArtifact,
)
from .types.local_services_verification_artifact import (
    BusinessRegistrationDocument,
)
from .types.local_services_verification_artifact import (
    BusinessRegistrationNumber,
)
from .types.local_services_verification_artifact import (
    InsuranceVerificationArtifact,
)
from .types.local_services_verification_artifact import (
    LicenseVerificationArtifact,
)
from .types.local_services_verification_artifact import (
    LocalServicesVerificationArtifact,
)
from .types.location_interest_view import LocationInterestView
from .types.location_view import LocationView
from .types.managed_placement_view import ManagedPlacementView
from .types.media_file import MediaAudio
from .types.media_file import MediaBundle
from .types.media_file import MediaFile
from .types.media_file import MediaImage
from .types.media_file import MediaVideo
from .types.mobile_app_category_constant import MobileAppCategoryConstant
from .types.mobile_device_constant import MobileDeviceConstant
from .types.offline_conversion_upload_client_summary import (
    OfflineConversionAlert,
)
from .types.offline_conversion_upload_client_summary import (
    OfflineConversionError,
)
from .types.offline_conversion_upload_client_summary import (
    OfflineConversionSummary,
)
from .types.offline_conversion_upload_client_summary import (
    OfflineConversionUploadClientSummary,
)
from .types.offline_conversion_upload_conversion_action_summary import (
    OfflineConversionUploadConversionActionSummary,
)
from .types.offline_user_data_job import OfflineUserDataJob
from .types.offline_user_data_job import OfflineUserDataJobMetadata
from .types.operating_system_version_constant import (
    OperatingSystemVersionConstant,
)
from .types.paid_organic_search_term_view import PaidOrganicSearchTermView
from .types.parental_status_view import ParentalStatusView
from .types.payments_account import PaymentsAccount
from .types.per_store_view import PerStoreView
from .types.performance_max_placement_view import PerformanceMaxPlacementView
from .types.product_category_constant import ProductCategoryConstant
from .types.product_group_view import ProductGroupView
from .types.product_link import AdvertisingPartnerIdentifier
from .types.product_link import DataPartnerIdentifier
from .types.product_link import GoogleAdsIdentifier
from .types.product_link import MerchantCenterIdentifier
from .types.product_link import ProductLink
from .types.product_link_invitation import (
    AdvertisingPartnerLinkInvitationIdentifier,
)
from .types.product_link_invitation import HotelCenterLinkInvitationIdentifier
from .types.product_link_invitation import (
    MerchantCenterLinkInvitationIdentifier,
)
from .types.product_link_invitation import ProductLinkInvitation
from .types.qualifying_question import QualifyingQuestion
from .types.recommendation import Recommendation
from .types.recommendation_subscription import RecommendationSubscription
from .types.remarketing_action import RemarketingAction
from .types.search_term_view import SearchTermView
from .types.shared_criterion import SharedCriterion
from .types.shared_set import SharedSet
from .types.shopping_performance_view import ShoppingPerformanceView
from .types.shopping_product import ShoppingProduct
from .types.smart_campaign_search_term_view import SmartCampaignSearchTermView
from .types.smart_campaign_setting import SmartCampaignSetting
from .types.targeting_expansion_view import TargetingExpansionView
from .types.third_party_app_analytics_link import ThirdPartyAppAnalyticsLink
from .types.topic_constant import TopicConstant
from .types.topic_view import TopicView
from .types.travel_activity_group_view import TravelActivityGroupView
from .types.travel_activity_performance_view import (
    TravelActivityPerformanceView,
)
from .types.user_interest import UserInterest
from .types.user_list import UserList
from .types.user_list_customer_type import UserListCustomerType
from .types.user_location_view import UserLocationView
from .types.video import Video
from .types.webpage_view import WebpageView

if hasattr(api_core, "check_python_version") and hasattr(
    api_core, "check_dependency_versions"
):  # pragma: NO COVER
    api_core.check_python_version("google.ads.googleads.v22")  # type: ignore
    api_core.check_dependency_versions("google.ads.googleads.v22")  # type: ignore
else:  # pragma: NO COVER
    # An older version of api_core is installed which does not define the
    # functions above. We do equivalent checks manually.
    try:
        import warnings
        import sys

        _py_version_str = sys.version.split()[0]
        _package_label = "google.ads.googleads.v22"
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
        _version_used, _version_used_string = _get_version(_dependency_package)
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
    "AccessibleBiddingStrategy",
    "AccountBudget",
    "AccountBudgetProposal",
    "AccountLink",
    "Ad",
    "AdGroup",
    "AdGroupAd",
    "AdGroupAdAssetAutomationSetting",
    "AdGroupAdAssetCombinationView",
    "AdGroupAdAssetPolicySummary",
    "AdGroupAdAssetView",
    "AdGroupAdLabel",
    "AdGroupAdPolicySummary",
    "AdGroupAsset",
    "AdGroupAssetSet",
    "AdGroupAudienceView",
    "AdGroupBidModifier",
    "AdGroupCriterion",
    "AdGroupCriterionCustomizer",
    "AdGroupCriterionLabel",
    "AdGroupCriterionSimulation",
    "AdGroupCustomizer",
    "AdGroupLabel",
    "AdGroupSimulation",
    "AdParameter",
    "AdScheduleView",
    "AdStrengthActionItem",
    "AdvertisingPartnerIdentifier",
    "AdvertisingPartnerLinkInvitationIdentifier",
    "AgeRangeView",
    "AiMaxSearchTermAdCombinationView",
    "AndroidPrivacySharedKeyGoogleAdGroup",
    "AndroidPrivacySharedKeyGoogleCampaign",
    "AndroidPrivacySharedKeyGoogleNetworkType",
    "Asset",
    "AssetCoverage",
    "AssetFieldTypePolicySummary",
    "AssetFieldTypeView",
    "AssetGroup",
    "AssetGroupAsset",
    "AssetGroupAssetCombinationData",
    "AssetGroupListingGroupFilter",
    "AssetGroupProductGroupView",
    "AssetGroupSignal",
    "AssetGroupTopCombinationView",
    "AssetPolicySummary",
    "AssetSet",
    "AssetSetAsset",
    "AssetSetTypeView",
    "Audience",
    "BackgroundCheckVerificationArtifact",
    "BatchJob",
    "BiddingDataExclusion",
    "BiddingSeasonalityAdjustment",
    "BiddingStrategy",
    "BiddingStrategySimulation",
    "BillingSetup",
    "BusinessRegistrationCheckVerificationArtifact",
    "BusinessRegistrationDocument",
    "BusinessRegistrationNumber",
    "CallReportingSetting",
    "CallView",
    "Campaign",
    "CampaignAggregateAssetView",
    "CampaignAsset",
    "CampaignAssetSet",
    "CampaignAudienceView",
    "CampaignBidModifier",
    "CampaignBudget",
    "CampaignConversionGoal",
    "CampaignCriterion",
    "CampaignCustomizer",
    "CampaignDraft",
    "CampaignGoalConfig",
    "CampaignGroup",
    "CampaignLabel",
    "CampaignLifecycleGoal",
    "CampaignSearchTermInsight",
    "CampaignSearchTermView",
    "CampaignSharedSet",
    "CampaignSimulation",
    "CarrierConstant",
    "ChangeEvent",
    "ChangeStatus",
    "ChannelAggregateAssetView",
    "ClickView",
    "CombinedAudience",
    "ContactDetails",
    "ContentCriterionView",
    "ConversionAction",
    "ConversionCustomVariable",
    "ConversionGoalCampaignConfig",
    "ConversionTrackingSetting",
    "ConversionValueRule",
    "ConversionValueRuleSet",
    "CreditDetails",
    "CurrencyConstant",
    "CustomAudience",
    "CustomAudienceMember",
    "CustomConversionGoal",
    "CustomInterest",
    "CustomInterestMember",
    "CustomLeadFormSubmissionField",
    "Customer",
    "CustomerAcquisitionGoalSettings",
    "CustomerAgreementSetting",
    "CustomerAsset",
    "CustomerAssetSet",
    "CustomerClient",
    "CustomerClientLink",
    "CustomerConversionGoal",
    "CustomerCustomizer",
    "CustomerLabel",
    "CustomerLifecycleGoal",
    "CustomerManagerLink",
    "CustomerNegativeCriterion",
    "CustomerSearchTermInsight",
    "CustomerSkAdNetworkConversionValueSchema",
    "CustomerUserAccess",
    "CustomerUserAccessInvitation",
    "CustomizerAttribute",
    "DataLink",
    "DataPartnerIdentifier",
    "DetailContentSuitabilityPlacementView",
    "DetailPlacementView",
    "DetailedDemographic",
    "DisplayKeywordView",
    "DistanceView",
    "DomainCategory",
    "DynamicSearchAdsSearchTermView",
    "ExpandedLandingPageView",
    "Experiment",
    "ExperimentArm",
    "Fellowship",
    "FinalUrlExpansionAssetView",
    "GenderView",
    "GeoTargetConstant",
    "GeographicView",
    "Goal",
    "GoogleAdsField",
    "GoogleAdsIdentifier",
    "GranularInsuranceStatus",
    "GranularLicenseStatus",
    "GroupContentSuitabilityPlacementView",
    "GroupPlacementView",
    "HotelCenterLinkInvitationIdentifier",
    "HotelGroupView",
    "HotelPerformanceView",
    "HotelReconciliation",
    "IncomeRangeView",
    "InsuranceVerificationArtifact",
    "Invoice",
    "KeywordPlan",
    "KeywordPlanAdGroup",
    "KeywordPlanAdGroupKeyword",
    "KeywordPlanCampaign",
    "KeywordPlanCampaignKeyword",
    "KeywordPlanForecastPeriod",
    "KeywordPlanGeoTarget",
    "KeywordThemeConstant",
    "KeywordView",
    "Label",
    "LandingPageView",
    "LanguageConstant",
    "LeadFormSubmissionData",
    "LeadFormSubmissionField",
    "LicenseVerificationArtifact",
    "LifeEvent",
    "ListingGroupFilterDimension",
    "ListingGroupFilterDimensionPath",
    "LocalServicesEmployee",
    "LocalServicesLead",
    "LocalServicesLeadConversation",
    "LocalServicesSettings",
    "LocalServicesVerificationArtifact",
    "LocationInterestView",
    "LocationView",
    "ManagedPlacementView",
    "MediaAudio",
    "MediaBundle",
    "MediaFile",
    "MediaImage",
    "MediaVideo",
    "MerchantCenterIdentifier",
    "MerchantCenterLinkInvitationIdentifier",
    "MessageDetails",
    "MobileAppCategoryConstant",
    "MobileDeviceConstant",
    "Note",
    "OfflineConversionAlert",
    "OfflineConversionError",
    "OfflineConversionSummary",
    "OfflineConversionUploadClientSummary",
    "OfflineConversionUploadConversionActionSummary",
    "OfflineUserDataJob",
    "OfflineUserDataJobMetadata",
    "OperatingSystemVersionConstant",
    "PaidOrganicSearchTermView",
    "ParentalStatusView",
    "PaymentsAccount",
    "PerStoreView",
    "PerformanceMaxPlacementView",
    "PhoneCallDetails",
    "ProductCategoryConstant",
    "ProductGroupView",
    "ProductLink",
    "ProductLinkInvitation",
    "QualifyingQuestion",
    "Recommendation",
    "RecommendationSubscription",
    "RemarketingAction",
    "RemarketingSetting",
    "Residency",
    "SearchTermView",
    "SharedCriterion",
    "SharedSet",
    "ShoppingPerformanceView",
    "ShoppingProduct",
    "SmartCampaignSearchTermView",
    "SmartCampaignSetting",
    "TargetingExpansionView",
    "ThirdPartyAppAnalyticsLink",
    "ThirdPartyAppAnalyticsLinkIdentifier",
    "TopicConstant",
    "TopicView",
    "TravelActivityGroupView",
    "TravelActivityPerformanceView",
    "UniversityDegree",
    "UserInterest",
    "UserList",
    "UserListCustomerType",
    "UserLocationView",
    "Video",
    "VideoCustomer",
    "WebpageView",
    "YoutubeVideoIdentifier",
)

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


from .types.ad_asset import AdAppDeepLinkAsset
from .types.ad_asset import AdCallToActionAsset
from .types.ad_asset import AdDemandGenCarouselCardAsset
from .types.ad_asset import AdImageAsset
from .types.ad_asset import AdMediaBundleAsset
from .types.ad_asset import AdTextAsset
from .types.ad_asset import AdVideoAsset
from .types.ad_asset import AdVideoAssetInfo
from .types.ad_asset import AdVideoAssetInventoryPreferences
from .types.ad_asset import AdVideoAssetLinkFeatureControl
from .types.ad_type_infos import AppAdInfo
from .types.ad_type_infos import AppEngagementAdInfo
from .types.ad_type_infos import AppPreRegistrationAdInfo
from .types.ad_type_infos import DemandGenCarouselAdInfo
from .types.ad_type_infos import DemandGenMultiAssetAdInfo
from .types.ad_type_infos import DemandGenProductAdInfo
from .types.ad_type_infos import DemandGenVideoResponsiveAdInfo
from .types.ad_type_infos import DisplayUploadAdInfo
from .types.ad_type_infos import ExpandedDynamicSearchAdInfo
from .types.ad_type_infos import ExpandedTextAdInfo
from .types.ad_type_infos import HotelAdInfo
from .types.ad_type_infos import ImageAdInfo
from .types.ad_type_infos import InFeedVideoAdInfo
from .types.ad_type_infos import LegacyAppInstallAdInfo
from .types.ad_type_infos import LegacyResponsiveDisplayAdInfo
from .types.ad_type_infos import LocalAdInfo
from .types.ad_type_infos import ResponsiveDisplayAdControlSpec
from .types.ad_type_infos import ResponsiveDisplayAdInfo
from .types.ad_type_infos import ResponsiveSearchAdInfo
from .types.ad_type_infos import ShoppingComparisonListingAdInfo
from .types.ad_type_infos import ShoppingProductAdInfo
from .types.ad_type_infos import ShoppingSmartAdInfo
from .types.ad_type_infos import SmartCampaignAdInfo
from .types.ad_type_infos import TextAdInfo
from .types.ad_type_infos import TravelAdInfo
from .types.ad_type_infos import VideoAdInfo
from .types.ad_type_infos import VideoBumperInStreamAdInfo
from .types.ad_type_infos import VideoNonSkippableInStreamAdInfo
from .types.ad_type_infos import VideoOutstreamAdInfo
from .types.ad_type_infos import VideoResponsiveAdInfo
from .types.ad_type_infos import VideoTrueViewInStreamAdInfo
from .types.ad_type_infos import YouTubeAudioAdInfo
from .types.additional_application_info import AdditionalApplicationInfo
from .types.asset_policy import AdAssetPolicySummary
from .types.asset_policy import AssetDisapproved
from .types.asset_policy import AssetLinkPrimaryStatusDetails
from .types.asset_set_types import BusinessProfileBusinessNameFilter
from .types.asset_set_types import BusinessProfileLocationGroup
from .types.asset_set_types import BusinessProfileLocationSet
from .types.asset_set_types import ChainFilter
from .types.asset_set_types import ChainLocationGroup
from .types.asset_set_types import ChainSet
from .types.asset_set_types import DynamicBusinessProfileLocationGroupFilter
from .types.asset_set_types import LocationSet
from .types.asset_set_types import MapsLocationInfo
from .types.asset_set_types import MapsLocationSet
from .types.asset_types import AppDeepLinkAsset
from .types.asset_types import BookOnGoogleAsset
from .types.asset_types import BusinessMessageAsset
from .types.asset_types import BusinessMessageCallToActionInfo
from .types.asset_types import BusinessProfileLocation
from .types.asset_types import CallAsset
from .types.asset_types import CalloutAsset
from .types.asset_types import CallToActionAsset
from .types.asset_types import DemandGenCarouselCardAsset
from .types.asset_types import DynamicCustomAsset
from .types.asset_types import DynamicEducationAsset
from .types.asset_types import DynamicFlightsAsset
from .types.asset_types import DynamicHotelsAndRentalsAsset
from .types.asset_types import DynamicJobsAsset
from .types.asset_types import DynamicLocalAsset
from .types.asset_types import DynamicRealEstateAsset
from .types.asset_types import DynamicTravelAsset
from .types.asset_types import FacebookMessengerBusinessMessageInfo
from .types.asset_types import HotelCalloutAsset
from .types.asset_types import HotelPropertyAsset
from .types.asset_types import ImageAsset
from .types.asset_types import ImageDimension
from .types.asset_types import LeadFormAsset
from .types.asset_types import LeadFormCustomQuestionField
from .types.asset_types import LeadFormDeliveryMethod
from .types.asset_types import LeadFormField
from .types.asset_types import LeadFormSingleChoiceAnswers
from .types.asset_types import LocationAsset
from .types.asset_types import MediaBundleAsset
from .types.asset_types import MobileAppAsset
from .types.asset_types import PageFeedAsset
from .types.asset_types import PriceAsset
from .types.asset_types import PriceOffering
from .types.asset_types import PromotionAsset
from .types.asset_types import PromotionBarcodeInfo
from .types.asset_types import PromotionQrCodeInfo
from .types.asset_types import SitelinkAsset
from .types.asset_types import StructuredSnippetAsset
from .types.asset_types import TextAsset
from .types.asset_types import WebhookDelivery
from .types.asset_types import WhatsappBusinessMessageInfo
from .types.asset_types import YoutubeVideoAsset
from .types.asset_types import YouTubeVideoListAsset
from .types.asset_types import ZaloBusinessMessageInfo
from .types.asset_usage import AssetUsage
from .types.audience_insights_attribute import AudienceInsightsAttribute
from .types.audience_insights_attribute import AudienceInsightsAttributeMetadata
from .types.audience_insights_attribute import (
    AudienceInsightsAttributeMetadataGroup,
)
from .types.audience_insights_attribute import AudienceInsightsCategory
from .types.audience_insights_attribute import AudienceInsightsEntity
from .types.audience_insights_attribute import AudienceInsightsLineup
from .types.audience_insights_attribute import KnowledgeGraphAttributeMetadata
from .types.audience_insights_attribute import LineupAttributeMetadata
from .types.audience_insights_attribute import LocationAttributeMetadata
from .types.audience_insights_attribute import UserInterestAttributeMetadata
from .types.audience_insights_attribute import UserListAttributeMetadata
from .types.audience_insights_attribute import YouTubeChannelAttributeMetadata
from .types.audience_insights_attribute import YouTubeVideoAttributeMetadata
from .types.audiences import AgeDimension
from .types.audiences import AgeSegment
from .types.audiences import AudienceDimension
from .types.audiences import AudienceExclusionDimension
from .types.audiences import AudienceSegment
from .types.audiences import AudienceSegmentDimension
from .types.audiences import CustomAudienceSegment
from .types.audiences import DetailedDemographicSegment
from .types.audiences import ExclusionSegment
from .types.audiences import GenderDimension
from .types.audiences import HouseholdIncomeDimension
from .types.audiences import LifeEventSegment
from .types.audiences import ParentalStatusDimension
from .types.audiences import UserInterestSegment
from .types.audiences import UserListSegment
from .types.bidding import Commission
from .types.bidding import EnhancedCpc
from .types.bidding import FixedCpm
from .types.bidding import FixedCpmTargetFrequencyGoalInfo
from .types.bidding import ManualCpa
from .types.bidding import ManualCpc
from .types.bidding import ManualCpm
from .types.bidding import ManualCpv
from .types.bidding import MaximizeConversions
from .types.bidding import MaximizeConversionValue
from .types.bidding import PercentCpc
from .types.bidding import TargetCpa
from .types.bidding import TargetCpc
from .types.bidding import TargetCpm
from .types.bidding import TargetCpmTargetFrequencyGoal
from .types.bidding import TargetCpv
from .types.bidding import TargetImpressionShare
from .types.bidding import TargetRoas
from .types.bidding import TargetSpend
from .types.campaign_goal_settings import CampaignGoalSettings
from .types.click_location import ClickLocation
from .types.consent import Consent
from .types.criteria import ActivityCityInfo
from .types.criteria import ActivityCountryInfo
from .types.criteria import ActivityIdInfo
from .types.criteria import ActivityRatingInfo
from .types.criteria import ActivityStateInfo
from .types.criteria import AddressInfo
from .types.criteria import AdScheduleInfo
from .types.criteria import AgeRangeInfo
from .types.criteria import AppPaymentModelInfo
from .types.criteria import AudienceInfo
from .types.criteria import BrandInfo
from .types.criteria import BrandListInfo
from .types.criteria import CarrierInfo
from .types.criteria import CombinedAudienceInfo
from .types.criteria import ContentLabelInfo
from .types.criteria import CustomAffinityInfo
from .types.criteria import CustomAudienceInfo
from .types.criteria import CustomIntentInfo
from .types.criteria import DeviceInfo
from .types.criteria import ExtendedDemographicInfo
from .types.criteria import GenderInfo
from .types.criteria import GeoPointInfo
from .types.criteria import HotelAdvanceBookingWindowInfo
from .types.criteria import HotelCheckInDateRangeInfo
from .types.criteria import HotelCheckInDayInfo
from .types.criteria import HotelCityInfo
from .types.criteria import HotelClassInfo
from .types.criteria import HotelCountryRegionInfo
from .types.criteria import HotelDateSelectionTypeInfo
from .types.criteria import HotelIdInfo
from .types.criteria import HotelLengthOfStayInfo
from .types.criteria import HotelStateInfo
from .types.criteria import IncomeRangeInfo
from .types.criteria import InteractionTypeInfo
from .types.criteria import IpBlockInfo
from .types.criteria import KeywordInfo
from .types.criteria import KeywordThemeInfo
from .types.criteria import LanguageInfo
from .types.criteria import LifeEventInfo
from .types.criteria import ListingDimensionInfo
from .types.criteria import ListingDimensionPath
from .types.criteria import ListingGroupInfo
from .types.criteria import ListingScopeInfo
from .types.criteria import LocalServiceIdInfo
from .types.criteria import LocationGroupInfo
from .types.criteria import LocationInfo
from .types.criteria import MobileAppCategoryInfo
from .types.criteria import MobileApplicationInfo
from .types.criteria import MobileDeviceInfo
from .types.criteria import NegativeKeywordListInfo
from .types.criteria import OperatingSystemVersionInfo
from .types.criteria import ParentalStatusInfo
from .types.criteria import PlacementInfo
from .types.criteria import PlacementListInfo
from .types.criteria import ProductBrandInfo
from .types.criteria import ProductCategoryInfo
from .types.criteria import ProductChannelExclusivityInfo
from .types.criteria import ProductChannelInfo
from .types.criteria import ProductConditionInfo
from .types.criteria import ProductCustomAttributeInfo
from .types.criteria import ProductGroupingInfo
from .types.criteria import ProductItemIdInfo
from .types.criteria import ProductLabelsInfo
from .types.criteria import ProductLegacyConditionInfo
from .types.criteria import ProductTypeFullInfo
from .types.criteria import ProductTypeInfo
from .types.criteria import ProximityInfo
from .types.criteria import SearchThemeInfo
from .types.criteria import TopicInfo
from .types.criteria import UnknownListingDimensionInfo
from .types.criteria import UserInterestInfo
from .types.criteria import UserListInfo
from .types.criteria import VerticalAdsItemGroupRuleInfo
from .types.criteria import VerticalAdsItemGroupRuleListInfo
from .types.criteria import VideoLineupInfo
from .types.criteria import WebpageConditionInfo
from .types.criteria import WebpageInfo
from .types.criteria import WebpageListInfo
from .types.criteria import WebpageSampleInfo
from .types.criteria import YouTubeChannelInfo
from .types.criteria import YouTubeVideoInfo
from .types.criterion_category_availability import CriterionCategoryAvailability
from .types.criterion_category_availability import (
    CriterionCategoryChannelAvailability,
)
from .types.criterion_category_availability import (
    CriterionCategoryLocaleAvailability,
)
from .types.custom_parameter import CustomParameter
from .types.customizer_value import CustomizerValue
from .types.dates import DateRange
from .types.dates import YearMonth
from .types.dates import YearMonthRange
from .types.extensions import CallFeedItem
from .types.extensions import CalloutFeedItem
from .types.extensions import SitelinkFeedItem
from .types.feed_common import Money
from .types.final_app_url import FinalAppUrl
from .types.frequency_cap import FrequencyCapEntry
from .types.frequency_cap import FrequencyCapKey
from .types.goal_common import CustomerLifecycleOptimizationValueSettings
from .types.goal_setting import GoalSetting
from .types.keyword_plan_common import ConceptGroup
from .types.keyword_plan_common import HistoricalMetricsOptions
from .types.keyword_plan_common import KeywordAnnotations
from .types.keyword_plan_common import KeywordConcept
from .types.keyword_plan_common import KeywordPlanAggregateMetricResults
from .types.keyword_plan_common import KeywordPlanAggregateMetrics
from .types.keyword_plan_common import KeywordPlanDeviceSearches
from .types.keyword_plan_common import KeywordPlanHistoricalMetrics
from .types.keyword_plan_common import MonthlySearchVolume
from .types.lifecycle_goals import LifecycleGoalValueSettings
from .types.local_services import LocalServicesDocumentReadOnly
from .types.metric_goal import MetricGoal
from .types.metrics import Metrics
from .types.metrics import SearchVolumeRange
from .types.offline_user_data import CustomerMatchUserListMetadata
from .types.offline_user_data import EventAttribute
from .types.offline_user_data import EventItemAttribute
from .types.offline_user_data import ItemAttribute
from .types.offline_user_data import OfflineUserAddressInfo
from .types.offline_user_data import ShoppingLoyalty
from .types.offline_user_data import StoreAttribute
from .types.offline_user_data import StoreSalesMetadata
from .types.offline_user_data import StoreSalesThirdPartyMetadata
from .types.offline_user_data import TransactionAttribute
from .types.offline_user_data import UserAttribute
from .types.offline_user_data import UserData
from .types.offline_user_data import UserIdentifier
from .types.policy import PolicyTopicConstraint
from .types.policy import PolicyTopicEntry
from .types.policy import PolicyTopicEvidence
from .types.policy import PolicyValidationParameter
from .types.policy import PolicyViolationKey
from .types.policy_summary import PolicySummary
from .types.real_time_bidding_setting import RealTimeBiddingSetting
from .types.segments import AssetInteractionTarget
from .types.segments import BudgetCampaignAssociationStatus
from .types.segments import Keyword
from .types.segments import Segments
from .types.segments import SkAdNetworkSourceApp
from .types.simulation import BudgetSimulationPoint
from .types.simulation import BudgetSimulationPointList
from .types.simulation import CpcBidSimulationPoint
from .types.simulation import CpcBidSimulationPointList
from .types.simulation import CpvBidSimulationPoint
from .types.simulation import CpvBidSimulationPointList
from .types.simulation import PercentCpcBidSimulationPoint
from .types.simulation import PercentCpcBidSimulationPointList
from .types.simulation import TargetCpaSimulationPoint
from .types.simulation import TargetCpaSimulationPointList
from .types.simulation import TargetImpressionShareSimulationPoint
from .types.simulation import TargetImpressionShareSimulationPointList
from .types.simulation import TargetRoasSimulationPoint
from .types.simulation import TargetRoasSimulationPointList
from .types.tag_snippet import TagSnippet
from .types.targeting_setting import TargetingSetting
from .types.targeting_setting import TargetRestriction
from .types.targeting_setting import TargetRestrictionOperation
from .types.text_label import TextLabel
from .types.third_party_integration_partners import (
    CampaignThirdPartyBrandLiftIntegrationPartner,
)
from .types.third_party_integration_partners import (
    CampaignThirdPartyBrandSafetyIntegrationPartner,
)
from .types.third_party_integration_partners import (
    CampaignThirdPartyIntegrationPartners,
)
from .types.third_party_integration_partners import (
    CampaignThirdPartyReachIntegrationPartner,
)
from .types.third_party_integration_partners import (
    CampaignThirdPartyViewabilityIntegrationPartner,
)
from .types.third_party_integration_partners import (
    CustomerThirdPartyBrandLiftIntegrationPartner,
)
from .types.third_party_integration_partners import (
    CustomerThirdPartyBrandSafetyIntegrationPartner,
)
from .types.third_party_integration_partners import (
    CustomerThirdPartyIntegrationPartners,
)
from .types.third_party_integration_partners import (
    CustomerThirdPartyReachIntegrationPartner,
)
from .types.third_party_integration_partners import (
    CustomerThirdPartyViewabilityIntegrationPartner,
)
from .types.third_party_integration_partners import (
    ThirdPartyIntegrationPartnerData,
)
from .types.url_collection import UrlCollection
from .types.user_lists import BasicUserListInfo
from .types.user_lists import CrmBasedUserListInfo
from .types.user_lists import FlexibleRuleOperandInfo
from .types.user_lists import FlexibleRuleUserListInfo
from .types.user_lists import LogicalUserListInfo
from .types.user_lists import LogicalUserListOperandInfo
from .types.user_lists import LookalikeUserListInfo
from .types.user_lists import RuleBasedUserListInfo
from .types.user_lists import SimilarUserListInfo
from .types.user_lists import UserListActionInfo
from .types.user_lists import UserListDateRuleItemInfo
from .types.user_lists import UserListLogicalRuleInfo
from .types.user_lists import UserListNumberRuleItemInfo
from .types.user_lists import UserListRuleInfo
from .types.user_lists import UserListRuleItemGroupInfo
from .types.user_lists import UserListRuleItemInfo
from .types.user_lists import UserListStringRuleItemInfo
from .types.value import Value

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
    "ActivityCityInfo",
    "ActivityCountryInfo",
    "ActivityIdInfo",
    "ActivityRatingInfo",
    "ActivityStateInfo",
    "AdAppDeepLinkAsset",
    "AdAssetPolicySummary",
    "AdCallToActionAsset",
    "AdDemandGenCarouselCardAsset",
    "AdImageAsset",
    "AdMediaBundleAsset",
    "AdScheduleInfo",
    "AdTextAsset",
    "AdVideoAsset",
    "AdVideoAssetInfo",
    "AdVideoAssetInventoryPreferences",
    "AdVideoAssetLinkFeatureControl",
    "AdditionalApplicationInfo",
    "AddressInfo",
    "AgeDimension",
    "AgeRangeInfo",
    "AgeSegment",
    "AppAdInfo",
    "AppDeepLinkAsset",
    "AppEngagementAdInfo",
    "AppPaymentModelInfo",
    "AppPreRegistrationAdInfo",
    "AssetDisapproved",
    "AssetInteractionTarget",
    "AssetLinkPrimaryStatusDetails",
    "AssetUsage",
    "AudienceDimension",
    "AudienceExclusionDimension",
    "AudienceInfo",
    "AudienceInsightsAttribute",
    "AudienceInsightsAttributeMetadata",
    "AudienceInsightsAttributeMetadataGroup",
    "AudienceInsightsCategory",
    "AudienceInsightsEntity",
    "AudienceInsightsLineup",
    "AudienceSegment",
    "AudienceSegmentDimension",
    "BasicUserListInfo",
    "BookOnGoogleAsset",
    "BrandInfo",
    "BrandListInfo",
    "BudgetCampaignAssociationStatus",
    "BudgetSimulationPoint",
    "BudgetSimulationPointList",
    "BusinessMessageAsset",
    "BusinessMessageCallToActionInfo",
    "BusinessProfileBusinessNameFilter",
    "BusinessProfileLocation",
    "BusinessProfileLocationGroup",
    "BusinessProfileLocationSet",
    "CallAsset",
    "CallFeedItem",
    "CallToActionAsset",
    "CalloutAsset",
    "CalloutFeedItem",
    "CampaignGoalSettings",
    "CampaignThirdPartyBrandLiftIntegrationPartner",
    "CampaignThirdPartyBrandSafetyIntegrationPartner",
    "CampaignThirdPartyIntegrationPartners",
    "CampaignThirdPartyReachIntegrationPartner",
    "CampaignThirdPartyViewabilityIntegrationPartner",
    "CarrierInfo",
    "ChainFilter",
    "ChainLocationGroup",
    "ChainSet",
    "ClickLocation",
    "CombinedAudienceInfo",
    "Commission",
    "ConceptGroup",
    "Consent",
    "ContentLabelInfo",
    "CpcBidSimulationPoint",
    "CpcBidSimulationPointList",
    "CpvBidSimulationPoint",
    "CpvBidSimulationPointList",
    "CriterionCategoryAvailability",
    "CriterionCategoryChannelAvailability",
    "CriterionCategoryLocaleAvailability",
    "CrmBasedUserListInfo",
    "CustomAffinityInfo",
    "CustomAudienceInfo",
    "CustomAudienceSegment",
    "CustomIntentInfo",
    "CustomParameter",
    "CustomerLifecycleOptimizationValueSettings",
    "CustomerMatchUserListMetadata",
    "CustomerThirdPartyBrandLiftIntegrationPartner",
    "CustomerThirdPartyBrandSafetyIntegrationPartner",
    "CustomerThirdPartyIntegrationPartners",
    "CustomerThirdPartyReachIntegrationPartner",
    "CustomerThirdPartyViewabilityIntegrationPartner",
    "CustomizerValue",
    "DateRange",
    "DemandGenCarouselAdInfo",
    "DemandGenCarouselCardAsset",
    "DemandGenMultiAssetAdInfo",
    "DemandGenProductAdInfo",
    "DemandGenVideoResponsiveAdInfo",
    "DetailedDemographicSegment",
    "DeviceInfo",
    "DisplayUploadAdInfo",
    "DynamicBusinessProfileLocationGroupFilter",
    "DynamicCustomAsset",
    "DynamicEducationAsset",
    "DynamicFlightsAsset",
    "DynamicHotelsAndRentalsAsset",
    "DynamicJobsAsset",
    "DynamicLocalAsset",
    "DynamicRealEstateAsset",
    "DynamicTravelAsset",
    "EnhancedCpc",
    "EventAttribute",
    "EventItemAttribute",
    "ExclusionSegment",
    "ExpandedDynamicSearchAdInfo",
    "ExpandedTextAdInfo",
    "ExtendedDemographicInfo",
    "FacebookMessengerBusinessMessageInfo",
    "FinalAppUrl",
    "FixedCpm",
    "FixedCpmTargetFrequencyGoalInfo",
    "FlexibleRuleOperandInfo",
    "FlexibleRuleUserListInfo",
    "FrequencyCapEntry",
    "FrequencyCapKey",
    "GenderDimension",
    "GenderInfo",
    "GeoPointInfo",
    "GoalSetting",
    "HistoricalMetricsOptions",
    "HotelAdInfo",
    "HotelAdvanceBookingWindowInfo",
    "HotelCalloutAsset",
    "HotelCheckInDateRangeInfo",
    "HotelCheckInDayInfo",
    "HotelCityInfo",
    "HotelClassInfo",
    "HotelCountryRegionInfo",
    "HotelDateSelectionTypeInfo",
    "HotelIdInfo",
    "HotelLengthOfStayInfo",
    "HotelPropertyAsset",
    "HotelStateInfo",
    "HouseholdIncomeDimension",
    "ImageAdInfo",
    "ImageAsset",
    "ImageDimension",
    "InFeedVideoAdInfo",
    "IncomeRangeInfo",
    "InteractionTypeInfo",
    "IpBlockInfo",
    "ItemAttribute",
    "Keyword",
    "KeywordAnnotations",
    "KeywordConcept",
    "KeywordInfo",
    "KeywordPlanAggregateMetricResults",
    "KeywordPlanAggregateMetrics",
    "KeywordPlanDeviceSearches",
    "KeywordPlanHistoricalMetrics",
    "KeywordThemeInfo",
    "KnowledgeGraphAttributeMetadata",
    "LanguageInfo",
    "LeadFormAsset",
    "LeadFormCustomQuestionField",
    "LeadFormDeliveryMethod",
    "LeadFormField",
    "LeadFormSingleChoiceAnswers",
    "LegacyAppInstallAdInfo",
    "LegacyResponsiveDisplayAdInfo",
    "LifeEventInfo",
    "LifeEventSegment",
    "LifecycleGoalValueSettings",
    "LineupAttributeMetadata",
    "ListingDimensionInfo",
    "ListingDimensionPath",
    "ListingGroupInfo",
    "ListingScopeInfo",
    "LocalAdInfo",
    "LocalServiceIdInfo",
    "LocalServicesDocumentReadOnly",
    "LocationAsset",
    "LocationAttributeMetadata",
    "LocationGroupInfo",
    "LocationInfo",
    "LocationSet",
    "LogicalUserListInfo",
    "LogicalUserListOperandInfo",
    "LookalikeUserListInfo",
    "ManualCpa",
    "ManualCpc",
    "ManualCpm",
    "ManualCpv",
    "MapsLocationInfo",
    "MapsLocationSet",
    "MaximizeConversionValue",
    "MaximizeConversions",
    "MediaBundleAsset",
    "MetricGoal",
    "Metrics",
    "MobileAppAsset",
    "MobileAppCategoryInfo",
    "MobileApplicationInfo",
    "MobileDeviceInfo",
    "Money",
    "MonthlySearchVolume",
    "NegativeKeywordListInfo",
    "OfflineUserAddressInfo",
    "OperatingSystemVersionInfo",
    "PageFeedAsset",
    "ParentalStatusDimension",
    "ParentalStatusInfo",
    "PercentCpc",
    "PercentCpcBidSimulationPoint",
    "PercentCpcBidSimulationPointList",
    "PlacementInfo",
    "PlacementListInfo",
    "PolicySummary",
    "PolicyTopicConstraint",
    "PolicyTopicEntry",
    "PolicyTopicEvidence",
    "PolicyValidationParameter",
    "PolicyViolationKey",
    "PriceAsset",
    "PriceOffering",
    "ProductBrandInfo",
    "ProductCategoryInfo",
    "ProductChannelExclusivityInfo",
    "ProductChannelInfo",
    "ProductConditionInfo",
    "ProductCustomAttributeInfo",
    "ProductGroupingInfo",
    "ProductItemIdInfo",
    "ProductLabelsInfo",
    "ProductLegacyConditionInfo",
    "ProductTypeFullInfo",
    "ProductTypeInfo",
    "PromotionAsset",
    "PromotionBarcodeInfo",
    "PromotionQrCodeInfo",
    "ProximityInfo",
    "RealTimeBiddingSetting",
    "ResponsiveDisplayAdControlSpec",
    "ResponsiveDisplayAdInfo",
    "ResponsiveSearchAdInfo",
    "RuleBasedUserListInfo",
    "SearchThemeInfo",
    "SearchVolumeRange",
    "Segments",
    "ShoppingComparisonListingAdInfo",
    "ShoppingLoyalty",
    "ShoppingProductAdInfo",
    "ShoppingSmartAdInfo",
    "SimilarUserListInfo",
    "SitelinkAsset",
    "SitelinkFeedItem",
    "SkAdNetworkSourceApp",
    "SmartCampaignAdInfo",
    "StoreAttribute",
    "StoreSalesMetadata",
    "StoreSalesThirdPartyMetadata",
    "StructuredSnippetAsset",
    "TagSnippet",
    "TargetCpa",
    "TargetCpaSimulationPoint",
    "TargetCpaSimulationPointList",
    "TargetCpc",
    "TargetCpm",
    "TargetCpmTargetFrequencyGoal",
    "TargetCpv",
    "TargetImpressionShare",
    "TargetImpressionShareSimulationPoint",
    "TargetImpressionShareSimulationPointList",
    "TargetRestriction",
    "TargetRestrictionOperation",
    "TargetRoas",
    "TargetRoasSimulationPoint",
    "TargetRoasSimulationPointList",
    "TargetSpend",
    "TargetingSetting",
    "TextAdInfo",
    "TextAsset",
    "TextLabel",
    "ThirdPartyIntegrationPartnerData",
    "TopicInfo",
    "TransactionAttribute",
    "TravelAdInfo",
    "UnknownListingDimensionInfo",
    "UrlCollection",
    "UserAttribute",
    "UserData",
    "UserIdentifier",
    "UserInterestAttributeMetadata",
    "UserInterestInfo",
    "UserInterestSegment",
    "UserListActionInfo",
    "UserListAttributeMetadata",
    "UserListDateRuleItemInfo",
    "UserListInfo",
    "UserListLogicalRuleInfo",
    "UserListNumberRuleItemInfo",
    "UserListRuleInfo",
    "UserListRuleItemGroupInfo",
    "UserListRuleItemInfo",
    "UserListSegment",
    "UserListStringRuleItemInfo",
    "Value",
    "VerticalAdsItemGroupRuleInfo",
    "VerticalAdsItemGroupRuleListInfo",
    "VideoAdInfo",
    "VideoBumperInStreamAdInfo",
    "VideoLineupInfo",
    "VideoNonSkippableInStreamAdInfo",
    "VideoOutstreamAdInfo",
    "VideoResponsiveAdInfo",
    "VideoTrueViewInStreamAdInfo",
    "WebhookDelivery",
    "WebpageConditionInfo",
    "WebpageInfo",
    "WebpageListInfo",
    "WebpageSampleInfo",
    "WhatsappBusinessMessageInfo",
    "YearMonth",
    "YearMonthRange",
    "YouTubeAudioAdInfo",
    "YouTubeChannelAttributeMetadata",
    "YouTubeChannelInfo",
    "YouTubeVideoAttributeMetadata",
    "YouTubeVideoInfo",
    "YouTubeVideoListAsset",
    "YoutubeVideoAsset",
    "ZaloBusinessMessageInfo",
)

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
from .accessible_bidding_strategy import (
    AccessibleBiddingStrategy,
)
from .account_budget import (
    AccountBudget,
)
from .account_budget_proposal import (
    AccountBudgetProposal,
)
from .account_link import (
    AccountLink,
    ThirdPartyAppAnalyticsLinkIdentifier,
)
from .ad import (
    Ad,
)
from .ad_group import (
    AdGroup,
)
from .ad_group_ad import (
    AdGroupAd,
    AdGroupAdAssetAutomationSetting,
    AdGroupAdPolicySummary,
)
from .ad_group_ad_asset_combination_view import (
    AdGroupAdAssetCombinationView,
)
from .ad_group_ad_asset_view import (
    AdGroupAdAssetPolicySummary,
    AdGroupAdAssetView,
)
from .ad_group_ad_label import (
    AdGroupAdLabel,
)
from .ad_group_asset import (
    AdGroupAsset,
)
from .ad_group_asset_set import (
    AdGroupAssetSet,
)
from .ad_group_audience_view import (
    AdGroupAudienceView,
)
from .ad_group_bid_modifier import (
    AdGroupBidModifier,
)
from .ad_group_criterion import (
    AdGroupCriterion,
)
from .ad_group_criterion_customizer import (
    AdGroupCriterionCustomizer,
)
from .ad_group_criterion_label import (
    AdGroupCriterionLabel,
)
from .ad_group_criterion_simulation import (
    AdGroupCriterionSimulation,
)
from .ad_group_customizer import (
    AdGroupCustomizer,
)
from .ad_group_label import (
    AdGroupLabel,
)
from .ad_group_simulation import (
    AdGroupSimulation,
)
from .ad_parameter import (
    AdParameter,
)
from .ad_schedule_view import (
    AdScheduleView,
)
from .age_range_view import (
    AgeRangeView,
)
from .android_privacy_shared_key_google_ad_group import (
    AndroidPrivacySharedKeyGoogleAdGroup,
)
from .android_privacy_shared_key_google_campaign import (
    AndroidPrivacySharedKeyGoogleCampaign,
)
from .android_privacy_shared_key_google_network_type import (
    AndroidPrivacySharedKeyGoogleNetworkType,
)
from .asset import (
    Asset,
    AssetFieldTypePolicySummary,
    AssetPolicySummary,
)
from .asset_field_type_view import (
    AssetFieldTypeView,
)
from .asset_group import (
    AdStrengthActionItem,
    AssetCoverage,
    AssetGroup,
)
from .asset_group_asset import (
    AssetGroupAsset,
)
from .asset_group_listing_group_filter import (
    AssetGroupListingGroupFilter,
    ListingGroupFilterDimension,
    ListingGroupFilterDimensionPath,
)
from .asset_group_product_group_view import (
    AssetGroupProductGroupView,
)
from .asset_group_signal import (
    AssetGroupSignal,
)
from .asset_group_top_combination_view import (
    AssetGroupAssetCombinationData,
    AssetGroupTopCombinationView,
)
from .asset_set import (
    AssetSet,
)
from .asset_set_asset import (
    AssetSetAsset,
)
from .asset_set_type_view import (
    AssetSetTypeView,
)
from .audience import (
    Audience,
)
from .batch_job import (
    BatchJob,
)
from .bidding_data_exclusion import (
    BiddingDataExclusion,
)
from .bidding_seasonality_adjustment import (
    BiddingSeasonalityAdjustment,
)
from .bidding_strategy import (
    BiddingStrategy,
)
from .bidding_strategy_simulation import (
    BiddingStrategySimulation,
)
from .billing_setup import (
    BillingSetup,
)
from .call_view import (
    CallView,
)
from .campaign import (
    Campaign,
)
from .campaign_aggregate_asset_view import (
    CampaignAggregateAssetView,
)
from .campaign_asset import (
    CampaignAsset,
)
from .campaign_asset_set import (
    CampaignAssetSet,
)
from .campaign_audience_view import (
    CampaignAudienceView,
)
from .campaign_bid_modifier import (
    CampaignBidModifier,
)
from .campaign_budget import (
    CampaignBudget,
)
from .campaign_conversion_goal import (
    CampaignConversionGoal,
)
from .campaign_criterion import (
    CampaignCriterion,
)
from .campaign_customizer import (
    CampaignCustomizer,
)
from .campaign_draft import (
    CampaignDraft,
)
from .campaign_group import (
    CampaignGroup,
)
from .campaign_label import (
    CampaignLabel,
)
from .campaign_lifecycle_goal import (
    CampaignLifecycleGoal,
    CustomerAcquisitionGoalSettings,
)
from .campaign_search_term_insight import (
    CampaignSearchTermInsight,
)
from .campaign_shared_set import (
    CampaignSharedSet,
)
from .campaign_simulation import (
    CampaignSimulation,
)
from .carrier_constant import (
    CarrierConstant,
)
from .change_event import (
    ChangeEvent,
)
from .change_status import (
    ChangeStatus,
)
from .channel_aggregate_asset_view import (
    ChannelAggregateAssetView,
)
from .click_view import (
    ClickView,
)
from .combined_audience import (
    CombinedAudience,
)
from .content_criterion_view import (
    ContentCriterionView,
)
from .conversion_action import (
    ConversionAction,
)
from .conversion_custom_variable import (
    ConversionCustomVariable,
)
from .conversion_goal_campaign_config import (
    ConversionGoalCampaignConfig,
)
from .conversion_value_rule import (
    ConversionValueRule,
)
from .conversion_value_rule_set import (
    ConversionValueRuleSet,
)
from .currency_constant import (
    CurrencyConstant,
)
from .custom_audience import (
    CustomAudience,
    CustomAudienceMember,
)
from .custom_conversion_goal import (
    CustomConversionGoal,
)
from .custom_interest import (
    CustomInterest,
    CustomInterestMember,
)
from .customer import (
    CallReportingSetting,
    ConversionTrackingSetting,
    Customer,
    CustomerAgreementSetting,
    GranularInsuranceStatus,
    GranularLicenseStatus,
    LocalServicesSettings,
    RemarketingSetting,
)
from .customer_asset import (
    CustomerAsset,
)
from .customer_asset_set import (
    CustomerAssetSet,
)
from .customer_client import (
    CustomerClient,
)
from .customer_client_link import (
    CustomerClientLink,
)
from .customer_conversion_goal import (
    CustomerConversionGoal,
)
from .customer_customizer import (
    CustomerCustomizer,
)
from .customer_label import (
    CustomerLabel,
)
from .customer_lifecycle_goal import (
    CustomerLifecycleGoal,
)
from .customer_manager_link import (
    CustomerManagerLink,
)
from .customer_negative_criterion import (
    CustomerNegativeCriterion,
)
from .customer_search_term_insight import (
    CustomerSearchTermInsight,
)
from .customer_sk_ad_network_conversion_value_schema import (
    CustomerSkAdNetworkConversionValueSchema,
)
from .customer_user_access import (
    CustomerUserAccess,
)
from .customer_user_access_invitation import (
    CustomerUserAccessInvitation,
)
from .customizer_attribute import (
    CustomizerAttribute,
)
from .data_link import (
    DataLink,
    YoutubeVideoIdentifier,
)
from .detail_placement_view import (
    DetailPlacementView,
)
from .detailed_demographic import (
    DetailedDemographic,
)
from .display_keyword_view import (
    DisplayKeywordView,
)
from .distance_view import (
    DistanceView,
)
from .domain_category import (
    DomainCategory,
)
from .dynamic_search_ads_search_term_view import (
    DynamicSearchAdsSearchTermView,
)
from .expanded_landing_page_view import (
    ExpandedLandingPageView,
)
from .experiment import (
    Experiment,
)
from .experiment_arm import (
    ExperimentArm,
)
from .gender_view import (
    GenderView,
)
from .geo_target_constant import (
    GeoTargetConstant,
)
from .geographic_view import (
    GeographicView,
)
from .google_ads_field import (
    GoogleAdsField,
)
from .group_placement_view import (
    GroupPlacementView,
)
from .hotel_group_view import (
    HotelGroupView,
)
from .hotel_performance_view import (
    HotelPerformanceView,
)
from .hotel_reconciliation import (
    HotelReconciliation,
)
from .income_range_view import (
    IncomeRangeView,
)
from .invoice import (
    Invoice,
)
from .keyword_plan import (
    KeywordPlan,
    KeywordPlanForecastPeriod,
)
from .keyword_plan_ad_group import (
    KeywordPlanAdGroup,
)
from .keyword_plan_ad_group_keyword import (
    KeywordPlanAdGroupKeyword,
)
from .keyword_plan_campaign import (
    KeywordPlanCampaign,
    KeywordPlanGeoTarget,
)
from .keyword_plan_campaign_keyword import (
    KeywordPlanCampaignKeyword,
)
from .keyword_theme_constant import (
    KeywordThemeConstant,
)
from .keyword_view import (
    KeywordView,
)
from .label import (
    Label,
)
from .landing_page_view import (
    LandingPageView,
)
from .language_constant import (
    LanguageConstant,
)
from .lead_form_submission_data import (
    CustomLeadFormSubmissionField,
    LeadFormSubmissionData,
    LeadFormSubmissionField,
)
from .life_event import (
    LifeEvent,
)
from .local_services_employee import (
    Fellowship,
    LocalServicesEmployee,
    Residency,
    UniversityDegree,
)
from .local_services_lead import (
    ContactDetails,
    CreditDetails,
    LocalServicesLead,
    Note,
)
from .local_services_lead_conversation import (
    LocalServicesLeadConversation,
    MessageDetails,
    PhoneCallDetails,
)
from .local_services_verification_artifact import (
    BackgroundCheckVerificationArtifact,
    BusinessRegistrationCheckVerificationArtifact,
    BusinessRegistrationDocument,
    BusinessRegistrationNumber,
    InsuranceVerificationArtifact,
    LicenseVerificationArtifact,
    LocalServicesVerificationArtifact,
)
from .location_view import (
    LocationView,
)
from .managed_placement_view import (
    ManagedPlacementView,
)
from .media_file import (
    MediaAudio,
    MediaBundle,
    MediaFile,
    MediaImage,
    MediaVideo,
)
from .mobile_app_category_constant import (
    MobileAppCategoryConstant,
)
from .mobile_device_constant import (
    MobileDeviceConstant,
)
from .offline_conversion_upload_client_summary import (
    OfflineConversionAlert,
    OfflineConversionError,
    OfflineConversionSummary,
    OfflineConversionUploadClientSummary,
)
from .offline_conversion_upload_conversion_action_summary import (
    OfflineConversionUploadConversionActionSummary,
)
from .offline_user_data_job import (
    OfflineUserDataJob,
    OfflineUserDataJobMetadata,
)
from .operating_system_version_constant import (
    OperatingSystemVersionConstant,
)
from .paid_organic_search_term_view import (
    PaidOrganicSearchTermView,
)
from .parental_status_view import (
    ParentalStatusView,
)
from .payments_account import (
    PaymentsAccount,
)
from .per_store_view import (
    PerStoreView,
)
from .performance_max_placement_view import (
    PerformanceMaxPlacementView,
)
from .product_category_constant import (
    ProductCategoryConstant,
)
from .product_group_view import (
    ProductGroupView,
)
from .product_link import (
    AdvertisingPartnerIdentifier,
    DataPartnerIdentifier,
    GoogleAdsIdentifier,
    MerchantCenterIdentifier,
    ProductLink,
)
from .product_link_invitation import (
    AdvertisingPartnerLinkInvitationIdentifier,
    HotelCenterLinkInvitationIdentifier,
    MerchantCenterLinkInvitationIdentifier,
    ProductLinkInvitation,
)
from .qualifying_question import (
    QualifyingQuestion,
)
from .recommendation import (
    Recommendation,
)
from .recommendation_subscription import (
    RecommendationSubscription,
)
from .remarketing_action import (
    RemarketingAction,
)
from .search_term_view import (
    SearchTermView,
)
from .shared_criterion import (
    SharedCriterion,
)
from .shared_set import (
    SharedSet,
)
from .shopping_performance_view import (
    ShoppingPerformanceView,
)
from .shopping_product import (
    ShoppingProduct,
)
from .smart_campaign_search_term_view import (
    SmartCampaignSearchTermView,
)
from .smart_campaign_setting import (
    SmartCampaignSetting,
)
from .third_party_app_analytics_link import (
    ThirdPartyAppAnalyticsLink,
)
from .topic_constant import (
    TopicConstant,
)
from .topic_view import (
    TopicView,
)
from .travel_activity_group_view import (
    TravelActivityGroupView,
)
from .travel_activity_performance_view import (
    TravelActivityPerformanceView,
)
from .user_interest import (
    UserInterest,
)
from .user_list import (
    UserList,
)
from .user_list_customer_type import (
    UserListCustomerType,
)
from .user_location_view import (
    UserLocationView,
)
from .video import (
    Video,
)
from .webpage_view import (
    WebpageView,
)

__all__ = (
    "AccessibleBiddingStrategy",
    "AccountBudget",
    "AccountBudgetProposal",
    "AccountLink",
    "ThirdPartyAppAnalyticsLinkIdentifier",
    "Ad",
    "AdGroup",
    "AdGroupAd",
    "AdGroupAdAssetAutomationSetting",
    "AdGroupAdPolicySummary",
    "AdGroupAdAssetCombinationView",
    "AdGroupAdAssetPolicySummary",
    "AdGroupAdAssetView",
    "AdGroupAdLabel",
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
    "AgeRangeView",
    "AndroidPrivacySharedKeyGoogleAdGroup",
    "AndroidPrivacySharedKeyGoogleCampaign",
    "AndroidPrivacySharedKeyGoogleNetworkType",
    "Asset",
    "AssetFieldTypePolicySummary",
    "AssetPolicySummary",
    "AssetFieldTypeView",
    "AdStrengthActionItem",
    "AssetCoverage",
    "AssetGroup",
    "AssetGroupAsset",
    "AssetGroupListingGroupFilter",
    "ListingGroupFilterDimension",
    "ListingGroupFilterDimensionPath",
    "AssetGroupProductGroupView",
    "AssetGroupSignal",
    "AssetGroupAssetCombinationData",
    "AssetGroupTopCombinationView",
    "AssetSet",
    "AssetSetAsset",
    "AssetSetTypeView",
    "Audience",
    "BatchJob",
    "BiddingDataExclusion",
    "BiddingSeasonalityAdjustment",
    "BiddingStrategy",
    "BiddingStrategySimulation",
    "BillingSetup",
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
    "CampaignGroup",
    "CampaignLabel",
    "CampaignLifecycleGoal",
    "CustomerAcquisitionGoalSettings",
    "CampaignSearchTermInsight",
    "CampaignSharedSet",
    "CampaignSimulation",
    "CarrierConstant",
    "ChangeEvent",
    "ChangeStatus",
    "ChannelAggregateAssetView",
    "ClickView",
    "CombinedAudience",
    "ContentCriterionView",
    "ConversionAction",
    "ConversionCustomVariable",
    "ConversionGoalCampaignConfig",
    "ConversionValueRule",
    "ConversionValueRuleSet",
    "CurrencyConstant",
    "CustomAudience",
    "CustomAudienceMember",
    "CustomConversionGoal",
    "CustomInterest",
    "CustomInterestMember",
    "CallReportingSetting",
    "ConversionTrackingSetting",
    "Customer",
    "CustomerAgreementSetting",
    "GranularInsuranceStatus",
    "GranularLicenseStatus",
    "LocalServicesSettings",
    "RemarketingSetting",
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
    "YoutubeVideoIdentifier",
    "DetailPlacementView",
    "DetailedDemographic",
    "DisplayKeywordView",
    "DistanceView",
    "DomainCategory",
    "DynamicSearchAdsSearchTermView",
    "ExpandedLandingPageView",
    "Experiment",
    "ExperimentArm",
    "GenderView",
    "GeoTargetConstant",
    "GeographicView",
    "GoogleAdsField",
    "GroupPlacementView",
    "HotelGroupView",
    "HotelPerformanceView",
    "HotelReconciliation",
    "IncomeRangeView",
    "Invoice",
    "KeywordPlan",
    "KeywordPlanForecastPeriod",
    "KeywordPlanAdGroup",
    "KeywordPlanAdGroupKeyword",
    "KeywordPlanCampaign",
    "KeywordPlanGeoTarget",
    "KeywordPlanCampaignKeyword",
    "KeywordThemeConstant",
    "KeywordView",
    "Label",
    "LandingPageView",
    "LanguageConstant",
    "CustomLeadFormSubmissionField",
    "LeadFormSubmissionData",
    "LeadFormSubmissionField",
    "LifeEvent",
    "Fellowship",
    "LocalServicesEmployee",
    "Residency",
    "UniversityDegree",
    "ContactDetails",
    "CreditDetails",
    "LocalServicesLead",
    "Note",
    "LocalServicesLeadConversation",
    "MessageDetails",
    "PhoneCallDetails",
    "BackgroundCheckVerificationArtifact",
    "BusinessRegistrationCheckVerificationArtifact",
    "BusinessRegistrationDocument",
    "BusinessRegistrationNumber",
    "InsuranceVerificationArtifact",
    "LicenseVerificationArtifact",
    "LocalServicesVerificationArtifact",
    "LocationView",
    "ManagedPlacementView",
    "MediaAudio",
    "MediaBundle",
    "MediaFile",
    "MediaImage",
    "MediaVideo",
    "MobileAppCategoryConstant",
    "MobileDeviceConstant",
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
    "ProductCategoryConstant",
    "ProductGroupView",
    "AdvertisingPartnerIdentifier",
    "DataPartnerIdentifier",
    "GoogleAdsIdentifier",
    "MerchantCenterIdentifier",
    "ProductLink",
    "AdvertisingPartnerLinkInvitationIdentifier",
    "HotelCenterLinkInvitationIdentifier",
    "MerchantCenterLinkInvitationIdentifier",
    "ProductLinkInvitation",
    "QualifyingQuestion",
    "Recommendation",
    "RecommendationSubscription",
    "RemarketingAction",
    "SearchTermView",
    "SharedCriterion",
    "SharedSet",
    "ShoppingPerformanceView",
    "ShoppingProduct",
    "SmartCampaignSearchTermView",
    "SmartCampaignSetting",
    "ThirdPartyAppAnalyticsLink",
    "TopicConstant",
    "TopicView",
    "TravelActivityGroupView",
    "TravelActivityPerformanceView",
    "UserInterest",
    "UserList",
    "UserListCustomerType",
    "UserLocationView",
    "Video",
    "WebpageView",
)

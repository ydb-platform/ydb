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
from google.ads.googleads.v20 import gapic_version as package_version

import google.api_core as api_core
import sys

__version__ = package_version.__version__

if sys.version_info >= (3, 8):  # pragma: NO COVER
    from importlib import metadata
else:  # pragma: NO COVER
    # TODO(https://github.com/googleapis/python-api-core/issues/835): Remove
    # this code path once we drop support for Python 3.7
    import importlib_metadata as metadata


from .types.access_invitation_status import AccessInvitationStatusEnum
from .types.access_reason import AccessReasonEnum
from .types.access_role import AccessRoleEnum
from .types.account_budget_proposal_status import (
    AccountBudgetProposalStatusEnum,
)
from .types.account_budget_proposal_type import AccountBudgetProposalTypeEnum
from .types.account_budget_status import AccountBudgetStatusEnum
from .types.account_link_status import AccountLinkStatusEnum
from .types.ad_destination_type import AdDestinationTypeEnum
from .types.ad_format_type import AdFormatTypeEnum
from .types.ad_group_ad_primary_status import AdGroupAdPrimaryStatusEnum
from .types.ad_group_ad_primary_status_reason import (
    AdGroupAdPrimaryStatusReasonEnum,
)
from .types.ad_group_ad_rotation_mode import AdGroupAdRotationModeEnum
from .types.ad_group_ad_status import AdGroupAdStatusEnum
from .types.ad_group_criterion_approval_status import (
    AdGroupCriterionApprovalStatusEnum,
)
from .types.ad_group_criterion_primary_status import (
    AdGroupCriterionPrimaryStatusEnum,
)
from .types.ad_group_criterion_primary_status_reason import (
    AdGroupCriterionPrimaryStatusReasonEnum,
)
from .types.ad_group_criterion_status import AdGroupCriterionStatusEnum
from .types.ad_group_primary_status import AdGroupPrimaryStatusEnum
from .types.ad_group_primary_status_reason import AdGroupPrimaryStatusReasonEnum
from .types.ad_group_status import AdGroupStatusEnum
from .types.ad_group_type import AdGroupTypeEnum
from .types.ad_network_type import AdNetworkTypeEnum
from .types.ad_serving_optimization_status import (
    AdServingOptimizationStatusEnum,
)
from .types.ad_strength import AdStrengthEnum
from .types.ad_strength_action_item_type import AdStrengthActionItemTypeEnum
from .types.ad_type import AdTypeEnum
from .types.advertising_channel_sub_type import AdvertisingChannelSubTypeEnum
from .types.advertising_channel_type import AdvertisingChannelTypeEnum
from .types.age_range_type import AgeRangeTypeEnum
from .types.android_privacy_interaction_type import (
    AndroidPrivacyInteractionTypeEnum,
)
from .types.android_privacy_network_type import AndroidPrivacyNetworkTypeEnum
from .types.app_bidding_goal import AppBiddingGoalEnum
from .types.app_campaign_app_store import AppCampaignAppStoreEnum
from .types.app_campaign_bidding_strategy_goal_type import (
    AppCampaignBiddingStrategyGoalTypeEnum,
)
from .types.app_payment_model_type import AppPaymentModelTypeEnum
from .types.app_url_operating_system_type import AppUrlOperatingSystemTypeEnum
from .types.application_instance import ApplicationInstanceEnum
from .types.asset_automation_status import AssetAutomationStatusEnum
from .types.asset_automation_type import AssetAutomationTypeEnum
from .types.asset_coverage_video_aspect_ratio_requirement import (
    AssetCoverageVideoAspectRatioRequirementEnum,
)
from .types.asset_field_type import AssetFieldTypeEnum
from .types.asset_group_primary_status import AssetGroupPrimaryStatusEnum
from .types.asset_group_primary_status_reason import (
    AssetGroupPrimaryStatusReasonEnum,
)
from .types.asset_group_signal_approval_status import (
    AssetGroupSignalApprovalStatusEnum,
)
from .types.asset_group_status import AssetGroupStatusEnum
from .types.asset_link_primary_status import AssetLinkPrimaryStatusEnum
from .types.asset_link_primary_status_reason import (
    AssetLinkPrimaryStatusReasonEnum,
)
from .types.asset_link_status import AssetLinkStatusEnum
from .types.asset_offline_evaluation_error_reasons import (
    AssetOfflineEvaluationErrorReasonsEnum,
)
from .types.asset_performance_label import AssetPerformanceLabelEnum
from .types.asset_set_asset_status import AssetSetAssetStatusEnum
from .types.asset_set_link_status import AssetSetLinkStatusEnum
from .types.asset_set_status import AssetSetStatusEnum
from .types.asset_set_type import AssetSetTypeEnum
from .types.asset_source import AssetSourceEnum
from .types.asset_type import AssetTypeEnum
from .types.async_action_status import AsyncActionStatusEnum
from .types.attribution_model import AttributionModelEnum
from .types.audience_insights_dimension import AudienceInsightsDimensionEnum
from .types.audience_insights_marketing_objective import (
    AudienceInsightsMarketingObjectiveEnum,
)
from .types.audience_scope import AudienceScopeEnum
from .types.audience_status import AudienceStatusEnum
from .types.batch_job_status import BatchJobStatusEnum
from .types.bid_modifier_source import BidModifierSourceEnum
from .types.bidding_source import BiddingSourceEnum
from .types.bidding_strategy_status import BiddingStrategyStatusEnum
from .types.bidding_strategy_system_status import (
    BiddingStrategySystemStatusEnum,
)
from .types.bidding_strategy_type import BiddingStrategyTypeEnum
from .types.billing_setup_status import BillingSetupStatusEnum
from .types.brand_request_rejection_reason import (
    BrandRequestRejectionReasonEnum,
)
from .types.brand_safety_suitability import BrandSafetySuitabilityEnum
from .types.brand_state import BrandStateEnum
from .types.budget_campaign_association_status import (
    BudgetCampaignAssociationStatusEnum,
)
from .types.budget_delivery_method import BudgetDeliveryMethodEnum
from .types.budget_period import BudgetPeriodEnum
from .types.budget_status import BudgetStatusEnum
from .types.budget_type import BudgetTypeEnum
from .types.business_message_call_to_action_type import (
    BusinessMessageCallToActionTypeEnum,
)
from .types.business_message_provider import BusinessMessageProviderEnum
from .types.call_conversion_reporting_state import (
    CallConversionReportingStateEnum,
)
from .types.call_to_action_type import CallToActionTypeEnum
from .types.call_tracking_display_location import (
    CallTrackingDisplayLocationEnum,
)
from .types.call_type import CallTypeEnum
from .types.campaign_criterion_status import CampaignCriterionStatusEnum
from .types.campaign_draft_status import CampaignDraftStatusEnum
from .types.campaign_experiment_type import CampaignExperimentTypeEnum
from .types.campaign_group_status import CampaignGroupStatusEnum
from .types.campaign_keyword_match_type import CampaignKeywordMatchTypeEnum
from .types.campaign_primary_status import CampaignPrimaryStatusEnum
from .types.campaign_primary_status_reason import (
    CampaignPrimaryStatusReasonEnum,
)
from .types.campaign_serving_status import CampaignServingStatusEnum
from .types.campaign_shared_set_status import CampaignSharedSetStatusEnum
from .types.campaign_status import CampaignStatusEnum
from .types.chain_relationship_type import ChainRelationshipTypeEnum
from .types.change_client_type import ChangeClientTypeEnum
from .types.change_event_resource_type import ChangeEventResourceTypeEnum
from .types.change_status_operation import ChangeStatusOperationEnum
from .types.change_status_resource_type import ChangeStatusResourceTypeEnum
from .types.click_type import ClickTypeEnum
from .types.combined_audience_status import CombinedAudienceStatusEnum
from .types.consent_status import ConsentStatusEnum
from .types.content_label_type import ContentLabelTypeEnum
from .types.conversion_action_category import ConversionActionCategoryEnum
from .types.conversion_action_counting_type import (
    ConversionActionCountingTypeEnum,
)
from .types.conversion_action_status import ConversionActionStatusEnum
from .types.conversion_action_type import ConversionActionTypeEnum
from .types.conversion_adjustment_type import ConversionAdjustmentTypeEnum
from .types.conversion_attribution_event_type import (
    ConversionAttributionEventTypeEnum,
)
from .types.conversion_custom_variable_status import (
    ConversionCustomVariableStatusEnum,
)
from .types.conversion_customer_type import ConversionCustomerTypeEnum
from .types.conversion_environment_enum import ConversionEnvironmentEnum
from .types.conversion_lag_bucket import ConversionLagBucketEnum
from .types.conversion_or_adjustment_lag_bucket import (
    ConversionOrAdjustmentLagBucketEnum,
)
from .types.conversion_origin import ConversionOriginEnum
from .types.conversion_tracking_status_enum import ConversionTrackingStatusEnum
from .types.conversion_value_rule_primary_dimension import (
    ConversionValueRulePrimaryDimensionEnum,
)
from .types.conversion_value_rule_set_status import (
    ConversionValueRuleSetStatusEnum,
)
from .types.conversion_value_rule_status import ConversionValueRuleStatusEnum
from .types.converting_user_prior_engagement_type_and_ltv_bucket import (
    ConvertingUserPriorEngagementTypeAndLtvBucketEnum,
)
from .types.criterion_category_channel_availability_mode import (
    CriterionCategoryChannelAvailabilityModeEnum,
)
from .types.criterion_category_locale_availability_mode import (
    CriterionCategoryLocaleAvailabilityModeEnum,
)
from .types.criterion_system_serving_status import (
    CriterionSystemServingStatusEnum,
)
from .types.criterion_type import CriterionTypeEnum
from .types.custom_audience_member_type import CustomAudienceMemberTypeEnum
from .types.custom_audience_status import CustomAudienceStatusEnum
from .types.custom_audience_type import CustomAudienceTypeEnum
from .types.custom_conversion_goal_status import CustomConversionGoalStatusEnum
from .types.custom_interest_member_type import CustomInterestMemberTypeEnum
from .types.custom_interest_status import CustomInterestStatusEnum
from .types.custom_interest_type import CustomInterestTypeEnum
from .types.customer_acquisition_optimization_mode import (
    CustomerAcquisitionOptimizationModeEnum,
)
from .types.customer_match_upload_key_type import CustomerMatchUploadKeyTypeEnum
from .types.customer_pay_per_conversion_eligibility_failure_reason import (
    CustomerPayPerConversionEligibilityFailureReasonEnum,
)
from .types.customer_status import CustomerStatusEnum
from .types.customizer_attribute_status import CustomizerAttributeStatusEnum
from .types.customizer_attribute_type import CustomizerAttributeTypeEnum
from .types.customizer_value_status import CustomizerValueStatusEnum
from .types.data_driven_model_status import DataDrivenModelStatusEnum
from .types.data_link_status import DataLinkStatusEnum
from .types.data_link_type import DataLinkTypeEnum
from .types.day_of_week import DayOfWeekEnum
from .types.demand_gen_channel_config import DemandGenChannelConfigEnum
from .types.demand_gen_channel_strategy import DemandGenChannelStrategyEnum
from .types.device import DeviceEnum
from .types.display_ad_format_setting import DisplayAdFormatSettingEnum
from .types.display_upload_product_type import DisplayUploadProductTypeEnum
from .types.distance_bucket import DistanceBucketEnum
from .types.eu_political_advertising_status import (
    EuPoliticalAdvertisingStatusEnum,
)
from .types.experiment_metric import ExperimentMetricEnum
from .types.experiment_metric_direction import ExperimentMetricDirectionEnum
from .types.experiment_status import ExperimentStatusEnum
from .types.experiment_type import ExperimentTypeEnum
from .types.external_conversion_source import ExternalConversionSourceEnum
from .types.fixed_cpm_goal import FixedCpmGoalEnum
from .types.fixed_cpm_target_frequency_time_unit import (
    FixedCpmTargetFrequencyTimeUnitEnum,
)
from .types.frequency_cap_event_type import FrequencyCapEventTypeEnum
from .types.frequency_cap_level import FrequencyCapLevelEnum
from .types.frequency_cap_time_unit import FrequencyCapTimeUnitEnum
from .types.gender_type import GenderTypeEnum
from .types.geo_target_constant_status import GeoTargetConstantStatusEnum
from .types.geo_targeting_type import GeoTargetingTypeEnum
from .types.goal_config_level import GoalConfigLevelEnum
from .types.google_ads_field_category import GoogleAdsFieldCategoryEnum
from .types.google_ads_field_data_type import GoogleAdsFieldDataTypeEnum
from .types.google_voice_call_status import GoogleVoiceCallStatusEnum
from .types.hotel_asset_suggestion_status import HotelAssetSuggestionStatusEnum
from .types.hotel_date_selection_type import HotelDateSelectionTypeEnum
from .types.hotel_price_bucket import HotelPriceBucketEnum
from .types.hotel_rate_type import HotelRateTypeEnum
from .types.hotel_reconciliation_status import HotelReconciliationStatusEnum
from .types.identity_verification_program import IdentityVerificationProgramEnum
from .types.identity_verification_program_status import (
    IdentityVerificationProgramStatusEnum,
)
from .types.income_range_type import IncomeRangeTypeEnum
from .types.insights_knowledge_graph_entity_capabilities import (
    InsightsKnowledgeGraphEntityCapabilitiesEnum,
)
from .types.insights_trend import InsightsTrendEnum
from .types.interaction_event_type import InteractionEventTypeEnum
from .types.interaction_type import InteractionTypeEnum
from .types.invoice_type import InvoiceTypeEnum
from .types.keyword_match_type import KeywordMatchTypeEnum
from .types.keyword_plan_aggregate_metric_type import (
    KeywordPlanAggregateMetricTypeEnum,
)
from .types.keyword_plan_competition_level import (
    KeywordPlanCompetitionLevelEnum,
)
from .types.keyword_plan_concept_group_type import (
    KeywordPlanConceptGroupTypeEnum,
)
from .types.keyword_plan_forecast_interval import (
    KeywordPlanForecastIntervalEnum,
)
from .types.keyword_plan_keyword_annotation import (
    KeywordPlanKeywordAnnotationEnum,
)
from .types.keyword_plan_network import KeywordPlanNetworkEnum
from .types.label_status import LabelStatusEnum
from .types.lead_form_call_to_action_type import LeadFormCallToActionTypeEnum
from .types.lead_form_desired_intent import LeadFormDesiredIntentEnum
from .types.lead_form_field_user_input_type import (
    LeadFormFieldUserInputTypeEnum,
)
from .types.lead_form_post_submit_call_to_action_type import (
    LeadFormPostSubmitCallToActionTypeEnum,
)
from .types.legacy_app_install_ad_app_store import (
    LegacyAppInstallAdAppStoreEnum,
)
from .types.linked_account_type import LinkedAccountTypeEnum
from .types.linked_product_type import LinkedProductTypeEnum
from .types.listing_group_filter_custom_attribute_index import (
    ListingGroupFilterCustomAttributeIndexEnum,
)
from .types.listing_group_filter_listing_source import (
    ListingGroupFilterListingSourceEnum,
)
from .types.listing_group_filter_product_category_level import (
    ListingGroupFilterProductCategoryLevelEnum,
)
from .types.listing_group_filter_product_channel import (
    ListingGroupFilterProductChannelEnum,
)
from .types.listing_group_filter_product_condition import (
    ListingGroupFilterProductConditionEnum,
)
from .types.listing_group_filter_product_type_level import (
    ListingGroupFilterProductTypeLevelEnum,
)
from .types.listing_group_filter_type_enum import ListingGroupFilterTypeEnum
from .types.listing_group_type import ListingGroupTypeEnum
from .types.listing_type import ListingTypeEnum
from .types.local_services_business_registration_check_rejection_reason import (
    LocalServicesBusinessRegistrationCheckRejectionReasonEnum,
)
from .types.local_services_business_registration_type import (
    LocalServicesBusinessRegistrationTypeEnum,
)
from .types.local_services_conversation_type import (
    LocalServicesLeadConversationTypeEnum,
)
from .types.local_services_employee_status import (
    LocalServicesEmployeeStatusEnum,
)
from .types.local_services_employee_type import LocalServicesEmployeeTypeEnum
from .types.local_services_insurance_rejection_reason import (
    LocalServicesInsuranceRejectionReasonEnum,
)
from .types.local_services_lead_credit_issuance_decision import (
    LocalServicesLeadCreditIssuanceDecisionEnum,
)
from .types.local_services_lead_credit_state import LocalServicesCreditStateEnum
from .types.local_services_lead_status import LocalServicesLeadStatusEnum
from .types.local_services_lead_survey_answer import (
    LocalServicesLeadSurveyAnswerEnum,
)
from .types.local_services_lead_survey_dissatisfied_reason import (
    LocalServicesLeadSurveyDissatisfiedReasonEnum,
)
from .types.local_services_lead_survey_satisfied_reason import (
    LocalServicesLeadSurveySatisfiedReasonEnum,
)
from .types.local_services_lead_type import LocalServicesLeadTypeEnum
from .types.local_services_license_rejection_reason import (
    LocalServicesLicenseRejectionReasonEnum,
)
from .types.local_services_participant_type import (
    LocalServicesParticipantTypeEnum,
)
from .types.local_services_verification_artifact_status import (
    LocalServicesVerificationArtifactStatusEnum,
)
from .types.local_services_verification_artifact_type import (
    LocalServicesVerificationArtifactTypeEnum,
)
from .types.local_services_verification_status import (
    LocalServicesVerificationStatusEnum,
)
from .types.location_group_radius_units import LocationGroupRadiusUnitsEnum
from .types.location_ownership_type import LocationOwnershipTypeEnum
from .types.location_source_type import LocationSourceTypeEnum
from .types.location_string_filter_type import LocationStringFilterTypeEnum
from .types.lookalike_expansion_level import LookalikeExpansionLevelEnum
from .types.manager_link_status import ManagerLinkStatusEnum
from .types.media_type import MediaTypeEnum
from .types.mime_type import MimeTypeEnum
from .types.minute_of_hour import MinuteOfHourEnum
from .types.mobile_app_vendor import MobileAppVendorEnum
from .types.mobile_device_type import MobileDeviceTypeEnum
from .types.month_of_year import MonthOfYearEnum
from .types.negative_geo_target_type import NegativeGeoTargetTypeEnum
from .types.non_skippable_max_duration import NonSkippableMaxDurationEnum
from .types.non_skippable_min_duration import NonSkippableMinDurationEnum
from .types.offline_conversion_diagnostic_status_enum import (
    OfflineConversionDiagnosticStatusEnum,
)
from .types.offline_event_upload_client_enum import OfflineEventUploadClientEnum
from .types.offline_user_data_job_failure_reason import (
    OfflineUserDataJobFailureReasonEnum,
)
from .types.offline_user_data_job_match_rate_range import (
    OfflineUserDataJobMatchRateRangeEnum,
)
from .types.offline_user_data_job_status import OfflineUserDataJobStatusEnum
from .types.offline_user_data_job_type import OfflineUserDataJobTypeEnum
from .types.operating_system_version_operator_type import (
    OperatingSystemVersionOperatorTypeEnum,
)
from .types.optimization_goal_type import OptimizationGoalTypeEnum
from .types.parental_status_type import ParentalStatusTypeEnum
from .types.payment_mode import PaymentModeEnum
from .types.performance_max_upgrade_status import (
    PerformanceMaxUpgradeStatusEnum,
)
from .types.placement_type import PlacementTypeEnum
from .types.policy_approval_status import PolicyApprovalStatusEnum
from .types.policy_review_status import PolicyReviewStatusEnum
from .types.policy_topic_entry_type import PolicyTopicEntryTypeEnum
from .types.policy_topic_evidence_destination_mismatch_url_type import (
    PolicyTopicEvidenceDestinationMismatchUrlTypeEnum,
)
from .types.policy_topic_evidence_destination_not_working_device import (
    PolicyTopicEvidenceDestinationNotWorkingDeviceEnum,
)
from .types.policy_topic_evidence_destination_not_working_dns_error_type import (
    PolicyTopicEvidenceDestinationNotWorkingDnsErrorTypeEnum,
)
from .types.positive_geo_target_type import PositiveGeoTargetTypeEnum
from .types.price_extension_price_qualifier import (
    PriceExtensionPriceQualifierEnum,
)
from .types.price_extension_price_unit import PriceExtensionPriceUnitEnum
from .types.price_extension_type import PriceExtensionTypeEnum
from .types.product_availability import ProductAvailabilityEnum
from .types.product_category_level import ProductCategoryLevelEnum
from .types.product_category_state import ProductCategoryStateEnum
from .types.product_channel import ProductChannelEnum
from .types.product_channel_exclusivity import ProductChannelExclusivityEnum
from .types.product_condition import ProductConditionEnum
from .types.product_custom_attribute_index import (
    ProductCustomAttributeIndexEnum,
)
from .types.product_issue_severity import ProductIssueSeverityEnum
from .types.product_link_invitation_status import (
    ProductLinkInvitationStatusEnum,
)
from .types.product_status import ProductStatusEnum
from .types.product_type_level import ProductTypeLevelEnum
from .types.promotion_extension_discount_modifier import (
    PromotionExtensionDiscountModifierEnum,
)
from .types.promotion_extension_occasion import PromotionExtensionOccasionEnum
from .types.proximity_radius_units import ProximityRadiusUnitsEnum
from .types.quality_score_bucket import QualityScoreBucketEnum
from .types.reach_plan_age_range import ReachPlanAgeRangeEnum
from .types.reach_plan_conversion_rate_model import (
    ReachPlanConversionRateModelEnum,
)
from .types.reach_plan_network import ReachPlanNetworkEnum
from .types.reach_plan_plannable_user_list_status import (
    ReachPlanPlannableUserListStatusEnum,
)
from .types.reach_plan_surface import ReachPlanSurfaceEnum
from .types.recommendation_subscription_status import (
    RecommendationSubscriptionStatusEnum,
)
from .types.recommendation_type import RecommendationTypeEnum
from .types.resource_change_operation import ResourceChangeOperationEnum
from .types.resource_limit_type import ResourceLimitTypeEnum
from .types.response_content_type import ResponseContentTypeEnum
from .types.search_engine_results_page_type import (
    SearchEngineResultsPageTypeEnum,
)
from .types.search_term_match_type import SearchTermMatchTypeEnum
from .types.search_term_targeting_status import SearchTermTargetingStatusEnum
from .types.seasonality_event_scope import SeasonalityEventScopeEnum
from .types.seasonality_event_status import SeasonalityEventStatusEnum
from .types.served_asset_field_type import ServedAssetFieldTypeEnum
from .types.shared_set_status import SharedSetStatusEnum
from .types.shared_set_type import SharedSetTypeEnum
from .types.shopping_add_products_to_campaign_recommendation_enum import (
    ShoppingAddProductsToCampaignRecommendationEnum,
)
from .types.simulation_modification_method import (
    SimulationModificationMethodEnum,
)
from .types.simulation_type import SimulationTypeEnum
from .types.sk_ad_network_ad_event_type import SkAdNetworkAdEventTypeEnum
from .types.sk_ad_network_attribution_credit import (
    SkAdNetworkAttributionCreditEnum,
)
from .types.sk_ad_network_coarse_conversion_value import (
    SkAdNetworkCoarseConversionValueEnum,
)
from .types.sk_ad_network_source_type import SkAdNetworkSourceTypeEnum
from .types.sk_ad_network_user_type import SkAdNetworkUserTypeEnum
from .types.slot import SlotEnum
from .types.smart_campaign_not_eligible_reason import (
    SmartCampaignNotEligibleReasonEnum,
)
from .types.smart_campaign_status import SmartCampaignStatusEnum
from .types.spending_limit_type import SpendingLimitTypeEnum
from .types.summary_row_setting import SummaryRowSettingEnum
from .types.system_managed_entity_source import SystemManagedResourceSourceEnum
from .types.target_cpa_opt_in_recommendation_goal import (
    TargetCpaOptInRecommendationGoalEnum,
)
from .types.target_frequency_time_unit import TargetFrequencyTimeUnitEnum
from .types.target_impression_share_location import (
    TargetImpressionShareLocationEnum,
)
from .types.targeting_dimension import TargetingDimensionEnum
from .types.time_type import TimeTypeEnum
from .types.tracking_code_page_format import TrackingCodePageFormatEnum
from .types.tracking_code_type import TrackingCodeTypeEnum
from .types.user_identifier_source import UserIdentifierSourceEnum
from .types.user_interest_taxonomy_type import UserInterestTaxonomyTypeEnum
from .types.user_list_access_status import UserListAccessStatusEnum
from .types.user_list_closing_reason import UserListClosingReasonEnum
from .types.user_list_crm_data_source_type import UserListCrmDataSourceTypeEnum
from .types.user_list_customer_type_category import (
    UserListCustomerTypeCategoryEnum,
)
from .types.user_list_date_rule_item_operator import (
    UserListDateRuleItemOperatorEnum,
)
from .types.user_list_flexible_rule_operator import (
    UserListFlexibleRuleOperatorEnum,
)
from .types.user_list_logical_rule_operator import (
    UserListLogicalRuleOperatorEnum,
)
from .types.user_list_membership_status import UserListMembershipStatusEnum
from .types.user_list_number_rule_item_operator import (
    UserListNumberRuleItemOperatorEnum,
)
from .types.user_list_prepopulation_status import (
    UserListPrepopulationStatusEnum,
)
from .types.user_list_rule_type import UserListRuleTypeEnum
from .types.user_list_size_range import UserListSizeRangeEnum
from .types.user_list_string_rule_item_operator import (
    UserListStringRuleItemOperatorEnum,
)
from .types.user_list_type import UserListTypeEnum
from .types.value_rule_device_type import ValueRuleDeviceTypeEnum
from .types.value_rule_geo_location_match_type import (
    ValueRuleGeoLocationMatchTypeEnum,
)
from .types.value_rule_operation import ValueRuleOperationEnum
from .types.value_rule_set_attachment_type import ValueRuleSetAttachmentTypeEnum
from .types.value_rule_set_dimension import ValueRuleSetDimensionEnum
from .types.vanity_pharma_display_url_mode import VanityPharmaDisplayUrlModeEnum
from .types.vanity_pharma_text import VanityPharmaTextEnum
from .types.video_ad_format_restriction import VideoAdFormatRestrictionEnum
from .types.video_thumbnail import VideoThumbnailEnum
from .types.webpage_condition_operand import WebpageConditionOperandEnum
from .types.webpage_condition_operator import WebpageConditionOperatorEnum

if hasattr(api_core, "check_python_version") and hasattr(
    api_core, "check_dependency_versions"
):  # pragma: NO COVER
    api_core.check_python_version("google.ads.googleads.v20")  # type: ignore
    api_core.check_dependency_versions("google.ads.googleads.v20")  # type: ignore
else:  # pragma: NO COVER
    # An older version of api_core is installed which does not define the
    # functions above. We do equivalent checks manually.
    try:
        import warnings
        import sys

        _py_version_str = sys.version.split()[0]
        _package_label = "google.ads.googleads.v20"
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
    "AccessInvitationStatusEnum",
    "AccessReasonEnum",
    "AccessRoleEnum",
    "AccountBudgetProposalStatusEnum",
    "AccountBudgetProposalTypeEnum",
    "AccountBudgetStatusEnum",
    "AccountLinkStatusEnum",
    "AdDestinationTypeEnum",
    "AdFormatTypeEnum",
    "AdGroupAdPrimaryStatusEnum",
    "AdGroupAdPrimaryStatusReasonEnum",
    "AdGroupAdRotationModeEnum",
    "AdGroupAdStatusEnum",
    "AdGroupCriterionApprovalStatusEnum",
    "AdGroupCriterionPrimaryStatusEnum",
    "AdGroupCriterionPrimaryStatusReasonEnum",
    "AdGroupCriterionStatusEnum",
    "AdGroupPrimaryStatusEnum",
    "AdGroupPrimaryStatusReasonEnum",
    "AdGroupStatusEnum",
    "AdGroupTypeEnum",
    "AdNetworkTypeEnum",
    "AdServingOptimizationStatusEnum",
    "AdStrengthActionItemTypeEnum",
    "AdStrengthEnum",
    "AdTypeEnum",
    "AdvertisingChannelSubTypeEnum",
    "AdvertisingChannelTypeEnum",
    "AgeRangeTypeEnum",
    "AndroidPrivacyInteractionTypeEnum",
    "AndroidPrivacyNetworkTypeEnum",
    "AppBiddingGoalEnum",
    "AppCampaignAppStoreEnum",
    "AppCampaignBiddingStrategyGoalTypeEnum",
    "AppPaymentModelTypeEnum",
    "AppUrlOperatingSystemTypeEnum",
    "ApplicationInstanceEnum",
    "AssetAutomationStatusEnum",
    "AssetAutomationTypeEnum",
    "AssetCoverageVideoAspectRatioRequirementEnum",
    "AssetFieldTypeEnum",
    "AssetGroupPrimaryStatusEnum",
    "AssetGroupPrimaryStatusReasonEnum",
    "AssetGroupSignalApprovalStatusEnum",
    "AssetGroupStatusEnum",
    "AssetLinkPrimaryStatusEnum",
    "AssetLinkPrimaryStatusReasonEnum",
    "AssetLinkStatusEnum",
    "AssetOfflineEvaluationErrorReasonsEnum",
    "AssetPerformanceLabelEnum",
    "AssetSetAssetStatusEnum",
    "AssetSetLinkStatusEnum",
    "AssetSetStatusEnum",
    "AssetSetTypeEnum",
    "AssetSourceEnum",
    "AssetTypeEnum",
    "AsyncActionStatusEnum",
    "AttributionModelEnum",
    "AudienceInsightsDimensionEnum",
    "AudienceInsightsMarketingObjectiveEnum",
    "AudienceScopeEnum",
    "AudienceStatusEnum",
    "BatchJobStatusEnum",
    "BidModifierSourceEnum",
    "BiddingSourceEnum",
    "BiddingStrategyStatusEnum",
    "BiddingStrategySystemStatusEnum",
    "BiddingStrategyTypeEnum",
    "BillingSetupStatusEnum",
    "BrandRequestRejectionReasonEnum",
    "BrandSafetySuitabilityEnum",
    "BrandStateEnum",
    "BudgetCampaignAssociationStatusEnum",
    "BudgetDeliveryMethodEnum",
    "BudgetPeriodEnum",
    "BudgetStatusEnum",
    "BudgetTypeEnum",
    "BusinessMessageCallToActionTypeEnum",
    "BusinessMessageProviderEnum",
    "CallConversionReportingStateEnum",
    "CallToActionTypeEnum",
    "CallTrackingDisplayLocationEnum",
    "CallTypeEnum",
    "CampaignCriterionStatusEnum",
    "CampaignDraftStatusEnum",
    "CampaignExperimentTypeEnum",
    "CampaignGroupStatusEnum",
    "CampaignKeywordMatchTypeEnum",
    "CampaignPrimaryStatusEnum",
    "CampaignPrimaryStatusReasonEnum",
    "CampaignServingStatusEnum",
    "CampaignSharedSetStatusEnum",
    "CampaignStatusEnum",
    "ChainRelationshipTypeEnum",
    "ChangeClientTypeEnum",
    "ChangeEventResourceTypeEnum",
    "ChangeStatusOperationEnum",
    "ChangeStatusResourceTypeEnum",
    "ClickTypeEnum",
    "CombinedAudienceStatusEnum",
    "ConsentStatusEnum",
    "ContentLabelTypeEnum",
    "ConversionActionCategoryEnum",
    "ConversionActionCountingTypeEnum",
    "ConversionActionStatusEnum",
    "ConversionActionTypeEnum",
    "ConversionAdjustmentTypeEnum",
    "ConversionAttributionEventTypeEnum",
    "ConversionCustomVariableStatusEnum",
    "ConversionCustomerTypeEnum",
    "ConversionEnvironmentEnum",
    "ConversionLagBucketEnum",
    "ConversionOrAdjustmentLagBucketEnum",
    "ConversionOriginEnum",
    "ConversionTrackingStatusEnum",
    "ConversionValueRulePrimaryDimensionEnum",
    "ConversionValueRuleSetStatusEnum",
    "ConversionValueRuleStatusEnum",
    "ConvertingUserPriorEngagementTypeAndLtvBucketEnum",
    "CriterionCategoryChannelAvailabilityModeEnum",
    "CriterionCategoryLocaleAvailabilityModeEnum",
    "CriterionSystemServingStatusEnum",
    "CriterionTypeEnum",
    "CustomAudienceMemberTypeEnum",
    "CustomAudienceStatusEnum",
    "CustomAudienceTypeEnum",
    "CustomConversionGoalStatusEnum",
    "CustomInterestMemberTypeEnum",
    "CustomInterestStatusEnum",
    "CustomInterestTypeEnum",
    "CustomerAcquisitionOptimizationModeEnum",
    "CustomerMatchUploadKeyTypeEnum",
    "CustomerPayPerConversionEligibilityFailureReasonEnum",
    "CustomerStatusEnum",
    "CustomizerAttributeStatusEnum",
    "CustomizerAttributeTypeEnum",
    "CustomizerValueStatusEnum",
    "DataDrivenModelStatusEnum",
    "DataLinkStatusEnum",
    "DataLinkTypeEnum",
    "DayOfWeekEnum",
    "DemandGenChannelConfigEnum",
    "DemandGenChannelStrategyEnum",
    "DeviceEnum",
    "DisplayAdFormatSettingEnum",
    "DisplayUploadProductTypeEnum",
    "DistanceBucketEnum",
    "EuPoliticalAdvertisingStatusEnum",
    "ExperimentMetricDirectionEnum",
    "ExperimentMetricEnum",
    "ExperimentStatusEnum",
    "ExperimentTypeEnum",
    "ExternalConversionSourceEnum",
    "FixedCpmGoalEnum",
    "FixedCpmTargetFrequencyTimeUnitEnum",
    "FrequencyCapEventTypeEnum",
    "FrequencyCapLevelEnum",
    "FrequencyCapTimeUnitEnum",
    "GenderTypeEnum",
    "GeoTargetConstantStatusEnum",
    "GeoTargetingTypeEnum",
    "GoalConfigLevelEnum",
    "GoogleAdsFieldCategoryEnum",
    "GoogleAdsFieldDataTypeEnum",
    "GoogleVoiceCallStatusEnum",
    "HotelAssetSuggestionStatusEnum",
    "HotelDateSelectionTypeEnum",
    "HotelPriceBucketEnum",
    "HotelRateTypeEnum",
    "HotelReconciliationStatusEnum",
    "IdentityVerificationProgramEnum",
    "IdentityVerificationProgramStatusEnum",
    "IncomeRangeTypeEnum",
    "InsightsKnowledgeGraphEntityCapabilitiesEnum",
    "InsightsTrendEnum",
    "InteractionEventTypeEnum",
    "InteractionTypeEnum",
    "InvoiceTypeEnum",
    "KeywordMatchTypeEnum",
    "KeywordPlanAggregateMetricTypeEnum",
    "KeywordPlanCompetitionLevelEnum",
    "KeywordPlanConceptGroupTypeEnum",
    "KeywordPlanForecastIntervalEnum",
    "KeywordPlanKeywordAnnotationEnum",
    "KeywordPlanNetworkEnum",
    "LabelStatusEnum",
    "LeadFormCallToActionTypeEnum",
    "LeadFormDesiredIntentEnum",
    "LeadFormFieldUserInputTypeEnum",
    "LeadFormPostSubmitCallToActionTypeEnum",
    "LegacyAppInstallAdAppStoreEnum",
    "LinkedAccountTypeEnum",
    "LinkedProductTypeEnum",
    "ListingGroupFilterCustomAttributeIndexEnum",
    "ListingGroupFilterListingSourceEnum",
    "ListingGroupFilterProductCategoryLevelEnum",
    "ListingGroupFilterProductChannelEnum",
    "ListingGroupFilterProductConditionEnum",
    "ListingGroupFilterProductTypeLevelEnum",
    "ListingGroupFilterTypeEnum",
    "ListingGroupTypeEnum",
    "ListingTypeEnum",
    "LocalServicesBusinessRegistrationCheckRejectionReasonEnum",
    "LocalServicesBusinessRegistrationTypeEnum",
    "LocalServicesCreditStateEnum",
    "LocalServicesEmployeeStatusEnum",
    "LocalServicesEmployeeTypeEnum",
    "LocalServicesInsuranceRejectionReasonEnum",
    "LocalServicesLeadConversationTypeEnum",
    "LocalServicesLeadCreditIssuanceDecisionEnum",
    "LocalServicesLeadStatusEnum",
    "LocalServicesLeadSurveyAnswerEnum",
    "LocalServicesLeadSurveyDissatisfiedReasonEnum",
    "LocalServicesLeadSurveySatisfiedReasonEnum",
    "LocalServicesLeadTypeEnum",
    "LocalServicesLicenseRejectionReasonEnum",
    "LocalServicesParticipantTypeEnum",
    "LocalServicesVerificationArtifactStatusEnum",
    "LocalServicesVerificationArtifactTypeEnum",
    "LocalServicesVerificationStatusEnum",
    "LocationGroupRadiusUnitsEnum",
    "LocationOwnershipTypeEnum",
    "LocationSourceTypeEnum",
    "LocationStringFilterTypeEnum",
    "LookalikeExpansionLevelEnum",
    "ManagerLinkStatusEnum",
    "MediaTypeEnum",
    "MimeTypeEnum",
    "MinuteOfHourEnum",
    "MobileAppVendorEnum",
    "MobileDeviceTypeEnum",
    "MonthOfYearEnum",
    "NegativeGeoTargetTypeEnum",
    "NonSkippableMaxDurationEnum",
    "NonSkippableMinDurationEnum",
    "OfflineConversionDiagnosticStatusEnum",
    "OfflineEventUploadClientEnum",
    "OfflineUserDataJobFailureReasonEnum",
    "OfflineUserDataJobMatchRateRangeEnum",
    "OfflineUserDataJobStatusEnum",
    "OfflineUserDataJobTypeEnum",
    "OperatingSystemVersionOperatorTypeEnum",
    "OptimizationGoalTypeEnum",
    "ParentalStatusTypeEnum",
    "PaymentModeEnum",
    "PerformanceMaxUpgradeStatusEnum",
    "PlacementTypeEnum",
    "PolicyApprovalStatusEnum",
    "PolicyReviewStatusEnum",
    "PolicyTopicEntryTypeEnum",
    "PolicyTopicEvidenceDestinationMismatchUrlTypeEnum",
    "PolicyTopicEvidenceDestinationNotWorkingDeviceEnum",
    "PolicyTopicEvidenceDestinationNotWorkingDnsErrorTypeEnum",
    "PositiveGeoTargetTypeEnum",
    "PriceExtensionPriceQualifierEnum",
    "PriceExtensionPriceUnitEnum",
    "PriceExtensionTypeEnum",
    "ProductAvailabilityEnum",
    "ProductCategoryLevelEnum",
    "ProductCategoryStateEnum",
    "ProductChannelEnum",
    "ProductChannelExclusivityEnum",
    "ProductConditionEnum",
    "ProductCustomAttributeIndexEnum",
    "ProductIssueSeverityEnum",
    "ProductLinkInvitationStatusEnum",
    "ProductStatusEnum",
    "ProductTypeLevelEnum",
    "PromotionExtensionDiscountModifierEnum",
    "PromotionExtensionOccasionEnum",
    "ProximityRadiusUnitsEnum",
    "QualityScoreBucketEnum",
    "ReachPlanAgeRangeEnum",
    "ReachPlanConversionRateModelEnum",
    "ReachPlanNetworkEnum",
    "ReachPlanPlannableUserListStatusEnum",
    "ReachPlanSurfaceEnum",
    "RecommendationSubscriptionStatusEnum",
    "RecommendationTypeEnum",
    "ResourceChangeOperationEnum",
    "ResourceLimitTypeEnum",
    "ResponseContentTypeEnum",
    "SearchEngineResultsPageTypeEnum",
    "SearchTermMatchTypeEnum",
    "SearchTermTargetingStatusEnum",
    "SeasonalityEventScopeEnum",
    "SeasonalityEventStatusEnum",
    "ServedAssetFieldTypeEnum",
    "SharedSetStatusEnum",
    "SharedSetTypeEnum",
    "ShoppingAddProductsToCampaignRecommendationEnum",
    "SimulationModificationMethodEnum",
    "SimulationTypeEnum",
    "SkAdNetworkAdEventTypeEnum",
    "SkAdNetworkAttributionCreditEnum",
    "SkAdNetworkCoarseConversionValueEnum",
    "SkAdNetworkSourceTypeEnum",
    "SkAdNetworkUserTypeEnum",
    "SlotEnum",
    "SmartCampaignNotEligibleReasonEnum",
    "SmartCampaignStatusEnum",
    "SpendingLimitTypeEnum",
    "SummaryRowSettingEnum",
    "SystemManagedResourceSourceEnum",
    "TargetCpaOptInRecommendationGoalEnum",
    "TargetFrequencyTimeUnitEnum",
    "TargetImpressionShareLocationEnum",
    "TargetingDimensionEnum",
    "TimeTypeEnum",
    "TrackingCodePageFormatEnum",
    "TrackingCodeTypeEnum",
    "UserIdentifierSourceEnum",
    "UserInterestTaxonomyTypeEnum",
    "UserListAccessStatusEnum",
    "UserListClosingReasonEnum",
    "UserListCrmDataSourceTypeEnum",
    "UserListCustomerTypeCategoryEnum",
    "UserListDateRuleItemOperatorEnum",
    "UserListFlexibleRuleOperatorEnum",
    "UserListLogicalRuleOperatorEnum",
    "UserListMembershipStatusEnum",
    "UserListNumberRuleItemOperatorEnum",
    "UserListPrepopulationStatusEnum",
    "UserListRuleTypeEnum",
    "UserListSizeRangeEnum",
    "UserListStringRuleItemOperatorEnum",
    "UserListTypeEnum",
    "ValueRuleDeviceTypeEnum",
    "ValueRuleGeoLocationMatchTypeEnum",
    "ValueRuleOperationEnum",
    "ValueRuleSetAttachmentTypeEnum",
    "ValueRuleSetDimensionEnum",
    "VanityPharmaDisplayUrlModeEnum",
    "VanityPharmaTextEnum",
    "VideoAdFormatRestrictionEnum",
    "VideoThumbnailEnum",
    "WebpageConditionOperandEnum",
    "WebpageConditionOperatorEnum",
)

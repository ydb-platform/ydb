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
from __future__ import annotations

from typing import MutableSequence

import proto  # type: ignore

from google.ads.googleads.v23.common.types import bidding
from google.ads.googleads.v23.common.types import custom_parameter
from google.ads.googleads.v23.common.types import frequency_cap
from google.ads.googleads.v23.common.types import (
    real_time_bidding_setting as gagc_real_time_bidding_setting,
)
from google.ads.googleads.v23.common.types import (
    targeting_setting as gagc_targeting_setting,
)
from google.ads.googleads.v23.common.types import (
    third_party_integration_partners as gagc_third_party_integration_partners,
)
from google.ads.googleads.v23.enums.types import (
    ad_group_type as gage_ad_group_type,
)
from google.ads.googleads.v23.enums.types import (
    ad_serving_optimization_status as gage_ad_serving_optimization_status,
)
from google.ads.googleads.v23.enums.types import (
    advertising_channel_sub_type as gage_advertising_channel_sub_type,
)
from google.ads.googleads.v23.enums.types import (
    advertising_channel_type as gage_advertising_channel_type,
)
from google.ads.googleads.v23.enums.types import app_campaign_app_store
from google.ads.googleads.v23.enums.types import (
    app_campaign_bidding_strategy_goal_type,
)
from google.ads.googleads.v23.enums.types import (
    asset_automation_status as gage_asset_automation_status,
)
from google.ads.googleads.v23.enums.types import (
    asset_automation_type as gage_asset_automation_type,
)
from google.ads.googleads.v23.enums.types import asset_field_type
from google.ads.googleads.v23.enums.types import asset_set_type
from google.ads.googleads.v23.enums.types import (
    bidding_strategy_system_status as gage_bidding_strategy_system_status,
)
from google.ads.googleads.v23.enums.types import (
    bidding_strategy_type as gage_bidding_strategy_type,
)
from google.ads.googleads.v23.enums.types import booking_status
from google.ads.googleads.v23.enums.types import brand_safety_suitability
from google.ads.googleads.v23.enums.types import campaign_experiment_type
from google.ads.googleads.v23.enums.types import campaign_keyword_match_type
from google.ads.googleads.v23.enums.types import campaign_primary_status
from google.ads.googleads.v23.enums.types import campaign_primary_status_reason
from google.ads.googleads.v23.enums.types import campaign_serving_status
from google.ads.googleads.v23.enums.types import campaign_status
from google.ads.googleads.v23.enums.types import eu_political_advertising_status
from google.ads.googleads.v23.enums.types import (
    listing_type as gage_listing_type,
)
from google.ads.googleads.v23.enums.types import (
    location_source_type as gage_location_source_type,
)
from google.ads.googleads.v23.enums.types import messaging_restriction_type
from google.ads.googleads.v23.enums.types import (
    negative_geo_target_type as gage_negative_geo_target_type,
)
from google.ads.googleads.v23.enums.types import non_skippable_max_duration
from google.ads.googleads.v23.enums.types import non_skippable_min_duration
from google.ads.googleads.v23.enums.types import optimization_goal_type
from google.ads.googleads.v23.enums.types import (
    payment_mode as gage_payment_mode,
)
from google.ads.googleads.v23.enums.types import performance_max_upgrade_status
from google.ads.googleads.v23.enums.types import (
    positive_geo_target_type as gage_positive_geo_target_type,
)
from google.ads.googleads.v23.enums.types import (
    vanity_pharma_display_url_mode as gage_vanity_pharma_display_url_mode,
)
from google.ads.googleads.v23.enums.types import (
    vanity_pharma_text as gage_vanity_pharma_text,
)
from google.ads.googleads.v23.enums.types import video_ad_format_restriction
from google.ads.googleads.v23.enums.types import (
    video_ad_sequence_interaction_type,
)
from google.ads.googleads.v23.enums.types import (
    video_ad_sequence_minimum_duration,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.resources",
    marshal="google.ads.googleads.v23",
    manifest={
        "Campaign",
    },
)


class Campaign(proto.Message):
    r"""A campaign.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the campaign. Campaign
            resource names have the form:

            ``customers/{customer_id}/campaigns/{campaign_id}``
        id (int):
            Output only. The ID of the campaign.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the campaign.

            This field is required and should not be empty
            when creating new campaigns.

            It must not contain any null (code point 0x0),
            NL line feed (code point 0xA) or carriage return
            (code point 0xD) characters.

            This field is a member of `oneof`_ ``_name``.
        primary_status (google.ads.googleads.v23.enums.types.CampaignPrimaryStatusEnum.CampaignPrimaryStatus):
            Output only. The primary status of the
            campaign.
            Provides insight into why a campaign is not
            serving or not serving optimally. Modification
            to the campaign and its related entities might
            take a while to be reflected in this status.
        primary_status_reasons (MutableSequence[google.ads.googleads.v23.enums.types.CampaignPrimaryStatusReasonEnum.CampaignPrimaryStatusReason]):
            Output only. The primary status reasons of
            the campaign.
            Provides insight into why a campaign is not
            serving or not serving optimally. These reasons
            are aggregated to determine an overall
            CampaignPrimaryStatus.
        status (google.ads.googleads.v23.enums.types.CampaignStatusEnum.CampaignStatus):
            The status of the campaign.

            When a new campaign is added, the status
            defaults to ENABLED.
        serving_status (google.ads.googleads.v23.enums.types.CampaignServingStatusEnum.CampaignServingStatus):
            Output only. The ad serving status of the
            campaign.
        bidding_strategy_system_status (google.ads.googleads.v23.enums.types.BiddingStrategySystemStatusEnum.BiddingStrategySystemStatus):
            Output only. The system status of the
            campaign's bidding strategy.
        ad_serving_optimization_status (google.ads.googleads.v23.enums.types.AdServingOptimizationStatusEnum.AdServingOptimizationStatus):
            The ad serving optimization status of the
            campaign.
        advertising_channel_type (google.ads.googleads.v23.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType):
            Immutable. The primary serving target for ads within the
            campaign. The targeting options can be refined in
            ``network_settings``.

            This field is required and should not be empty when creating
            new campaigns.

            Can be set only when creating campaigns. After the campaign
            is created, the field can not be changed.
        advertising_channel_sub_type (google.ads.googleads.v23.enums.types.AdvertisingChannelSubTypeEnum.AdvertisingChannelSubType):
            Immutable. Optional refinement to
            ``advertising_channel_type``. Must be a valid sub-type of
            the parent channel type.

            Can be set only when creating campaigns. After campaign is
            created, the field can not be changed.
        tracking_url_template (str):
            The URL template for constructing a tracking
            URL.

            This field is a member of `oneof`_ ``_tracking_url_template``.
        url_custom_parameters (MutableSequence[google.ads.googleads.v23.common.types.CustomParameter]):
            The list of mappings used to substitute custom parameter
            tags in a ``tracking_url_template``, ``final_urls``, or
            ``mobile_final_urls``.
        local_services_campaign_settings (google.ads.googleads.v23.resources.types.Campaign.LocalServicesCampaignSettings):
            The Local Services Campaign related settings.
        travel_campaign_settings (google.ads.googleads.v23.resources.types.Campaign.TravelCampaignSettings):
            Settings for Travel campaign.
        demand_gen_campaign_settings (google.ads.googleads.v23.resources.types.Campaign.DemandGenCampaignSettings):
            Settings for Demand Gen campaign.
        video_campaign_settings (google.ads.googleads.v23.resources.types.Campaign.VideoCampaignSettings):
            Settings for Video campaign.
        pmax_campaign_settings (google.ads.googleads.v23.resources.types.Campaign.PmaxCampaignSettings):
            Settings for Performance Max campaign.
        real_time_bidding_setting (google.ads.googleads.v23.common.types.RealTimeBiddingSetting):
            Settings for Real-Time Bidding, a feature
            only available for campaigns targeting the Ad
            Exchange network.
        network_settings (google.ads.googleads.v23.resources.types.Campaign.NetworkSettings):
            The network settings for the campaign.
        hotel_setting (google.ads.googleads.v23.resources.types.Campaign.HotelSettingInfo):
            Immutable. The hotel setting for the
            campaign.
        dynamic_search_ads_setting (google.ads.googleads.v23.resources.types.Campaign.DynamicSearchAdsSetting):
            The setting for controlling Dynamic Search
            Ads (DSA).
        shopping_setting (google.ads.googleads.v23.resources.types.Campaign.ShoppingSetting):
            The setting for controlling Shopping
            campaigns.
        targeting_setting (google.ads.googleads.v23.common.types.TargetingSetting):
            Setting for targeting related features.
        audience_setting (google.ads.googleads.v23.resources.types.Campaign.AudienceSetting):
            Immutable. Setting for audience related
            features.

            This field is a member of `oneof`_ ``_audience_setting``.
        geo_target_type_setting (google.ads.googleads.v23.resources.types.Campaign.GeoTargetTypeSetting):
            The setting for ads geotargeting.
        local_campaign_setting (google.ads.googleads.v23.resources.types.Campaign.LocalCampaignSetting):
            The setting for local campaign.
        app_campaign_setting (google.ads.googleads.v23.resources.types.Campaign.AppCampaignSetting):
            The setting related to App Campaign.
        labels (MutableSequence[str]):
            Output only. The resource names of labels
            attached to this campaign.
        experiment_type (google.ads.googleads.v23.enums.types.CampaignExperimentTypeEnum.CampaignExperimentType):
            Output only. The type of campaign: normal,
            draft, or experiment.
        base_campaign (str):
            Output only. The resource name of the base campaign of a
            draft or experiment campaign. For base campaigns, this is
            equal to ``resource_name``.

            This field is read-only.

            This field is a member of `oneof`_ ``_base_campaign``.
        campaign_budget (str):
            The resource name of the campaign budget of
            the campaign.

            This field is a member of `oneof`_ ``_campaign_budget``.
        bidding_strategy_type (google.ads.googleads.v23.enums.types.BiddingStrategyTypeEnum.BiddingStrategyType):
            Output only. The type of bidding strategy.

            A bidding strategy can be created by setting either the
            bidding scheme to create a standard bidding strategy or the
            ``bidding_strategy`` field to create a portfolio bidding
            strategy.

            This field is read-only.
        accessible_bidding_strategy (str):
            Output only. Resource name of AccessibleBiddingStrategy, a
            read-only view of the unrestricted attributes of the
            attached portfolio bidding strategy identified by
            'bidding_strategy'. Empty, if the campaign does not use a
            portfolio strategy. Unrestricted strategy attributes are
            available to all customers with whom the strategy is shared
            and are read from the AccessibleBiddingStrategy resource. In
            contrast, restricted attributes are only available to the
            owner customer of the strategy and their managers.
            Restricted attributes can only be read from the
            BiddingStrategy resource.
        campaign_group (str):
            The resource name of the campaign group that
            this campaign belongs to.

            This field is a member of `oneof`_ ``_campaign_group``.
        start_date_time (str):
            The date and time when campaign started in
            serving. The timestamp is in the customer's time
            zone and in "yyyy-MM-dd HH:mm:ss" format. Set
            the time component to 00:00:00 for daily
            granularity, time granularity is only supported
            for some campaign types.

            This field is a member of `oneof`_ ``_start_date_time``.
        end_date_time (str):
            The last day and time of the campaign in
            serving customer's timezone in "yyyy-MM-dd
            HH:mm:ss" format. Set the time component to
            23:59:59 for daily granularity, time granularity
            is only supported for some campaign types. On
            create, defaults to running indefinitely. To set
            an existing campaign to run indefinitely, clear
            this field.

            This field is a member of `oneof`_ ``_end_date_time``.
        final_url_suffix (str):
            Suffix used to append query parameters to
            landing pages that are served with parallel
            tracking.

            This field is a member of `oneof`_ ``_final_url_suffix``.
        frequency_caps (MutableSequence[google.ads.googleads.v23.common.types.FrequencyCapEntry]):
            A list that limits how often each user will
            see this campaign's ads.
        video_brand_safety_suitability (google.ads.googleads.v23.enums.types.BrandSafetySuitabilityEnum.BrandSafetySuitability):
            Brand Safety setting at the individual
            campaign level. Allows for selecting an
            inventory type to show your ads on content that
            is the right fit for your brand. See
            https://support.google.com/google-ads/answer/7515513.
        vanity_pharma (google.ads.googleads.v23.resources.types.Campaign.VanityPharma):
            Describes how unbranded pharma ads will be
            displayed.
        selective_optimization (google.ads.googleads.v23.resources.types.Campaign.SelectiveOptimization):
            Selective optimization setting for this campaign, which
            includes a set of conversion actions to optimize this
            campaign towards. This feature only applies to app campaigns
            that use MULTI_CHANNEL as AdvertisingChannelType and
            APP_CAMPAIGN or APP_CAMPAIGN_FOR_ENGAGEMENT as
            AdvertisingChannelSubType.
        optimization_goal_setting (google.ads.googleads.v23.resources.types.Campaign.OptimizationGoalSetting):
            Optimization goal setting for this campaign,
            which includes a set of optimization goal types.
        tracking_setting (google.ads.googleads.v23.resources.types.Campaign.TrackingSetting):
            Output only. Campaign-level settings for
            tracking information.
        payment_mode (google.ads.googleads.v23.enums.types.PaymentModeEnum.PaymentMode):
            Payment mode for the campaign.
        optimization_score (float):
            Output only. Optimization score of the
            campaign.
            Optimization score is an estimate of how well a
            campaign is set to perform. It ranges from 0%
            (0.0) to 100% (1.0), with 100% indicating that
            the campaign is performing at full potential.
            This field is null for unscored campaigns.

            See "About optimization score" at
            https://support.google.com/google-ads/answer/9061546.

            This field is read-only.

            This field is a member of `oneof`_ ``_optimization_score``.
        excluded_parent_asset_field_types (MutableSequence[google.ads.googleads.v23.enums.types.AssetFieldTypeEnum.AssetFieldType]):
            The asset field types that should be excluded
            from this campaign. Asset links with these field
            types will not be inherited by this campaign
            from the upper level.
        excluded_parent_asset_set_types (MutableSequence[google.ads.googleads.v23.enums.types.AssetSetTypeEnum.AssetSetType]):
            The asset set types that should be excluded from this
            campaign. Asset set links with these types will not be
            inherited by this campaign from the upper level. Location
            group types (GMB_DYNAMIC_LOCATION_GROUP,
            CHAIN_DYNAMIC_LOCATION_GROUP, and STATIC_LOCATION_GROUP) are
            child types of LOCATION_SYNC. Therefore, if LOCATION_SYNC is
            set for this field, all location group asset sets are not
            allowed to be linked to this campaign, and all Location
            Extension (LE) and Affiliate Location Extensions (ALE) will
            not be served under this campaign. Only LOCATION_SYNC is
            currently supported.
        performance_max_upgrade (google.ads.googleads.v23.resources.types.Campaign.PerformanceMaxUpgrade):
            Output only. Information about campaigns
            being upgraded to Performance Max.
        hotel_property_asset_set (str):
            Immutable. The resource name for a set of
            hotel properties for Performance Max for travel
            goals campaigns.

            This field is a member of `oneof`_ ``_hotel_property_asset_set``.
        listing_type (google.ads.googleads.v23.enums.types.ListingTypeEnum.ListingType):
            Immutable. Listing type of ads served for
            this campaign. Field is restricted for usage
            with Performance Max campaigns.

            This field is a member of `oneof`_ ``_listing_type``.
        asset_automation_settings (MutableSequence[google.ads.googleads.v23.resources.types.Campaign.AssetAutomationSetting]):
            Contains the opt-in/out status of each
            AssetAutomationType. See documentation of each
            asset automation type enum for default opt
            in/out behavior.
        keyword_match_type (google.ads.googleads.v23.enums.types.CampaignKeywordMatchTypeEnum.CampaignKeywordMatchType):
            Keyword match type of Campaign. Set to BROAD
            to set broad matching for all keywords in a
            campaign.
        brand_guidelines_enabled (bool):
            Immutable. Whether Brand Guidelines are
            enabled for this Campaign. Only applicable to
            Performance Max campaigns. If enabled, business
            name and logo assets must be linked as
            CampaignAssets instead of AssetGroupAssets.

            Writable only at campaign creation. Set to true
            to enable Brand Guidelines when creating a new
            Performance Max campaign.

            Immutable after creation. This field cannot be
            modified using standard update operations after
            the campaign has been created.

            For existing campaigns: To enable Brand
            Guidelines on a campaign after it has been
            created, use the
            CampaignService.EnablePMaxBrandGuidelines
            method, which is a separate operation. It is not
            possible to disable Brand Guidelines for an
            existing campaign.

            Incompatible with Travel Goals: This feature is
            not supported for Performance Max campaigns with
            Travel Goals. Attempting to set this field to
            true for a Travel Goals campaign will result in
            an error.

            This field is a member of `oneof`_ ``_brand_guidelines_enabled``.
        brand_guidelines (google.ads.googleads.v23.resources.types.Campaign.BrandGuidelines):
            These settings control how your brand appears
            in automatically generated assets and formats
            within this campaign. Note: These settings can
            only be used for Performance Max campaigns that
            have Brand Guidelines enabled.
        text_guidelines (google.ads.googleads.v23.resources.types.Campaign.TextGuidelines):
            Settings to control automatically generated
            text assets. Only available in Performance Max
            and Search campaigns (Brand Guidelines does not
            need to be enabled).
        third_party_integration_partners (google.ads.googleads.v23.common.types.CampaignThirdPartyIntegrationPartners):
            Third-Party integration partners.
        ai_max_setting (google.ads.googleads.v23.resources.types.Campaign.AiMaxSetting):
            Settings for AI Max in search campaigns.
        contains_eu_political_advertising (google.ads.googleads.v23.enums.types.EuPoliticalAdvertisingStatusEnum.EuPoliticalAdvertisingStatus):
            The advertiser should self-declare whether
            this campaign contains political advertising
            content targeted towards the European Union.
        feed_types (MutableSequence[google.ads.googleads.v23.enums.types.AssetSetTypeEnum.AssetSetType]):
            Output only. Types of feeds that are attached
            directly to this campaign.
        missing_eu_political_advertising_declaration (bool):
            Output only. Indicates whether this campaign is missing a
            declaration about whether it contains political advertising
            targeted towards the EU and is ineligible for any
            exemptions. If this field is true, use the
            contains_eu_political_advertising field to add the required
            declaration.

            This field is read-only.
        bidding_strategy (str):
            The resource name of the portfolio bidding
            strategy used by the campaign.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        commission (google.ads.googleads.v23.common.types.Commission):
            Commission is an automatic bidding strategy
            in which the advertiser pays a certain portion
            of the conversion value.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        manual_cpa (google.ads.googleads.v23.common.types.ManualCpa):
            Standard Manual CPA bidding strategy.
            Manual bidding strategy that allows advertiser
            to set the bid per advertiser-specified action.
            Supported only for Local Services campaigns.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        manual_cpc (google.ads.googleads.v23.common.types.ManualCpc):
            Standard Manual CPC bidding strategy.
            Manual click-based bidding where user pays per
            click.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        manual_cpm (google.ads.googleads.v23.common.types.ManualCpm):
            Standard Manual CPM bidding strategy.
            Manual impression-based bidding where user pays
            per thousand impressions.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        manual_cpv (google.ads.googleads.v23.common.types.ManualCpv):
            A bidding strategy that pays a configurable
            amount per video view.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        maximize_conversions (google.ads.googleads.v23.common.types.MaximizeConversions):
            Standard Maximize Conversions bidding
            strategy that automatically maximizes number of
            conversions while spending your budget.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        maximize_conversion_value (google.ads.googleads.v23.common.types.MaximizeConversionValue):
            Standard Maximize Conversion Value bidding
            strategy that automatically sets bids to
            maximize revenue while spending your budget.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_cpa (google.ads.googleads.v23.common.types.TargetCpa):
            Standard Target CPA bidding strategy that
            automatically sets bids to help get as many
            conversions as possible at the target
            cost-per-acquisition (CPA) you set.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_impression_share (google.ads.googleads.v23.common.types.TargetImpressionShare):
            Target Impression Share bidding strategy. An
            automated bidding strategy that sets bids to
            achieve a chosen percentage of impressions.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_roas (google.ads.googleads.v23.common.types.TargetRoas):
            Standard Target ROAS bidding strategy that
            automatically maximizes revenue while averaging
            a specific target return on ad spend (ROAS).

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_spend (google.ads.googleads.v23.common.types.TargetSpend):
            Standard Target Spend bidding strategy that
            automatically sets your bids to help get as many
            clicks as possible within your budget.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        percent_cpc (google.ads.googleads.v23.common.types.PercentCpc):
            Standard Percent Cpc bidding strategy where
            bids are a fraction of the advertised price for
            some good or service.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_cpm (google.ads.googleads.v23.common.types.TargetCpm):
            A bidding strategy that automatically
            optimizes cost per thousand impressions.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        fixed_cpm (google.ads.googleads.v23.common.types.FixedCpm):
            A manual bidding strategy with a fixed CPM.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_cpv (google.ads.googleads.v23.common.types.TargetCpv):
            An automated bidding strategy that sets bids
            to optimize performance given the target CPV you
            set.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_cpc (google.ads.googleads.v23.common.types.TargetCpc):
            An automated bidding strategy that sets bids
            to help get as many clicks as possible at the
            target cost-per-click (CPC) you set.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
    """

    class PerformanceMaxUpgrade(proto.Message):
        r"""Information about a campaign being upgraded to Performance
        Max.

        Attributes:
            performance_max_campaign (str):
                Output only. The resource name of the
                Performance Max campaign the campaign is
                upgraded to.
            pre_upgrade_campaign (str):
                Output only. The resource name of the legacy
                campaign upgraded to Performance Max.
            status (google.ads.googleads.v23.enums.types.PerformanceMaxUpgradeStatusEnum.PerformanceMaxUpgradeStatus):
                Output only. The upgrade status of a campaign
                requested to be upgraded to Performance Max.
        """

        performance_max_campaign: str = proto.Field(
            proto.STRING,
            number=1,
        )
        pre_upgrade_campaign: str = proto.Field(
            proto.STRING,
            number=2,
        )
        status: (
            performance_max_upgrade_status.PerformanceMaxUpgradeStatusEnum.PerformanceMaxUpgradeStatus
        ) = proto.Field(
            proto.ENUM,
            number=3,
            enum=performance_max_upgrade_status.PerformanceMaxUpgradeStatusEnum.PerformanceMaxUpgradeStatus,
        )

    class NetworkSettings(proto.Message):
        r"""The network settings for the campaign.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            target_google_search (bool):
                Whether ads will be served with google.com
                search results.

                This field is a member of `oneof`_ ``_target_google_search``.
            target_search_network (bool):
                Whether ads will be served on partner sites in the Google
                Search Network (requires ``target_google_search`` to also be
                ``true``).

                This field is a member of `oneof`_ ``_target_search_network``.
            target_content_network (bool):
                Whether ads will be served on specified
                placements in the Google Display Network.
                Placements are specified using the Placement
                criterion.

                This field is a member of `oneof`_ ``_target_content_network``.
            target_partner_search_network (bool):
                Whether ads will be served on the Google
                Partner Network. This is available only to some
                select Google partner accounts.

                This field is a member of `oneof`_ ``_target_partner_search_network``.
            target_youtube (bool):
                Whether ads will be served on YouTube.

                This field is a member of `oneof`_ ``_target_youtube``.
            target_google_tv_network (bool):
                Whether ads will be served on the Google TV
                network.

                This field is a member of `oneof`_ ``_target_google_tv_network``.
        """

        target_google_search: bool = proto.Field(
            proto.BOOL,
            number=5,
            optional=True,
        )
        target_search_network: bool = proto.Field(
            proto.BOOL,
            number=6,
            optional=True,
        )
        target_content_network: bool = proto.Field(
            proto.BOOL,
            number=7,
            optional=True,
        )
        target_partner_search_network: bool = proto.Field(
            proto.BOOL,
            number=8,
            optional=True,
        )
        target_youtube: bool = proto.Field(
            proto.BOOL,
            number=9,
            optional=True,
        )
        target_google_tv_network: bool = proto.Field(
            proto.BOOL,
            number=10,
            optional=True,
        )

    class HotelSettingInfo(proto.Message):
        r"""Campaign-level settings for hotel ads.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            hotel_center_id (int):
                Immutable. The linked Hotel Center account.

                This field is a member of `oneof`_ ``_hotel_center_id``.
        """

        hotel_center_id: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class DynamicSearchAdsSetting(proto.Message):
        r"""The setting for controlling Dynamic Search Ads (DSA).

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            domain_name (str):
                Required. The Internet domain name that this
                setting represents, for example, "google.com" or
                "www.google.com".
            language_code (str):
                Required. The language code specifying the
                language of the domain, for example, "en".
            use_supplied_urls_only (bool):
                Whether the campaign uses advertiser supplied
                URLs exclusively.

                This field is a member of `oneof`_ ``_use_supplied_urls_only``.
        """

        domain_name: str = proto.Field(
            proto.STRING,
            number=6,
        )
        language_code: str = proto.Field(
            proto.STRING,
            number=7,
        )
        use_supplied_urls_only: bool = proto.Field(
            proto.BOOL,
            number=8,
            optional=True,
        )

    class ShoppingSetting(proto.Message):
        r"""The setting for Shopping campaigns. Defines the universe of
        products that can be advertised by the campaign, and how this
        campaign interacts with other Shopping campaigns.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            merchant_id (int):
                ID of the Merchant Center account.
                This field is required for create operations.
                This field is immutable for Shopping campaigns.

                This field is a member of `oneof`_ ``_merchant_id``.
            feed_label (str):
                Feed label of products to include in the campaign. Valid
                feed labels may contain a maximum of 20 characters including
                uppercase letters, numbers, hyphens, and underscores. If you
                previously used the deprecated ``sales_country`` in the
                two-letter country code (``XX``) format, the ``feed_label``
                field should be used instead. For more information see the
                `feed
                label <//support.google.com/merchants/answer/12453549>`__
                support article.
            campaign_priority (int):
                Priority of the campaign. Campaigns with
                numerically higher priorities take precedence
                over those with lower priorities. This field is
                required for Shopping campaigns, with values
                between 0 and 2, inclusive.
                This field is optional for Smart Shopping
                campaigns, but must be equal to 3 if set.

                This field is a member of `oneof`_ ``_campaign_priority``.
            enable_local (bool):
                Whether to include local products.

                This field is a member of `oneof`_ ``_enable_local``.
            use_vehicle_inventory (bool):
                Immutable. Whether to target Vehicle Listing inventory. This
                field is supported only in Smart Shopping Campaigns. For
                setting Vehicle Listing inventory in Performance Max
                campaigns, use ``listing_type`` instead.
            advertising_partner_ids (MutableSequence[int]):
                The list of Google Ads accounts IDs of
                advertising partners cooperating within the
                campaign. This feature is currently available
                only for accounts having an advertising partner
                link. Once set, the field is immutable. This
                feature is currently supported only for
                Performance Max, Shopping, Search and Demand Gen
                campaign types.
            disable_product_feed (bool):
                Disable the optional product feed. This field
                is currently supported only for Demand Gen
                campaigns. See
                https://support.google.com/google-ads/answer/13721750
                to learn more about this feature.

                This field is a member of `oneof`_ ``_disable_product_feed``.
        """

        merchant_id: int = proto.Field(
            proto.INT64,
            number=5,
            optional=True,
        )
        feed_label: str = proto.Field(
            proto.STRING,
            number=10,
        )
        campaign_priority: int = proto.Field(
            proto.INT32,
            number=7,
            optional=True,
        )
        enable_local: bool = proto.Field(
            proto.BOOL,
            number=8,
            optional=True,
        )
        use_vehicle_inventory: bool = proto.Field(
            proto.BOOL,
            number=9,
        )
        advertising_partner_ids: MutableSequence[int] = proto.RepeatedField(
            proto.INT64,
            number=11,
        )
        disable_product_feed: bool = proto.Field(
            proto.BOOL,
            number=12,
            optional=True,
        )

    class TrackingSetting(proto.Message):
        r"""Campaign-level settings for tracking information.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            tracking_url (str):
                Output only. The url used for dynamic
                tracking.

                This field is a member of `oneof`_ ``_tracking_url``.
        """

        tracking_url: str = proto.Field(
            proto.STRING,
            number=2,
            optional=True,
        )

    class GeoTargetTypeSetting(proto.Message):
        r"""Represents a collection of settings related to ads
        geotargeting.

        Attributes:
            positive_geo_target_type (google.ads.googleads.v23.enums.types.PositiveGeoTargetTypeEnum.PositiveGeoTargetType):
                The setting used for positive geotargeting in
                this particular campaign.
            negative_geo_target_type (google.ads.googleads.v23.enums.types.NegativeGeoTargetTypeEnum.NegativeGeoTargetType):
                The setting used for negative geotargeting in
                this particular campaign.
        """

        positive_geo_target_type: (
            gage_positive_geo_target_type.PositiveGeoTargetTypeEnum.PositiveGeoTargetType
        ) = proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_positive_geo_target_type.PositiveGeoTargetTypeEnum.PositiveGeoTargetType,
        )
        negative_geo_target_type: (
            gage_negative_geo_target_type.NegativeGeoTargetTypeEnum.NegativeGeoTargetType
        ) = proto.Field(
            proto.ENUM,
            number=2,
            enum=gage_negative_geo_target_type.NegativeGeoTargetTypeEnum.NegativeGeoTargetType,
        )

    class LocalCampaignSetting(proto.Message):
        r"""Campaign setting for local campaigns.

        Attributes:
            location_source_type (google.ads.googleads.v23.enums.types.LocationSourceTypeEnum.LocationSourceType):
                The location source type for this local
                campaign.
        """

        location_source_type: (
            gage_location_source_type.LocationSourceTypeEnum.LocationSourceType
        ) = proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_location_source_type.LocationSourceTypeEnum.LocationSourceType,
        )

    class AppCampaignSetting(proto.Message):
        r"""Campaign-level settings for App Campaigns.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            bidding_strategy_goal_type (google.ads.googleads.v23.enums.types.AppCampaignBiddingStrategyGoalTypeEnum.AppCampaignBiddingStrategyGoalType):
                Represents the goal which the bidding
                strategy of this app campaign should optimize
                towards.
            app_id (str):
                Immutable. A string that uniquely identifies
                a mobile application.

                This field is a member of `oneof`_ ``_app_id``.
            app_store (google.ads.googleads.v23.enums.types.AppCampaignAppStoreEnum.AppCampaignAppStore):
                Immutable. The application store that
                distributes this specific app.
        """

        bidding_strategy_goal_type: (
            app_campaign_bidding_strategy_goal_type.AppCampaignBiddingStrategyGoalTypeEnum.AppCampaignBiddingStrategyGoalType
        ) = proto.Field(
            proto.ENUM,
            number=1,
            enum=app_campaign_bidding_strategy_goal_type.AppCampaignBiddingStrategyGoalTypeEnum.AppCampaignBiddingStrategyGoalType,
        )
        app_id: str = proto.Field(
            proto.STRING,
            number=4,
            optional=True,
        )
        app_store: (
            app_campaign_app_store.AppCampaignAppStoreEnum.AppCampaignAppStore
        ) = proto.Field(
            proto.ENUM,
            number=3,
            enum=app_campaign_app_store.AppCampaignAppStoreEnum.AppCampaignAppStore,
        )

    class VanityPharma(proto.Message):
        r"""Describes how unbranded pharma ads will be displayed.

        Attributes:
            vanity_pharma_display_url_mode (google.ads.googleads.v23.enums.types.VanityPharmaDisplayUrlModeEnum.VanityPharmaDisplayUrlMode):
                The display mode for vanity pharma URLs.
            vanity_pharma_text (google.ads.googleads.v23.enums.types.VanityPharmaTextEnum.VanityPharmaText):
                The text that will be displayed in display
                URL of the text ad when website description is
                the selected display mode for vanity pharma
                URLs.
        """

        vanity_pharma_display_url_mode: (
            gage_vanity_pharma_display_url_mode.VanityPharmaDisplayUrlModeEnum.VanityPharmaDisplayUrlMode
        ) = proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_vanity_pharma_display_url_mode.VanityPharmaDisplayUrlModeEnum.VanityPharmaDisplayUrlMode,
        )
        vanity_pharma_text: (
            gage_vanity_pharma_text.VanityPharmaTextEnum.VanityPharmaText
        ) = proto.Field(
            proto.ENUM,
            number=2,
            enum=gage_vanity_pharma_text.VanityPharmaTextEnum.VanityPharmaText,
        )

    class SelectiveOptimization(proto.Message):
        r"""Selective optimization setting for this campaign, which includes a
        set of conversion actions to optimize this campaign towards. This
        feature only applies to app campaigns that use MULTI_CHANNEL as
        AdvertisingChannelType and APP_CAMPAIGN or
        APP_CAMPAIGN_FOR_ENGAGEMENT as AdvertisingChannelSubType.

        Attributes:
            conversion_actions (MutableSequence[str]):
                The selected set of resource names for
                conversion actions for optimizing this campaign.
        """

        conversion_actions: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=2,
        )

    class OptimizationGoalSetting(proto.Message):
        r"""Optimization goal setting for this campaign, which includes a
        set of optimization goal types.

        Attributes:
            optimization_goal_types (MutableSequence[google.ads.googleads.v23.enums.types.OptimizationGoalTypeEnum.OptimizationGoalType]):
                The list of optimization goal types.
        """

        optimization_goal_types: MutableSequence[
            optimization_goal_type.OptimizationGoalTypeEnum.OptimizationGoalType
        ] = proto.RepeatedField(
            proto.ENUM,
            number=1,
            enum=optimization_goal_type.OptimizationGoalTypeEnum.OptimizationGoalType,
        )

    class AudienceSetting(proto.Message):
        r"""Settings for the audience targeting.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            use_audience_grouped (bool):
                Immutable. If true, this campaign uses an
                Audience resource for audience targeting. If
                false, this campaign may use audience segment
                criteria instead.

                This field is a member of `oneof`_ ``_use_audience_grouped``.
        """

        use_audience_grouped: bool = proto.Field(
            proto.BOOL,
            number=1,
            optional=True,
        )

    class LocalServicesCampaignSettings(proto.Message):
        r"""Settings for LocalServicesCampaign subresource.

        Attributes:
            category_bids (MutableSequence[google.ads.googleads.v23.resources.types.Campaign.CategoryBid]):
                Categorical level bids associated with MANUAL_CPA bidding
                strategy.
        """

        category_bids: MutableSequence["Campaign.CategoryBid"] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=1,
                message="Campaign.CategoryBid",
            )
        )

    class CategoryBid(proto.Message):
        r"""Category bids in LocalServicesReportingCampaignSettings.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            category_id (str):
                Category for which the bid will be associated with. For
                example, xcat:service_area_business_plumber.

                This field is a member of `oneof`_ ``_category_id``.
            manual_cpa_bid_micros (int):
                Manual CPA bid for the category. Bid must be
                greater than the reserve price associated for
                that category. Value is in micros and in the
                advertiser's currency.

                This field is a member of `oneof`_ ``_manual_cpa_bid_micros``.
            target_cpa_bid_micros (int):
                Target CPA bid for the category. Value is in
                micros and in the advertiser's currency.

                This field is a member of `oneof`_ ``_target_cpa_bid_micros``.
        """

        category_id: str = proto.Field(
            proto.STRING,
            number=1,
            optional=True,
        )
        manual_cpa_bid_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )
        target_cpa_bid_micros: int = proto.Field(
            proto.INT64,
            number=3,
            optional=True,
        )

    class TravelCampaignSettings(proto.Message):
        r"""Settings for Travel campaign.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            travel_account_id (int):
                Immutable. The Travel account ID associated
                with the Travel campaign.

                This field is a member of `oneof`_ ``_travel_account_id``.
        """

        travel_account_id: int = proto.Field(
            proto.INT64,
            number=1,
            optional=True,
        )

    class DemandGenCampaignSettings(proto.Message):
        r"""Settings for Demand Gen campaign.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            upgraded_targeting (bool):
                Immutable. Specifies whether this campaign uses upgraded
                targeting options. When this field is set to ``true``, you
                can use location and language targeting at the ad group
                level as opposed to the standard campaign-level targeting.
                This field defaults to ``true``, and can only be set when
                creating a campaign.

                This field is a member of `oneof`_ ``_upgraded_targeting``.
        """

        upgraded_targeting: bool = proto.Field(
            proto.BOOL,
            number=1,
            optional=True,
        )

    class VideoCampaignSettings(proto.Message):
        r"""Settings for Video campaign.

        This message has `oneof`_ fields (mutually exclusive fields).
        For each oneof, at most one member field can be set at the same time.
        Setting any member of the oneof automatically clears all other
        members.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            video_ad_sequence (google.ads.googleads.v23.resources.types.Campaign.VideoCampaignSettings.VideoAdSequence):
                Container for video ads sequencing
                definition.
            reservation_ad_category_self_disclosure (google.ads.googleads.v23.resources.types.Campaign.VideoCampaignSettings.ReservationAdCategorySelfDisclosure):
                Ad category self-disclosure for campaigns with the FIXED_CPM
                or FIXED_SHARE_OF_VOICE bidding strategies.
            booking_details (google.ads.googleads.v23.resources.types.Campaign.VideoCampaignSettings.BookingDetails):
                Output only. Booking information for campaigns with the
                FIXED_CPM or FIXED_SHARE_OF_VOICE bidding strategies.
            video_ad_inventory_control (google.ads.googleads.v23.resources.types.Campaign.VideoCampaignSettings.VideoAdInventoryControl):
                Inventory control for video responsive ads in
                reach campaigns.

                This field is a member of `oneof`_ ``fluidity_control``.
            video_ad_format_control (google.ads.googleads.v23.resources.types.Campaign.VideoCampaignSettings.VideoAdFormatControl):
                Format-restricting control enabling usage of
                video responsive ads in format defined Video
                campaigns (for example, non-skippable).

                This field is a member of `oneof`_ ``fluidity_control``.
        """

        class VideoAdInventoryControl(proto.Message):
            r"""For campaigns using video responsive ads inventory controls
            determine on which inventories the ads can be shown. This only
            applies for campaigns with the bidding strategies TARGET_CPM and
            FIXED_CPM.


            .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

            Attributes:
                allow_in_stream (bool):
                    Determine if video responsive ads can be used
                    for in-stream video ads.

                    This field is a member of `oneof`_ ``_allow_in_stream``.
                allow_in_feed (bool):
                    Determine if video responsive ads can be used
                    for in-feed video ads.

                    This field is a member of `oneof`_ ``_allow_in_feed``.
                allow_shorts (bool):
                    Determine if video responsive ads can be used
                    as shorts format.

                    This field is a member of `oneof`_ ``_allow_shorts``.
                allow_non_skippable_in_stream (bool):
                    Determine if video responsive ads can be used
                    for non-skippable in-stream ads. This is only
                    available for campaigns that allow mixing of
                    non-skippable with other formats (Video reach
                    campaign with Target Frequency bidding strategy
                    goal).

                    This field is a member of `oneof`_ ``_allow_non_skippable_in_stream``.
            """

            allow_in_stream: bool = proto.Field(
                proto.BOOL,
                number=1,
                optional=True,
            )
            allow_in_feed: bool = proto.Field(
                proto.BOOL,
                number=2,
                optional=True,
            )
            allow_shorts: bool = proto.Field(
                proto.BOOL,
                number=3,
                optional=True,
            )
            allow_non_skippable_in_stream: bool = proto.Field(
                proto.BOOL,
                number=4,
                optional=True,
            )

        class VideoAdFormatControl(proto.Message):
            r"""Format-restricting control enabling usage of video responsive
            ads in format defined Video campaigns (for example,
            non-skippable).

            Attributes:
                format_restriction (google.ads.googleads.v23.enums.types.VideoAdFormatRestrictionEnum.VideoAdFormatRestriction):
                    All contained responsive ads are expected to
                    respect this restriction.
                non_skippable_in_stream_restrictions (google.ads.googleads.v23.resources.types.Campaign.VideoCampaignSettings.NonSkippableInStreamRestrictions):
                    Restrictions for non-skippable format.
            """

            format_restriction: (
                video_ad_format_restriction.VideoAdFormatRestrictionEnum.VideoAdFormatRestriction
            ) = proto.Field(
                proto.ENUM,
                number=1,
                enum=video_ad_format_restriction.VideoAdFormatRestrictionEnum.VideoAdFormatRestriction,
            )
            non_skippable_in_stream_restrictions: "Campaign.VideoCampaignSettings.NonSkippableInStreamRestrictions" = proto.Field(
                proto.MESSAGE,
                number=2,
                message="Campaign.VideoCampaignSettings.NonSkippableInStreamRestrictions",
            )

        class NonSkippableInStreamRestrictions(proto.Message):
            r"""Restrictions for non-skippable format.

            Attributes:
                min_duration (google.ads.googleads.v23.enums.types.NonSkippableMinDurationEnum.NonSkippableMinDuration):
                    The minimum allowed duration for
                    non-skippable ads.
                max_duration (google.ads.googleads.v23.enums.types.NonSkippableMaxDurationEnum.NonSkippableMaxDuration):
                    The maximum allowed duration for
                    non-skippable ads.
            """

            min_duration: (
                non_skippable_min_duration.NonSkippableMinDurationEnum.NonSkippableMinDuration
            ) = proto.Field(
                proto.ENUM,
                number=1,
                enum=non_skippable_min_duration.NonSkippableMinDurationEnum.NonSkippableMinDuration,
            )
            max_duration: (
                non_skippable_max_duration.NonSkippableMaxDurationEnum.NonSkippableMaxDuration
            ) = proto.Field(
                proto.ENUM,
                number=2,
                enum=non_skippable_max_duration.NonSkippableMaxDurationEnum.NonSkippableMaxDuration,
            )

        class VideoAdSequence(proto.Message):
            r"""Container for video ads sequencing definition.

            Attributes:
                steps (MutableSequence[google.ads.googleads.v23.resources.types.Campaign.VideoCampaignSettings.VideoAdSequenceStep]):
                    The list of sequence steps and data
                    associated with them.
                minimum_duration (google.ads.googleads.v23.enums.types.VideoAdSequenceMinimumDurationEnum.VideoAdSequenceMinimumDuration):
                    Users are eligible to repeat sequence after
                    this period. Defaults to WEEK if not specified.
            """

            steps: MutableSequence[
                "Campaign.VideoCampaignSettings.VideoAdSequenceStep"
            ] = proto.RepeatedField(
                proto.MESSAGE,
                number=1,
                message="Campaign.VideoCampaignSettings.VideoAdSequenceStep",
            )
            minimum_duration: (
                video_ad_sequence_minimum_duration.VideoAdSequenceMinimumDurationEnum.VideoAdSequenceMinimumDuration
            ) = proto.Field(
                proto.ENUM,
                number=2,
                enum=video_ad_sequence_minimum_duration.VideoAdSequenceMinimumDurationEnum.VideoAdSequenceMinimumDuration,
            )

        class VideoAdSequenceStep(proto.Message):
            r"""Information about a step within a video sequence.

            Attributes:
                video_ad_sequence_step_id (int):
                    The ID of this sequence step.
                asset_id (int):
                    The ID of the Asset for this step. The asset must be type
                    YOUTUBE_VIDEO.
                ad_group_type (google.ads.googleads.v23.enums.types.AdGroupTypeEnum.AdGroupType):
                    The ad group type for this step (denoting the
                    video format).
                previous_step_id (int):
                    The ID of the previous step. This field is
                    required for all steps except the first one. It
                    must point to a step that appears in the step
                    definition list before this step.
                previous_step_interaction_type (google.ads.googleads.v23.enums.types.VideoAdSequenceInteractionTypeEnum.VideoAdSequenceInteractionType):
                    Type of interaction *on the previous step* required in order
                    for the user to advance to this step. As with the previous
                    step ID, it's required for every step except for the first
                    one.
            """

            video_ad_sequence_step_id: int = proto.Field(
                proto.INT64,
                number=1,
            )
            asset_id: int = proto.Field(
                proto.INT64,
                number=2,
            )
            ad_group_type: gage_ad_group_type.AdGroupTypeEnum.AdGroupType = (
                proto.Field(
                    proto.ENUM,
                    number=3,
                    enum=gage_ad_group_type.AdGroupTypeEnum.AdGroupType,
                )
            )
            previous_step_id: int = proto.Field(
                proto.INT64,
                number=4,
            )
            previous_step_interaction_type: (
                video_ad_sequence_interaction_type.VideoAdSequenceInteractionTypeEnum.VideoAdSequenceInteractionType
            ) = proto.Field(
                proto.ENUM,
                number=5,
                enum=video_ad_sequence_interaction_type.VideoAdSequenceInteractionTypeEnum.VideoAdSequenceInteractionType,
            )

        class ReservationAdCategorySelfDisclosure(proto.Message):
            r"""Container for ad category self-disclosure for campaigns with the
            FIXED_CPM or FIXED_SHARE_OF_VOICE bidding strategies.

            Attributes:
                gambling (bool):
                    The campaign is expected to contain
                    gambling-related ads.
                alcohol (bool):
                    The campaign is expected to contain
                    alcohol-related ads.
                politics (bool):
                    The campaign is expected to contain
                    politics-related ads.
            """

            gambling: bool = proto.Field(
                proto.BOOL,
                number=1,
            )
            alcohol: bool = proto.Field(
                proto.BOOL,
                number=2,
            )
            politics: bool = proto.Field(
                proto.BOOL,
                number=3,
            )

        class BookingDetails(proto.Message):
            r"""Container for booking details for campaigns with the FIXED_CPM or
            FIXED_SHARE_OF_VOICE bidding strategies.

            Attributes:
                status (google.ads.googleads.v23.enums.types.BookingStatusEnum.BookingStatus):
                    Output only. The status of the booking.
                hold_expiration_date_time (str):
                    Output only. Time until which booked inventory will be held
                    or has been held for this campaign. Available for status
                    HELD and HOLD_EXPIRED. Format is "yyyy-MM-dd HH:mm:ss" in
                    the customer's time zone.
                cancellation_date_time (str):
                    Output only. Time when the booked inventory of this campaign
                    will be cancelled or has been cancelled. Available for
                    primary status NOT_ELIGIBLE if the campaign will be
                    cancelled and for primary status reason BOOKING_CANCELLED.
                    Format is "yyyy-MM-dd HH:mm:ss" in the customer's time zone.
            """

            status: booking_status.BookingStatusEnum.BookingStatus = (
                proto.Field(
                    proto.ENUM,
                    number=1,
                    enum=booking_status.BookingStatusEnum.BookingStatus,
                )
            )
            hold_expiration_date_time: str = proto.Field(
                proto.STRING,
                number=2,
            )
            cancellation_date_time: str = proto.Field(
                proto.STRING,
                number=3,
            )

        video_ad_sequence: "Campaign.VideoCampaignSettings.VideoAdSequence" = (
            proto.Field(
                proto.MESSAGE,
                number=4,
                message="Campaign.VideoCampaignSettings.VideoAdSequence",
            )
        )
        reservation_ad_category_self_disclosure: (
            "Campaign.VideoCampaignSettings.ReservationAdCategorySelfDisclosure"
        ) = proto.Field(
            proto.MESSAGE,
            number=5,
            message="Campaign.VideoCampaignSettings.ReservationAdCategorySelfDisclosure",
        )
        booking_details: "Campaign.VideoCampaignSettings.BookingDetails" = (
            proto.Field(
                proto.MESSAGE,
                number=6,
                message="Campaign.VideoCampaignSettings.BookingDetails",
            )
        )
        video_ad_inventory_control: (
            "Campaign.VideoCampaignSettings.VideoAdInventoryControl"
        ) = proto.Field(
            proto.MESSAGE,
            number=2,
            oneof="fluidity_control",
            message="Campaign.VideoCampaignSettings.VideoAdInventoryControl",
        )
        video_ad_format_control: (
            "Campaign.VideoCampaignSettings.VideoAdFormatControl"
        ) = proto.Field(
            proto.MESSAGE,
            number=3,
            oneof="fluidity_control",
            message="Campaign.VideoCampaignSettings.VideoAdFormatControl",
        )

    class PmaxCampaignSettings(proto.Message):
        r"""Settings for Performance Max campaigns.

        Attributes:
            brand_targeting_overrides (google.ads.googleads.v23.resources.types.Campaign.PmaxCampaignSettings.BrandTargetingOverrides):
                Overrides of brand targeting for various ad
                types.
        """

        class BrandTargetingOverrides(proto.Message):
            r"""Overrides of brand targeting for various ad types.

            .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

            Attributes:
                ignore_exclusions_for_shopping_ads (bool):
                    If true, brand exclusions are ignored for
                    Shopping ads.

                    This field is a member of `oneof`_ ``_ignore_exclusions_for_shopping_ads``.
            """

            ignore_exclusions_for_shopping_ads: bool = proto.Field(
                proto.BOOL,
                number=1,
                optional=True,
            )

        brand_targeting_overrides: (
            "Campaign.PmaxCampaignSettings.BrandTargetingOverrides"
        ) = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Campaign.PmaxCampaignSettings.BrandTargetingOverrides",
        )

    class AssetAutomationSetting(proto.Message):
        r"""Asset automation setting contains pair of AssetAutomationType
        and the asset automation opt-in/out status


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            asset_automation_type (google.ads.googleads.v23.enums.types.AssetAutomationTypeEnum.AssetAutomationType):
                The asset automation type advertiser would
                like to opt-in/out.

                This field is a member of `oneof`_ ``_asset_automation_type``.
            asset_automation_status (google.ads.googleads.v23.enums.types.AssetAutomationStatusEnum.AssetAutomationStatus):
                The opt-in/out status of asset automation
                type.

                This field is a member of `oneof`_ ``_asset_automation_status``.
        """

        asset_automation_type: (
            gage_asset_automation_type.AssetAutomationTypeEnum.AssetAutomationType
        ) = proto.Field(
            proto.ENUM,
            number=1,
            optional=True,
            enum=gage_asset_automation_type.AssetAutomationTypeEnum.AssetAutomationType,
        )
        asset_automation_status: (
            gage_asset_automation_status.AssetAutomationStatusEnum.AssetAutomationStatus
        ) = proto.Field(
            proto.ENUM,
            number=2,
            optional=True,
            enum=gage_asset_automation_status.AssetAutomationStatusEnum.AssetAutomationStatus,
        )

    class BrandGuidelines(proto.Message):
        r"""Settings that control the visual appearance of your brand in
        a campaign's automatically generated assets and formats. Only
        applicable to Performance Max campaigns.

        Attributes:
            main_color (str):
                The main brand color, entered as a hex code (e.g., #00ff00).
                You must provide the main_color if you provide an
                accent_color.
            accent_color (str):
                The accent brand color, entered as a hex code (e.g.,
                #00ff00). You must provide the accent_color if you provide a
                main_color.
            predefined_font_family (str):
                The brand's font family. Must be one of the
                following Google Fonts (case sensitive): Open
                Sans, Roboto, Montserrat, Poppins, Lato, Oswald,
                Playfair Display, Roboto Slab.
        """

        main_color: str = proto.Field(
            proto.STRING,
            number=1,
        )
        accent_color: str = proto.Field(
            proto.STRING,
            number=2,
        )
        predefined_font_family: str = proto.Field(
            proto.STRING,
            number=3,
        )

    class TextGuidelines(proto.Message):
        r"""Settings to control automatically generated text assets.

        Attributes:
            term_exclusions (MutableSequence[str]):
                Exact words or phrases that will be excluded
                from generated text assets. At most 25
                exclusions may be provided. Valid exclusions may
                contain a maximum of 30 characters.
            messaging_restrictions (MutableSequence[google.ads.googleads.v23.resources.types.Campaign.MessagingRestriction]):
                Freeform instructions that will be used to
                guide text asset generation using LLM inference.
                At most 40 restrictions may be provided.
        """

        term_exclusions: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=1,
        )
        messaging_restrictions: MutableSequence[
            "Campaign.MessagingRestriction"
        ] = proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="Campaign.MessagingRestriction",
        )

    class MessagingRestriction(proto.Message):
        r"""Freeform instructions that will be used to guide text asset
        generation using LLM inference.

        Attributes:
            restriction_text (str):
                Freeform instructions to guide text asset
                generation using LLM inference. Valid
                instructions may contain a maximum of 300
                characters.
            restriction_type (google.ads.googleads.v23.enums.types.MessagingRestrictionTypeEnum.MessagingRestrictionType):
                Determines how the guideline is applied. Only
                ``RESTRICTION_BASED_EXCLUSION`` is currently supported.
        """

        restriction_text: str = proto.Field(
            proto.STRING,
            number=1,
        )
        restriction_type: (
            messaging_restriction_type.MessagingRestrictionTypeEnum.MessagingRestrictionType
        ) = proto.Field(
            proto.ENUM,
            number=2,
            enum=messaging_restriction_type.MessagingRestrictionTypeEnum.MessagingRestrictionType,
        )

    class AiMaxSetting(proto.Message):
        r"""Settings for AI Max in search campaigns.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            enable_ai_max (bool):
                Controls whether or not AI Max features are served for this
                campaign.

                Individual AI Max features are enabled or disabled by their
                respective settings. But if enable_ai_max is set to false or
                cleared, then no AI Max features will serve for this
                campaign, regardless of the other settings.

                Search Term Matching is enabled by default when AI Max is
                enabled, and can be disabled at the ad group level.

                This field is a member of `oneof`_ ``_enable_ai_max``.
            bundling_required (google.ads.googleads.v23.resources.types.Campaign.AiMaxSetting.AiMaxBundlingRequired):
                Output only. Indicates whether a search
                campaign has adopted AI Max before, and is
                required to have AI Max enabled to adopt
                campaign-level text asset automation and brand
                list targeting in all API versions.

                This field is a member of `oneof`_ ``_bundling_required``.
        """

        class AiMaxBundlingRequired(proto.Enum):
            r"""Enum describing whether AI Max must be enabled to serve and
            update text asset automation and brand list features newly
            bundled with AI Max.

            Values:
                UNSPECIFIED (0):
                    Not specified.
                UNKNOWN (1):
                    Used for return value only. Represents value
                    unknown in this version.
                NOT_REQUIRED (2):
                    Search campaign is using text asset
                    automation or brand list targeting, and AI Max
                    is not required to be enabled to serve these
                    features.
                REQUIRED (3):
                    AI Max is required to be enabled for this
                    search campaign to serve existing text asset
                    automation and brand list targeting, or to add
                    new text asset automation and brand list
                    targeting settings.
            """

            UNSPECIFIED = 0
            UNKNOWN = 1
            NOT_REQUIRED = 2
            REQUIRED = 3

        enable_ai_max: bool = proto.Field(
            proto.BOOL,
            number=1,
            optional=True,
        )
        bundling_required: "Campaign.AiMaxSetting.AiMaxBundlingRequired" = (
            proto.Field(
                proto.ENUM,
                number=2,
                optional=True,
                enum="Campaign.AiMaxSetting.AiMaxBundlingRequired",
            )
        )

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    id: int = proto.Field(
        proto.INT64,
        number=59,
        optional=True,
    )
    name: str = proto.Field(
        proto.STRING,
        number=58,
        optional=True,
    )
    primary_status: (
        campaign_primary_status.CampaignPrimaryStatusEnum.CampaignPrimaryStatus
    ) = proto.Field(
        proto.ENUM,
        number=81,
        enum=campaign_primary_status.CampaignPrimaryStatusEnum.CampaignPrimaryStatus,
    )
    primary_status_reasons: MutableSequence[
        campaign_primary_status_reason.CampaignPrimaryStatusReasonEnum.CampaignPrimaryStatusReason
    ] = proto.RepeatedField(
        proto.ENUM,
        number=82,
        enum=campaign_primary_status_reason.CampaignPrimaryStatusReasonEnum.CampaignPrimaryStatusReason,
    )
    status: campaign_status.CampaignStatusEnum.CampaignStatus = proto.Field(
        proto.ENUM,
        number=5,
        enum=campaign_status.CampaignStatusEnum.CampaignStatus,
    )
    serving_status: (
        campaign_serving_status.CampaignServingStatusEnum.CampaignServingStatus
    ) = proto.Field(
        proto.ENUM,
        number=21,
        enum=campaign_serving_status.CampaignServingStatusEnum.CampaignServingStatus,
    )
    bidding_strategy_system_status: (
        gage_bidding_strategy_system_status.BiddingStrategySystemStatusEnum.BiddingStrategySystemStatus
    ) = proto.Field(
        proto.ENUM,
        number=78,
        enum=gage_bidding_strategy_system_status.BiddingStrategySystemStatusEnum.BiddingStrategySystemStatus,
    )
    ad_serving_optimization_status: (
        gage_ad_serving_optimization_status.AdServingOptimizationStatusEnum.AdServingOptimizationStatus
    ) = proto.Field(
        proto.ENUM,
        number=8,
        enum=gage_ad_serving_optimization_status.AdServingOptimizationStatusEnum.AdServingOptimizationStatus,
    )
    advertising_channel_type: (
        gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType
    ) = proto.Field(
        proto.ENUM,
        number=9,
        enum=gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType,
    )
    advertising_channel_sub_type: (
        gage_advertising_channel_sub_type.AdvertisingChannelSubTypeEnum.AdvertisingChannelSubType
    ) = proto.Field(
        proto.ENUM,
        number=10,
        enum=gage_advertising_channel_sub_type.AdvertisingChannelSubTypeEnum.AdvertisingChannelSubType,
    )
    tracking_url_template: str = proto.Field(
        proto.STRING,
        number=60,
        optional=True,
    )
    url_custom_parameters: MutableSequence[custom_parameter.CustomParameter] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=12,
            message=custom_parameter.CustomParameter,
        )
    )
    local_services_campaign_settings: LocalServicesCampaignSettings = (
        proto.Field(
            proto.MESSAGE,
            number=75,
            message=LocalServicesCampaignSettings,
        )
    )
    travel_campaign_settings: TravelCampaignSettings = proto.Field(
        proto.MESSAGE,
        number=85,
        message=TravelCampaignSettings,
    )
    demand_gen_campaign_settings: DemandGenCampaignSettings = proto.Field(
        proto.MESSAGE,
        number=91,
        message=DemandGenCampaignSettings,
    )
    video_campaign_settings: VideoCampaignSettings = proto.Field(
        proto.MESSAGE,
        number=94,
        message=VideoCampaignSettings,
    )
    pmax_campaign_settings: PmaxCampaignSettings = proto.Field(
        proto.MESSAGE,
        number=97,
        message=PmaxCampaignSettings,
    )
    real_time_bidding_setting: (
        gagc_real_time_bidding_setting.RealTimeBiddingSetting
    ) = proto.Field(
        proto.MESSAGE,
        number=39,
        message=gagc_real_time_bidding_setting.RealTimeBiddingSetting,
    )
    network_settings: NetworkSettings = proto.Field(
        proto.MESSAGE,
        number=14,
        message=NetworkSettings,
    )
    hotel_setting: HotelSettingInfo = proto.Field(
        proto.MESSAGE,
        number=32,
        message=HotelSettingInfo,
    )
    dynamic_search_ads_setting: DynamicSearchAdsSetting = proto.Field(
        proto.MESSAGE,
        number=33,
        message=DynamicSearchAdsSetting,
    )
    shopping_setting: ShoppingSetting = proto.Field(
        proto.MESSAGE,
        number=36,
        message=ShoppingSetting,
    )
    targeting_setting: gagc_targeting_setting.TargetingSetting = proto.Field(
        proto.MESSAGE,
        number=43,
        message=gagc_targeting_setting.TargetingSetting,
    )
    audience_setting: AudienceSetting = proto.Field(
        proto.MESSAGE,
        number=73,
        optional=True,
        message=AudienceSetting,
    )
    geo_target_type_setting: GeoTargetTypeSetting = proto.Field(
        proto.MESSAGE,
        number=47,
        message=GeoTargetTypeSetting,
    )
    local_campaign_setting: LocalCampaignSetting = proto.Field(
        proto.MESSAGE,
        number=50,
        message=LocalCampaignSetting,
    )
    app_campaign_setting: AppCampaignSetting = proto.Field(
        proto.MESSAGE,
        number=51,
        message=AppCampaignSetting,
    )
    labels: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=61,
    )
    experiment_type: (
        campaign_experiment_type.CampaignExperimentTypeEnum.CampaignExperimentType
    ) = proto.Field(
        proto.ENUM,
        number=17,
        enum=campaign_experiment_type.CampaignExperimentTypeEnum.CampaignExperimentType,
    )
    base_campaign: str = proto.Field(
        proto.STRING,
        number=56,
        optional=True,
    )
    campaign_budget: str = proto.Field(
        proto.STRING,
        number=62,
        optional=True,
    )
    bidding_strategy_type: (
        gage_bidding_strategy_type.BiddingStrategyTypeEnum.BiddingStrategyType
    ) = proto.Field(
        proto.ENUM,
        number=22,
        enum=gage_bidding_strategy_type.BiddingStrategyTypeEnum.BiddingStrategyType,
    )
    accessible_bidding_strategy: str = proto.Field(
        proto.STRING,
        number=71,
    )
    campaign_group: str = proto.Field(
        proto.STRING,
        number=76,
        optional=True,
    )
    start_date_time: str = proto.Field(
        proto.STRING,
        number=104,
        optional=True,
    )
    end_date_time: str = proto.Field(
        proto.STRING,
        number=105,
        optional=True,
    )
    final_url_suffix: str = proto.Field(
        proto.STRING,
        number=65,
        optional=True,
    )
    frequency_caps: MutableSequence[frequency_cap.FrequencyCapEntry] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=40,
            message=frequency_cap.FrequencyCapEntry,
        )
    )
    video_brand_safety_suitability: (
        brand_safety_suitability.BrandSafetySuitabilityEnum.BrandSafetySuitability
    ) = proto.Field(
        proto.ENUM,
        number=42,
        enum=brand_safety_suitability.BrandSafetySuitabilityEnum.BrandSafetySuitability,
    )
    vanity_pharma: VanityPharma = proto.Field(
        proto.MESSAGE,
        number=44,
        message=VanityPharma,
    )
    selective_optimization: SelectiveOptimization = proto.Field(
        proto.MESSAGE,
        number=45,
        message=SelectiveOptimization,
    )
    optimization_goal_setting: OptimizationGoalSetting = proto.Field(
        proto.MESSAGE,
        number=54,
        message=OptimizationGoalSetting,
    )
    tracking_setting: TrackingSetting = proto.Field(
        proto.MESSAGE,
        number=46,
        message=TrackingSetting,
    )
    payment_mode: gage_payment_mode.PaymentModeEnum.PaymentMode = proto.Field(
        proto.ENUM,
        number=52,
        enum=gage_payment_mode.PaymentModeEnum.PaymentMode,
    )
    optimization_score: float = proto.Field(
        proto.DOUBLE,
        number=66,
        optional=True,
    )
    excluded_parent_asset_field_types: MutableSequence[
        asset_field_type.AssetFieldTypeEnum.AssetFieldType
    ] = proto.RepeatedField(
        proto.ENUM,
        number=69,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    excluded_parent_asset_set_types: MutableSequence[
        asset_set_type.AssetSetTypeEnum.AssetSetType
    ] = proto.RepeatedField(
        proto.ENUM,
        number=80,
        enum=asset_set_type.AssetSetTypeEnum.AssetSetType,
    )
    performance_max_upgrade: PerformanceMaxUpgrade = proto.Field(
        proto.MESSAGE,
        number=77,
        message=PerformanceMaxUpgrade,
    )
    hotel_property_asset_set: str = proto.Field(
        proto.STRING,
        number=83,
        optional=True,
    )
    listing_type: gage_listing_type.ListingTypeEnum.ListingType = proto.Field(
        proto.ENUM,
        number=86,
        optional=True,
        enum=gage_listing_type.ListingTypeEnum.ListingType,
    )
    asset_automation_settings: MutableSequence[AssetAutomationSetting] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=88,
            message=AssetAutomationSetting,
        )
    )
    keyword_match_type: (
        campaign_keyword_match_type.CampaignKeywordMatchTypeEnum.CampaignKeywordMatchType
    ) = proto.Field(
        proto.ENUM,
        number=90,
        enum=campaign_keyword_match_type.CampaignKeywordMatchTypeEnum.CampaignKeywordMatchType,
    )
    brand_guidelines_enabled: bool = proto.Field(
        proto.BOOL,
        number=96,
        optional=True,
    )
    brand_guidelines: BrandGuidelines = proto.Field(
        proto.MESSAGE,
        number=98,
        message=BrandGuidelines,
    )
    text_guidelines: TextGuidelines = proto.Field(
        proto.MESSAGE,
        number=107,
        message=TextGuidelines,
    )
    third_party_integration_partners: (
        gagc_third_party_integration_partners.CampaignThirdPartyIntegrationPartners
    ) = proto.Field(
        proto.MESSAGE,
        number=100,
        message=gagc_third_party_integration_partners.CampaignThirdPartyIntegrationPartners,
    )
    ai_max_setting: AiMaxSetting = proto.Field(
        proto.MESSAGE,
        number=101,
        message=AiMaxSetting,
    )
    contains_eu_political_advertising: (
        eu_political_advertising_status.EuPoliticalAdvertisingStatusEnum.EuPoliticalAdvertisingStatus
    ) = proto.Field(
        proto.ENUM,
        number=102,
        enum=eu_political_advertising_status.EuPoliticalAdvertisingStatusEnum.EuPoliticalAdvertisingStatus,
    )
    feed_types: MutableSequence[
        asset_set_type.AssetSetTypeEnum.AssetSetType
    ] = proto.RepeatedField(
        proto.ENUM,
        number=103,
        enum=asset_set_type.AssetSetTypeEnum.AssetSetType,
    )
    missing_eu_political_advertising_declaration: bool = proto.Field(
        proto.BOOL,
        number=108,
    )
    bidding_strategy: str = proto.Field(
        proto.STRING,
        number=67,
        oneof="campaign_bidding_strategy",
    )
    commission: bidding.Commission = proto.Field(
        proto.MESSAGE,
        number=49,
        oneof="campaign_bidding_strategy",
        message=bidding.Commission,
    )
    manual_cpa: bidding.ManualCpa = proto.Field(
        proto.MESSAGE,
        number=74,
        oneof="campaign_bidding_strategy",
        message=bidding.ManualCpa,
    )
    manual_cpc: bidding.ManualCpc = proto.Field(
        proto.MESSAGE,
        number=24,
        oneof="campaign_bidding_strategy",
        message=bidding.ManualCpc,
    )
    manual_cpm: bidding.ManualCpm = proto.Field(
        proto.MESSAGE,
        number=25,
        oneof="campaign_bidding_strategy",
        message=bidding.ManualCpm,
    )
    manual_cpv: bidding.ManualCpv = proto.Field(
        proto.MESSAGE,
        number=37,
        oneof="campaign_bidding_strategy",
        message=bidding.ManualCpv,
    )
    maximize_conversions: bidding.MaximizeConversions = proto.Field(
        proto.MESSAGE,
        number=30,
        oneof="campaign_bidding_strategy",
        message=bidding.MaximizeConversions,
    )
    maximize_conversion_value: bidding.MaximizeConversionValue = proto.Field(
        proto.MESSAGE,
        number=31,
        oneof="campaign_bidding_strategy",
        message=bidding.MaximizeConversionValue,
    )
    target_cpa: bidding.TargetCpa = proto.Field(
        proto.MESSAGE,
        number=26,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetCpa,
    )
    target_impression_share: bidding.TargetImpressionShare = proto.Field(
        proto.MESSAGE,
        number=48,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetImpressionShare,
    )
    target_roas: bidding.TargetRoas = proto.Field(
        proto.MESSAGE,
        number=29,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetRoas,
    )
    target_spend: bidding.TargetSpend = proto.Field(
        proto.MESSAGE,
        number=27,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetSpend,
    )
    percent_cpc: bidding.PercentCpc = proto.Field(
        proto.MESSAGE,
        number=34,
        oneof="campaign_bidding_strategy",
        message=bidding.PercentCpc,
    )
    target_cpm: bidding.TargetCpm = proto.Field(
        proto.MESSAGE,
        number=41,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetCpm,
    )
    fixed_cpm: bidding.FixedCpm = proto.Field(
        proto.MESSAGE,
        number=92,
        oneof="campaign_bidding_strategy",
        message=bidding.FixedCpm,
    )
    target_cpv: bidding.TargetCpv = proto.Field(
        proto.MESSAGE,
        number=93,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetCpv,
    )
    target_cpc: bidding.TargetCpc = proto.Field(
        proto.MESSAGE,
        number=99,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetCpc,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

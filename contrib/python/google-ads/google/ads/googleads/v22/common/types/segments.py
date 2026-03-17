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


import proto  # type: ignore

from google.ads.googleads.v22.common.types import criteria
from google.ads.googleads.v22.enums.types import (
    ad_destination_type as gage_ad_destination_type,
)
from google.ads.googleads.v22.enums.types import (
    ad_format_type as gage_ad_format_type,
)
from google.ads.googleads.v22.enums.types import (
    ad_network_type as gage_ad_network_type,
)
from google.ads.googleads.v22.enums.types import age_range_type
from google.ads.googleads.v22.enums.types import (
    budget_campaign_association_status as gage_budget_campaign_association_status,
)
from google.ads.googleads.v22.enums.types import click_type as gage_click_type
from google.ads.googleads.v22.enums.types import (
    conversion_action_category as gage_conversion_action_category,
)
from google.ads.googleads.v22.enums.types import (
    conversion_attribution_event_type as gage_conversion_attribution_event_type,
)
from google.ads.googleads.v22.enums.types import (
    conversion_lag_bucket as gage_conversion_lag_bucket,
)
from google.ads.googleads.v22.enums.types import (
    conversion_or_adjustment_lag_bucket as gage_conversion_or_adjustment_lag_bucket,
)
from google.ads.googleads.v22.enums.types import (
    conversion_value_rule_primary_dimension as gage_conversion_value_rule_primary_dimension,
)
from google.ads.googleads.v22.enums.types import (
    converting_user_prior_engagement_type_and_ltv_bucket,
)
from google.ads.googleads.v22.enums.types import day_of_week as gage_day_of_week
from google.ads.googleads.v22.enums.types import device as gage_device
from google.ads.googleads.v22.enums.types import (
    external_conversion_source as gage_external_conversion_source,
)
from google.ads.googleads.v22.enums.types import gender_type
from google.ads.googleads.v22.enums.types import (
    hotel_date_selection_type as gage_hotel_date_selection_type,
)
from google.ads.googleads.v22.enums.types import (
    hotel_price_bucket as gage_hotel_price_bucket,
)
from google.ads.googleads.v22.enums.types import (
    hotel_rate_type as gage_hotel_rate_type,
)
from google.ads.googleads.v22.enums.types import (
    landing_page_source as gage_landing_page_source,
)
from google.ads.googleads.v22.enums.types import match_type as gage_match_type
from google.ads.googleads.v22.enums.types import (
    month_of_year as gage_month_of_year,
)
from google.ads.googleads.v22.enums.types import (
    product_channel as gage_product_channel,
)
from google.ads.googleads.v22.enums.types import (
    product_channel_exclusivity as gage_product_channel_exclusivity,
)
from google.ads.googleads.v22.enums.types import (
    product_condition as gage_product_condition,
)
from google.ads.googleads.v22.enums.types import (
    recommendation_type as gage_recommendation_type,
)
from google.ads.googleads.v22.enums.types import (
    search_engine_results_page_type as gage_search_engine_results_page_type,
)
from google.ads.googleads.v22.enums.types import (
    search_term_match_source as gage_search_term_match_source,
)
from google.ads.googleads.v22.enums.types import (
    search_term_match_type as gage_search_term_match_type,
)
from google.ads.googleads.v22.enums.types import (
    search_term_targeting_status as gage_search_term_targeting_status,
)
from google.ads.googleads.v22.enums.types import (
    sk_ad_network_ad_event_type as gage_sk_ad_network_ad_event_type,
)
from google.ads.googleads.v22.enums.types import (
    sk_ad_network_attribution_credit as gage_sk_ad_network_attribution_credit,
)
from google.ads.googleads.v22.enums.types import (
    sk_ad_network_coarse_conversion_value as gage_sk_ad_network_coarse_conversion_value,
)
from google.ads.googleads.v22.enums.types import (
    sk_ad_network_source_type as gage_sk_ad_network_source_type,
)
from google.ads.googleads.v22.enums.types import (
    sk_ad_network_user_type as gage_sk_ad_network_user_type,
)
from google.ads.googleads.v22.enums.types import slot as gage_slot


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.common",
    marshal="google.ads.googleads.v22",
    manifest={
        "Segments",
        "Keyword",
        "BudgetCampaignAssociationStatus",
        "AssetInteractionTarget",
        "SkAdNetworkSourceApp",
    },
)


class Segments(proto.Message):
    r"""Segment only fields.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        activity_account_id (int):
            Activity account ID.

            This field is a member of `oneof`_ ``_activity_account_id``.
        activity_city (str):
            The city where the travel activity is
            available.

            This field is a member of `oneof`_ ``_activity_city``.
        activity_country (str):
            The country where the travel activity is
            available.

            This field is a member of `oneof`_ ``_activity_country``.
        activity_rating (int):
            Activity rating.

            This field is a member of `oneof`_ ``_activity_rating``.
        activity_state (str):
            The state where the travel activity is
            available.

            This field is a member of `oneof`_ ``_activity_state``.
        external_activity_id (str):
            Advertiser supplied activity ID.

            This field is a member of `oneof`_ ``_external_activity_id``.
        ad_destination_type (google.ads.googleads.v22.enums.types.AdDestinationTypeEnum.AdDestinationType):
            Ad Destination type.
        ad_format_type (google.ads.googleads.v22.enums.types.AdFormatTypeEnum.AdFormatType):
            Ad Format type.
        ad_network_type (google.ads.googleads.v22.enums.types.AdNetworkTypeEnum.AdNetworkType):
            Ad network type.
        ad_group (str):
            Resource name of the ad group.

            This field is a member of `oneof`_ ``_ad_group``.
        asset_group (str):
            Resource name of the asset group.

            This field is a member of `oneof`_ ``_asset_group``.
        auction_insight_domain (str):
            Domain (visible URL) of a participant in the
            Auction Insights report.

            This field is a member of `oneof`_ ``_auction_insight_domain``.
        budget_campaign_association_status (google.ads.googleads.v22.common.types.BudgetCampaignAssociationStatus):
            Budget campaign association status.
        campaign (str):
            Resource name of the campaign.

            This field is a member of `oneof`_ ``_campaign``.
        click_type (google.ads.googleads.v22.enums.types.ClickTypeEnum.ClickType):
            Click type.
        conversion_action (str):
            Resource name of the conversion action.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_action_category (google.ads.googleads.v22.enums.types.ConversionActionCategoryEnum.ConversionActionCategory):
            Conversion action category.
        conversion_action_name (str):
            Conversion action name.

            This field is a member of `oneof`_ ``_conversion_action_name``.
        conversion_adjustment (bool):
            This segments your conversion columns by the
            original conversion and conversion value versus
            the delta if conversions were adjusted. False
            row has the data as originally stated; While
            true row has the delta between data now and the
            data as originally stated. Summing the two
            together results post-adjustment data.

            This field is a member of `oneof`_ ``_conversion_adjustment``.
        conversion_attribution_event_type (google.ads.googleads.v22.enums.types.ConversionAttributionEventTypeEnum.ConversionAttributionEventType):
            Conversion attribution event type.
        conversion_lag_bucket (google.ads.googleads.v22.enums.types.ConversionLagBucketEnum.ConversionLagBucket):
            An enum value representing the number of days
            between the impression and the conversion.
        conversion_or_adjustment_lag_bucket (google.ads.googleads.v22.enums.types.ConversionOrAdjustmentLagBucketEnum.ConversionOrAdjustmentLagBucket):
            An enum value representing the number of days
            between the impression and the conversion or
            between the impression and adjustments to the
            conversion.
        date (str):
            Date to which metrics apply.
            yyyy-MM-dd format, for example, 2018-04-17.

            This field is a member of `oneof`_ ``_date``.
        day_of_week (google.ads.googleads.v22.enums.types.DayOfWeekEnum.DayOfWeek):
            Day of the week, for example, MONDAY.
        device (google.ads.googleads.v22.enums.types.DeviceEnum.Device):
            Device to which metrics apply.
        external_conversion_source (google.ads.googleads.v22.enums.types.ExternalConversionSourceEnum.ExternalConversionSource):
            External conversion source.
        geo_target_airport (str):
            Resource name of the geo target constant that
            represents an airport.

            This field is a member of `oneof`_ ``_geo_target_airport``.
        geo_target_canton (str):
            Resource name of the geo target constant that
            represents a canton.

            This field is a member of `oneof`_ ``_geo_target_canton``.
        geo_target_city (str):
            Resource name of the geo target constant that
            represents a city.

            This field is a member of `oneof`_ ``_geo_target_city``.
        geo_target_country (str):
            Resource name of the geo target constant that
            represents a country.

            This field is a member of `oneof`_ ``_geo_target_country``.
        geo_target_county (str):
            Resource name of the geo target constant that
            represents a county.

            This field is a member of `oneof`_ ``_geo_target_county``.
        geo_target_district (str):
            Resource name of the geo target constant that
            represents a district.

            This field is a member of `oneof`_ ``_geo_target_district``.
        geo_target_metro (str):
            Resource name of the geo target constant that
            represents a metro.

            This field is a member of `oneof`_ ``_geo_target_metro``.
        geo_target_most_specific_location (str):
            Resource name of the geo target constant that
            represents the most specific location.

            This field is a member of `oneof`_ ``_geo_target_most_specific_location``.
        geo_target_postal_code (str):
            Resource name of the geo target constant that
            represents a postal code.

            This field is a member of `oneof`_ ``_geo_target_postal_code``.
        geo_target_province (str):
            Resource name of the geo target constant that
            represents a province.

            This field is a member of `oneof`_ ``_geo_target_province``.
        geo_target_region (str):
            Resource name of the geo target constant that
            represents a region.

            This field is a member of `oneof`_ ``_geo_target_region``.
        geo_target_state (str):
            Resource name of the geo target constant that
            represents a state.

            This field is a member of `oneof`_ ``_geo_target_state``.
        hotel_booking_window_days (int):
            Hotel booking window in days.

            This field is a member of `oneof`_ ``_hotel_booking_window_days``.
        hotel_center_id (int):
            Hotel center ID.

            This field is a member of `oneof`_ ``_hotel_center_id``.
        hotel_check_in_date (str):
            Hotel check-in date. Formatted as yyyy-MM-dd.

            This field is a member of `oneof`_ ``_hotel_check_in_date``.
        hotel_check_in_day_of_week (google.ads.googleads.v22.enums.types.DayOfWeekEnum.DayOfWeek):
            Hotel check-in day of week.
        hotel_city (str):
            Hotel city.

            This field is a member of `oneof`_ ``_hotel_city``.
        hotel_class (int):
            Hotel class.

            This field is a member of `oneof`_ ``_hotel_class``.
        hotel_country (str):
            Hotel country.

            This field is a member of `oneof`_ ``_hotel_country``.
        hotel_date_selection_type (google.ads.googleads.v22.enums.types.HotelDateSelectionTypeEnum.HotelDateSelectionType):
            Hotel date selection type.
        hotel_length_of_stay (int):
            Hotel length of stay.

            This field is a member of `oneof`_ ``_hotel_length_of_stay``.
        hotel_rate_rule_id (str):
            Hotel rate rule ID.

            This field is a member of `oneof`_ ``_hotel_rate_rule_id``.
        hotel_rate_type (google.ads.googleads.v22.enums.types.HotelRateTypeEnum.HotelRateType):
            Hotel rate type.
        hotel_price_bucket (google.ads.googleads.v22.enums.types.HotelPriceBucketEnum.HotelPriceBucket):
            Hotel price bucket.
        hotel_state (str):
            Hotel state.

            This field is a member of `oneof`_ ``_hotel_state``.
        hour (int):
            Hour of day as a number between 0 and 23,
            inclusive.

            This field is a member of `oneof`_ ``_hour``.
        interaction_on_this_extension (bool):
            Only used with feed item metrics.
            Indicates whether the interaction metrics
            occurred on the feed item itself or a different
            extension or ad unit.

            This field is a member of `oneof`_ ``_interaction_on_this_extension``.
        keyword (google.ads.googleads.v22.common.types.Keyword):
            Keyword criterion.
        landing_page_source (google.ads.googleads.v22.enums.types.LandingPageSourceEnum.LandingPageSource):
            The source of a landing page in the landing
            page report.
        month (str):
            Month as represented by the date of the first
            day of a month. Formatted as yyyy-MM-dd.

            This field is a member of `oneof`_ ``_month``.
        month_of_year (google.ads.googleads.v22.enums.types.MonthOfYearEnum.MonthOfYear):
            Month of the year, for example, January.
        partner_hotel_id (str):
            Partner hotel ID.

            This field is a member of `oneof`_ ``_partner_hotel_id``.
        product_aggregator_id (int):
            Aggregator ID of the product.

            This field is a member of `oneof`_ ``_product_aggregator_id``.
        product_category_level1 (str):
            Category (level 1) of the product.

            This field is a member of `oneof`_ ``_product_category_level1``.
        product_category_level2 (str):
            Category (level 2) of the product.

            This field is a member of `oneof`_ ``_product_category_level2``.
        product_category_level3 (str):
            Category (level 3) of the product.

            This field is a member of `oneof`_ ``_product_category_level3``.
        product_category_level4 (str):
            Category (level 4) of the product.

            This field is a member of `oneof`_ ``_product_category_level4``.
        product_category_level5 (str):
            Category (level 5) of the product.

            This field is a member of `oneof`_ ``_product_category_level5``.
        product_brand (str):
            Brand of the product.

            This field is a member of `oneof`_ ``_product_brand``.
        product_channel (google.ads.googleads.v22.enums.types.ProductChannelEnum.ProductChannel):
            Channel of the product.
        product_channel_exclusivity (google.ads.googleads.v22.enums.types.ProductChannelExclusivityEnum.ProductChannelExclusivity):
            Channel exclusivity of the product.
        product_condition (google.ads.googleads.v22.enums.types.ProductConditionEnum.ProductCondition):
            Condition of the product.
        product_country (str):
            Resource name of the geo target constant for
            the country of sale of the product.

            This field is a member of `oneof`_ ``_product_country``.
        product_custom_attribute0 (str):
            Custom attribute 0 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute0``.
        product_custom_attribute1 (str):
            Custom attribute 1 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute1``.
        product_custom_attribute2 (str):
            Custom attribute 2 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute2``.
        product_custom_attribute3 (str):
            Custom attribute 3 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute3``.
        product_custom_attribute4 (str):
            Custom attribute 4 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute4``.
        product_feed_label (str):
            Feed label of the product.

            This field is a member of `oneof`_ ``_product_feed_label``.
        product_item_id (str):
            Item ID of the product.

            This field is a member of `oneof`_ ``_product_item_id``.
        product_language (str):
            Resource name of the language constant for
            the language of the product.

            This field is a member of `oneof`_ ``_product_language``.
        product_merchant_id (int):
            Merchant ID of the product.

            This field is a member of `oneof`_ ``_product_merchant_id``.
        product_store_id (str):
            Store ID of the product.

            This field is a member of `oneof`_ ``_product_store_id``.
        product_title (str):
            Title of the product.

            This field is a member of `oneof`_ ``_product_title``.
        product_type_l1 (str):
            Type (level 1) of the product.

            This field is a member of `oneof`_ ``_product_type_l1``.
        product_type_l2 (str):
            Type (level 2) of the product.

            This field is a member of `oneof`_ ``_product_type_l2``.
        product_type_l3 (str):
            Type (level 3) of the product.

            This field is a member of `oneof`_ ``_product_type_l3``.
        product_type_l4 (str):
            Type (level 4) of the product.

            This field is a member of `oneof`_ ``_product_type_l4``.
        product_type_l5 (str):
            Type (level 5) of the product.

            This field is a member of `oneof`_ ``_product_type_l5``.
        quarter (str):
            Quarter as represented by the date of the
            first day of a quarter. Uses the calendar year
            for quarters, for example, the second quarter of
            2018 starts on 2018-04-01. Formatted as
            yyyy-MM-dd.

            This field is a member of `oneof`_ ``_quarter``.
        travel_destination_city (str):
            The city the user is searching for at query
            time.

            This field is a member of `oneof`_ ``_travel_destination_city``.
        travel_destination_country (str):
            The country the user is searching for at
            query time.

            This field is a member of `oneof`_ ``_travel_destination_country``.
        travel_destination_region (str):
            The region the user is searching for at query
            time.

            This field is a member of `oneof`_ ``_travel_destination_region``.
        recommendation_type (google.ads.googleads.v22.enums.types.RecommendationTypeEnum.RecommendationType):
            Recommendation type.
        search_engine_results_page_type (google.ads.googleads.v22.enums.types.SearchEngineResultsPageTypeEnum.SearchEngineResultsPageType):
            Type of the search engine results page.
        search_subcategory (str):
            A search term subcategory. An empty string
            denotes the catch-all subcategory for search
            terms that didn't fit into another subcategory.

            This field is a member of `oneof`_ ``_search_subcategory``.
        search_term (str):
            A search term.

            This field is a member of `oneof`_ ``_search_term``.
        search_term_match_type (google.ads.googleads.v22.enums.types.SearchTermMatchTypeEnum.SearchTermMatchType):
            Match type of the keyword that triggered the ad. This
            segment is for use with keyword_view. For other resources,
            use match_type. While match_type is filtered to Broad,
            Exact, Phrase and Ai Max, search_term_match_type includes
            variants like Near Exact and Near Phrase.
        match_type (google.ads.googleads.v22.enums.types.MatchTypeEnum.MatchType):
            The match type of the keyword that triggered the ad. This
            segment is for use with keyword_view. For other resources,
            use search_term_match_type. While match_type is filtered to
            Broad, Exact, Phrase and Ai Max, search_term_match_type
            includes variants like Near Exact, Near Phrase.
        slot (google.ads.googleads.v22.enums.types.SlotEnum.Slot):
            Position of the ad.
        conversion_value_rule_primary_dimension (google.ads.googleads.v22.enums.types.ConversionValueRulePrimaryDimensionEnum.ConversionValueRulePrimaryDimension):
            Primary dimension of applied conversion value rules.
            NO_RULE_APPLIED shows the total recorded value of
            conversions that do not have a value rule applied. ORIGINAL
            shows the original value of conversions to which a value
            rule has been applied. GEO_LOCATION, DEVICE, AUDIENCE,
            ITINERARY show the net adjustment after value rules were
            applied.
        webpage (str):
            Resource name of the ad group criterion that
            represents webpage criterion.

            This field is a member of `oneof`_ ``_webpage``.
        week (str):
            Week as defined as Monday through Sunday, and
            represented by the date of Monday. Formatted as
            yyyy-MM-dd.

            This field is a member of `oneof`_ ``_week``.
        year (int):
            Year, formatted as yyyy.

            This field is a member of `oneof`_ ``_year``.
        sk_ad_network_fine_conversion_value (int):
            iOS Store Kit Ad Network conversion value.
            Null value means this segment is not applicable,
            for example, non-iOS campaign.

            This field is a member of `oneof`_ ``_sk_ad_network_fine_conversion_value``.
        sk_ad_network_redistributed_fine_conversion_value (int):
            iOS Store Kit Ad Network redistributed fine
            conversion value.
            Google uses modeling on observed conversion
            values(obtained from Apple) to calculate
            conversions from SKAN postbacks where NULLs are
            returned. This column represents the sum of the
            modeled conversion values and the observed
            conversion values. See
            https://support.google.com/google-ads/answer/14892597
            to lean more.

            This field is a member of `oneof`_ ``_sk_ad_network_redistributed_fine_conversion_value``.
        sk_ad_network_user_type (google.ads.googleads.v22.enums.types.SkAdNetworkUserTypeEnum.SkAdNetworkUserType):
            iOS Store Kit Ad Network user type.
        sk_ad_network_ad_event_type (google.ads.googleads.v22.enums.types.SkAdNetworkAdEventTypeEnum.SkAdNetworkAdEventType):
            iOS Store Kit Ad Network ad event type.
        sk_ad_network_source_app (google.ads.googleads.v22.common.types.SkAdNetworkSourceApp):
            App where the ad that drove the iOS Store Kit
            Ad Network install was shown. Null value means
            this segment is not applicable, for example,
            non-iOS campaign, or was not present in any
            postbacks sent by Apple.

            This field is a member of `oneof`_ ``_sk_ad_network_source_app``.
        sk_ad_network_attribution_credit (google.ads.googleads.v22.enums.types.SkAdNetworkAttributionCreditEnum.SkAdNetworkAttributionCredit):
            iOS Store Kit Ad Network attribution credit
        sk_ad_network_coarse_conversion_value (google.ads.googleads.v22.enums.types.SkAdNetworkCoarseConversionValueEnum.SkAdNetworkCoarseConversionValue):
            iOS Store Kit Ad Network coarse conversion
            value.
        sk_ad_network_source_domain (str):
            Website where the ad that drove the iOS Store
            Kit Ad Network install was shown. Null value
            means this segment is not applicable, for
            example, non-iOS campaign, or was not present in
            any postbacks sent by Apple.

            This field is a member of `oneof`_ ``_sk_ad_network_source_domain``.
        sk_ad_network_source_type (google.ads.googleads.v22.enums.types.SkAdNetworkSourceTypeEnum.SkAdNetworkSourceType):
            The source type where the ad that drove the
            iOS Store Kit Ad Network install was shown. Null
            value means this segment is not applicable, for
            example, non-iOS campaign, or neither source
            domain nor source app were present in any
            postbacks sent by Apple.
        sk_ad_network_postback_sequence_index (int):
            iOS Store Kit Ad Network postback sequence
            index.

            This field is a member of `oneof`_ ``_sk_ad_network_postback_sequence_index``.
        sk_ad_network_version (str):
            The version of the SKAdNetwork API used.

            This field is a member of `oneof`_ ``_sk_ad_network_version``.
        asset_interaction_target (google.ads.googleads.v22.common.types.AssetInteractionTarget):
            Only used with CustomerAsset, CampaignAsset and AdGroupAsset
            metrics. Indicates whether the interaction metrics occurred
            on the asset itself or a different asset or ad unit.
            Interactions (for example, clicks) are counted across all
            the parts of the served ad (for example, Ad itself and other
            components like Sitelinks) when they are served together.
            When interaction_on_this_asset is true, it means the
            interactions are on this specific asset and when
            interaction_on_this_asset is false, it means the
            interactions is not on this specific asset but on other
            parts of the served ad this asset is served with.

            This field is a member of `oneof`_ ``_asset_interaction_target``.
        new_versus_returning_customers (google.ads.googleads.v22.enums.types.ConvertingUserPriorEngagementTypeAndLtvBucketEnum.ConvertingUserPriorEngagementTypeAndLtvBucket):
            This is for segmenting conversions by whether
            the user is a new customer or a returning
            customer. This segmentation is typically used to
            measure the impact of customer acquisition goal.
        adjusted_age_range (google.ads.googleads.v22.enums.types.AgeRangeTypeEnum.AgeRangeType):
            Adjusted age range. This is the age range of the user after
            applying modeling to get more accurate age and gender
            information. Currently, both adjusted_age_range and
            adjusted_gender need to be selected together to get valid
            reach stats. These segmentations are only available for
            allowlisted customers.
        adjusted_gender (google.ads.googleads.v22.enums.types.GenderTypeEnum.GenderType):
            Adjusted gender. This is the gender of the user after
            applying modeling to get more accurate age and gender
            information. Currently, both adjusted_age_range and
            adjusted_gender need to be selected together to get valid
            reach stats. These segmentations are only available for
            allowlisted customers.
        search_term_match_source (google.ads.googleads.v22.enums.types.SearchTermMatchSourceEnum.SearchTermMatchSource):
            Specifies the source for how the search term
            was matched, which reveals the type of ad
            campaign responsible. Use this to distinguish
            between automated campaigns (like AI Max,
            Dynamic Search Ads) and keyword-based campaigns
            (Standard).
        search_term_targeting_status (google.ads.googleads.v22.enums.types.SearchTermTargetingStatusEnum.SearchTermTargetingStatus):
            Indicates whether the search term is
            currently one of your targeted or excluded
            keywords.
        ad_using_product_data (bool):
            Indicates whether an ad is using product data
            from a Google Merchant Center feed. This segment
            is only available for PMax campaigns and will
            not return data when any other campaign type is
            selected.

            This field is a member of `oneof`_ ``_ad_using_product_data``.
        ad_using_video (bool):
            Indicates whether an ad is using a video
            asset. This segment is only available for PMax
            campaigns and will not return data when any
            other campaign type is selected.

            This field is a member of `oneof`_ ``_ad_using_video``.
    """

    activity_account_id: int = proto.Field(
        proto.INT64,
        number=148,
        optional=True,
    )
    activity_city: str = proto.Field(
        proto.STRING,
        number=185,
        optional=True,
    )
    activity_country: str = proto.Field(
        proto.STRING,
        number=186,
        optional=True,
    )
    activity_rating: int = proto.Field(
        proto.INT64,
        number=149,
        optional=True,
    )
    activity_state: str = proto.Field(
        proto.STRING,
        number=187,
        optional=True,
    )
    external_activity_id: str = proto.Field(
        proto.STRING,
        number=150,
        optional=True,
    )
    ad_destination_type: (
        gage_ad_destination_type.AdDestinationTypeEnum.AdDestinationType
    ) = proto.Field(
        proto.ENUM,
        number=136,
        enum=gage_ad_destination_type.AdDestinationTypeEnum.AdDestinationType,
    )
    ad_format_type: gage_ad_format_type.AdFormatTypeEnum.AdFormatType = (
        proto.Field(
            proto.ENUM,
            number=191,
            enum=gage_ad_format_type.AdFormatTypeEnum.AdFormatType,
        )
    )
    ad_network_type: gage_ad_network_type.AdNetworkTypeEnum.AdNetworkType = (
        proto.Field(
            proto.ENUM,
            number=3,
            enum=gage_ad_network_type.AdNetworkTypeEnum.AdNetworkType,
        )
    )
    ad_group: str = proto.Field(
        proto.STRING,
        number=158,
        optional=True,
    )
    asset_group: str = proto.Field(
        proto.STRING,
        number=159,
        optional=True,
    )
    auction_insight_domain: str = proto.Field(
        proto.STRING,
        number=145,
        optional=True,
    )
    budget_campaign_association_status: "BudgetCampaignAssociationStatus" = (
        proto.Field(
            proto.MESSAGE,
            number=134,
            message="BudgetCampaignAssociationStatus",
        )
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=157,
        optional=True,
    )
    click_type: gage_click_type.ClickTypeEnum.ClickType = proto.Field(
        proto.ENUM,
        number=26,
        enum=gage_click_type.ClickTypeEnum.ClickType,
    )
    conversion_action: str = proto.Field(
        proto.STRING,
        number=113,
        optional=True,
    )
    conversion_action_category: (
        gage_conversion_action_category.ConversionActionCategoryEnum.ConversionActionCategory
    ) = proto.Field(
        proto.ENUM,
        number=53,
        enum=gage_conversion_action_category.ConversionActionCategoryEnum.ConversionActionCategory,
    )
    conversion_action_name: str = proto.Field(
        proto.STRING,
        number=114,
        optional=True,
    )
    conversion_adjustment: bool = proto.Field(
        proto.BOOL,
        number=115,
        optional=True,
    )
    conversion_attribution_event_type: (
        gage_conversion_attribution_event_type.ConversionAttributionEventTypeEnum.ConversionAttributionEventType
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_conversion_attribution_event_type.ConversionAttributionEventTypeEnum.ConversionAttributionEventType,
    )
    conversion_lag_bucket: (
        gage_conversion_lag_bucket.ConversionLagBucketEnum.ConversionLagBucket
    ) = proto.Field(
        proto.ENUM,
        number=50,
        enum=gage_conversion_lag_bucket.ConversionLagBucketEnum.ConversionLagBucket,
    )
    conversion_or_adjustment_lag_bucket: (
        gage_conversion_or_adjustment_lag_bucket.ConversionOrAdjustmentLagBucketEnum.ConversionOrAdjustmentLagBucket
    ) = proto.Field(
        proto.ENUM,
        number=51,
        enum=gage_conversion_or_adjustment_lag_bucket.ConversionOrAdjustmentLagBucketEnum.ConversionOrAdjustmentLagBucket,
    )
    date: str = proto.Field(
        proto.STRING,
        number=79,
        optional=True,
    )
    day_of_week: gage_day_of_week.DayOfWeekEnum.DayOfWeek = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_day_of_week.DayOfWeekEnum.DayOfWeek,
    )
    device: gage_device.DeviceEnum.Device = proto.Field(
        proto.ENUM,
        number=1,
        enum=gage_device.DeviceEnum.Device,
    )
    external_conversion_source: (
        gage_external_conversion_source.ExternalConversionSourceEnum.ExternalConversionSource
    ) = proto.Field(
        proto.ENUM,
        number=55,
        enum=gage_external_conversion_source.ExternalConversionSourceEnum.ExternalConversionSource,
    )
    geo_target_airport: str = proto.Field(
        proto.STRING,
        number=116,
        optional=True,
    )
    geo_target_canton: str = proto.Field(
        proto.STRING,
        number=117,
        optional=True,
    )
    geo_target_city: str = proto.Field(
        proto.STRING,
        number=118,
        optional=True,
    )
    geo_target_country: str = proto.Field(
        proto.STRING,
        number=119,
        optional=True,
    )
    geo_target_county: str = proto.Field(
        proto.STRING,
        number=120,
        optional=True,
    )
    geo_target_district: str = proto.Field(
        proto.STRING,
        number=121,
        optional=True,
    )
    geo_target_metro: str = proto.Field(
        proto.STRING,
        number=122,
        optional=True,
    )
    geo_target_most_specific_location: str = proto.Field(
        proto.STRING,
        number=123,
        optional=True,
    )
    geo_target_postal_code: str = proto.Field(
        proto.STRING,
        number=124,
        optional=True,
    )
    geo_target_province: str = proto.Field(
        proto.STRING,
        number=125,
        optional=True,
    )
    geo_target_region: str = proto.Field(
        proto.STRING,
        number=126,
        optional=True,
    )
    geo_target_state: str = proto.Field(
        proto.STRING,
        number=127,
        optional=True,
    )
    hotel_booking_window_days: int = proto.Field(
        proto.INT64,
        number=135,
        optional=True,
    )
    hotel_center_id: int = proto.Field(
        proto.INT64,
        number=80,
        optional=True,
    )
    hotel_check_in_date: str = proto.Field(
        proto.STRING,
        number=81,
        optional=True,
    )
    hotel_check_in_day_of_week: gage_day_of_week.DayOfWeekEnum.DayOfWeek = (
        proto.Field(
            proto.ENUM,
            number=9,
            enum=gage_day_of_week.DayOfWeekEnum.DayOfWeek,
        )
    )
    hotel_city: str = proto.Field(
        proto.STRING,
        number=82,
        optional=True,
    )
    hotel_class: int = proto.Field(
        proto.INT32,
        number=83,
        optional=True,
    )
    hotel_country: str = proto.Field(
        proto.STRING,
        number=84,
        optional=True,
    )
    hotel_date_selection_type: (
        gage_hotel_date_selection_type.HotelDateSelectionTypeEnum.HotelDateSelectionType
    ) = proto.Field(
        proto.ENUM,
        number=13,
        enum=gage_hotel_date_selection_type.HotelDateSelectionTypeEnum.HotelDateSelectionType,
    )
    hotel_length_of_stay: int = proto.Field(
        proto.INT32,
        number=85,
        optional=True,
    )
    hotel_rate_rule_id: str = proto.Field(
        proto.STRING,
        number=86,
        optional=True,
    )
    hotel_rate_type: gage_hotel_rate_type.HotelRateTypeEnum.HotelRateType = (
        proto.Field(
            proto.ENUM,
            number=74,
            enum=gage_hotel_rate_type.HotelRateTypeEnum.HotelRateType,
        )
    )
    hotel_price_bucket: (
        gage_hotel_price_bucket.HotelPriceBucketEnum.HotelPriceBucket
    ) = proto.Field(
        proto.ENUM,
        number=78,
        enum=gage_hotel_price_bucket.HotelPriceBucketEnum.HotelPriceBucket,
    )
    hotel_state: str = proto.Field(
        proto.STRING,
        number=87,
        optional=True,
    )
    hour: int = proto.Field(
        proto.INT32,
        number=88,
        optional=True,
    )
    interaction_on_this_extension: bool = proto.Field(
        proto.BOOL,
        number=89,
        optional=True,
    )
    keyword: "Keyword" = proto.Field(
        proto.MESSAGE,
        number=61,
        message="Keyword",
    )
    landing_page_source: (
        gage_landing_page_source.LandingPageSourceEnum.LandingPageSource
    ) = proto.Field(
        proto.ENUM,
        number=200,
        enum=gage_landing_page_source.LandingPageSourceEnum.LandingPageSource,
    )
    month: str = proto.Field(
        proto.STRING,
        number=90,
        optional=True,
    )
    month_of_year: gage_month_of_year.MonthOfYearEnum.MonthOfYear = proto.Field(
        proto.ENUM,
        number=18,
        enum=gage_month_of_year.MonthOfYearEnum.MonthOfYear,
    )
    partner_hotel_id: str = proto.Field(
        proto.STRING,
        number=91,
        optional=True,
    )
    product_aggregator_id: int = proto.Field(
        proto.INT64,
        number=132,
        optional=True,
    )
    product_category_level1: str = proto.Field(
        proto.STRING,
        number=161,
        optional=True,
    )
    product_category_level2: str = proto.Field(
        proto.STRING,
        number=162,
        optional=True,
    )
    product_category_level3: str = proto.Field(
        proto.STRING,
        number=163,
        optional=True,
    )
    product_category_level4: str = proto.Field(
        proto.STRING,
        number=164,
        optional=True,
    )
    product_category_level5: str = proto.Field(
        proto.STRING,
        number=165,
        optional=True,
    )
    product_brand: str = proto.Field(
        proto.STRING,
        number=97,
        optional=True,
    )
    product_channel: gage_product_channel.ProductChannelEnum.ProductChannel = (
        proto.Field(
            proto.ENUM,
            number=30,
            enum=gage_product_channel.ProductChannelEnum.ProductChannel,
        )
    )
    product_channel_exclusivity: (
        gage_product_channel_exclusivity.ProductChannelExclusivityEnum.ProductChannelExclusivity
    ) = proto.Field(
        proto.ENUM,
        number=31,
        enum=gage_product_channel_exclusivity.ProductChannelExclusivityEnum.ProductChannelExclusivity,
    )
    product_condition: (
        gage_product_condition.ProductConditionEnum.ProductCondition
    ) = proto.Field(
        proto.ENUM,
        number=32,
        enum=gage_product_condition.ProductConditionEnum.ProductCondition,
    )
    product_country: str = proto.Field(
        proto.STRING,
        number=98,
        optional=True,
    )
    product_custom_attribute0: str = proto.Field(
        proto.STRING,
        number=99,
        optional=True,
    )
    product_custom_attribute1: str = proto.Field(
        proto.STRING,
        number=100,
        optional=True,
    )
    product_custom_attribute2: str = proto.Field(
        proto.STRING,
        number=101,
        optional=True,
    )
    product_custom_attribute3: str = proto.Field(
        proto.STRING,
        number=102,
        optional=True,
    )
    product_custom_attribute4: str = proto.Field(
        proto.STRING,
        number=103,
        optional=True,
    )
    product_feed_label: str = proto.Field(
        proto.STRING,
        number=147,
        optional=True,
    )
    product_item_id: str = proto.Field(
        proto.STRING,
        number=104,
        optional=True,
    )
    product_language: str = proto.Field(
        proto.STRING,
        number=105,
        optional=True,
    )
    product_merchant_id: int = proto.Field(
        proto.INT64,
        number=133,
        optional=True,
    )
    product_store_id: str = proto.Field(
        proto.STRING,
        number=106,
        optional=True,
    )
    product_title: str = proto.Field(
        proto.STRING,
        number=107,
        optional=True,
    )
    product_type_l1: str = proto.Field(
        proto.STRING,
        number=108,
        optional=True,
    )
    product_type_l2: str = proto.Field(
        proto.STRING,
        number=109,
        optional=True,
    )
    product_type_l3: str = proto.Field(
        proto.STRING,
        number=110,
        optional=True,
    )
    product_type_l4: str = proto.Field(
        proto.STRING,
        number=111,
        optional=True,
    )
    product_type_l5: str = proto.Field(
        proto.STRING,
        number=112,
        optional=True,
    )
    quarter: str = proto.Field(
        proto.STRING,
        number=128,
        optional=True,
    )
    travel_destination_city: str = proto.Field(
        proto.STRING,
        number=193,
        optional=True,
    )
    travel_destination_country: str = proto.Field(
        proto.STRING,
        number=194,
        optional=True,
    )
    travel_destination_region: str = proto.Field(
        proto.STRING,
        number=195,
        optional=True,
    )
    recommendation_type: (
        gage_recommendation_type.RecommendationTypeEnum.RecommendationType
    ) = proto.Field(
        proto.ENUM,
        number=140,
        enum=gage_recommendation_type.RecommendationTypeEnum.RecommendationType,
    )
    search_engine_results_page_type: (
        gage_search_engine_results_page_type.SearchEngineResultsPageTypeEnum.SearchEngineResultsPageType
    ) = proto.Field(
        proto.ENUM,
        number=70,
        enum=gage_search_engine_results_page_type.SearchEngineResultsPageTypeEnum.SearchEngineResultsPageType,
    )
    search_subcategory: str = proto.Field(
        proto.STRING,
        number=155,
        optional=True,
    )
    search_term: str = proto.Field(
        proto.STRING,
        number=156,
        optional=True,
    )
    search_term_match_type: (
        gage_search_term_match_type.SearchTermMatchTypeEnum.SearchTermMatchType
    ) = proto.Field(
        proto.ENUM,
        number=22,
        enum=gage_search_term_match_type.SearchTermMatchTypeEnum.SearchTermMatchType,
    )
    match_type: gage_match_type.MatchTypeEnum.MatchType = proto.Field(
        proto.ENUM,
        number=199,
        enum=gage_match_type.MatchTypeEnum.MatchType,
    )
    slot: gage_slot.SlotEnum.Slot = proto.Field(
        proto.ENUM,
        number=23,
        enum=gage_slot.SlotEnum.Slot,
    )
    conversion_value_rule_primary_dimension: (
        gage_conversion_value_rule_primary_dimension.ConversionValueRulePrimaryDimensionEnum.ConversionValueRulePrimaryDimension
    ) = proto.Field(
        proto.ENUM,
        number=138,
        enum=gage_conversion_value_rule_primary_dimension.ConversionValueRulePrimaryDimensionEnum.ConversionValueRulePrimaryDimension,
    )
    webpage: str = proto.Field(
        proto.STRING,
        number=129,
        optional=True,
    )
    week: str = proto.Field(
        proto.STRING,
        number=130,
        optional=True,
    )
    year: int = proto.Field(
        proto.INT32,
        number=131,
        optional=True,
    )
    sk_ad_network_fine_conversion_value: int = proto.Field(
        proto.INT64,
        number=137,
        optional=True,
    )
    sk_ad_network_redistributed_fine_conversion_value: int = proto.Field(
        proto.INT64,
        number=190,
        optional=True,
    )
    sk_ad_network_user_type: (
        gage_sk_ad_network_user_type.SkAdNetworkUserTypeEnum.SkAdNetworkUserType
    ) = proto.Field(
        proto.ENUM,
        number=141,
        enum=gage_sk_ad_network_user_type.SkAdNetworkUserTypeEnum.SkAdNetworkUserType,
    )
    sk_ad_network_ad_event_type: (
        gage_sk_ad_network_ad_event_type.SkAdNetworkAdEventTypeEnum.SkAdNetworkAdEventType
    ) = proto.Field(
        proto.ENUM,
        number=142,
        enum=gage_sk_ad_network_ad_event_type.SkAdNetworkAdEventTypeEnum.SkAdNetworkAdEventType,
    )
    sk_ad_network_source_app: "SkAdNetworkSourceApp" = proto.Field(
        proto.MESSAGE,
        number=143,
        optional=True,
        message="SkAdNetworkSourceApp",
    )
    sk_ad_network_attribution_credit: (
        gage_sk_ad_network_attribution_credit.SkAdNetworkAttributionCreditEnum.SkAdNetworkAttributionCredit
    ) = proto.Field(
        proto.ENUM,
        number=144,
        enum=gage_sk_ad_network_attribution_credit.SkAdNetworkAttributionCreditEnum.SkAdNetworkAttributionCredit,
    )
    sk_ad_network_coarse_conversion_value: (
        gage_sk_ad_network_coarse_conversion_value.SkAdNetworkCoarseConversionValueEnum.SkAdNetworkCoarseConversionValue
    ) = proto.Field(
        proto.ENUM,
        number=151,
        enum=gage_sk_ad_network_coarse_conversion_value.SkAdNetworkCoarseConversionValueEnum.SkAdNetworkCoarseConversionValue,
    )
    sk_ad_network_source_domain: str = proto.Field(
        proto.STRING,
        number=152,
        optional=True,
    )
    sk_ad_network_source_type: (
        gage_sk_ad_network_source_type.SkAdNetworkSourceTypeEnum.SkAdNetworkSourceType
    ) = proto.Field(
        proto.ENUM,
        number=153,
        enum=gage_sk_ad_network_source_type.SkAdNetworkSourceTypeEnum.SkAdNetworkSourceType,
    )
    sk_ad_network_postback_sequence_index: int = proto.Field(
        proto.INT64,
        number=154,
        optional=True,
    )
    sk_ad_network_version: str = proto.Field(
        proto.STRING,
        number=192,
        optional=True,
    )
    asset_interaction_target: "AssetInteractionTarget" = proto.Field(
        proto.MESSAGE,
        number=139,
        optional=True,
        message="AssetInteractionTarget",
    )
    new_versus_returning_customers: (
        converting_user_prior_engagement_type_and_ltv_bucket.ConvertingUserPriorEngagementTypeAndLtvBucketEnum.ConvertingUserPriorEngagementTypeAndLtvBucket
    ) = proto.Field(
        proto.ENUM,
        number=160,
        enum=converting_user_prior_engagement_type_and_ltv_bucket.ConvertingUserPriorEngagementTypeAndLtvBucketEnum.ConvertingUserPriorEngagementTypeAndLtvBucket,
    )
    adjusted_age_range: age_range_type.AgeRangeTypeEnum.AgeRangeType = (
        proto.Field(
            proto.ENUM,
            number=196,
            enum=age_range_type.AgeRangeTypeEnum.AgeRangeType,
        )
    )
    adjusted_gender: gender_type.GenderTypeEnum.GenderType = proto.Field(
        proto.ENUM,
        number=197,
        enum=gender_type.GenderTypeEnum.GenderType,
    )
    search_term_match_source: (
        gage_search_term_match_source.SearchTermMatchSourceEnum.SearchTermMatchSource
    ) = proto.Field(
        proto.ENUM,
        number=198,
        enum=gage_search_term_match_source.SearchTermMatchSourceEnum.SearchTermMatchSource,
    )
    search_term_targeting_status: (
        gage_search_term_targeting_status.SearchTermTargetingStatusEnum.SearchTermTargetingStatus
    ) = proto.Field(
        proto.ENUM,
        number=201,
        enum=gage_search_term_targeting_status.SearchTermTargetingStatusEnum.SearchTermTargetingStatus,
    )
    ad_using_product_data: bool = proto.Field(
        proto.BOOL,
        number=202,
        optional=True,
    )
    ad_using_video: bool = proto.Field(
        proto.BOOL,
        number=203,
        optional=True,
    )


class Keyword(proto.Message):
    r"""A Keyword criterion segment.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        ad_group_criterion (str):
            The AdGroupCriterion resource name.

            This field is a member of `oneof`_ ``_ad_group_criterion``.
        info (google.ads.googleads.v22.common.types.KeywordInfo):
            Keyword info.
    """

    ad_group_criterion: str = proto.Field(
        proto.STRING,
        number=3,
        optional=True,
    )
    info: criteria.KeywordInfo = proto.Field(
        proto.MESSAGE,
        number=2,
        message=criteria.KeywordInfo,
    )


class BudgetCampaignAssociationStatus(proto.Message):
    r"""A BudgetCampaignAssociationStatus segment.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        campaign (str):
            The campaign resource name.

            This field is a member of `oneof`_ ``_campaign``.
        status (google.ads.googleads.v22.enums.types.BudgetCampaignAssociationStatusEnum.BudgetCampaignAssociationStatus):
            Budget campaign association status.
    """

    campaign: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )
    status: (
        gage_budget_campaign_association_status.BudgetCampaignAssociationStatusEnum.BudgetCampaignAssociationStatus
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_budget_campaign_association_status.BudgetCampaignAssociationStatusEnum.BudgetCampaignAssociationStatus,
    )


class AssetInteractionTarget(proto.Message):
    r"""An AssetInteractionTarget segment.

    Attributes:
        asset (str):
            The asset resource name.
        interaction_on_this_asset (bool):
            Only used with CustomerAsset, CampaignAsset
            and AdGroupAsset metrics. Indicates whether the
            interaction metrics occurred on the asset itself
            or a different asset or ad unit.
    """

    asset: str = proto.Field(
        proto.STRING,
        number=1,
    )
    interaction_on_this_asset: bool = proto.Field(
        proto.BOOL,
        number=2,
    )


class SkAdNetworkSourceApp(proto.Message):
    r"""A SkAdNetworkSourceApp segment.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        sk_ad_network_source_app_id (str):
            App id where the ad that drove the iOS Store
            Kit Ad Network install was shown.

            This field is a member of `oneof`_ ``_sk_ad_network_source_app_id``.
    """

    sk_ad_network_source_app_id: str = proto.Field(
        proto.STRING,
        number=1,
        optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

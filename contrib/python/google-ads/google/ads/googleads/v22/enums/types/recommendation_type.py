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


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "RecommendationTypeEnum",
    },
)


class RecommendationTypeEnum(proto.Message):
    r"""Container for enum describing types of recommendations."""

    class RecommendationType(proto.Enum):
        r"""Types of recommendations.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CAMPAIGN_BUDGET (2):
                Provides optimized budget recommendations for
                campaigns.
            KEYWORD (3):
                Keyword recommendation.
            TEXT_AD (4):
                Recommendation to add a new text ad.
            TARGET_CPA_OPT_IN (5):
                Recommendation to update a campaign to use a
                Target CPA bidding strategy.
            MAXIMIZE_CONVERSIONS_OPT_IN (6):
                Recommendation to update a campaign to use
                the Maximize Conversions bidding strategy.
            ENHANCED_CPC_OPT_IN (7):
                Recommendation to enable Enhanced Cost Per
                Click for a campaign.
            SEARCH_PARTNERS_OPT_IN (8):
                Recommendation to start showing your
                campaign's ads on Google Search Partners
                Websites.
            MAXIMIZE_CLICKS_OPT_IN (9):
                Recommendation to update a campaign to use a
                Maximize Clicks bidding strategy.
            OPTIMIZE_AD_ROTATION (10):
                Recommendation to start using the "Optimize"
                ad rotation setting for the given ad group.
            KEYWORD_MATCH_TYPE (14):
                Recommendation to change an existing keyword
                from one match type to a broader match type.
            MOVE_UNUSED_BUDGET (15):
                Recommendation to move unused budget from one
                budget to a constrained budget.
            FORECASTING_CAMPAIGN_BUDGET (16):
                Budget recommendation for campaigns that are expected to
                become budget-constrained in the future (as opposed to the
                CAMPAIGN_BUDGET recommendation, which applies to campaigns
                that are currently budget-constrained).
            TARGET_ROAS_OPT_IN (17):
                Recommendation to update a campaign to use a
                Target ROAS bidding strategy.
            RESPONSIVE_SEARCH_AD (18):
                Recommendation to add a new responsive search
                ad.
            MARGINAL_ROI_CAMPAIGN_BUDGET (19):
                Budget recommendation for campaigns whose ROI
                is predicted to increase with a budget
                adjustment.
            USE_BROAD_MATCH_KEYWORD (20):
                Recommendation to add broad match versions of
                keywords for fully automated conversion-based
                bidding campaigns.
            RESPONSIVE_SEARCH_AD_ASSET (21):
                Recommendation to add new responsive search
                ad assets.
            UPGRADE_SMART_SHOPPING_CAMPAIGN_TO_PERFORMANCE_MAX (22):
                Recommendation to upgrade a Smart Shopping
                campaign to a Performance Max campaign.
            RESPONSIVE_SEARCH_AD_IMPROVE_AD_STRENGTH (23):
                Recommendation to improve strength of
                responsive search ad.
            DISPLAY_EXPANSION_OPT_IN (24):
                Recommendation to update a campaign to use
                Display Expansion.
            UPGRADE_LOCAL_CAMPAIGN_TO_PERFORMANCE_MAX (25):
                Recommendation to upgrade a Local campaign to
                a Performance Max campaign.
            RAISE_TARGET_CPA_BID_TOO_LOW (26):
                Recommendation to raise target CPA when it is
                too low and there are very few or no
                conversions. It is applied asynchronously and
                can take minutes depending on the number of ad
                groups there are in the related campaign.
            FORECASTING_SET_TARGET_ROAS (27):
                Recommendation to raise the budget in advance
                of a seasonal event that is forecasted to
                increase traffic, and change bidding strategy
                from maximize conversion value to target ROAS.
            CALLOUT_ASSET (28):
                Recommendation to add callout assets to
                campaign or customer level.
            SITELINK_ASSET (29):
                Recommendation to add sitelink assets to
                campaign or customer level.
            CALL_ASSET (30):
                Recommendation to add call assets to campaign
                or customer level.
            SHOPPING_ADD_AGE_GROUP (31):
                Recommendation to add the age group attribute
                to offers that are demoted because of a missing
                age group.
            SHOPPING_ADD_COLOR (32):
                Recommendation to add a color to offers that
                are demoted because of a missing color.
            SHOPPING_ADD_GENDER (33):
                Recommendation to add a gender to offers that
                are demoted because of a missing gender.
            SHOPPING_ADD_GTIN (34):
                Recommendation to add a GTIN (Global Trade
                Item Number) to offers that are demoted because
                of a missing GTIN.
            SHOPPING_ADD_MORE_IDENTIFIERS (35):
                Recommendation to add more identifiers to
                offers that are demoted because of missing
                identifiers.
            SHOPPING_ADD_SIZE (36):
                Recommendation to add the size to offers that
                are demoted because of a missing size.
            SHOPPING_ADD_PRODUCTS_TO_CAMPAIGN (37):
                Recommendation informing a customer about a
                campaign that cannot serve because no products
                are being targeted.
            SHOPPING_FIX_DISAPPROVED_PRODUCTS (38):
                The shopping recommendation informing a
                customer about campaign with a high percentage
                of disapproved products.
            SHOPPING_TARGET_ALL_OFFERS (39):
                Recommendation to create a catch-all campaign
                that targets all offers.
            SHOPPING_FIX_SUSPENDED_MERCHANT_CENTER_ACCOUNT (40):
                Recommendation to fix Merchant Center account
                suspension issues.
            SHOPPING_FIX_MERCHANT_CENTER_ACCOUNT_SUSPENSION_WARNING (41):
                Recommendation to fix Merchant Center account
                suspension warning issues.
            SHOPPING_MIGRATE_REGULAR_SHOPPING_CAMPAIGN_OFFERS_TO_PERFORMANCE_MAX (42):
                Recommendation to migrate offers targeted by
                Regular Shopping Campaigns to existing
                Performance Max campaigns.
            DYNAMIC_IMAGE_EXTENSION_OPT_IN (43):
                Recommendation to enable dynamic image
                extensions on the account, allowing Google to
                find the best images from ad landing pages and
                complement text ads.
            RAISE_TARGET_CPA (44):
                Recommendation to raise Target CPA based on
                Google predictions modeled from past
                conversions. It is applied asynchronously and
                can take minutes depending on the number of ad
                groups there are in the related campaign.
            LOWER_TARGET_ROAS (45):
                Recommendation to lower Target ROAS.
            PERFORMANCE_MAX_OPT_IN (46):
                Recommendation to opt into Performance Max
                campaigns.
            IMPROVE_PERFORMANCE_MAX_AD_STRENGTH (47):
                Recommendation to improve the asset group
                strength of a Performance Max campaign to an
                "Excellent" rating.
            MIGRATE_DYNAMIC_SEARCH_ADS_CAMPAIGN_TO_PERFORMANCE_MAX (48):
                Recommendation to migrate Dynamic Search Ads
                to Performance Max campaigns.
            FORECASTING_SET_TARGET_CPA (49):
                Recommendation to set a target CPA for
                campaigns that do not have one specified, in
                advance of a seasonal event that is forecasted
                to increase traffic.
            SET_TARGET_CPA (50):
                Recommendation to set a target CPA for
                campaigns that do not have one specified.
            SET_TARGET_ROAS (51):
                Recommendation to set a target ROAS for
                campaigns that do not have one specified.
            MAXIMIZE_CONVERSION_VALUE_OPT_IN (52):
                Recommendation to update a campaign to use
                the Maximize Conversion Value bidding strategy.
            IMPROVE_GOOGLE_TAG_COVERAGE (53):
                Recommendation to deploy Google Tag on more
                pages.
            PERFORMANCE_MAX_FINAL_URL_OPT_IN (54):
                Recommendation to turn on Final URL expansion
                for your Performance Max campaigns.
            REFRESH_CUSTOMER_MATCH_LIST (55):
                Recommendation to update a customer list that
                hasn't been updated in the last 90 days.
            CUSTOM_AUDIENCE_OPT_IN (56):
                Recommendation to create a custom audience.
            LEAD_FORM_ASSET (57):
                Recommendation to add lead form assets to
                campaign or customer level.
            IMPROVE_DEMAND_GEN_AD_STRENGTH (58):
                Recommendation to improve the strength of ads
                in Demand Gen campaigns.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_BUDGET = 2
        KEYWORD = 3
        TEXT_AD = 4
        TARGET_CPA_OPT_IN = 5
        MAXIMIZE_CONVERSIONS_OPT_IN = 6
        ENHANCED_CPC_OPT_IN = 7
        SEARCH_PARTNERS_OPT_IN = 8
        MAXIMIZE_CLICKS_OPT_IN = 9
        OPTIMIZE_AD_ROTATION = 10
        KEYWORD_MATCH_TYPE = 14
        MOVE_UNUSED_BUDGET = 15
        FORECASTING_CAMPAIGN_BUDGET = 16
        TARGET_ROAS_OPT_IN = 17
        RESPONSIVE_SEARCH_AD = 18
        MARGINAL_ROI_CAMPAIGN_BUDGET = 19
        USE_BROAD_MATCH_KEYWORD = 20
        RESPONSIVE_SEARCH_AD_ASSET = 21
        UPGRADE_SMART_SHOPPING_CAMPAIGN_TO_PERFORMANCE_MAX = 22
        RESPONSIVE_SEARCH_AD_IMPROVE_AD_STRENGTH = 23
        DISPLAY_EXPANSION_OPT_IN = 24
        UPGRADE_LOCAL_CAMPAIGN_TO_PERFORMANCE_MAX = 25
        RAISE_TARGET_CPA_BID_TOO_LOW = 26
        FORECASTING_SET_TARGET_ROAS = 27
        CALLOUT_ASSET = 28
        SITELINK_ASSET = 29
        CALL_ASSET = 30
        SHOPPING_ADD_AGE_GROUP = 31
        SHOPPING_ADD_COLOR = 32
        SHOPPING_ADD_GENDER = 33
        SHOPPING_ADD_GTIN = 34
        SHOPPING_ADD_MORE_IDENTIFIERS = 35
        SHOPPING_ADD_SIZE = 36
        SHOPPING_ADD_PRODUCTS_TO_CAMPAIGN = 37
        SHOPPING_FIX_DISAPPROVED_PRODUCTS = 38
        SHOPPING_TARGET_ALL_OFFERS = 39
        SHOPPING_FIX_SUSPENDED_MERCHANT_CENTER_ACCOUNT = 40
        SHOPPING_FIX_MERCHANT_CENTER_ACCOUNT_SUSPENSION_WARNING = 41
        SHOPPING_MIGRATE_REGULAR_SHOPPING_CAMPAIGN_OFFERS_TO_PERFORMANCE_MAX = (
            42
        )
        DYNAMIC_IMAGE_EXTENSION_OPT_IN = 43
        RAISE_TARGET_CPA = 44
        LOWER_TARGET_ROAS = 45
        PERFORMANCE_MAX_OPT_IN = 46
        IMPROVE_PERFORMANCE_MAX_AD_STRENGTH = 47
        MIGRATE_DYNAMIC_SEARCH_ADS_CAMPAIGN_TO_PERFORMANCE_MAX = 48
        FORECASTING_SET_TARGET_CPA = 49
        SET_TARGET_CPA = 50
        SET_TARGET_ROAS = 51
        MAXIMIZE_CONVERSION_VALUE_OPT_IN = 52
        IMPROVE_GOOGLE_TAG_COVERAGE = 53
        PERFORMANCE_MAX_FINAL_URL_OPT_IN = 54
        REFRESH_CUSTOMER_MATCH_LIST = 55
        CUSTOM_AUDIENCE_OPT_IN = 56
        LEAD_FORM_ASSET = 57
        IMPROVE_DEMAND_GEN_AD_STRENGTH = 58


__all__ = tuple(sorted(__protobuf__.manifest))

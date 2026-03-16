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
        "ResourceLimitTypeEnum",
    },
)


class ResourceLimitTypeEnum(proto.Message):
    r"""Container for enum describing possible resource limit types."""

    class ResourceLimitType(proto.Enum):
        r"""Resource limit type.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents an
                unclassified operation unknown in this version.
            CAMPAIGNS_PER_CUSTOMER (2):
                Number of ENABLED and PAUSED campaigns per
                customer.
            BASE_CAMPAIGNS_PER_CUSTOMER (3):
                Number of ENABLED and PAUSED base campaigns
                per customer.
            EXPERIMENT_CAMPAIGNS_PER_CUSTOMER (105):
                Number of ENABLED and PAUSED experiment
                campaigns per customer.
            HOTEL_CAMPAIGNS_PER_CUSTOMER (4):
                Number of ENABLED and PAUSED Hotel campaigns
                per customer.
            SMART_SHOPPING_CAMPAIGNS_PER_CUSTOMER (5):
                Number of ENABLED and PAUSED Smart Shopping
                campaigns per customer.
            AD_GROUPS_PER_CAMPAIGN (6):
                Number of ENABLED ad groups per campaign.
            AD_GROUPS_PER_SHOPPING_CAMPAIGN (8):
                Number of ENABLED ad groups per Shopping
                campaign.
            AD_GROUPS_PER_HOTEL_CAMPAIGN (9):
                Number of ENABLED ad groups per Hotel
                campaign.
            REPORTING_AD_GROUPS_PER_LOCAL_CAMPAIGN (10):
                Number of ENABLED reporting ad groups per
                local campaign.
            REPORTING_AD_GROUPS_PER_APP_CAMPAIGN (11):
                Number of ENABLED reporting ad groups per App
                campaign. It includes app campaign and app
                campaign for engagement.
            MANAGED_AD_GROUPS_PER_SMART_CAMPAIGN (52):
                Number of ENABLED managed ad groups per smart
                campaign.
            AD_GROUP_CRITERIA_PER_CUSTOMER (12):
                Number of ENABLED ad group criteria per
                customer. An ad group criterion is considered as
                ENABLED if:

                1. it's not REMOVED
                2. its ad group is not REMOVED
                3. its campaign is not REMOVED.
            BASE_AD_GROUP_CRITERIA_PER_CUSTOMER (13):
                Number of ad group criteria across all base
                campaigns for a customer.
            EXPERIMENT_AD_GROUP_CRITERIA_PER_CUSTOMER (107):
                Number of ad group criteria across all
                experiment campaigns for a customer.
            AD_GROUP_CRITERIA_PER_CAMPAIGN (14):
                Number of ENABLED ad group criteria per
                campaign. An ad group criterion is considered as
                ENABLED if:

                1. it's not REMOVED
                2. its ad group is not REMOVED.
            CAMPAIGN_CRITERIA_PER_CUSTOMER (15):
                Number of ENABLED campaign criteria per
                customer.
            BASE_CAMPAIGN_CRITERIA_PER_CUSTOMER (16):
                Number of ENABLED campaign criteria across
                all base campaigns for a customer.
            EXPERIMENT_CAMPAIGN_CRITERIA_PER_CUSTOMER (108):
                Number of ENABLED campaign criteria across
                all experiment campaigns for a customer.
            WEBPAGE_CRITERIA_PER_CUSTOMER (17):
                Number of ENABLED webpage criteria per
                customer, including campaign level and ad group
                level.
            BASE_WEBPAGE_CRITERIA_PER_CUSTOMER (18):
                Number of ENABLED webpage criteria across all
                base campaigns for a customer.
            EXPERIMENT_WEBPAGE_CRITERIA_PER_CUSTOMER (19):
                Meximum number of ENABLED webpage criteria
                across all experiment campaigns for a customer.
            COMBINED_AUDIENCE_CRITERIA_PER_AD_GROUP (20):
                Number of combined audience criteria per ad
                group.
            CUSTOMER_NEGATIVE_PLACEMENT_CRITERIA_PER_CUSTOMER (21):
                Limit for placement criterion type group in
                customer negative criterion.
            CUSTOMER_NEGATIVE_YOUTUBE_CHANNEL_CRITERIA_PER_CUSTOMER (22):
                Limit for YouTube TV channels in customer
                negative criterion.
            CRITERIA_PER_AD_GROUP (23):
                Number of ENABLED criteria per ad group.
            LISTING_GROUPS_PER_AD_GROUP (24):
                Number of listing group criteria per ad
                group.
            EXPLICITLY_SHARED_BUDGETS_PER_CUSTOMER (25):
                Number of ENABLED explicitly shared budgets
                per customer.
            IMPLICITLY_SHARED_BUDGETS_PER_CUSTOMER (26):
                Number of ENABLED implicitly shared budgets
                per customer.
            COMBINED_AUDIENCE_CRITERIA_PER_CAMPAIGN (27):
                Number of combined audience criteria per
                campaign.
            NEGATIVE_KEYWORDS_PER_CAMPAIGN (28):
                Number of negative keywords per campaign.
            NEGATIVE_PLACEMENTS_PER_CAMPAIGN (29):
                Number of excluded campaign criteria in
                placement dimension, for example, placement,
                mobile application, YouTube channel, etc. The
                API criterion type is NOT limited to placement
                only, and this does not include exclusions at
                the ad group or other levels.
            GEO_TARGETS_PER_CAMPAIGN (30):
                Number of geo targets per campaign.
            NEGATIVE_IP_BLOCKS_PER_CAMPAIGN (32):
                Number of negative IP blocks per campaign.
            PROXIMITIES_PER_CAMPAIGN (33):
                Number of proximity targets per campaign.
            LISTING_SCOPES_PER_SHOPPING_CAMPAIGN (34):
                Number of listing scopes per Shopping
                campaign.
            LISTING_SCOPES_PER_NON_SHOPPING_CAMPAIGN (35):
                Number of listing scopes per non-Shopping
                campaign.
            NEGATIVE_KEYWORDS_PER_SHARED_SET (36):
                Number of criteria per negative keyword
                shared set.
            NEGATIVE_PLACEMENTS_PER_SHARED_SET (37):
                Number of criteria per negative placement
                shared set.
            SHARED_SETS_PER_CUSTOMER_FOR_TYPE_DEFAULT (40):
                Default number of shared sets allowed per
                type per customer.
            SHARED_SETS_PER_CUSTOMER_FOR_NEGATIVE_PLACEMENT_LIST_LOWER (41):
                Number of shared sets of negative placement
                list type for a manager customer.
            HOTEL_ADVANCE_BOOKING_WINDOW_BID_MODIFIERS_PER_AD_GROUP (44):
                Number of hotel_advance_booking_window bid modifiers per ad
                group.
            BIDDING_STRATEGIES_PER_CUSTOMER (45):
                Number of ENABLED shared bidding strategies
                per customer.
            BASIC_USER_LISTS_PER_CUSTOMER (47):
                Number of open basic user lists per customer.
            LOGICAL_USER_LISTS_PER_CUSTOMER (48):
                Number of open logical user lists per
                customer.
            RULE_BASED_USER_LISTS_PER_CUSTOMER (153):
                Number of open rule based user lists per
                customer.
            BASE_AD_GROUP_ADS_PER_CUSTOMER (53):
                Number of ENABLED and PAUSED ad group ads
                across all base campaigns for a customer.
            EXPERIMENT_AD_GROUP_ADS_PER_CUSTOMER (54):
                Number of ENABLED and PAUSED ad group ads
                across all experiment campaigns for a customer.
            AD_GROUP_ADS_PER_CAMPAIGN (55):
                Number of ENABLED and PAUSED ad group ads per
                campaign.
            TEXT_AND_OTHER_ADS_PER_AD_GROUP (56):
                Number of ENABLED ads per ad group that do
                not fall in to other buckets. Includes text and
                many other types.
            IMAGE_ADS_PER_AD_GROUP (57):
                Number of ENABLED image ads per ad group.
            SHOPPING_SMART_ADS_PER_AD_GROUP (58):
                Number of ENABLED shopping smart ads per ad
                group.
            RESPONSIVE_SEARCH_ADS_PER_AD_GROUP (59):
                Number of ENABLED responsive search ads per
                ad group.
            APP_ADS_PER_AD_GROUP (60):
                Number of ENABLED app ads per ad group.
            APP_ENGAGEMENT_ADS_PER_AD_GROUP (61):
                Number of ENABLED app engagement ads per ad
                group.
            LOCAL_ADS_PER_AD_GROUP (62):
                Number of ENABLED local ads per ad group.
            VIDEO_ADS_PER_AD_GROUP (63):
                Number of ENABLED video ads per ad group.
            LEAD_FORM_CAMPAIGN_ASSETS_PER_CAMPAIGN (143):
                Number of ENABLED lead form CampaignAssets
                per campaign.
            PROMOTION_CUSTOMER_ASSETS_PER_CUSTOMER (79):
                Number of ENABLED promotion CustomerAssets
                per customer.
            PROMOTION_CAMPAIGN_ASSETS_PER_CAMPAIGN (80):
                Number of ENABLED promotion CampaignAssets
                per campaign.
            PROMOTION_AD_GROUP_ASSETS_PER_AD_GROUP (81):
                Number of ENABLED promotion AdGroupAssets per
                ad group.
            CALLOUT_CUSTOMER_ASSETS_PER_CUSTOMER (134):
                Number of ENABLED callout CustomerAssets per
                customer.
            CALLOUT_CAMPAIGN_ASSETS_PER_CAMPAIGN (135):
                Number of ENABLED callout CampaignAssets per
                campaign.
            CALLOUT_AD_GROUP_ASSETS_PER_AD_GROUP (136):
                Number of ENABLED callout AdGroupAssets per
                ad group.
            SITELINK_CUSTOMER_ASSETS_PER_CUSTOMER (137):
                Number of ENABLED sitelink CustomerAssets per
                customer.
            SITELINK_CAMPAIGN_ASSETS_PER_CAMPAIGN (138):
                Number of ENABLED sitelink CampaignAssets per
                campaign.
            SITELINK_AD_GROUP_ASSETS_PER_AD_GROUP (139):
                Number of ENABLED sitelink AdGroupAssets per
                ad group.
            STRUCTURED_SNIPPET_CUSTOMER_ASSETS_PER_CUSTOMER (140):
                Number of ENABLED structured snippet
                CustomerAssets per customer.
            STRUCTURED_SNIPPET_CAMPAIGN_ASSETS_PER_CAMPAIGN (141):
                Number of ENABLED structured snippet
                CampaignAssets per campaign.
            STRUCTURED_SNIPPET_AD_GROUP_ASSETS_PER_AD_GROUP (142):
                Number of ENABLED structured snippet
                AdGroupAssets per ad group.
            MOBILE_APP_CUSTOMER_ASSETS_PER_CUSTOMER (144):
                Number of ENABLED mobile app CustomerAssets
                per customer.
            MOBILE_APP_CAMPAIGN_ASSETS_PER_CAMPAIGN (145):
                Number of ENABLED mobile app CampaignAssets
                per campaign.
            MOBILE_APP_AD_GROUP_ASSETS_PER_AD_GROUP (146):
                Number of ENABLED mobile app AdGroupAssets
                per ad group.
            HOTEL_CALLOUT_CUSTOMER_ASSETS_PER_CUSTOMER (147):
                Number of ENABLED hotel callout
                CustomerAssets per customer.
            HOTEL_CALLOUT_CAMPAIGN_ASSETS_PER_CAMPAIGN (148):
                Number of ENABLED hotel callout
                CampaignAssets per campaign.
            HOTEL_CALLOUT_AD_GROUP_ASSETS_PER_AD_GROUP (149):
                Number of ENABLED hotel callout AdGroupAssets
                per ad group.
            CALL_CUSTOMER_ASSETS_PER_CUSTOMER (150):
                Number of ENABLED call CustomerAssets per
                customer.
            CALL_CAMPAIGN_ASSETS_PER_CAMPAIGN (151):
                Number of ENABLED call CampaignAssets per
                campaign.
            CALL_AD_GROUP_ASSETS_PER_AD_GROUP (152):
                Number of ENABLED call AdGroupAssets per ad
                group.
            PRICE_CUSTOMER_ASSETS_PER_CUSTOMER (154):
                Number of ENABLED price CustomerAssets per
                customer.
            PRICE_CAMPAIGN_ASSETS_PER_CAMPAIGN (155):
                Number of ENABLED price CampaignAssets per
                campaign.
            PRICE_AD_GROUP_ASSETS_PER_AD_GROUP (156):
                Number of ENABLED price AdGroupAssets per ad
                group.
            AD_IMAGE_CAMPAIGN_ASSETS_PER_CAMPAIGN (175):
                Number of ENABLED ad image CampaignAssets per
                campaign.
            AD_IMAGE_AD_GROUP_ASSETS_PER_AD_GROUP (176):
                Number of ENABLED ad image AdGroupAssets per
                ad group.
            PAGE_FEED_ASSET_SETS_PER_CUSTOMER (157):
                Number of ENABLED page feed asset sets per
                customer.
            DYNAMIC_EDUCATION_FEED_ASSET_SETS_PER_CUSTOMER (158):
                Number of ENABLED dynamic education feed
                asset sets per customer.
            ASSETS_PER_PAGE_FEED_ASSET_SET (159):
                Number of ENABLED assets per page feed asset
                set.
            ASSETS_PER_DYNAMIC_EDUCATION_FEED_ASSET_SET (160):
                Number of ENABLED assets per dynamic
                education asset set.
            DYNAMIC_REAL_ESTATE_ASSET_SETS_PER_CUSTOMER (161):
                Number of ENABLED dynamic real estate asset
                sets per customer.
            ASSETS_PER_DYNAMIC_REAL_ESTATE_ASSET_SET (162):
                Number of ENABLED assets per dynamic real
                estate asset set.
            DYNAMIC_CUSTOM_ASSET_SETS_PER_CUSTOMER (163):
                Number of ENABLED dynamic custom asset sets
                per customer.
            ASSETS_PER_DYNAMIC_CUSTOM_ASSET_SET (164):
                Number of ENABLED assets per dynamic custom
                asset set.
            DYNAMIC_HOTELS_AND_RENTALS_ASSET_SETS_PER_CUSTOMER (165):
                Number of ENABLED dynamic hotels and rentals
                asset sets per customer.
            ASSETS_PER_DYNAMIC_HOTELS_AND_RENTALS_ASSET_SET (166):
                Number of ENABLED assets per dynamic hotels
                and rentals asset set.
            DYNAMIC_LOCAL_ASSET_SETS_PER_CUSTOMER (167):
                Number of ENABLED dynamic local asset sets
                per customer.
            ASSETS_PER_DYNAMIC_LOCAL_ASSET_SET (168):
                Number of ENABLED assets per dynamic local
                asset set.
            DYNAMIC_FLIGHTS_ASSET_SETS_PER_CUSTOMER (169):
                Number of ENABLED dynamic flights asset sets
                per customer.
            ASSETS_PER_DYNAMIC_FLIGHTS_ASSET_SET (170):
                Number of ENABLED assets per dynamic flights
                asset set.
            DYNAMIC_TRAVEL_ASSET_SETS_PER_CUSTOMER (171):
                Number of ENABLED dynamic travel asset sets
                per customer.
            ASSETS_PER_DYNAMIC_TRAVEL_ASSET_SET (172):
                Number of ENABLED assets per dynamic travel
                asset set.
            DYNAMIC_JOBS_ASSET_SETS_PER_CUSTOMER (173):
                Number of ENABLED dynamic jobs asset sets per
                customer.
            ASSETS_PER_DYNAMIC_JOBS_ASSET_SET (174):
                Number of ENABLED assets per dynamic jobs
                asset set.
            BUSINESS_NAME_CAMPAIGN_ASSETS_PER_CAMPAIGN (179):
                Number of ENABLED business name
                CampaignAssets per campaign.
            BUSINESS_LOGO_CAMPAIGN_ASSETS_PER_CAMPAIGN (180):
                Number of ENABLED business logo
                CampaignAssets per campaign.
            VERSIONS_PER_AD (82):
                Number of versions per ad.
            USER_FEEDS_PER_CUSTOMER (90):
                Number of ENABLED user feeds per customer.
            SYSTEM_FEEDS_PER_CUSTOMER (91):
                Number of ENABLED system feeds per customer.
            FEED_ATTRIBUTES_PER_FEED (92):
                Number of feed attributes per feed.
            FEED_ITEMS_PER_CUSTOMER (94):
                Number of ENABLED feed items per customer.
            CAMPAIGN_FEEDS_PER_CUSTOMER (95):
                Number of ENABLED campaign feeds per
                customer.
            BASE_CAMPAIGN_FEEDS_PER_CUSTOMER (96):
                Number of ENABLED campaign feeds across all
                base campaigns for a customer.
            EXPERIMENT_CAMPAIGN_FEEDS_PER_CUSTOMER (109):
                Number of ENABLED campaign feeds across all
                experiment campaigns for a customer.
            AD_GROUP_FEEDS_PER_CUSTOMER (97):
                Number of ENABLED ad group feeds per
                customer.
            BASE_AD_GROUP_FEEDS_PER_CUSTOMER (98):
                Number of ENABLED ad group feeds across all
                base campaigns for a customer.
            EXPERIMENT_AD_GROUP_FEEDS_PER_CUSTOMER (110):
                Number of ENABLED ad group feeds across all
                experiment campaigns for a customer.
            AD_GROUP_FEEDS_PER_CAMPAIGN (99):
                Number of ENABLED ad group feeds per
                campaign.
            FEED_ITEM_SETS_PER_CUSTOMER (100):
                Number of ENABLED feed items per customer.
            FEED_ITEMS_PER_FEED_ITEM_SET (101):
                Number of feed items per feed item set.
            CAMPAIGN_EXPERIMENTS_PER_CUSTOMER (112):
                Number of ENABLED campaign experiments per
                customer.
            EXPERIMENT_ARMS_PER_VIDEO_EXPERIMENT (113):
                Number of video experiment arms per
                experiment.
            OWNED_LABELS_PER_CUSTOMER (115):
                Number of owned labels per customer.
            LABELS_PER_CAMPAIGN (117):
                Number of applied labels per campaign.
            LABELS_PER_AD_GROUP (118):
                Number of applied labels per ad group.
            LABELS_PER_AD_GROUP_AD (119):
                Number of applied labels per ad group ad.
            LABELS_PER_AD_GROUP_CRITERION (120):
                Number of applied labels per ad group
                criterion.
            TARGET_CUSTOMERS_PER_LABEL (121):
                Number of customers with a single label
                applied.
            KEYWORD_PLANS_PER_USER_PER_CUSTOMER (122):
                Number of ENABLED keyword plans per user per
                customer. The limit is applied per <user,
                customer> pair because by default a plan is
                private to a user of a customer. Each user of a
                customer has their own independent limit.
            KEYWORD_PLAN_AD_GROUP_KEYWORDS_PER_KEYWORD_PLAN (123):
                Number of keyword plan ad group keywords per
                keyword plan.
            KEYWORD_PLAN_AD_GROUPS_PER_KEYWORD_PLAN (124):
                Number of keyword plan ad groups per keyword
                plan.
            KEYWORD_PLAN_NEGATIVE_KEYWORDS_PER_KEYWORD_PLAN (125):
                Number of keyword plan negative keywords
                (both campaign and ad group) per keyword plan.
            KEYWORD_PLAN_CAMPAIGNS_PER_KEYWORD_PLAN (126):
                Number of keyword plan campaigns per keyword
                plan.
            CONVERSION_ACTIONS_PER_CUSTOMER (128):
                Number of ENABLED conversion actions per
                customer.
            BATCH_JOB_OPERATIONS_PER_JOB (130):
                Number of operations in a single batch job.
            BATCH_JOBS_PER_CUSTOMER (131):
                Number of PENDING or ENABLED batch jobs per
                customer.
            HOTEL_CHECK_IN_DATE_RANGE_BID_MODIFIERS_PER_AD_GROUP (132):
                Number of hotel check-in date range bid
                modifiers per ad agroup.
            SHARED_SETS_PER_ACCOUNT_FOR_ACCOUNT_LEVEL_NEGATIVE_KEYWORDS (177):
                Number of shared sets of type
                ACCOUNT_LEVEL_NEGATIVE_KEYWORDS per account.
            ACCOUNT_LEVEL_NEGATIVE_KEYWORDS_PER_SHARED_SET (178):
                Number of keywords per ACCOUNT_LEVEL_NEGATIVE_KEYWORDS
                shared set.
            ENABLED_ASSET_PER_HOTEL_PROPERTY_ASSET_SET (181):
                Maximum number of asset per hotel property
                asset set.
            ENABLED_HOTEL_PROPERTY_ASSET_LINKS_PER_ASSET_GROUP (182):
                Maximum number of enabled hotel property
                assets per asset group.
            BRANDS_PER_SHARED_SET (183):
                Number of criteria per brand shared set.
            ENABLED_BRAND_LIST_CRITERIA_PER_CAMPAIGN (184):
                Number of active brand list criteria per
                campaign.
            SHARED_SETS_PER_ACCOUNT_FOR_BRAND (185):
                Maximum number of shared sets of brand type
                for an account.
            LOOKALIKE_USER_LISTS_PER_CUSTOMER (186):
                Maximum number of lookalike lists per
                customer.
            LOGO_CAMPAIGN_ASSETS_PER_CAMPAIGN (187):
                Total number of enabled IMAGE CampaignAssets with LOGO and
                LANDSCAPE_LOGO field types per campaign.
            BUSINESS_MESSAGE_ASSET_LINKS_PER_CUSTOMER (188):
                Maximum number of active business message
                asset links at customer level.
            WHATSAPP_BUSINESS_MESSAGE_ASSET_LINKS_PER_CAMPAIGN (189):
                Maximum number of active WhatsApp business
                message asset links at campaign level.
            WHATSAPP_BUSINESS_MESSAGE_ASSET_LINKS_PER_AD_GROUP (190):
                Maximum number of active WhatsApp business
                message asset links at ad group level.
            BRAND_LIST_CRITERIA_PER_AD_GROUP (193):
                Number of ENABLED brand list criteria per ad
                group.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGNS_PER_CUSTOMER = 2
        BASE_CAMPAIGNS_PER_CUSTOMER = 3
        EXPERIMENT_CAMPAIGNS_PER_CUSTOMER = 105
        HOTEL_CAMPAIGNS_PER_CUSTOMER = 4
        SMART_SHOPPING_CAMPAIGNS_PER_CUSTOMER = 5
        AD_GROUPS_PER_CAMPAIGN = 6
        AD_GROUPS_PER_SHOPPING_CAMPAIGN = 8
        AD_GROUPS_PER_HOTEL_CAMPAIGN = 9
        REPORTING_AD_GROUPS_PER_LOCAL_CAMPAIGN = 10
        REPORTING_AD_GROUPS_PER_APP_CAMPAIGN = 11
        MANAGED_AD_GROUPS_PER_SMART_CAMPAIGN = 52
        AD_GROUP_CRITERIA_PER_CUSTOMER = 12
        BASE_AD_GROUP_CRITERIA_PER_CUSTOMER = 13
        EXPERIMENT_AD_GROUP_CRITERIA_PER_CUSTOMER = 107
        AD_GROUP_CRITERIA_PER_CAMPAIGN = 14
        CAMPAIGN_CRITERIA_PER_CUSTOMER = 15
        BASE_CAMPAIGN_CRITERIA_PER_CUSTOMER = 16
        EXPERIMENT_CAMPAIGN_CRITERIA_PER_CUSTOMER = 108
        WEBPAGE_CRITERIA_PER_CUSTOMER = 17
        BASE_WEBPAGE_CRITERIA_PER_CUSTOMER = 18
        EXPERIMENT_WEBPAGE_CRITERIA_PER_CUSTOMER = 19
        COMBINED_AUDIENCE_CRITERIA_PER_AD_GROUP = 20
        CUSTOMER_NEGATIVE_PLACEMENT_CRITERIA_PER_CUSTOMER = 21
        CUSTOMER_NEGATIVE_YOUTUBE_CHANNEL_CRITERIA_PER_CUSTOMER = 22
        CRITERIA_PER_AD_GROUP = 23
        LISTING_GROUPS_PER_AD_GROUP = 24
        EXPLICITLY_SHARED_BUDGETS_PER_CUSTOMER = 25
        IMPLICITLY_SHARED_BUDGETS_PER_CUSTOMER = 26
        COMBINED_AUDIENCE_CRITERIA_PER_CAMPAIGN = 27
        NEGATIVE_KEYWORDS_PER_CAMPAIGN = 28
        NEGATIVE_PLACEMENTS_PER_CAMPAIGN = 29
        GEO_TARGETS_PER_CAMPAIGN = 30
        NEGATIVE_IP_BLOCKS_PER_CAMPAIGN = 32
        PROXIMITIES_PER_CAMPAIGN = 33
        LISTING_SCOPES_PER_SHOPPING_CAMPAIGN = 34
        LISTING_SCOPES_PER_NON_SHOPPING_CAMPAIGN = 35
        NEGATIVE_KEYWORDS_PER_SHARED_SET = 36
        NEGATIVE_PLACEMENTS_PER_SHARED_SET = 37
        SHARED_SETS_PER_CUSTOMER_FOR_TYPE_DEFAULT = 40
        SHARED_SETS_PER_CUSTOMER_FOR_NEGATIVE_PLACEMENT_LIST_LOWER = 41
        HOTEL_ADVANCE_BOOKING_WINDOW_BID_MODIFIERS_PER_AD_GROUP = 44
        BIDDING_STRATEGIES_PER_CUSTOMER = 45
        BASIC_USER_LISTS_PER_CUSTOMER = 47
        LOGICAL_USER_LISTS_PER_CUSTOMER = 48
        RULE_BASED_USER_LISTS_PER_CUSTOMER = 153
        BASE_AD_GROUP_ADS_PER_CUSTOMER = 53
        EXPERIMENT_AD_GROUP_ADS_PER_CUSTOMER = 54
        AD_GROUP_ADS_PER_CAMPAIGN = 55
        TEXT_AND_OTHER_ADS_PER_AD_GROUP = 56
        IMAGE_ADS_PER_AD_GROUP = 57
        SHOPPING_SMART_ADS_PER_AD_GROUP = 58
        RESPONSIVE_SEARCH_ADS_PER_AD_GROUP = 59
        APP_ADS_PER_AD_GROUP = 60
        APP_ENGAGEMENT_ADS_PER_AD_GROUP = 61
        LOCAL_ADS_PER_AD_GROUP = 62
        VIDEO_ADS_PER_AD_GROUP = 63
        LEAD_FORM_CAMPAIGN_ASSETS_PER_CAMPAIGN = 143
        PROMOTION_CUSTOMER_ASSETS_PER_CUSTOMER = 79
        PROMOTION_CAMPAIGN_ASSETS_PER_CAMPAIGN = 80
        PROMOTION_AD_GROUP_ASSETS_PER_AD_GROUP = 81
        CALLOUT_CUSTOMER_ASSETS_PER_CUSTOMER = 134
        CALLOUT_CAMPAIGN_ASSETS_PER_CAMPAIGN = 135
        CALLOUT_AD_GROUP_ASSETS_PER_AD_GROUP = 136
        SITELINK_CUSTOMER_ASSETS_PER_CUSTOMER = 137
        SITELINK_CAMPAIGN_ASSETS_PER_CAMPAIGN = 138
        SITELINK_AD_GROUP_ASSETS_PER_AD_GROUP = 139
        STRUCTURED_SNIPPET_CUSTOMER_ASSETS_PER_CUSTOMER = 140
        STRUCTURED_SNIPPET_CAMPAIGN_ASSETS_PER_CAMPAIGN = 141
        STRUCTURED_SNIPPET_AD_GROUP_ASSETS_PER_AD_GROUP = 142
        MOBILE_APP_CUSTOMER_ASSETS_PER_CUSTOMER = 144
        MOBILE_APP_CAMPAIGN_ASSETS_PER_CAMPAIGN = 145
        MOBILE_APP_AD_GROUP_ASSETS_PER_AD_GROUP = 146
        HOTEL_CALLOUT_CUSTOMER_ASSETS_PER_CUSTOMER = 147
        HOTEL_CALLOUT_CAMPAIGN_ASSETS_PER_CAMPAIGN = 148
        HOTEL_CALLOUT_AD_GROUP_ASSETS_PER_AD_GROUP = 149
        CALL_CUSTOMER_ASSETS_PER_CUSTOMER = 150
        CALL_CAMPAIGN_ASSETS_PER_CAMPAIGN = 151
        CALL_AD_GROUP_ASSETS_PER_AD_GROUP = 152
        PRICE_CUSTOMER_ASSETS_PER_CUSTOMER = 154
        PRICE_CAMPAIGN_ASSETS_PER_CAMPAIGN = 155
        PRICE_AD_GROUP_ASSETS_PER_AD_GROUP = 156
        AD_IMAGE_CAMPAIGN_ASSETS_PER_CAMPAIGN = 175
        AD_IMAGE_AD_GROUP_ASSETS_PER_AD_GROUP = 176
        PAGE_FEED_ASSET_SETS_PER_CUSTOMER = 157
        DYNAMIC_EDUCATION_FEED_ASSET_SETS_PER_CUSTOMER = 158
        ASSETS_PER_PAGE_FEED_ASSET_SET = 159
        ASSETS_PER_DYNAMIC_EDUCATION_FEED_ASSET_SET = 160
        DYNAMIC_REAL_ESTATE_ASSET_SETS_PER_CUSTOMER = 161
        ASSETS_PER_DYNAMIC_REAL_ESTATE_ASSET_SET = 162
        DYNAMIC_CUSTOM_ASSET_SETS_PER_CUSTOMER = 163
        ASSETS_PER_DYNAMIC_CUSTOM_ASSET_SET = 164
        DYNAMIC_HOTELS_AND_RENTALS_ASSET_SETS_PER_CUSTOMER = 165
        ASSETS_PER_DYNAMIC_HOTELS_AND_RENTALS_ASSET_SET = 166
        DYNAMIC_LOCAL_ASSET_SETS_PER_CUSTOMER = 167
        ASSETS_PER_DYNAMIC_LOCAL_ASSET_SET = 168
        DYNAMIC_FLIGHTS_ASSET_SETS_PER_CUSTOMER = 169
        ASSETS_PER_DYNAMIC_FLIGHTS_ASSET_SET = 170
        DYNAMIC_TRAVEL_ASSET_SETS_PER_CUSTOMER = 171
        ASSETS_PER_DYNAMIC_TRAVEL_ASSET_SET = 172
        DYNAMIC_JOBS_ASSET_SETS_PER_CUSTOMER = 173
        ASSETS_PER_DYNAMIC_JOBS_ASSET_SET = 174
        BUSINESS_NAME_CAMPAIGN_ASSETS_PER_CAMPAIGN = 179
        BUSINESS_LOGO_CAMPAIGN_ASSETS_PER_CAMPAIGN = 180
        VERSIONS_PER_AD = 82
        USER_FEEDS_PER_CUSTOMER = 90
        SYSTEM_FEEDS_PER_CUSTOMER = 91
        FEED_ATTRIBUTES_PER_FEED = 92
        FEED_ITEMS_PER_CUSTOMER = 94
        CAMPAIGN_FEEDS_PER_CUSTOMER = 95
        BASE_CAMPAIGN_FEEDS_PER_CUSTOMER = 96
        EXPERIMENT_CAMPAIGN_FEEDS_PER_CUSTOMER = 109
        AD_GROUP_FEEDS_PER_CUSTOMER = 97
        BASE_AD_GROUP_FEEDS_PER_CUSTOMER = 98
        EXPERIMENT_AD_GROUP_FEEDS_PER_CUSTOMER = 110
        AD_GROUP_FEEDS_PER_CAMPAIGN = 99
        FEED_ITEM_SETS_PER_CUSTOMER = 100
        FEED_ITEMS_PER_FEED_ITEM_SET = 101
        CAMPAIGN_EXPERIMENTS_PER_CUSTOMER = 112
        EXPERIMENT_ARMS_PER_VIDEO_EXPERIMENT = 113
        OWNED_LABELS_PER_CUSTOMER = 115
        LABELS_PER_CAMPAIGN = 117
        LABELS_PER_AD_GROUP = 118
        LABELS_PER_AD_GROUP_AD = 119
        LABELS_PER_AD_GROUP_CRITERION = 120
        TARGET_CUSTOMERS_PER_LABEL = 121
        KEYWORD_PLANS_PER_USER_PER_CUSTOMER = 122
        KEYWORD_PLAN_AD_GROUP_KEYWORDS_PER_KEYWORD_PLAN = 123
        KEYWORD_PLAN_AD_GROUPS_PER_KEYWORD_PLAN = 124
        KEYWORD_PLAN_NEGATIVE_KEYWORDS_PER_KEYWORD_PLAN = 125
        KEYWORD_PLAN_CAMPAIGNS_PER_KEYWORD_PLAN = 126
        CONVERSION_ACTIONS_PER_CUSTOMER = 128
        BATCH_JOB_OPERATIONS_PER_JOB = 130
        BATCH_JOBS_PER_CUSTOMER = 131
        HOTEL_CHECK_IN_DATE_RANGE_BID_MODIFIERS_PER_AD_GROUP = 132
        SHARED_SETS_PER_ACCOUNT_FOR_ACCOUNT_LEVEL_NEGATIVE_KEYWORDS = 177
        ACCOUNT_LEVEL_NEGATIVE_KEYWORDS_PER_SHARED_SET = 178
        ENABLED_ASSET_PER_HOTEL_PROPERTY_ASSET_SET = 181
        ENABLED_HOTEL_PROPERTY_ASSET_LINKS_PER_ASSET_GROUP = 182
        BRANDS_PER_SHARED_SET = 183
        ENABLED_BRAND_LIST_CRITERIA_PER_CAMPAIGN = 184
        SHARED_SETS_PER_ACCOUNT_FOR_BRAND = 185
        LOOKALIKE_USER_LISTS_PER_CUSTOMER = 186
        LOGO_CAMPAIGN_ASSETS_PER_CAMPAIGN = 187
        BUSINESS_MESSAGE_ASSET_LINKS_PER_CUSTOMER = 188
        WHATSAPP_BUSINESS_MESSAGE_ASSET_LINKS_PER_CAMPAIGN = 189
        WHATSAPP_BUSINESS_MESSAGE_ASSET_LINKS_PER_AD_GROUP = 190
        BRAND_LIST_CRITERIA_PER_AD_GROUP = 193


__all__ = tuple(sorted(__protobuf__.manifest))

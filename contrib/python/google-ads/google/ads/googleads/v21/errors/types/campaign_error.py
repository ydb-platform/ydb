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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "CampaignErrorEnum",
    },
)


class CampaignErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign errors."""

    class CampaignError(proto.Enum):
        r"""Enum describing possible campaign errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CANNOT_TARGET_CONTENT_NETWORK (3):
                Cannot target content network.
            CANNOT_TARGET_SEARCH_NETWORK (4):
                Cannot target search network.
            CANNOT_TARGET_SEARCH_NETWORK_WITHOUT_GOOGLE_SEARCH (5):
                Cannot cover search network without google
                search network.
            CANNOT_TARGET_GOOGLE_SEARCH_FOR_CPM_CAMPAIGN (6):
                Cannot target Google Search network for a CPM
                campaign.
            CAMPAIGN_MUST_TARGET_AT_LEAST_ONE_NETWORK (7):
                Must target at least one network.
            CANNOT_TARGET_PARTNER_SEARCH_NETWORK (8):
                Only some Google partners are allowed to
                target partner search network.
            CANNOT_TARGET_CONTENT_NETWORK_ONLY_WITH_CRITERIA_LEVEL_BIDDING_STRATEGY (9):
                Cannot target content network only as
                campaign has criteria-level bidding strategy.
            CAMPAIGN_DURATION_MUST_CONTAIN_ALL_RUNNABLE_TRIALS (10):
                Cannot modify the start or end date such that
                the campaign duration would not contain the
                durations of all runnable trials.
            CANNOT_MODIFY_FOR_TRIAL_CAMPAIGN (11):
                Cannot modify dates, budget or status of a
                trial campaign.
            DUPLICATE_CAMPAIGN_NAME (12):
                Trying to modify the name of an active or
                paused campaign, where the name is already
                assigned to another active or paused campaign.
            INCOMPATIBLE_CAMPAIGN_FIELD (13):
                Two fields are in conflicting modes.
            INVALID_CAMPAIGN_NAME (14):
                Campaign name cannot be used.
            INVALID_AD_SERVING_OPTIMIZATION_STATUS (15):
                Given status is invalid.
            INVALID_TRACKING_URL (16):
                Error in the campaign level tracking URL.
            CANNOT_SET_BOTH_TRACKING_URL_TEMPLATE_AND_TRACKING_SETTING (17):
                Cannot set both tracking URL template and
                tracking setting. A user has to clear legacy
                tracking setting in order to add tracking URL
                template.
            MAX_IMPRESSIONS_NOT_IN_RANGE (18):
                The maximum number of impressions for
                Frequency Cap should be an integer greater than
                0.
            TIME_UNIT_NOT_SUPPORTED (19):
                Only the Day, Week and Month time units are
                supported.
            INVALID_OPERATION_IF_SERVING_STATUS_HAS_ENDED (20):
                Operation not allowed on a campaign whose
                serving status has ended
            BUDGET_CANNOT_BE_SHARED (21):
                This budget is exclusively linked to a
                Campaign that is using experiments so it cannot
                be shared.
            CAMPAIGN_CANNOT_USE_SHARED_BUDGET (22):
                Campaigns using experiments cannot use a
                shared budget.
            CANNOT_CHANGE_BUDGET_ON_CAMPAIGN_WITH_TRIALS (23):
                A different budget cannot be assigned to a
                campaign when there are running or scheduled
                trials.
            CAMPAIGN_LABEL_DOES_NOT_EXIST (24):
                No link found between the campaign and the
                label.
            CAMPAIGN_LABEL_ALREADY_EXISTS (25):
                The label has already been attached to the
                campaign.
            MISSING_SHOPPING_SETTING (26):
                A ShoppingSetting was not found when creating
                a shopping campaign.
            INVALID_SHOPPING_SALES_COUNTRY (27):
                The country in shopping setting is not an
                allowed country.
            ADVERTISING_CHANNEL_TYPE_NOT_AVAILABLE_FOR_ACCOUNT_TYPE (31):
                The requested channel type is not available
                according to the customer's account setting.
            INVALID_ADVERTISING_CHANNEL_SUB_TYPE (32):
                The AdvertisingChannelSubType is not a valid
                subtype of the primary channel type.
            AT_LEAST_ONE_CONVERSION_MUST_BE_SELECTED (33):
                At least one conversion must be selected.
            CANNOT_SET_AD_ROTATION_MODE (34):
                Setting ad rotation mode for a campaign is
                not allowed. Ad rotation mode at campaign is
                deprecated.
            CANNOT_MODIFY_START_DATE_IF_ALREADY_STARTED (35):
                Trying to change start date on a campaign
                that has started.
            CANNOT_SET_DATE_TO_PAST (36):
                Trying to modify a date into the past.
            MISSING_HOTEL_CUSTOMER_LINK (37):
                Hotel center id in the hotel setting does not
                match any customer links.
            INVALID_HOTEL_CUSTOMER_LINK (38):
                Hotel center id in the hotel setting must
                match an active customer link.
            MISSING_HOTEL_SETTING (39):
                Hotel setting was not found when creating a
                hotel ads campaign.
            CANNOT_USE_SHARED_CAMPAIGN_BUDGET_WHILE_PART_OF_CAMPAIGN_GROUP (40):
                A Campaign cannot use shared campaign budgets
                and be part of a campaign group.
            APP_NOT_FOUND (41):
                The app ID was not found.
            SHOPPING_ENABLE_LOCAL_NOT_SUPPORTED_FOR_CAMPAIGN_TYPE (42):
                Campaign.shopping_setting.enable_local is not supported for
                the specified campaign type.
            MERCHANT_NOT_ALLOWED_FOR_COMPARISON_LISTING_ADS (43):
                The merchant does not support the creation of
                campaigns for Shopping Comparison Listing Ads.
            INSUFFICIENT_APP_INSTALLS_COUNT (44):
                The App campaign for engagement cannot be
                created because there aren't enough installs.
            SENSITIVE_CATEGORY_APP (45):
                The App campaign for engagement cannot be
                created because the app is sensitive.
            HEC_AGREEMENT_REQUIRED (46):
                Customers with Housing, Employment, or Credit
                ads must accept updated personalized ads policy
                to continue creating campaigns.
            NOT_COMPATIBLE_WITH_VIEW_THROUGH_CONVERSION_OPTIMIZATION (49):
                The field is not compatible with view through
                conversion optimization.
            INVALID_EXCLUDED_PARENT_ASSET_FIELD_TYPE (48):
                The field type cannot be excluded because an
                active campaign-asset link of this type exists.
            CANNOT_CREATE_APP_PRE_REGISTRATION_FOR_NON_ANDROID_APP (50):
                The app pre-registration campaign cannot be
                created for non-Android applications.
            APP_NOT_AVAILABLE_TO_CREATE_APP_PRE_REGISTRATION_CAMPAIGN (51):
                The campaign cannot be created since the app
                is not available for pre-registration in any
                country.
            INCOMPATIBLE_BUDGET_TYPE (52):
                The type of the Budget is not compatible with
                this Campaign.
            LOCAL_SERVICES_DUPLICATE_CATEGORY_BID (53):
                Category bid list in the local services
                campaign setting contains multiple bids for the
                same category ID.
            LOCAL_SERVICES_INVALID_CATEGORY_BID (54):
                Category bid list in the local services
                campaign setting contains a bid for an invalid
                category ID.
            LOCAL_SERVICES_MISSING_CATEGORY_BID (55):
                Category bid list in the local services
                campaign setting is missing a bid for a category
                ID that must be present.
            INVALID_STATUS_CHANGE (57):
                The requested change in status is not
                supported.
            MISSING_TRAVEL_CUSTOMER_LINK (58):
                Travel Campaign's travel_account_id does not match any
                customer links.
            INVALID_TRAVEL_CUSTOMER_LINK (59):
                Travel Campaign's travel_account_id matches an existing
                customer link but the customer link is not active.
            INVALID_EXCLUDED_PARENT_ASSET_SET_TYPE (62):
                The asset set type is invalid to be set in
                excluded_parent_asset_set_types field.
            ASSET_SET_NOT_A_HOTEL_PROPERTY_ASSET_SET (63):
                Campaign.hotel_property_asset_set must point to an asset set
                of type HOTEL_PROPERTY.
            HOTEL_PROPERTY_ASSET_SET_ONLY_FOR_PERFORMANCE_MAX_FOR_TRAVEL_GOALS (64):
                The hotel property asset set can only be set
                on Performance Max for travel goals campaigns.
            AVERAGE_DAILY_SPEND_TOO_HIGH (65):
                Customer's average daily spend is too high to
                enable this feature.
            CANNOT_ATTACH_TO_REMOVED_CAMPAIGN_GROUP (66):
                Cannot attach the campaign to a deleted
                campaign group.
            CANNOT_ATTACH_TO_BIDDING_STRATEGY (67):
                Cannot attach the campaign to this bidding
                strategy.
            CANNOT_CHANGE_BUDGET_PERIOD (68):
                A budget with a different period cannot be
                assigned to the campaign.
            NOT_ENOUGH_CONVERSIONS (71):
                Customer does not have enough conversions to
                enable this feature.
            CANNOT_SET_MORE_THAN_ONE_CONVERSION_ACTION (72):
                This campaign type can only have one
                conversion action.
            NOT_COMPATIBLE_WITH_BUDGET_TYPE (73):
                The field is not compatible with the budget
                type.
            NOT_COMPATIBLE_WITH_UPLOAD_CLICKS_CONVERSION (74):
                The feature is incompatible with
                ConversionActionType.UPLOAD_CLICKS.
            APP_ID_MUST_MATCH_CONVERSION_ACTION_APP_ID (76):
                App campaign setting app ID must match
                selective optimization conversion action app ID.
            CONVERSION_ACTION_WITH_DOWNLOAD_CATEGORY_NOT_ALLOWED (77):
                Selective optimization conversion action with
                Download category is not allowed.
            CONVERSION_ACTION_WITH_DOWNLOAD_CATEGORY_REQUIRED (78):
                One software download for selective
                optimization conversion action is required for
                this campaign conversion action.
            CONVERSION_TRACKING_NOT_ENABLED (79):
                Conversion tracking is not enabled and is
                required for this feature.
            NOT_COMPATIBLE_WITH_BIDDING_STRATEGY_TYPE (80):
                The field is not compatible with the bidding
                strategy type.
            NOT_COMPATIBLE_WITH_GOOGLE_ATTRIBUTION_CONVERSIONS (81):
                Campaign is not compatible with a conversion
                tracker that has Google attribution enabled.
            CONVERSION_LAG_TOO_HIGH (82):
                Customer level conversion lag is too high.
            NOT_LINKED_ADVERTISING_PARTNER (83):
                The advertiser set as an advertising partner
                is not an actively linked advertiser to this
                customer.
            INVALID_NUMBER_OF_ADVERTISING_PARTNER_IDS (84):
                Invalid number of advertising partner IDs.
            CANNOT_TARGET_DISPLAY_NETWORK_WITHOUT_YOUTUBE (85):
                Cannot target the display network without
                also targeting YouTube.
            CANNOT_LINK_TO_COMPARISON_SHOPPING_SERVICE_ACCOUNT (86):
                This campaign type cannot be linked to a
                Comparison Shopping Service account.
            CANNOT_TARGET_NETWORK_FOR_COMPARISON_SHOPPING_SERVICE_LINKED_ACCOUNTS (87):
                Standard Shopping campaigns that are linked
                to a Comparison Shopping Service account cannot
                target this network.
            CANNOT_MODIFY_TEXT_ASSET_AUTOMATION_WITH_ENABLED_TRIAL (88):
                Text asset automation settings can not be
                modified when there is an active Performance Max
                optimization automatically created assets
                experiment. End the experiment to modify these
                settings.
            DYNAMIC_TEXT_ASSET_CANNOT_OPT_OUT_WITH_FINAL_URL_EXPANSION_OPT_IN (89):
                Dynamic text asset cannot be opted out when
                final URL expansion is opted in.
            CANNOT_SET_CAMPAIGN_KEYWORD_MATCH_TYPE (90):
                Can not set a campaign level match type.
            CANNOT_DISABLE_BROAD_MATCH_WHEN_KEYWORD_CONVERSION_IN_PROCESS (91):
                The campaign level keyword match type cannot
                be switched to non-broad when keyword conversion
                to broad match is in process.
            CANNOT_DISABLE_BROAD_MATCH_WHEN_TARGETING_BRANDS (92):
                The campaign level keyword match type cannot
                be switched to non-broad when the campaign has
                any attached brand list or when a brand hint
                shared set is attached to the campaign.
            CANNOT_ENABLE_BROAD_MATCH_FOR_BASE_CAMPAIGN_WITH_PROMOTING_TRIAL (93):
                Cannot set campaign level keyword match type
                to BROAD if the campaign is a base campaign with
                an associated trial that is currently promoting.
            CANNOT_ENABLE_BROAD_MATCH_FOR_PROMOTING_TRIAL_CAMPAIGN (94):
                Cannot set campaign level keyword match type
                to BROAD if the campaign is a trial currently
                promoting.
            REQUIRED_BUSINESS_NAME_ASSET_NOT_LINKED (95):
                Performance Max campaigns with Brand
                Guidelines enabled require at least one business
                name to be linked as a CampaignAsset.
                Performance Max campaigns for online sales with
                a product feed must meet this requirement only
                when there are assets that are linked to the
                campaign's asset groups.
            REQUIRED_LOGO_ASSET_NOT_LINKED (96):
                Performance Max campaigns with Brand
                Guidelines enabled require at least one square
                logo to be linked as a CampaignAsset.
                Performance Max campaigns for online sales with
                a product feed must meet this requirement only
                when there are assets that are linked to the
                campaign's asset groups.
            BRAND_TARGETING_OVERRIDES_NOT_SUPPORTED (97):
                This campaign does not support brand
                targeting overrides. Brand targeting overrides
                are only supported for Performance Max campaigns
                that have a product feed.
            BRAND_GUIDELINES_NOT_ENABLED_FOR_CAMPAIGN (98):
                Brand Guideline fields can only be set for
                campaigns that have Brand Guidelines enabled.
            BRAND_GUIDELINES_MAIN_AND_ACCENT_COLORS_REQUIRED (99):
                When a Brand Guidelines color field is set,
                both main color and accent color are required.
            BRAND_GUIDELINES_COLOR_INVALID_FORMAT (100):
                Brand Guidelines colors must be hex colors matching the
                regular expression '#[0-9a-fA-F]{6}', for example '#abc123'
            BRAND_GUIDELINES_UNSUPPORTED_FONT_FAMILY (101):
                Brand Guidelines font family must be one of the supported
                Google Fonts. See
                Campaign.brand_guidelines.predefined_font_family for the
                list of supported fonts.
            BRAND_GUIDELINES_UNSUPPORTED_CHANNEL (102):
                Brand Guidelines cannot be set for this
                channel type. Brand Guidelines supports
                Performance Max campaigns.
            CANNOT_ENABLE_BRAND_GUIDELINES_FOR_TRAVEL_GOALS (103):
                Brand Guidelines cannot be enabled for
                Performance Max for travel goals campaigns.
            CUSTOMER_NOT_ALLOWLISTED_FOR_BRAND_GUIDELINES (104):
                This customer is not allowlisted for enabling
                Brand Guidelines.
            THIRD_PARTY_INTEGRATION_PARTNER_NOT_ALLOWED (105):
                Using campaign third-party integration
                partners that are not set at the customer level
                is not allowed.
            THIRD_PARTY_INTEGRATION_PARTNER_SHARE_COST_NOT_ALLOWED (106):
                Campaign third-party integration partners are
                not allowed to share cost if it is not enabled
                at the customer level.
            DUPLICATE_INTERACTION_TYPE (107):
                Each ``previous_step_interaction_type`` can be used at most
                once for the same ``previous_step_id``
            INVALID_INTERACTION_TYPE (108):
                Previous step interaction type cannot happen for previous
                step AdGroup type. For example, ``SKIP`` interaction type is
                not valid for non-skippable formats.
            VIDEO_SEQUENCE_ERROR_SEQUENCE_DEFINITION_REQUIRED (109):
                Campaign video ads sequence is required for
                ``VIDEO_SEQUENCE`` advertising channel sub type.
            AI_MAX_MUST_BE_ENABLED (110):
                This feature is only available for campaigns
                with AI Max enabled.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_TARGET_CONTENT_NETWORK = 3
        CANNOT_TARGET_SEARCH_NETWORK = 4
        CANNOT_TARGET_SEARCH_NETWORK_WITHOUT_GOOGLE_SEARCH = 5
        CANNOT_TARGET_GOOGLE_SEARCH_FOR_CPM_CAMPAIGN = 6
        CAMPAIGN_MUST_TARGET_AT_LEAST_ONE_NETWORK = 7
        CANNOT_TARGET_PARTNER_SEARCH_NETWORK = 8
        CANNOT_TARGET_CONTENT_NETWORK_ONLY_WITH_CRITERIA_LEVEL_BIDDING_STRATEGY = (
            9
        )
        CAMPAIGN_DURATION_MUST_CONTAIN_ALL_RUNNABLE_TRIALS = 10
        CANNOT_MODIFY_FOR_TRIAL_CAMPAIGN = 11
        DUPLICATE_CAMPAIGN_NAME = 12
        INCOMPATIBLE_CAMPAIGN_FIELD = 13
        INVALID_CAMPAIGN_NAME = 14
        INVALID_AD_SERVING_OPTIMIZATION_STATUS = 15
        INVALID_TRACKING_URL = 16
        CANNOT_SET_BOTH_TRACKING_URL_TEMPLATE_AND_TRACKING_SETTING = 17
        MAX_IMPRESSIONS_NOT_IN_RANGE = 18
        TIME_UNIT_NOT_SUPPORTED = 19
        INVALID_OPERATION_IF_SERVING_STATUS_HAS_ENDED = 20
        BUDGET_CANNOT_BE_SHARED = 21
        CAMPAIGN_CANNOT_USE_SHARED_BUDGET = 22
        CANNOT_CHANGE_BUDGET_ON_CAMPAIGN_WITH_TRIALS = 23
        CAMPAIGN_LABEL_DOES_NOT_EXIST = 24
        CAMPAIGN_LABEL_ALREADY_EXISTS = 25
        MISSING_SHOPPING_SETTING = 26
        INVALID_SHOPPING_SALES_COUNTRY = 27
        ADVERTISING_CHANNEL_TYPE_NOT_AVAILABLE_FOR_ACCOUNT_TYPE = 31
        INVALID_ADVERTISING_CHANNEL_SUB_TYPE = 32
        AT_LEAST_ONE_CONVERSION_MUST_BE_SELECTED = 33
        CANNOT_SET_AD_ROTATION_MODE = 34
        CANNOT_MODIFY_START_DATE_IF_ALREADY_STARTED = 35
        CANNOT_SET_DATE_TO_PAST = 36
        MISSING_HOTEL_CUSTOMER_LINK = 37
        INVALID_HOTEL_CUSTOMER_LINK = 38
        MISSING_HOTEL_SETTING = 39
        CANNOT_USE_SHARED_CAMPAIGN_BUDGET_WHILE_PART_OF_CAMPAIGN_GROUP = 40
        APP_NOT_FOUND = 41
        SHOPPING_ENABLE_LOCAL_NOT_SUPPORTED_FOR_CAMPAIGN_TYPE = 42
        MERCHANT_NOT_ALLOWED_FOR_COMPARISON_LISTING_ADS = 43
        INSUFFICIENT_APP_INSTALLS_COUNT = 44
        SENSITIVE_CATEGORY_APP = 45
        HEC_AGREEMENT_REQUIRED = 46
        NOT_COMPATIBLE_WITH_VIEW_THROUGH_CONVERSION_OPTIMIZATION = 49
        INVALID_EXCLUDED_PARENT_ASSET_FIELD_TYPE = 48
        CANNOT_CREATE_APP_PRE_REGISTRATION_FOR_NON_ANDROID_APP = 50
        APP_NOT_AVAILABLE_TO_CREATE_APP_PRE_REGISTRATION_CAMPAIGN = 51
        INCOMPATIBLE_BUDGET_TYPE = 52
        LOCAL_SERVICES_DUPLICATE_CATEGORY_BID = 53
        LOCAL_SERVICES_INVALID_CATEGORY_BID = 54
        LOCAL_SERVICES_MISSING_CATEGORY_BID = 55
        INVALID_STATUS_CHANGE = 57
        MISSING_TRAVEL_CUSTOMER_LINK = 58
        INVALID_TRAVEL_CUSTOMER_LINK = 59
        INVALID_EXCLUDED_PARENT_ASSET_SET_TYPE = 62
        ASSET_SET_NOT_A_HOTEL_PROPERTY_ASSET_SET = 63
        HOTEL_PROPERTY_ASSET_SET_ONLY_FOR_PERFORMANCE_MAX_FOR_TRAVEL_GOALS = 64
        AVERAGE_DAILY_SPEND_TOO_HIGH = 65
        CANNOT_ATTACH_TO_REMOVED_CAMPAIGN_GROUP = 66
        CANNOT_ATTACH_TO_BIDDING_STRATEGY = 67
        CANNOT_CHANGE_BUDGET_PERIOD = 68
        NOT_ENOUGH_CONVERSIONS = 71
        CANNOT_SET_MORE_THAN_ONE_CONVERSION_ACTION = 72
        NOT_COMPATIBLE_WITH_BUDGET_TYPE = 73
        NOT_COMPATIBLE_WITH_UPLOAD_CLICKS_CONVERSION = 74
        APP_ID_MUST_MATCH_CONVERSION_ACTION_APP_ID = 76
        CONVERSION_ACTION_WITH_DOWNLOAD_CATEGORY_NOT_ALLOWED = 77
        CONVERSION_ACTION_WITH_DOWNLOAD_CATEGORY_REQUIRED = 78
        CONVERSION_TRACKING_NOT_ENABLED = 79
        NOT_COMPATIBLE_WITH_BIDDING_STRATEGY_TYPE = 80
        NOT_COMPATIBLE_WITH_GOOGLE_ATTRIBUTION_CONVERSIONS = 81
        CONVERSION_LAG_TOO_HIGH = 82
        NOT_LINKED_ADVERTISING_PARTNER = 83
        INVALID_NUMBER_OF_ADVERTISING_PARTNER_IDS = 84
        CANNOT_TARGET_DISPLAY_NETWORK_WITHOUT_YOUTUBE = 85
        CANNOT_LINK_TO_COMPARISON_SHOPPING_SERVICE_ACCOUNT = 86
        CANNOT_TARGET_NETWORK_FOR_COMPARISON_SHOPPING_SERVICE_LINKED_ACCOUNTS = (
            87
        )
        CANNOT_MODIFY_TEXT_ASSET_AUTOMATION_WITH_ENABLED_TRIAL = 88
        DYNAMIC_TEXT_ASSET_CANNOT_OPT_OUT_WITH_FINAL_URL_EXPANSION_OPT_IN = 89
        CANNOT_SET_CAMPAIGN_KEYWORD_MATCH_TYPE = 90
        CANNOT_DISABLE_BROAD_MATCH_WHEN_KEYWORD_CONVERSION_IN_PROCESS = 91
        CANNOT_DISABLE_BROAD_MATCH_WHEN_TARGETING_BRANDS = 92
        CANNOT_ENABLE_BROAD_MATCH_FOR_BASE_CAMPAIGN_WITH_PROMOTING_TRIAL = 93
        CANNOT_ENABLE_BROAD_MATCH_FOR_PROMOTING_TRIAL_CAMPAIGN = 94
        REQUIRED_BUSINESS_NAME_ASSET_NOT_LINKED = 95
        REQUIRED_LOGO_ASSET_NOT_LINKED = 96
        BRAND_TARGETING_OVERRIDES_NOT_SUPPORTED = 97
        BRAND_GUIDELINES_NOT_ENABLED_FOR_CAMPAIGN = 98
        BRAND_GUIDELINES_MAIN_AND_ACCENT_COLORS_REQUIRED = 99
        BRAND_GUIDELINES_COLOR_INVALID_FORMAT = 100
        BRAND_GUIDELINES_UNSUPPORTED_FONT_FAMILY = 101
        BRAND_GUIDELINES_UNSUPPORTED_CHANNEL = 102
        CANNOT_ENABLE_BRAND_GUIDELINES_FOR_TRAVEL_GOALS = 103
        CUSTOMER_NOT_ALLOWLISTED_FOR_BRAND_GUIDELINES = 104
        THIRD_PARTY_INTEGRATION_PARTNER_NOT_ALLOWED = 105
        THIRD_PARTY_INTEGRATION_PARTNER_SHARE_COST_NOT_ALLOWED = 106
        DUPLICATE_INTERACTION_TYPE = 107
        INVALID_INTERACTION_TYPE = 108
        VIDEO_SEQUENCE_ERROR_SEQUENCE_DEFINITION_REQUIRED = 109
        AI_MAX_MUST_BE_ENABLED = 110


__all__ = tuple(sorted(__protobuf__.manifest))

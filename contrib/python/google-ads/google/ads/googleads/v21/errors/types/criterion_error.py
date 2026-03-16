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
        "CriterionErrorEnum",
    },
)


class CriterionErrorEnum(proto.Message):
    r"""Container for enum describing possible criterion errors."""

    class CriterionError(proto.Enum):
        r"""Enum describing possible criterion errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CONCRETE_TYPE_REQUIRED (2):
                Concrete type of criterion is required for
                CREATE and UPDATE operations.
            INVALID_EXCLUDED_CATEGORY (3):
                The category requested for exclusion is
                invalid.
            INVALID_KEYWORD_TEXT (4):
                Invalid keyword criteria text.
            KEYWORD_TEXT_TOO_LONG (5):
                Keyword text should be less than 80 chars.
            KEYWORD_HAS_TOO_MANY_WORDS (6):
                Keyword text has too many words.
            KEYWORD_HAS_INVALID_CHARS (7):
                Keyword text has invalid characters or
                symbols.
            INVALID_PLACEMENT_URL (8):
                Invalid placement URL.
            INVALID_USER_LIST (9):
                Invalid user list criterion.
            INVALID_USER_INTEREST (10):
                Invalid user interest criterion.
            INVALID_FORMAT_FOR_PLACEMENT_URL (11):
                Placement URL has wrong format.
            PLACEMENT_URL_IS_TOO_LONG (12):
                Placement URL is too long.
            PLACEMENT_URL_HAS_ILLEGAL_CHAR (13):
                Indicates the URL contains an illegal
                character.
            PLACEMENT_URL_HAS_MULTIPLE_SITES_IN_LINE (14):
                Indicates the URL contains multiple comma
                separated URLs.
            PLACEMENT_IS_NOT_AVAILABLE_FOR_TARGETING_OR_EXCLUSION (15):
                Indicates the domain is blocked.
            INVALID_TOPIC_PATH (16):
                Invalid topic path.
            INVALID_YOUTUBE_CHANNEL_ID (17):
                The YouTube Channel Id is invalid.
            INVALID_YOUTUBE_VIDEO_ID (18):
                The YouTube Video Id is invalid.
            YOUTUBE_VERTICAL_CHANNEL_DEPRECATED (19):
                Indicates the placement is a YouTube vertical
                channel, which is no longer supported.
            YOUTUBE_DEMOGRAPHIC_CHANNEL_DEPRECATED (20):
                Indicates the placement is a YouTube
                demographic channel, which is no longer
                supported.
            YOUTUBE_URL_UNSUPPORTED (21):
                YouTube urls are not supported in Placement
                criterion. Use YouTubeChannel and YouTubeVideo
                criterion instead.
            CANNOT_EXCLUDE_CRITERIA_TYPE (22):
                Criteria type can not be excluded by the
                customer, like AOL account type cannot target
                site type criteria.
            CANNOT_ADD_CRITERIA_TYPE (23):
                Criteria type can not be targeted.
            CANNOT_EXCLUDE_SIMILAR_USER_LIST (26):
                Not allowed to exclude similar user list.
            CANNOT_ADD_CLOSED_USER_LIST (27):
                Not allowed to target a closed user list.
            CANNOT_ADD_DISPLAY_ONLY_LISTS_TO_SEARCH_ONLY_CAMPAIGNS (28):
                Not allowed to add display only UserLists to
                search only campaigns.
            CANNOT_ADD_DISPLAY_ONLY_LISTS_TO_SEARCH_CAMPAIGNS (29):
                Not allowed to add display only UserLists to
                search plus campaigns.
            CANNOT_ADD_DISPLAY_ONLY_LISTS_TO_SHOPPING_CAMPAIGNS (30):
                Not allowed to add display only UserLists to
                shopping campaigns.
            CANNOT_ADD_USER_INTERESTS_TO_SEARCH_CAMPAIGNS (31):
                Not allowed to add User interests to search
                only campaigns.
            CANNOT_SET_BIDS_ON_CRITERION_TYPE_IN_SEARCH_CAMPAIGNS (32):
                Not allowed to set bids for this criterion
                type in search campaigns
            CANNOT_ADD_URLS_TO_CRITERION_TYPE_FOR_CAMPAIGN_TYPE (33):
                Final URLs, URL Templates and
                CustomParameters cannot be set for the criterion
                types of Gender, AgeRange, UserList, Placement,
                MobileApp, and MobileAppCategory in search
                campaigns and shopping campaigns.
            INVALID_COMBINED_AUDIENCE (122):
                Invalid combined audience criterion.
            INVALID_CUSTOM_AFFINITY (96):
                Invalid custom affinity criterion.
            INVALID_CUSTOM_INTENT (97):
                Invalid custom intent criterion.
            INVALID_CUSTOM_AUDIENCE (121):
                Invalid custom audience criterion.
            INVALID_IP_ADDRESS (34):
                IP address is not valid.
            INVALID_IP_FORMAT (35):
                IP format is not valid.
            INVALID_MOBILE_APP (36):
                Mobile application is not valid.
            INVALID_MOBILE_APP_CATEGORY (37):
                Mobile application category is not valid.
            INVALID_CRITERION_ID (38):
                The CriterionId does not exist or is of the
                incorrect type.
            CANNOT_TARGET_CRITERION (39):
                The Criterion is not allowed to be targeted.
            CANNOT_TARGET_OBSOLETE_CRITERION (40):
                The criterion is not allowed to be targeted
                as it is deprecated.
            CRITERION_ID_AND_TYPE_MISMATCH (41):
                The CriterionId is not valid for the type.
            INVALID_PROXIMITY_RADIUS (42):
                Distance for the radius for the proximity
                criterion is invalid.
            INVALID_PROXIMITY_RADIUS_UNITS (43):
                Units for the distance for the radius for the
                proximity criterion is invalid.
            INVALID_STREETADDRESS_LENGTH (44):
                Street address in the address is not valid.
            INVALID_CITYNAME_LENGTH (45):
                City name in the address is not valid.
            INVALID_REGIONCODE_LENGTH (46):
                Region code in the address is not valid.
            INVALID_REGIONNAME_LENGTH (47):
                Region name in the address is not valid.
            INVALID_POSTALCODE_LENGTH (48):
                Postal code in the address is not valid.
            INVALID_COUNTRY_CODE (49):
                Country code in the address is not valid.
            INVALID_LATITUDE (50):
                Latitude for the GeoPoint is not valid.
            INVALID_LONGITUDE (51):
                Longitude for the GeoPoint is not valid.
            PROXIMITY_GEOPOINT_AND_ADDRESS_BOTH_CANNOT_BE_NULL (52):
                The Proximity input is not valid. Both
                address and geoPoint cannot be null.
            INVALID_PROXIMITY_ADDRESS (53):
                The Proximity address cannot be geocoded to a
                valid lat/long.
            INVALID_USER_DOMAIN_NAME (54):
                User domain name is not valid.
            CRITERION_PARAMETER_TOO_LONG (55):
                Length of serialized criterion parameter
                exceeded size limit.
            AD_SCHEDULE_TIME_INTERVALS_OVERLAP (56):
                Time interval in the AdSchedule overlaps with
                another AdSchedule.
            AD_SCHEDULE_INTERVAL_CANNOT_SPAN_MULTIPLE_DAYS (57):
                AdSchedule time interval cannot span multiple
                days.
            AD_SCHEDULE_INVALID_TIME_INTERVAL (58):
                AdSchedule time interval specified is
                invalid, endTime cannot be earlier than
                startTime.
            AD_SCHEDULE_EXCEEDED_INTERVALS_PER_DAY_LIMIT (59):
                The number of AdSchedule entries in a day
                exceeds the limit.
            AD_SCHEDULE_CRITERION_ID_MISMATCHING_FIELDS (60):
                CriteriaId does not match the interval of the
                AdSchedule specified.
            CANNOT_BID_MODIFY_CRITERION_TYPE (61):
                Cannot set bid modifier for this criterion
                type.
            CANNOT_BID_MODIFY_CRITERION_CAMPAIGN_OPTED_OUT (62):
                Cannot bid modify criterion, since it is
                opted out of the campaign.
            CANNOT_BID_MODIFY_NEGATIVE_CRITERION (63):
                Cannot set bid modifier for a negative
                criterion.
            BID_MODIFIER_ALREADY_EXISTS (64):
                Bid Modifier already exists. Use SET
                operation to update.
            FEED_ID_NOT_ALLOWED (65):
                Feed Id is not allowed in these Location
                Groups.
            ACCOUNT_INELIGIBLE_FOR_CRITERIA_TYPE (66):
                The account may not use the requested
                criteria type. For example, some accounts are
                restricted to keywords only.
            CRITERIA_TYPE_INVALID_FOR_BIDDING_STRATEGY (67):
                The requested criteria type cannot be used
                with campaign or ad group bidding strategy.
            CANNOT_EXCLUDE_CRITERION (68):
                The Criterion is not allowed to be excluded.
            CANNOT_REMOVE_CRITERION (69):
                The criterion is not allowed to be removed.
                For example, we cannot remove any of the device
                criterion.
            INVALID_PRODUCT_BIDDING_CATEGORY (76):
                Bidding categories do not form a valid path
                in the Shopping bidding category taxonomy.
            MISSING_SHOPPING_SETTING (77):
                ShoppingSetting must be added to the campaign
                before ProductScope criteria can be added.
            INVALID_MATCHING_FUNCTION (78):
                Matching function is invalid.
            LOCATION_FILTER_NOT_ALLOWED (79):
                Filter parameters not allowed for location
                groups targeting.
            INVALID_FEED_FOR_LOCATION_FILTER (98):
                Feed not found, or the feed is not an enabled
                location feed.
            LOCATION_FILTER_INVALID (80):
                Given location filter parameter is invalid
                for location groups targeting.
            CANNOT_SET_GEO_TARGET_CONSTANTS_WITH_FEED_ITEM_SETS (123):
                Cannot set geo target constants and feed item
                sets at the same time.
            CANNOT_SET_BOTH_ASSET_SET_AND_FEED (140):
                Cannot set both assetset and feed at the same
                time.
            CANNOT_SET_FEED_OR_FEED_ITEM_SETS_FOR_CUSTOMER (142):
                Cannot set feed or feed item sets for
                Customer.
            CANNOT_SET_ASSET_SET_FIELD_FOR_CUSTOMER (150):
                Cannot set AssetSet criteria for customer.
            CANNOT_SET_GEO_TARGET_CONSTANTS_WITH_ASSET_SETS (143):
                Cannot set geo target constants and asset
                sets at the same time.
            CANNOT_SET_ASSET_SETS_WITH_FEED_ITEM_SETS (144):
                Cannot set asset sets and feed item sets at
                the same time.
            INVALID_LOCATION_GROUP_ASSET_SET (141):
                The location group asset set id is invalid
            INVALID_LOCATION_GROUP_RADIUS (124):
                The location group radius is in the range but
                not at the valid increment.
            INVALID_LOCATION_GROUP_RADIUS_UNIT (125):
                The location group radius unit is invalid.
            CANNOT_ATTACH_CRITERIA_AT_CAMPAIGN_AND_ADGROUP (81):
                Criteria type cannot be associated with a
                campaign and its ad group(s) simultaneously.
            HOTEL_LENGTH_OF_STAY_OVERLAPS_WITH_EXISTING_CRITERION (82):
                Range represented by hotel length of stay's
                min nights and max nights overlaps with an
                existing criterion.
            HOTEL_ADVANCE_BOOKING_WINDOW_OVERLAPS_WITH_EXISTING_CRITERION (83):
                Range represented by hotel advance booking
                window's min days and max days overlaps with an
                existing criterion.
            FIELD_INCOMPATIBLE_WITH_NEGATIVE_TARGETING (84):
                The field is not allowed to be set when the
                negative field is set to true, for example, we
                don't allow bids in negative ad group or
                campaign criteria.
            INVALID_WEBPAGE_CONDITION (85):
                The combination of operand and operator in
                webpage condition is invalid.
            INVALID_WEBPAGE_CONDITION_URL (86):
                The URL of webpage condition is invalid.
            WEBPAGE_CONDITION_URL_CANNOT_BE_EMPTY (87):
                The URL of webpage condition cannot be empty
                or contain white space.
            WEBPAGE_CONDITION_URL_UNSUPPORTED_PROTOCOL (88):
                The URL of webpage condition contains an
                unsupported protocol.
            WEBPAGE_CONDITION_URL_CANNOT_BE_IP_ADDRESS (89):
                The URL of webpage condition cannot be an IP
                address.
            WEBPAGE_CONDITION_URL_DOMAIN_NOT_CONSISTENT_WITH_CAMPAIGN_SETTING (90):
                The domain of the URL is not consistent with
                the domain in campaign setting.
            WEBPAGE_CONDITION_URL_CANNOT_BE_PUBLIC_SUFFIX (91):
                The URL of webpage condition cannot be a
                public suffix itself.
            WEBPAGE_CONDITION_URL_INVALID_PUBLIC_SUFFIX (92):
                The URL of webpage condition has an invalid
                public suffix.
            WEBPAGE_CONDITION_URL_VALUE_TRACK_VALUE_NOT_SUPPORTED (93):
                Value track parameter is not supported in
                webpage condition URL.
            WEBPAGE_CRITERION_URL_EQUALS_CAN_HAVE_ONLY_ONE_CONDITION (94):
                Only one URL-EQUALS webpage condition is
                allowed in a webpage criterion and it cannot be
                combined with other conditions.
            WEBPAGE_CRITERION_NOT_SUPPORTED_ON_NON_DSA_AD_GROUP (95):
                A webpage criterion cannot be added to a
                non-DSA ad group.
            CANNOT_TARGET_USER_LIST_FOR_SMART_DISPLAY_CAMPAIGNS (99):
                Cannot add positive user list criteria in
                Smart Display campaigns.
            CANNOT_TARGET_PLACEMENTS_FOR_SEARCH_CAMPAIGNS (126):
                Cannot add positive placement criterion types
                in search campaigns.
            LISTING_SCOPE_TOO_MANY_DIMENSION_TYPES (100):
                Listing scope contains too many dimension
                types.
            LISTING_SCOPE_TOO_MANY_IN_OPERATORS (101):
                Listing scope has too many IN operators.
            LISTING_SCOPE_IN_OPERATOR_NOT_SUPPORTED (102):
                Listing scope contains IN operator on an
                unsupported dimension type.
            DUPLICATE_LISTING_DIMENSION_TYPE (103):
                There are dimensions with duplicate dimension
                type.
            DUPLICATE_LISTING_DIMENSION_VALUE (104):
                There are dimensions with duplicate dimension
                value.
            CANNOT_SET_BIDS_ON_LISTING_GROUP_SUBDIVISION (105):
                Listing group SUBDIVISION nodes cannot have
                bids.
            LISTING_GROUP_ERROR_IN_ANOTHER_OPERATION (169):
                Product group operation is invalid because
                another operation targeting the same AdGroupId
                is failing.
            INVALID_LISTING_GROUP_HIERARCHY (106):
                Ad group is invalid due to the listing groups
                it contains.
            LISTING_GROUP_TREE_WAS_INVALID_BEFORE_MUTATION (170):
                Tree was invalid before the mutation.
            LISTING_GROUP_UNIT_CANNOT_HAVE_CHILDREN (107):
                Listing group unit cannot have children.
            LISTING_GROUP_SUBDIVISION_REQUIRES_OTHERS_CASE (108):
                Subdivided listing groups must have an
                "others" case.
            LISTING_GROUP_REQUIRES_SAME_DIMENSION_TYPE_AS_SIBLINGS (109):
                Dimension type of listing group must be the
                same as that of its siblings.
            LISTING_GROUP_ALREADY_EXISTS (110):
                Listing group cannot be added to the ad group
                because it already exists.
            LISTING_GROUP_DOES_NOT_EXIST (111):
                Listing group referenced in the operation was
                not found in the ad group.
            LISTING_GROUP_CANNOT_BE_REMOVED (112):
                Recursive removal failed because listing
                group subdivision is being created or modified
                in this request.
            INVALID_LISTING_GROUP_TYPE (113):
                Listing group type is not allowed for
                specified ad group criterion type.
            LISTING_GROUP_ADD_MAY_ONLY_USE_TEMP_ID (114):
                Listing group in an ADD operation specifies a
                non temporary criterion id.
            LISTING_SCOPE_TOO_LONG (115):
                The combined length of dimension values of
                the Listing scope criterion is too long.
            LISTING_SCOPE_TOO_MANY_DIMENSIONS (116):
                Listing scope contains too many dimensions.
            LISTING_GROUP_TOO_LONG (117):
                The combined length of dimension values of
                the Listing group criterion is too long.
            LISTING_GROUP_TREE_TOO_DEEP (118):
                Listing group tree is too deep.
            INVALID_LISTING_DIMENSION (119):
                Listing dimension is invalid (for example,
                dimension contains illegal value, dimension type
                is represented with wrong class, etc). Listing
                dimension value can not contain "==" or "&+".
            INVALID_LISTING_DIMENSION_TYPE (120):
                Listing dimension type is either invalid for campaigns of
                this type or cannot be used in the current context.
                BIDDING_CATEGORY_Lx and PRODUCT_TYPE_Lx dimensions must be
                used in ascending order of their levels: L1, L2, L3, L4,
                L5... The levels must be specified sequentially and start
                from L1. Furthermore, an "others" Listing group cannot be
                subdivided with a dimension of the same type but of a higher
                level ("others" BIDDING_CATEGORY_L3 can be subdivided with
                BRAND but not with BIDDING_CATEGORY_L4).
            ADVERTISER_NOT_ON_ALLOWLIST_FOR_COMBINED_AUDIENCE_ON_DISPLAY (127):
                Customer is not on allowlist for composite
                audience in display campaigns.
            CANNOT_TARGET_REMOVED_COMBINED_AUDIENCE (128):
                Cannot target on a removed combined audience.
            INVALID_COMBINED_AUDIENCE_ID (129):
                Combined audience ID is invalid.
            CANNOT_TARGET_REMOVED_CUSTOM_AUDIENCE (130):
                Can not target removed combined audience.
            HOTEL_CHECK_IN_DATE_RANGE_OVERLAPS_WITH_EXISTING_CRITERION (131):
                Range represented by hotel check-in date's
                start date and end date overlaps with an
                existing criterion.
            HOTEL_CHECK_IN_DATE_RANGE_START_DATE_TOO_EARLY (132):
                Start date is earlier than earliest allowed
                value of yesterday UTC.
            HOTEL_CHECK_IN_DATE_RANGE_END_DATE_TOO_LATE (133):
                End date later is than latest allowed day of
                330 days in the future UTC.
            HOTEL_CHECK_IN_DATE_RANGE_REVERSED (134):
                Start date is after end date.
            BROAD_MATCH_MODIFIER_KEYWORD_NOT_ALLOWED (135):
                Broad match modifier (BMM) keywords can no
                longer be created. See
                https://ads-developers.googleblog.com/2021/06/broad-match-modifier-upcoming-changes.html.
            ONE_AUDIENCE_ALLOWED_PER_ASSET_GROUP (136):
                Only one audience is allowed in an asset
                group.
            AUDIENCE_NOT_ELIGIBLE_FOR_CAMPAIGN_TYPE (137):
                Audience is not supported for the specified
                campaign type.
            AUDIENCE_NOT_ALLOWED_TO_ATTACH_WHEN_AUDIENCE_GROUPED_SET_TO_FALSE (138):
                Audience is not allowed to attach when use_audience_grouped
                bit is set to false.
            CANNOT_TARGET_CUSTOMER_MATCH_USER_LIST (139):
                Targeting is not allowed for Customer Match
                lists as per Customer Match policy. See
                https://support.google.com/google-ads/answer/6299717.
            NEGATIVE_KEYWORD_SHARED_SET_DOES_NOT_EXIST (145):
                Cannot create a negative keyword list
                criterion with a shared set that does not exist.
            CANNOT_ADD_REMOVED_NEGATIVE_KEYWORD_SHARED_SET (146):
                Cannot create a negative keyword list with
                deleted shared set.
            CANNOT_HAVE_MULTIPLE_NEGATIVE_KEYWORD_LIST_PER_ACCOUNT (147):
                Can only have one Negative Keyword List per
                account.
            CUSTOMER_CANNOT_ADD_CRITERION_OF_THIS_TYPE (149):
                Only allowlisted customers can add criteria
                of this type.
            CANNOT_TARGET_SIMILAR_USER_LIST (151):
                Targeting for Similar audiences is not
                supported, since this feature has been
                deprecated. See
                https://support.google.com/google-ads/answer/12463119
                to learn more.
            CANNOT_ADD_AUDIENCE_SEGMENT_CRITERION_WHEN_AUDIENCE_GROUPED_IS_SET (152):
                Audience segment criteria cannot be added when
                use_audience_grouped bit is set.
            ONE_AUDIENCE_ALLOWED_PER_AD_GROUP (153):
                Only one audience is allowed in an ad group.
            INVALID_DETAILED_DEMOGRAPHIC (154):
                Invalid detailed demographics criterion.
            CANNOT_RECOGNIZE_BRAND (155):
                The brand criteria has a brand input that is
                not recognized as a valid brand.
            BRAND_SHARED_SET_DOES_NOT_EXIST (156):
                The brand_list.shared_set_id references a shared set that
                does not exist.
            CANNOT_ADD_REMOVED_BRAND_SHARED_SET (157):
                Cannot create a brand list with deleted
                shared set.
            ONLY_EXCLUSION_BRAND_LIST_ALLOWED_FOR_CAMPAIGN_TYPE (158):
                Brand list can only be negatively targeted
                for the campaign type.
            LOCATION_TARGETING_NOT_ELIGIBLE_FOR_RESTRICTED_CAMPAIGN (166):
                Cannot positively target locations outside of
                restricted area for campaign.
            ONLY_INCLUSION_BRAND_LIST_ALLOWED_FOR_AD_GROUPS (171):
                Ad group level brand list criteria only
                support inclusionary targeting. Negative
                targeting at this level is not supported.
            CANNOT_ADD_REMOVED_PLACEMENT_LIST_SHARED_SET (172):
                Cannot create a placement list with deleted
                shared set.
            PLACEMENT_LIST_SHARED_SET_DOES_NOT_EXIST (173):
                The placement_list.shared_set_id references a shared set
                that does not exist.
            AI_MAX_MUST_BE_ENABLED (174):
                This feature is only available for AI Max
                campaigns.
            NOT_AVAILABLE_FOR_AI_MAX_CAMPAIGNS (175):
                This feature is not available for AI Max
                campaigns.
            MISSING_EU_POLITICAL_ADVERTISING_SELF_DECLARATION (176):
                The operation failed because the campaign is
                missing the self-declaration on political
                advertising status in the EU.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONCRETE_TYPE_REQUIRED = 2
        INVALID_EXCLUDED_CATEGORY = 3
        INVALID_KEYWORD_TEXT = 4
        KEYWORD_TEXT_TOO_LONG = 5
        KEYWORD_HAS_TOO_MANY_WORDS = 6
        KEYWORD_HAS_INVALID_CHARS = 7
        INVALID_PLACEMENT_URL = 8
        INVALID_USER_LIST = 9
        INVALID_USER_INTEREST = 10
        INVALID_FORMAT_FOR_PLACEMENT_URL = 11
        PLACEMENT_URL_IS_TOO_LONG = 12
        PLACEMENT_URL_HAS_ILLEGAL_CHAR = 13
        PLACEMENT_URL_HAS_MULTIPLE_SITES_IN_LINE = 14
        PLACEMENT_IS_NOT_AVAILABLE_FOR_TARGETING_OR_EXCLUSION = 15
        INVALID_TOPIC_PATH = 16
        INVALID_YOUTUBE_CHANNEL_ID = 17
        INVALID_YOUTUBE_VIDEO_ID = 18
        YOUTUBE_VERTICAL_CHANNEL_DEPRECATED = 19
        YOUTUBE_DEMOGRAPHIC_CHANNEL_DEPRECATED = 20
        YOUTUBE_URL_UNSUPPORTED = 21
        CANNOT_EXCLUDE_CRITERIA_TYPE = 22
        CANNOT_ADD_CRITERIA_TYPE = 23
        CANNOT_EXCLUDE_SIMILAR_USER_LIST = 26
        CANNOT_ADD_CLOSED_USER_LIST = 27
        CANNOT_ADD_DISPLAY_ONLY_LISTS_TO_SEARCH_ONLY_CAMPAIGNS = 28
        CANNOT_ADD_DISPLAY_ONLY_LISTS_TO_SEARCH_CAMPAIGNS = 29
        CANNOT_ADD_DISPLAY_ONLY_LISTS_TO_SHOPPING_CAMPAIGNS = 30
        CANNOT_ADD_USER_INTERESTS_TO_SEARCH_CAMPAIGNS = 31
        CANNOT_SET_BIDS_ON_CRITERION_TYPE_IN_SEARCH_CAMPAIGNS = 32
        CANNOT_ADD_URLS_TO_CRITERION_TYPE_FOR_CAMPAIGN_TYPE = 33
        INVALID_COMBINED_AUDIENCE = 122
        INVALID_CUSTOM_AFFINITY = 96
        INVALID_CUSTOM_INTENT = 97
        INVALID_CUSTOM_AUDIENCE = 121
        INVALID_IP_ADDRESS = 34
        INVALID_IP_FORMAT = 35
        INVALID_MOBILE_APP = 36
        INVALID_MOBILE_APP_CATEGORY = 37
        INVALID_CRITERION_ID = 38
        CANNOT_TARGET_CRITERION = 39
        CANNOT_TARGET_OBSOLETE_CRITERION = 40
        CRITERION_ID_AND_TYPE_MISMATCH = 41
        INVALID_PROXIMITY_RADIUS = 42
        INVALID_PROXIMITY_RADIUS_UNITS = 43
        INVALID_STREETADDRESS_LENGTH = 44
        INVALID_CITYNAME_LENGTH = 45
        INVALID_REGIONCODE_LENGTH = 46
        INVALID_REGIONNAME_LENGTH = 47
        INVALID_POSTALCODE_LENGTH = 48
        INVALID_COUNTRY_CODE = 49
        INVALID_LATITUDE = 50
        INVALID_LONGITUDE = 51
        PROXIMITY_GEOPOINT_AND_ADDRESS_BOTH_CANNOT_BE_NULL = 52
        INVALID_PROXIMITY_ADDRESS = 53
        INVALID_USER_DOMAIN_NAME = 54
        CRITERION_PARAMETER_TOO_LONG = 55
        AD_SCHEDULE_TIME_INTERVALS_OVERLAP = 56
        AD_SCHEDULE_INTERVAL_CANNOT_SPAN_MULTIPLE_DAYS = 57
        AD_SCHEDULE_INVALID_TIME_INTERVAL = 58
        AD_SCHEDULE_EXCEEDED_INTERVALS_PER_DAY_LIMIT = 59
        AD_SCHEDULE_CRITERION_ID_MISMATCHING_FIELDS = 60
        CANNOT_BID_MODIFY_CRITERION_TYPE = 61
        CANNOT_BID_MODIFY_CRITERION_CAMPAIGN_OPTED_OUT = 62
        CANNOT_BID_MODIFY_NEGATIVE_CRITERION = 63
        BID_MODIFIER_ALREADY_EXISTS = 64
        FEED_ID_NOT_ALLOWED = 65
        ACCOUNT_INELIGIBLE_FOR_CRITERIA_TYPE = 66
        CRITERIA_TYPE_INVALID_FOR_BIDDING_STRATEGY = 67
        CANNOT_EXCLUDE_CRITERION = 68
        CANNOT_REMOVE_CRITERION = 69
        INVALID_PRODUCT_BIDDING_CATEGORY = 76
        MISSING_SHOPPING_SETTING = 77
        INVALID_MATCHING_FUNCTION = 78
        LOCATION_FILTER_NOT_ALLOWED = 79
        INVALID_FEED_FOR_LOCATION_FILTER = 98
        LOCATION_FILTER_INVALID = 80
        CANNOT_SET_GEO_TARGET_CONSTANTS_WITH_FEED_ITEM_SETS = 123
        CANNOT_SET_BOTH_ASSET_SET_AND_FEED = 140
        CANNOT_SET_FEED_OR_FEED_ITEM_SETS_FOR_CUSTOMER = 142
        CANNOT_SET_ASSET_SET_FIELD_FOR_CUSTOMER = 150
        CANNOT_SET_GEO_TARGET_CONSTANTS_WITH_ASSET_SETS = 143
        CANNOT_SET_ASSET_SETS_WITH_FEED_ITEM_SETS = 144
        INVALID_LOCATION_GROUP_ASSET_SET = 141
        INVALID_LOCATION_GROUP_RADIUS = 124
        INVALID_LOCATION_GROUP_RADIUS_UNIT = 125
        CANNOT_ATTACH_CRITERIA_AT_CAMPAIGN_AND_ADGROUP = 81
        HOTEL_LENGTH_OF_STAY_OVERLAPS_WITH_EXISTING_CRITERION = 82
        HOTEL_ADVANCE_BOOKING_WINDOW_OVERLAPS_WITH_EXISTING_CRITERION = 83
        FIELD_INCOMPATIBLE_WITH_NEGATIVE_TARGETING = 84
        INVALID_WEBPAGE_CONDITION = 85
        INVALID_WEBPAGE_CONDITION_URL = 86
        WEBPAGE_CONDITION_URL_CANNOT_BE_EMPTY = 87
        WEBPAGE_CONDITION_URL_UNSUPPORTED_PROTOCOL = 88
        WEBPAGE_CONDITION_URL_CANNOT_BE_IP_ADDRESS = 89
        WEBPAGE_CONDITION_URL_DOMAIN_NOT_CONSISTENT_WITH_CAMPAIGN_SETTING = 90
        WEBPAGE_CONDITION_URL_CANNOT_BE_PUBLIC_SUFFIX = 91
        WEBPAGE_CONDITION_URL_INVALID_PUBLIC_SUFFIX = 92
        WEBPAGE_CONDITION_URL_VALUE_TRACK_VALUE_NOT_SUPPORTED = 93
        WEBPAGE_CRITERION_URL_EQUALS_CAN_HAVE_ONLY_ONE_CONDITION = 94
        WEBPAGE_CRITERION_NOT_SUPPORTED_ON_NON_DSA_AD_GROUP = 95
        CANNOT_TARGET_USER_LIST_FOR_SMART_DISPLAY_CAMPAIGNS = 99
        CANNOT_TARGET_PLACEMENTS_FOR_SEARCH_CAMPAIGNS = 126
        LISTING_SCOPE_TOO_MANY_DIMENSION_TYPES = 100
        LISTING_SCOPE_TOO_MANY_IN_OPERATORS = 101
        LISTING_SCOPE_IN_OPERATOR_NOT_SUPPORTED = 102
        DUPLICATE_LISTING_DIMENSION_TYPE = 103
        DUPLICATE_LISTING_DIMENSION_VALUE = 104
        CANNOT_SET_BIDS_ON_LISTING_GROUP_SUBDIVISION = 105
        LISTING_GROUP_ERROR_IN_ANOTHER_OPERATION = 169
        INVALID_LISTING_GROUP_HIERARCHY = 106
        LISTING_GROUP_TREE_WAS_INVALID_BEFORE_MUTATION = 170
        LISTING_GROUP_UNIT_CANNOT_HAVE_CHILDREN = 107
        LISTING_GROUP_SUBDIVISION_REQUIRES_OTHERS_CASE = 108
        LISTING_GROUP_REQUIRES_SAME_DIMENSION_TYPE_AS_SIBLINGS = 109
        LISTING_GROUP_ALREADY_EXISTS = 110
        LISTING_GROUP_DOES_NOT_EXIST = 111
        LISTING_GROUP_CANNOT_BE_REMOVED = 112
        INVALID_LISTING_GROUP_TYPE = 113
        LISTING_GROUP_ADD_MAY_ONLY_USE_TEMP_ID = 114
        LISTING_SCOPE_TOO_LONG = 115
        LISTING_SCOPE_TOO_MANY_DIMENSIONS = 116
        LISTING_GROUP_TOO_LONG = 117
        LISTING_GROUP_TREE_TOO_DEEP = 118
        INVALID_LISTING_DIMENSION = 119
        INVALID_LISTING_DIMENSION_TYPE = 120
        ADVERTISER_NOT_ON_ALLOWLIST_FOR_COMBINED_AUDIENCE_ON_DISPLAY = 127
        CANNOT_TARGET_REMOVED_COMBINED_AUDIENCE = 128
        INVALID_COMBINED_AUDIENCE_ID = 129
        CANNOT_TARGET_REMOVED_CUSTOM_AUDIENCE = 130
        HOTEL_CHECK_IN_DATE_RANGE_OVERLAPS_WITH_EXISTING_CRITERION = 131
        HOTEL_CHECK_IN_DATE_RANGE_START_DATE_TOO_EARLY = 132
        HOTEL_CHECK_IN_DATE_RANGE_END_DATE_TOO_LATE = 133
        HOTEL_CHECK_IN_DATE_RANGE_REVERSED = 134
        BROAD_MATCH_MODIFIER_KEYWORD_NOT_ALLOWED = 135
        ONE_AUDIENCE_ALLOWED_PER_ASSET_GROUP = 136
        AUDIENCE_NOT_ELIGIBLE_FOR_CAMPAIGN_TYPE = 137
        AUDIENCE_NOT_ALLOWED_TO_ATTACH_WHEN_AUDIENCE_GROUPED_SET_TO_FALSE = 138
        CANNOT_TARGET_CUSTOMER_MATCH_USER_LIST = 139
        NEGATIVE_KEYWORD_SHARED_SET_DOES_NOT_EXIST = 145
        CANNOT_ADD_REMOVED_NEGATIVE_KEYWORD_SHARED_SET = 146
        CANNOT_HAVE_MULTIPLE_NEGATIVE_KEYWORD_LIST_PER_ACCOUNT = 147
        CUSTOMER_CANNOT_ADD_CRITERION_OF_THIS_TYPE = 149
        CANNOT_TARGET_SIMILAR_USER_LIST = 151
        CANNOT_ADD_AUDIENCE_SEGMENT_CRITERION_WHEN_AUDIENCE_GROUPED_IS_SET = 152
        ONE_AUDIENCE_ALLOWED_PER_AD_GROUP = 153
        INVALID_DETAILED_DEMOGRAPHIC = 154
        CANNOT_RECOGNIZE_BRAND = 155
        BRAND_SHARED_SET_DOES_NOT_EXIST = 156
        CANNOT_ADD_REMOVED_BRAND_SHARED_SET = 157
        ONLY_EXCLUSION_BRAND_LIST_ALLOWED_FOR_CAMPAIGN_TYPE = 158
        LOCATION_TARGETING_NOT_ELIGIBLE_FOR_RESTRICTED_CAMPAIGN = 166
        ONLY_INCLUSION_BRAND_LIST_ALLOWED_FOR_AD_GROUPS = 171
        CANNOT_ADD_REMOVED_PLACEMENT_LIST_SHARED_SET = 172
        PLACEMENT_LIST_SHARED_SET_DOES_NOT_EXIST = 173
        AI_MAX_MUST_BE_ENABLED = 174
        NOT_AVAILABLE_FOR_AI_MAX_CAMPAIGNS = 175
        MISSING_EU_POLITICAL_ADVERTISING_SELF_DECLARATION = 176


__all__ = tuple(sorted(__protobuf__.manifest))

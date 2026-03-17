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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "AdErrorEnum",
    },
)


class AdErrorEnum(proto.Message):
    r"""Container for enum describing possible ad errors."""

    class AdError(proto.Enum):
        r"""Enum describing possible ad errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            AD_CUSTOMIZERS_NOT_SUPPORTED_FOR_AD_TYPE (2):
                Ad customizers are not supported for ad type.
            APPROXIMATELY_TOO_LONG (3):
                Estimating character sizes the string is too
                long.
            APPROXIMATELY_TOO_SHORT (4):
                Estimating character sizes the string is too
                short.
            BAD_SNIPPET (5):
                There is a problem with the snippet.
            CANNOT_MODIFY_AD (6):
                Cannot modify an ad.
            CANNOT_SET_BUSINESS_NAME_IF_URL_SET (7):
                business name and url cannot be set at the
                same time
            CANNOT_SET_FIELD (8):
                The specified field is incompatible with this
                ad's type or settings.
            CANNOT_SET_FIELD_WITH_ORIGIN_AD_ID_SET (9):
                Cannot set field when originAdId is set.
            CANNOT_SET_FIELD_WITH_AD_ID_SET_FOR_SHARING (10):
                Cannot set field when an existing ad id is
                set for sharing.
            CANNOT_SET_ALLOW_FLEXIBLE_COLOR_FALSE (11):
                Cannot set allowFlexibleColor false if no
                color is provided by user.
            CANNOT_SET_COLOR_CONTROL_WHEN_NATIVE_FORMAT_SETTING (12):
                When user select native, no color control is
                allowed because we will always respect publisher
                color for native format serving.
            CANNOT_SET_URL (13):
                Cannot specify a url for the ad type
            CANNOT_SET_WITHOUT_FINAL_URLS (14):
                Cannot specify a tracking or mobile url
                without also setting final urls
            CANNOT_SET_WITH_FINAL_URLS (15):
                Cannot specify a legacy url and a final url
                simultaneously
            CANNOT_SET_WITH_URL_DATA (17):
                Cannot specify a urls in UrlData and in
                template fields simultaneously.
            CANNOT_USE_AD_SUBCLASS_FOR_OPERATOR (18):
                This operator cannot be used with a subclass
                of Ad.
            CUSTOMER_NOT_APPROVED_MOBILEADS (19):
                Customer is not approved for mobile ads.
            CUSTOMER_NOT_APPROVED_THIRDPARTY_ADS (20):
                Customer is not approved for 3PAS richmedia
                ads.
            CUSTOMER_NOT_APPROVED_THIRDPARTY_REDIRECT_ADS (21):
                Customer is not approved for 3PAS redirect
                richmedia (Ad Exchange) ads.
            CUSTOMER_NOT_ELIGIBLE (22):
                Not an eligible customer
            CUSTOMER_NOT_ELIGIBLE_FOR_UPDATING_BEACON_URL (23):
                Customer is not eligible for updating beacon
                url
            DIMENSION_ALREADY_IN_UNION (24):
                There already exists an ad with the same
                dimensions in the union.
            DIMENSION_MUST_BE_SET (25):
                Ad's dimension must be set before setting
                union dimension.
            DIMENSION_NOT_IN_UNION (26):
                Ad's dimension must be included in the union
                dimensions.
            DISPLAY_URL_CANNOT_BE_SPECIFIED (27):
                Display Url cannot be specified (applies to
                Ad Exchange Ads)
            DOMESTIC_PHONE_NUMBER_FORMAT (28):
                Telephone number contains invalid characters
                or invalid format. Re-enter your number using
                digits (0-9), dashes (-), and parentheses only.
            EMERGENCY_PHONE_NUMBER (29):
                Emergency telephone numbers are not allowed.
                Enter a valid domestic phone number to connect
                customers to your business.
            EMPTY_FIELD (30):
                A required field was not specified or is an
                empty string.
            FEED_ATTRIBUTE_MUST_HAVE_MAPPING_FOR_TYPE_ID (31):
                A feed attribute referenced in an ad
                customizer tag is not in the ad customizer
                mapping for the feed.
            FEED_ATTRIBUTE_MAPPING_TYPE_MISMATCH (32):
                The ad customizer field mapping for the feed
                attribute does not match the expected field
                type.
            ILLEGAL_AD_CUSTOMIZER_TAG_USE (33):
                The use of ad customizer tags in the ad text
                is disallowed. Details in trigger.
            ILLEGAL_TAG_USE (34):
                Tags of the form {PH_x}, where x is a number, are disallowed
                in ad text.
            INCONSISTENT_DIMENSIONS (35):
                The dimensions of the ad are specified or
                derived in multiple ways and are not consistent.
            INCONSISTENT_STATUS_IN_TEMPLATE_UNION (36):
                The status cannot differ among template ads
                of the same union.
            INCORRECT_LENGTH (37):
                The length of the string is not valid.
            INELIGIBLE_FOR_UPGRADE (38):
                The ad is ineligible for upgrade.
            INVALID_AD_ADDRESS_CAMPAIGN_TARGET (39):
                User cannot create mobile ad for countries
                targeted in specified campaign.
            INVALID_AD_TYPE (40):
                Invalid Ad type. A specific type of Ad is
                required.
            INVALID_ATTRIBUTES_FOR_MOBILE_IMAGE (41):
                Headline, description or phone cannot be
                present when creating mobile image ad.
            INVALID_ATTRIBUTES_FOR_MOBILE_TEXT (42):
                Image cannot be present when creating mobile
                text ad.
            INVALID_CALL_TO_ACTION_TEXT (43):
                Invalid call to action text.
            INVALID_CHARACTER_FOR_URL (44):
                Invalid character in URL.
            INVALID_COUNTRY_CODE (45):
                Creative's country code is not valid.
            INVALID_EXPANDED_DYNAMIC_SEARCH_AD_TAG (47):
                Invalid use of Expanded Dynamic Search Ads
                tags ({lpurl} etc.)
            INVALID_INPUT (48):
                An input error whose real reason was not
                properly mapped (should not happen).
            INVALID_MARKUP_LANGUAGE (49):
                An invalid markup language was entered.
            INVALID_MOBILE_CARRIER (50):
                An invalid mobile carrier was entered.
            INVALID_MOBILE_CARRIER_TARGET (51):
                Specified mobile carriers target a country
                not targeted by the campaign.
            INVALID_NUMBER_OF_ELEMENTS (52):
                Wrong number of elements for given element
                type
            INVALID_PHONE_NUMBER_FORMAT (53):
                The format of the telephone number is
                incorrect. Re-enter the number using the correct
                format.
            INVALID_RICH_MEDIA_CERTIFIED_VENDOR_FORMAT_ID (54):
                The certified vendor format id is incorrect.
            INVALID_TEMPLATE_DATA (55):
                The template ad data contains validation
                errors.
            INVALID_TEMPLATE_ELEMENT_FIELD_TYPE (56):
                The template field doesn't have have the
                correct type.
            INVALID_TEMPLATE_ID (57):
                Invalid template id.
            LINE_TOO_WIDE (58):
                After substituting replacement strings, the
                line is too wide.
            MISSING_AD_CUSTOMIZER_MAPPING (59):
                The feed referenced must have ad customizer
                mapping to be used in a customizer tag.
            MISSING_ADDRESS_COMPONENT (60):
                Missing address component in template element
                address field.
            MISSING_ADVERTISEMENT_NAME (61):
                An ad name must be entered.
            MISSING_BUSINESS_NAME (62):
                Business name must be entered.
            MISSING_DESCRIPTION1 (63):
                Description (line 2) must be entered.
            MISSING_DESCRIPTION2 (64):
                Description (line 3) must be entered.
            MISSING_DESTINATION_URL_TAG (65):
                The destination url must contain at least one
                tag (for example, {lpurl})
            MISSING_LANDING_PAGE_URL_TAG (66):
                The tracking url template of
                ExpandedDynamicSearchAd must contain at least
                one tag. (for example, {lpurl})
            MISSING_DIMENSION (67):
                A valid dimension must be specified for this
                ad.
            MISSING_DISPLAY_URL (68):
                A display URL must be entered.
            MISSING_HEADLINE (69):
                Headline must be entered.
            MISSING_HEIGHT (70):
                A height must be entered.
            MISSING_IMAGE (71):
                An image must be entered.
            MISSING_MARKETING_IMAGE_OR_PRODUCT_VIDEOS (72):
                Marketing image or product videos are
                required.
            MISSING_MARKUP_LANGUAGES (73):
                The markup language in which your site is
                written must be entered.
            MISSING_MOBILE_CARRIER (74):
                A mobile carrier must be entered.
            MISSING_PHONE (75):
                Phone number must be entered.
            MISSING_REQUIRED_TEMPLATE_FIELDS (76):
                Missing required template fields
            MISSING_TEMPLATE_FIELD_VALUE (77):
                Missing a required field value
            MISSING_TEXT (78):
                The ad must have text.
            MISSING_VISIBLE_URL (79):
                A visible URL must be entered.
            MISSING_WIDTH (80):
                A width must be entered.
            MULTIPLE_DISTINCT_FEEDS_UNSUPPORTED (81):
                Only 1 feed can be used as the source of ad
                customizer substitutions in a single ad.
            MUST_USE_TEMP_AD_UNION_ID_ON_ADD (82):
                TempAdUnionId must be use when adding
                template ads.
            TOO_LONG (83):
                The string has too many characters.
            TOO_SHORT (84):
                The string has too few characters.
            UNION_DIMENSIONS_CANNOT_CHANGE (85):
                Ad union dimensions cannot change for saved
                ads.
            UNKNOWN_ADDRESS_COMPONENT (86):
                Address component is not {country, lat, lng}.
            UNKNOWN_FIELD_NAME (87):
                Unknown unique field name
            UNKNOWN_UNIQUE_NAME (88):
                Unknown unique name (template element type
                specifier)
            UNSUPPORTED_DIMENSIONS (89):
                Unsupported ad dimension
            URL_INVALID_SCHEME (90):
                URL starts with an invalid scheme.
            URL_INVALID_TOP_LEVEL_DOMAIN (91):
                URL ends with an invalid top-level domain
                name.
            URL_MALFORMED (92):
                URL contains illegal characters.
            URL_NO_HOST (93):
                URL must contain a host name.
            URL_NOT_EQUIVALENT (94):
                URL not equivalent during upgrade.
            URL_HOST_NAME_TOO_LONG (95):
                URL host name too long to be stored as
                visible URL (applies to Ad Exchange ads)
            URL_NO_SCHEME (96):
                URL must start with a scheme.
            URL_NO_TOP_LEVEL_DOMAIN (97):
                URL should end in a valid domain extension,
                such as .com or .net.
            URL_PATH_NOT_ALLOWED (98):
                URL must not end with a path.
            URL_PORT_NOT_ALLOWED (99):
                URL must not specify a port.
            URL_QUERY_NOT_ALLOWED (100):
                URL must not contain a query.
            URL_SCHEME_BEFORE_EXPANDED_DYNAMIC_SEARCH_AD_TAG (102):
                A url scheme is not allowed in front of tag
                in tracking url template (for example,
                http://{lpurl})
            USER_DOES_NOT_HAVE_ACCESS_TO_TEMPLATE (103):
                The user does not have permissions to create
                a template ad for the given template.
            INCONSISTENT_EXPANDABLE_SETTINGS (104):
                Expandable setting is inconsistent/wrong. For
                example, an AdX ad is invalid if it has a
                expandable vendor format but no expanding
                directions specified, or expanding directions is
                specified, but the vendor format is not
                expandable.
            INVALID_FORMAT (105):
                Format is invalid
            INVALID_FIELD_TEXT (106):
                The text of this field did not match a
                pattern of allowed values.
            ELEMENT_NOT_PRESENT (107):
                Template element is mising
            IMAGE_ERROR (108):
                Error occurred during image processing
            VALUE_NOT_IN_RANGE (109):
                The value is not within the valid range
            FIELD_NOT_PRESENT (110):
                Template element field is not present
            ADDRESS_NOT_COMPLETE (111):
                Address is incomplete
            ADDRESS_INVALID (112):
                Invalid address
            VIDEO_RETRIEVAL_ERROR (113):
                Error retrieving specified video
            AUDIO_ERROR (114):
                Error processing audio
            INVALID_YOUTUBE_DISPLAY_URL (115):
                Display URL is incorrect for YouTube PYV ads
            TOO_MANY_PRODUCT_IMAGES (116):
                Too many product Images in GmailAd
            TOO_MANY_PRODUCT_VIDEOS (117):
                Too many product Videos in GmailAd
            INCOMPATIBLE_AD_TYPE_AND_DEVICE_PREFERENCE (118):
                The device preference is not compatible with
                the ad type
            CALLTRACKING_NOT_SUPPORTED_FOR_COUNTRY (119):
                Call tracking is not supported for specified
                country.
            CARRIER_SPECIFIC_SHORT_NUMBER_NOT_ALLOWED (120):
                Carrier specific short number is not allowed.
            DISALLOWED_NUMBER_TYPE (121):
                Specified phone number type is disallowed.
            PHONE_NUMBER_NOT_SUPPORTED_FOR_COUNTRY (122):
                Phone number not supported for country.
            PHONE_NUMBER_NOT_SUPPORTED_WITH_CALLTRACKING_FOR_COUNTRY (123):
                Phone number not supported with call tracking
                enabled for country.
            PREMIUM_RATE_NUMBER_NOT_ALLOWED (124):
                Premium rate phone number is not allowed.
            VANITY_PHONE_NUMBER_NOT_ALLOWED (125):
                Vanity phone number is not allowed.
            INVALID_CALL_CONVERSION_TYPE_ID (126):
                Invalid call conversion type id.
            CANNOT_DISABLE_CALL_CONVERSION_AND_SET_CONVERSION_TYPE_ID (127):
                Cannot disable call conversion and set
                conversion type id.
            CANNOT_SET_PATH2_WITHOUT_PATH1 (128):
                Cannot set path2 without path1.
            MISSING_DYNAMIC_SEARCH_ADS_SETTING_DOMAIN_NAME (129):
                Missing domain name in campaign setting when
                adding expanded dynamic search ad.
            INCOMPATIBLE_WITH_RESTRICTION_TYPE (130):
                The associated ad is not compatible with
                restriction type.
            CUSTOMER_CONSENT_FOR_CALL_RECORDING_REQUIRED (131):
                Consent for call recording is required for
                creating/updating call only ads. See
                https://support.google.com/google-ads/answer/7412639.
            MISSING_IMAGE_OR_MEDIA_BUNDLE (132):
                Either an image or a media bundle is required
                in a display upload ad.
            PRODUCT_TYPE_NOT_SUPPORTED_IN_THIS_CAMPAIGN (133):
                The display upload product type is not
                supported in this campaign.
            PLACEHOLDER_CANNOT_HAVE_EMPTY_DEFAULT_VALUE (134):
                The default value of an ad placeholder can
                not be the empty string.
            PLACEHOLDER_COUNTDOWN_FUNCTION_CANNOT_HAVE_DEFAULT_VALUE (135):
                Ad placeholders with countdown functions must
                not have a default value.
            PLACEHOLDER_DEFAULT_VALUE_MISSING (136):
                A previous ad placeholder that had a default
                value was found which means that all
                (non-countdown) placeholders must have a default
                value. This ad placeholder does not have a
                default value.
            UNEXPECTED_PLACEHOLDER_DEFAULT_VALUE (137):
                A previous ad placeholder that did not have a
                default value was found which means that no
                placeholders may have a default value. This ad
                placeholder does have a default value.
            AD_CUSTOMIZERS_MAY_NOT_BE_ADJACENT (138):
                Two ad customizers may not be directly
                adjacent in an ad text. They must be separated
                by at least one character.
            UPDATING_AD_WITH_NO_ENABLED_ASSOCIATION (139):
                The ad is not associated with any enabled
                AdGroupAd, and cannot be updated.
            CALL_AD_VERIFICATION_URL_FINAL_URL_DOES_NOT_HAVE_SAME_DOMAIN (140):
                Call Ad verification url and final url don't
                have same domain.
            CALL_AD_FINAL_URL_AND_VERIFICATION_URL_CANNOT_BOTH_BE_EMPTY (154):
                Final url and verification url cannot both be
                empty for call ads.
            TOO_MANY_AD_CUSTOMIZERS (141):
                Too many ad customizers in one asset.
            INVALID_AD_CUSTOMIZER_FORMAT (142):
                The ad customizer tag is recognized, but the
                format is invalid.
            NESTED_AD_CUSTOMIZER_SYNTAX (143):
                Customizer tags cannot be nested.
            UNSUPPORTED_AD_CUSTOMIZER_SYNTAX (144):
                The ad customizer syntax used in the ad is
                not supported.
            UNPAIRED_BRACE_IN_AD_CUSTOMIZER_TAG (145):
                There exists unpaired brace in the ad
                customizer tag.
            MORE_THAN_ONE_COUNTDOWN_TAG_TYPE_EXISTS (146):
                More than one type of countdown tag exists
                among all text lines.
            DATE_TIME_IN_COUNTDOWN_TAG_IS_INVALID (147):
                Date time in the countdown tag is invalid.
            DATE_TIME_IN_COUNTDOWN_TAG_IS_PAST (148):
                Date time in the countdown tag is in the
                past.
            UNRECOGNIZED_AD_CUSTOMIZER_TAG_FOUND (149):
                Cannot recognize the ad customizer tag.
            CUSTOMIZER_TYPE_FORBIDDEN_FOR_FIELD (150):
                Customizer type forbidden for this field.
            INVALID_CUSTOMIZER_ATTRIBUTE_NAME (151):
                Customizer attribute name is invalid.
            STORE_MISMATCH (152):
                App store value does not match the value of
                the app store in the app specified in the
                campaign.
            MISSING_REQUIRED_IMAGE_ASPECT_RATIO (153):
                Missing required image aspect ratio.
            MISMATCHED_ASPECT_RATIOS (155):
                Aspect ratios mismatch between different
                assets.
            DUPLICATE_IMAGE_ACROSS_CAROUSEL_CARDS (156):
                Images must be unique between different
                carousel card assets.
            INVALID_YOUTUBE_VIDEO_ASSET_ID_FOR_VIDEO_ADS_SEQUENCING (157):
                For video ads sequencing, YouTube video asset ID has to be
                defined in
                ``campaign.video_campaign_settings.video_ad_sequence.steps.asset_id``.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_CUSTOMIZERS_NOT_SUPPORTED_FOR_AD_TYPE = 2
        APPROXIMATELY_TOO_LONG = 3
        APPROXIMATELY_TOO_SHORT = 4
        BAD_SNIPPET = 5
        CANNOT_MODIFY_AD = 6
        CANNOT_SET_BUSINESS_NAME_IF_URL_SET = 7
        CANNOT_SET_FIELD = 8
        CANNOT_SET_FIELD_WITH_ORIGIN_AD_ID_SET = 9
        CANNOT_SET_FIELD_WITH_AD_ID_SET_FOR_SHARING = 10
        CANNOT_SET_ALLOW_FLEXIBLE_COLOR_FALSE = 11
        CANNOT_SET_COLOR_CONTROL_WHEN_NATIVE_FORMAT_SETTING = 12
        CANNOT_SET_URL = 13
        CANNOT_SET_WITHOUT_FINAL_URLS = 14
        CANNOT_SET_WITH_FINAL_URLS = 15
        CANNOT_SET_WITH_URL_DATA = 17
        CANNOT_USE_AD_SUBCLASS_FOR_OPERATOR = 18
        CUSTOMER_NOT_APPROVED_MOBILEADS = 19
        CUSTOMER_NOT_APPROVED_THIRDPARTY_ADS = 20
        CUSTOMER_NOT_APPROVED_THIRDPARTY_REDIRECT_ADS = 21
        CUSTOMER_NOT_ELIGIBLE = 22
        CUSTOMER_NOT_ELIGIBLE_FOR_UPDATING_BEACON_URL = 23
        DIMENSION_ALREADY_IN_UNION = 24
        DIMENSION_MUST_BE_SET = 25
        DIMENSION_NOT_IN_UNION = 26
        DISPLAY_URL_CANNOT_BE_SPECIFIED = 27
        DOMESTIC_PHONE_NUMBER_FORMAT = 28
        EMERGENCY_PHONE_NUMBER = 29
        EMPTY_FIELD = 30
        FEED_ATTRIBUTE_MUST_HAVE_MAPPING_FOR_TYPE_ID = 31
        FEED_ATTRIBUTE_MAPPING_TYPE_MISMATCH = 32
        ILLEGAL_AD_CUSTOMIZER_TAG_USE = 33
        ILLEGAL_TAG_USE = 34
        INCONSISTENT_DIMENSIONS = 35
        INCONSISTENT_STATUS_IN_TEMPLATE_UNION = 36
        INCORRECT_LENGTH = 37
        INELIGIBLE_FOR_UPGRADE = 38
        INVALID_AD_ADDRESS_CAMPAIGN_TARGET = 39
        INVALID_AD_TYPE = 40
        INVALID_ATTRIBUTES_FOR_MOBILE_IMAGE = 41
        INVALID_ATTRIBUTES_FOR_MOBILE_TEXT = 42
        INVALID_CALL_TO_ACTION_TEXT = 43
        INVALID_CHARACTER_FOR_URL = 44
        INVALID_COUNTRY_CODE = 45
        INVALID_EXPANDED_DYNAMIC_SEARCH_AD_TAG = 47
        INVALID_INPUT = 48
        INVALID_MARKUP_LANGUAGE = 49
        INVALID_MOBILE_CARRIER = 50
        INVALID_MOBILE_CARRIER_TARGET = 51
        INVALID_NUMBER_OF_ELEMENTS = 52
        INVALID_PHONE_NUMBER_FORMAT = 53
        INVALID_RICH_MEDIA_CERTIFIED_VENDOR_FORMAT_ID = 54
        INVALID_TEMPLATE_DATA = 55
        INVALID_TEMPLATE_ELEMENT_FIELD_TYPE = 56
        INVALID_TEMPLATE_ID = 57
        LINE_TOO_WIDE = 58
        MISSING_AD_CUSTOMIZER_MAPPING = 59
        MISSING_ADDRESS_COMPONENT = 60
        MISSING_ADVERTISEMENT_NAME = 61
        MISSING_BUSINESS_NAME = 62
        MISSING_DESCRIPTION1 = 63
        MISSING_DESCRIPTION2 = 64
        MISSING_DESTINATION_URL_TAG = 65
        MISSING_LANDING_PAGE_URL_TAG = 66
        MISSING_DIMENSION = 67
        MISSING_DISPLAY_URL = 68
        MISSING_HEADLINE = 69
        MISSING_HEIGHT = 70
        MISSING_IMAGE = 71
        MISSING_MARKETING_IMAGE_OR_PRODUCT_VIDEOS = 72
        MISSING_MARKUP_LANGUAGES = 73
        MISSING_MOBILE_CARRIER = 74
        MISSING_PHONE = 75
        MISSING_REQUIRED_TEMPLATE_FIELDS = 76
        MISSING_TEMPLATE_FIELD_VALUE = 77
        MISSING_TEXT = 78
        MISSING_VISIBLE_URL = 79
        MISSING_WIDTH = 80
        MULTIPLE_DISTINCT_FEEDS_UNSUPPORTED = 81
        MUST_USE_TEMP_AD_UNION_ID_ON_ADD = 82
        TOO_LONG = 83
        TOO_SHORT = 84
        UNION_DIMENSIONS_CANNOT_CHANGE = 85
        UNKNOWN_ADDRESS_COMPONENT = 86
        UNKNOWN_FIELD_NAME = 87
        UNKNOWN_UNIQUE_NAME = 88
        UNSUPPORTED_DIMENSIONS = 89
        URL_INVALID_SCHEME = 90
        URL_INVALID_TOP_LEVEL_DOMAIN = 91
        URL_MALFORMED = 92
        URL_NO_HOST = 93
        URL_NOT_EQUIVALENT = 94
        URL_HOST_NAME_TOO_LONG = 95
        URL_NO_SCHEME = 96
        URL_NO_TOP_LEVEL_DOMAIN = 97
        URL_PATH_NOT_ALLOWED = 98
        URL_PORT_NOT_ALLOWED = 99
        URL_QUERY_NOT_ALLOWED = 100
        URL_SCHEME_BEFORE_EXPANDED_DYNAMIC_SEARCH_AD_TAG = 102
        USER_DOES_NOT_HAVE_ACCESS_TO_TEMPLATE = 103
        INCONSISTENT_EXPANDABLE_SETTINGS = 104
        INVALID_FORMAT = 105
        INVALID_FIELD_TEXT = 106
        ELEMENT_NOT_PRESENT = 107
        IMAGE_ERROR = 108
        VALUE_NOT_IN_RANGE = 109
        FIELD_NOT_PRESENT = 110
        ADDRESS_NOT_COMPLETE = 111
        ADDRESS_INVALID = 112
        VIDEO_RETRIEVAL_ERROR = 113
        AUDIO_ERROR = 114
        INVALID_YOUTUBE_DISPLAY_URL = 115
        TOO_MANY_PRODUCT_IMAGES = 116
        TOO_MANY_PRODUCT_VIDEOS = 117
        INCOMPATIBLE_AD_TYPE_AND_DEVICE_PREFERENCE = 118
        CALLTRACKING_NOT_SUPPORTED_FOR_COUNTRY = 119
        CARRIER_SPECIFIC_SHORT_NUMBER_NOT_ALLOWED = 120
        DISALLOWED_NUMBER_TYPE = 121
        PHONE_NUMBER_NOT_SUPPORTED_FOR_COUNTRY = 122
        PHONE_NUMBER_NOT_SUPPORTED_WITH_CALLTRACKING_FOR_COUNTRY = 123
        PREMIUM_RATE_NUMBER_NOT_ALLOWED = 124
        VANITY_PHONE_NUMBER_NOT_ALLOWED = 125
        INVALID_CALL_CONVERSION_TYPE_ID = 126
        CANNOT_DISABLE_CALL_CONVERSION_AND_SET_CONVERSION_TYPE_ID = 127
        CANNOT_SET_PATH2_WITHOUT_PATH1 = 128
        MISSING_DYNAMIC_SEARCH_ADS_SETTING_DOMAIN_NAME = 129
        INCOMPATIBLE_WITH_RESTRICTION_TYPE = 130
        CUSTOMER_CONSENT_FOR_CALL_RECORDING_REQUIRED = 131
        MISSING_IMAGE_OR_MEDIA_BUNDLE = 132
        PRODUCT_TYPE_NOT_SUPPORTED_IN_THIS_CAMPAIGN = 133
        PLACEHOLDER_CANNOT_HAVE_EMPTY_DEFAULT_VALUE = 134
        PLACEHOLDER_COUNTDOWN_FUNCTION_CANNOT_HAVE_DEFAULT_VALUE = 135
        PLACEHOLDER_DEFAULT_VALUE_MISSING = 136
        UNEXPECTED_PLACEHOLDER_DEFAULT_VALUE = 137
        AD_CUSTOMIZERS_MAY_NOT_BE_ADJACENT = 138
        UPDATING_AD_WITH_NO_ENABLED_ASSOCIATION = 139
        CALL_AD_VERIFICATION_URL_FINAL_URL_DOES_NOT_HAVE_SAME_DOMAIN = 140
        CALL_AD_FINAL_URL_AND_VERIFICATION_URL_CANNOT_BOTH_BE_EMPTY = 154
        TOO_MANY_AD_CUSTOMIZERS = 141
        INVALID_AD_CUSTOMIZER_FORMAT = 142
        NESTED_AD_CUSTOMIZER_SYNTAX = 143
        UNSUPPORTED_AD_CUSTOMIZER_SYNTAX = 144
        UNPAIRED_BRACE_IN_AD_CUSTOMIZER_TAG = 145
        MORE_THAN_ONE_COUNTDOWN_TAG_TYPE_EXISTS = 146
        DATE_TIME_IN_COUNTDOWN_TAG_IS_INVALID = 147
        DATE_TIME_IN_COUNTDOWN_TAG_IS_PAST = 148
        UNRECOGNIZED_AD_CUSTOMIZER_TAG_FOUND = 149
        CUSTOMIZER_TYPE_FORBIDDEN_FOR_FIELD = 150
        INVALID_CUSTOMIZER_ATTRIBUTE_NAME = 151
        STORE_MISMATCH = 152
        MISSING_REQUIRED_IMAGE_ASPECT_RATIO = 153
        MISMATCHED_ASPECT_RATIOS = 155
        DUPLICATE_IMAGE_ACROSS_CAROUSEL_CARDS = 156
        INVALID_YOUTUBE_VIDEO_ASSET_ID_FOR_VIDEO_ADS_SEQUENCING = 157


__all__ = tuple(sorted(__protobuf__.manifest))

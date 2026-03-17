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
        "FeedItemValidationErrorEnum",
    },
)


class FeedItemValidationErrorEnum(proto.Message):
    r"""Container for enum describing possible validation errors of a
    feed item.

    """

    class FeedItemValidationError(proto.Enum):
        r"""The possible validation errors of a feed item.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            STRING_TOO_SHORT (2):
                String is too short.
            STRING_TOO_LONG (3):
                String is too long.
            VALUE_NOT_SPECIFIED (4):
                Value is not provided.
            INVALID_DOMESTIC_PHONE_NUMBER_FORMAT (5):
                Phone number format is invalid for region.
            INVALID_PHONE_NUMBER (6):
                String does not represent a phone number.
            PHONE_NUMBER_NOT_SUPPORTED_FOR_COUNTRY (7):
                Phone number format is not compatible with
                country code.
            PREMIUM_RATE_NUMBER_NOT_ALLOWED (8):
                Premium rate number is not allowed.
            DISALLOWED_NUMBER_TYPE (9):
                Phone number type is not allowed.
            VALUE_OUT_OF_RANGE (10):
                Specified value is outside of the valid
                range.
            CALLTRACKING_NOT_SUPPORTED_FOR_COUNTRY (11):
                Call tracking is not supported in the
                selected country.
            CUSTOMER_NOT_IN_ALLOWLIST_FOR_CALLTRACKING (99):
                Customer is not on the allow-list for call
                tracking.
            INVALID_COUNTRY_CODE (13):
                Country code is invalid.
            INVALID_APP_ID (14):
                The specified mobile app id is invalid.
            MISSING_ATTRIBUTES_FOR_FIELDS (15):
                Some required field attributes are missing.
            INVALID_TYPE_ID (16):
                Invalid email button type for email
                extension.
            INVALID_EMAIL_ADDRESS (17):
                Email address is invalid.
            INVALID_HTTPS_URL (18):
                The HTTPS URL in email extension is invalid.
            MISSING_DELIVERY_ADDRESS (19):
                Delivery address is missing from email
                extension.
            START_DATE_AFTER_END_DATE (20):
                FeedItem scheduling start date comes after
                end date.
            MISSING_FEED_ITEM_START_TIME (21):
                FeedItem scheduling start time is missing.
            MISSING_FEED_ITEM_END_TIME (22):
                FeedItem scheduling end time is missing.
            MISSING_FEED_ITEM_ID (23):
                Cannot compute system attributes on a
                FeedItem that has no FeedItemId.
            VANITY_PHONE_NUMBER_NOT_ALLOWED (24):
                Call extension vanity phone numbers are not
                supported.
            INVALID_REVIEW_EXTENSION_SNIPPET (25):
                Invalid review text.
            INVALID_NUMBER_FORMAT (26):
                Invalid format for numeric value in ad
                parameter.
            INVALID_DATE_FORMAT (27):
                Invalid format for date value in ad
                parameter.
            INVALID_PRICE_FORMAT (28):
                Invalid format for price value in ad
                parameter.
            UNKNOWN_PLACEHOLDER_FIELD (29):
                Unrecognized type given for value in ad
                parameter.
            MISSING_ENHANCED_SITELINK_DESCRIPTION_LINE (30):
                Enhanced sitelinks must have both description
                lines specified.
            REVIEW_EXTENSION_SOURCE_INELIGIBLE (31):
                Review source is ineligible.
            HYPHENS_IN_REVIEW_EXTENSION_SNIPPET (32):
                Review text cannot contain hyphens or dashes.
            DOUBLE_QUOTES_IN_REVIEW_EXTENSION_SNIPPET (33):
                Review text cannot contain double quote
                characters.
            QUOTES_IN_REVIEW_EXTENSION_SNIPPET (34):
                Review text cannot contain quote characters.
            INVALID_FORM_ENCODED_PARAMS (35):
                Parameters are encoded in the wrong format.
            INVALID_URL_PARAMETER_NAME (36):
                URL parameter name must contain only letters,
                numbers, underscores, and dashes.
            NO_GEOCODING_RESULT (37):
                Cannot find address location.
            SOURCE_NAME_IN_REVIEW_EXTENSION_TEXT (38):
                Review extension text has source name.
            CARRIER_SPECIFIC_SHORT_NUMBER_NOT_ALLOWED (39):
                Some phone numbers can be shorter than usual.
                Some of these short numbers are
                carrier-specific, and we disallow those in ad
                extensions because they will not be available to
                all users.
            INVALID_PLACEHOLDER_FIELD_ID (40):
                Triggered when a request references a
                placeholder field id that does not exist.
            INVALID_URL_TAG (41):
                URL contains invalid ValueTrack tags or
                format.
            LIST_TOO_LONG (42):
                Provided list exceeds acceptable size.
            INVALID_ATTRIBUTES_COMBINATION (43):
                Certain combinations of attributes aren't
                allowed to be specified in the same feed item.
            DUPLICATE_VALUES (44):
                An attribute has the same value repeatedly.
            INVALID_CALL_CONVERSION_ACTION_ID (45):
                Advertisers can link a conversion action with
                a phone number to indicate that sufficiently
                long calls forwarded to that phone number should
                be counted as conversions of the specified type.
                This is an error message indicating that the
                conversion action specified is invalid (for
                example, the conversion action does not exist
                within the appropriate Google Ads account, or it
                is a type of conversion not appropriate to phone
                call conversions).
            CANNOT_SET_WITHOUT_FINAL_URLS (46):
                Tracking template requires final url to be
                set.
            APP_ID_DOESNT_EXIST_IN_APP_STORE (47):
                An app id was provided that doesn't exist in
                the given app store.
            INVALID_FINAL_URL (48):
                Invalid U2 final url.
            INVALID_TRACKING_URL (49):
                Invalid U2 tracking url.
            INVALID_FINAL_URL_FOR_APP_DOWNLOAD_URL (50):
                Final URL should start from App download URL.
            LIST_TOO_SHORT (51):
                List provided is too short.
            INVALID_USER_ACTION (52):
                User Action field has invalid value.
            INVALID_TYPE_NAME (53):
                Type field has invalid value.
            INVALID_EVENT_CHANGE_STATUS (54):
                Change status for event is invalid.
            INVALID_SNIPPETS_HEADER (55):
                The header of a structured snippets extension
                is not one of the valid headers.
            INVALID_ANDROID_APP_LINK (56):
                Android app link is not formatted correctly
            NUMBER_TYPE_WITH_CALLTRACKING_NOT_SUPPORTED_FOR_COUNTRY (57):
                Phone number incompatible with call tracking
                for country.
            RESERVED_KEYWORD_OTHER (58):
                The input is identical to a reserved keyword
            DUPLICATE_OPTION_LABELS (59):
                Each option label in the message extension
                must be unique.
            DUPLICATE_OPTION_PREFILLS (60):
                Each option prefill in the message extension
                must be unique.
            UNEQUAL_LIST_LENGTHS (61):
                In message extensions, the number of optional
                labels and optional prefills must be the same.
            INCONSISTENT_CURRENCY_CODES (62):
                All currency codes in an ad extension must be
                the same.
            PRICE_EXTENSION_HAS_DUPLICATED_HEADERS (63):
                Headers in price extension are not unique.
            ITEM_HAS_DUPLICATED_HEADER_AND_DESCRIPTION (64):
                Header and description in an item are the
                same.
            PRICE_EXTENSION_HAS_TOO_FEW_ITEMS (65):
                Price extension has too few items.
            UNSUPPORTED_VALUE (66):
                The given value is not supported.
            INVALID_FINAL_MOBILE_URL (67):
                Invalid final mobile url.
            INVALID_KEYWORDLESS_AD_RULE_LABEL (68):
                The given string value of Label contains
                invalid characters
            VALUE_TRACK_PARAMETER_NOT_SUPPORTED (69):
                The given URL contains value track
                parameters.
            UNSUPPORTED_VALUE_IN_SELECTED_LANGUAGE (70):
                The given value is not supported in the
                selected language of an extension.
            INVALID_IOS_APP_LINK (71):
                The iOS app link is not formatted correctly.
            MISSING_IOS_APP_LINK_OR_IOS_APP_STORE_ID (72):
                iOS app link or iOS app store id is missing.
            PROMOTION_INVALID_TIME (73):
                Promotion time is invalid.
            PROMOTION_CANNOT_SET_PERCENT_OFF_AND_MONEY_AMOUNT_OFF (74):
                Both the percent off and money amount off
                fields are set.
            PROMOTION_CANNOT_SET_PROMOTION_CODE_AND_ORDERS_OVER_AMOUNT (75):
                Both the promotion code and orders over
                amount fields are set.
            TOO_MANY_DECIMAL_PLACES_SPECIFIED (76):
                Too many decimal places are specified.
            AD_CUSTOMIZERS_NOT_ALLOWED (77):
                Ad Customizers are present and not allowed.
            INVALID_LANGUAGE_CODE (78):
                Language code is not valid.
            UNSUPPORTED_LANGUAGE (79):
                Language is not supported.
            IF_FUNCTION_NOT_ALLOWED (80):
                IF Function is present and not allowed.
            INVALID_FINAL_URL_SUFFIX (81):
                Final url suffix is not valid.
            INVALID_TAG_IN_FINAL_URL_SUFFIX (82):
                Final url suffix contains an invalid tag.
            INVALID_FINAL_URL_SUFFIX_FORMAT (83):
                Final url suffix is formatted incorrectly.
            CUSTOMER_CONSENT_FOR_CALL_RECORDING_REQUIRED (84):
                Consent for call recording, which is required
                for the use of call extensions, was not provided
                by the advertiser. See
                https://support.google.com/google-ads/answer/7412639.
            ONLY_ONE_DELIVERY_OPTION_IS_ALLOWED (85):
                Multiple message delivery options are set.
            NO_DELIVERY_OPTION_IS_SET (86):
                No message delivery option is set.
            INVALID_CONVERSION_REPORTING_STATE (87):
                String value of conversion reporting state
                field is not valid.
            IMAGE_SIZE_WRONG (88):
                Image size is not right.
            EMAIL_DELIVERY_NOT_AVAILABLE_IN_COUNTRY (89):
                Email delivery is not supported in the
                country specified in the country code field.
            AUTO_REPLY_NOT_AVAILABLE_IN_COUNTRY (90):
                Auto reply is not supported in the country
                specified in the country code field.
            INVALID_LATITUDE_VALUE (91):
                Invalid value specified for latitude.
            INVALID_LONGITUDE_VALUE (92):
                Invalid value specified for longitude.
            TOO_MANY_LABELS (93):
                Too many label fields provided.
            INVALID_IMAGE_URL (94):
                Invalid image url.
            MISSING_LATITUDE_VALUE (95):
                Latitude value is missing.
            MISSING_LONGITUDE_VALUE (96):
                Longitude value is missing.
            ADDRESS_NOT_FOUND (97):
                Unable to find address.
            ADDRESS_NOT_TARGETABLE (98):
                Cannot target provided address.
            INVALID_ASSET_ID (100):
                The specified asset ID does not exist.
            INCOMPATIBLE_ASSET_TYPE (101):
                The asset type cannot be set for the field.
            IMAGE_ERROR_UNEXPECTED_SIZE (102):
                The image has unexpected size.
            IMAGE_ERROR_ASPECT_RATIO_NOT_ALLOWED (103):
                The image aspect ratio is not allowed.
            IMAGE_ERROR_FILE_TOO_LARGE (104):
                The image file is too large.
            IMAGE_ERROR_FORMAT_NOT_ALLOWED (105):
                The image format is unsupported.
            IMAGE_ERROR_CONSTRAINTS_VIOLATED (106):
                Image violates constraints without more
                details.
            IMAGE_ERROR_SERVER_ERROR (107):
                An error occurred when validating image.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        STRING_TOO_SHORT = 2
        STRING_TOO_LONG = 3
        VALUE_NOT_SPECIFIED = 4
        INVALID_DOMESTIC_PHONE_NUMBER_FORMAT = 5
        INVALID_PHONE_NUMBER = 6
        PHONE_NUMBER_NOT_SUPPORTED_FOR_COUNTRY = 7
        PREMIUM_RATE_NUMBER_NOT_ALLOWED = 8
        DISALLOWED_NUMBER_TYPE = 9
        VALUE_OUT_OF_RANGE = 10
        CALLTRACKING_NOT_SUPPORTED_FOR_COUNTRY = 11
        CUSTOMER_NOT_IN_ALLOWLIST_FOR_CALLTRACKING = 99
        INVALID_COUNTRY_CODE = 13
        INVALID_APP_ID = 14
        MISSING_ATTRIBUTES_FOR_FIELDS = 15
        INVALID_TYPE_ID = 16
        INVALID_EMAIL_ADDRESS = 17
        INVALID_HTTPS_URL = 18
        MISSING_DELIVERY_ADDRESS = 19
        START_DATE_AFTER_END_DATE = 20
        MISSING_FEED_ITEM_START_TIME = 21
        MISSING_FEED_ITEM_END_TIME = 22
        MISSING_FEED_ITEM_ID = 23
        VANITY_PHONE_NUMBER_NOT_ALLOWED = 24
        INVALID_REVIEW_EXTENSION_SNIPPET = 25
        INVALID_NUMBER_FORMAT = 26
        INVALID_DATE_FORMAT = 27
        INVALID_PRICE_FORMAT = 28
        UNKNOWN_PLACEHOLDER_FIELD = 29
        MISSING_ENHANCED_SITELINK_DESCRIPTION_LINE = 30
        REVIEW_EXTENSION_SOURCE_INELIGIBLE = 31
        HYPHENS_IN_REVIEW_EXTENSION_SNIPPET = 32
        DOUBLE_QUOTES_IN_REVIEW_EXTENSION_SNIPPET = 33
        QUOTES_IN_REVIEW_EXTENSION_SNIPPET = 34
        INVALID_FORM_ENCODED_PARAMS = 35
        INVALID_URL_PARAMETER_NAME = 36
        NO_GEOCODING_RESULT = 37
        SOURCE_NAME_IN_REVIEW_EXTENSION_TEXT = 38
        CARRIER_SPECIFIC_SHORT_NUMBER_NOT_ALLOWED = 39
        INVALID_PLACEHOLDER_FIELD_ID = 40
        INVALID_URL_TAG = 41
        LIST_TOO_LONG = 42
        INVALID_ATTRIBUTES_COMBINATION = 43
        DUPLICATE_VALUES = 44
        INVALID_CALL_CONVERSION_ACTION_ID = 45
        CANNOT_SET_WITHOUT_FINAL_URLS = 46
        APP_ID_DOESNT_EXIST_IN_APP_STORE = 47
        INVALID_FINAL_URL = 48
        INVALID_TRACKING_URL = 49
        INVALID_FINAL_URL_FOR_APP_DOWNLOAD_URL = 50
        LIST_TOO_SHORT = 51
        INVALID_USER_ACTION = 52
        INVALID_TYPE_NAME = 53
        INVALID_EVENT_CHANGE_STATUS = 54
        INVALID_SNIPPETS_HEADER = 55
        INVALID_ANDROID_APP_LINK = 56
        NUMBER_TYPE_WITH_CALLTRACKING_NOT_SUPPORTED_FOR_COUNTRY = 57
        RESERVED_KEYWORD_OTHER = 58
        DUPLICATE_OPTION_LABELS = 59
        DUPLICATE_OPTION_PREFILLS = 60
        UNEQUAL_LIST_LENGTHS = 61
        INCONSISTENT_CURRENCY_CODES = 62
        PRICE_EXTENSION_HAS_DUPLICATED_HEADERS = 63
        ITEM_HAS_DUPLICATED_HEADER_AND_DESCRIPTION = 64
        PRICE_EXTENSION_HAS_TOO_FEW_ITEMS = 65
        UNSUPPORTED_VALUE = 66
        INVALID_FINAL_MOBILE_URL = 67
        INVALID_KEYWORDLESS_AD_RULE_LABEL = 68
        VALUE_TRACK_PARAMETER_NOT_SUPPORTED = 69
        UNSUPPORTED_VALUE_IN_SELECTED_LANGUAGE = 70
        INVALID_IOS_APP_LINK = 71
        MISSING_IOS_APP_LINK_OR_IOS_APP_STORE_ID = 72
        PROMOTION_INVALID_TIME = 73
        PROMOTION_CANNOT_SET_PERCENT_OFF_AND_MONEY_AMOUNT_OFF = 74
        PROMOTION_CANNOT_SET_PROMOTION_CODE_AND_ORDERS_OVER_AMOUNT = 75
        TOO_MANY_DECIMAL_PLACES_SPECIFIED = 76
        AD_CUSTOMIZERS_NOT_ALLOWED = 77
        INVALID_LANGUAGE_CODE = 78
        UNSUPPORTED_LANGUAGE = 79
        IF_FUNCTION_NOT_ALLOWED = 80
        INVALID_FINAL_URL_SUFFIX = 81
        INVALID_TAG_IN_FINAL_URL_SUFFIX = 82
        INVALID_FINAL_URL_SUFFIX_FORMAT = 83
        CUSTOMER_CONSENT_FOR_CALL_RECORDING_REQUIRED = 84
        ONLY_ONE_DELIVERY_OPTION_IS_ALLOWED = 85
        NO_DELIVERY_OPTION_IS_SET = 86
        INVALID_CONVERSION_REPORTING_STATE = 87
        IMAGE_SIZE_WRONG = 88
        EMAIL_DELIVERY_NOT_AVAILABLE_IN_COUNTRY = 89
        AUTO_REPLY_NOT_AVAILABLE_IN_COUNTRY = 90
        INVALID_LATITUDE_VALUE = 91
        INVALID_LONGITUDE_VALUE = 92
        TOO_MANY_LABELS = 93
        INVALID_IMAGE_URL = 94
        MISSING_LATITUDE_VALUE = 95
        MISSING_LONGITUDE_VALUE = 96
        ADDRESS_NOT_FOUND = 97
        ADDRESS_NOT_TARGETABLE = 98
        INVALID_ASSET_ID = 100
        INCOMPATIBLE_ASSET_TYPE = 101
        IMAGE_ERROR_UNEXPECTED_SIZE = 102
        IMAGE_ERROR_ASPECT_RATIO_NOT_ALLOWED = 103
        IMAGE_ERROR_FILE_TOO_LARGE = 104
        IMAGE_ERROR_FORMAT_NOT_ALLOWED = 105
        IMAGE_ERROR_CONSTRAINTS_VIOLATED = 106
        IMAGE_ERROR_SERVER_ERROR = 107


__all__ = tuple(sorted(__protobuf__.manifest))

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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "ExtensionSettingErrorEnum",
    },
)


class ExtensionSettingErrorEnum(proto.Message):
    r"""Container for enum describing validation errors of extension
    settings.

    """

    class ExtensionSettingError(proto.Enum):
        r"""Enum describing possible extension setting errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            EXTENSIONS_REQUIRED (2):
                A platform restriction was provided without
                input extensions or existing extensions.
            FEED_TYPE_EXTENSION_TYPE_MISMATCH (3):
                The provided feed type does not correspond to
                the provided extensions.
            INVALID_FEED_TYPE (4):
                The provided feed type cannot be used.
            INVALID_FEED_TYPE_FOR_CUSTOMER_EXTENSION_SETTING (5):
                The provided feed type cannot be used at the
                customer level.
            CANNOT_CHANGE_FEED_ITEM_ON_CREATE (6):
                Cannot change a feed item field on a CREATE
                operation.
            CANNOT_UPDATE_NEWLY_CREATED_EXTENSION (7):
                Cannot update an extension that is not
                already in this setting.
            NO_EXISTING_AD_GROUP_EXTENSION_SETTING_FOR_TYPE (8):
                There is no existing AdGroupExtensionSetting
                for this type.
            NO_EXISTING_CAMPAIGN_EXTENSION_SETTING_FOR_TYPE (9):
                There is no existing CampaignExtensionSetting
                for this type.
            NO_EXISTING_CUSTOMER_EXTENSION_SETTING_FOR_TYPE (10):
                There is no existing CustomerExtensionSetting
                for this type.
            AD_GROUP_EXTENSION_SETTING_ALREADY_EXISTS (11):
                The AdGroupExtensionSetting already exists.
                UPDATE should be used to modify the existing
                AdGroupExtensionSetting.
            CAMPAIGN_EXTENSION_SETTING_ALREADY_EXISTS (12):
                The CampaignExtensionSetting already exists.
                UPDATE should be used to modify the existing
                CampaignExtensionSetting.
            CUSTOMER_EXTENSION_SETTING_ALREADY_EXISTS (13):
                The CustomerExtensionSetting already exists.
                UPDATE should be used to modify the existing
                CustomerExtensionSetting.
            AD_GROUP_FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE (14):
                An active ad group feed already exists for
                this place holder type.
            CAMPAIGN_FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE (15):
                An active campaign feed already exists for
                this place holder type.
            CUSTOMER_FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE (16):
                An active customer feed already exists for
                this place holder type.
            VALUE_OUT_OF_RANGE (17):
                Value is not within the accepted range.
            CANNOT_SET_FIELD_WITH_FINAL_URLS (18):
                Cannot simultaneously set specified field
                with final urls.
            FINAL_URLS_NOT_SET (19):
                Must set field with final urls.
            INVALID_PHONE_NUMBER (20):
                Phone number for a call extension is invalid.
            PHONE_NUMBER_NOT_SUPPORTED_FOR_COUNTRY (21):
                Phone number for a call extension is not
                supported for the given country code.
            CARRIER_SPECIFIC_SHORT_NUMBER_NOT_ALLOWED (22):
                A carrier specific number in short format is
                not allowed for call extensions.
            PREMIUM_RATE_NUMBER_NOT_ALLOWED (23):
                Premium rate numbers are not allowed for call
                extensions.
            DISALLOWED_NUMBER_TYPE (24):
                Phone number type for a call extension is not
                allowed.
            INVALID_DOMESTIC_PHONE_NUMBER_FORMAT (25):
                Phone number for a call extension does not
                meet domestic format requirements.
            VANITY_PHONE_NUMBER_NOT_ALLOWED (26):
                Vanity phone numbers (for example, those
                including letters) are not allowed for call
                extensions.
            INVALID_COUNTRY_CODE (27):
                Country code provided for a call extension is
                invalid.
            INVALID_CALL_CONVERSION_TYPE_ID (28):
                Call conversion type id provided for a call
                extension is invalid.
            CUSTOMER_NOT_IN_ALLOWLIST_FOR_CALLTRACKING (69):
                For a call extension, the customer is not on
                the allow-list for call tracking.
            CALLTRACKING_NOT_SUPPORTED_FOR_COUNTRY (30):
                Call tracking is not supported for the given
                country for a call extension.
            INVALID_APP_ID (31):
                App id provided for an app extension is
                invalid.
            QUOTES_IN_REVIEW_EXTENSION_SNIPPET (32):
                Quotation marks present in the review text
                for a review extension.
            HYPHENS_IN_REVIEW_EXTENSION_SNIPPET (33):
                Hyphen character present in the review text
                for a review extension.
            REVIEW_EXTENSION_SOURCE_NOT_ELIGIBLE (34):
                A blocked review source name or url was
                provided for a review extension.
            SOURCE_NAME_IN_REVIEW_EXTENSION_TEXT (35):
                Review source name should not be found in the
                review text.
            MISSING_FIELD (36):
                Field must be set.
            INCONSISTENT_CURRENCY_CODES (37):
                Inconsistent currency codes.
            PRICE_EXTENSION_HAS_DUPLICATED_HEADERS (38):
                Price extension cannot have duplicated
                headers.
            PRICE_ITEM_HAS_DUPLICATED_HEADER_AND_DESCRIPTION (39):
                Price item cannot have duplicated header and
                description.
            PRICE_EXTENSION_HAS_TOO_FEW_ITEMS (40):
                Price extension has too few items
            PRICE_EXTENSION_HAS_TOO_MANY_ITEMS (41):
                Price extension has too many items
            UNSUPPORTED_VALUE (42):
                The input value is not currently supported.
            INVALID_DEVICE_PREFERENCE (43):
                Unknown or unsupported device preference.
            INVALID_SCHEDULE_END (45):
                Invalid feed item schedule end time (for
                example, endHour = 24 and endMinute != 0).
            DATE_TIME_MUST_BE_IN_ACCOUNT_TIME_ZONE (47):
                Date time zone does not match the account's
                time zone.
            OVERLAPPING_SCHEDULES_NOT_ALLOWED (48):
                Overlapping feed item schedule times (for
                example, 7-10AM and 8-11AM) are not allowed.
            SCHEDULE_END_NOT_AFTER_START (49):
                Feed item schedule end time must be after
                start time.
            TOO_MANY_SCHEDULES_PER_DAY (50):
                There are too many feed item schedules per
                day.
            DUPLICATE_EXTENSION_FEED_ITEM_EDIT (51):
                Cannot edit the same extension feed item more
                than once in the same request.
            INVALID_SNIPPETS_HEADER (52):
                Invalid structured snippet header.
            PHONE_NUMBER_NOT_SUPPORTED_WITH_CALLTRACKING_FOR_COUNTRY (53):
                Phone number with call tracking enabled is
                not supported for the specified country.
            CAMPAIGN_TARGETING_MISMATCH (54):
                The targeted adgroup must belong to the
                targeted campaign.
            CANNOT_OPERATE_ON_REMOVED_FEED (55):
                The feed used by the ExtensionSetting is
                removed and cannot be operated on. Remove the
                ExtensionSetting to allow a new one to be
                created using an active feed.
            EXTENSION_TYPE_REQUIRED (56):
                The ExtensionFeedItem type is required for
                this operation.
            INCOMPATIBLE_UNDERLYING_MATCHING_FUNCTION (57):
                The matching function that links the
                extension feed to the customer, campaign, or ad
                group is not compatible with the
                ExtensionSetting services.
            START_DATE_AFTER_END_DATE (58):
                Start date must be before end date.
            INVALID_PRICE_FORMAT (59):
                Input price is not in a valid format.
            PROMOTION_INVALID_TIME (60):
                The promotion time is invalid.
            PROMOTION_CANNOT_SET_PERCENT_DISCOUNT_AND_MONEY_DISCOUNT (61):
                Cannot set both percent discount and money
                discount fields.
            PROMOTION_CANNOT_SET_PROMOTION_CODE_AND_ORDERS_OVER_AMOUNT (62):
                Cannot set both promotion code and orders
                over amount fields.
            TOO_MANY_DECIMAL_PLACES_SPECIFIED (63):
                This field has too many decimal places
                specified.
            INVALID_LANGUAGE_CODE (64):
                The language code is not valid.
            UNSUPPORTED_LANGUAGE (65):
                The language is not supported.
            CUSTOMER_CONSENT_FOR_CALL_RECORDING_REQUIRED (66):
                Customer hasn't consented for call recording,
                which is required for adding/updating call
                extensions. See
                https://support.google.com/google-ads/answer/7412639.
            EXTENSION_SETTING_UPDATE_IS_A_NOOP (67):
                The UPDATE operation does not specify any
                fields other than the resource name in the
                update mask.
            DISALLOWED_TEXT (68):
                The extension contains text which has been
                prohibited on policy grounds.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        EXTENSIONS_REQUIRED = 2
        FEED_TYPE_EXTENSION_TYPE_MISMATCH = 3
        INVALID_FEED_TYPE = 4
        INVALID_FEED_TYPE_FOR_CUSTOMER_EXTENSION_SETTING = 5
        CANNOT_CHANGE_FEED_ITEM_ON_CREATE = 6
        CANNOT_UPDATE_NEWLY_CREATED_EXTENSION = 7
        NO_EXISTING_AD_GROUP_EXTENSION_SETTING_FOR_TYPE = 8
        NO_EXISTING_CAMPAIGN_EXTENSION_SETTING_FOR_TYPE = 9
        NO_EXISTING_CUSTOMER_EXTENSION_SETTING_FOR_TYPE = 10
        AD_GROUP_EXTENSION_SETTING_ALREADY_EXISTS = 11
        CAMPAIGN_EXTENSION_SETTING_ALREADY_EXISTS = 12
        CUSTOMER_EXTENSION_SETTING_ALREADY_EXISTS = 13
        AD_GROUP_FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE = 14
        CAMPAIGN_FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE = 15
        CUSTOMER_FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE = 16
        VALUE_OUT_OF_RANGE = 17
        CANNOT_SET_FIELD_WITH_FINAL_URLS = 18
        FINAL_URLS_NOT_SET = 19
        INVALID_PHONE_NUMBER = 20
        PHONE_NUMBER_NOT_SUPPORTED_FOR_COUNTRY = 21
        CARRIER_SPECIFIC_SHORT_NUMBER_NOT_ALLOWED = 22
        PREMIUM_RATE_NUMBER_NOT_ALLOWED = 23
        DISALLOWED_NUMBER_TYPE = 24
        INVALID_DOMESTIC_PHONE_NUMBER_FORMAT = 25
        VANITY_PHONE_NUMBER_NOT_ALLOWED = 26
        INVALID_COUNTRY_CODE = 27
        INVALID_CALL_CONVERSION_TYPE_ID = 28
        CUSTOMER_NOT_IN_ALLOWLIST_FOR_CALLTRACKING = 69
        CALLTRACKING_NOT_SUPPORTED_FOR_COUNTRY = 30
        INVALID_APP_ID = 31
        QUOTES_IN_REVIEW_EXTENSION_SNIPPET = 32
        HYPHENS_IN_REVIEW_EXTENSION_SNIPPET = 33
        REVIEW_EXTENSION_SOURCE_NOT_ELIGIBLE = 34
        SOURCE_NAME_IN_REVIEW_EXTENSION_TEXT = 35
        MISSING_FIELD = 36
        INCONSISTENT_CURRENCY_CODES = 37
        PRICE_EXTENSION_HAS_DUPLICATED_HEADERS = 38
        PRICE_ITEM_HAS_DUPLICATED_HEADER_AND_DESCRIPTION = 39
        PRICE_EXTENSION_HAS_TOO_FEW_ITEMS = 40
        PRICE_EXTENSION_HAS_TOO_MANY_ITEMS = 41
        UNSUPPORTED_VALUE = 42
        INVALID_DEVICE_PREFERENCE = 43
        INVALID_SCHEDULE_END = 45
        DATE_TIME_MUST_BE_IN_ACCOUNT_TIME_ZONE = 47
        OVERLAPPING_SCHEDULES_NOT_ALLOWED = 48
        SCHEDULE_END_NOT_AFTER_START = 49
        TOO_MANY_SCHEDULES_PER_DAY = 50
        DUPLICATE_EXTENSION_FEED_ITEM_EDIT = 51
        INVALID_SNIPPETS_HEADER = 52
        PHONE_NUMBER_NOT_SUPPORTED_WITH_CALLTRACKING_FOR_COUNTRY = 53
        CAMPAIGN_TARGETING_MISMATCH = 54
        CANNOT_OPERATE_ON_REMOVED_FEED = 55
        EXTENSION_TYPE_REQUIRED = 56
        INCOMPATIBLE_UNDERLYING_MATCHING_FUNCTION = 57
        START_DATE_AFTER_END_DATE = 58
        INVALID_PRICE_FORMAT = 59
        PROMOTION_INVALID_TIME = 60
        PROMOTION_CANNOT_SET_PERCENT_DISCOUNT_AND_MONEY_DISCOUNT = 61
        PROMOTION_CANNOT_SET_PROMOTION_CODE_AND_ORDERS_OVER_AMOUNT = 62
        TOO_MANY_DECIMAL_PLACES_SPECIFIED = 63
        INVALID_LANGUAGE_CODE = 64
        UNSUPPORTED_LANGUAGE = 65
        CUSTOMER_CONSENT_FOR_CALL_RECORDING_REQUIRED = 66
        EXTENSION_SETTING_UPDATE_IS_A_NOOP = 67
        DISALLOWED_TEXT = 68


__all__ = tuple(sorted(__protobuf__.manifest))

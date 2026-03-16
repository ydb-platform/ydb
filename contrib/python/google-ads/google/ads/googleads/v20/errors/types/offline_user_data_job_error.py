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
        "OfflineUserDataJobErrorEnum",
    },
)


class OfflineUserDataJobErrorEnum(proto.Message):
    r"""Container for enum describing possible offline user data job
    errors.

    """

    class OfflineUserDataJobError(proto.Enum):
        r"""Enum describing possible request errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_USER_LIST_ID (3):
                The user list ID provided for the job is
                invalid.
            INVALID_USER_LIST_TYPE (4):
                Type of the user list is not applicable for
                the job.
            NOT_ON_ALLOWLIST_FOR_USER_ID (33):
                Customer is not allowisted for using user ID
                in upload data.
            INCOMPATIBLE_UPLOAD_KEY_TYPE (6):
                Upload data is not compatible with the upload
                key type of the associated user list.
            MISSING_USER_IDENTIFIER (7):
                The user identifier is missing valid data.
            INVALID_MOBILE_ID_FORMAT (8):
                The mobile ID is malformed.
            TOO_MANY_USER_IDENTIFIERS (9):
                Maximum number of user identifiers allowed
                per request is 100,000 and per operation is 20.
            NOT_ON_ALLOWLIST_FOR_STORE_SALES_DIRECT (31):
                Customer is not on the allow-list for store
                sales direct data.
            NOT_ON_ALLOWLIST_FOR_UNIFIED_STORE_SALES (32):
                Customer is not on the allow-list for unified
                store sales data.
            INVALID_PARTNER_ID (11):
                The partner ID in store sales direct metadata
                is invalid.
            INVALID_ENCODING (12):
                The data in user identifier should not be
                encoded.
            INVALID_COUNTRY_CODE (13):
                The country code is invalid.
            INCOMPATIBLE_USER_IDENTIFIER (14):
                Incompatible user identifier when using third_party_user_id
                for store sales direct first party data or not using
                third_party_user_id for store sales third party data.
            FUTURE_TRANSACTION_TIME (15):
                A transaction time in the future is not
                allowed.
            INVALID_CONVERSION_ACTION (16):
                The conversion_action specified in transaction_attributes is
                used to report conversions to a conversion action configured
                in Google Ads. This error indicates there is no such
                conversion action in the account.
            MOBILE_ID_NOT_SUPPORTED (17):
                Mobile ID is not supported for store sales
                direct data.
            INVALID_OPERATION_ORDER (18):
                When a remove-all operation is provided, it
                has to be the first operation of the operation
                list.
            CONFLICTING_OPERATION (19):
                Mixing creation and removal of offline data
                in the same job is not allowed.
            EXTERNAL_UPDATE_ID_ALREADY_EXISTS (21):
                The external update ID already exists.
            JOB_ALREADY_STARTED (22):
                Once the upload job is started, new
                operations cannot be added.
            REMOVE_NOT_SUPPORTED (23):
                Remove operation is not allowed for store
                sales direct updates.
            REMOVE_ALL_NOT_SUPPORTED (24):
                Remove-all is not supported for certain
                offline user data job types.
            INVALID_SHA256_FORMAT (25):
                The SHA256 encoded value is malformed.
            CUSTOM_KEY_DISABLED (26):
                The custom key specified is not enabled for
                the unified store sales upload.
            CUSTOM_KEY_NOT_PREDEFINED (27):
                The custom key specified is not predefined
                through the Google Ads UI.
            CUSTOM_KEY_NOT_SET (29):
                The custom key specified is not set in the
                upload.
            CUSTOMER_NOT_ACCEPTED_CUSTOMER_DATA_TERMS (30):
                The customer has not accepted the customer
                data terms in the conversion settings page.
            ATTRIBUTES_NOT_APPLICABLE_FOR_CUSTOMER_MATCH_USER_LIST (34):
                User attributes cannot be uploaded into a
                user list.
            LIFETIME_VALUE_BUCKET_NOT_IN_RANGE (35):
                Lifetime bucket value must be a number from 0
                to 10; 0 is only accepted for remove operations
            INCOMPATIBLE_USER_IDENTIFIER_FOR_ATTRIBUTES (36):
                Identifiers not supported for Customer Match
                attributes. User attributes can only be provided
                with contact info (email, phone, address) user
                identifiers.
            FUTURE_TIME_NOT_ALLOWED (37):
                A time in the future is not allowed.
            LAST_PURCHASE_TIME_LESS_THAN_ACQUISITION_TIME (38):
                Last purchase date time cannot be less than
                acquisition date time.
            CUSTOMER_IDENTIFIER_NOT_ALLOWED (39):
                Only emails are accepted as user identifiers
                for shopping loyalty match. {--
                api.dev/not-precedent: The identifier is not
                limited to ids, but also include other user info
                eg. phone numbers.}
            INVALID_ITEM_ID (40):
                Provided item ID is invalid.
            FIRST_PURCHASE_TIME_GREATER_THAN_LAST_PURCHASE_TIME (42):
                First purchase date time cannot be greater
                than the last purchase date time.
            INVALID_LIFECYCLE_STAGE (43):
                Provided lifecycle stage is invalid.
            INVALID_EVENT_VALUE (44):
                The event value of the Customer Match user
                attribute is invalid.
            EVENT_ATTRIBUTE_ALL_FIELDS_ARE_REQUIRED (45):
                All the fields are not present in the
                EventAttribute of the Customer Match.
            OPERATION_LEVEL_CONSENT_PROVIDED (48):
                Consent was provided at the operation level
                for an OfflineUserDataJobType that expects it at
                the job level. The provided operation-level
                consent will be ignored.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_USER_LIST_ID = 3
        INVALID_USER_LIST_TYPE = 4
        NOT_ON_ALLOWLIST_FOR_USER_ID = 33
        INCOMPATIBLE_UPLOAD_KEY_TYPE = 6
        MISSING_USER_IDENTIFIER = 7
        INVALID_MOBILE_ID_FORMAT = 8
        TOO_MANY_USER_IDENTIFIERS = 9
        NOT_ON_ALLOWLIST_FOR_STORE_SALES_DIRECT = 31
        NOT_ON_ALLOWLIST_FOR_UNIFIED_STORE_SALES = 32
        INVALID_PARTNER_ID = 11
        INVALID_ENCODING = 12
        INVALID_COUNTRY_CODE = 13
        INCOMPATIBLE_USER_IDENTIFIER = 14
        FUTURE_TRANSACTION_TIME = 15
        INVALID_CONVERSION_ACTION = 16
        MOBILE_ID_NOT_SUPPORTED = 17
        INVALID_OPERATION_ORDER = 18
        CONFLICTING_OPERATION = 19
        EXTERNAL_UPDATE_ID_ALREADY_EXISTS = 21
        JOB_ALREADY_STARTED = 22
        REMOVE_NOT_SUPPORTED = 23
        REMOVE_ALL_NOT_SUPPORTED = 24
        INVALID_SHA256_FORMAT = 25
        CUSTOM_KEY_DISABLED = 26
        CUSTOM_KEY_NOT_PREDEFINED = 27
        CUSTOM_KEY_NOT_SET = 29
        CUSTOMER_NOT_ACCEPTED_CUSTOMER_DATA_TERMS = 30
        ATTRIBUTES_NOT_APPLICABLE_FOR_CUSTOMER_MATCH_USER_LIST = 34
        LIFETIME_VALUE_BUCKET_NOT_IN_RANGE = 35
        INCOMPATIBLE_USER_IDENTIFIER_FOR_ATTRIBUTES = 36
        FUTURE_TIME_NOT_ALLOWED = 37
        LAST_PURCHASE_TIME_LESS_THAN_ACQUISITION_TIME = 38
        CUSTOMER_IDENTIFIER_NOT_ALLOWED = 39
        INVALID_ITEM_ID = 40
        FIRST_PURCHASE_TIME_GREATER_THAN_LAST_PURCHASE_TIME = 42
        INVALID_LIFECYCLE_STAGE = 43
        INVALID_EVENT_VALUE = 44
        EVENT_ATTRIBUTE_ALL_FIELDS_ARE_REQUIRED = 45
        OPERATION_LEVEL_CONSENT_PROVIDED = 48


__all__ = tuple(sorted(__protobuf__.manifest))

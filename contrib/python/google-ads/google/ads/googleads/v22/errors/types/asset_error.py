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
        "AssetErrorEnum",
    },
)


class AssetErrorEnum(proto.Message):
    r"""Container for enum describing possible asset errors."""

    class AssetError(proto.Enum):
        r"""Enum describing possible asset errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CUSTOMER_NOT_ON_ALLOWLIST_FOR_ASSET_TYPE (13):
                The customer is not is not on the allow-list
                for this asset type.
            DUPLICATE_ASSET (3):
                Assets are duplicated across operations.
            DUPLICATE_ASSET_NAME (4):
                The asset name is duplicated, either across
                operations or with an existing asset.
            ASSET_DATA_IS_MISSING (5):
                The Asset.asset_data oneof is empty.
            CANNOT_MODIFY_ASSET_NAME (6):
                The asset has a name which is different from
                an existing duplicate that represents the same
                content.
            FIELD_INCOMPATIBLE_WITH_ASSET_TYPE (7):
                The field cannot be set for this asset type.
            INVALID_CALL_TO_ACTION_TEXT (8):
                Call to action must come from the list of
                supported values.
            LEAD_FORM_INVALID_FIELDS_COMBINATION (9):
                A lead form asset is created with an invalid
                combination of input fields.
            LEAD_FORM_MISSING_AGREEMENT (10):
                Lead forms require that the Terms of Service
                have been agreed to before mutates can be
                executed.
            INVALID_ASSET_STATUS (11):
                Asset status is invalid in this operation.
            FIELD_CANNOT_BE_MODIFIED_FOR_ASSET_TYPE (12):
                The field cannot be modified by this asset
                type.
            SCHEDULES_CANNOT_OVERLAP (14):
                Ad schedules for the same asset cannot
                overlap.
            PROMOTION_CANNOT_SET_PERCENT_OFF_AND_MONEY_AMOUNT_OFF (15):
                Cannot set both percent off and money amount
                off fields of promotion asset.
            PROMOTION_CANNOT_SET_PROMOTION_CODE_AND_ORDERS_OVER_AMOUNT (16):
                Cannot set both promotion code and orders
                over amount fields of promotion asset.
            TOO_MANY_DECIMAL_PLACES_SPECIFIED (17):
                The field has too many decimal places
                specified.
            DUPLICATE_ASSETS_WITH_DIFFERENT_FIELD_VALUE (18):
                Duplicate assets across operations, which have identical
                Asset.asset_data oneof, cannot have different asset level
                fields for asset types which are deduped.
            CALL_CARRIER_SPECIFIC_SHORT_NUMBER_NOT_ALLOWED (19):
                Carrier specific short number is not allowed.
            CALL_CUSTOMER_CONSENT_FOR_CALL_RECORDING_REQUIRED (20):
                Customer consent required for call recording
                Terms of Service.
            CALL_DISALLOWED_NUMBER_TYPE (21):
                The type of the specified phone number is not
                allowed.
            CALL_INVALID_CONVERSION_ACTION (22):
                If the default call_conversion_action is not used, the
                customer must have a ConversionAction with the same id and
                the ConversionAction must be call conversion type.
            CALL_INVALID_COUNTRY_CODE (23):
                The country code of the phone number is
                invalid.
            CALL_INVALID_DOMESTIC_PHONE_NUMBER_FORMAT (24):
                The format of the phone number is incorrect.
            CALL_INVALID_PHONE_NUMBER (25):
                The input phone number is not a valid phone
                number.
            CALL_PHONE_NUMBER_NOT_SUPPORTED_FOR_COUNTRY (26):
                The phone number is not supported for
                country.
            CALL_PREMIUM_RATE_NUMBER_NOT_ALLOWED (27):
                Premium rate phone number is not allowed.
            CALL_VANITY_PHONE_NUMBER_NOT_ALLOWED (28):
                Vanity phone number is not allowed.
            PRICE_HEADER_SAME_AS_DESCRIPTION (29):
                PriceOffering cannot have the same value for
                header and description.
            MOBILE_APP_INVALID_APP_ID (30):
                AppId is invalid.
            MOBILE_APP_INVALID_FINAL_URL_FOR_APP_DOWNLOAD_URL (31):
                Invalid App download URL in final URLs.
            NAME_REQUIRED_FOR_ASSET_TYPE (32):
                Asset name is required for the asset type.
            LEAD_FORM_LEGACY_QUALIFYING_QUESTIONS_DISALLOWED (33):
                Legacy qualifying questions cannot be in the
                same Lead Form as custom questions.
            NAME_CONFLICT_FOR_ASSET_TYPE (34):
                Unique name is required for this asset type.
            CANNOT_MODIFY_ASSET_SOURCE (35):
                Cannot modify asset source.
            CANNOT_MODIFY_AUTOMATICALLY_CREATED_ASSET (36):
                User can not modify the automatically created
                asset.
            LEAD_FORM_LOCATION_ANSWER_TYPE_DISALLOWED (37):
                Lead Form is disallowed to use "LOCATION"
                answer type.
            PAGE_FEED_INVALID_LABEL_TEXT (38):
                Page Feed label text contains invalid
                characters.
            CUSTOMER_NOT_ON_ALLOWLIST_FOR_WHATSAPP_MESSAGE_ASSETS (39):
                The customer is not in the allow-list for
                whatsapp message asset type.
            CUSTOMER_NOT_ON_ALLOWLIST_FOR_APP_DEEP_LINK_ASSETS (40):
                Only customers on the allowlist can create
                AppDeepLinkAsset.
            PROMOTION_BARCODE_CANNOT_CONTAIN_LINKS (41):
                Promotion barcode cannot contain links.
            PROMOTION_BARCODE_INVALID_FORMAT (42):
                Failed to encode promotion barcode: Invalid
                format.
            UNSUPPORTED_BARCODE_TYPE (43):
                Barcode type is not supported.
            PROMOTION_QR_CODE_CANNOT_CONTAIN_LINKS (44):
                Promotion QR code cannot contain links.
            PROMOTION_QR_CODE_INVALID_FORMAT (45):
                Failed to encode promotion QR code: Invalid
                format.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CUSTOMER_NOT_ON_ALLOWLIST_FOR_ASSET_TYPE = 13
        DUPLICATE_ASSET = 3
        DUPLICATE_ASSET_NAME = 4
        ASSET_DATA_IS_MISSING = 5
        CANNOT_MODIFY_ASSET_NAME = 6
        FIELD_INCOMPATIBLE_WITH_ASSET_TYPE = 7
        INVALID_CALL_TO_ACTION_TEXT = 8
        LEAD_FORM_INVALID_FIELDS_COMBINATION = 9
        LEAD_FORM_MISSING_AGREEMENT = 10
        INVALID_ASSET_STATUS = 11
        FIELD_CANNOT_BE_MODIFIED_FOR_ASSET_TYPE = 12
        SCHEDULES_CANNOT_OVERLAP = 14
        PROMOTION_CANNOT_SET_PERCENT_OFF_AND_MONEY_AMOUNT_OFF = 15
        PROMOTION_CANNOT_SET_PROMOTION_CODE_AND_ORDERS_OVER_AMOUNT = 16
        TOO_MANY_DECIMAL_PLACES_SPECIFIED = 17
        DUPLICATE_ASSETS_WITH_DIFFERENT_FIELD_VALUE = 18
        CALL_CARRIER_SPECIFIC_SHORT_NUMBER_NOT_ALLOWED = 19
        CALL_CUSTOMER_CONSENT_FOR_CALL_RECORDING_REQUIRED = 20
        CALL_DISALLOWED_NUMBER_TYPE = 21
        CALL_INVALID_CONVERSION_ACTION = 22
        CALL_INVALID_COUNTRY_CODE = 23
        CALL_INVALID_DOMESTIC_PHONE_NUMBER_FORMAT = 24
        CALL_INVALID_PHONE_NUMBER = 25
        CALL_PHONE_NUMBER_NOT_SUPPORTED_FOR_COUNTRY = 26
        CALL_PREMIUM_RATE_NUMBER_NOT_ALLOWED = 27
        CALL_VANITY_PHONE_NUMBER_NOT_ALLOWED = 28
        PRICE_HEADER_SAME_AS_DESCRIPTION = 29
        MOBILE_APP_INVALID_APP_ID = 30
        MOBILE_APP_INVALID_FINAL_URL_FOR_APP_DOWNLOAD_URL = 31
        NAME_REQUIRED_FOR_ASSET_TYPE = 32
        LEAD_FORM_LEGACY_QUALIFYING_QUESTIONS_DISALLOWED = 33
        NAME_CONFLICT_FOR_ASSET_TYPE = 34
        CANNOT_MODIFY_ASSET_SOURCE = 35
        CANNOT_MODIFY_AUTOMATICALLY_CREATED_ASSET = 36
        LEAD_FORM_LOCATION_ANSWER_TYPE_DISALLOWED = 37
        PAGE_FEED_INVALID_LABEL_TEXT = 38
        CUSTOMER_NOT_ON_ALLOWLIST_FOR_WHATSAPP_MESSAGE_ASSETS = 39
        CUSTOMER_NOT_ON_ALLOWLIST_FOR_APP_DEEP_LINK_ASSETS = 40
        PROMOTION_BARCODE_CANNOT_CONTAIN_LINKS = 41
        PROMOTION_BARCODE_INVALID_FORMAT = 42
        UNSUPPORTED_BARCODE_TYPE = 43
        PROMOTION_QR_CODE_CANNOT_CONTAIN_LINKS = 44
        PROMOTION_QR_CODE_INVALID_FORMAT = 45


__all__ = tuple(sorted(__protobuf__.manifest))

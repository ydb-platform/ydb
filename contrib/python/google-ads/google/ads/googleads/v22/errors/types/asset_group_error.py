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
        "AssetGroupErrorEnum",
    },
)


class AssetGroupErrorEnum(proto.Message):
    r"""Container for enum describing possible asset group errors."""

    class AssetGroupError(proto.Enum):
        r"""Enum describing possible asset group errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            DUPLICATE_NAME (2):
                Each asset group in a single campaign must
                have a unique name.
            CANNOT_ADD_ASSET_GROUP_FOR_CAMPAIGN_TYPE (3):
                Cannot add asset group for the campaign type.
            NOT_ENOUGH_HEADLINE_ASSET (4):
                Not enough headline asset for a valid asset
                group.
            NOT_ENOUGH_LONG_HEADLINE_ASSET (5):
                Not enough long headline asset for a valid
                asset group.
            NOT_ENOUGH_DESCRIPTION_ASSET (6):
                Not enough description headline asset for a
                valid asset group.
            NOT_ENOUGH_BUSINESS_NAME_ASSET (7):
                Not enough business name asset for a valid
                asset group.
            NOT_ENOUGH_MARKETING_IMAGE_ASSET (8):
                Not enough marketing image asset for a valid
                asset group.
            NOT_ENOUGH_SQUARE_MARKETING_IMAGE_ASSET (9):
                Not enough square marketing image asset for a
                valid asset group.
            NOT_ENOUGH_LOGO_ASSET (10):
                Not enough logo asset for a valid asset
                group.
            FINAL_URL_SHOPPING_MERCHANT_HOME_PAGE_URL_DOMAINS_DIFFER (11):
                Final url and shopping merchant url does not
                have the same domain.
            PATH1_REQUIRED_WHEN_PATH2_IS_SET (12):
                Path1 required when path2 is set.
            SHORT_DESCRIPTION_REQUIRED (13):
                At least one short description asset is
                required for a valid asset group.
            FINAL_URL_REQUIRED (14):
                Final url field is required for asset group.
            FINAL_URL_CONTAINS_INVALID_DOMAIN_NAME (15):
                Final url contains invalid domain name.
            AD_CUSTOMIZER_NOT_SUPPORTED (16):
                Ad customizers are not supported in asset
                group's text field.
            CANNOT_MUTATE_ASSET_GROUP_FOR_REMOVED_CAMPAIGN (17):
                Cannot mutate asset group for campaign with
                removed status.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_NAME = 2
        CANNOT_ADD_ASSET_GROUP_FOR_CAMPAIGN_TYPE = 3
        NOT_ENOUGH_HEADLINE_ASSET = 4
        NOT_ENOUGH_LONG_HEADLINE_ASSET = 5
        NOT_ENOUGH_DESCRIPTION_ASSET = 6
        NOT_ENOUGH_BUSINESS_NAME_ASSET = 7
        NOT_ENOUGH_MARKETING_IMAGE_ASSET = 8
        NOT_ENOUGH_SQUARE_MARKETING_IMAGE_ASSET = 9
        NOT_ENOUGH_LOGO_ASSET = 10
        FINAL_URL_SHOPPING_MERCHANT_HOME_PAGE_URL_DOMAINS_DIFFER = 11
        PATH1_REQUIRED_WHEN_PATH2_IS_SET = 12
        SHORT_DESCRIPTION_REQUIRED = 13
        FINAL_URL_REQUIRED = 14
        FINAL_URL_CONTAINS_INVALID_DOMAIN_NAME = 15
        AD_CUSTOMIZER_NOT_SUPPORTED = 16
        CANNOT_MUTATE_ASSET_GROUP_FOR_REMOVED_CAMPAIGN = 17


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "AssetLinkErrorEnum",
    },
)


class AssetLinkErrorEnum(proto.Message):
    r"""Container for enum describing possible asset link errors."""

    class AssetLinkError(proto.Enum):
        r"""Enum describing possible asset link errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            PINNING_UNSUPPORTED (2):
                Pinning is not supported for the given asset
                link field.
            UNSUPPORTED_FIELD_TYPE (3):
                The given field type is not supported to be
                added directly through asset links.
            FIELD_TYPE_INCOMPATIBLE_WITH_ASSET_TYPE (4):
                The given asset's type and the specified
                field type are incompatible.
            FIELD_TYPE_INCOMPATIBLE_WITH_CAMPAIGN_TYPE (5):
                The specified field type is incompatible with
                the given campaign type.
            INCOMPATIBLE_ADVERTISING_CHANNEL_TYPE (6):
                The campaign advertising channel type cannot
                be associated with the given asset due to
                channel-based restrictions on the asset's
                fields.
            IMAGE_NOT_WITHIN_SPECIFIED_DIMENSION_RANGE (7):
                The image asset provided is not within the
                dimension constraints specified for the
                submitted asset field.
            INVALID_PINNED_FIELD (8):
                The pinned field is not valid for the
                submitted asset field.
            MEDIA_BUNDLE_ASSET_FILE_SIZE_TOO_LARGE (9):
                The media bundle asset provided is too large
                for the submitted asset field.
            NOT_ENOUGH_AVAILABLE_ASSET_LINKS_FOR_VALID_COMBINATION (10):
                Not enough assets are available for use with
                other fields since other assets are pinned to
                specific fields.
            NOT_ENOUGH_AVAILABLE_ASSET_LINKS_WITH_FALLBACK (11):
                Not enough assets with fallback are
                available. When validating the minimum number of
                assets, assets without fallback (for example,
                assets that contain location tag without default
                value "{LOCATION(City)}") will not be counted.
            NOT_ENOUGH_AVAILABLE_ASSET_LINKS_WITH_FALLBACK_FOR_VALID_COMBINATION (12):
                This is a combination of the
                NOT_ENOUGH_AVAILABLE_ASSET_LINKS_FOR_VALID_COMBINATION and
                NOT_ENOUGH_AVAILABLE_ASSET_LINKS_WITH_FALLBACK errors. Not
                enough assets with fallback are available since some assets
                are pinned.
            YOUTUBE_VIDEO_REMOVED (13):
                The YouTube video referenced in the provided
                asset has been removed.
            YOUTUBE_VIDEO_TOO_LONG (14):
                The YouTube video referenced in the provided
                asset is too long for the field submitted.
            YOUTUBE_VIDEO_TOO_SHORT (15):
                The YouTube video referenced in the provided
                asset is too short for the field submitted.
            EXCLUDED_PARENT_FIELD_TYPE (16):
                The specified field type is excluded for
                given campaign or ad group.
            INVALID_STATUS (17):
                The status is invalid for the operation
                specified.
            YOUTUBE_VIDEO_DURATION_NOT_DEFINED (18):
                The YouTube video referenced in the provided
                asset has unknown duration. This might be the
                case for a livestream video or a video being
                currently uploaded to YouTube. In both cases,
                the video duration should eventually get
                resolved.
            CANNOT_CREATE_AUTOMATICALLY_CREATED_LINKS (19):
                User cannot create automatically created
                links.
            CANNOT_LINK_TO_AUTOMATICALLY_CREATED_ASSET (20):
                Advertiser links cannot link to automatically
                created asset.
            CANNOT_MODIFY_ASSET_LINK_SOURCE (21):
                Automatically created links cannot be changed
                into advertiser links or the reverse.
            CANNOT_LINK_LOCATION_LEAD_FORM_WITHOUT_LOCATION_ASSET (22):
                Lead Form asset with Location answer type
                can't be linked to the Customer/Campaign because
                there are no Location assets.
            CUSTOMER_NOT_VERIFIED (23):
                Customer is not verified.
            UNSUPPORTED_CALL_TO_ACTION (24):
                Call to action value is not supported.
            BRAND_ASSETS_NOT_LINKED_AT_ASSET_GROUP_LEVEL (25):
                For Performance Max campaigns where brand_guidelines_enabled
                is false, business name and logo assets must be linked as
                AssetGroupAssets.
            BRAND_ASSETS_NOT_LINKED_AT_CAMPAIGN_LEVEL (26):
                For Performance Max campaigns where brand_guidelines_enabled
                is true, business name and logo assets must be linked as
                CampaignAssets.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PINNING_UNSUPPORTED = 2
        UNSUPPORTED_FIELD_TYPE = 3
        FIELD_TYPE_INCOMPATIBLE_WITH_ASSET_TYPE = 4
        FIELD_TYPE_INCOMPATIBLE_WITH_CAMPAIGN_TYPE = 5
        INCOMPATIBLE_ADVERTISING_CHANNEL_TYPE = 6
        IMAGE_NOT_WITHIN_SPECIFIED_DIMENSION_RANGE = 7
        INVALID_PINNED_FIELD = 8
        MEDIA_BUNDLE_ASSET_FILE_SIZE_TOO_LARGE = 9
        NOT_ENOUGH_AVAILABLE_ASSET_LINKS_FOR_VALID_COMBINATION = 10
        NOT_ENOUGH_AVAILABLE_ASSET_LINKS_WITH_FALLBACK = 11
        NOT_ENOUGH_AVAILABLE_ASSET_LINKS_WITH_FALLBACK_FOR_VALID_COMBINATION = (
            12
        )
        YOUTUBE_VIDEO_REMOVED = 13
        YOUTUBE_VIDEO_TOO_LONG = 14
        YOUTUBE_VIDEO_TOO_SHORT = 15
        EXCLUDED_PARENT_FIELD_TYPE = 16
        INVALID_STATUS = 17
        YOUTUBE_VIDEO_DURATION_NOT_DEFINED = 18
        CANNOT_CREATE_AUTOMATICALLY_CREATED_LINKS = 19
        CANNOT_LINK_TO_AUTOMATICALLY_CREATED_ASSET = 20
        CANNOT_MODIFY_ASSET_LINK_SOURCE = 21
        CANNOT_LINK_LOCATION_LEAD_FORM_WITHOUT_LOCATION_ASSET = 22
        CUSTOMER_NOT_VERIFIED = 23
        UNSUPPORTED_CALL_TO_ACTION = 24
        BRAND_ASSETS_NOT_LINKED_AT_ASSET_GROUP_LEVEL = 25
        BRAND_ASSETS_NOT_LINKED_AT_CAMPAIGN_LEVEL = 26


__all__ = tuple(sorted(__protobuf__.manifest))

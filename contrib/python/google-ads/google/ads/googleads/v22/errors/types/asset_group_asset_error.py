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
        "AssetGroupAssetErrorEnum",
    },
)


class AssetGroupAssetErrorEnum(proto.Message):
    r"""Container for enum describing possible asset group asset
    errors.

    """

    class AssetGroupAssetError(proto.Enum):
        r"""Enum describing possible asset group asset errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            DUPLICATE_RESOURCE (2):
                Cannot add duplicated asset group asset.
            EXPANDABLE_TAGS_NOT_ALLOWED_IN_DESCRIPTION (3):
                Expandable tags are not allowed in
                description assets.
            AD_CUSTOMIZER_NOT_SUPPORTED (4):
                Ad customizers are not supported in
                assetgroup's text assets.
            HOTEL_PROPERTY_ASSET_NOT_LINKED_TO_CAMPAIGN (5):
                Cannot add a HotelPropertyAsset to an AssetGroup that isn't
                linked to the parent campaign's hotel_property_asset_set
                field.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_RESOURCE = 2
        EXPANDABLE_TAGS_NOT_ALLOWED_IN_DESCRIPTION = 3
        AD_CUSTOMIZER_NOT_SUPPORTED = 4
        HOTEL_PROPERTY_ASSET_NOT_LINKED_TO_CAMPAIGN = 5


__all__ = tuple(sorted(__protobuf__.manifest))

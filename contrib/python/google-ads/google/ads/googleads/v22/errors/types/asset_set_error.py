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
        "AssetSetErrorEnum",
    },
)


class AssetSetErrorEnum(proto.Message):
    r"""Container for enum describing possible asset set errors."""

    class AssetSetError(proto.Enum):
        r"""Enum describing possible asset set errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            DUPLICATE_ASSET_SET_NAME (2):
                The asset set name matches that of another
                enabled asset set.
            INVALID_PARENT_ASSET_SET_TYPE (3):
                The type of AssetSet.asset_set_source does not match the
                type of AssetSet.location_set.source in its parent AssetSet.
            ASSET_SET_SOURCE_INCOMPATIBLE_WITH_PARENT_ASSET_SET (4):
                The asset set source doesn't match its parent
                AssetSet's data.
            ASSET_SET_TYPE_CANNOT_BE_LINKED_TO_CUSTOMER (5):
                This AssetSet type cannot be linked to
                CustomerAssetSet.
            INVALID_CHAIN_IDS (6):
                The chain id(s) in ChainSet of a LOCATION_SYNC typed
                AssetSet is invalid.
            LOCATION_SYNC_ASSET_SET_DOES_NOT_SUPPORT_RELATIONSHIP_TYPE (7):
                The relationship type in ChainSet of a LOCATION_SYNC typed
                AssetSet is not supported.
            NOT_UNIQUE_ENABLED_LOCATION_SYNC_TYPED_ASSET_SET (8):
                There is more than one enabled LocationSync
                typed AssetSet under one customer.
            INVALID_PLACE_IDS (9):
                The place id(s) in a LocationSync typed
                AssetSet is invalid and can't be decoded.
            OAUTH_INFO_INVALID (11):
                The Google Business Profile OAuth info is
                invalid.
            OAUTH_INFO_MISSING (12):
                The Google Business Profile OAuth info is
                missing.
            CANNOT_DELETE_AS_ENABLED_LINKAGES_EXIST (10):
                Can't delete an AssetSet if it has any
                enabled linkages (e.g. CustomerAssetSet), or
                AssetSet is a parent AssetSet and has enabled
                child AssetSet associated.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_ASSET_SET_NAME = 2
        INVALID_PARENT_ASSET_SET_TYPE = 3
        ASSET_SET_SOURCE_INCOMPATIBLE_WITH_PARENT_ASSET_SET = 4
        ASSET_SET_TYPE_CANNOT_BE_LINKED_TO_CUSTOMER = 5
        INVALID_CHAIN_IDS = 6
        LOCATION_SYNC_ASSET_SET_DOES_NOT_SUPPORT_RELATIONSHIP_TYPE = 7
        NOT_UNIQUE_ENABLED_LOCATION_SYNC_TYPED_ASSET_SET = 8
        INVALID_PLACE_IDS = 9
        OAUTH_INFO_INVALID = 11
        OAUTH_INFO_MISSING = 12
        CANNOT_DELETE_AS_ENABLED_LINKAGES_EXIST = 10


__all__ = tuple(sorted(__protobuf__.manifest))

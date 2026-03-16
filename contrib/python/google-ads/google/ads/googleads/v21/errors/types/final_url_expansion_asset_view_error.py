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
        "FinalUrlExpansionAssetViewErrorEnum",
    },
)


class FinalUrlExpansionAssetViewErrorEnum(proto.Message):
    r"""Container for enum describing possible final url expansion
    asset view errors.

    """

    class FinalUrlExpansionAssetViewError(proto.Enum):
        r"""Enum describing possible final url expansion asset view
        errors.

        Values:
            UNSPECIFIED (0):
                Name unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            MISSING_REQUIRED_FILTER (2):
                At least one required filter has to be
                applied in the query.
            REQUIRES_ADVERTISING_CHANNEL_TYPE_FILTER (3):
                Advertising channel type filter is required.
            INVALID_ADVERTISING_CHANNEL_TYPE_IN_FILTER (4):
                Advertising channel type filter has an
                invalid value.
            CANNOT_SELECT_ASSET_GROUP (5):
                Asset group cannot be selected in the query.
            CANNOT_SELECT_AD_GROUP (6):
                Ad group cannot be selected in the query.
            REQUIRES_FILTER_BY_SINGLE_RESOURCE (7):
                A selected field/resource requires filtering
                by a single resource.
            CANNOT_SELECT_BOTH_AD_GROUP_AND_ASSET_GROUP (8):
                Both ad group and asset group cannot be
                selected in the query.
            CANNOT_FILTER_BY_BOTH_AD_GROUP_AND_ASSET_GROUP (9):
                Both ad group and asset group cannot be
                filtered in the query.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        MISSING_REQUIRED_FILTER = 2
        REQUIRES_ADVERTISING_CHANNEL_TYPE_FILTER = 3
        INVALID_ADVERTISING_CHANNEL_TYPE_IN_FILTER = 4
        CANNOT_SELECT_ASSET_GROUP = 5
        CANNOT_SELECT_AD_GROUP = 6
        REQUIRES_FILTER_BY_SINGLE_RESOURCE = 7
        CANNOT_SELECT_BOTH_AD_GROUP_AND_ASSET_GROUP = 8
        CANNOT_FILTER_BY_BOTH_AD_GROUP_AND_ASSET_GROUP = 9


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "AudienceErrorEnum",
    },
)


class AudienceErrorEnum(proto.Message):
    r"""Container for enum describing possible audience errors."""

    class AudienceError(proto.Enum):
        r"""Enum describing possible audience errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            NAME_ALREADY_IN_USE (2):
                An audience with this name already exists.
            DIMENSION_INVALID (3):
                A dimension within the audience definition is
                not valid.
            AUDIENCE_SEGMENT_NOT_FOUND (4):
                One of the audience segment added is not
                found.
            AUDIENCE_SEGMENT_TYPE_NOT_SUPPORTED (5):
                One of the audience segment type is not
                supported.
            DUPLICATE_AUDIENCE_SEGMENT (6):
                The same segment already exists in this
                audience.
            TOO_MANY_SEGMENTS (7):
                Audience can't have more than allowed number
                segments.
            TOO_MANY_DIMENSIONS_OF_SAME_TYPE (8):
                Audience can't have multiple dimensions of
                same type.
            IN_USE (9):
                The audience cannot be removed, because it is
                currently used in an ad group criterion or asset
                group signal in an (enabled or paused) ad group
                or campaign.
            MISSING_ASSET_GROUP_ID (10):
                Asset Group scoped audience requires an asset
                group ID.
            CANNOT_CHANGE_FROM_CUSTOMER_TO_ASSET_GROUP_SCOPE (11):
                Audience scope may not be changed from
                Customer to AssetGroup.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NAME_ALREADY_IN_USE = 2
        DIMENSION_INVALID = 3
        AUDIENCE_SEGMENT_NOT_FOUND = 4
        AUDIENCE_SEGMENT_TYPE_NOT_SUPPORTED = 5
        DUPLICATE_AUDIENCE_SEGMENT = 6
        TOO_MANY_SEGMENTS = 7
        TOO_MANY_DIMENSIONS_OF_SAME_TYPE = 8
        IN_USE = 9
        MISSING_ASSET_GROUP_ID = 10
        CANNOT_CHANGE_FROM_CUSTOMER_TO_ASSET_GROUP_SCOPE = 11


__all__ = tuple(sorted(__protobuf__.manifest))

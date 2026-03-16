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
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "PositiveGeoTargetTypeEnum",
    },
)


class PositiveGeoTargetTypeEnum(proto.Message):
    r"""Container for enum describing possible positive geo target
    types.

    """

    class PositiveGeoTargetType(proto.Enum):
        r"""The possible positive geo target types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            PRESENCE_OR_INTEREST (5):
                Specifies that an ad is triggered if the user
                is in, or shows interest in, advertiser's
                targeted locations.
            SEARCH_INTEREST (6):
                Specifies that an ad is triggered if the user
                searches for advertiser's targeted locations.
                This can only be used with Search and standard
                Shopping campaigns.
            PRESENCE (7):
                Specifies that an ad is triggered if the user
                is in or regularly in advertiser's targeted
                locations.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PRESENCE_OR_INTEREST = 5
        SEARCH_INTEREST = 6
        PRESENCE = 7


__all__ = tuple(sorted(__protobuf__.manifest))

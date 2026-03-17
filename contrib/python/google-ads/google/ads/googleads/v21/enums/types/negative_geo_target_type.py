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
        "NegativeGeoTargetTypeEnum",
    },
)


class NegativeGeoTargetTypeEnum(proto.Message):
    r"""Container for enum describing possible negative geo target
    types.

    """

    class NegativeGeoTargetType(proto.Enum):
        r"""The possible negative geo target types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            PRESENCE_OR_INTEREST (4):
                Specifies that a user is excluded from seeing
                the ad if they are in, or show interest in,
                advertiser's excluded locations.
            PRESENCE (5):
                Specifies that a user is excluded from seeing
                the ad if they are in advertiser's excluded
                locations.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PRESENCE_OR_INTEREST = 4
        PRESENCE = 5


__all__ = tuple(sorted(__protobuf__.manifest))

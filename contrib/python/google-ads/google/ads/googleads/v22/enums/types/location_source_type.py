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
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "LocationSourceTypeEnum",
    },
)


class LocationSourceTypeEnum(proto.Message):
    r"""Used to distinguish the location source type."""

    class LocationSourceType(proto.Enum):
        r"""The possible types of a location source.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            GOOGLE_MY_BUSINESS (2):
                Locations associated with the customer's
                linked Business Profile.
            AFFILIATE (3):
                Affiliate (chain) store locations. For
                example, Best Buy store locations.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        GOOGLE_MY_BUSINESS = 2
        AFFILIATE = 3


__all__ = tuple(sorted(__protobuf__.manifest))

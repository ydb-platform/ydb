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
        "HotelPriceBucketEnum",
    },
)


class HotelPriceBucketEnum(proto.Message):
    r"""Container for enum describing hotel price bucket for a hotel
    itinerary.

    """

    class HotelPriceBucket(proto.Enum):
        r"""Enum describing possible hotel price buckets.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            LOWEST_UNIQUE (2):
                Uniquely lowest price. Partner has the lowest
                price, and no other partners are within a small
                variance of that price.
            LOWEST_TIED (3):
                Tied for lowest price. Partner is within a
                small variance of the lowest price.
            NOT_LOWEST (4):
                Not lowest price. Partner is not within a
                small variance of the lowest price.
            ONLY_PARTNER_SHOWN (5):
                Partner was the only one shown.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        LOWEST_UNIQUE = 2
        LOWEST_TIED = 3
        NOT_LOWEST = 4
        ONLY_PARTNER_SHOWN = 5


__all__ = tuple(sorted(__protobuf__.manifest))

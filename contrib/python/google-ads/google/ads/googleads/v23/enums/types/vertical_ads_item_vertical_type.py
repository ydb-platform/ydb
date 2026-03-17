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
    package="google.ads.googleads.v23.enums",
    marshal="google.ads.googleads.v23",
    manifest={
        "VerticalAdsItemVerticalTypeEnum",
    },
)


class VerticalAdsItemVerticalTypeEnum(proto.Message):
    r"""Container for enum describing Vertical Ads Item Vertical Type for
    SharedSet of type VERTICAL_ADS_ITEM_GROUP_RULE_LIST.

    """

    class VerticalAdsItemVerticalType(proto.Enum):
        r"""Enum describing Vertical Ads Item Vertical Type for SharedSet of
        type VERTICAL_ADS_ITEM_GROUP_RULE_LIST.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            HOTELS (2):
                Hotels travel vertical.
            VACATION_RENTALS (3):
                Vacation rentals travel vertical.
            RENTAL_CARS (4):
                Rental cars travel vertical.
            EVENTS (5):
                Events travel vertical.
            THINGS_TO_DO (6):
                Things to do travel vertical.
            FLIGHTS (7):
                Flights travel vertical.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        HOTELS = 2
        VACATION_RENTALS = 3
        RENTAL_CARS = 4
        EVENTS = 5
        THINGS_TO_DO = 6
        FLIGHTS = 7


__all__ = tuple(sorted(__protobuf__.manifest))

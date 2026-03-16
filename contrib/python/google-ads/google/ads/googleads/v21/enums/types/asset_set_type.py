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
        "AssetSetTypeEnum",
    },
)


class AssetSetTypeEnum(proto.Message):
    r"""Container for enum describing possible types of an asset set."""

    class AssetSetType(proto.Enum):
        r"""Possible types of an asset set.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PAGE_FEED (2):
                Page asset set.
            DYNAMIC_EDUCATION (3):
                Dynamic education asset set.
            MERCHANT_CENTER_FEED (4):
                Google Merchant Center asset set.
            DYNAMIC_REAL_ESTATE (5):
                Dynamic real estate asset set.
            DYNAMIC_CUSTOM (6):
                Dynamic custom asset set.
            DYNAMIC_HOTELS_AND_RENTALS (7):
                Dynamic hotels and rentals asset set.
            DYNAMIC_FLIGHTS (8):
                Dynamic flights asset set.
            DYNAMIC_TRAVEL (9):
                Dynamic travel asset set.
            DYNAMIC_LOCAL (10):
                Dynamic local asset set.
            DYNAMIC_JOBS (11):
                Dynamic jobs asset set.
            LOCATION_SYNC (12):
                Location sync level asset set.
            BUSINESS_PROFILE_DYNAMIC_LOCATION_GROUP (13):
                Business Profile location group asset set.
            CHAIN_DYNAMIC_LOCATION_GROUP (14):
                Chain location group asset set which can be
                used for both owned locations and affiliate
                locations.
            STATIC_LOCATION_GROUP (15):
                Static location group asset set which can be
                used for both owned locations and affiliate
                locations.
            HOTEL_PROPERTY (16):
                Hotel Property asset set which is used to
                link a hotel property feed to Performance Max
                for travel goals campaigns.
            TRAVEL_FEED (17):
                Travel Feed asset set type. Can represent
                either a Hotel feed or a Things to Do
                (activities) feed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PAGE_FEED = 2
        DYNAMIC_EDUCATION = 3
        MERCHANT_CENTER_FEED = 4
        DYNAMIC_REAL_ESTATE = 5
        DYNAMIC_CUSTOM = 6
        DYNAMIC_HOTELS_AND_RENTALS = 7
        DYNAMIC_FLIGHTS = 8
        DYNAMIC_TRAVEL = 9
        DYNAMIC_LOCAL = 10
        DYNAMIC_JOBS = 11
        LOCATION_SYNC = 12
        BUSINESS_PROFILE_DYNAMIC_LOCATION_GROUP = 13
        CHAIN_DYNAMIC_LOCATION_GROUP = 14
        STATIC_LOCATION_GROUP = 15
        HOTEL_PROPERTY = 16
        TRAVEL_FEED = 17


__all__ = tuple(sorted(__protobuf__.manifest))

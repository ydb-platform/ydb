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
        "AssetTypeEnum",
    },
)


class AssetTypeEnum(proto.Message):
    r"""Container for enum describing the types of asset."""

    class AssetType(proto.Enum):
        r"""Enum describing possible types of asset.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            YOUTUBE_VIDEO (2):
                YouTube video asset.
            MEDIA_BUNDLE (3):
                Media bundle asset.
            IMAGE (4):
                Image asset.
            TEXT (5):
                Text asset.
            LEAD_FORM (6):
                Lead form asset.
            BOOK_ON_GOOGLE (7):
                Book on Google asset.
            PROMOTION (8):
                Promotion asset.
            CALLOUT (9):
                Callout asset.
            STRUCTURED_SNIPPET (10):
                Structured Snippet asset.
            SITELINK (11):
                Sitelink asset.
            PAGE_FEED (12):
                Page Feed asset.
            DYNAMIC_EDUCATION (13):
                Dynamic Education asset.
            MOBILE_APP (14):
                Mobile app asset.
            HOTEL_CALLOUT (15):
                Hotel callout asset.
            CALL (16):
                Call asset.
            PRICE (17):
                Price asset.
            CALL_TO_ACTION (18):
                Call to action asset.
            DYNAMIC_REAL_ESTATE (19):
                Dynamic real estate asset.
            DYNAMIC_CUSTOM (20):
                Dynamic custom asset.
            DYNAMIC_HOTELS_AND_RENTALS (21):
                Dynamic hotels and rentals asset.
            DYNAMIC_FLIGHTS (22):
                Dynamic flights asset.
            DYNAMIC_TRAVEL (24):
                Dynamic travel asset.
            DYNAMIC_LOCAL (25):
                Dynamic local asset.
            DYNAMIC_JOBS (26):
                Dynamic jobs asset.
            LOCATION (27):
                Location asset.
            HOTEL_PROPERTY (28):
                Hotel property asset.
            DEMAND_GEN_CAROUSEL_CARD (29):
                Demand Gen Carousel Card asset.
            BUSINESS_MESSAGE (30):
                Business message asset.
            APP_DEEP_LINK (31):
                App deep link asset.
            YOUTUBE_VIDEO_LIST (32):
                YouTube video list asset.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        YOUTUBE_VIDEO = 2
        MEDIA_BUNDLE = 3
        IMAGE = 4
        TEXT = 5
        LEAD_FORM = 6
        BOOK_ON_GOOGLE = 7
        PROMOTION = 8
        CALLOUT = 9
        STRUCTURED_SNIPPET = 10
        SITELINK = 11
        PAGE_FEED = 12
        DYNAMIC_EDUCATION = 13
        MOBILE_APP = 14
        HOTEL_CALLOUT = 15
        CALL = 16
        PRICE = 17
        CALL_TO_ACTION = 18
        DYNAMIC_REAL_ESTATE = 19
        DYNAMIC_CUSTOM = 20
        DYNAMIC_HOTELS_AND_RENTALS = 21
        DYNAMIC_FLIGHTS = 22
        DYNAMIC_TRAVEL = 24
        DYNAMIC_LOCAL = 25
        DYNAMIC_JOBS = 26
        LOCATION = 27
        HOTEL_PROPERTY = 28
        DEMAND_GEN_CAROUSEL_CARD = 29
        BUSINESS_MESSAGE = 30
        APP_DEEP_LINK = 31
        YOUTUBE_VIDEO_LIST = 32


__all__ = tuple(sorted(__protobuf__.manifest))

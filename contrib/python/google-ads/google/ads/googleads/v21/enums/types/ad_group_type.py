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
        "AdGroupTypeEnum",
    },
)


class AdGroupTypeEnum(proto.Message):
    r"""Defines types of an ad group, specific to a particular
    campaign channel type. This type drives validations that
    restrict which entities can be added to the ad group.

    """

    class AdGroupType(proto.Enum):
        r"""Enum listing the possible types of an ad group.

        Values:
            UNSPECIFIED (0):
                The type has not been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            SEARCH_STANDARD (2):
                The default ad group type for Search
                campaigns.
            DISPLAY_STANDARD (3):
                The default ad group type for Display
                campaigns.
            SHOPPING_PRODUCT_ADS (4):
                The ad group type for Shopping campaigns
                serving standard product ads.
            HOTEL_ADS (6):
                The default ad group type for Hotel
                campaigns.
            SHOPPING_SMART_ADS (7):
                The type for ad groups in Smart Shopping
                campaigns.
            VIDEO_BUMPER (8):
                Short unskippable in-stream video ads.
            VIDEO_TRUE_VIEW_IN_STREAM (9):
                TrueView (skippable) in-stream video ads.
            VIDEO_TRUE_VIEW_IN_DISPLAY (10):
                TrueView in-display video ads.
            VIDEO_NON_SKIPPABLE_IN_STREAM (11):
                Unskippable in-stream video ads.
            SEARCH_DYNAMIC_ADS (13):
                Ad group type for Dynamic Search Ads ad
                groups.
            SHOPPING_COMPARISON_LISTING_ADS (14):
                The type for ad groups in Shopping Comparison
                Listing campaigns.
            PROMOTED_HOTEL_ADS (15):
                The ad group type for Promoted Hotel ad
                groups.
            VIDEO_RESPONSIVE (16):
                Video responsive ad groups.
            VIDEO_EFFICIENT_REACH (17):
                Video efficient reach ad groups.
            SMART_CAMPAIGN_ADS (18):
                Ad group type for Smart campaigns.
            TRAVEL_ADS (19):
                Ad group type for Travel campaigns.
            YOUTUBE_AUDIO (20):
                Ad group type for YouTube Audio campaigns.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SEARCH_STANDARD = 2
        DISPLAY_STANDARD = 3
        SHOPPING_PRODUCT_ADS = 4
        HOTEL_ADS = 6
        SHOPPING_SMART_ADS = 7
        VIDEO_BUMPER = 8
        VIDEO_TRUE_VIEW_IN_STREAM = 9
        VIDEO_TRUE_VIEW_IN_DISPLAY = 10
        VIDEO_NON_SKIPPABLE_IN_STREAM = 11
        SEARCH_DYNAMIC_ADS = 13
        SHOPPING_COMPARISON_LISTING_ADS = 14
        PROMOTED_HOTEL_ADS = 15
        VIDEO_RESPONSIVE = 16
        VIDEO_EFFICIENT_REACH = 17
        SMART_CAMPAIGN_ADS = 18
        TRAVEL_ADS = 19
        YOUTUBE_AUDIO = 20


__all__ = tuple(sorted(__protobuf__.manifest))

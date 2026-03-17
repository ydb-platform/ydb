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
        "AdvertisingChannelSubTypeEnum",
    },
)


class AdvertisingChannelSubTypeEnum(proto.Message):
    r"""An immutable specialization of an Advertising Channel."""

    class AdvertisingChannelSubType(proto.Enum):
        r"""Enum describing the different channel subtypes.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used as a return value only. Represents value
                unknown in this version.
            SEARCH_MOBILE_APP (2):
                Mobile app campaigns for Search.
            DISPLAY_MOBILE_APP (3):
                Mobile app campaigns for Display.
            SEARCH_EXPRESS (4):
                AdWords express campaigns for search.
            DISPLAY_EXPRESS (5):
                AdWords Express campaigns for display.
            SHOPPING_SMART_ADS (6):
                Smart Shopping campaigns.
            DISPLAY_GMAIL_AD (7):
                Gmail Ad campaigns.
            DISPLAY_SMART_CAMPAIGN (8):
                Smart display campaigns. New campaigns of
                this sub type cannot be created.
            VIDEO_ACTION (10):
                Video TrueView for Action campaigns.
            VIDEO_NON_SKIPPABLE (11):
                Video campaigns with non-skippable video ads.
            APP_CAMPAIGN (12):
                App Campaign that lets you easily promote
                your Android or iOS app across Google's top
                properties including Search, Play, YouTube, and
                the Google Display Network.
            APP_CAMPAIGN_FOR_ENGAGEMENT (13):
                App Campaign for engagement, focused on
                driving re-engagement with the app across
                several of Google's top properties including
                Search, YouTube, and the Google Display Network.
            LOCAL_CAMPAIGN (14):
                Campaigns specialized for local advertising.
            SHOPPING_COMPARISON_LISTING_ADS (15):
                Shopping Comparison Listing campaigns.
            SMART_CAMPAIGN (16):
                Standard Smart campaigns.
            VIDEO_SEQUENCE (17):
                Video campaigns with sequence video ads.
            APP_CAMPAIGN_FOR_PRE_REGISTRATION (18):
                App Campaign for pre registration,
                specialized for advertising mobile app
                pre-registration, that targets multiple
                advertising channels across Google Play, YouTube
                and Display Network. See
                https://support.google.com/google-ads/answer/9441344
                to learn more.
            VIDEO_REACH_TARGET_FREQUENCY (19):
                Video reach campaign with Target Frequency
                bidding strategy.
            TRAVEL_ACTIVITIES (20):
                Travel Activities campaigns.
            YOUTUBE_AUDIO (22):
                YouTube Audio campaigns.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SEARCH_MOBILE_APP = 2
        DISPLAY_MOBILE_APP = 3
        SEARCH_EXPRESS = 4
        DISPLAY_EXPRESS = 5
        SHOPPING_SMART_ADS = 6
        DISPLAY_GMAIL_AD = 7
        DISPLAY_SMART_CAMPAIGN = 8
        VIDEO_ACTION = 10
        VIDEO_NON_SKIPPABLE = 11
        APP_CAMPAIGN = 12
        APP_CAMPAIGN_FOR_ENGAGEMENT = 13
        LOCAL_CAMPAIGN = 14
        SHOPPING_COMPARISON_LISTING_ADS = 15
        SMART_CAMPAIGN = 16
        VIDEO_SEQUENCE = 17
        APP_CAMPAIGN_FOR_PRE_REGISTRATION = 18
        VIDEO_REACH_TARGET_FREQUENCY = 19
        TRAVEL_ACTIVITIES = 20
        YOUTUBE_AUDIO = 22


__all__ = tuple(sorted(__protobuf__.manifest))

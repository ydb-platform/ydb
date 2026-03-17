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
        "AdTypeEnum",
    },
)


class AdTypeEnum(proto.Message):
    r"""Container for enum describing possible types of an ad."""

    class AdType(proto.Enum):
        r"""The possible types of an ad.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            TEXT_AD (2):
                The ad is a text ad.
            EXPANDED_TEXT_AD (3):
                The ad is an expanded text ad.
            EXPANDED_DYNAMIC_SEARCH_AD (7):
                The ad is an expanded dynamic search ad.
            HOTEL_AD (8):
                The ad is a hotel ad.
            SHOPPING_SMART_AD (9):
                The ad is a Smart Shopping ad.
            SHOPPING_PRODUCT_AD (10):
                The ad is a standard Shopping ad.
            VIDEO_AD (12):
                The ad is a video ad.
            IMAGE_AD (14):
                This ad is an Image ad.
            RESPONSIVE_SEARCH_AD (15):
                The ad is a responsive search ad.
            LEGACY_RESPONSIVE_DISPLAY_AD (16):
                The ad is a legacy responsive display ad.
            APP_AD (17):
                The ad is an app ad.
            LEGACY_APP_INSTALL_AD (18):
                The ad is a legacy app install ad.
            RESPONSIVE_DISPLAY_AD (19):
                The ad is a responsive display ad.
            LOCAL_AD (20):
                The ad is a local ad.
            HTML5_UPLOAD_AD (21):
                The ad is a display upload ad with the HTML5_UPLOAD_AD
                product type.
            DYNAMIC_HTML5_AD (22):
                The ad is a display upload ad with one of the
                DYNAMIC_HTML5\_\* product types.
            APP_ENGAGEMENT_AD (23):
                The ad is an app engagement ad.
            SHOPPING_COMPARISON_LISTING_AD (24):
                The ad is a Shopping Comparison Listing ad.
            VIDEO_BUMPER_AD (25):
                Video bumper ad.
            VIDEO_NON_SKIPPABLE_IN_STREAM_AD (26):
                Video non-skippable in-stream ad.
            VIDEO_TRUEVIEW_IN_STREAM_AD (29):
                Video TrueView in-stream ad.
            VIDEO_RESPONSIVE_AD (30):
                Video responsive ad.
            SMART_CAMPAIGN_AD (31):
                Smart campaign ad.
            CALL_AD (32):
                Call ad.
            APP_PRE_REGISTRATION_AD (33):
                Universal app pre-registration ad.
            IN_FEED_VIDEO_AD (34):
                In-feed video ad.
            DEMAND_GEN_MULTI_ASSET_AD (40):
                Demand Gen multi asset ad.
            DEMAND_GEN_CAROUSEL_AD (41):
                Demand Gen carousel ad.
            TRAVEL_AD (37):
                Travel ad.
            DEMAND_GEN_VIDEO_RESPONSIVE_AD (42):
                Demand Gen video responsive ad.
            DEMAND_GEN_PRODUCT_AD (39):
                Demand Gen product ad.
            YOUTUBE_AUDIO_AD (44):
                YouTube Audio ad.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        TEXT_AD = 2
        EXPANDED_TEXT_AD = 3
        EXPANDED_DYNAMIC_SEARCH_AD = 7
        HOTEL_AD = 8
        SHOPPING_SMART_AD = 9
        SHOPPING_PRODUCT_AD = 10
        VIDEO_AD = 12
        IMAGE_AD = 14
        RESPONSIVE_SEARCH_AD = 15
        LEGACY_RESPONSIVE_DISPLAY_AD = 16
        APP_AD = 17
        LEGACY_APP_INSTALL_AD = 18
        RESPONSIVE_DISPLAY_AD = 19
        LOCAL_AD = 20
        HTML5_UPLOAD_AD = 21
        DYNAMIC_HTML5_AD = 22
        APP_ENGAGEMENT_AD = 23
        SHOPPING_COMPARISON_LISTING_AD = 24
        VIDEO_BUMPER_AD = 25
        VIDEO_NON_SKIPPABLE_IN_STREAM_AD = 26
        VIDEO_TRUEVIEW_IN_STREAM_AD = 29
        VIDEO_RESPONSIVE_AD = 30
        SMART_CAMPAIGN_AD = 31
        CALL_AD = 32
        APP_PRE_REGISTRATION_AD = 33
        IN_FEED_VIDEO_AD = 34
        DEMAND_GEN_MULTI_ASSET_AD = 40
        DEMAND_GEN_CAROUSEL_AD = 41
        TRAVEL_AD = 37
        DEMAND_GEN_VIDEO_RESPONSIVE_AD = 42
        DEMAND_GEN_PRODUCT_AD = 39
        YOUTUBE_AUDIO_AD = 44


__all__ = tuple(sorted(__protobuf__.manifest))

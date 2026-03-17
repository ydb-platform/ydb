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
        "AssetFieldTypeEnum",
    },
)


class AssetFieldTypeEnum(proto.Message):
    r"""Container for enum describing the possible placements of an
    asset.

    """

    class AssetFieldType(proto.Enum):
        r"""Enum describing the possible placements of an asset.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            HEADLINE (2):
                The asset is linked for use as a headline.
            DESCRIPTION (3):
                The asset is linked for use as a description.
            MANDATORY_AD_TEXT (4):
                The asset is linked for use as mandatory ad
                text.
            MARKETING_IMAGE (5):
                The asset is linked for use as a marketing
                image.
            MEDIA_BUNDLE (6):
                The asset is linked for use as a media
                bundle.
            YOUTUBE_VIDEO (7):
                The asset is linked for use as a YouTube
                video.
            BOOK_ON_GOOGLE (8):
                The asset is linked to indicate that a hotels
                campaign is "Book on Google" enabled.
            LEAD_FORM (9):
                The asset is linked for use as a Lead Form
                extension.
            PROMOTION (10):
                The asset is linked for use as a Promotion
                extension.
            CALLOUT (11):
                The asset is linked for use as a Callout
                extension.
            STRUCTURED_SNIPPET (12):
                The asset is linked for use as a Structured
                Snippet extension.
            SITELINK (13):
                The asset is linked for use as a Sitelink.
            MOBILE_APP (14):
                The asset is linked for use as a Mobile App
                extension.
            HOTEL_CALLOUT (15):
                The asset is linked for use as a Hotel
                Callout extension.
            CALL (16):
                The asset is linked for use as a Call
                extension.
            PRICE (24):
                The asset is linked for use as a Price
                extension.
            LONG_HEADLINE (17):
                The asset is linked for use as a long
                headline.
            BUSINESS_NAME (18):
                The asset is linked for use as a business
                name.
            SQUARE_MARKETING_IMAGE (19):
                The asset is linked for use as a square
                marketing image.
            PORTRAIT_MARKETING_IMAGE (20):
                The asset is linked for use as a portrait
                marketing image.
            LOGO (21):
                The asset is linked for use as a logo.
            LANDSCAPE_LOGO (22):
                The asset is linked for use as a landscape
                logo.
            VIDEO (23):
                The asset is linked for use as a non YouTube
                logo.
            CALL_TO_ACTION_SELECTION (25):
                The asset is linked for use to select a
                call-to-action.
            AD_IMAGE (26):
                The asset is linked for use to select an ad
                image.
            BUSINESS_LOGO (27):
                The asset is linked for use as a business
                logo.
            HOTEL_PROPERTY (28):
                The asset is linked for use as a hotel
                property in a Performance Max for travel goals
                campaign.
            DEMAND_GEN_CAROUSEL_CARD (30):
                The asset is linked for use as a Demand Gen
                carousel card.
            BUSINESS_MESSAGE (31):
                The asset is linked for use as a Business
                Message.
            TALL_PORTRAIT_MARKETING_IMAGE (32):
                The asset is linked for use as a tall
                portrait marketing image.
            RELATED_YOUTUBE_VIDEOS (33):
                The asset is linked for use as related
                YouTube videos.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        HEADLINE = 2
        DESCRIPTION = 3
        MANDATORY_AD_TEXT = 4
        MARKETING_IMAGE = 5
        MEDIA_BUNDLE = 6
        YOUTUBE_VIDEO = 7
        BOOK_ON_GOOGLE = 8
        LEAD_FORM = 9
        PROMOTION = 10
        CALLOUT = 11
        STRUCTURED_SNIPPET = 12
        SITELINK = 13
        MOBILE_APP = 14
        HOTEL_CALLOUT = 15
        CALL = 16
        PRICE = 24
        LONG_HEADLINE = 17
        BUSINESS_NAME = 18
        SQUARE_MARKETING_IMAGE = 19
        PORTRAIT_MARKETING_IMAGE = 20
        LOGO = 21
        LANDSCAPE_LOGO = 22
        VIDEO = 23
        CALL_TO_ACTION_SELECTION = 25
        AD_IMAGE = 26
        BUSINESS_LOGO = 27
        HOTEL_PROPERTY = 28
        DEMAND_GEN_CAROUSEL_CARD = 30
        BUSINESS_MESSAGE = 31
        TALL_PORTRAIT_MARKETING_IMAGE = 32
        RELATED_YOUTUBE_VIDEOS = 33


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "ServedAssetFieldTypeEnum",
    },
)


class ServedAssetFieldTypeEnum(proto.Message):
    r"""Container for enum describing possible asset field types."""

    class ServedAssetFieldType(proto.Enum):
        r"""The possible asset field types.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            HEADLINE_1 (2):
                The asset is used in headline 1.
            HEADLINE_2 (3):
                The asset is used in headline 2.
            HEADLINE_3 (4):
                The asset is used in headline 3.
            DESCRIPTION_1 (5):
                The asset is used in description 1.
            DESCRIPTION_2 (6):
                The asset is used in description 2.
            HEADLINE (7):
                The asset was used in a headline. Use this only if there is
                only one headline in the ad. Otherwise, use the HEADLINE_1,
                HEADLINE_2 or HEADLINE_3 enums
            HEADLINE_IN_PORTRAIT (8):
                The asset was used as a headline in portrait
                image.
            LONG_HEADLINE (9):
                The asset was used in a long headline (used
                in MultiAssetResponsiveAd).
            DESCRIPTION (10):
                The asset was used in a description. Use this only if there
                is only one description in the ad. Otherwise, use the
                DESCRIPTION_1 or DESCRIPTION\_@ enums
            DESCRIPTION_IN_PORTRAIT (11):
                The asset was used as description in portrait
                image.
            BUSINESS_NAME_IN_PORTRAIT (12):
                The asset was used as business name in
                portrait image.
            BUSINESS_NAME (13):
                The asset was used as business name.
            MARKETING_IMAGE (14):
                The asset was used as a marketing image.
            MARKETING_IMAGE_IN_PORTRAIT (15):
                The asset was used as a marketing image in
                portrait image.
            SQUARE_MARKETING_IMAGE (16):
                The asset was used as a square marketing
                image.
            PORTRAIT_MARKETING_IMAGE (17):
                The asset was used as a portrait marketing
                image.
            LOGO (18):
                The asset was used as a logo.
            LANDSCAPE_LOGO (19):
                The asset was used as a landscape logo.
            CALL_TO_ACTION (20):
                The asset was used as a call-to-action.
            YOU_TUBE_VIDEO (21):
                The asset was used as a YouTube video.
            SITELINK (22):
                This asset is used as a sitelink.
            CALL (23):
                This asset is used as a call.
            MOBILE_APP (24):
                This asset is used as a mobile app.
            CALLOUT (25):
                This asset is used as a callout.
            STRUCTURED_SNIPPET (26):
                This asset is used as a structured snippet.
            PRICE (27):
                This asset is used as a price.
            PROMOTION (28):
                This asset is used as a promotion.
            AD_IMAGE (29):
                This asset is used as an image.
            LEAD_FORM (30):
                The asset is used as a lead form.
            BUSINESS_LOGO (31):
                The asset is used as a business logo.
            DESCRIPTION_PREFIX (32):
                The asset is used as a description prefix.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        HEADLINE_1 = 2
        HEADLINE_2 = 3
        HEADLINE_3 = 4
        DESCRIPTION_1 = 5
        DESCRIPTION_2 = 6
        HEADLINE = 7
        HEADLINE_IN_PORTRAIT = 8
        LONG_HEADLINE = 9
        DESCRIPTION = 10
        DESCRIPTION_IN_PORTRAIT = 11
        BUSINESS_NAME_IN_PORTRAIT = 12
        BUSINESS_NAME = 13
        MARKETING_IMAGE = 14
        MARKETING_IMAGE_IN_PORTRAIT = 15
        SQUARE_MARKETING_IMAGE = 16
        PORTRAIT_MARKETING_IMAGE = 17
        LOGO = 18
        LANDSCAPE_LOGO = 19
        CALL_TO_ACTION = 20
        YOU_TUBE_VIDEO = 21
        SITELINK = 22
        CALL = 23
        MOBILE_APP = 24
        CALLOUT = 25
        STRUCTURED_SNIPPET = 26
        PRICE = 27
        PROMOTION = 28
        AD_IMAGE = 29
        LEAD_FORM = 30
        BUSINESS_LOGO = 31
        DESCRIPTION_PREFIX = 32


__all__ = tuple(sorted(__protobuf__.manifest))

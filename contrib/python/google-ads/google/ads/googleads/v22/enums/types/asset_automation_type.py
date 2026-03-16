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
        "AssetAutomationTypeEnum",
    },
)


class AssetAutomationTypeEnum(proto.Message):
    r"""Container for enum describing the type of asset automation."""

    class AssetAutomationType(proto.Enum):
        r"""The type of asset automation.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used as a return value only. Represents value
                unknown in this version.
            TEXT_ASSET_AUTOMATION (2):
                Text asset automation includes headlines and
                descriptions. By default, advertisers are
                opted-in for Performance Max and opted-out for
                Search.
            GENERATE_VERTICAL_YOUTUBE_VIDEOS (3):
                Converts horizontal video assets to vertical
                orientation using content-aware technology. By
                default, advertisers are opted in for
                DemandGenVideoResponsiveAd.
            GENERATE_SHORTER_YOUTUBE_VIDEOS (4):
                Shortens video assets to better capture user
                attention using content-aware technology. By
                default, advertisers are opted in for
                DemandGenVideoResponsiveAd.
            GENERATE_LANDING_PAGE_PREVIEW (5):
                Generates a preview of the landing page shown
                in the engagement panel.
                By using this feature, you confirm that you own
                all legal rights to the images on the landing
                page used by this account (or you have
                permission to share the images with Google). You
                hereby instruct Google to publish these images
                on your behalf for advertising or other
                commercial purposes.
            GENERATE_ENHANCED_YOUTUBE_VIDEOS (6):
                Generates video enhancements (vertical and
                shorter videos) for PMax campaigns. Opted in by
                default.
            GENERATE_IMAGE_ENHANCEMENT (7):
                Generates image enhancements (AutoCrop and
                AutoEnhance). Opted in by default for pmax.
            GENERATE_IMAGE_EXTRACTION (9):
                Generates image extraction. It defaults to
                account level Dynamic Image Extension control
                value.
            GENERATE_DESIGN_VERSIONS_FOR_IMAGES (10):
                Adds design elements and embeds text assets
                into image assets to create images with
                different aspect ratios. By default, advertisers
                are opted in for DemandGenMultiAssetAd.
            FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION (11):
                Controls automation for text assets related
                to Final URL expansion. This includes
                automatically creating dynamic landing pages
                from the final URL and generating text assets
                from the content of those landing pages. This
                setting is turned OFF by default for Search
                campaigns, but it is turned ON by default for
                Performance Max campaigns.
            GENERATE_VIDEOS_FROM_OTHER_ASSETS (12):
                Generates videos using other Assets as input,
                such as images and text. By default, advertisers
                are opted in for DemandGenMultiAssetAd.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        TEXT_ASSET_AUTOMATION = 2
        GENERATE_VERTICAL_YOUTUBE_VIDEOS = 3
        GENERATE_SHORTER_YOUTUBE_VIDEOS = 4
        GENERATE_LANDING_PAGE_PREVIEW = 5
        GENERATE_ENHANCED_YOUTUBE_VIDEOS = 6
        GENERATE_IMAGE_ENHANCEMENT = 7
        GENERATE_IMAGE_EXTRACTION = 9
        GENERATE_DESIGN_VERSIONS_FOR_IMAGES = 10
        FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION = 11
        GENERATE_VIDEOS_FROM_OTHER_ASSETS = 12


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "ExperimentTypeEnum",
    },
)


class ExperimentTypeEnum(proto.Message):
    r"""Container for enum describing the type of experiment."""

    class ExperimentType(proto.Enum):
        r"""The type of the experiment.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            DISPLAY_AND_VIDEO_360 (2):
                This is a DISPLAY_AND_VIDEO_360 experiment.
            AD_VARIATION (3):
                This is an ad variation experiment.
            YOUTUBE_CUSTOM (5):
                A custom experiment consisting of Video
                campaigns.
            DISPLAY_CUSTOM (6):
                A custom experiment consisting of display
                campaigns.
            SEARCH_CUSTOM (7):
                A custom experiment consisting of search
                campaigns.
            DISPLAY_AUTOMATED_BIDDING_STRATEGY (8):
                An experiment that compares bidding
                strategies for display campaigns.
            SEARCH_AUTOMATED_BIDDING_STRATEGY (9):
                An experiment that compares bidding
                strategies for search campaigns.".
            SHOPPING_AUTOMATED_BIDDING_STRATEGY (10):
                An experiment that compares bidding
                strategies for shopping campaigns.
            SMART_MATCHING (11):
                DEPRECATED. A smart matching experiment with
                search campaigns.
            HOTEL_CUSTOM (12):
                A custom experiment consisting of hotel
                campaigns.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DISPLAY_AND_VIDEO_360 = 2
        AD_VARIATION = 3
        YOUTUBE_CUSTOM = 5
        DISPLAY_CUSTOM = 6
        SEARCH_CUSTOM = 7
        DISPLAY_AUTOMATED_BIDDING_STRATEGY = 8
        SEARCH_AUTOMATED_BIDDING_STRATEGY = 9
        SHOPPING_AUTOMATED_BIDDING_STRATEGY = 10
        SMART_MATCHING = 11
        HOTEL_CUSTOM = 12


__all__ = tuple(sorted(__protobuf__.manifest))

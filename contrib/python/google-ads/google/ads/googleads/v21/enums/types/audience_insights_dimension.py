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
        "AudienceInsightsDimensionEnum",
    },
)


class AudienceInsightsDimensionEnum(proto.Message):
    r"""Container for enum describing insights dimensions."""

    class AudienceInsightsDimension(proto.Enum):
        r"""Possible dimensions for use in generating insights.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            CATEGORY (2):
                A Product & Service category.
            KNOWLEDGE_GRAPH (3):
                A Knowledge Graph entity.
            GEO_TARGET_COUNTRY (4):
                A country, represented by a geo target.
            SUB_COUNTRY_LOCATION (5):
                A geographic location within a country.
            YOUTUBE_CHANNEL (6):
                A YouTube channel.
            AFFINITY_USER_INTEREST (8):
                An Affinity UserInterest.
            IN_MARKET_USER_INTEREST (9):
                An In-Market UserInterest.
            PARENTAL_STATUS (10):
                A Parental Status value (parent, or not a
                parent).
            INCOME_RANGE (11):
                A household income percentile range.
            AGE_RANGE (12):
                An age range.
            GENDER (13):
                A gender.
            YOUTUBE_VIDEO (14):
                A YouTube video.
            DEVICE (15):
                A device type, such as Mobile, Desktop,
                Tablet, and Connected TV.
            YOUTUBE_LINEUP (16):
                A YouTube Lineup.
            USER_LIST (17):
                A User List.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CATEGORY = 2
        KNOWLEDGE_GRAPH = 3
        GEO_TARGET_COUNTRY = 4
        SUB_COUNTRY_LOCATION = 5
        YOUTUBE_CHANNEL = 6
        AFFINITY_USER_INTEREST = 8
        IN_MARKET_USER_INTEREST = 9
        PARENTAL_STATUS = 10
        INCOME_RANGE = 11
        AGE_RANGE = 12
        GENDER = 13
        YOUTUBE_VIDEO = 14
        DEVICE = 15
        YOUTUBE_LINEUP = 16
        USER_LIST = 17


__all__ = tuple(sorted(__protobuf__.manifest))

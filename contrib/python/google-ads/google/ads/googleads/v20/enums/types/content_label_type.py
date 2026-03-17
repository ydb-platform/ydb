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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "ContentLabelTypeEnum",
    },
)


class ContentLabelTypeEnum(proto.Message):
    r"""Container for enum describing content label types in
    ContentLabel.

    """

    class ContentLabelType(proto.Enum):
        r"""Enum listing the content label types supported by
        ContentLabel criterion.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            SEXUALLY_SUGGESTIVE (2):
                Sexually suggestive content.
            BELOW_THE_FOLD (3):
                Below the fold placement.
            PARKED_DOMAIN (4):
                Parked domain.
            JUVENILE (6):
                Juvenile, gross & bizarre content.
            PROFANITY (7):
                Profanity & rough language.
            TRAGEDY (8):
                Death & tragedy.
            VIDEO (9):
                Video.
            VIDEO_RATING_DV_G (10):
                Content rating: G.
            VIDEO_RATING_DV_PG (11):
                Content rating: PG.
            VIDEO_RATING_DV_T (12):
                Content rating: T.
            VIDEO_RATING_DV_MA (13):
                Content rating: MA.
            VIDEO_NOT_YET_RATED (14):
                Content rating: not yet rated.
            EMBEDDED_VIDEO (15):
                Embedded video.
            LIVE_STREAMING_VIDEO (16):
                Live streaming video.
            SOCIAL_ISSUES (17):
                Sensitive social issues.
            BRAND_SUITABILITY_CONTENT_FOR_FAMILIES (18):
                Content that's suitable for families to view
                together, including Made for Kids videos on
                YouTube.
            BRAND_SUITABILITY_GAMES_FIGHTING (19):
                Video games that simulate hand-to-hand
                fighting or combat with the use of modern or
                medieval weapons.
            BRAND_SUITABILITY_GAMES_MATURE (20):
                Video games that feature mature content, such
                as violence, inappropriate language, or sexual
                suggestiveness.
            BRAND_SUITABILITY_HEALTH_SENSITIVE (21):
                Health content that people might find
                sensitive or upsetting, such as medical
                procedures or images and descriptions of various
                medical conditions.
            BRAND_SUITABILITY_HEALTH_SOURCE_UNDETERMINED (22):
                Health content from sources that may provide
                accurate information but aren't as commonly
                cited as other, more well-known sources.
            BRAND_SUITABILITY_NEWS_RECENT (23):
                News content that's been recently announced,
                regardless of the themes or people being
                reported on.
            BRAND_SUITABILITY_NEWS_SENSITIVE (24):
                News content that people might find sensitive
                or upsetting, such as crimes, accidents, and
                natural incidents, or commentary on potentially
                controversial social and political issues.
            BRAND_SUITABILITY_NEWS_SOURCE_NOT_FEATURED (25):
                News content from sources that aren't
                featured on Google News or YouTube News.
            BRAND_SUITABILITY_POLITICS (26):
                Political content, such as political
                statements made by well-known politicians,
                political elections, or events widely perceived
                to be political in nature.
            BRAND_SUITABILITY_RELIGION (27):
                Content with religious themes, such as
                religious teachings or customs, holy sites or
                places of worship, well-known religious figures
                or people dressed in religious attire, or
                religious opinions on social and political
                issues.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SEXUALLY_SUGGESTIVE = 2
        BELOW_THE_FOLD = 3
        PARKED_DOMAIN = 4
        JUVENILE = 6
        PROFANITY = 7
        TRAGEDY = 8
        VIDEO = 9
        VIDEO_RATING_DV_G = 10
        VIDEO_RATING_DV_PG = 11
        VIDEO_RATING_DV_T = 12
        VIDEO_RATING_DV_MA = 13
        VIDEO_NOT_YET_RATED = 14
        EMBEDDED_VIDEO = 15
        LIVE_STREAMING_VIDEO = 16
        SOCIAL_ISSUES = 17
        BRAND_SUITABILITY_CONTENT_FOR_FAMILIES = 18
        BRAND_SUITABILITY_GAMES_FIGHTING = 19
        BRAND_SUITABILITY_GAMES_MATURE = 20
        BRAND_SUITABILITY_HEALTH_SENSITIVE = 21
        BRAND_SUITABILITY_HEALTH_SOURCE_UNDETERMINED = 22
        BRAND_SUITABILITY_NEWS_RECENT = 23
        BRAND_SUITABILITY_NEWS_SENSITIVE = 24
        BRAND_SUITABILITY_NEWS_SOURCE_NOT_FEATURED = 25
        BRAND_SUITABILITY_POLITICS = 26
        BRAND_SUITABILITY_RELIGION = 27


__all__ = tuple(sorted(__protobuf__.manifest))

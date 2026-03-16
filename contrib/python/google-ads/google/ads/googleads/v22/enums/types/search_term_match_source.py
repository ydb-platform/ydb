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
        "SearchTermMatchSourceEnum",
    },
)


class SearchTermMatchSourceEnum(proto.Message):
    r"""Container for enum describing the source for search term
    matches in the search term report.

    """

    class SearchTermMatchSource(proto.Enum):
        r"""The search term match sources.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ADVERTISER_PROVIDED_KEYWORD (2):
                The match is from a user-provided keyword.
            AI_MAX_KEYWORDLESS (3):
                The match is from the keywordless expansion
                portion of AI Max.
            AI_MAX_BROAD_MATCH (4):
                The match is from the broad match expansion
                portion of AI Max.
            DYNAMIC_SEARCH_ADS (5):
                The match is from a Dynamic Search Ad.
            PERFORMANCE_MAX (6):
                The match is from the search term matching
                functionality in PMax.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ADVERTISER_PROVIDED_KEYWORD = 2
        AI_MAX_KEYWORDLESS = 3
        AI_MAX_BROAD_MATCH = 4
        DYNAMIC_SEARCH_ADS = 5
        PERFORMANCE_MAX = 6


__all__ = tuple(sorted(__protobuf__.manifest))

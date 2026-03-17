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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "KeywordPlanAdGroupKeywordErrorEnum",
    },
)


class KeywordPlanAdGroupKeywordErrorEnum(proto.Message):
    r"""Container for enum describing possible errors from applying
    an ad group keyword or a campaign keyword from a keyword plan.

    """

    class KeywordPlanAdGroupKeywordError(proto.Enum):
        r"""Enum describing possible errors from applying a keyword plan
        ad group keyword or keyword plan campaign keyword.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_KEYWORD_MATCH_TYPE (2):
                A keyword or negative keyword has invalid
                match type.
            DUPLICATE_KEYWORD (3):
                A keyword or negative keyword with same text
                and match type already exists.
            KEYWORD_TEXT_TOO_LONG (4):
                Keyword or negative keyword text exceeds the
                allowed limit.
            KEYWORD_HAS_INVALID_CHARS (5):
                Keyword or negative keyword text has invalid
                characters or symbols.
            KEYWORD_HAS_TOO_MANY_WORDS (6):
                Keyword or negative keyword text has too many
                words.
            INVALID_KEYWORD_TEXT (7):
                Keyword or negative keyword has invalid text.
            NEGATIVE_KEYWORD_HAS_CPC_BID (8):
                Cpc Bid set for negative keyword.
            NEW_BMM_KEYWORDS_NOT_ALLOWED (9):
                New broad match modifier (BMM)
                KpAdGroupKeywords are not allowed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_KEYWORD_MATCH_TYPE = 2
        DUPLICATE_KEYWORD = 3
        KEYWORD_TEXT_TOO_LONG = 4
        KEYWORD_HAS_INVALID_CHARS = 5
        KEYWORD_HAS_TOO_MANY_WORDS = 6
        INVALID_KEYWORD_TEXT = 7
        NEGATIVE_KEYWORD_HAS_CPC_BID = 8
        NEW_BMM_KEYWORDS_NOT_ALLOWED = 9


__all__ = tuple(sorted(__protobuf__.manifest))

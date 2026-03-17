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
        "SearchTermInsightErrorEnum",
    },
)


class SearchTermInsightErrorEnum(proto.Message):
    r"""Container for enum describing possible search term insight
    errors.

    """

    class SearchTermInsightError(proto.Enum):
        r"""Enum describing possible search term insight errors.

        Values:
            UNSPECIFIED (0):
                Name unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            FILTERING_NOT_ALLOWED_WITH_SEGMENTS (2):
                Search term insights cannot be filtered by
                metrics when segmenting.
            LIMIT_NOT_ALLOWED_WITH_SEGMENTS (3):
                Search term insights cannot have a LIMIT when
                segmenting.
            MISSING_FIELD_IN_SELECT_CLAUSE (4):
                A selected field requires another field to be
                selected with it.
            REQUIRES_FILTER_BY_SINGLE_RESOURCE (5):
                A selected field/resource requires filtering
                by a single resource.
            SORTING_NOT_ALLOWED_WITH_SEGMENTS (6):
                Search term insights cannot be sorted when
                segmenting.
            SUMMARY_ROW_NOT_ALLOWED_WITH_SEGMENTS (7):
                Search term insights cannot have a summary
                row when segmenting.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FILTERING_NOT_ALLOWED_WITH_SEGMENTS = 2
        LIMIT_NOT_ALLOWED_WITH_SEGMENTS = 3
        MISSING_FIELD_IN_SELECT_CLAUSE = 4
        REQUIRES_FILTER_BY_SINGLE_RESOURCE = 5
        SORTING_NOT_ALLOWED_WITH_SEGMENTS = 6
        SUMMARY_ROW_NOT_ALLOWED_WITH_SEGMENTS = 7


__all__ = tuple(sorted(__protobuf__.manifest))

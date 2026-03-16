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
        "SearchEngineResultsPageTypeEnum",
    },
)


class SearchEngineResultsPageTypeEnum(proto.Message):
    r"""The type of the search engine results page."""

    class SearchEngineResultsPageType(proto.Enum):
        r"""The type of the search engine results page.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ADS_ONLY (2):
                Only ads were contained in the search engine
                results page.
            ORGANIC_ONLY (3):
                Only organic results were contained in the
                search engine results page.
            ADS_AND_ORGANIC (4):
                Both ads and organic results were contained
                in the search engine results page.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ADS_ONLY = 2
        ORGANIC_ONLY = 3
        ADS_AND_ORGANIC = 4


__all__ = tuple(sorted(__protobuf__.manifest))

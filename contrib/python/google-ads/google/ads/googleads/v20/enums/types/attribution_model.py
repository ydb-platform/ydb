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
        "AttributionModelEnum",
    },
)


class AttributionModelEnum(proto.Message):
    r"""Container for enum representing the attribution model that
    describes how to distribute credit for a particular conversion
    across potentially many prior interactions.

    """

    class AttributionModel(proto.Enum):
        r"""The attribution model that describes how to distribute credit
        for a particular conversion across potentially many prior
        interactions.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            EXTERNAL (100):
                Uses external attribution.
            GOOGLE_ADS_LAST_CLICK (101):
                Attributes all credit for a conversion to its
                last click.
            GOOGLE_SEARCH_ATTRIBUTION_FIRST_CLICK (102):
                Attributes all credit for a conversion to its
                first click using Google Search attribution.
            GOOGLE_SEARCH_ATTRIBUTION_LINEAR (103):
                Attributes credit for a conversion equally
                across all of its clicks using Google Search
                attribution.
            GOOGLE_SEARCH_ATTRIBUTION_TIME_DECAY (104):
                Attributes exponentially more credit for a
                conversion to its more recent clicks using
                Google Search attribution (half-life is 1 week).
            GOOGLE_SEARCH_ATTRIBUTION_POSITION_BASED (105):
                Attributes 40% of the credit for a conversion
                to its first and last clicks. Remaining 20% is
                evenly distributed across all other clicks. This
                uses Google Search attribution.
            GOOGLE_SEARCH_ATTRIBUTION_DATA_DRIVEN (106):
                Flexible model that uses machine learning to
                determine the appropriate distribution of credit
                among clicks using Google Search attribution.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        EXTERNAL = 100
        GOOGLE_ADS_LAST_CLICK = 101
        GOOGLE_SEARCH_ATTRIBUTION_FIRST_CLICK = 102
        GOOGLE_SEARCH_ATTRIBUTION_LINEAR = 103
        GOOGLE_SEARCH_ATTRIBUTION_TIME_DECAY = 104
        GOOGLE_SEARCH_ATTRIBUTION_POSITION_BASED = 105
        GOOGLE_SEARCH_ATTRIBUTION_DATA_DRIVEN = 106


__all__ = tuple(sorted(__protobuf__.manifest))

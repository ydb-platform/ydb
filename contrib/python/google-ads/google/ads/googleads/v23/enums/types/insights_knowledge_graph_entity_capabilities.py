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
    package="google.ads.googleads.v23.enums",
    marshal="google.ads.googleads.v23",
    manifest={
        "InsightsKnowledgeGraphEntityCapabilitiesEnum",
    },
)


class InsightsKnowledgeGraphEntityCapabilitiesEnum(proto.Message):
    r"""Container for enum describing the capabilities of an entity
    related to ContentCreatorInsightsService.

    """

    class InsightsKnowledgeGraphEntityCapabilities(proto.Enum):
        r"""The capabilities of an entity.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            CONTENT_TRENDING_INSIGHTS (2):
                An entity that is supported to use as a trending topic in
                [ContentCreatorInsightsService.GenerateTrendingInsights][google.ads.googleads.v23.services.ContentCreatorInsightsService.GenerateTrendingInsights].
            CREATOR_ATTRIBUTE (3):
                An entity that is supported to use as a creator attribute in
                [ContentCreatorInsightsService.GenerateCreatorInsights][google.ads.googleads.v23.services.ContentCreatorInsightsService.GenerateCreatorInsights].
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONTENT_TRENDING_INSIGHTS = 2
        CREATOR_ATTRIBUTE = 3


__all__ = tuple(sorted(__protobuf__.manifest))

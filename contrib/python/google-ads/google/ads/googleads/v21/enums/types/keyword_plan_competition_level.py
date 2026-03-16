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
        "KeywordPlanCompetitionLevelEnum",
    },
)


class KeywordPlanCompetitionLevelEnum(proto.Message):
    r"""Container for enumeration of keyword competition levels. The
    competition level indicates how competitive ad placement is for
    a keyword and is determined by the number of advertisers bidding
    on that keyword relative to all keywords across Google. The
    competition level can depend on the location and Search Network
    targeting options you've selected.

    """

    class KeywordPlanCompetitionLevel(proto.Enum):
        r"""Competition level of a keyword.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            LOW (2):
                Low competition. The Competition Index range for this is [0,
                33].
            MEDIUM (3):
                Medium competition. The Competition Index range for this is
                [34, 66].
            HIGH (4):
                High competition. The Competition Index range for this is
                [67, 100].
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        LOW = 2
        MEDIUM = 3
        HIGH = 4


__all__ = tuple(sorted(__protobuf__.manifest))

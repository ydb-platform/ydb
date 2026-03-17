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
        "LookalikeExpansionLevelEnum",
    },
)


class LookalikeExpansionLevelEnum(proto.Message):
    r"""Lookalike Expansion level proto"""

    class LookalikeExpansionLevel(proto.Enum):
        r"""Expansion level, reflecting the size of the lookalike
        audience

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Invalid expansion level.
            NARROW (2):
                Expansion to a small set of users that are
                similar to the seed lists
            BALANCED (3):
                Expansion to a medium set of users that are similar to the
                seed lists. Includes all users of EXPANSION_LEVEL_NARROW,
                and more.
            BROAD (4):
                Expansion to a large set of users that are similar to the
                seed lists. Includes all users of EXPANSION_LEVEL_BALANCED,
                and more.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NARROW = 2
        BALANCED = 3
        BROAD = 4


__all__ = tuple(sorted(__protobuf__.manifest))

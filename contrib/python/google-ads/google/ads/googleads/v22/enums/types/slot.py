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
        "SlotEnum",
    },
)


class SlotEnum(proto.Message):
    r"""Container for enumeration of possible positions of the Ad."""

    class Slot(proto.Enum):
        r"""Enumerates possible positions of the Ad.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            SEARCH_SIDE (2):
                Google search: Side.
            SEARCH_TOP (3):
                Google search: Top.
            SEARCH_OTHER (4):
                Google search: Other.
            CONTENT (5):
                Google Display Network.
            SEARCH_PARTNER_TOP (6):
                Search partners: Top.
            SEARCH_PARTNER_OTHER (7):
                Search partners: Other.
            MIXED (8):
                Cross-network.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SEARCH_SIDE = 2
        SEARCH_TOP = 3
        SEARCH_OTHER = 4
        CONTENT = 5
        SEARCH_PARTNER_TOP = 6
        SEARCH_PARTNER_OTHER = 7
        MIXED = 8


__all__ = tuple(sorted(__protobuf__.manifest))

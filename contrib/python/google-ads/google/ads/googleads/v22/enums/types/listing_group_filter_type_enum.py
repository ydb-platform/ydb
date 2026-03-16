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
        "ListingGroupFilterTypeEnum",
    },
)


class ListingGroupFilterTypeEnum(proto.Message):
    r"""Container for enum describing the type of the listing group
    filter node.

    """

    class ListingGroupFilterType(proto.Enum):
        r"""The type of the listing group filter.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            SUBDIVISION (2):
                Subdivision of products along some listing
                dimensions.
            UNIT_INCLUDED (3):
                An included listing group filter leaf node.
            UNIT_EXCLUDED (4):
                An excluded listing group filter leaf node.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SUBDIVISION = 2
        UNIT_INCLUDED = 3
        UNIT_EXCLUDED = 4


__all__ = tuple(sorted(__protobuf__.manifest))

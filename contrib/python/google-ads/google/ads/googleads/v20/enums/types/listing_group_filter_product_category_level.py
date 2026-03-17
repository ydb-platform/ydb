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
        "ListingGroupFilterProductCategoryLevelEnum",
    },
)


class ListingGroupFilterProductCategoryLevelEnum(proto.Message):
    r"""Container for enum describing the levels of product category
    used in ListingGroupFilterDimension.

    """

    class ListingGroupFilterProductCategoryLevel(proto.Enum):
        r"""The level of the listing group filter product category.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            LEVEL1 (2):
                Level 1.
            LEVEL2 (3):
                Level 2.
            LEVEL3 (4):
                Level 3.
            LEVEL4 (5):
                Level 4.
            LEVEL5 (6):
                Level 5.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        LEVEL1 = 2
        LEVEL2 = 3
        LEVEL3 = 4
        LEVEL4 = 5
        LEVEL5 = 6


__all__ = tuple(sorted(__protobuf__.manifest))

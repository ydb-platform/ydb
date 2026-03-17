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
        "AssetPerformanceLabelEnum",
    },
)


class AssetPerformanceLabelEnum(proto.Message):
    r"""Container for enum describing the performance label of an
    asset.

    """

    class AssetPerformanceLabel(proto.Enum):
        r"""Enum describing the possible performance labels of an asset,
        usually computed in the context of a linkage.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PENDING (2):
                This asset does not yet have any performance
                informantion. This may be because it is still
                under review.
            LEARNING (3):
                The asset has started getting impressions but
                the stats are not statistically significant
                enough to get an asset performance label.
            LOW (4):
                Worst performing assets.
            GOOD (5):
                Good performing assets.
            BEST (6):
                Best performing assets.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PENDING = 2
        LEARNING = 3
        LOW = 4
        GOOD = 5
        BEST = 6


__all__ = tuple(sorted(__protobuf__.manifest))

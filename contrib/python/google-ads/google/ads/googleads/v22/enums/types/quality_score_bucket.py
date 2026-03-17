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
        "QualityScoreBucketEnum",
    },
)


class QualityScoreBucketEnum(proto.Message):
    r"""The relative performance compared to other advertisers."""

    class QualityScoreBucket(proto.Enum):
        r"""Enum listing the possible quality score buckets.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            BELOW_AVERAGE (2):
                Quality of the creative is below average.
            AVERAGE (3):
                Quality of the creative is average.
            ABOVE_AVERAGE (4):
                Quality of the creative is above average.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BELOW_AVERAGE = 2
        AVERAGE = 3
        ABOVE_AVERAGE = 4


__all__ = tuple(sorted(__protobuf__.manifest))

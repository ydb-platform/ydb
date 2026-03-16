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
        "SkAdNetworkCoarseConversionValueEnum",
    },
)


class SkAdNetworkCoarseConversionValueEnum(proto.Message):
    r"""Container for enumeration of SkAdNetwork coarse conversion
    values.

    """

    class SkAdNetworkCoarseConversionValue(proto.Enum):
        r"""Enumerates SkAdNetwork coarse conversion values

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            UNAVAILABLE (2):
                The value was not present in the postback or
                we do not have this data for other reasons.
            LOW (3):
                A low coarse conversion value.
            MEDIUM (4):
                A medium coarse conversion value.
            HIGH (5):
                A high coarse conversion value.
            NONE (6):
                A coarse conversion value was not configured.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        UNAVAILABLE = 2
        LOW = 3
        MEDIUM = 4
        HIGH = 5
        NONE = 6


__all__ = tuple(sorted(__protobuf__.manifest))

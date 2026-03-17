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
        "NonSkippableMinDurationEnum",
    },
)


class NonSkippableMinDurationEnum(proto.Message):
    r"""Container for enum describing the allowed minimum duration
    values for videos used in non-skippable video responsive ads.

    """

    class NonSkippableMinDuration(proto.Enum):
        r"""Enum describing the allowed minimum duration values for
        videos used in non-skippable video responsive ads.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            MIN_DURATION_FIVE_SECONDS (2):
                Indicates that non-skippable ads must be at
                least 5 seconds long.
            MIN_DURATION_SEVEN_SECONDS (3):
                Indicates that non-skippable ads must be at
                least 7 seconds long.
            MIN_DURATION_SIXTEEN_SECONDS (4):
                Indicates that non-skippable ads must be at
                least 16 seconds long.
            MIN_DURATION_THIRTY_ONE_SECONDS (5):
                Indicates that non-skippable ads must be at
                least 31 seconds long.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        MIN_DURATION_FIVE_SECONDS = 2
        MIN_DURATION_SEVEN_SECONDS = 3
        MIN_DURATION_SIXTEEN_SECONDS = 4
        MIN_DURATION_THIRTY_ONE_SECONDS = 5


__all__ = tuple(sorted(__protobuf__.manifest))

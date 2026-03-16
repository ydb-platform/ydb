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
        "NonSkippableMaxDurationEnum",
    },
)


class NonSkippableMaxDurationEnum(proto.Message):
    r"""Container for enum describing the allowed maximum duration
    values for videos used in non-skippable video responsive ads.

    """

    class NonSkippableMaxDuration(proto.Enum):
        r"""Enum describing the allowed maximum duration values for
        videos used in non-skippable video responsive ads.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            MAX_DURATION_FIFTEEN_SECONDS (2):
                Indicates that non-skippable ads must be at
                most 15 seconds long.
            MAX_DURATION_THIRTY_SECONDS (3):
                Indicates that non-skippable ads must be at
                most 30 seconds long.
            MAX_DURATION_SIXTY_SECONDS (4):
                Indicates that non-skippable ads must be at
                most 60 seconds long.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        MAX_DURATION_FIFTEEN_SECONDS = 2
        MAX_DURATION_THIRTY_SECONDS = 3
        MAX_DURATION_SIXTY_SECONDS = 4


__all__ = tuple(sorted(__protobuf__.manifest))

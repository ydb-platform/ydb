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
        "VideoAdSequenceMinimumDurationEnum",
    },
)


class VideoAdSequenceMinimumDurationEnum(proto.Message):
    r"""Container for enum describing possible times after starting a
    video ad sequence when a user is eligible to repeat the
    sequence.

    """

    class VideoAdSequenceMinimumDuration(proto.Enum):
        r"""Enum describing possible times after starting a video ad
        sequence when a user is eligible to repeat the sequence.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            WEEK (2):
                Users are eligible to restart a sequence once
                they have completed the sequence and at least 7
                days have passed since sequence start.
            MONTH (3):
                Users are eligible to restart a sequence once
                at least 30 days have passed since sequence
                start. Users are eligible to start the sequence
                again even if they haven't completed the
                sequence.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        WEEK = 2
        MONTH = 3


__all__ = tuple(sorted(__protobuf__.manifest))

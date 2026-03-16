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
        "VideoAdSequenceInteractionTypeEnum",
    },
)


class VideoAdSequenceInteractionTypeEnum(proto.Message):
    r"""Container for enum describing an interaction between a viewer
    and a video in a video ad sequence.

    """

    class VideoAdSequenceInteractionType(proto.Enum):
        r"""Enum describing an interaction between a viewer and a video
        in a video ad sequence.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PAID_VIEW (2):
                The viewer engaged with or watched at least
                30 seconds of the ad (or the entire ad, if it
                was less than 30 seconds). Only available for
                campaigns using Target CPM as the bidding
                strategy and skippable in-stream ads as the ad
                format.
            SKIP (3):
                The viewer skipped the ad. Only available for
                campaigns using Target CPM as the bidding
                strategy and skippable in-stream ads as the ad
                format.
            IMPRESSION (4):
                The ad was shown to the viewer.
            ENGAGED_IMPRESSION (5):
                An ad impression that was not immediately
                skipped, but didn't reach the billable event
                either.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PAID_VIEW = 2
        SKIP = 3
        IMPRESSION = 4
        ENGAGED_IMPRESSION = 5


__all__ = tuple(sorted(__protobuf__.manifest))

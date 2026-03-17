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
        "InteractionEventTypeEnum",
    },
)


class InteractionEventTypeEnum(proto.Message):
    r"""Container for enum describing types of payable and free
    interactions.

    """

    class InteractionEventType(proto.Enum):
        r"""Enum describing possible types of payable and free
        interactions.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CLICK (2):
                Click to site. In most cases, this
                interaction navigates to an external location,
                usually the advertiser's landing page. This is
                also the default InteractionEventType for click
                events.
            ENGAGEMENT (3):
                The user's expressed intent to engage with
                the ad in-place.
            VIDEO_VIEW (4):
                User viewed a video ad.
            NONE (5):
                The default InteractionEventType for ad
                conversion events. This is used when an ad
                conversion row does NOT indicate that the free
                interactions (for example, the ad conversions)
                should be 'promoted' and reported as part of the
                core metrics. These are simply other (ad)
                conversions.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CLICK = 2
        ENGAGEMENT = 3
        VIDEO_VIEW = 4
        NONE = 5


__all__ = tuple(sorted(__protobuf__.manifest))

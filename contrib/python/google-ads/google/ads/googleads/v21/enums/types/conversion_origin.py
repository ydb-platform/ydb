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
        "ConversionOriginEnum",
    },
)


class ConversionOriginEnum(proto.Message):
    r"""Container for enum describing possible conversion origins."""

    class ConversionOrigin(proto.Enum):
        r"""The possible places where a conversion can occur.

        Values:
            UNSPECIFIED (0):
                The conversion origin has not been specified.
            UNKNOWN (1):
                The conversion origin is not known in this
                version.
            WEBSITE (2):
                Conversion that occurs when a user visits a
                website or takes an action there after viewing
                an ad.
            GOOGLE_HOSTED (3):
                Conversions reported by an offline pipeline
                which collects local actions from Google-hosted
                pages (for example, Google Maps, Google Place
                Page, etc) and attributes them to relevant ad
                events.
            APP (4):
                Conversion that occurs when a user performs
                an action through any app platforms.
            CALL_FROM_ADS (5):
                Conversion that occurs when a user makes a
                call from ads.
            STORE (6):
                Conversion that occurs when a user visits or
                makes a purchase at a physical store.
            YOUTUBE_HOSTED (7):
                Conversion that occurs on YouTube.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        WEBSITE = 2
        GOOGLE_HOSTED = 3
        APP = 4
        CALL_FROM_ADS = 5
        STORE = 6
        YOUTUBE_HOSTED = 7


__all__ = tuple(sorted(__protobuf__.manifest))

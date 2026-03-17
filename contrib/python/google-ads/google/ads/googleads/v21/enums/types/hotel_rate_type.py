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
        "HotelRateTypeEnum",
    },
)


class HotelRateTypeEnum(proto.Message):
    r"""Container for enum describing possible hotel rate types."""

    class HotelRateType(proto.Enum):
        r"""Enum describing possible hotel rate types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            UNAVAILABLE (2):
                Rate type information is unavailable.
            PUBLIC_RATE (3):
                Rates available to everyone.
            QUALIFIED_RATE (4):
                A membership program rate is available and
                satisfies basic requirements like having a
                public rate available. UI treatment will
                strikethrough the public rate and indicate that
                a discount is available to the user. For more on
                Qualified Rates, visit
                https://developers.google.com/hotels/hotel-ads/dev-guide/qualified-rates
            PRIVATE_RATE (5):
                Rates available to users that satisfy some
                eligibility criteria, for example, all signed-in
                users, 20% of mobile users, all mobile users in
                Canada, etc.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        UNAVAILABLE = 2
        PUBLIC_RATE = 3
        QUALIFIED_RATE = 4
        PRIVATE_RATE = 5


__all__ = tuple(sorted(__protobuf__.manifest))

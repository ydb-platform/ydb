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
        "SkAdNetworkAttributionCreditEnum",
    },
)


class SkAdNetworkAttributionCreditEnum(proto.Message):
    r"""Container for enumeration of SkAdNetwork attribution credits."""

    class SkAdNetworkAttributionCredit(proto.Enum):
        r"""Enumerates SkAdNetwork attribution credits.

        Values:
            UNSPECIFIED (0):
                Default value. This value is equivalent to
                null.
            UNKNOWN (1):
                The value is unknown in this API version. The
                true enum value cannot be returned in this API
                version or is not supported yet.
            UNAVAILABLE (2):
                The value was not present in the postback or
                we do not have this data for other reasons.
            WON (3):
                Google was the ad network that won ad
                attribution.
            CONTRIBUTED (4):
                Google qualified for attribution, but didn't
                win.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        UNAVAILABLE = 2
        WON = 3
        CONTRIBUTED = 4


__all__ = tuple(sorted(__protobuf__.manifest))

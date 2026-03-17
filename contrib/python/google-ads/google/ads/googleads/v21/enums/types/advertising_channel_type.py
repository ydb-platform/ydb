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
        "AdvertisingChannelTypeEnum",
    },
)


class AdvertisingChannelTypeEnum(proto.Message):
    r"""The channel type a campaign may target to serve on."""

    class AdvertisingChannelType(proto.Enum):
        r"""Enum describing the various advertising channel types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            SEARCH (2):
                Search Network. Includes display bundled, and
                Search+ campaigns.
            DISPLAY (3):
                Google Display Network only.
            SHOPPING (4):
                Shopping campaigns serve on the shopping
                property and on google.com search results.
            HOTEL (5):
                Hotel Ads campaigns.
            VIDEO (6):
                Video campaigns.
            MULTI_CHANNEL (7):
                App Campaigns, and App Campaigns for
                Engagement, that run across multiple channels.
            LOCAL (8):
                Local ads campaigns.
            SMART (9):
                Smart campaigns.
            PERFORMANCE_MAX (10):
                Performance Max campaigns.
            LOCAL_SERVICES (11):
                Local services campaigns.
            TRAVEL (13):
                Travel campaigns.
            DEMAND_GEN (14):
                Demand Gen campaigns.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SEARCH = 2
        DISPLAY = 3
        SHOPPING = 4
        HOTEL = 5
        VIDEO = 6
        MULTI_CHANNEL = 7
        LOCAL = 8
        SMART = 9
        PERFORMANCE_MAX = 10
        LOCAL_SERVICES = 11
        TRAVEL = 13
        DEMAND_GEN = 14


__all__ = tuple(sorted(__protobuf__.manifest))

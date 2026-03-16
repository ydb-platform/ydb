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
        "SeasonalityEventScopeEnum",
    },
)


class SeasonalityEventScopeEnum(proto.Message):
    r"""Message describing seasonality event scopes. The two types of
    seasonality events are BiddingSeasonalityAdjustments and
    BiddingDataExclusions.

    """

    class SeasonalityEventScope(proto.Enum):
        r"""The possible scopes of a Seasonality Event.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            CUSTOMER (2):
                The seasonality event is applied to all the
                customer's traffic for supported advertising
                channel types and device types. The CUSTOMER
                scope cannot be used in mutates.
            CAMPAIGN (4):
                The seasonality event is applied to all
                specified campaigns.
            CHANNEL (5):
                The seasonality event is applied to all
                campaigns that belong to specified channel
                types.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CUSTOMER = 2
        CAMPAIGN = 4
        CHANNEL = 5


__all__ = tuple(sorted(__protobuf__.manifest))

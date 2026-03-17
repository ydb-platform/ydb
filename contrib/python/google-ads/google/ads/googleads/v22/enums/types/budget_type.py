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
        "BudgetTypeEnum",
    },
)


class BudgetTypeEnum(proto.Message):
    r"""Describes Budget types."""

    class BudgetType(proto.Enum):
        r"""Possible Budget types.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            STANDARD (2):
                Budget type for standard Google Ads usage.
                Caps daily spend at two times the specified
                budget amount. Full details:
                https://support.google.com/google-ads/answer/6385083
            FIXED_CPA (4):
                Budget type with a fixed cost-per-acquisition (conversion).
                Full details:
                https://support.google.com/google-ads/answer/7528254

                This type is only supported by campaigns with
                AdvertisingChannelType.DISPLAY (excluding
                AdvertisingChannelSubType.DISPLAY_GMAIL),
                BiddingStrategyType.TARGET_CPA and PaymentMode.CONVERSIONS.
            SMART_CAMPAIGN (5):
                Budget type for Smart Campaign. Full details:
                https://support.google.com/google-ads/answer/7653509

                This type is only supported by campaigns with
                AdvertisingChannelType.SMART and
                AdvertisingChannelSubType.SMART_CAMPAIGN.
            LOCAL_SERVICES (6):
                Budget type for Local Services Campaign. Full details:
                https://support.google.com/localservices/answer/7434558

                This type is only supported by campaigns with
                AdvertisingChannelType.LOCAL_SERVICES.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        STANDARD = 2
        FIXED_CPA = 4
        SMART_CAMPAIGN = 5
        LOCAL_SERVICES = 6


__all__ = tuple(sorted(__protobuf__.manifest))

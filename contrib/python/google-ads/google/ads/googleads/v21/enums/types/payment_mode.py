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
        "PaymentModeEnum",
    },
)


class PaymentModeEnum(proto.Message):
    r"""Container for enum describing possible payment modes."""

    class PaymentMode(proto.Enum):
        r"""Enum describing possible payment modes.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CLICKS (4):
                Pay per interaction.
            CONVERSION_VALUE (5):
                Pay per conversion value. This mode is only
                supported by campaigns with
                AdvertisingChannelType.HOTEL,
                BiddingStrategyType.COMMISSION, and
                BudgetType.STANDARD.
            CONVERSIONS (6):
                Pay per conversion. This mode is only supported by campaigns
                with AdvertisingChannelType.DISPLAY (excluding
                AdvertisingChannelSubType.DISPLAY_GMAIL),
                BiddingStrategyType.TARGET_CPA, and BudgetType.FIXED_CPA.
                The customer must also be eligible for this mode. See
                Customer.eligibility_failure_reasons for details.
            GUEST_STAY (7):
                Pay per guest stay value. This mode is only
                supported by campaigns with
                AdvertisingChannelType.HOTEL,
                BiddingStrategyType.COMMISSION, and
                BudgetType.STANDARD.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CLICKS = 4
        CONVERSION_VALUE = 5
        CONVERSIONS = 6
        GUEST_STAY = 7


__all__ = tuple(sorted(__protobuf__.manifest))

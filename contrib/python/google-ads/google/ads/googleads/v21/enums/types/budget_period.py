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
        "BudgetPeriodEnum",
    },
)


class BudgetPeriodEnum(proto.Message):
    r"""Message describing Budget period."""

    class BudgetPeriod(proto.Enum):
        r"""Possible period of a Budget.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            DAILY (2):
                Daily budget.
            CUSTOM_PERIOD (5):
                Custom budget, added back in V5. Custom budget can be used
                with total_amount to specify lifetime budget limit. See:
                https://support.google.com/google-ads/answer/6385083 for
                more info.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DAILY = 2
        CUSTOM_PERIOD = 5


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "BudgetDeliveryMethodEnum",
    },
)


class BudgetDeliveryMethodEnum(proto.Message):
    r"""Message describing Budget delivery methods. A delivery method
    determines the rate at which the Budget is spent.

    """

    class BudgetDeliveryMethod(proto.Enum):
        r"""Possible delivery methods of a Budget.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            STANDARD (2):
                The budget server will throttle serving
                evenly across the entire time period.
            ACCELERATED (3):
                The budget server will not throttle serving,
                and ads will serve as fast as possible.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        STANDARD = 2
        ACCELERATED = 3


__all__ = tuple(sorted(__protobuf__.manifest))

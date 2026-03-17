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
        "ReachPlanConversionRateModelEnum",
    },
)


class ReachPlanConversionRateModelEnum(proto.Message):
    r"""Container for enum describing the type of model used to
    create a conversion rate suggestion for a supported ad product.

    """

    class ReachPlanConversionRateModel(proto.Enum):
        r"""Types of models used to create conversion rate suggestions.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            CUSTOMER_HISTORY (2):
                Suggested conversion rate for the
                authenticated customer based on the previous 70
                days.
            INVENTORY_AGGRESSIVE (3):
                Suggested conversion rate based on an
                aggressive rate for the entire inventory.
            INVENTORY_CONSERVATIVE (4):
                Suggested conversion rate based on a
                conservative rate for the entire inventory.
            INVENTORY_MEDIAN (5):
                Suggested conversion rate based on the median
                rate for the entire inventory.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CUSTOMER_HISTORY = 2
        INVENTORY_AGGRESSIVE = 3
        INVENTORY_CONSERVATIVE = 4
        INVENTORY_MEDIAN = 5


__all__ = tuple(sorted(__protobuf__.manifest))

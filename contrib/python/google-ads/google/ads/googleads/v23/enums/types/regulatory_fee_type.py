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
    package="google.ads.googleads.v23.enums",
    marshal="google.ads.googleads.v23",
    manifest={
        "RegulatoryFeeTypeEnum",
    },
)


class RegulatoryFeeTypeEnum(proto.Message):
    r"""Container for enum describing the type of regulatory fees."""

    class RegulatoryFeeType(proto.Enum):
        r"""The possible type of regulatory fees.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AUSTRIA_DST_FEE (2):
                Represents Austria DST fee.
            TURKIYE_REGULATORY_OPERATING_COST (3):
                Represents Türkiye regulatory operating cost.
            UK_DST_FEE (4):
                Represents UK DST fee.
            SPAIN_REGULATORY_OPERATING_COST (5):
                Represents Spain regulatory operating cost.
            FRANCE_REGULATORY_OPERATING_COST (6):
                Represents France regulatory operating cost.
            ITALY_REGULATORY_OPERATING_COST (7):
                Represents Italy regulatory operating cost.
            INDIA_REGULATORY_OPERATING_COST (8):
                Represents India regulatory operating cost.
            POLAND_REGULATORY_OPERATING_COST (9):
                Represents Poland regulatory operating cost.
            OPERATING_CHARGES (10):
                Represents operating charges.
            CANADA_DST_FEE (11):
                Represents Canada DST fee.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AUSTRIA_DST_FEE = 2
        TURKIYE_REGULATORY_OPERATING_COST = 3
        UK_DST_FEE = 4
        SPAIN_REGULATORY_OPERATING_COST = 5
        FRANCE_REGULATORY_OPERATING_COST = 6
        ITALY_REGULATORY_OPERATING_COST = 7
        INDIA_REGULATORY_OPERATING_COST = 8
        POLAND_REGULATORY_OPERATING_COST = 9
        OPERATING_CHARGES = 10
        CANADA_DST_FEE = 11


__all__ = tuple(sorted(__protobuf__.manifest))

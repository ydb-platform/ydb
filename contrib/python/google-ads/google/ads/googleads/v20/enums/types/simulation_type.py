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
        "SimulationTypeEnum",
    },
)


class SimulationTypeEnum(proto.Message):
    r"""Container for enum describing the field a simulation
    modifies.

    """

    class SimulationType(proto.Enum):
        r"""Enum describing the field a simulation modifies.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CPC_BID (2):
                The simulation is for a CPC bid.
            CPV_BID (3):
                The simulation is for a CPV bid.
            TARGET_CPA (4):
                The simulation is for a CPA target.
            BID_MODIFIER (5):
                The simulation is for a bid modifier.
            TARGET_ROAS (6):
                The simulation is for a ROAS target.
            PERCENT_CPC_BID (7):
                The simulation is for a percent CPC bid.
            TARGET_IMPRESSION_SHARE (8):
                The simulation is for an impression share
                target.
            BUDGET (9):
                The simulation is for a budget.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CPC_BID = 2
        CPV_BID = 3
        TARGET_CPA = 4
        BID_MODIFIER = 5
        TARGET_ROAS = 6
        PERCENT_CPC_BID = 7
        TARGET_IMPRESSION_SHARE = 8
        BUDGET = 9


__all__ = tuple(sorted(__protobuf__.manifest))

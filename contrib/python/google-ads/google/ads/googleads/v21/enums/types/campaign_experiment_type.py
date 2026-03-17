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
        "CampaignExperimentTypeEnum",
    },
)


class CampaignExperimentTypeEnum(proto.Message):
    r"""Container for enum describing campaign experiment type."""

    class CampaignExperimentType(proto.Enum):
        r"""Indicates if this campaign is a normal campaign,
        a draft campaign, or an experiment campaign.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            BASE (2):
                This is a regular campaign.
            DRAFT (3):
                This is a draft version of a campaign.
                It has some modifications from a base campaign,
                but it does not serve or accrue metrics.
            EXPERIMENT (4):
                This is an experiment version of a campaign.
                It has some modifications from a base campaign,
                and a percentage of traffic is being diverted
                from the BASE campaign to this experiment
                campaign.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BASE = 2
        DRAFT = 3
        EXPERIMENT = 4


__all__ = tuple(sorted(__protobuf__.manifest))

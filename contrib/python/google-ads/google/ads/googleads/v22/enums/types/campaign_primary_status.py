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
        "CampaignPrimaryStatusEnum",
    },
)


class CampaignPrimaryStatusEnum(proto.Message):
    r"""Container for enum describing possible campaign primary
    status.

    """

    class CampaignPrimaryStatus(proto.Enum):
        r"""Enum describing the possible campaign primary status.
        Provides insight into why a campaign is not serving or not
        serving optimally. Modification to the campaign and its related
        entities might take a while to be reflected in this status.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ELIGIBLE (2):
                The campaign is eligible to serve.
            PAUSED (3):
                The user-specified campaign status is paused.
            REMOVED (4):
                The user-specified campaign status is
                removed.
            ENDED (5):
                The user-specified time for this campaign to
                end has passed.
            PENDING (6):
                The campaign may serve in the future.
            MISCONFIGURED (7):
                The campaign or its associated entities have
                incorrect user-specified settings.
            LIMITED (8):
                The campaign or its associated entities are
                limited by user-specified settings.
            LEARNING (9):
                The automated bidding system is adjusting to
                user-specified changes to the campaign or
                associated entities.
            NOT_ELIGIBLE (10):
                The campaign is not eligible to serve.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ELIGIBLE = 2
        PAUSED = 3
        REMOVED = 4
        ENDED = 5
        PENDING = 6
        MISCONFIGURED = 7
        LIMITED = 8
        LEARNING = 9
        NOT_ELIGIBLE = 10


__all__ = tuple(sorted(__protobuf__.manifest))

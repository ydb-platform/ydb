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
        "UnitOfMeasureEnum",
    },
)


class UnitOfMeasureEnum(proto.Message):
    r"""Container for enum describing the type of unit of measure."""

    class UnitOfMeasure(proto.Enum):
        r"""The possible type of unit of measure.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CLICKS (2):
                Clicks as unit of measure.
            IMPRESSIONS (3):
                Impressions as unit of measure.
            ACQUISITIONS (4):
                Acquisitions as unit of measure.
            PHONE_CALLS (5):
                Phone calls as unit of measure.
            VIDEO_PLAYS (6):
                Video plays as unit of measure.
            DAYS (7):
                Days as unit of measure.
            AUDIO_PLAYS (8):
                Audio plays as unit of measure.
            ENGAGEMENTS (9):
                Engagements as unit of measure.
            SECONDS (10):
                Seconds as unit of measure.
            LEADS (11):
                Leads as unit of measure.
            GUEST_STAYS (12):
                Guest stays as unit of measure.
            HOURS (13):
                Hours as unit of measure.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CLICKS = 2
        IMPRESSIONS = 3
        ACQUISITIONS = 4
        PHONE_CALLS = 5
        VIDEO_PLAYS = 6
        DAYS = 7
        AUDIO_PLAYS = 8
        ENGAGEMENTS = 9
        SECONDS = 10
        LEADS = 11
        GUEST_STAYS = 12
        HOURS = 13


__all__ = tuple(sorted(__protobuf__.manifest))

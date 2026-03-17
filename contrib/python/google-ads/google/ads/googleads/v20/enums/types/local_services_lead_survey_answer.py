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
        "LocalServicesLeadSurveyAnswerEnum",
    },
)


class LocalServicesLeadSurveyAnswerEnum(proto.Message):
    r"""Container for enum describing possible survey answers for a
    lead.

    """

    class SurveyAnswer(proto.Enum):
        r"""Survey answer for Local Services Ads Lead.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            VERY_SATISFIED (2):
                Very satisfied with the lead.
            SATISFIED (3):
                Satisfied with the lead.
            NEUTRAL (4):
                Neutral with the lead.
            DISSATISFIED (5):
                Dissatisfied with the lead.
            VERY_DISSATISFIED (6):
                Very dissatisfied with the lead.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        VERY_SATISFIED = 2
        SATISFIED = 3
        NEUTRAL = 4
        DISSATISFIED = 5
        VERY_DISSATISFIED = 6


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "GoalConfigLevelEnum",
    },
)


class GoalConfigLevelEnum(proto.Message):
    r"""Container for enum describing possible goal config levels."""

    class GoalConfigLevel(proto.Enum):
        r"""The possible goal config levels. Campaigns automatically
        inherit the effective conversion account's customer goals unless
        they have been configured with their own set of campaign goals.

        Values:
            UNSPECIFIED (0):
                The goal config level has not been specified.
            UNKNOWN (1):
                The goal config level is not known in this
                version.
            CUSTOMER (2):
                The goal config is defined at the customer
                level.
            CAMPAIGN (3):
                The goal config is defined at the campaign
                level.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CUSTOMER = 2
        CAMPAIGN = 3


__all__ = tuple(sorted(__protobuf__.manifest))

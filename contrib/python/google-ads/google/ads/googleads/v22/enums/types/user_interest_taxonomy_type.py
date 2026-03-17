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
        "UserInterestTaxonomyTypeEnum",
    },
)


class UserInterestTaxonomyTypeEnum(proto.Message):
    r"""Message describing a UserInterestTaxonomyType."""

    class UserInterestTaxonomyType(proto.Enum):
        r"""Enum containing the possible UserInterestTaxonomyTypes.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AFFINITY (2):
                The affinity for this user interest.
            IN_MARKET (3):
                The market for this user interest.
            MOBILE_APP_INSTALL_USER (4):
                Users known to have installed applications in
                the specified categories.
            VERTICAL_GEO (5):
                The geographical location of the
                interest-based vertical.
            NEW_SMART_PHONE_USER (6):
                User interest criteria for new smart phone
                users.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AFFINITY = 2
        IN_MARKET = 3
        MOBILE_APP_INSTALL_USER = 4
        VERTICAL_GEO = 5
        NEW_SMART_PHONE_USER = 6


__all__ = tuple(sorted(__protobuf__.manifest))

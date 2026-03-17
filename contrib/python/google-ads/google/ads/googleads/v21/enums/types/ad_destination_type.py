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
        "AdDestinationTypeEnum",
    },
)


class AdDestinationTypeEnum(proto.Message):
    r"""Container for enumeration of Google Ads destination types."""

    class AdDestinationType(proto.Enum):
        r"""Enumerates Google Ads destination types

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            NOT_APPLICABLE (2):
                Ads that don't intend to drive users off from
                ads to other destinations
            WEBSITE (3):
                Website
            APP_DEEP_LINK (4):
                App Deep Link
            APP_STORE (5):
                iOS App Store or Play Store
            PHONE_CALL (6):
                Call Dialer
            MAP_DIRECTIONS (7):
                Map App
            LOCATION_LISTING (8):
                Location Dedicated Page
            MESSAGE (9):
                Text Message
            LEAD_FORM (10):
                Lead Generation Form
            YOUTUBE (11):
                YouTube
            UNMODELED_FOR_CONVERSIONS (12):
                Ad Destination for Conversions with keys
                unknown
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NOT_APPLICABLE = 2
        WEBSITE = 3
        APP_DEEP_LINK = 4
        APP_STORE = 5
        PHONE_CALL = 6
        MAP_DIRECTIONS = 7
        LOCATION_LISTING = 8
        MESSAGE = 9
        LEAD_FORM = 10
        YOUTUBE = 11
        UNMODELED_FOR_CONVERSIONS = 12


__all__ = tuple(sorted(__protobuf__.manifest))

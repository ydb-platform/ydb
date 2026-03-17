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
        "BookingStatusEnum",
    },
)


class BookingStatusEnum(proto.Message):
    r"""Container for enum with booking status."""

    class BookingStatus(proto.Enum):
        r"""Booking status.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            BOOKED (2):
                The booking is active and holds inventory for
                the campaign.
            HELD (3):
                The campaign is holding inventory, but the
                booking is not confirmed.
            CAMPAIGN_ENDED (4):
                The campaign has ended and is no longer
                holding inventory.
            HOLD_EXPIRED (5):
                The hold on the inventory has expired.
            BOOKING_CANCELLED (6):
                The campaign was booked, but was in a
                non-servable state for too long and the booking
                was cancelled by the system.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BOOKED = 2
        HELD = 3
        CAMPAIGN_ENDED = 4
        HOLD_EXPIRED = 5
        BOOKING_CANCELLED = 6


__all__ = tuple(sorted(__protobuf__.manifest))

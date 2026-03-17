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
        "HotelReconciliationStatusEnum",
    },
)


class HotelReconciliationStatusEnum(proto.Message):
    r"""Container for HotelReconciliationStatus."""

    class HotelReconciliationStatus(proto.Enum):
        r"""Status of the hotel booking reconciliation.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            RESERVATION_ENABLED (2):
                Bookings are for a future date, or a stay is
                underway but the check-out date hasn't passed.
                An active reservation can't be reconciled.
            RECONCILIATION_NEEDED (3):
                Check-out has already taken place, or the
                booked dates have passed without cancellation.
                Bookings that are not reconciled within 45 days
                of the check-out date are billed based on the
                original booking price.
            RECONCILED (4):
                These bookings have been reconciled.
                Reconciled bookings are billed 45 days after the
                check-out date.
            CANCELED (5):
                This booking was marked as canceled. Canceled
                stays with a value greater than zero (due to
                minimum stay rules or cancellation fees) are
                billed 45 days after the check-out date.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        RESERVATION_ENABLED = 2
        RECONCILIATION_NEEDED = 3
        RECONCILED = 4
        CANCELED = 5


__all__ = tuple(sorted(__protobuf__.manifest))

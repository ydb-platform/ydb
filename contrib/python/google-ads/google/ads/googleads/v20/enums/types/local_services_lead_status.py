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
        "LocalServicesLeadStatusEnum",
    },
)


class LocalServicesLeadStatusEnum(proto.Message):
    r"""Container for enum describing possible statuses of lead."""

    class LeadStatus(proto.Enum):
        r"""Possible statuses of lead.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            NEW (2):
                New lead which hasn't yet been seen by
                advertiser.
            ACTIVE (3):
                Lead that thas been interacted by advertiser.
            BOOKED (4):
                Lead has been booked.
            DECLINED (5):
                Lead was declined by advertiser.
            EXPIRED (6):
                Lead has expired due to inactivity.
            DISABLED (7):
                Disabled due to spam or dispute.
            CONSUMER_DECLINED (8):
                Consumer declined the lead.
            WIPED_OUT (9):
                Personally Identifiable Information of the
                lead is wiped out.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NEW = 2
        ACTIVE = 3
        BOOKED = 4
        DECLINED = 5
        EXPIRED = 6
        DISABLED = 7
        CONSUMER_DECLINED = 8
        WIPED_OUT = 9


__all__ = tuple(sorted(__protobuf__.manifest))

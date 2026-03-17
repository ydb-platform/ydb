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
        "LocalServicesLeadSurveySatisfiedReasonEnum",
    },
)


class LocalServicesLeadSurveySatisfiedReasonEnum(proto.Message):
    r"""Container for enum describing possible survey satisfied
    reasons for a lead.

    """

    class SurveySatisfiedReason(proto.Enum):
        r"""Provider's reason for being satisfied with the lead.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            OTHER_SATISFIED_REASON (2):
                Other reasons.
            BOOKED_CUSTOMER (3):
                Lead converted into a booked customer or
                client.
            LIKELY_BOOKED_CUSTOMER (4):
                Lead could convert into a booked customer or
                client soon.
            SERVICE_RELATED (5):
                Lead was related to the services the business
                offers.
            HIGH_VALUE_SERVICE (6):
                Lead was for a service that generates high
                value for the business.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        OTHER_SATISFIED_REASON = 2
        BOOKED_CUSTOMER = 3
        LIKELY_BOOKED_CUSTOMER = 4
        SERVICE_RELATED = 5
        HIGH_VALUE_SERVICE = 6


__all__ = tuple(sorted(__protobuf__.manifest))

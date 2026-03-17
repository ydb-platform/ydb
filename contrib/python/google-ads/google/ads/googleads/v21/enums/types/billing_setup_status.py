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
        "BillingSetupStatusEnum",
    },
)


class BillingSetupStatusEnum(proto.Message):
    r"""Message describing BillingSetup statuses."""

    class BillingSetupStatus(proto.Enum):
        r"""The possible statuses of a BillingSetup.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PENDING (2):
                The billing setup is pending approval.
            APPROVED_HELD (3):
                The billing setup has been approved but the
                corresponding first budget has not.  This can
                only occur for billing setups configured for
                monthly invoicing.
            APPROVED (4):
                The billing setup has been approved.
            CANCELLED (5):
                The billing setup was cancelled by the user
                prior to approval.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PENDING = 2
        APPROVED_HELD = 3
        APPROVED = 4
        CANCELLED = 5


__all__ = tuple(sorted(__protobuf__.manifest))

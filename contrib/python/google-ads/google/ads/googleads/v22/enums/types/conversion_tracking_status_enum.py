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
        "ConversionTrackingStatusEnum",
    },
)


class ConversionTrackingStatusEnum(proto.Message):
    r"""Container for enum representing the conversion tracking
    status of the customer.

    """

    class ConversionTrackingStatus(proto.Enum):
        r"""Conversion Tracking status of the customer.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            NOT_CONVERSION_TRACKED (2):
                Customer does not use any conversion
                tracking.
            CONVERSION_TRACKING_MANAGED_BY_SELF (3):
                The conversion actions are created and
                managed by this customer.
            CONVERSION_TRACKING_MANAGED_BY_THIS_MANAGER (4):
                The conversion actions are created and managed by the
                manager specified in the request's ``login-customer-id``.
            CONVERSION_TRACKING_MANAGED_BY_ANOTHER_MANAGER (5):
                The conversion actions are created and managed by a manager
                different from the customer or manager specified in the
                request's ``login-customer-id``.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NOT_CONVERSION_TRACKED = 2
        CONVERSION_TRACKING_MANAGED_BY_SELF = 3
        CONVERSION_TRACKING_MANAGED_BY_THIS_MANAGER = 4
        CONVERSION_TRACKING_MANAGED_BY_ANOTHER_MANAGER = 5


__all__ = tuple(sorted(__protobuf__.manifest))

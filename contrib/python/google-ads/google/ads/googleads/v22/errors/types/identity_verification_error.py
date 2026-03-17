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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "IdentityVerificationErrorEnum",
    },
)


class IdentityVerificationErrorEnum(proto.Message):
    r"""Container for enum describing possible identity verification
    errors.

    """

    class IdentityVerificationError(proto.Enum):
        r"""Enum describing possible identity verification errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            NO_EFFECTIVE_BILLING (2):
                No effective billing linked to this customer.
            BILLING_NOT_ON_MONTHLY_INVOICING (3):
                Customer is not on monthly invoicing.
            VERIFICATION_ALREADY_STARTED (4):
                Verification for this program type was
                already started.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NO_EFFECTIVE_BILLING = 2
        BILLING_NOT_ON_MONTHLY_INVOICING = 3
        VERIFICATION_ALREADY_STARTED = 4


__all__ = tuple(sorted(__protobuf__.manifest))

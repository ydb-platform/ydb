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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "CustomerErrorEnum",
    },
)


class CustomerErrorEnum(proto.Message):
    r"""Container for enum describing possible customer errors."""

    class CustomerError(proto.Enum):
        r"""Set of errors that are related to requests dealing with
        Customer.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            STATUS_CHANGE_DISALLOWED (2):
                Customer status is not allowed to be changed
                from DRAFT and CLOSED. Currency code and at
                least one of country code and time zone needs to
                be set when status is changed to ENABLED.
            ACCOUNT_NOT_SET_UP (3):
                CustomerService cannot get a customer that
                has not been fully set up.
            CREATION_DENIED_FOR_POLICY_VIOLATION (4):
                Customer creation is denied for policy
                violation.
            CREATION_DENIED_INELIGIBLE_MCC (5):
                Manager account is ineligible to create new
                accounts.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        STATUS_CHANGE_DISALLOWED = 2
        ACCOUNT_NOT_SET_UP = 3
        CREATION_DENIED_FOR_POLICY_VIOLATION = 4
        CREATION_DENIED_INELIGIBLE_MCC = 5


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "UserDataErrorEnum",
    },
)


class UserDataErrorEnum(proto.Message):
    r"""Container for enum describing possible user data errors."""

    class UserDataError(proto.Enum):
        r"""Enum describing possible request errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            OPERATIONS_FOR_CUSTOMER_MATCH_NOT_ALLOWED (2):
                Customer is not allowed to perform operations
                related to Customer Match.
            TOO_MANY_USER_IDENTIFIERS (3):
                Maximum number of user identifiers allowed
                for each request is 100 and for each operation
                is 20.
            USER_LIST_NOT_APPLICABLE (4):
                Current user list is not applicable for the
                given customer.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        OPERATIONS_FOR_CUSTOMER_MATCH_NOT_ALLOWED = 2
        TOO_MANY_USER_IDENTIFIERS = 3
        USER_LIST_NOT_APPLICABLE = 4


__all__ = tuple(sorted(__protobuf__.manifest))

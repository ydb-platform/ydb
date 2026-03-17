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
        "CustomerUserAccessErrorEnum",
    },
)


class CustomerUserAccessErrorEnum(proto.Message):
    r"""Container for enum describing possible CustomerUserAccess
    errors.

    """

    class CustomerUserAccessError(proto.Enum):
        r"""Enum describing possible customer user access errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_USER_ID (2):
                There is no user associated with the user id
                specified.
            REMOVAL_DISALLOWED (3):
                Unable to remove the access between the user
                and customer.
            DISALLOWED_ACCESS_ROLE (4):
                Unable to add or update the access role as
                specified.
            LAST_ADMIN_USER_OF_SERVING_CUSTOMER (5):
                The user can't remove itself from an active
                serving customer if it's the last admin user and
                the customer doesn't have any owner manager
            LAST_ADMIN_USER_OF_MANAGER (6):
                Last admin user cannot be removed from a
                manager.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_USER_ID = 2
        REMOVAL_DISALLOWED = 3
        DISALLOWED_ACCESS_ROLE = 4
        LAST_ADMIN_USER_OF_SERVING_CUSTOMER = 5
        LAST_ADMIN_USER_OF_MANAGER = 6


__all__ = tuple(sorted(__protobuf__.manifest))

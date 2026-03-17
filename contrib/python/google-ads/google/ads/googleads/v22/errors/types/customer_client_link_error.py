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
        "CustomerClientLinkErrorEnum",
    },
)


class CustomerClientLinkErrorEnum(proto.Message):
    r"""Container for enum describing possible CustomeClientLink
    errors.

    """

    class CustomerClientLinkError(proto.Enum):
        r"""Enum describing possible CustomerClientLink errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CLIENT_ALREADY_INVITED_BY_THIS_MANAGER (2):
                Trying to manage a client that already in
                being managed by customer.
            CLIENT_ALREADY_MANAGED_IN_HIERARCHY (3):
                Already managed by some other manager in the
                hierarchy.
            CYCLIC_LINK_NOT_ALLOWED (4):
                Attempt to create a cycle in the hierarchy.
            CUSTOMER_HAS_TOO_MANY_ACCOUNTS (5):
                Managed accounts has the maximum number of
                linked accounts.
            CLIENT_HAS_TOO_MANY_INVITATIONS (6):
                Invitor has the maximum pending invitations.
            CANNOT_HIDE_OR_UNHIDE_MANAGER_ACCOUNTS (7):
                Attempt to change hidden status of a link
                that is not active.
            CUSTOMER_HAS_TOO_MANY_ACCOUNTS_AT_MANAGER (8):
                Parent manager account has the maximum number
                of linked accounts.
            CLIENT_HAS_TOO_MANY_MANAGERS (9):
                Client has too many managers.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CLIENT_ALREADY_INVITED_BY_THIS_MANAGER = 2
        CLIENT_ALREADY_MANAGED_IN_HIERARCHY = 3
        CYCLIC_LINK_NOT_ALLOWED = 4
        CUSTOMER_HAS_TOO_MANY_ACCOUNTS = 5
        CLIENT_HAS_TOO_MANY_INVITATIONS = 6
        CANNOT_HIDE_OR_UNHIDE_MANAGER_ACCOUNTS = 7
        CUSTOMER_HAS_TOO_MANY_ACCOUNTS_AT_MANAGER = 8
        CLIENT_HAS_TOO_MANY_MANAGERS = 9


__all__ = tuple(sorted(__protobuf__.manifest))

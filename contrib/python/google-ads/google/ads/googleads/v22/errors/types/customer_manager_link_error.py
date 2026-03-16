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
        "CustomerManagerLinkErrorEnum",
    },
)


class CustomerManagerLinkErrorEnum(proto.Message):
    r"""Container for enum describing possible CustomerManagerLink
    errors.

    """

    class CustomerManagerLinkError(proto.Enum):
        r"""Enum describing possible CustomerManagerLink errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            NO_PENDING_INVITE (2):
                No pending invitation.
            SAME_CLIENT_MORE_THAN_ONCE_PER_CALL (3):
                Attempt to operate on the same client more
                than once in the same call.
            MANAGER_HAS_MAX_NUMBER_OF_LINKED_ACCOUNTS (4):
                Manager account has the maximum number of
                linked accounts.
            CANNOT_UNLINK_ACCOUNT_WITHOUT_ACTIVE_USER (5):
                If no active user on account it cannot be
                unlinked from its manager.
            CANNOT_REMOVE_LAST_CLIENT_ACCOUNT_OWNER (6):
                Account should have at least one active owner
                on it before being unlinked.
            CANNOT_CHANGE_ROLE_BY_NON_ACCOUNT_OWNER (7):
                Only account owners may change their
                permission role.
            CANNOT_CHANGE_ROLE_FOR_NON_ACTIVE_LINK_ACCOUNT (8):
                When a client's link to its manager is not
                active, the link role cannot be changed.
            DUPLICATE_CHILD_FOUND (9):
                Attempt to link a child to a parent that
                contains or will contain duplicate children.
            TEST_ACCOUNT_LINKS_TOO_MANY_CHILD_ACCOUNTS (10):
                The authorized customer is a test account. It
                can add no more than the allowed number of
                accounts
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NO_PENDING_INVITE = 2
        SAME_CLIENT_MORE_THAN_ONCE_PER_CALL = 3
        MANAGER_HAS_MAX_NUMBER_OF_LINKED_ACCOUNTS = 4
        CANNOT_UNLINK_ACCOUNT_WITHOUT_ACTIVE_USER = 5
        CANNOT_REMOVE_LAST_CLIENT_ACCOUNT_OWNER = 6
        CANNOT_CHANGE_ROLE_BY_NON_ACCOUNT_OWNER = 7
        CANNOT_CHANGE_ROLE_FOR_NON_ACTIVE_LINK_ACCOUNT = 8
        DUPLICATE_CHILD_FOUND = 9
        TEST_ACCOUNT_LINKS_TOO_MANY_CHILD_ACCOUNTS = 10


__all__ = tuple(sorted(__protobuf__.manifest))

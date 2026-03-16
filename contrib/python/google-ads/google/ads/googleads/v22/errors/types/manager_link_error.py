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
        "ManagerLinkErrorEnum",
    },
)


class ManagerLinkErrorEnum(proto.Message):
    r"""Container for enum describing possible ManagerLink errors."""

    class ManagerLinkError(proto.Enum):
        r"""Enum describing possible ManagerLink errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            ACCOUNTS_NOT_COMPATIBLE_FOR_LINKING (2):
                The manager and client have incompatible
                account types.
            TOO_MANY_MANAGERS (3):
                Client is already linked to too many
                managers.
            TOO_MANY_INVITES (4):
                Manager has too many pending invitations.
            ALREADY_INVITED_BY_THIS_MANAGER (5):
                Client is already invited by this manager.
            ALREADY_MANAGED_BY_THIS_MANAGER (6):
                The client is already managed by this
                manager.
            ALREADY_MANAGED_IN_HIERARCHY (7):
                Client is already managed in hierarchy.
            DUPLICATE_CHILD_FOUND (8):
                Manager and sub-manager to be linked have
                duplicate client.
            CLIENT_HAS_NO_ADMIN_USER (9):
                Client has no active user that can access the
                client account.
            MAX_DEPTH_EXCEEDED (10):
                Adding this link would exceed the maximum
                hierarchy depth.
            CYCLE_NOT_ALLOWED (11):
                Adding this link will create a cycle.
            TOO_MANY_ACCOUNTS (12):
                Manager account has the maximum number of
                linked clients.
            TOO_MANY_ACCOUNTS_AT_MANAGER (13):
                Parent manager account has the maximum number
                of linked clients.
            NON_OWNER_USER_CANNOT_MODIFY_LINK (14):
                The account is not authorized owner.
            SUSPENDED_ACCOUNT_CANNOT_ADD_CLIENTS (15):
                Your manager account is suspended, and you
                are no longer allowed to link to clients.
            CLIENT_OUTSIDE_TREE (16):
                You are not allowed to move a client to a
                manager that is not under your current
                hierarchy.
            INVALID_STATUS_CHANGE (17):
                The changed status for mutate link is
                invalid.
            INVALID_CHANGE (18):
                The change for mutate link is invalid.
            CUSTOMER_CANNOT_MANAGE_SELF (19):
                You are not allowed to link a manager account
                to itself.
            CREATING_ENABLED_LINK_NOT_ALLOWED (20):
                The link was created with status ACTIVE and
                not PENDING.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ACCOUNTS_NOT_COMPATIBLE_FOR_LINKING = 2
        TOO_MANY_MANAGERS = 3
        TOO_MANY_INVITES = 4
        ALREADY_INVITED_BY_THIS_MANAGER = 5
        ALREADY_MANAGED_BY_THIS_MANAGER = 6
        ALREADY_MANAGED_IN_HIERARCHY = 7
        DUPLICATE_CHILD_FOUND = 8
        CLIENT_HAS_NO_ADMIN_USER = 9
        MAX_DEPTH_EXCEEDED = 10
        CYCLE_NOT_ALLOWED = 11
        TOO_MANY_ACCOUNTS = 12
        TOO_MANY_ACCOUNTS_AT_MANAGER = 13
        NON_OWNER_USER_CANNOT_MODIFY_LINK = 14
        SUSPENDED_ACCOUNT_CANNOT_ADD_CLIENTS = 15
        CLIENT_OUTSIDE_TREE = 16
        INVALID_STATUS_CHANGE = 17
        INVALID_CHANGE = 18
        CUSTOMER_CANNOT_MANAGE_SELF = 19
        CREATING_ENABLED_LINK_NOT_ALLOWED = 20


__all__ = tuple(sorted(__protobuf__.manifest))

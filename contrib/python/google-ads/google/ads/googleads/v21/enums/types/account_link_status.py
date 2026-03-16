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
        "AccountLinkStatusEnum",
    },
)


class AccountLinkStatusEnum(proto.Message):
    r"""Container for enum describing possible statuses of an account
    link.

    """

    class AccountLinkStatus(proto.Enum):
        r"""Describes the possible statuses for a link between a Google
        Ads customer and another account.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ENABLED (2):
                The link is enabled.
            REMOVED (3):
                The link is removed/disabled.
            REQUESTED (4):
                The link to the other account has been
                requested. A user on the other account may now
                approve the link by setting the status to
                ENABLED.
            PENDING_APPROVAL (5):
                This link has been requested by a user on the
                other account. It may be approved by a user on
                this account by setting the status to ENABLED.
            REJECTED (6):
                The link is rejected by the approver.
            REVOKED (7):
                The link is revoked by the user who requested
                the link.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ENABLED = 2
        REMOVED = 3
        REQUESTED = 4
        PENDING_APPROVAL = 5
        REJECTED = 6
        REVOKED = 7


__all__ = tuple(sorted(__protobuf__.manifest))

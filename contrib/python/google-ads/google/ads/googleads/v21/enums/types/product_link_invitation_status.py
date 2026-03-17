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
        "ProductLinkInvitationStatusEnum",
    },
)


class ProductLinkInvitationStatusEnum(proto.Message):
    r"""Container for enum describing possible statuses of a product
    link invitation.

    """

    class ProductLinkInvitationStatus(proto.Enum):
        r"""Describes the possible statuses for an invitation between a
        Google Ads customer and another account.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ACCEPTED (2):
                The invitation is accepted.
            REQUESTED (3):
                An invitation has been sent to the other
                account. A user on the other account may now
                accept the invitation by setting the status to
                ACCEPTED.
            PENDING_APPROVAL (4):
                This invitation has been sent by a user on
                the other account. It may be accepted by a user
                on this account by setting the status to
                ACCEPTED.
            REVOKED (5):
                The invitation is revoked by the user who
                sent the invitation.
            REJECTED (6):
                The invitation has been rejected by the
                invitee.
            EXPIRED (7):
                The invitation has timed out before being
                accepted by the invitee.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ACCEPTED = 2
        REQUESTED = 3
        PENDING_APPROVAL = 4
        REVOKED = 5
        REJECTED = 6
        EXPIRED = 7


__all__ = tuple(sorted(__protobuf__.manifest))

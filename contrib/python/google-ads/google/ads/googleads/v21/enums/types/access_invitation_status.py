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
        "AccessInvitationStatusEnum",
    },
)


class AccessInvitationStatusEnum(proto.Message):
    r"""Container for enum for identifying the status of access
    invitation

    """

    class AccessInvitationStatus(proto.Enum):
        r"""Possible access invitation status of a user

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PENDING (2):
                The initial state of an invitation, before
                being acted upon by anyone.
            DECLINED (3):
                Invitation process was terminated by the
                email recipient. No new user was created.
            EXPIRED (4):
                Invitation URLs expired without being acted
                upon. No new user can be created.  Invitations
                expire 20 days after creation.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PENDING = 2
        DECLINED = 3
        EXPIRED = 4


__all__ = tuple(sorted(__protobuf__.manifest))

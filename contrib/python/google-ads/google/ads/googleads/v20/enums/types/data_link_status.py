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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "DataLinkStatusEnum",
    },
)


class DataLinkStatusEnum(proto.Message):
    r"""Container for enum describing different types of data links."""

    class DataLinkStatus(proto.Enum):
        r"""Describes the possible data link statuses.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            REQUESTED (2):
                Link has been requested by one party, but not
                confirmed by the other party.
            PENDING_APPROVAL (3):
                Link is waiting for the customer to approve.
            ENABLED (4):
                Link is established and can be used as
                needed.
            DISABLED (5):
                Link is no longer valid and should be
                ignored.
            REVOKED (6):
                Link request has been cancelled by the
                requester and further cleanup may be needed.
            REJECTED (7):
                Link request has been rejected by the
                approver.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        REQUESTED = 2
        PENDING_APPROVAL = 3
        ENABLED = 4
        DISABLED = 5
        REVOKED = 6
        REJECTED = 7


__all__ = tuple(sorted(__protobuf__.manifest))

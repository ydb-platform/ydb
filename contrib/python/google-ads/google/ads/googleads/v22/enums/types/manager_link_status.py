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
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "ManagerLinkStatusEnum",
    },
)


class ManagerLinkStatusEnum(proto.Message):
    r"""Container for enum describing possible status of a manager
    and client link.

    """

    class ManagerLinkStatus(proto.Enum):
        r"""Possible statuses of a link.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ACTIVE (2):
                Indicates current in-effect relationship
            INACTIVE (3):
                Indicates terminated relationship
            PENDING (4):
                Indicates relationship has been requested by
                manager, but the client hasn't accepted yet.
            REFUSED (5):
                Relationship was requested by the manager,
                but the client has refused.
            CANCELED (6):
                Indicates relationship has been requested by
                manager, but manager canceled it.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ACTIVE = 2
        INACTIVE = 3
        PENDING = 4
        REFUSED = 5
        CANCELED = 6


__all__ = tuple(sorted(__protobuf__.manifest))

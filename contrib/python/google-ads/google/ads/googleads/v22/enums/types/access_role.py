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
        "AccessRoleEnum",
    },
)


class AccessRoleEnum(proto.Message):
    r"""Container for enum describing possible access role for user."""

    class AccessRole(proto.Enum):
        r"""Possible access role of a user.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ADMIN (2):
                Owns its account and can control the addition
                of other users.
            STANDARD (3):
                Can modify campaigns, but can't affect other
                users.
            READ_ONLY (4):
                Can view campaigns and account changes, but
                cannot make edits.
            EMAIL_ONLY (5):
                Role for \"email only\" access. Represents an
                email recipient rather than a true User entity.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ADMIN = 2
        STANDARD = 3
        READ_ONLY = 4
        EMAIL_ONLY = 5


__all__ = tuple(sorted(__protobuf__.manifest))

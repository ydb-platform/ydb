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
        "CustomAudienceErrorEnum",
    },
)


class CustomAudienceErrorEnum(proto.Message):
    r"""Container for enum describing possible custom audience
    errors.

    """

    class CustomAudienceError(proto.Enum):
        r"""Enum describing possible custom audience errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            NAME_ALREADY_USED (2):
                New name in the custom audience is duplicated
                ignoring cases.
            CANNOT_REMOVE_WHILE_IN_USE (3):
                Cannot remove a custom audience while it's
                still being used as targeting.
            RESOURCE_ALREADY_REMOVED (4):
                Cannot update or remove a custom audience
                that is already removed.
            MEMBER_TYPE_AND_PARAMETER_ALREADY_EXISTED (5):
                The pair of [type, value] already exists in members.
            INVALID_MEMBER_TYPE (6):
                Member type is invalid.
            MEMBER_TYPE_AND_VALUE_DOES_NOT_MATCH (7):
                Member type does not have associated value.
            POLICY_VIOLATION (8):
                Custom audience contains a member that
                violates policy.
            INVALID_TYPE_CHANGE (9):
                Change in custom audience type is not
                allowed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NAME_ALREADY_USED = 2
        CANNOT_REMOVE_WHILE_IN_USE = 3
        RESOURCE_ALREADY_REMOVED = 4
        MEMBER_TYPE_AND_PARAMETER_ALREADY_EXISTED = 5
        INVALID_MEMBER_TYPE = 6
        MEMBER_TYPE_AND_VALUE_DOES_NOT_MATCH = 7
        POLICY_VIOLATION = 8
        INVALID_TYPE_CHANGE = 9


__all__ = tuple(sorted(__protobuf__.manifest))

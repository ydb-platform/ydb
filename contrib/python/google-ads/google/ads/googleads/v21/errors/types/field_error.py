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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "FieldErrorEnum",
    },
)


class FieldErrorEnum(proto.Message):
    r"""Container for enum describing possible field errors."""

    class FieldError(proto.Enum):
        r"""Enum describing possible field errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            REQUIRED (2):
                The required field was not present.
            IMMUTABLE_FIELD (3):
                The field attempted to be mutated is
                immutable.
            INVALID_VALUE (4):
                The field's value is invalid.
            VALUE_MUST_BE_UNSET (5):
                The field cannot be set.
            REQUIRED_NONEMPTY_LIST (6):
                The required repeated field was empty.
            FIELD_CANNOT_BE_CLEARED (7):
                The field cannot be cleared.
            BLOCKED_VALUE (9):
                The field's value is on a deny-list for this
                field.
            FIELD_CAN_ONLY_BE_CLEARED (10):
                The field's value cannot be modified, except
                for clearing.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        REQUIRED = 2
        IMMUTABLE_FIELD = 3
        INVALID_VALUE = 4
        VALUE_MUST_BE_UNSET = 5
        REQUIRED_NONEMPTY_LIST = 6
        FIELD_CANNOT_BE_CLEARED = 7
        BLOCKED_VALUE = 9
        FIELD_CAN_ONLY_BE_CLEARED = 10


__all__ = tuple(sorted(__protobuf__.manifest))

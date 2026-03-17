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
        "FieldMaskErrorEnum",
    },
)


class FieldMaskErrorEnum(proto.Message):
    r"""Container for enum describing possible field mask errors."""

    class FieldMaskError(proto.Enum):
        r"""Enum describing possible field mask errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            FIELD_MASK_MISSING (5):
                The field mask must be provided for update
                operations.
            FIELD_MASK_NOT_ALLOWED (4):
                The field mask must be empty for create and
                remove operations.
            FIELD_NOT_FOUND (2):
                The field mask contained an invalid field.
            FIELD_HAS_SUBFIELDS (3):
                The field mask updated a field with
                subfields. Fields with subfields may be cleared,
                but not updated. To fix this, the field mask
                should select all the subfields of the invalid
                field.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        FIELD_MASK_MISSING = 5
        FIELD_MASK_NOT_ALLOWED = 4
        FIELD_NOT_FOUND = 2
        FIELD_HAS_SUBFIELDS = 3


__all__ = tuple(sorted(__protobuf__.manifest))

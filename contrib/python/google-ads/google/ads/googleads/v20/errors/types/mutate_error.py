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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "MutateErrorEnum",
    },
)


class MutateErrorEnum(proto.Message):
    r"""Container for enum describing possible mutate errors."""

    class MutateError(proto.Enum):
        r"""Enum describing possible mutate errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            RESOURCE_NOT_FOUND (3):
                Requested resource was not found.
            ID_EXISTS_IN_MULTIPLE_MUTATES (7):
                Cannot mutate the same resource twice in one
                request.
            INCONSISTENT_FIELD_VALUES (8):
                The field's contents don't match another
                field that represents the same data.
            MUTATE_NOT_ALLOWED (9):
                Mutates are not allowed for the requested
                resource.
            RESOURCE_NOT_IN_GOOGLE_ADS (10):
                The resource isn't in Google Ads. It belongs
                to another ads system.
            RESOURCE_ALREADY_EXISTS (11):
                The resource being created already exists.
            RESOURCE_DOES_NOT_SUPPORT_VALIDATE_ONLY (12):
                This resource cannot be used with "validate_only".
            OPERATION_DOES_NOT_SUPPORT_PARTIAL_FAILURE (16):
                This operation cannot be used with "partial_failure".
            RESOURCE_READ_ONLY (13):
                Attempt to write to read-only fields.
            EU_POLITICAL_ADVERTISING_DECLARATION_REQUIRED (17):
                Mutates are generally not allowed if the
                customer contains non-exempt campaigns without
                the EU political advertising declaration.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        RESOURCE_NOT_FOUND = 3
        ID_EXISTS_IN_MULTIPLE_MUTATES = 7
        INCONSISTENT_FIELD_VALUES = 8
        MUTATE_NOT_ALLOWED = 9
        RESOURCE_NOT_IN_GOOGLE_ADS = 10
        RESOURCE_ALREADY_EXISTS = 11
        RESOURCE_DOES_NOT_SUPPORT_VALIDATE_ONLY = 12
        OPERATION_DOES_NOT_SUPPORT_PARTIAL_FAILURE = 16
        RESOURCE_READ_ONLY = 13
        EU_POLITICAL_ADVERTISING_DECLARATION_REQUIRED = 17


__all__ = tuple(sorted(__protobuf__.manifest))

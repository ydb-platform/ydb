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
        "SharedSetErrorEnum",
    },
)


class SharedSetErrorEnum(proto.Message):
    r"""Container for enum describing possible shared set errors."""

    class SharedSetError(proto.Enum):
        r"""Enum describing possible shared set errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CUSTOMER_CANNOT_CREATE_SHARED_SET_OF_THIS_TYPE (2):
                The customer cannot create this type of
                shared set.
            DUPLICATE_NAME (3):
                A shared set with this name already exists.
            SHARED_SET_REMOVED (4):
                Removed shared sets cannot be mutated.
            SHARED_SET_IN_USE (5):
                The shared set cannot be removed because it
                is in use.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CUSTOMER_CANNOT_CREATE_SHARED_SET_OF_THIS_TYPE = 2
        DUPLICATE_NAME = 3
        SHARED_SET_REMOVED = 4
        SHARED_SET_IN_USE = 5


__all__ = tuple(sorted(__protobuf__.manifest))

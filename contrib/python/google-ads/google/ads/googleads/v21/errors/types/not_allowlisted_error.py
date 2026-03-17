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
        "NotAllowlistedErrorEnum",
    },
)


class NotAllowlistedErrorEnum(proto.Message):
    r"""Container for enum describing possible not allowlisted
    errors.

    """

    class NotAllowlistedError(proto.Enum):
        r"""Enum describing possible not allowlisted errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE (2):
                Customer is not allowlisted for accessing
                this feature.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE = 2


__all__ = tuple(sorted(__protobuf__.manifest))

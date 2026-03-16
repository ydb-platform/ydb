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
        "QuotaErrorEnum",
    },
)


class QuotaErrorEnum(proto.Message):
    r"""Container for enum describing possible quota errors."""

    class QuotaError(proto.Enum):
        r"""Enum describing possible quota errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            RESOURCE_EXHAUSTED (2):
                Too many requests.
            ACCESS_PROHIBITED (3):
                Access is prohibited.
            RESOURCE_TEMPORARILY_EXHAUSTED (4):
                Too many requests in a short amount of time.
            EXCESSIVE_SHORT_TERM_QUERY_RESOURCE_CONSUMPTION (5):
                Too many expensive requests from query
                pattern over a short amount of time.
            EXCESSIVE_LONG_TERM_QUERY_RESOURCE_CONSUMPTION (6):
                Too many expensive requests from query
                pattern over an extended duration of time.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        RESOURCE_EXHAUSTED = 2
        ACCESS_PROHIBITED = 3
        RESOURCE_TEMPORARILY_EXHAUSTED = 4
        EXCESSIVE_SHORT_TERM_QUERY_RESOURCE_CONSUMPTION = 5
        EXCESSIVE_LONG_TERM_QUERY_RESOURCE_CONSUMPTION = 6


__all__ = tuple(sorted(__protobuf__.manifest))

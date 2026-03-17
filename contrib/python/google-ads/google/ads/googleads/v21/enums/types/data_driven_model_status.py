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
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "DataDrivenModelStatusEnum",
    },
)


class DataDrivenModelStatusEnum(proto.Message):
    r"""Container for enum indicating data driven model status."""

    class DataDrivenModelStatus(proto.Enum):
        r"""Enumerates data driven model statuses.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AVAILABLE (2):
                The data driven model is available.
            STALE (3):
                The data driven model is stale. It hasn't
                been updated for at least 7 days. It is still
                being used, but will become expired if it does
                not get updated for 30 days.
            EXPIRED (4):
                The data driven model expired. It hasn't been
                updated for at least 30 days and cannot be used.
                Most commonly this is because there hasn't been
                the required number of events in a recent 30-day
                period.
            NEVER_GENERATED (5):
                The data driven model has never been
                generated. Most commonly this is because there
                has never been the required number of events in
                any 30-day period.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AVAILABLE = 2
        STALE = 3
        EXPIRED = 4
        NEVER_GENERATED = 5


__all__ = tuple(sorted(__protobuf__.manifest))

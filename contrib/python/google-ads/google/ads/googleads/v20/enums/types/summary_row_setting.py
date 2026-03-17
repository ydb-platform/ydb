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
    package="google.ads.googleads.v20.enums",
    marshal="google.ads.googleads.v20",
    manifest={
        "SummaryRowSettingEnum",
    },
)


class SummaryRowSettingEnum(proto.Message):
    r"""Indicates summary row setting in request parameter."""

    class SummaryRowSetting(proto.Enum):
        r"""Enum describing return summary row settings.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Represent unknown values of return summary
                row.
            NO_SUMMARY_ROW (2):
                Do not return summary row.
            SUMMARY_ROW_WITH_RESULTS (3):
                Return summary row along with results. The
                summary row will be returned in the last batch
                alone (last batch will contain no results).
            SUMMARY_ROW_ONLY (4):
                Return summary row only and return no
                results.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NO_SUMMARY_ROW = 2
        SUMMARY_ROW_WITH_RESULTS = 3
        SUMMARY_ROW_ONLY = 4


__all__ = tuple(sorted(__protobuf__.manifest))

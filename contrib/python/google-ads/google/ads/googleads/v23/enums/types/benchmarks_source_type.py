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
    package="google.ads.googleads.v23.enums",
    marshal="google.ads.googleads.v23",
    manifest={
        "BenchmarksSourceTypeEnum",
    },
)


class BenchmarksSourceTypeEnum(proto.Message):
    r"""Container for enum describing YouTube ad benchmarks sources."""

    class BenchmarksSourceType(proto.Enum):
        r"""Possible YouTube ad benchmarks sources.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            INDUSTRY_VERTICAL (2):
                The classification of ad categories for
                benchmarking. (for example, "Technology" or
                "Finance").
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INDUSTRY_VERTICAL = 2


__all__ = tuple(sorted(__protobuf__.manifest))

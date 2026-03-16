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
        "ConversionActionCountingTypeEnum",
    },
)


class ConversionActionCountingTypeEnum(proto.Message):
    r"""Container for enum describing the conversion deduplication
    mode for conversion optimizer.

    """

    class ConversionActionCountingType(proto.Enum):
        r"""Indicates how conversions for this action will be counted.
        For more information, see
        https://support.google.com/google-ads/answer/3438531.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ONE_PER_CLICK (2):
                Count only one conversion per click.
            MANY_PER_CLICK (3):
                Count all conversions per click.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ONE_PER_CLICK = 2
        MANY_PER_CLICK = 3


__all__ = tuple(sorted(__protobuf__.manifest))

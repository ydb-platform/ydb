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
        "AssetAutomationStatusEnum",
    },
)


class AssetAutomationStatusEnum(proto.Message):
    r"""Container for enum describing the status of asset automation."""

    class AssetAutomationStatus(proto.Enum):
        r"""The status of asset automation.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used as a return value only. Represents value
                unknown in this version.
            OPTED_IN (2):
                Opted-in will enable generating and serving
                an asset automation type.
            OPTED_OUT (3):
                Opted-out will stop generating and serving an
                asset automation type.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        OPTED_IN = 2
        OPTED_OUT = 3


__all__ = tuple(sorted(__protobuf__.manifest))

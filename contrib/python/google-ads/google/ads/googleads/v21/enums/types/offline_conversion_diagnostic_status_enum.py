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
        "OfflineConversionDiagnosticStatusEnum",
    },
)


class OfflineConversionDiagnosticStatusEnum(proto.Message):
    r"""All possible statuses for oci diagnostics."""

    class OfflineConversionDiagnosticStatus(proto.Enum):
        r"""Possible statuses of the offline ingestion setup.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            EXCELLENT (2):
                Your offline data ingestion setup is active
                and optimal for downstream processing.
            GOOD (3):
                Your offline ingestion setup is active, but
                there are further improvements you could make.
                See alerts.
            NEEDS_ATTENTION (4):
                Your offline ingestion setup is active, but
                there are errors that require your attention.
                See alerts.
            NO_RECENT_UPLOAD (6):
                Your offline ingestion setup has not received
                data in the last 28 days, there may be something
                wrong.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        EXCELLENT = 2
        GOOD = 3
        NEEDS_ATTENTION = 4
        NO_RECENT_UPLOAD = 6


__all__ = tuple(sorted(__protobuf__.manifest))

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
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "ExperimentStatusEnum",
    },
)


class ExperimentStatusEnum(proto.Message):
    r"""Container for enum describing the experiment status."""

    class ExperimentStatus(proto.Enum):
        r"""The status of the experiment.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            ENABLED (2):
                The experiment is enabled.
            REMOVED (3):
                The experiment has been removed.
            HALTED (4):
                The experiment has been halted.
                This status can be set from ENABLED status
                through API.
            PROMOTED (5):
                The experiment will be promoted out of
                experimental status.
            SETUP (6):
                Initial status of the experiment.
            INITIATED (7):
                The experiment's campaigns are pending
                materialization. This status can be set from
                SETUP status through API.
            GRADUATED (8):
                The experiment has been graduated.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ENABLED = 2
        REMOVED = 3
        HALTED = 4
        PROMOTED = 5
        SETUP = 6
        INITIATED = 7
        GRADUATED = 8


__all__ = tuple(sorted(__protobuf__.manifest))

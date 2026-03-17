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
        "AsyncActionStatusEnum",
    },
)


class AsyncActionStatusEnum(proto.Message):
    r"""Container for enum describing the experiment async action
    status.

    """

    class AsyncActionStatus(proto.Enum):
        r"""The async action status of the experiment.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                The value is unknown in this version.
            NOT_STARTED (2):
                Action has not started.
            IN_PROGRESS (3):
                Action is in progress.
            COMPLETED (4):
                Action has completed successfully.
            FAILED (5):
                Action has failed.
            COMPLETED_WITH_WARNING (6):
                Action has completed successfully with
                warnings.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NOT_STARTED = 2
        IN_PROGRESS = 3
        COMPLETED = 4
        FAILED = 5
        COMPLETED_WITH_WARNING = 6


__all__ = tuple(sorted(__protobuf__.manifest))

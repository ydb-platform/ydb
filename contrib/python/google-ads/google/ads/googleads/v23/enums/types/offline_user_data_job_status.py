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
        "OfflineUserDataJobStatusEnum",
    },
)


class OfflineUserDataJobStatusEnum(proto.Message):
    r"""Container for enum describing status of an offline user data
    job.

    """

    class OfflineUserDataJobStatus(proto.Enum):
        r"""The status of an offline user data job.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PENDING (2):
                The job has been successfully created and
                pending for uploading.
            RUNNING (3):
                Upload(s) have been accepted and data is
                being processed.
            SUCCESS (4):
                Uploaded data has been successfully
                processed. The job might have no operations,
                which can happen if the job was run without any
                operations added, or if all operations failed
                validation individually when attempting to add
                them to the job.
            FAILED (5):
                Uploaded data has failed to be processed.
                Some operations may have been successfully
                processed.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PENDING = 2
        RUNNING = 3
        SUCCESS = 4
        FAILED = 5


__all__ = tuple(sorted(__protobuf__.manifest))

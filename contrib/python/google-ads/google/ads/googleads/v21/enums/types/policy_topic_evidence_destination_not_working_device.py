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
        "PolicyTopicEvidenceDestinationNotWorkingDeviceEnum",
    },
)


class PolicyTopicEvidenceDestinationNotWorkingDeviceEnum(proto.Message):
    r"""Container for enum describing possible policy topic evidence
    destination not working devices.

    """

    class PolicyTopicEvidenceDestinationNotWorkingDevice(proto.Enum):
        r"""The possible policy topic evidence destination not working
        devices.

        Values:
            UNSPECIFIED (0):
                No value has been specified.
            UNKNOWN (1):
                The received value is not known in this
                version.
                This is a response-only value.
            DESKTOP (2):
                Landing page doesn't work on desktop device.
            ANDROID (3):
                Landing page doesn't work on Android device.
            IOS (4):
                Landing page doesn't work on iOS device.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DESKTOP = 2
        ANDROID = 3
        IOS = 4


__all__ = tuple(sorted(__protobuf__.manifest))

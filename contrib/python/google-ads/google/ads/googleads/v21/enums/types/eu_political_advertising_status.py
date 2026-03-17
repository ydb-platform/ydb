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
        "EuPoliticalAdvertisingStatusEnum",
    },
)


class EuPoliticalAdvertisingStatusEnum(proto.Message):
    r"""Container for enum describing whether or not the campaign
    contains political advertising targeted towards the European
    Union.

    """

    class EuPoliticalAdvertisingStatus(proto.Enum):
        r"""Possible values describing whether or not the campaign
        contains political advertising targeted towards the European
        Union.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CONTAINS_EU_POLITICAL_ADVERTISING (2):
                The campaign contains political advertising
                targeted towards the EU. The campaign will be
                restricted from serving ads in the EU.
            DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING (3):
                The campaign does not contain political
                advertising targeted towards the EU. No
                additional serving restrictions will apply.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CONTAINS_EU_POLITICAL_ADVERTISING = 2
        DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING = 3


__all__ = tuple(sorted(__protobuf__.manifest))

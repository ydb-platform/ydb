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
        "LeadFormPostSubmitCallToActionTypeEnum",
    },
)


class LeadFormPostSubmitCallToActionTypeEnum(proto.Message):
    r"""Describes the type of post-submit call-to-action phrases for
    a lead form.

    """

    class LeadFormPostSubmitCallToActionType(proto.Enum):
        r"""Enum describing the type of post-submit call-to-action
        phrases for a lead form.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            VISIT_SITE (2):
                Visit site.
            DOWNLOAD (3):
                Download.
            LEARN_MORE (4):
                Learn more.
            SHOP_NOW (5):
                Shop now.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        VISIT_SITE = 2
        DOWNLOAD = 3
        LEARN_MORE = 4
        SHOP_NOW = 5


__all__ = tuple(sorted(__protobuf__.manifest))

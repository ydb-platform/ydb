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
        "GoogleAdsFieldCategoryEnum",
    },
)


class GoogleAdsFieldCategoryEnum(proto.Message):
    r"""Container for enum that determines if the described artifact
    is a resource or a field, and if it is a field, when it segments
    search queries.

    """

    class GoogleAdsFieldCategory(proto.Enum):
        r"""The category of the artifact.

        Values:
            UNSPECIFIED (0):
                Unspecified
            UNKNOWN (1):
                Unknown
            RESOURCE (2):
                The described artifact is a resource.
            ATTRIBUTE (3):
                The described artifact is a field and is an
                attribute of a resource. Including a resource
                attribute field in a query may segment the query
                if the resource to which it is attributed
                segments the resource found in the FROM clause.
            SEGMENT (5):
                The described artifact is a field and always
                segments search queries.
            METRIC (6):
                The described artifact is a field and is a
                metric. It never segments search queries.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        RESOURCE = 2
        ATTRIBUTE = 3
        SEGMENT = 5
        METRIC = 6


__all__ = tuple(sorted(__protobuf__.manifest))

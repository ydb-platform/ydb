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
        "ResponseContentTypeEnum",
    },
)


class ResponseContentTypeEnum(proto.Message):
    r"""Container for possible response content types."""

    class ResponseContentType(proto.Enum):
        r"""Possible response content types.

        Values:
            UNSPECIFIED (0):
                Not specified. Will return the resource name
                only in the response.
            RESOURCE_NAME_ONLY (1):
                The mutate response will be the resource
                name.
            MUTABLE_RESOURCE (2):
                The mutate response will contain the resource
                name and the resource with mutable fields if
                possible. Otherwise, only the resource name will
                be returned.
        """

        UNSPECIFIED = 0
        RESOURCE_NAME_ONLY = 1
        MUTABLE_RESOURCE = 2


__all__ = tuple(sorted(__protobuf__.manifest))

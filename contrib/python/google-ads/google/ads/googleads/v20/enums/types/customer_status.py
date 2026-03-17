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
        "CustomerStatusEnum",
    },
)


class CustomerStatusEnum(proto.Message):
    r"""Container for enum describing possible statuses of a
    customer.

    """

    class CustomerStatus(proto.Enum):
        r"""Possible statuses of a customer.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            ENABLED (2):
                Indicates an active account able to serve
                ads.
            CANCELED (3):
                Indicates a canceled account unable to serve
                ads. Can be reactivated by an admin user.
            SUSPENDED (4):
                Indicates a suspended account unable to serve
                ads. May only be activated by Google support.
            CLOSED (5):
                Indicates a closed account unable to serve
                ads. Test account will also have CLOSED status.
                Status is permanent and may not be reopened.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        ENABLED = 2
        CANCELED = 3
        SUSPENDED = 4
        CLOSED = 5


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "CustomerAcquisitionOptimizationModeEnum",
    },
)


class CustomerAcquisitionOptimizationModeEnum(proto.Message):
    r"""Container for enum describing possible optimization mode of a
    customer acquisition goal of a campaign.

    """

    class CustomerAcquisitionOptimizationMode(proto.Enum):
        r"""Possible optimization mode of a customer acquisition goal.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            TARGET_ALL_EQUALLY (2):
                The mode is used when campaign is optimizing
                equally for existing and new customers, which is
                the default value.
            BID_HIGHER_FOR_NEW_CUSTOMER (3):
                The mode is used when campaign is bidding
                higher for new customers than existing customer.
            TARGET_NEW_CUSTOMER (4):
                The mode is used when campaign is only
                optimizing for new customers.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        TARGET_ALL_EQUALLY = 2
        BID_HIGHER_FOR_NEW_CUSTOMER = 3
        TARGET_NEW_CUSTOMER = 4


__all__ = tuple(sorted(__protobuf__.manifest))

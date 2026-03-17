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
        "OfflineUserDataJobFailureReasonEnum",
    },
)


class OfflineUserDataJobFailureReasonEnum(proto.Message):
    r"""Container for enum describing reasons why an offline user
    data job failed to be processed.

    """

    class OfflineUserDataJobFailureReason(proto.Enum):
        r"""The failure reason of an offline user data job.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            INSUFFICIENT_MATCHED_TRANSACTIONS (2):
                The matched transactions are insufficient.
            INSUFFICIENT_TRANSACTIONS (3):
                The uploaded transactions are insufficient.
            HIGH_AVERAGE_TRANSACTION_VALUE (4):
                The average transaction value is unusually high for your
                account. If this is intended, contact support to request an
                exception. Learn more at
                https://support.google.com/google-ads/answer/10018944#transaction_value
            LOW_AVERAGE_TRANSACTION_VALUE (5):
                The average transaction value is unusually low for your
                account. If this is intended, contact support to request an
                exception. Learn more at
                https://support.google.com/google-ads/answer/10018944#transaction_value
            NEWLY_OBSERVED_CURRENCY_CODE (6):
                There's a currency code that you haven't used before in your
                uploads. If this is intended, contact support to request an
                exception. Learn more at
                https://support.google.com/google-ads/answer/10018944#Unrecognized_currency
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INSUFFICIENT_MATCHED_TRANSACTIONS = 2
        INSUFFICIENT_TRANSACTIONS = 3
        HIGH_AVERAGE_TRANSACTION_VALUE = 4
        LOW_AVERAGE_TRANSACTION_VALUE = 5
        NEWLY_OBSERVED_CURRENCY_CODE = 6


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "InvoiceTypeEnum",
    },
)


class InvoiceTypeEnum(proto.Message):
    r"""Container for enum describing the type of invoices."""

    class InvoiceType(proto.Enum):
        r"""The possible type of invoices.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            CREDIT_MEMO (2):
                An invoice with a negative amount. The
                account receives a credit.
            INVOICE (3):
                An invoice with a positive amount. The
                account owes a balance.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CREDIT_MEMO = 2
        INVOICE = 3


__all__ = tuple(sorted(__protobuf__.manifest))

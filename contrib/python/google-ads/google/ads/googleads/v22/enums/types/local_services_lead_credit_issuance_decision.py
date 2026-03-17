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
        "LocalServicesLeadCreditIssuanceDecisionEnum",
    },
)


class LocalServicesLeadCreditIssuanceDecisionEnum(proto.Message):
    r"""Container for enum describing possible credit issuance
    decisions for a lead.

    """

    class CreditIssuanceDecision(proto.Enum):
        r"""Decision of bonus credit issued or rejected.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            SUCCESS_NOT_REACHED_THRESHOLD (2):
                Bonus credit is issued successfully and bonus
                credit cap has not reached the threshold after
                issuing this bonus credit.
            SUCCESS_REACHED_THRESHOLD (3):
                Bonus credit is issued successfully and bonus
                credit cap has reached the threshold after
                issuing this bonus credit.
            FAIL_OVER_THRESHOLD (4):
                Bonus credit is not issued because the
                provider has reached the bonus credit cap.
            FAIL_NOT_ELIGIBLE (5):
                Bonus credit is not issued because this lead
                is not eligible for bonus credit.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        SUCCESS_NOT_REACHED_THRESHOLD = 2
        SUCCESS_REACHED_THRESHOLD = 3
        FAIL_OVER_THRESHOLD = 4
        FAIL_NOT_ELIGIBLE = 5


__all__ = tuple(sorted(__protobuf__.manifest))

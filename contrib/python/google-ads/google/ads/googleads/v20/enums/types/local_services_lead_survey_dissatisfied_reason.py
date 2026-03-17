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
        "LocalServicesLeadSurveyDissatisfiedReasonEnum",
    },
)


class LocalServicesLeadSurveyDissatisfiedReasonEnum(proto.Message):
    r"""Container for enum describing possible survey dissatisfied
    reasons for a lead.

    """

    class SurveyDissatisfiedReason(proto.Enum):
        r"""Provider's reason for not being satisfied with the lead.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            OTHER_DISSATISFIED_REASON (2):
                Other reasons.
            GEO_MISMATCH (3):
                Lead was for a service that does not match
                the business' service area.
            JOB_TYPE_MISMATCH (4):
                Lead was for a service that is not offered by
                the business.
            NOT_READY_TO_BOOK (5):
                Lead was by a customer that was not ready to
                book.
            SPAM (6):
                Lead was a spam. Example: lead was from a
                bot, silent called, scam, etc.
            DUPLICATE (7):
                Lead was a duplicate of another lead that is,
                customer contacted the business more than once.
            SOLICITATION (8):
                Lead due to solicitation. Example: a person
                trying to get a job or selling a product, etc.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        OTHER_DISSATISFIED_REASON = 2
        GEO_MISMATCH = 3
        JOB_TYPE_MISMATCH = 4
        NOT_READY_TO_BOOK = 5
        SPAM = 6
        DUPLICATE = 7
        SOLICITATION = 8


__all__ = tuple(sorted(__protobuf__.manifest))

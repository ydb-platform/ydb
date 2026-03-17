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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "CampaignDraftErrorEnum",
    },
)


class CampaignDraftErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign draft errors."""

    class CampaignDraftError(proto.Enum):
        r"""Enum describing possible campaign draft errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            DUPLICATE_DRAFT_NAME (2):
                A draft with this name already exists for
                this campaign.
            INVALID_STATUS_TRANSITION_FROM_REMOVED (3):
                The draft is removed and cannot be
                transitioned to another status.
            INVALID_STATUS_TRANSITION_FROM_PROMOTED (4):
                The draft has been promoted and cannot be
                transitioned to the specified status.
            INVALID_STATUS_TRANSITION_FROM_PROMOTE_FAILED (5):
                The draft has failed to be promoted and
                cannot be transitioned to the specified status.
            CUSTOMER_CANNOT_CREATE_DRAFT (6):
                This customer is not allowed to create
                drafts.
            CAMPAIGN_CANNOT_CREATE_DRAFT (7):
                This campaign is not allowed to create
                drafts.
            INVALID_DRAFT_CHANGE (8):
                This modification cannot be made on a draft.
            INVALID_STATUS_TRANSITION (9):
                The draft cannot be transitioned to the
                specified status from its current status.
            MAX_NUMBER_OF_DRAFTS_PER_CAMPAIGN_REACHED (10):
                The campaign has reached the maximum number
                of drafts that can be created for a campaign
                throughout its lifetime. No additional drafts
                can be created for this campaign. Removed drafts
                also count towards this limit.
            LIST_ERRORS_FOR_PROMOTED_DRAFT_ONLY (11):
                ListAsyncErrors was called without first
                promoting the draft.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_DRAFT_NAME = 2
        INVALID_STATUS_TRANSITION_FROM_REMOVED = 3
        INVALID_STATUS_TRANSITION_FROM_PROMOTED = 4
        INVALID_STATUS_TRANSITION_FROM_PROMOTE_FAILED = 5
        CUSTOMER_CANNOT_CREATE_DRAFT = 6
        CAMPAIGN_CANNOT_CREATE_DRAFT = 7
        INVALID_DRAFT_CHANGE = 8
        INVALID_STATUS_TRANSITION = 9
        MAX_NUMBER_OF_DRAFTS_PER_CAMPAIGN_REACHED = 10
        LIST_ERRORS_FOR_PROMOTED_DRAFT_ONLY = 11


__all__ = tuple(sorted(__protobuf__.manifest))

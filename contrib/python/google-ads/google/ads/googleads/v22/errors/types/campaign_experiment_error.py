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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "CampaignExperimentErrorEnum",
    },
)


class CampaignExperimentErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign experiment
    errors.

    """

    class CampaignExperimentError(proto.Enum):
        r"""Enum describing possible campaign experiment errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            DUPLICATE_NAME (2):
                An active campaign or experiment with this
                name already exists.
            INVALID_TRANSITION (3):
                Experiment cannot be updated from the current
                state to the requested target state. For
                example, an experiment can only graduate if its
                status is ENABLED.
            CANNOT_CREATE_EXPERIMENT_WITH_SHARED_BUDGET (4):
                Cannot create an experiment from a campaign
                using an explicitly shared budget.
            CANNOT_CREATE_EXPERIMENT_FOR_REMOVED_BASE_CAMPAIGN (5):
                Cannot create an experiment for a removed
                base campaign.
            CANNOT_CREATE_EXPERIMENT_FOR_NON_PROPOSED_DRAFT (6):
                Cannot create an experiment from a draft,
                which has a status other than proposed.
            CUSTOMER_CANNOT_CREATE_EXPERIMENT (7):
                This customer is not allowed to create an
                experiment.
            CAMPAIGN_CANNOT_CREATE_EXPERIMENT (8):
                This campaign is not allowed to create an
                experiment.
            EXPERIMENT_DURATIONS_MUST_NOT_OVERLAP (9):
                Trying to set an experiment duration which
                overlaps with another experiment.
            EXPERIMENT_DURATION_MUST_BE_WITHIN_CAMPAIGN_DURATION (10):
                All non-removed experiments must start and
                end within their campaign's duration.
            CANNOT_MUTATE_EXPERIMENT_DUE_TO_STATUS (11):
                The experiment cannot be modified because its
                status is in a terminal state, such as REMOVED.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_NAME = 2
        INVALID_TRANSITION = 3
        CANNOT_CREATE_EXPERIMENT_WITH_SHARED_BUDGET = 4
        CANNOT_CREATE_EXPERIMENT_FOR_REMOVED_BASE_CAMPAIGN = 5
        CANNOT_CREATE_EXPERIMENT_FOR_NON_PROPOSED_DRAFT = 6
        CUSTOMER_CANNOT_CREATE_EXPERIMENT = 7
        CAMPAIGN_CANNOT_CREATE_EXPERIMENT = 8
        EXPERIMENT_DURATIONS_MUST_NOT_OVERLAP = 9
        EXPERIMENT_DURATION_MUST_BE_WITHIN_CAMPAIGN_DURATION = 10
        CANNOT_MUTATE_EXPERIMENT_DUE_TO_STATUS = 11


__all__ = tuple(sorted(__protobuf__.manifest))

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
    package="google.ads.googleads.v20.errors",
    marshal="google.ads.googleads.v20",
    manifest={
        "ExperimentErrorEnum",
    },
)


class ExperimentErrorEnum(proto.Message):
    r"""Container for enum describing possible experiment error."""

    class ExperimentError(proto.Enum):
        r"""Enum describing possible experiment errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            CANNOT_SET_START_DATE_IN_PAST (2):
                The start date of an experiment cannot be set
                in the past. Use a start date in the future.
            END_DATE_BEFORE_START_DATE (3):
                The end date of an experiment is before its
                start date. Use an end date after the start
                date.
            START_DATE_TOO_FAR_IN_FUTURE (4):
                The start date of an experiment is too far in
                the future. Use a start date no more than 1 year
                in the future.
            DUPLICATE_EXPERIMENT_NAME (5):
                The experiment has the same name as an
                existing active experiment.
            CANNOT_MODIFY_REMOVED_EXPERIMENT (6):
                Experiments can only be modified when they
                are ENABLED.
            START_DATE_ALREADY_PASSED (7):
                The start date of an experiment cannot be
                modified if the existing start date has already
                passed.
            CANNOT_SET_END_DATE_IN_PAST (8):
                The end date of an experiment cannot be set
                in the past.
            CANNOT_SET_STATUS_TO_REMOVED (9):
                The status of an experiment cannot be set to
                REMOVED.
            CANNOT_MODIFY_PAST_END_DATE (10):
                The end date of an expired experiment cannot
                be modified.
            INVALID_STATUS (11):
                The status is invalid.
            INVALID_CAMPAIGN_CHANNEL_TYPE (12):
                Experiment arm contains campaigns with
                invalid advertising channel type.
            OVERLAPPING_MEMBERS_AND_DATE_RANGE (13):
                A pair of trials share members and have
                overlapping date ranges.
            INVALID_TRIAL_ARM_TRAFFIC_SPLIT (14):
                Experiment arm contains invalid traffic
                split.
            TRAFFIC_SPLIT_OVERLAPPING (15):
                Experiment contains trial arms with
                overlapping traffic split.
            SUM_TRIAL_ARM_TRAFFIC_UNEQUALS_TO_TRIAL_TRAFFIC_SPLIT_DENOMINATOR (16):
                The total traffic split of trial arms is not
                equal to 100.
            CANNOT_MODIFY_TRAFFIC_SPLIT_AFTER_START (17):
                Traffic split related settings (like traffic
                share bounds) can't be modified after the
                experiment has started.
            EXPERIMENT_NOT_FOUND (18):
                The experiment could not be found.
            EXPERIMENT_NOT_YET_STARTED (19):
                Experiment has not begun.
            CANNOT_HAVE_MULTIPLE_CONTROL_ARMS (20):
                The experiment cannot have more than one
                control arm.
            IN_DESIGN_CAMPAIGNS_NOT_SET (21):
                The experiment doesn't set in-design
                campaigns.
            CANNOT_SET_STATUS_TO_GRADUATED (22):
                Clients must use the graduate action to
                graduate experiments and cannot set the status
                to GRADUATED directly.
            CANNOT_CREATE_EXPERIMENT_CAMPAIGN_WITH_SHARED_BUDGET (23):
                Cannot use shared budget on base campaign
                when scheduling an experiment.
            CANNOT_CREATE_EXPERIMENT_CAMPAIGN_WITH_CUSTOM_BUDGET (24):
                Cannot use custom budget on base campaign
                when scheduling an experiment.
            STATUS_TRANSITION_INVALID (25):
                Invalid status transition.
            DUPLICATE_EXPERIMENT_CAMPAIGN_NAME (26):
                The experiment campaign name conflicts with a
                pre-existing campaign.
            CANNOT_REMOVE_IN_CREATION_EXPERIMENT (27):
                Cannot remove in creation experiments.
            CANNOT_ADD_CAMPAIGN_WITH_DEPRECATED_AD_TYPES (28):
                Cannot add campaign with deprecated ad types. Deprecated ad
                types: ENHANCED_DISPLAY, GALLERY, GMAIL, KEYWORDLESS, TEXT.
            CANNOT_ENABLE_SYNC_FOR_UNSUPPORTED_EXPERIMENT_TYPE (29):
                Sync can only be enabled for supported experiment types.
                Supported experiment types: SEARCH_CUSTOM, DISPLAY_CUSTOM,
                DISPLAY_AUTOMATED_BIDDING_STRATEGY,
                SEARCH_AUTOMATED_BIDDING_STRATEGY.
            INVALID_DURATION_FOR_AN_EXPERIMENT (30):
                Experiment length cannot be longer than max
                length.
            MISSING_EU_POLITICAL_ADVERTISING_SELF_DECLARATION (31):
                The experiment's campaigns must self-declare
                whether they contain political advertising that
                targets the European Union.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_SET_START_DATE_IN_PAST = 2
        END_DATE_BEFORE_START_DATE = 3
        START_DATE_TOO_FAR_IN_FUTURE = 4
        DUPLICATE_EXPERIMENT_NAME = 5
        CANNOT_MODIFY_REMOVED_EXPERIMENT = 6
        START_DATE_ALREADY_PASSED = 7
        CANNOT_SET_END_DATE_IN_PAST = 8
        CANNOT_SET_STATUS_TO_REMOVED = 9
        CANNOT_MODIFY_PAST_END_DATE = 10
        INVALID_STATUS = 11
        INVALID_CAMPAIGN_CHANNEL_TYPE = 12
        OVERLAPPING_MEMBERS_AND_DATE_RANGE = 13
        INVALID_TRIAL_ARM_TRAFFIC_SPLIT = 14
        TRAFFIC_SPLIT_OVERLAPPING = 15
        SUM_TRIAL_ARM_TRAFFIC_UNEQUALS_TO_TRIAL_TRAFFIC_SPLIT_DENOMINATOR = 16
        CANNOT_MODIFY_TRAFFIC_SPLIT_AFTER_START = 17
        EXPERIMENT_NOT_FOUND = 18
        EXPERIMENT_NOT_YET_STARTED = 19
        CANNOT_HAVE_MULTIPLE_CONTROL_ARMS = 20
        IN_DESIGN_CAMPAIGNS_NOT_SET = 21
        CANNOT_SET_STATUS_TO_GRADUATED = 22
        CANNOT_CREATE_EXPERIMENT_CAMPAIGN_WITH_SHARED_BUDGET = 23
        CANNOT_CREATE_EXPERIMENT_CAMPAIGN_WITH_CUSTOM_BUDGET = 24
        STATUS_TRANSITION_INVALID = 25
        DUPLICATE_EXPERIMENT_CAMPAIGN_NAME = 26
        CANNOT_REMOVE_IN_CREATION_EXPERIMENT = 27
        CANNOT_ADD_CAMPAIGN_WITH_DEPRECATED_AD_TYPES = 28
        CANNOT_ENABLE_SYNC_FOR_UNSUPPORTED_EXPERIMENT_TYPE = 29
        INVALID_DURATION_FOR_AN_EXPERIMENT = 30
        MISSING_EU_POLITICAL_ADVERTISING_SELF_DECLARATION = 31


__all__ = tuple(sorted(__protobuf__.manifest))

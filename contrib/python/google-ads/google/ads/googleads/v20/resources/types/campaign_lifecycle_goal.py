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

from google.ads.googleads.v20.common.types import lifecycle_goals
from google.ads.googleads.v20.enums.types import (
    customer_acquisition_optimization_mode,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "CampaignLifecycleGoal",
        "CustomerAcquisitionGoalSettings",
    },
)


class CampaignLifecycleGoal(proto.Message):
    r"""Campaign level customer lifecycle goal settings.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the customer lifecycle goal
            of a campaign.

            ``customers/{customer_id}/campaignLifecycleGoal/{campaign_id}``
        campaign (str):
            Output only. The campaign where the goal is
            attached.
        customer_acquisition_goal_settings (google.ads.googleads.v20.resources.types.CustomerAcquisitionGoalSettings):
            Output only. The customer acquisition goal
            settings for the campaign. The customer
            acquisition goal is described in this article:

            https://support.google.com/google-ads/answer/12080169
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=2,
    )
    customer_acquisition_goal_settings: "CustomerAcquisitionGoalSettings" = (
        proto.Field(
            proto.MESSAGE,
            number=3,
            message="CustomerAcquisitionGoalSettings",
        )
    )


class CustomerAcquisitionGoalSettings(proto.Message):
    r"""The customer acquisition goal settings for the campaign.

    Attributes:
        optimization_mode (google.ads.googleads.v20.enums.types.CustomerAcquisitionOptimizationModeEnum.CustomerAcquisitionOptimizationMode):
            Output only. Customer acquisition
            optimization mode of this campaign.
        value_settings (google.ads.googleads.v20.common.types.LifecycleGoalValueSettings):
            Output only. Campaign specific values for the
            customer acquisition goal.
    """

    optimization_mode: (
        customer_acquisition_optimization_mode.CustomerAcquisitionOptimizationModeEnum.CustomerAcquisitionOptimizationMode
    ) = proto.Field(
        proto.ENUM,
        number=1,
        enum=customer_acquisition_optimization_mode.CustomerAcquisitionOptimizationModeEnum.CustomerAcquisitionOptimizationMode,
    )
    value_settings: lifecycle_goals.LifecycleGoalValueSettings = proto.Field(
        proto.MESSAGE,
        number=2,
        message=lifecycle_goals.LifecycleGoalValueSettings,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

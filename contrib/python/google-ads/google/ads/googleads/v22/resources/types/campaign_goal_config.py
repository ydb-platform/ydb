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

from google.ads.googleads.v22.common.types import campaign_goal_settings
from google.ads.googleads.v22.enums.types import goal_type as gage_goal_type


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.resources",
    marshal="google.ads.googleads.v22",
    manifest={
        "CampaignGoalConfig",
    },
)


class CampaignGoalConfig(proto.Message):
    r"""A link between a campaign and a goal enabling
    campaign-specific optimization.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the campaign goal config.
            campaign goal config resource names have the form:
            ``customers/{customer_id}/campaignGoalConfigs/{campaign_id}~{goal_id}``
        campaign (str):
            Immutable. The resource name of the campaign
            for this link.
        goal (str):
            Immutable. The resource name of the goal this
            link is attached to.
        goal_type (google.ads.googleads.v22.enums.types.GoalTypeEnum.GoalType):
            Output only. The goal type this link is
            attached to.
        campaign_retention_settings (google.ads.googleads.v22.common.types.CampaignGoalSettings.CampaignRetentionGoalSettings):
            Retention goal campaign settings.

            This field is a member of `oneof`_ ``campaign_goal_config_settings``.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    campaign: str = proto.Field(
        proto.STRING,
        number=2,
    )
    goal: str = proto.Field(
        proto.STRING,
        number=3,
    )
    goal_type: gage_goal_type.GoalTypeEnum.GoalType = proto.Field(
        proto.ENUM,
        number=4,
        enum=gage_goal_type.GoalTypeEnum.GoalType,
    )
    campaign_retention_settings: (
        campaign_goal_settings.CampaignGoalSettings.CampaignRetentionGoalSettings
    ) = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="campaign_goal_config_settings",
        message=campaign_goal_settings.CampaignGoalSettings.CampaignRetentionGoalSettings,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

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

from typing import MutableSequence

import proto  # type: ignore

from google.ads.googleads.v23.resources.types import campaign_goal_config
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "MutateCampaignGoalConfigsRequest",
        "CampaignGoalConfigOperation",
        "MutateCampaignGoalConfigsResponse",
        "MutateCampaignGoalConfigResult",
    },
)


class MutateCampaignGoalConfigsRequest(proto.Message):
    r"""Request message for
    [CampaignGoalConfigService.MutateCampaignGoalConfigs][google.ads.googleads.v23.services.CampaignGoalConfigService.MutateCampaignGoalConfigs].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            campaign goal configs are being modified.
        operations (MutableSequence[google.ads.googleads.v23.services.types.CampaignGoalConfigOperation]):
            Required. The list of operations to perform
            on the campaign goal configs.
        partial_failure (bool):
            Optional. If true, successful operations will
            be carried out and invalid operations will
            return errors. If false, all operations will be
            carried out in one transaction if and only if
            they are all valid. Default is false.
        validate_only (bool):
            Optional. If true, the request is validated
            but not executed. Only errors are returned, not
            results.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    operations: MutableSequence["CampaignGoalConfigOperation"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="CampaignGoalConfigOperation",
        )
    )
    partial_failure: bool = proto.Field(
        proto.BOOL,
        number=3,
    )
    validate_only: bool = proto.Field(
        proto.BOOL,
        number=4,
    )


class CampaignGoalConfigOperation(proto.Message):
    r"""A single mutate operation on the campaign goal config.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which fields are
            modified in an update.
        create (google.ads.googleads.v23.resources.types.CampaignGoalConfig):
            Create a new campaign goal config.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v23.resources.types.CampaignGoalConfig):
            Update an existing campaign goal config.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove an existing campaign goal config.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask: field_mask_pb2.FieldMask = proto.Field(
        proto.MESSAGE,
        number=4,
        message=field_mask_pb2.FieldMask,
    )
    create: campaign_goal_config.CampaignGoalConfig = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=campaign_goal_config.CampaignGoalConfig,
    )
    update: campaign_goal_config.CampaignGoalConfig = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="operation",
        message=campaign_goal_config.CampaignGoalConfig,
    )
    remove: str = proto.Field(
        proto.STRING,
        number=2,
        oneof="operation",
    )


class MutateCampaignGoalConfigsResponse(proto.Message):
    r"""Response message for a campaign goal config mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in
            the partial failure mode.
        results (MutableSequence[google.ads.googleads.v23.services.types.MutateCampaignGoalConfigResult]):
            All results for the mutate.
    """

    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=1,
        message=status_pb2.Status,
    )
    results: MutableSequence["MutateCampaignGoalConfigResult"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="MutateCampaignGoalConfigResult",
        )
    )


class MutateCampaignGoalConfigResult(proto.Message):
    r"""The result for the campaign goal config mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

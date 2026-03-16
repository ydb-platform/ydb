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

from google.ads.googleads.v23.resources.types import goal
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "MutateGoalsRequest",
        "GoalOperation",
        "MutateGoalsResponse",
        "MutateGoalResult",
    },
)


class MutateGoalsRequest(proto.Message):
    r"""Request message for
    [GoalService.MutateGoals][google.ads.googleads.v23.services.GoalService.MutateGoals].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose goals
            are being modified.
        operations (MutableSequence[google.ads.googleads.v23.services.types.GoalOperation]):
            Required. The list of operations to perform
            on the goals.
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
    operations: MutableSequence["GoalOperation"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="GoalOperation",
    )
    partial_failure: bool = proto.Field(
        proto.BOOL,
        number=3,
    )
    validate_only: bool = proto.Field(
        proto.BOOL,
        number=4,
    )


class GoalOperation(proto.Message):
    r"""A single mutate operation on the goal.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v23.resources.types.Goal):
            Create a new goal.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v23.resources.types.Goal):
            Update an existing goal.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask: field_mask_pb2.FieldMask = proto.Field(
        proto.MESSAGE,
        number=3,
        message=field_mask_pb2.FieldMask,
    )
    create: goal.Goal = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=goal.Goal,
    )
    update: goal.Goal = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=goal.Goal,
    )


class MutateGoalsResponse(proto.Message):
    r"""Response message for a goal mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in
            the partial failure mode.
        results (MutableSequence[google.ads.googleads.v23.services.types.MutateGoalResult]):
            All results for the mutate.
    """

    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=1,
        message=status_pb2.Status,
    )
    results: MutableSequence["MutateGoalResult"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="MutateGoalResult",
    )


class MutateGoalResult(proto.Message):
    r"""The result for the goal mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

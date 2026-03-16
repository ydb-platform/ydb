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

from google.ads.googleads.v23.enums.types import (
    response_content_type as gage_response_content_type,
)
from google.ads.googleads.v23.resources.types import (
    recommendation_subscription as gagr_recommendation_subscription,
)
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "MutateRecommendationSubscriptionRequest",
        "RecommendationSubscriptionOperation",
        "MutateRecommendationSubscriptionResponse",
        "MutateRecommendationSubscriptionResult",
    },
)


class MutateRecommendationSubscriptionRequest(proto.Message):
    r"""Request message for
    [RecommendationSubscriptionService.MutateRecommendationSubscription][google.ads.googleads.v23.services.RecommendationSubscriptionService.MutateRecommendationSubscription]

    Attributes:
        customer_id (str):
            Required. The ID of the subscribing customer.
        operations (MutableSequence[google.ads.googleads.v23.services.types.RecommendationSubscriptionOperation]):
            Required. The list of create or update
            operations.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        response_content_type (google.ads.googleads.v23.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned post mutation.
            The mutable resource will only be returned if
            the resource has the appropriate response field.
            For example, MutateCampaignResult.campaign.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    operations: MutableSequence["RecommendationSubscriptionOperation"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="RecommendationSubscriptionOperation",
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
    response_content_type: (
        gage_response_content_type.ResponseContentTypeEnum.ResponseContentType
    ) = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class RecommendationSubscriptionOperation(proto.Message):
    r"""A single operation (create, update) on a recommendation
    subscription.
    [RecommendationSubscriptionService.MutateRecommendationSubscription][google.ads.googleads.v23.services.RecommendationSubscriptionService.MutateRecommendationSubscription]

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            Optional. FieldMask that determines which
            resource fields are modified in an update.
        create (google.ads.googleads.v23.resources.types.RecommendationSubscription):
            Create operation: No resource name is
            expected for the new subscription.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v23.resources.types.RecommendationSubscription):
            Update operation: The subscription is
            expected to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask: field_mask_pb2.FieldMask = proto.Field(
        proto.MESSAGE,
        number=3,
        message=field_mask_pb2.FieldMask,
    )
    create: gagr_recommendation_subscription.RecommendationSubscription = (
        proto.Field(
            proto.MESSAGE,
            number=1,
            oneof="operation",
            message=gagr_recommendation_subscription.RecommendationSubscription,
        )
    )
    update: gagr_recommendation_subscription.RecommendationSubscription = (
        proto.Field(
            proto.MESSAGE,
            number=2,
            oneof="operation",
            message=gagr_recommendation_subscription.RecommendationSubscription,
        )
    )


class MutateRecommendationSubscriptionResponse(proto.Message):
    r"""Response message for
    [RecommendationSubscriptionService.MutateRecommendationSubscription][google.ads.googleads.v23.services.RecommendationSubscriptionService.MutateRecommendationSubscription]

    Attributes:
        results (MutableSequence[google.ads.googleads.v23.services.types.MutateRecommendationSubscriptionResult]):
            Results, one per operation.
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors) we return
            the RPC level error.
    """

    results: MutableSequence["MutateRecommendationSubscriptionResult"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message="MutateRecommendationSubscriptionResult",
        )
    )
    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=2,
        message=status_pb2.Status,
    )


class MutateRecommendationSubscriptionResult(proto.Message):
    r"""Result message for
    [RecommendationSubscriptionService.MutateRecommendationSubscription][google.ads.googleads.v23.services.RecommendationSubscriptionService.MutateRecommendationSubscription]

    Attributes:
        resource_name (str):
            Resource name of the subscription that was
            modified.
        recommendation_subscription (google.ads.googleads.v23.resources.types.RecommendationSubscription):
            The mutated recommendation subscription with only mutable
            fields after mutate. The field will only be returned when
            response_content_type is set to "MUTABLE_RESOURCE".
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    recommendation_subscription: (
        gagr_recommendation_subscription.RecommendationSubscription
    ) = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_recommendation_subscription.RecommendationSubscription,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

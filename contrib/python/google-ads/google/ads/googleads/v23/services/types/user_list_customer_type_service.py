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

from google.ads.googleads.v23.resources.types import user_list_customer_type
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "MutateUserListCustomerTypesRequest",
        "UserListCustomerTypeOperation",
        "MutateUserListCustomerTypesResponse",
        "MutateUserListCustomerTypeResult",
    },
)


class MutateUserListCustomerTypesRequest(proto.Message):
    r"""Request message for
    [UserListCustomerTypeService.MutateUserListCustomerTypes][google.ads.googleads.v23.services.UserListCustomerTypeService.MutateUserListCustomerTypes].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose user
            list customer types are being modified.
        operations (MutableSequence[google.ads.googleads.v23.services.types.UserListCustomerTypeOperation]):
            Required. The list of operations to perform
            on the user list customer types.
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
    operations: MutableSequence["UserListCustomerTypeOperation"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="UserListCustomerTypeOperation",
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


class UserListCustomerTypeOperation(proto.Message):
    r"""A single mutate operation on the user list customer type.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        create (google.ads.googleads.v23.resources.types.UserListCustomerType):
            Attach a user list customer type to a user
            list. No resource name is expected for the new
            user list customer type.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove an existing user list customer type. A resource name
            for the removed user list customer type is expected, in this
            format:

            ``customers/{customer_id}/userListCustomerTypes/{user_list_id}~{customer_type_category}``

            This field is a member of `oneof`_ ``operation``.
    """

    create: user_list_customer_type.UserListCustomerType = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=user_list_customer_type.UserListCustomerType,
    )
    remove: str = proto.Field(
        proto.STRING,
        number=2,
        oneof="operation",
    )


class MutateUserListCustomerTypesResponse(proto.Message):
    r"""Response message for a user list customer type mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        results (MutableSequence[google.ads.googleads.v23.services.types.MutateUserListCustomerTypeResult]):
            All results for the mutate.
    """

    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=1,
        message=status_pb2.Status,
    )
    results: MutableSequence["MutateUserListCustomerTypeResult"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="MutateUserListCustomerTypeResult",
        )
    )


class MutateUserListCustomerTypeResult(proto.Message):
    r"""The result for the user list customer type mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

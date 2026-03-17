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

from google.ads.googleads.v20.enums.types import (
    user_list_customer_type_category,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v20.resources",
    marshal="google.ads.googleads.v20",
    manifest={
        "UserListCustomerType",
    },
)


class UserListCustomerType(proto.Message):
    r"""A user list customer type

    Attributes:
        resource_name (str):
            Immutable. The resource name of the user list customer type
            User list customer type resource names have the form:
            ``customers/{customer_id}/userListCustomerTypes/{user_list_id}~{customer_type_category}``
        user_list (str):
            Immutable. The resource name for the user
            list this user list customer type is associated
            with
        customer_type_category (google.ads.googleads.v20.enums.types.UserListCustomerTypeCategoryEnum.UserListCustomerTypeCategory):
            Immutable. The user list customer type
            category
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    user_list: str = proto.Field(
        proto.STRING,
        number=2,
    )
    customer_type_category: (
        user_list_customer_type_category.UserListCustomerTypeCategoryEnum.UserListCustomerTypeCategory
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=user_list_customer_type_category.UserListCustomerTypeCategoryEnum.UserListCustomerTypeCategory,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

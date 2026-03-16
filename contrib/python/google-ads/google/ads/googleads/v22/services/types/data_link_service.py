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

from google.ads.googleads.v22.enums.types import (
    data_link_status as gage_data_link_status,
)
from google.ads.googleads.v22.resources.types import data_link as gagr_data_link


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.services",
    marshal="google.ads.googleads.v22",
    manifest={
        "CreateDataLinkRequest",
        "CreateDataLinkResponse",
        "RemoveDataLinkRequest",
        "RemoveDataLinkResponse",
        "UpdateDataLinkRequest",
        "UpdateDataLinkResponse",
    },
)


class CreateDataLinkRequest(proto.Message):
    r"""Request message for
    [DataLinkService.CreateDataLink][google.ads.googleads.v22.services.DataLinkService.CreateDataLink].

    Attributes:
        customer_id (str):
            Required. The ID of the customer for which
            the data link is created.
        data_link (google.ads.googleads.v22.resources.types.DataLink):
            Required. The data link to be created.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    data_link: gagr_data_link.DataLink = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_data_link.DataLink,
    )


class CreateDataLinkResponse(proto.Message):
    r"""Response message for
    [DataLinkService.CreateDataLink][google.ads.googleads.v22.services.DataLinkService.CreateDataLink].

    Attributes:
        resource_name (str):
            Returned for successful operations. Resource
            name of the data link.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


class RemoveDataLinkRequest(proto.Message):
    r"""Request message for
    [DataLinkService.RemoveDataLink][google.ads.googleads.v22.services.DataLinkService.RemoveDataLink].

    Attributes:
        customer_id (str):
            Required. The ID of the customer for which
            the data link is updated.
        resource_name (str):
            Required. The data link is expected to have a
            valid resource name.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    resource_name: str = proto.Field(
        proto.STRING,
        number=2,
    )


class RemoveDataLinkResponse(proto.Message):
    r"""Response message for
    [DataLinkService.RemoveDataLink][google.ads.googleads.v22.services.DataLinkService.RemoveDataLink].

    Attributes:
        resource_name (str):
            Result for the remove request.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


class UpdateDataLinkRequest(proto.Message):
    r"""Request message for
    [DataLinkService.UpdateDataLink][google.ads.googleads.v22.services.DataLinkService.UpdateDataLink].

    Attributes:
        customer_id (str):
            Required. The ID of the customer for which
            the data link is created.
        data_link_status (google.ads.googleads.v22.enums.types.DataLinkStatusEnum.DataLinkStatus):
            Required. The data link status to be updated
            to.
        resource_name (str):
            Required. The data link is expected to have a
            valid resource name.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    data_link_status: (
        gage_data_link_status.DataLinkStatusEnum.DataLinkStatus
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_data_link_status.DataLinkStatusEnum.DataLinkStatus,
    )
    resource_name: str = proto.Field(
        proto.STRING,
        number=3,
    )


class UpdateDataLinkResponse(proto.Message):
    r"""Response message for
    [DataLinkService.UpdateDataLink][google.ads.googleads.v22.services.DataLinkService.UpdateDataLink].

    Attributes:
        resource_name (str):
            Returned for successful operations. Resource
            name of the data link.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


__all__ = tuple(sorted(__protobuf__.manifest))

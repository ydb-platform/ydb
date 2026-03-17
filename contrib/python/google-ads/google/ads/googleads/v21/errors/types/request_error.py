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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "RequestErrorEnum",
    },
)


class RequestErrorEnum(proto.Message):
    r"""Container for enum describing possible request errors."""

    class RequestError(proto.Enum):
        r"""Enum describing possible request errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            RESOURCE_NAME_MISSING (3):
                Resource name is required for this request.
            RESOURCE_NAME_MALFORMED (4):
                Resource name provided is malformed.
            BAD_RESOURCE_ID (17):
                Resource name provided is malformed.
            INVALID_CUSTOMER_ID (16):
                Customer ID is invalid.
            OPERATION_REQUIRED (5):
                Mutate operation should have either create,
                update, or remove specified.
            RESOURCE_NOT_FOUND (6):
                Requested resource not found.
            INVALID_PAGE_TOKEN (7):
                Next page token specified in user request is
                invalid.
            EXPIRED_PAGE_TOKEN (8):
                Next page token specified in user request has
                expired.
            INVALID_PAGE_SIZE (22):
                Page size specified in user request is
                invalid.
            PAGE_SIZE_NOT_SUPPORTED (40):
                Setting the page size is not supported, and
                will be unavailable in a future version.
            REQUIRED_FIELD_MISSING (9):
                Required field is missing.
            IMMUTABLE_FIELD (11):
                The field cannot be modified because it's
                immutable. It's also possible that the field can
                be modified using 'create' operation but not
                'update'.
            TOO_MANY_MUTATE_OPERATIONS (13):
                Received too many entries in request.
            CANNOT_BE_EXECUTED_BY_MANAGER_ACCOUNT (14):
                Request cannot be executed by a manager
                account.
            CANNOT_MODIFY_FOREIGN_FIELD (15):
                Mutate request was attempting to modify a
                readonly field. For instance, Budget fields can
                be requested for Ad Group, but are read-only for
                adGroups:mutate.
            INVALID_ENUM_VALUE (18):
                Enum value is not permitted.
            DEVELOPER_TOKEN_PARAMETER_MISSING (19):
                The developer-token parameter is required for
                all requests.
            LOGIN_CUSTOMER_ID_PARAMETER_MISSING (20):
                The login-customer-id parameter is required
                for this request.
            VALIDATE_ONLY_REQUEST_HAS_PAGE_TOKEN (21):
                page_token is set in the validate only request
            CANNOT_RETURN_SUMMARY_ROW_FOR_REQUEST_WITHOUT_METRICS (29):
                return_summary_row cannot be enabled if request did not
                select any metrics field.
            CANNOT_RETURN_SUMMARY_ROW_FOR_VALIDATE_ONLY_REQUESTS (30):
                return_summary_row should not be enabled for validate only
                requests.
            INCONSISTENT_RETURN_SUMMARY_ROW_VALUE (31):
                return_summary_row parameter value should be the same
                between requests with page_token field set and their
                original request.
            TOTAL_RESULTS_COUNT_NOT_ORIGINALLY_REQUESTED (32):
                The total results count cannot be returned if
                it was not requested in the original request.
            RPC_DEADLINE_TOO_SHORT (33):
                Deadline specified by the client was too
                short.
            UNSUPPORTED_VERSION (38):
                This API version has been sunset and is no
                longer supported.
            CLOUD_PROJECT_NOT_FOUND (39):
                The Google Cloud project in the request was
                not found.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        RESOURCE_NAME_MISSING = 3
        RESOURCE_NAME_MALFORMED = 4
        BAD_RESOURCE_ID = 17
        INVALID_CUSTOMER_ID = 16
        OPERATION_REQUIRED = 5
        RESOURCE_NOT_FOUND = 6
        INVALID_PAGE_TOKEN = 7
        EXPIRED_PAGE_TOKEN = 8
        INVALID_PAGE_SIZE = 22
        PAGE_SIZE_NOT_SUPPORTED = 40
        REQUIRED_FIELD_MISSING = 9
        IMMUTABLE_FIELD = 11
        TOO_MANY_MUTATE_OPERATIONS = 13
        CANNOT_BE_EXECUTED_BY_MANAGER_ACCOUNT = 14
        CANNOT_MODIFY_FOREIGN_FIELD = 15
        INVALID_ENUM_VALUE = 18
        DEVELOPER_TOKEN_PARAMETER_MISSING = 19
        LOGIN_CUSTOMER_ID_PARAMETER_MISSING = 20
        VALIDATE_ONLY_REQUEST_HAS_PAGE_TOKEN = 21
        CANNOT_RETURN_SUMMARY_ROW_FOR_REQUEST_WITHOUT_METRICS = 29
        CANNOT_RETURN_SUMMARY_ROW_FOR_VALIDATE_ONLY_REQUESTS = 30
        INCONSISTENT_RETURN_SUMMARY_ROW_VALUE = 31
        TOTAL_RESULTS_COUNT_NOT_ORIGINALLY_REQUESTED = 32
        RPC_DEADLINE_TOO_SHORT = 33
        UNSUPPORTED_VERSION = 38
        CLOUD_PROJECT_NOT_FOUND = 39


__all__ = tuple(sorted(__protobuf__.manifest))

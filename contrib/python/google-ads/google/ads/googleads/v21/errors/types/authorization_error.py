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
        "AuthorizationErrorEnum",
    },
)


class AuthorizationErrorEnum(proto.Message):
    r"""Container for enum describing possible authorization errors."""

    class AuthorizationError(proto.Enum):
        r"""Enum describing possible authorization errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            USER_PERMISSION_DENIED (2):
                User doesn't have permission to access customer. Note: If
                you're accessing a client customer, the manager's customer
                ID must be set in the ``login-customer-id`` header. Learn
                more at
                https://developers.google.com/google-ads/api/docs/concepts/call-structure#cid
            DEVELOPER_TOKEN_NOT_ON_ALLOWLIST (13):
                The developer token is not on the allow-list.
            DEVELOPER_TOKEN_PROHIBITED (4):
                The developer token is not allowed with the
                project sent in the request.
            PROJECT_DISABLED (5):
                The Google Cloud project sent in the request
                does not have permission to access the api.
            AUTHORIZATION_ERROR (6):
                Authorization of the client failed.
            ACTION_NOT_PERMITTED (7):
                The user does not have permission to perform
                this action (for example, ADD, UPDATE, REMOVE)
                on the resource or call a method.
            INCOMPLETE_SIGNUP (8):
                Signup not complete.
            CUSTOMER_NOT_ENABLED (24):
                The customer account can't be accessed
                because it is not yet enabled or has been
                deactivated.
            MISSING_TOS (9):
                The developer must sign the terms of service.
                They can be found here:
                ads.google.com/aw/apicenter
            DEVELOPER_TOKEN_NOT_APPROVED (10):
                The developer token is only approved for use
                with test accounts. To access non-test accounts,
                apply for Basic or Standard access.
            INVALID_LOGIN_CUSTOMER_ID_SERVING_CUSTOMER_ID_COMBINATION (11):
                The login customer specified does not have
                access to the account specified, so the request
                is invalid.
            SERVICE_ACCESS_DENIED (12):
                The developer specified does not have access
                to the service.
            ACCESS_DENIED_FOR_ACCOUNT_TYPE (25):
                The customer (or login customer) isn't in
                Google Ads. It belongs to another ads system.
            METRIC_ACCESS_DENIED (26):
                The developer does not have access to the
                metrics queried.
            CLOUD_PROJECT_NOT_UNDER_ORGANIZATION (27):
                The Google Cloud project is not under the
                required organization.
            ACTION_NOT_PERMITTED_FOR_SUSPENDED_ACCOUNT (28):
                The user does not have permission to perform
                this action on the resource or method because
                the Google Ads account is suspended.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        USER_PERMISSION_DENIED = 2
        DEVELOPER_TOKEN_NOT_ON_ALLOWLIST = 13
        DEVELOPER_TOKEN_PROHIBITED = 4
        PROJECT_DISABLED = 5
        AUTHORIZATION_ERROR = 6
        ACTION_NOT_PERMITTED = 7
        INCOMPLETE_SIGNUP = 8
        CUSTOMER_NOT_ENABLED = 24
        MISSING_TOS = 9
        DEVELOPER_TOKEN_NOT_APPROVED = 10
        INVALID_LOGIN_CUSTOMER_ID_SERVING_CUSTOMER_ID_COMBINATION = 11
        SERVICE_ACCESS_DENIED = 12
        ACCESS_DENIED_FOR_ACCOUNT_TYPE = 25
        METRIC_ACCESS_DENIED = 26
        CLOUD_PROJECT_NOT_UNDER_ORGANIZATION = 27
        ACTION_NOT_PERMITTED_FOR_SUSPENDED_ACCOUNT = 28


__all__ = tuple(sorted(__protobuf__.manifest))

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
        "AuthenticationErrorEnum",
    },
)


class AuthenticationErrorEnum(proto.Message):
    r"""Container for enum describing possible authentication errors."""

    class AuthenticationError(proto.Enum):
        r"""Enum describing possible authentication errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            AUTHENTICATION_ERROR (2):
                Authentication of the request failed.
            CLIENT_CUSTOMER_ID_INVALID (5):
                Client customer ID is not a number.
            CUSTOMER_NOT_FOUND (8):
                No customer found for the provided customer
                ID.
            GOOGLE_ACCOUNT_DELETED (9):
                Client's Google account is deleted.
            GOOGLE_ACCOUNT_COOKIE_INVALID (10):
                Google account login token in the cookie is
                invalid.
            GOOGLE_ACCOUNT_AUTHENTICATION_FAILED (25):
                A problem occurred during Google account
                authentication.
            GOOGLE_ACCOUNT_USER_AND_ADS_USER_MISMATCH (12):
                The user in the Google account login token
                does not match the user ID in the cookie.
            LOGIN_COOKIE_REQUIRED (13):
                Login cookie is required for authentication.
            NOT_ADS_USER (14):
                The Google account that generated the OAuth
                access token is not associated with a Google Ads
                account. Create a new account, or add the Google
                account to an existing Google Ads account.
            OAUTH_TOKEN_INVALID (15):
                OAuth token in the header is not valid.
            OAUTH_TOKEN_EXPIRED (16):
                OAuth token in the header has expired.
            OAUTH_TOKEN_DISABLED (17):
                OAuth token in the header has been disabled.
            OAUTH_TOKEN_REVOKED (18):
                OAuth token in the header has been revoked.
            OAUTH_TOKEN_HEADER_INVALID (19):
                OAuth token HTTP header is malformed.
            LOGIN_COOKIE_INVALID (20):
                Login cookie is not valid.
            USER_ID_INVALID (22):
                User ID in the header is not a valid ID.
            TWO_STEP_VERIFICATION_NOT_ENROLLED (23):
                An account administrator changed this
                account's authentication settings. To access
                this Google Ads account, enable 2-Step
                Verification in your Google account at
                https://www.google.com/landing/2step.
            ADVANCED_PROTECTION_NOT_ENROLLED (24):
                An account administrator changed this
                account's authentication settings. To access
                this Google Ads account, enable Advanced
                Protection in your Google account at
                https://landing.google.com/advancedprotection.
            ORGANIZATION_NOT_RECOGNIZED (26):
                The Cloud organization associated with the
                project is not recognized.
            ORGANIZATION_NOT_APPROVED (27):
                The Cloud organization associated with the
                project is not approved for prod access.
            ORGANIZATION_NOT_ASSOCIATED_WITH_DEVELOPER_TOKEN (28):
                The Cloud organization associated with the
                project is not associated with the developer
                token.
            DEVELOPER_TOKEN_INVALID (29):
                The developer token is not valid.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AUTHENTICATION_ERROR = 2
        CLIENT_CUSTOMER_ID_INVALID = 5
        CUSTOMER_NOT_FOUND = 8
        GOOGLE_ACCOUNT_DELETED = 9
        GOOGLE_ACCOUNT_COOKIE_INVALID = 10
        GOOGLE_ACCOUNT_AUTHENTICATION_FAILED = 25
        GOOGLE_ACCOUNT_USER_AND_ADS_USER_MISMATCH = 12
        LOGIN_COOKIE_REQUIRED = 13
        NOT_ADS_USER = 14
        OAUTH_TOKEN_INVALID = 15
        OAUTH_TOKEN_EXPIRED = 16
        OAUTH_TOKEN_DISABLED = 17
        OAUTH_TOKEN_REVOKED = 18
        OAUTH_TOKEN_HEADER_INVALID = 19
        LOGIN_COOKIE_INVALID = 20
        USER_ID_INVALID = 22
        TWO_STEP_VERIFICATION_NOT_ENROLLED = 23
        ADVANCED_PROTECTION_NOT_ENROLLED = 24
        ORGANIZATION_NOT_RECOGNIZED = 26
        ORGANIZATION_NOT_APPROVED = 27
        ORGANIZATION_NOT_ASSOCIATED_WITH_DEVELOPER_TOKEN = 28
        DEVELOPER_TOKEN_INVALID = 29


__all__ = tuple(sorted(__protobuf__.manifest))

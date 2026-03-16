#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

try:  # pragma: no cover
    from snowflake.connector.auth import AuthByOAuth
except ImportError:
    from snowflake.connector.auth_oauth import AuthByOAuth


def test_auth_oauth():
    """Simple OAuth test."""
    token = "oAuthToken"
    auth = AuthByOAuth(token)
    body = {"data": {}}
    auth.update_body(body)
    assert body["data"]["TOKEN"] == token, body
    assert body["data"]["AUTHENTICATOR"] == "OAUTH", body

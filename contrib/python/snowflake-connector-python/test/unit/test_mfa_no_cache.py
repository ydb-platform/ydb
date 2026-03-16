#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

import snowflake.connector
from snowflake.connector.compat import IS_LINUX

try:
    from snowflake.connector.options import installed_keyring
except ImportError:
    # if installed_keyring is unavailable, we set it as True to skip the test
    installed_keyring = True
try:
    from snowflake.connector.auth import delete_temporary_credential
except ImportError:
    delete_temporary_credential = None

MFA_TOKEN = "MFATOKEN"


@pytest.mark.skipif(
    IS_LINUX or installed_keyring or not delete_temporary_credential,
    reason="Required test env is Mac/Win with no pre-installed keyring package"
    "and available delete_temporary_credential.",
)
@patch("snowflake.connector.network.SnowflakeRestful._post_request")
def test_mfa_no_local_secure_storage(mockSnowflakeRestfulPostRequest):
    """Test whether username_password_mfa authenticator can work when no local secure storage is available."""
    global mock_post_req_cnt
    mock_post_req_cnt = 0

    # This test requires Mac/Win and no keyring lib is installed
    assert not installed_keyring

    def mock_post_request(url, headers, json_body, **kwargs):
        global mock_post_req_cnt
        ret = None
        body = json.loads(json_body)
        if mock_post_req_cnt == 0:
            # issue MFA token for a succeeded login
            assert (
                body["data"]["SESSION_PARAMETERS"].get("CLIENT_REQUEST_MFA_TOKEN")
                is True
            )
            ret = {
                "success": True,
                "message": None,
                "data": {
                    "token": "TOKEN",
                    "masterToken": "MASTER_TOKEN",
                    "mfaToken": "MFA_TOKEN",
                },
            }
        elif mock_post_req_cnt == 2:
            # No local secure storage available, so no mfa cache token should be provided
            assert (
                body["data"]["SESSION_PARAMETERS"].get("CLIENT_REQUEST_MFA_TOKEN")
                is True
            )
            assert "TOKEN" not in body["data"]
            ret = {
                "success": True,
                "message": None,
                "data": {
                    "token": "NEW_TOKEN",
                    "masterToken": "NEW_MASTER_TOKEN",
                },
            }
        elif mock_post_req_cnt in [1, 3]:
            # connection.close()
            ret = {"success": True}
        mock_post_req_cnt += 1
        return ret

    # POST requests mock
    mockSnowflakeRestfulPostRequest.side_effect = mock_post_request

    conn_cfg = {
        "account": "testaccount",
        "user": "testuser",
        "password": "testpwd",
        "authenticator": "username_password_mfa",
        "host": "testaccount.snowflakecomputing.com",
    }

    delete_temporary_credential(
        host=conn_cfg["host"], user=conn_cfg["user"], cred_type=MFA_TOKEN
    )

    # first connection, no mfa token cache
    con = snowflake.connector.connect(**conn_cfg)
    assert con._rest.token == "TOKEN"
    assert con._rest.master_token == "MASTER_TOKEN"
    assert con._rest.mfa_token == "MFA_TOKEN"
    con.close()

    # second connection, no mfa token should be issued as well since no available local secure storage
    con = snowflake.connector.connect(**conn_cfg)
    assert con._rest.token == "NEW_TOKEN"
    assert con._rest.master_token == "NEW_MASTER_TOKEN"
    assert not con._rest.mfa_token
    con.close()

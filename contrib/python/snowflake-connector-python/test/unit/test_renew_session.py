#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from unittest.mock import MagicMock, Mock, PropertyMock

from snowflake.connector.network import SnowflakeRestful


def test_renew_session():
    OLD_SESSION_TOKEN = "old_session_token"
    OLD_MASTER_TOKEN = "old_master_token"
    NEW_SESSION_TOKEN = "new_session_token"
    NEW_MASTER_TOKEN = "new_master_token"
    connection = MagicMock()
    connection.errorhandler = Mock(return_value=None)
    type(connection)._probe_connection = PropertyMock(return_value=False)

    rest = SnowflakeRestful(
        host="testaccount.snowflakecomputing.com", port=443, connection=connection
    )
    rest._token = OLD_SESSION_TOKEN
    rest._master_token = OLD_MASTER_TOKEN

    # inject a fake method (success)
    def fake_request_exec(**_):
        return {
            "success": True,
            "data": {
                "sessionToken": NEW_SESSION_TOKEN,
                "masterToken": NEW_MASTER_TOKEN,
            },
        }

    rest._request_exec = fake_request_exec

    rest._renew_session()
    assert not rest._connection.errorhandler.called  # no error
    assert rest.master_token == NEW_MASTER_TOKEN
    assert rest.token == NEW_SESSION_TOKEN

    # inject a fake method (failure)
    def fake_request_exec(**_):
        return {"success": False, "message": "failed to renew session", "code": 987654}

    rest._request_exec = fake_request_exec

    rest._renew_session()
    assert rest._connection.errorhandler.called  # error

    # no master token
    del rest._master_token
    rest._renew_session()
    assert rest._connection.errorhandler.called  # error

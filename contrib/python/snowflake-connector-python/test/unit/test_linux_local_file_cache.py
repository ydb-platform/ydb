#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import os

import pytest

import snowflake.connector.auth as auth
from snowflake.connector.compat import IS_LINUX

HOST_0 = "host_0"
HOST_1 = "host_1"
USER_0 = "user_0"
USER_1 = "user_1"
CRED_0 = "cred_0"
CRED_1 = "cred_1"

CRED_TYPE_0 = "ID_TOKEN"
CRED_TYPE_1 = "MFA_TOKEN"


def get_credential(sys, user):
    return auth._auth.TEMPORARY_CREDENTIAL.get(sys.upper(), {}).get(user.upper())


@pytest.mark.skipif(not IS_LINUX, reason="The test is only for Linux platform")
def test_basic_store(tmpdir):
    os.environ["SF_TEMPORARY_CREDENTIAL_CACHE_DIR"] = str(tmpdir)

    auth._auth.delete_temporary_credential_file()
    auth._auth.TEMPORARY_CREDENTIAL.clear()

    auth._auth.read_temporary_credential_file()
    assert not auth._auth.TEMPORARY_CREDENTIAL

    auth._auth.write_temporary_credential_file(HOST_0, USER_0, CRED_0)
    auth._auth.write_temporary_credential_file(HOST_1, USER_1, CRED_1)
    auth._auth.write_temporary_credential_file(HOST_0, USER_1, CRED_1)

    auth._auth.read_temporary_credential_file()
    assert auth._auth.TEMPORARY_CREDENTIAL
    assert get_credential(HOST_0, USER_0) == CRED_0
    assert get_credential(HOST_1, USER_1) == CRED_1
    assert get_credential(HOST_0, USER_1) == CRED_1

    auth._auth.delete_temporary_credential_file()


def test_delete_specific_item():
    """The old behavior of delete cache is deleting the whole cache file. Now we change it to partially deletion."""
    auth._auth.write_temporary_credential_file(
        HOST_0,
        auth._auth.build_temporary_credential_name(HOST_0, USER_0, CRED_TYPE_0),
        CRED_0,
    )
    auth._auth.write_temporary_credential_file(
        HOST_0,
        auth._auth.build_temporary_credential_name(HOST_0, USER_0, CRED_TYPE_1),
        CRED_1,
    )
    auth._auth.read_temporary_credential_file()

    assert auth._auth.TEMPORARY_CREDENTIAL
    assert (
        get_credential(
            HOST_0,
            auth._auth.build_temporary_credential_name(HOST_0, USER_0, CRED_TYPE_0),
        )
        == CRED_0
    )
    assert (
        get_credential(
            HOST_0,
            auth._auth.build_temporary_credential_name(HOST_0, USER_0, CRED_TYPE_1),
        )
        == CRED_1
    )

    auth._auth.temporary_credential_file_delete_password(HOST_0, USER_0, CRED_TYPE_0)
    auth._auth.read_temporary_credential_file()
    assert not get_credential(
        HOST_0, auth._auth.build_temporary_credential_name(HOST_0, USER_0, CRED_TYPE_0)
    )
    assert (
        get_credential(
            HOST_0,
            auth._auth.build_temporary_credential_name(HOST_0, USER_0, CRED_TYPE_1),
        )
        == CRED_1
    )

    auth._auth.delete_temporary_credential_file()

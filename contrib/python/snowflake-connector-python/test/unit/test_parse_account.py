#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from snowflake.connector.util_text import parse_account


def test_parse_account_basic():
    assert parse_account("account1") == "account1"

    assert parse_account("account1.eu-central-1") == "account1"

    assert (
        parse_account("account1-jkabfvdjisoa778wqfgeruishafeuw89q.global") == "account1"
    )

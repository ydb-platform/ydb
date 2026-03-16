#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import urllib.parse

from snowflake.sqlalchemy import URL


def test_url():
    assert (
        URL(account="testaccount", user="admin", password="test", warehouse="testwh")
        == "snowflake://admin:test@testaccount/?warehouse=testwh"
    )

    assert (
        URL(account="testaccount", user="admin", password="test")
        == "snowflake://admin:test@testaccount/"
    )

    assert (
        URL(
            account="testaccount",
            user="admin",
            password="1-pass 2-pass 3-: 4-@ 5-/ 6-pass",
        )
        == "snowflake://admin:1-pass 2-pass 3-%3A 4-%40 5-%2F 6-pass@testaccount/"
    )

    quoted_password = urllib.parse.quote("kx@% jj5/g")
    assert (
        URL(
            account="testaccount",
            user="admin",
            password=quoted_password,
        )
        == "snowflake://admin:kx%40%25%20jj5%2Fg@testaccount/"
    )

    assert (
        URL(account="testaccount", user="admin", password="test", database="testdb")
        == "snowflake://admin:test@testaccount/testdb"
    )

    assert (
        URL(
            account="testaccount",
            user="admin",
            password="test",
            database="testdb",
            schema="testschema",
        )
        == "snowflake://admin:test@testaccount/testdb/testschema"
    )

    assert (
        URL(
            account="testaccount",
            user="admin",
            password="test",
            database="testdb",
            schema="testschema",
            warehouse="testwh",
        )
        == "snowflake://admin:test@testaccount/testdb/testschema?warehouse"
        "=testwh"
    )

    assert (
        URL(
            host="snowflake.reg.local",
            account="testaccount",
            user="admin",
            password="test",
            database="testdb",
            schema="testschema",
        )
        == "snowflake://admin:test@snowflake.reg.local:443/testdb"
        "/testschema?account=testaccount"
    )

    assert URL(
        user="admin", account="testaccount", password="test", region="eu-central-1"
    ) == ("snowflake://admin:test@testaccount.eu-central-1/")

    assert URL(
        user="admin",
        account="testaccount",
        password="test",
        region="eu-central-1.azure",
    ) == ("snowflake://admin:test@testaccount.eu-central-1.azure/")

    assert URL(
        host="testaccount.eu-central-1.snowflakecomputing.com",
        user="admin",
        account="testaccount",
        password="test",
    ) == (
        "snowflake://admin:test@testaccount.eu-central-1"
        ".snowflakecomputing.com:443/?account=testaccount"
    )

    # empty password should be acceptable in URL utility. The validation will
    # happen in Python connector anyway.
    assert URL(
        host="testaccount.eu-central-1.snowflakecomputing.com",
        user="admin",
        account="testaccount",
    ) == (
        "snowflake://admin:@testaccount.eu-central-1"
        ".snowflakecomputing.com:443/?account=testaccount"
    )

    # authenticator=externalbrowser doesn't require a password.
    assert URL(
        host="testaccount.eu-central-1.snowflakecomputing.com",
        user="admin",
        account="testaccount",
        authenticator="externalbrowser",
    ) == (
        "snowflake://admin:@testaccount.eu-central-1"
        ".snowflakecomputing.com:443/?account=testaccount"
        "&authenticator=externalbrowser"
    )

    # authenticator=oktaurl support
    assert URL(
        user="testuser",
        account="testaccount",
        password="test",
        authenticator="https://testokta.okta.com",
    ) == (
        "snowflake://testuser:test@testaccount"
        "/?authenticator=https%3A%2F%2Ftestokta.okta.com"
    )

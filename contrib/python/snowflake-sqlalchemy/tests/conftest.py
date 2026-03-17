#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import os
import sys
import time
import uuid
from logging import getLogger

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

import snowflake.connector
import snowflake.connector.connection
from snowflake.connector.compat import IS_WINDOWS
from snowflake.sqlalchemy import URL, dialect
from snowflake.sqlalchemy._constants import (
    APPLICATION_NAME,
    PARAM_APPLICATION,
    PARAM_INTERNAL_APPLICATION_NAME,
    PARAM_INTERNAL_APPLICATION_VERSION,
    SNOWFLAKE_SQLALCHEMY_VERSION,
)

CONNECTION_PARAMETERS = {
    'account':  'testaccount',
    'user':     'user1',
    'password': 'testpasswd',
    'schema':   'testschema',
    'database': 'testdb',
}

CLOUD_PROVIDERS = {"aws", "azure", "gcp"}
EXTERNAL_SKIP_TAGS = {"internal"}
INTERNAL_SKIP_TAGS = {"external"}
RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"

snowflake.connector.connection.DEFAULT_CONFIGURATION[PARAM_APPLICATION] = (
    APPLICATION_NAME,
    (type(None), str),
)
snowflake.connector.connection.DEFAULT_CONFIGURATION[
    PARAM_INTERNAL_APPLICATION_NAME
] = (APPLICATION_NAME, (type(None), str))
snowflake.connector.connection.DEFAULT_CONFIGURATION[
    PARAM_INTERNAL_APPLICATION_VERSION
] = (SNOWFLAKE_SQLALCHEMY_VERSION, (type(None), str))

TEST_SCHEMA = f"sqlalchemy_tests_{str(uuid.uuid4()).replace('-', '_')}"


@pytest.fixture(scope="session")
def on_travis():
    return os.getenv("TRAVIS", "").lower() == "true"


@pytest.fixture(scope="session")
def on_appveyor():
    return os.getenv("APPVEYOR", "").lower() == "true"


@pytest.fixture(scope="session")
def on_public_ci(on_travis, on_appveyor):
    return on_travis or on_appveyor


def help():
    print(
        """Connection parameter must be specified in parameters.py,
    for example:
CONNECTION_PARAMETERS = {
    'account': 'testaccount',
    'user': 'user1',
    'password': 'test',
    'database': 'testdb',
    'schema': 'public',
}"""
    )


logger = getLogger(__name__)

DEFAULT_PARAMETERS = {
    "account": "<account_name>",
    "user": "<user_name>",
    "password": "<password>",
    "database": "<database_name>",
    "schema": "<schema_name>",
    "protocol": "https",
    "host": "<host>",
    "port": "443",
}


@pytest.fixture(scope="session")
def db_parameters():
    yield get_db_parameters()


def get_db_parameters() -> dict:
    """
    Sets the db connection parameters
    """
    ret = {}
    os.environ["TZ"] = "UTC"
    if not IS_WINDOWS:
        time.tzset()

    ret.update(DEFAULT_PARAMETERS)
    ret.update(CONNECTION_PARAMETERS)

    if "account" in ret and ret["account"] == DEFAULT_PARAMETERS["account"]:
        help()
        sys.exit(2)

    if "host" in ret and ret["host"] == DEFAULT_PARAMETERS["host"]:
        ret["host"] = ret["account"] + ".snowflakecomputing.com"

    # a unique table name
    ret["name"] = ("sqlalchemy_tests_" + str(uuid.uuid4())).replace("-", "_")
    ret["schema"] = TEST_SCHEMA

    # This reduces a chance to exposing password in test output.
    ret["a00"] = "dummy parameter"
    ret["a01"] = "dummy parameter"
    ret["a02"] = "dummy parameter"
    ret["a03"] = "dummy parameter"
    ret["a04"] = "dummy parameter"
    ret["a05"] = "dummy parameter"
    ret["a06"] = "dummy parameter"
    ret["a07"] = "dummy parameter"
    ret["a08"] = "dummy parameter"
    ret["a09"] = "dummy parameter"
    ret["a10"] = "dummy parameter"
    ret["a11"] = "dummy parameter"
    ret["a12"] = "dummy parameter"
    ret["a13"] = "dummy parameter"
    ret["a14"] = "dummy parameter"
    ret["a15"] = "dummy parameter"
    ret["a16"] = "dummy parameter"

    return ret


def url_factory(**kwargs) -> URL:
    url_params = get_db_parameters()
    url_params.update(kwargs)
    return URL(**url_params)


def get_engine(url: URL, **engine_kwargs):
    engine_params = {
        "poolclass": NullPool,
        "future": True,
        "echo": True,
    }
    engine_params.update(engine_kwargs)
    engine = create_engine(url, **engine_params)
    return engine


@pytest.fixture()
def engine_testaccount(request):
    url = url_factory()
    engine = get_engine(url)
    request.addfinalizer(engine.dispose)
    yield engine


@pytest.fixture()
def engine_testaccount_with_numpy(request):
    url = url_factory(numpy=True)
    engine = get_engine(url)
    request.addfinalizer(engine.dispose)
    yield engine


@pytest.fixture()
def engine_testaccount_with_qmark(request):
    snowflake.connector.paramstyle = "qmark"

    url = url_factory()
    engine = get_engine(url)
    request.addfinalizer(engine.dispose)

    yield engine

    snowflake.connector.paramstyle = "pyformat"


# @pytest.fixture(scope="session", autouse=True)
def init_test_schema(request, db_parameters):
    ret = db_parameters
    with snowflake.connector.connect(
        user=ret["user"],
        password=ret["password"],
        host=ret["host"],
        port=ret["port"],
        database=ret["database"],
        account=ret["account"],
        protocol=ret["protocol"],
    ) as con:
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")

    def fin():
        ret1 = db_parameters
        with snowflake.connector.connect(
            user=ret1["user"],
            password=ret1["password"],
            host=ret1["host"],
            port=ret1["port"],
            database=ret1["database"],
            account=ret1["account"],
            protocol=ret1["protocol"],
        ) as con1:
            con1.cursor().execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA}")

    request.addfinalizer(fin)


@pytest.fixture(scope="session")
def sql_compiler():
    return lambda sql_command: str(
        sql_command.compile(
            dialect=dialect(),
            compile_kwargs={"literal_binds": True, "deterministic": True},
        )
    ).replace("\n", "")


def running_on_public_ci() -> bool:
    """Whether or not tests are currently running on one of our public CIs."""
    return os.getenv("GITHUB_ACTIONS") == "true"


def pytest_runtest_setup(item) -> None:
    """Ran before calling each test, used to decide whether a test should be skipped."""
    test_tags = [mark.name for mark in item.iter_markers()]

    # Get what cloud providers the test is marked for if any
    test_supported_providers = CLOUD_PROVIDERS.intersection(test_tags)
    # Default value means that we are probably running on a developer's machine, allow everything in this case
    current_provider = os.getenv("cloud_provider", "dev")
    if test_supported_providers:
        # If test is tagged for specific cloud providers add the default cloud_provider as supported too
        test_supported_providers.add("dev")
        if current_provider not in test_supported_providers:
            pytest.skip(
                f"cannot run unit test against cloud provider {current_provider}"
            )
    if EXTERNAL_SKIP_TAGS.intersection(test_tags) and running_on_public_ci():
        pytest.skip("cannot run this test on external CI")
    elif INTERNAL_SKIP_TAGS.intersection(test_tags) and not running_on_public_ci():
        pytest.skip("cannot run this test on internal CI")

#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import decimal
import json
import os
import re
import string
import textwrap
import time
from datetime import date, datetime
from unittest.mock import patch

import pytest
import pytz
from sqlalchemy import (
    REAL,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    Sequence,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    dialects,
    insert,
    inspect,
    text,
)
from sqlalchemy.exc import DBAPIError, NoSuchTableError, OperationalError
from sqlalchemy.sql import and_, not_, or_, select

import snowflake.connector.errors
import snowflake.sqlalchemy.snowdialect
from snowflake.connector import Error, ProgrammingError, connect
from snowflake.sqlalchemy import URL, MergeInto, dialect
from snowflake.sqlalchemy._constants import (
    APPLICATION_NAME,
    SNOWFLAKE_SQLALCHEMY_VERSION,
)
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from .conftest import get_engine, url_factory
from .parameters import CONNECTION_PARAMETERS
from .util import ischema_names_baseline, random_string

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

PST_TZ = "America/Los_Angeles"
JST_TZ = "Asia/Tokyo"


def _create_users_addresses_tables(
    engine_testaccount, metadata, fk=None, pk=None, uq=None
):
    users = Table(
        "users",
        metadata,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )

    addresses = Table(
        "addresses",
        metadata,
        Column("id", Integer, Sequence("address_id_seq")),
        Column("user_id", None, ForeignKey("users.id", name=fk)),
        Column("email_address", String, nullable=False),
        PrimaryKeyConstraint("id", name=pk),
        UniqueConstraint("email_address", name=uq),
    )
    metadata.create_all(engine_testaccount)
    return users, addresses


def _create_users_addresses_tables_without_sequence(engine_testaccount, metadata):
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )

    addresses = Table(
        "addresses",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", None, ForeignKey("users.id")),
        Column("email_address", String, nullable=False),
    )
    metadata.create_all(engine_testaccount)
    return users, addresses


def verify_engine_connection(engine):
    with engine.connect() as conn:
        results = conn.execute(text("select current_version()")).fetchone()
        assert conn.connection.driver_connection.application == APPLICATION_NAME
        assert (
            conn.connection.driver_connection._internal_application_name
            == APPLICATION_NAME
        )
        assert (
            conn.connection.driver_connection._internal_application_version
            == SNOWFLAKE_SQLALCHEMY_VERSION
        )
        assert results is not None


def test_connect_args():
    """
    Tests connect string

    Snowflake connect string supports account name as a replacement of
    host:port
    """
    engine = create_engine(
        "snowflake://{user}:{password}@{host}:{port}/{database}/{schema}"
        "?account={account}&protocol={protocol}".format(
            user=CONNECTION_PARAMETERS["user"],
            account=CONNECTION_PARAMETERS["account"],
            password=CONNECTION_PARAMETERS["password"],
            host=CONNECTION_PARAMETERS["host"],
            port=CONNECTION_PARAMETERS["port"],
            database=CONNECTION_PARAMETERS["database"],
            schema=CONNECTION_PARAMETERS["schema"],
            protocol=CONNECTION_PARAMETERS["protocol"],
        )
    )
    try:
        verify_engine_connection(engine)
    finally:
        engine.dispose()

    engine = create_engine(
        URL(
            user=CONNECTION_PARAMETERS["user"],
            password=CONNECTION_PARAMETERS["password"],
            account=CONNECTION_PARAMETERS["account"],
            host=CONNECTION_PARAMETERS["host"],
            port=CONNECTION_PARAMETERS["port"],
            protocol=CONNECTION_PARAMETERS["protocol"],
        )
    )
    try:
        verify_engine_connection(engine)
    finally:
        engine.dispose()

    engine = create_engine(
        URL(
            user=CONNECTION_PARAMETERS["user"],
            password=CONNECTION_PARAMETERS["password"],
            account=CONNECTION_PARAMETERS["account"],
            host=CONNECTION_PARAMETERS["host"],
            port=CONNECTION_PARAMETERS["port"],
            protocol=CONNECTION_PARAMETERS["protocol"],
            warehouse="testwh",
        )
    )
    try:
        verify_engine_connection(engine)
    finally:
        engine.dispose()


def test_boolean_query_argument_parsing():
    engine = create_engine(
        URL(
            user=CONNECTION_PARAMETERS["user"],
            password=CONNECTION_PARAMETERS["password"],
            account=CONNECTION_PARAMETERS["account"],
            host=CONNECTION_PARAMETERS["host"],
            port=CONNECTION_PARAMETERS["port"],
            protocol=CONNECTION_PARAMETERS["protocol"],
            validate_default_parameters=True,
        )
    )
    try:
        verify_engine_connection(engine)
        connection = engine.raw_connection()
        assert connection.validate_default_parameters is True
    finally:
        connection.close()
        engine.dispose()


def test_create_dialect():
    """
    Tests getting only dialect object through create_engine
    """
    engine = create_engine("snowflake://")
    try:
        assert engine.dialect
    finally:
        engine.dispose()


def test_simple_sql(engine_testaccount):
    """
    Simple SQL by SQLAlchemy
    """
    with engine_testaccount.connect() as conn:
        result = conn.execute(text("show databases"))
    rows = [row for row in result]
    assert len(rows) >= 0, "show database results"


def test_create_drop_tables(engine_testaccount):
    """
    Creates and Drops tables
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount, metadata
    )

    try:
        # validate the tables exists
        with engine_testaccount.connect() as conn:
            results = conn.execute(text("desc table users"))
            assert len([row for row in results]) > 0, "users table doesn't exist"

            # validate the tables exists
            results = conn.execute(text("desc table addresses"))
            assert len([row for row in results]) > 0, "addresses table doesn't exist"
    finally:
        # drop tables
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_insert_tables(engine_testaccount):
    """
    Inserts data into tables
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables(engine_testaccount, metadata)

    with engine_testaccount.connect() as conn:
        try:
            with conn.begin():
                # inserts data with an implicitly generated id
                results = conn.execute(
                    users.insert().values(name="jack", fullname="Jack Jones")
                )
                # Note: SQLAlchemy 1.4 changed what ``inserted_primary_key`` returns
                #  a cast is here to make sure the test works with both older and newer
                #  versions
                assert list(results.inserted_primary_key) == [1], "sequence value"
                results.close()

                # inserts data with the given id
                conn.execute(
                    users.insert(),
                    {"id": 2, "name": "wendy", "fullname": "Wendy Williams"},
                )

                # verify the results
                results = conn.execute(select(users))
                assert (
                    len([row for row in results]) == 2
                ), "number of rows from users table"
                results.close()

                # fetchone
                results = conn.execute(select(users).order_by("id"))
                row = results.fetchone()
                results.close()
                assert row._mapping._data[2] == "Jack Jones", "user name"
                assert row._mapping["fullname"] == "Jack Jones", "user name by dict"
                assert (
                    row._mapping[users.c.fullname] == "Jack Jones"
                ), "user name by Column object"

                conn.execute(
                    addresses.insert(),
                    [
                        {"user_id": 1, "email_address": "jack@yahoo.com"},
                        {"user_id": 1, "email_address": "jack@msn.com"},
                        {"user_id": 2, "email_address": "www@www.org"},
                        {"user_id": 2, "email_address": "wendy@aol.com"},
                    ],
                )

                # more records
                results = conn.execute(select(addresses))
                assert (
                    len([row for row in results]) == 4
                ), "number of rows from addresses table"
                results.close()

                # select specified column names
                results = conn.execute(
                    select(users.c.name, users.c.fullname).order_by("name")
                )
                results.fetchone()
                row = results.fetchone()
                assert row._mapping["name"] == "wendy", "name"

                # join
                results = conn.execute(
                    select(users, addresses).where(users.c.id == addresses.c.user_id)
                )
                results.fetchone()
                results.fetchone()
                results.fetchone()
                row = results.fetchone()
                assert row._mapping["email_address"] == "wendy@aol.com", "email address"

                # Operator
                assert (
                    str(users.c.id == addresses.c.user_id)
                    == "users.id = addresses.user_id"
                ), "equal operator"
                assert (
                    str(users.c.id == 7) == "users.id = :id_1"
                ), "equal to a static number"
                assert str(users.c.name == None)  # NOQA
                assert (
                    str(users.c.id + addresses.c.id) == "users.id + addresses.id"
                ), "number + number"
                assert (
                    str(users.c.name + users.c.fullname)
                    == "users.name || users.fullname"
                ), "str + str"

                # Conjunctions
                # example 1
                obj = and_(
                    users.c.name.like("j%"),
                    users.c.id == addresses.c.user_id,
                    or_(
                        addresses.c.email_address == "wendy@aol.com",
                        addresses.c.email_address == "jack@yahoo.com",
                    ),
                    not_(users.c.id > 5),
                )
                expected_sql = textwrap.dedent(
                    """\
                    users.name LIKE :name_1
                     AND users.id = addresses.user_id
                     AND (addresses.email_address = :email_address_1
                     OR addresses.email_address = :email_address_2)
                     AND users.id <= :id_1"""
                )
                assert str(obj) == "".join(
                    expected_sql.split("\n")
                ), "complex condition"

                # example 2
                obj = (
                    users.c.name.like("j%")
                    & (users.c.id == addresses.c.user_id)
                    & (
                        (addresses.c.email_address == "wendy@aol.com")
                        | (addresses.c.email_address == "jack@yahoo.com")
                    )
                    & ~(users.c.id > 5)
                )
                assert str(obj) == "".join(
                    expected_sql.split("\n")
                ), "complex condition using python operators"

                # example 3
                s = select(
                    (users.c.fullname + ", " + addresses.c.email_address).label("title")
                ).where(
                    and_(
                        users.c.id == addresses.c.user_id,
                        users.c.name.between("m", "z"),
                        or_(
                            addresses.c.email_address.like("%@aol.com"),
                            addresses.c.email_address.like("%@msn.com"),
                        ),
                    )
                )
                results = conn.execute(s).fetchall()
                assert results[0][0] == "Wendy Williams, wendy@aol.com"

                # Aliases
                a1 = addresses.alias()
                a2 = addresses.alias()
                s = select(users).where(
                    and_(
                        users.c.id == a1.c.user_id,
                        users.c.id == a2.c.user_id,
                        a1.c.email_address == "jack@msn.com",
                        a2.c.email_address == "jack@yahoo.com",
                    )
                )
                results = conn.execute(s).fetchone()
                assert results == (1, "jack", "Jack Jones")

                # Joins
                assert (
                    str(users.join(addresses)) == "users JOIN addresses ON "
                    "users.id = addresses.user_id"
                )

                s = select(users.c.fullname).select_from(
                    users.join(
                        addresses, addresses.c.email_address.like(users.c.name + "%")
                    )
                )
                results = conn.execute(s).fetchall()
                assert results[1] == ("Jack Jones",)

                s = (
                    select(users.c.fullname)
                    .select_from(users.outerjoin(addresses))
                    .order_by(users.c.fullname)
                )
                results = conn.execute(s).fetchall()
                assert results[-1] == ("Wendy Williams",)
        finally:
            # drop tables
            addresses.drop(engine_testaccount)
            users.drop(engine_testaccount)


def test_table_does_not_exist(engine_testaccount):
    """
    Tests Correct Exception Thrown When Table Does Not Exist
    """
    meta = MetaData()
    with pytest.raises(NoSuchTableError):
        Table("does_not_exist", meta, autoload_with=engine_testaccount)


@pytest.mark.skip(
    """
Reflection is not implemented yet.
"""
)
def test_reflextion(engine_testaccount):
    """
    Tests Reflection
    """
    with engine_testaccount.connect() as conn:
        engine_testaccount.execute(
            textwrap.dedent(
                """\
            CREATE OR REPLACE TABLE user (
                id       Integer primary key,
                name     String,
                fullname String
            )
            """
            )
        )
        try:
            meta = MetaData()
            user_reflected = Table("user", meta, autoload_with=engine_testaccount)
            assert user_reflected.c == ["user.id", "user.name", "user.fullname"]
        finally:
            conn.execute("DROP TABLE IF EXISTS user")


def test_inspect_column(engine_testaccount):
    """
    Tests Inspect
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount, metadata
    )
    try:
        inspector = inspect(engine_testaccount)
        all_table_names = inspector.get_table_names()
        assert "users" in all_table_names
        assert "addresses" in all_table_names

        columns_in_users = inspector.get_columns("users")

        assert columns_in_users[0]["autoincrement"], "autoincrement"
        assert columns_in_users[0]["default"] is None, "default"
        assert columns_in_users[0]["name"] == "id", "name"
        assert columns_in_users[0]["primary_key"], "primary key"

        assert not columns_in_users[1]["autoincrement"], "autoincrement"
        assert columns_in_users[1]["default"] is None, "default"
        assert columns_in_users[1]["name"] == "name", "name"
        assert not columns_in_users[1]["primary_key"], "primary key"

        assert not columns_in_users[2]["autoincrement"], "autoincrement"
        assert columns_in_users[2]["default"] is None, "default"
        assert columns_in_users[2]["name"] == "fullname", "name"
        assert not columns_in_users[2]["primary_key"], "primary key"

    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_indexes(engine_testaccount):
    """
    Tests get indexes

    NOTE: Snowflake doesn't support indexes
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount, metadata
    )
    try:
        inspector = inspect(engine_testaccount)
        assert inspector.get_indexes("users") == []

    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_check_constraints(engine_testaccount):
    """
    Tests get check constraints

    NOTE: Snowflake doesn't support check constraints
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount, metadata
    )
    try:
        inspector = inspect(engine_testaccount)
        assert inspector.get_check_constraints("users") == []

    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_primary_keys(engine_testaccount):
    """
    Tests get primary keys
    """
    metadata = MetaData()
    pk_name = "pk_addresses"
    users, addresses = _create_users_addresses_tables(
        engine_testaccount, metadata, pk=pk_name
    )
    try:
        inspector = inspect(engine_testaccount)

        primary_keys = inspector.get_pk_constraint("users")
        assert primary_keys["name"].startswith("SYS_CONSTRAINT_")
        assert primary_keys["constrained_columns"] == ["id"]

        primary_keys = inspector.get_pk_constraint("addresses")
        assert primary_keys["name"] == pk_name
        assert primary_keys["constrained_columns"] == ["id"]

    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_unique_constraints(engine_testaccount):
    """
    Tests unqiue constraints
    """
    metadata = MetaData()
    uq_name = "uq_addresses_email_address"
    users, addresses = _create_users_addresses_tables(
        engine_testaccount, metadata, uq=uq_name
    )

    try:
        inspector = inspect(engine_testaccount)
        unique_constraints = inspector.get_unique_constraints("addresses")
        assert unique_constraints[0]["name"] == uq_name
        assert unique_constraints[0]["column_names"] == ["email_address"]
    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_foreign_keys(engine_testaccount):
    """
    Tests foreign keys
    """
    metadata = MetaData()
    fk_name = "fk_users_id_from_addresses"
    users, addresses = _create_users_addresses_tables(
        engine_testaccount, metadata, fk=fk_name
    )

    try:
        inspector = inspect(engine_testaccount)
        foreign_keys = inspector.get_foreign_keys("addresses")
        assert foreign_keys[0]["name"] == fk_name
        assert foreign_keys[0]["constrained_columns"] == ["user_id"]
        assert foreign_keys[0]["referred_table"] == "users"
        assert foreign_keys[0]["referred_schema"] is None
    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_naming_convention_constraint_names(engine_testaccount):
    metadata = MetaData(
        naming_convention={
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )

    users = Table(
        # long name to force foreign key over the max identifier length
        "users" + ("s" * 250),
        metadata,
        Column("id", Integer, primary_key=True),
    )
    addresses = Table(
        "addresses",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", None, ForeignKey(users.c.id)),
        Column("email_address", String, nullable=False, unique=True),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        assert (
            inspector.get_unique_constraints("addresses")[0]["name"]
            == "uq_addresses_email_address"
        )
        assert inspector.get_pk_constraint("addresses")["name"] == "pk_addresses"
        assert (
            inspector.get_foreign_keys("addresses")[0]["name"]
            == "fk_addresses_user_id_users" + ("s" * 221) + "_ee5a"
        )
    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_multile_column_primary_key(engine_testaccount):
    """
    Tests multicolumn primary key with and without autoincrement
    """
    metadata = MetaData()
    mytable = Table(
        "mytable",
        metadata,
        Column("gid", Integer, primary_key=True, autoincrement=False),
        Column("id", Integer, primary_key=True, autoincrement=True),
    )

    metadata.create_all(engine_testaccount)
    try:
        inspector = inspect(engine_testaccount)
        columns_in_mytable = inspector.get_columns("mytable")
        assert not columns_in_mytable[0]["autoincrement"], "autoincrement"
        assert columns_in_mytable[0]["default"] is None, "default"
        assert columns_in_mytable[0]["name"] == "gid", "name"
        assert columns_in_mytable[0]["primary_key"], "primary key"
        assert columns_in_mytable[1]["autoincrement"], "autoincrement"
        assert columns_in_mytable[1]["default"] is None, "default"
        assert columns_in_mytable[1]["name"] == "id", "name"
        assert columns_in_mytable[1]["primary_key"], "primary key"

        primary_keys = inspector.get_pk_constraint("mytable")
        assert primary_keys["constrained_columns"] == ["gid", "id"]

    finally:
        mytable.drop(engine_testaccount)


def test_create_table_with_cluster_by(engine_testaccount):
    # Test case for https://github.com/snowflakedb/snowflake-sqlalchemy/pull/14
    metadata = MetaData()
    user = Table(
        "clustered_user",
        metadata,
        Column("Id", Integer, primary_key=True),
        Column("name", String),
        snowflake_clusterby=["Id", "name"],
    )
    metadata.create_all(engine_testaccount)
    try:
        inspector = inspect(engine_testaccount)
        columns_in_table = inspector.get_columns("clustered_user")
        assert columns_in_table[0]["name"] == "Id", "name"
    finally:
        user.drop(engine_testaccount)


def test_view_names(engine_testaccount):
    """
    Tests all views
    """
    inspector = inspect(engine_testaccount)

    information_schema_views = inspector.get_view_names(schema="information_schema")
    assert "columns" in information_schema_views
    assert "table_constraints" in information_schema_views


def test_view_definition(engine_testaccount, db_parameters):
    """
    Tests view definition
    """
    test_table_name = "test_table_sqlalchemy"
    test_view_name = "testview_sqlalchemy"
    with engine_testaccount.connect() as conn:
        with conn.begin():
            conn.execute(
                text(
                    textwrap.dedent(
                        f"""\
                    CREATE OR REPLACE TABLE {test_table_name} (
                        id INTEGER,
                        name STRING
                    )
                    """
                    )
                )
            )
            sql = f"CREATE OR REPLACE VIEW {test_view_name} AS SELECT * FROM {test_table_name} WHERE id > 10"
            conn.execute(text(sql).execution_options(autocommit=True))
        try:
            inspector = inspect(engine_testaccount)
            assert inspector.get_view_definition(test_view_name) == sql.strip()
            assert (
                inspector.get_view_definition(test_view_name, db_parameters["schema"])
                == sql.strip()
            )
            assert inspector.get_view_names() == [test_view_name]
        finally:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_table_name}"))
            conn.execute(text(f"DROP VIEW IF EXISTS {test_view_name}"))


def test_view_comment_reading(engine_testaccount, db_parameters):
    """
    Tests reading a comment from a view once it's defined
    """
    test_table_name = "test_table_sqlalchemy"
    test_view_name = "testview_sqlalchemy"
    with engine_testaccount.connect() as conn:
        with conn.begin():
            conn.execute(
                text(
                    textwrap.dedent(
                        f"""\
                    CREATE OR REPLACE TABLE {test_table_name} (
                        id INTEGER,
                        name STRING
                    )
                    """
                    )
                )
            )
            sql = f"CREATE OR REPLACE VIEW {test_view_name} AS SELECT * FROM {test_table_name} WHERE id > 10"
            conn.execute(text(sql).execution_options(autocommit=True))
            comment_text = "hello my viewing friends"
            sql = f"COMMENT ON VIEW {test_view_name} IS '{comment_text}';"
            conn.execute(text(sql).execution_options(autocommit=True))
        try:
            inspector = inspect(engine_testaccount)
            # NOTE: sqlalchemy doesn't have a way to get view comments specifically,
            # but the code to get table comments should work for views too
            assert inspector.get_table_comment(test_view_name) == {"text": comment_text}
            assert inspector.get_table_comment(test_table_name) == {"text": None}
            assert str(inspector.get_columns(test_table_name)) == str(
                inspector.get_columns(test_view_name)
            )
        finally:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_table_name}"))
            conn.execute(text(f"DROP VIEW IF EXISTS {test_view_name}"))


@pytest.mark.skip("Temp table cannot be viewed for some reason")
def test_get_temp_table_names(engine_testaccount):
    num_of_temp_tables = 2
    temp_table_name = "temp_table"
    for idx in range(num_of_temp_tables):
        engine_testaccount.execute(
            text(
                f"CREATE TEMPORARY TABLE {temp_table_name + str(idx)} (col1 integer, col2 string)"
            ).execution_options(autocommit=True)
        )
    for row in engine_testaccount.execute("SHOW TABLES"):
        print(row)
    inspector = inspect(engine_testaccount)
    temp_table_names = inspector.get_temp_table_names()
    assert len(temp_table_names) == num_of_temp_tables


def test_create_table_with_schema(engine_testaccount, db_parameters):
    metadata = MetaData()
    new_schema = f"{db_parameters['schema']}_NEW_{random_string(5, choices=string.ascii_uppercase)}"
    with engine_testaccount.connect() as conn:
        conn.execute(text(f'CREATE OR REPLACE SCHEMA "{new_schema}"'))
        Table(
            "users",
            metadata,
            Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
            Column("name", String),
            Column("fullname", String),
            schema=new_schema,
        )
        metadata.create_all(engine_testaccount)

        try:
            inspector = inspect(engine_testaccount)
            columns_in_users = inspector.get_columns("users", schema=new_schema)
            assert columns_in_users is not None
        finally:
            metadata.drop_all(engine_testaccount)
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{new_schema}"'))


@pytest.mark.skipif(
    os.getenv("SNOWFLAKE_GCP") is not None,
    reason="PUT and GET is not supported for GCP yet",
)
def test_copy(engine_testaccount):
    """
    COPY must be in a transaction
    """
    with engine_testaccount.connect() as conn:
        metadata = MetaData()
        users, addresses = _create_users_addresses_tables_without_sequence(
            engine_testaccount, metadata
        )

        try:
            with conn.begin():
                conn.execute(
                    text(
                        f"PUT file://{os.path.join(THIS_DIR, 'data', 'users.txt')} @%users"
                    )
                )
                conn.execute(text("COPY INTO users"))
                results = conn.execute(text("SELECT * FROM USERS")).fetchall()
                assert results is not None and len(results) > 0
        finally:
            addresses.drop(engine_testaccount)
            users.drop(engine_testaccount)


@pytest.mark.skip(
    """
No transaction works yet in the core API. Use orm API or Python Connector
directly if needed at the moment.
Note Snowflake DB supports DML transaction natively, but we have not figured out
how to integrate with SQLAlchemy core API yet.
"""
)
def test_transaction(engine_testaccount, db_parameters):
    engine_testaccount.execute(
        text(f"CREATE TABLE {db_parameters['name']} (c1 number)")
    )
    trans = engine_testaccount.connect().begin()
    try:
        engine_testaccount.execute(
            text(f"INSERT INTO {db_parameters['name']} VALUES(123)")
        )
        trans.commit()
        engine_testaccount.execute(
            text(f"INSERT INTO {db_parameters['name']} VALUES(456)")
        )
        trans.rollback()
        results = engine_testaccount.execute(
            f"SELECT * FROM {db_parameters['name']}"
        ).fetchall()
        assert results == [(123,)]
    finally:
        engine_testaccount.execute(
            text(f"DROP TABLE IF EXISTS {db_parameters['name']}")
        )


def test_get_schemas(engine_testaccount):
    """
    Tests get schemas from inspect.

    Although the method get_schema_names is not part of DefaultDialect,
    inspect() may call the method if exists.
    """
    inspector = inspect(engine_testaccount)

    schemas = inspector.get_schema_names()
    assert "information_schema" in schemas


def test_column_metadata(engine_testaccount):
    from sqlalchemy.orm import declarative_base

    Base = declarative_base()

    class Appointment(Base):
        __tablename__ = "appointment"
        id = Column(Numeric(38, 3), primary_key=True)
        string_with_len = Column(String(100))
        binary_data = Column(LargeBinary)
        real_data = Column(REAL)

    Base.metadata.create_all(engine_testaccount)

    metadata = Base.metadata

    t = Table("appointment", metadata)

    inspector = inspect(engine_testaccount)
    inspector.reflect_table(t, None)
    assert str(t.columns["id"].type) == "DECIMAL(38, 3)"
    assert str(t.columns["string_with_len"].type) == "VARCHAR(100)"
    assert str(t.columns["binary_data"].type) == "BINARY"
    assert str(t.columns["real_data"].type) == "FLOAT"


def test_many_table_column_metadta(db_parameters):
    """
    Get dozens of table metadata with column metadata cache.

    cache_column_metadata=True will cache all column metadata for all tables
    in the schema.
    """
    url = url_factory(cache_column_metadata=True)
    engine = get_engine(url)

    RE_SUFFIX_NUM = re.compile(r".*(\d+)$")
    metadata = MetaData()
    total_objects = 10
    for idx in range(total_objects):
        Table(
            "mainusers" + str(idx),
            metadata,
            Column("id" + str(idx), Integer, Sequence("user_id_seq"), primary_key=True),
            Column("name" + str(idx), String),
            Column("fullname", String),
            Column("password", String),
        )
        Table(
            "mainaddresses" + str(idx),
            metadata,
            Column(
                "id" + str(idx), Integer, Sequence("address_id_seq"), primary_key=True
            ),
            Column(
                "user_id" + str(idx),
                None,
                ForeignKey("mainusers" + str(idx) + ".id" + str(idx)),
            ),
            Column("email_address" + str(idx), String, nullable=False),
        )
    metadata.create_all(engine)

    inspector = inspect(engine)
    cnt = 0
    schema = inspector.default_schema_name
    for table_name in inspector.get_table_names(schema):
        m = RE_SUFFIX_NUM.match(table_name)
        if m:
            suffix = m.group(1)
            cs = inspector.get_columns(table_name, schema)
            if table_name.startswith("mainusers"):
                assert len(cs) == 4
                assert cs[1]["name"] == "name" + suffix
                cnt += 1
            elif table_name.startswith("mainaddresses"):
                assert len(cs) == 3
                assert cs[2]["name"] == "email_address" + suffix
                cnt += 1
            ps = inspector.get_pk_constraint(table_name, schema)
            if table_name.startswith("mainusers"):
                assert ps["constrained_columns"] == ["id" + suffix]
            elif table_name.startswith("mainaddresses"):
                assert ps["constrained_columns"] == ["id" + suffix]
            fs = inspector.get_foreign_keys(table_name, schema)
            if table_name.startswith("mainusers"):
                assert len(fs) == 0
            elif table_name.startswith("mainaddresses"):
                assert len(fs) == 1
                assert fs[0]["constrained_columns"] == ["user_id" + suffix]
                assert fs[0]["referred_table"] == "mainusers" + suffix

    assert cnt == total_objects * 2, "total number of test objects"


@pytest.mark.skip(
    reason="SQLAlchemy 1.4 release seem to have caused a pretty big"
    "performance degradation, but addressing this should also"
    "address fully supporting SQLAlchemy 1.4 which has a lot "
    "of changes"
)
def test_cache_time(engine_testaccount, db_parameters):
    """Check whether Inspector cache is working"""
    # Set up necessary tables
    metadata = MetaData()
    total_objects = 10
    for idx in range(total_objects):
        Table(
            "mainusers" + str(idx),
            metadata,
            Column("id" + str(idx), Integer, Sequence("user_id_seq"), primary_key=True),
            Column("name" + str(idx), String),
            Column("fullname", String),
            Column("password", String),
        )
        Table(
            "mainaddresses" + str(idx),
            metadata,
            Column(
                "id" + str(idx), Integer, Sequence("address_id_seq"), primary_key=True
            ),
            Column(
                "user_id" + str(idx),
                None,
                ForeignKey("mainusers" + str(idx) + ".id" + str(idx)),
            ),
            Column("email_address" + str(idx), String, nullable=False),
        )
    metadata.create_all(engine_testaccount)
    inspector = inspect(engine_testaccount)
    schema = db_parameters["schema"]

    def harass_inspector():
        for table_name in inspector.get_table_names(schema):
            inspector.get_columns(table_name, schema)
            inspector.get_pk_constraint(table_name, schema)
            inspector.get_foreign_keys(table_name, schema)

    outcome = False
    # Allow up to 5 times for the speed test to pass to avoid flaky test
    for _ in range(5):
        # Python 2.7 has no timeit.timeit with globals and locals parameters
        s_time = time.time()
        harass_inspector()
        m_time = time.time()
        harass_inspector()
        time2 = time.time() - m_time
        time1 = m_time - s_time
        print(
            f"Ran inspector through tables twice, times:\n\tfirst: {time1}\n\tsecond: {time2}"
        )
        if time2 < time1 * 0.01:
            outcome = True
            break
        else:
            # Reset inspector to reset cache
            inspector = inspect(engine_testaccount)
    metadata.drop_all(engine_testaccount)
    assert outcome


@pytest.mark.timeout(10)
@pytest.mark.parametrize(
    "region",
    (
        pytest.param("eu-central-1", id="region"),
        pytest.param("east-us-2.azure", id="azure"),
    ),
)
def test_connection_timeout_error(region):
    engine = create_engine(
        URL(
            user="testuser",
            password="testpassword",
            account="testaccount",
            region="east-us-2.azure",
            login_timeout=5,
        )
    )

    with pytest.raises(OperationalError) as excinfo:
        engine.connect()

    assert excinfo.value.orig.errno == 250001
    assert "Could not connect to Snowflake backend" in excinfo.value.orig.msg
    assert region not in excinfo.value.orig.msg


def test_load_dialect():
    """
    Test loading Snowflake SQLAlchemy dialect class
    """
    assert isinstance(dialects.registry.load("snowflake")(), dialect)


@pytest.mark.parametrize("conditional_flag", [True, False])
@pytest.mark.parametrize(
    "update_flag,insert_flag,delete_flag",
    [
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (False, True, True),
        (True, True, False),
    ],
)
def test_upsert(
    engine_testaccount, update_flag, insert_flag, delete_flag, conditional_flag
):
    meta = MetaData()
    users = Table(
        "users",
        meta,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )
    onboarding_users = Table(
        "onboarding_users",
        meta,
        Column("id", Integer, Sequence("new_user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("delete", Boolean),
    )
    meta.create_all(engine_testaccount)
    try:
        with engine_testaccount.connect() as conn:
            with conn.begin():
                conn.execute(
                    users.insert(),
                    [
                        {"id": 1, "name": "mark", "fullname": "Mark Keller"},
                        {"id": 4, "name": "luke", "fullname": "Luke Lorimer"},
                        {"id": 2, "name": "amanda", "fullname": "Amanda Harris"},
                    ],
                )
                conn.execute(
                    onboarding_users.insert(),
                    [
                        {
                            "id": 2,
                            "name": "amanda",
                            "fullname": "Amanda Charlotte Harris",
                            "delete": True,
                        },
                        {
                            "id": 3,
                            "name": "jim",
                            "fullname": "Jim Wang",
                            "delete": False,
                        },
                        {
                            "id": 4,
                            "name": "lukas",
                            "fullname": "Lukas Lorimer",
                            "delete": False,
                        },
                        {"id": 5, "name": "andras", "fullname": None, "delete": False},
                    ],
                )

                merge = MergeInto(
                    users, onboarding_users, users.c.id == onboarding_users.c.id
                )
                if update_flag:
                    clause = merge.when_matched_then_update().values(
                        name=onboarding_users.c.name,
                        fullname=onboarding_users.c.fullname,
                    )
                    if conditional_flag:
                        clause.where(onboarding_users.c.name != "amanda")
                if insert_flag:
                    clause = merge.when_not_matched_then_insert().values(
                        id=onboarding_users.c.id,
                        name=onboarding_users.c.name,
                        fullname=onboarding_users.c.fullname,
                    )
                    if conditional_flag:
                        clause.where(onboarding_users.c.fullname != None)  # NOQA
                if delete_flag:
                    clause = merge.when_matched_then_delete()
                    if conditional_flag:
                        clause.where(onboarding_users.c.delete == True)  # NOQA
                conn.execute(merge)
                users_tuples = {tuple(row) for row in conn.execute(select(users))}
                onboarding_users_tuples = {
                    tuple(row) for row in conn.execute(select(onboarding_users))
                }
                expected_users = {
                    (1, "mark", "Mark Keller"),
                    (2, "amanda", "Amanda Harris"),
                    (4, "luke", "Luke Lorimer"),
                }
                if update_flag:
                    if not conditional_flag:
                        expected_users.remove((2, "amanda", "Amanda Harris"))
                        expected_users.add((2, "amanda", "Amanda Charlotte Harris"))
                    expected_users.remove((4, "luke", "Luke Lorimer"))
                    expected_users.add((4, "lukas", "Lukas Lorimer"))
                elif delete_flag:
                    if not conditional_flag:
                        expected_users.remove((4, "luke", "Luke Lorimer"))
                    expected_users.remove((2, "amanda", "Amanda Harris"))
                if insert_flag:
                    if not conditional_flag:
                        expected_users.add((5, "andras", None))
                    expected_users.add((3, "jim", "Jim Wang"))
                expected_onboarding_users = {
                    (2, "amanda", "Amanda Charlotte Harris", True),
                    (3, "jim", "Jim Wang", False),
                    (4, "lukas", "Lukas Lorimer", False),
                    (5, "andras", None, False),
                }
                assert users_tuples == expected_users
                assert onboarding_users_tuples == expected_onboarding_users
    finally:
        users.drop(engine_testaccount)
        onboarding_users.drop(engine_testaccount)


def test_deterministic_merge_into(sql_compiler):
    meta = MetaData()
    users = Table(
        "users",
        meta,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )
    onboarding_users = Table(
        "onboarding_users",
        meta,
        Column("id", Integer, Sequence("new_user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("delete", Boolean),
    )
    merge = MergeInto(users, onboarding_users, users.c.id == onboarding_users.c.id)
    merge.when_matched_then_update().values(
        name=onboarding_users.c.name, fullname=onboarding_users.c.fullname
    )
    merge.when_not_matched_then_insert().values(
        id=onboarding_users.c.id,
        name=onboarding_users.c.name,
        fullname=onboarding_users.c.fullname,
    ).where(
        onboarding_users.c.fullname != None  # NOQA
    )
    assert (
        sql_compiler(merge)
        == "MERGE INTO users USING onboarding_users ON users.id = onboarding_users.id "
        "WHEN MATCHED THEN UPDATE SET fullname = onboarding_users.fullname, "
        "name = onboarding_users.name WHEN NOT MATCHED AND onboarding_users.fullname "
        "IS NOT NULL THEN INSERT (fullname, id, name) VALUES (onboarding_users.fullname, "
        "onboarding_users.id, onboarding_users.name)"
    )


def test_comments(engine_testaccount):
    """Tests strictly reading column comment through SQLAlchemy"""
    table_name = random_string(5, choices=string.ascii_uppercase)
    with engine_testaccount.connect() as conn:
        try:
            conn.execute(text(f'create table public.{table_name} ("col1" text);'))
            conn.execute(
                text(
                    f"alter table public.{table_name} alter \"col1\" comment 'this is my comment'"
                )
            )
            conn.execute(
                text(
                    f"select comment from information_schema.columns where table_name='{table_name}'"
                )
            ).fetchall()
            inspector = inspect(engine_testaccount)
            columns = inspector.get_columns(table_name, schema="PUBLIC")
            assert columns[0].get("comment") == "this is my comment"
        finally:
            conn.execute(text(f"drop table public.{table_name}"))


def test_comment_sqlalchemy(db_parameters, engine_testaccount, on_public_ci):
    """Testing adding/reading column and table comments through SQLAlchemy"""
    new_schema = db_parameters["schema"] + "2"
    # Use same table name in 2 different schemas to make sure comment retrieval works properly
    table_name = random_string(5, choices=string.ascii_uppercase)
    table_comment1 = random_string(10, choices=string.ascii_uppercase)
    column_comment1 = random_string(10, choices=string.ascii_uppercase)
    table_comment2 = random_string(10, choices=string.ascii_uppercase)
    column_comment2 = random_string(10, choices=string.ascii_uppercase)

    engine2 = get_engine(url_factory(schema=new_schema))
    con2 = None
    if not on_public_ci:
        con2 = engine2.connect()
        con2.execute(text(f"CREATE SCHEMA IF NOT EXISTS {new_schema}"))
    inspector = inspect(engine_testaccount)
    metadata1 = MetaData()
    metadata2 = MetaData()
    mytable1 = Table(
        table_name,
        metadata1,
        Column("tstamp", DateTime, comment=column_comment1),
        comment=table_comment1,
    )
    mytable2 = Table(
        table_name,
        metadata2,
        Column("tstamp", DateTime, comment=column_comment2),
        comment=table_comment2,
    )

    metadata1.create_all(engine_testaccount, tables=[mytable1])
    if not on_public_ci:
        metadata2.create_all(engine2, tables=[mytable2])

    try:
        assert inspector.get_columns(table_name)[0]["comment"] == column_comment1
        assert inspector.get_table_comment(table_name)["text"] == table_comment1
        if not on_public_ci:
            assert (
                inspector.get_columns(table_name, schema=new_schema)[0]["comment"]
                == column_comment2
            )
            assert (
                inspector.get_table_comment(
                    table_name,
                    schema=new_schema.upper(),  # Note: since did not quote schema name it was uppercase'd
                )["text"]
                == table_comment2
            )
    finally:
        mytable1.drop(engine_testaccount)
        if not on_public_ci:
            mytable2.drop(engine2)
            con2.execute(text(f"DROP SCHEMA IF EXISTS {new_schema}"))
            con2.close()
        engine2.dispose()


@pytest.mark.internal
def test_special_schema_character(db_parameters, on_public_ci):
    """Make sure we decode special characters correctly"""
    # Constants
    database = f"{random_string(5)}a/b/c"  # "'/'.join([choice(ascii_lowercase) for _ in range(3)])
    schema = f"{random_string(5)}d/e/f"  # '/'.join([choice(ascii_lowercase) for _ in range(3)])
    # Setup
    options = dict(**db_parameters)
    with connect(**options) as conn:
        conn.cursor().execute(f'CREATE OR REPLACE DATABASE "{database}"')
        conn.cursor().execute(f'CREATE OR REPLACE SCHEMA "{schema}"')

    # Test
    options.update({"database": '"' + database + '"', "schema": '"' + schema + '"'})
    with connect(**options) as sf_conn:
        sf_connection = (
            sf_conn.cursor()
            .execute("select current_database(), " "current_schema();")
            .fetchall()
        )
    with create_engine(URL(**options)).connect() as sa_conn:
        sa_connection = sa_conn.execute(
            text("select current_database(), " "current_schema();")
        ).fetchall()
    # Teardown
    with connect(**options) as conn:
        conn.cursor().execute(f'DROP DATABASE IF EXISTS "{database}"')

    assert [(database, schema)] == sf_connection == sa_connection


def test_autoincrement(engine_testaccount):
    """Snowflake does not guarantee generating sequence numbers without gaps.

    The generated numbers are not necessarily contiguous.
    https://docs.snowflake.com/en/user-guide/querying-sequences
    """
    metadata = MetaData()
    users = Table(
        "users",
        metadata,
        Column("uid", Integer, Sequence("id_seq", order=True), primary_key=True),
        Column("name", String(39)),
    )

    try:
        metadata.create_all(engine_testaccount)

        with engine_testaccount.begin() as connection:
            connection.execute(insert(users), [{"name": "sf1"}])
            assert connection.execute(select(users)).fetchall() == [(1, "sf1")]
            connection.execute(insert(users), [{"name": "sf2"}, {"name": "sf3"}])
            assert connection.execute(select(users)).fetchall() == [
                (1, "sf1"),
                (2, "sf2"),
                (3, "sf3"),
            ]
            connection.execute(insert(users), {"name": "sf4"})
            assert connection.execute(select(users)).fetchall() == [
                (1, "sf1"),
                (2, "sf2"),
                (3, "sf3"),
                (4, "sf4"),
            ]

            seq = Sequence("id_seq")
            nextid = connection.execute(seq)
            connection.execute(insert(users), [{"uid": nextid, "name": "sf5"}])
            assert connection.execute(select(users)).fetchall() == [
                (1, "sf1"),
                (2, "sf2"),
                (3, "sf3"),
                (4, "sf4"),
                (5, "sf5"),
            ]
    finally:
        metadata.drop_all(engine_testaccount)


@pytest.mark.skip(
    reason="SQLAlchemy 1.4 release seem to have caused a pretty big"
    "performance degradation, but addressing this should also"
    "address fully supporting SQLAlchemy 1.4 which has a lot "
    "of changes"
)
def test_get_too_many_columns(engine_testaccount, db_parameters):
    """Check whether Inspector cache is working, when there are too many column to cache whole schema's columns"""
    # Set up necessary tables
    metadata = MetaData()
    total_objects = 10
    for idx in range(total_objects):
        Table(
            "mainuserss" + str(idx),
            metadata,
            Column("id" + str(idx), Integer, Sequence("user_id_seq"), primary_key=True),
            Column("name" + str(idx), String),
            Column("fullname", String),
            Column("password", String),
        )
        Table(
            "mainaddressess" + str(idx),
            metadata,
            Column(
                "id" + str(idx), Integer, Sequence("address_id_seq"), primary_key=True
            ),
            Column(
                "user_id" + str(idx),
                None,
                ForeignKey("mainuserss" + str(idx) + ".id" + str(idx)),
            ),
            Column("email_address" + str(idx), String, nullable=False),
        )
    metadata.create_all(engine_testaccount)
    inspector = inspect(engine_testaccount)
    schema = db_parameters["schema"]

    # Emulate error
    with patch.object(
        inspector.dialect, "_get_schema_columns", return_value=None
    ) as mock_method:

        def harass_inspector():
            for table_name in inspector.get_table_names(schema):
                column_metadata = inspector.get_columns(table_name, schema)
                inspector.get_pk_constraint(table_name, schema)
                inspector.get_foreign_keys(table_name, schema)
                assert (
                    3 <= len(column_metadata) <= 4
                )  # Either one of the tables should have 3 or 4 columns

        outcome = False
        # Allow up to 5 times for the speed test to pass to avoid flaky test
        for _ in range(5):
            # Python 2.7 has no timeit.timeit with globals and locals parameters
            s_time = time.time()
            harass_inspector()
            m_time = time.time()
            harass_inspector()
            time2 = time.time() - m_time
            time1 = m_time - s_time
            print(
                f"Ran inspector through tables twice, times:\n\tfirst: {time1}\n\tsecond: {time2}"
            )
            if time2 < time1 * 0.01:
                outcome = True
                break
            else:
                # Reset inspector to reset cache
                inspector = inspect(engine_testaccount)
        metadata.drop_all(engine_testaccount)
        assert (
            mock_method.call_count > 0
        )  # Make sure we actually mocked the issue happening
        assert outcome


def test_too_many_columns_detection(engine_testaccount, db_parameters):
    """This tests whether a too many column error actually triggers the more granular table version"""
    # Set up a single table
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("password", String),
    )
    metadata.create_all(engine_testaccount)
    inspector = inspect(engine_testaccount)
    # Do test
    connection = inspector.bind.connect()
    original_execute = connection.execute

    too_many_columns_was_raised = False

    def mock_helper(command, *args, **kwargs):
        if "_get_schema_columns" in command.text:
            # Creating exception exactly how SQLAlchemy does
            nonlocal too_many_columns_was_raised
            too_many_columns_was_raised = True
            raise DBAPIError.instance(
                """
            SELECT /* sqlalchemy:_get_schema_columns */
                   ic.table_name,
                   ic.column_name,
                   ic.data_type,
                   ic.character_maximum_length,
                   ic.numeric_precision,
                   ic.numeric_scale,
                   ic.is_nullable,
                   ic.column_default,
                   ic.is_identity,
                   ic.comment
              FROM information_schema.columns ic
             WHERE ic.table_schema='schema_name'
             ORDER BY ic.ordinal_position""",
                {"table_schema": "TESTSCHEMA"},
                ProgrammingError(
                    "Information schema query returned too much data. Please repeat query with more "
                    "selective predicates.",
                    90030,
                ),
                Error,
                hide_parameters=False,
                connection_invalidated=False,
                dialect=SnowflakeDialect(),
                ismulti=None,
            )
        else:
            return original_execute(command, *args, **kwargs)

    with patch.object(engine_testaccount, "connect") as conn:
        conn.return_value = connection
        with patch.object(connection, "execute", side_effect=mock_helper):
            column_metadata = inspector.get_columns("users", db_parameters["schema"])
    assert len(column_metadata) == 4
    assert too_many_columns_was_raised
    # Clean up
    metadata.drop_all(engine_testaccount)


def test_empty_comments(engine_testaccount):
    """Test that no comment returns None"""
    table_name = random_string(5, choices=string.ascii_uppercase)
    with engine_testaccount.connect() as conn:
        try:
            conn.execute(text(f'create table public.{table_name} ("col1" text);'))
            conn.execute(
                text(
                    f"select comment from information_schema.columns where table_name='{table_name}'"
                )
            ).fetchall()
            inspector = inspect(engine_testaccount)
            columns = inspector.get_columns(table_name, schema="PUBLIC")
            assert inspector.get_table_comment(table_name, schema="PUBLIC") == {
                "text": None
            }
            assert all([c["comment"] is None for c in columns])
        finally:
            conn.execute(text(f"drop table public.{table_name}"))


def test_column_type_schema(engine_testaccount):
    with engine_testaccount.connect() as conn:
        table_name = random_string(5)
        # column type FIXED not supported, triggers SQL compilation error: Unsupported data type 'FIXED'.
        conn.exec_driver_sql(
            f"""\
CREATE TEMP TABLE {table_name} (
    C1 BIGINT, C2 BINARY, C3 BOOLEAN, C4 CHAR, C5 CHARACTER, C6 DATE, C7 DATETIME, C8 DEC,
    C9 DECIMAL, C10 DOUBLE, C11 FLOAT, C12 INT, C13 INTEGER, C14 NUMBER, C15 REAL, C16 BYTEINT,
    C17 SMALLINT, C18 STRING, C19 TEXT, C20 TIME, C21 TIMESTAMP, C22 TIMESTAMP_TZ, C23 TIMESTAMP_LTZ,
    C24 TIMESTAMP_NTZ, C25 TINYINT, C26 VARBINARY, C27 VARCHAR, C28 VARIANT, C29 OBJECT, C30 ARRAY, C31 GEOGRAPHY,
    C32 GEOMETRY
)
"""
        )

        table_reflected = Table(table_name, MetaData(), autoload_with=conn)
        columns = table_reflected.columns
        assert (
            len(columns) == len(ischema_names_baseline) - 1
        )  # -1 because FIXED is not supported


def test_result_type_and_value(engine_testaccount):
    with engine_testaccount.connect() as conn:
        table_name = random_string(5)
        conn.exec_driver_sql(
            f"""\
CREATE TEMP TABLE {table_name} (
    C1 BIGINT, C2 BINARY, C3 BOOLEAN, C4 CHAR, C5 CHARACTER, C6 DATE, C7 DATETIME, C8 DEC(12,3),
    C9 DECIMAL(12,3), C10 DOUBLE, C11 FLOAT, C12 INT, C13 INTEGER, C14 NUMBER, C15 REAL, C16 BYTEINT,
    C17 SMALLINT, C18 STRING, C19 TEXT, C20 TIME, C21 TIMESTAMP, C22 TIMESTAMP_TZ, C23 TIMESTAMP_LTZ,
    C24 TIMESTAMP_NTZ, C25 TINYINT, C26 VARBINARY, C27 VARCHAR, C28 VARIANT, C29 OBJECT, C30 ARRAY, C31 GEOGRAPHY,
    C32 GEOMETRY
)
"""
        )
        table_reflected = Table(table_name, MetaData(), autoload_with=conn)
        current_date = date.today()
        current_utctime = datetime.utcnow()
        current_localtime = pytz.utc.localize(current_utctime, is_dst=False).astimezone(
            pytz.timezone(PST_TZ)
        )
        current_localtime_without_tz = datetime.now()
        current_localtime_with_other_tz = pytz.utc.localize(
            current_localtime_without_tz, is_dst=False
        ).astimezone(pytz.timezone(JST_TZ))
        TIME_VALUE = current_utctime.time()
        DECIMAL_VALUE = decimal.Decimal("123456789.123")
        MAX_INT_VALUE = 99999999999999999999999999999999999999
        MIN_INT_VALUE = -99999999999999999999999999999999999999
        FLOAT_VALUE = 123456789.123
        STRING_VALUE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        BINARY_VALUE = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        CHAR_VALUE = "A"
        GEOGRAPHY_VALUE = "POINT(-122.35 37.55)"
        GEOGRAPHY_RESULT_VALUE = '{"coordinates": [-122.35,37.55],"type": "Point"}'
        GEOMETRY_VALUE = "POINT(-94.58473 39.08985)"
        GEOMETRY_RESULT_VALUE = '{"coordinates": [-94.58473,39.08985],"type": "Point"}'

        ins = table_reflected.insert().values(
            c1=MAX_INT_VALUE,  # BIGINT
            c2=BINARY_VALUE,  # BINARY
            c3=True,  # BOOLEAN
            c4=CHAR_VALUE,  # CHAR
            c5=CHAR_VALUE,  # CHARACTER
            c6=current_date,  # DATE
            c7=current_localtime_without_tz,  # DATETIME
            c8=DECIMAL_VALUE,  # DEC(12,3)
            c9=DECIMAL_VALUE,  # DECIMAL(12,3)
            c10=FLOAT_VALUE,  # DOUBLE
            c11=FLOAT_VALUE,  # FLOAT
            c12=MIN_INT_VALUE,  # INT
            c13=MAX_INT_VALUE,  # INTEGER
            c14=MIN_INT_VALUE,  # NUMBER
            c15=FLOAT_VALUE,  # REAL
            c16=MAX_INT_VALUE,  # BYTEINT
            c17=MIN_INT_VALUE,  # SMALLINT
            c18=STRING_VALUE,  # STRING
            c19=STRING_VALUE,  # TEXT
            c20=TIME_VALUE,  # TIME
            c21=current_utctime,  # TIMESTAMP
            c22=current_localtime_with_other_tz,  # TIMESTAMP_TZ
            c23=current_localtime,  # TIMESTAMP_LTZ
            c24=current_utctime,  # TIMESTAMP_NTZ
            c25=MAX_INT_VALUE,  # TINYINT
            c26=BINARY_VALUE,  # VARBINARY
            c27=STRING_VALUE,  # VARCHAR
            c28=None,  # VARIANT, currently snowflake-sqlalchemy/connector does not support binding variant
            c29=None,  # OBJECT, currently snowflake-sqlalchemy/connector does not support binding variant
            c30=None,  # ARRAY, currently snowflake-sqlalchemy/connector does not support binding variant
            c31=GEOGRAPHY_VALUE,  # GEOGRAPHY
            c32=GEOMETRY_VALUE,  # GEOMETRY
        )
        conn.execute(ins)

        results = conn.execute(select(table_reflected)).fetchall()
        assert len(results) == 1
        result = results[0]
        assert (
            result[0] == MAX_INT_VALUE
            and result[1] == BINARY_VALUE
            and result[2] is True
            and result[3] == CHAR_VALUE
            and result[4] == CHAR_VALUE
            and result[5] == current_date
            and result[6] == current_localtime_without_tz
            and result[7] == DECIMAL_VALUE
            and result[8] == DECIMAL_VALUE
            and result[9] == FLOAT_VALUE
            and result[10] == FLOAT_VALUE
            and result[11] == MIN_INT_VALUE
            and result[12] == MAX_INT_VALUE
            and result[13] == MIN_INT_VALUE
            and result[14] == FLOAT_VALUE
            and result[15] == MAX_INT_VALUE
            and result[16] == MIN_INT_VALUE
            and result[17] == STRING_VALUE
            and result[18] == STRING_VALUE
            and result[19] == TIME_VALUE
            and result[20] == current_utctime
            and result[21] == current_localtime_with_other_tz
            and result[22] == current_localtime
            and result[23] == current_utctime
            and result[24] == MAX_INT_VALUE
            and result[25] == BINARY_VALUE
            and result[26] == STRING_VALUE
            and result[27] is None
            and result[28] is None
            and result[29] is None
            and json.loads(result[30]) == json.loads(GEOGRAPHY_RESULT_VALUE)
            and json.loads(result[31]) == json.loads(GEOMETRY_RESULT_VALUE)
        )

        sql = f"""
INSERT INTO {table_name}(c28, c29, c30)
SELECT PARSE_JSON('{{"vk1":100, "vk2":200, "vk3":300}}'),
       OBJECT_CONSTRUCT('vk1', 100, 'vk2', 200, 'vk3', 300),
       PARSE_JSON('[
{{"k":1, "v":"str1"}},
{{"k":2, "v":"str2"}},
{{"k":3, "v":"str3"}}]'
)"""
        conn.exec_driver_sql(sql)
        results = conn.execute(select(table_reflected)).fetchall()
        assert len(results) == 2
        data = json.loads(results[-1][27])
        assert json.loads(results[-1][28]) == data
        assert data["vk1"] == 100
        assert data["vk3"] == 300
        assert data is not None
        data = json.loads(results[-1][29])
        assert data[1]["k"] == 2


def test_normalize_and_denormalize_empty_string_column_name(engine_testaccount):
    with engine_testaccount.connect() as conn:
        table_name = random_string(5)
        conn.exec_driver_sql(
            f"""
CREATE OR REPLACE TEMP TABLE {table_name}
(EMPID INT, DEPT TEXT, JAN INT, FEB INT)
"""
        )
        conn.exec_driver_sql(
            f"""
INSERT INTO {table_name} VALUES
    (1, 'ELECTRONICS', 100, 200),
    (2, 'CLOTHES', 100, 300)
"""
        )
        results = conn.exec_driver_sql(
            f'SELECT * FROM {table_name} UNPIVOT(SALES FOR "" IN (JAN, FEB)) ORDER BY EMPID;'
        ).fetchall()  # normalize_name will be called
        assert results == [
            (1, "ELECTRONICS", "JAN", 100),
            (1, "ELECTRONICS", "FEB", 200),
            (2, "CLOTHES", "JAN", 100),
            (2, "CLOTHES", "FEB", 300),
        ]

        conn.exec_driver_sql(
            f"""
CREATE OR REPLACE TEMP TABLE {table_name}
(COL INT, "" INT)
"""
        )
        inspector = inspect(conn)
        columns = inspector.get_columns(table_name)  # denormalize_name will be called
        assert (
            len(columns) == 2
            and columns[0]["name"] == "col"
            and columns[1]["name"] == ""
        )


def test_snowflake_sqlalchemy_as_valid_client_type():
    engine = create_engine(
        URL(
            user=CONNECTION_PARAMETERS["user"],
            password=CONNECTION_PARAMETERS["password"],
            account=CONNECTION_PARAMETERS["account"],
            host=CONNECTION_PARAMETERS["host"],
            port=CONNECTION_PARAMETERS["port"],
            protocol=CONNECTION_PARAMETERS["protocol"],
        ),
        connect_args={"internal_application_name": "UnknownClient"},
    )
    with engine.connect() as conn:
        with pytest.raises(snowflake.connector.errors.NotSupportedError):
            conn.exec_driver_sql("select 1").cursor.fetch_pandas_all()

    engine = create_engine(
        URL(
            user=CONNECTION_PARAMETERS["user"],
            password=CONNECTION_PARAMETERS["password"],
            account=CONNECTION_PARAMETERS["account"],
            host=CONNECTION_PARAMETERS["host"],
            port=CONNECTION_PARAMETERS["port"],
            protocol=CONNECTION_PARAMETERS["protocol"],
        )
    )
    with engine.connect() as conn:
        conn.exec_driver_sql("select 1").cursor.fetch_pandas_all()

    try:
        snowflake.sqlalchemy.snowdialect._ENABLE_SQLALCHEMY_AS_APPLICATION_NAME = False
        origin_app = snowflake.connector.connection.DEFAULT_CONFIGURATION["application"]
        origin_internal_app_name = snowflake.connector.connection.DEFAULT_CONFIGURATION[
            "internal_application_name"
        ]
        origin_internal_app_version = (
            snowflake.connector.connection.DEFAULT_CONFIGURATION[
                "internal_application_version"
            ]
        )
        snowflake.connector.connection.DEFAULT_CONFIGURATION["application"] = (
            None,
            (type(None), str),
        )
        snowflake.connector.connection.DEFAULT_CONFIGURATION[
            "internal_application_name"
        ] = (
            "PythonConnector",
            (type(None), str),
        )
        snowflake.connector.connection.DEFAULT_CONFIGURATION[
            "internal_application_version"
        ] = (
            "3.0.0",
            (type(None), str),
        )
        engine = create_engine(
            URL(
                user=CONNECTION_PARAMETERS["user"],
                password=CONNECTION_PARAMETERS["password"],
                account=CONNECTION_PARAMETERS["account"],
                host=CONNECTION_PARAMETERS["host"],
                port=CONNECTION_PARAMETERS["port"],
                protocol=CONNECTION_PARAMETERS["protocol"],
            )
        )
        with engine.connect() as conn:
            conn.exec_driver_sql("select 1").cursor.fetch_pandas_all()
            assert (
                conn.connection.driver_connection._internal_application_name
                == "PythonConnector"
            )
            assert (
                conn.connection.driver_connection._internal_application_version
                == "3.0.0"
            )
    finally:
        snowflake.sqlalchemy.snowdialect._ENABLE_SQLALCHEMY_AS_APPLICATION_NAME = True
        snowflake.connector.connection.DEFAULT_CONFIGURATION["application"] = origin_app
        snowflake.connector.connection.DEFAULT_CONFIGURATION[
            "internal_application_name"
        ] = origin_internal_app_name
        snowflake.connector.connection.DEFAULT_CONFIGURATION[
            "internal_application_version"
        ] = origin_internal_app_version

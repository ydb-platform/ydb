#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy import (
    Column,
    Identity,
    Integer,
    MetaData,
    Sequence,
    String,
    Table,
    insert,
    select,
)
from sqlalchemy.sql import text
from sqlalchemy.sql.ddl import CreateTable


def test_table_with_sequence(engine_testaccount, db_parameters):
    """Snowflake does not guarantee generating sequence numbers without gaps.

    The generated numbers are not necessarily contiguous.
    https://docs.snowflake.com/en/user-guide/querying-sequences
    """
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/124
    test_table_name = "sequence"
    test_sequence_name = f"{test_table_name}_id_seq"
    metadata = MetaData()

    sequence_table = Table(
        test_table_name,
        metadata,
        Column(
            "id", Integer, Sequence(test_sequence_name, order=True), primary_key=True
        ),
        Column("data", String(39)),
    )

    autoload_metadata = MetaData()

    try:
        metadata.create_all(engine_testaccount)

        with engine_testaccount.begin() as conn:
            conn.execute(insert(sequence_table), ({"data": "test_insert_1"}))
            result = conn.execute(select(sequence_table)).fetchall()
            assert result == [(1, "test_insert_1")], result

            autoload_sequence_table = Table(
                test_table_name,
                autoload_metadata,
                autoload_with=engine_testaccount,
            )
            seq = Sequence(test_sequence_name, order=True)

            conn.execute(
                insert(autoload_sequence_table),
                (
                    {"data": "multi_insert_1"},
                    {"data": "multi_insert_2"},
                ),
            )
            conn.execute(
                insert(autoload_sequence_table),
                ({"data": "test_insert_2"},),
            )

            nextid = conn.execute(seq)
            conn.execute(
                insert(autoload_sequence_table),
                ({"id": nextid, "data": "test_insert_seq"}),
            )

            result = conn.execute(select(sequence_table)).fetchall()

            assert result == [
                (1, "test_insert_1"),
                (2, "multi_insert_1"),
                (3, "multi_insert_2"),
                (4, "test_insert_2"),
                (5, "test_insert_seq"),
            ], result

    finally:
        metadata.drop_all(engine_testaccount)


def test_table_with_autoincrement(engine_testaccount):
    """Snowflake does not guarantee generating sequence numbers without gaps.

    The generated numbers are not necessarily contiguous.
    https://docs.snowflake.com/en/user-guide/querying-sequences
    """
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/124
    test_table_name = "sequence"
    metadata = MetaData()
    autoincrement_table = Table(
        test_table_name,
        metadata,
        Column("id", Integer, autoincrement=True, primary_key=True),
        Column("data", String(39)),
    )

    select_stmt = select(autoincrement_table).order_by("id")

    try:
        with engine_testaccount.begin() as conn:
            conn.execute(text("ALTER SESSION SET NOORDER_SEQUENCE_AS_DEFAULT = FALSE"))
            metadata.create_all(conn)

            conn.execute(insert(autoincrement_table), ({"data": "test_insert_1"}))
            result = conn.execute(select_stmt).fetchall()
            assert result == [(1, "test_insert_1")]

            autoload_sequence_table = Table(
                test_table_name, MetaData(), autoload_with=engine_testaccount
            )
            conn.execute(
                insert(autoload_sequence_table),
                [
                    {"data": "multi_insert_1"},
                    {"data": "multi_insert_2"},
                ],
            )
            conn.execute(
                insert(autoload_sequence_table),
                [{"data": "test_insert_2"}],
            )
            result = conn.execute(select_stmt).fetchall()
            assert result == [
                (1, "test_insert_1"),
                (2, "multi_insert_1"),
                (3, "multi_insert_2"),
                (4, "test_insert_2"),
            ], result

    finally:
        metadata.drop_all(engine_testaccount)


def test_table_with_identity(sql_compiler):
    test_table_name = "identity"
    metadata = MetaData()
    identity_autoincrement_table = Table(
        test_table_name,
        metadata,
        Column(
            "id", Integer, Identity(start=1, increment=1, order=True), primary_key=True
        ),
        Column("identity_col_unordered", Integer, Identity(order=False)),
        Column("identity_col", Integer, Identity()),
    )
    create_table = CreateTable(identity_autoincrement_table)
    actual = sql_compiler(create_table)
    expected = (
        "CREATE TABLE identity ("
        "\tid INTEGER NOT NULL IDENTITY(1,1) ORDER, "
        "\tidentity_col_unordered INTEGER NOT NULL IDENTITY NOORDER, "
        "\tidentity_col INTEGER NOT NULL IDENTITY, "
        "\tPRIMARY KEY (id))"
    )
    assert actual == expected

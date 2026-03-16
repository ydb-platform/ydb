#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import operator
import random
import string
import sys
import textwrap
import uuid

import numpy as np
import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    Sequence,
    String,
    Table,
    select,
    text,
)

from snowflake.connector import ProgrammingError
from snowflake.connector.pandas_tools import make_pd_writer, pd_writer
from snowflake.sqlalchemy.compat import IS_VERSION_20


def _create_users_addresses_tables(engine_testaccount, metadata):
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
        Column("id", Integer, Sequence("address_id_seq"), primary_key=True),
        Column("user_id", None, ForeignKey("users.id")),
        Column("email_address", String, nullable=False),
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


def test_a_simple_read_sql(engine_testaccount):
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables(engine_testaccount, metadata)

    try:
        with engine_testaccount.connect() as conn:
            # inserts data with an implicitly generated id
            with conn.begin():
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

                df = pd.read_sql(
                    select(users).where(users.c.name == "jack"),
                    con=conn,
                )

                assert len(df.values) == 1
                assert df.values[0][0] == 1
                assert df.values[0][1] == "jack"
                assert hasattr(df, "id")
                assert hasattr(df, "name")
                assert hasattr(df, "fullname")
    finally:
        # drop tables
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_numpy_datatypes(engine_testaccount_with_numpy, db_parameters):
    with engine_testaccount_with_numpy.connect() as conn:
        try:
            specific_date = np.datetime64("2016-03-04T12:03:05.123456789")
            with conn.begin():
                conn.exec_driver_sql(
                    f"CREATE OR REPLACE TABLE {db_parameters['name']}(c1 timestamp_ntz)"
                )
                conn.exec_driver_sql(
                    f"INSERT INTO {db_parameters['name']}(c1) values(%s)",
                    (specific_date,),
                )
                df = pd.read_sql_query(
                    text(f"SELECT * FROM {db_parameters['name']}"), conn
                )
                assert df.c1.values[0] == specific_date
        finally:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {db_parameters['name']}")
            engine_testaccount_with_numpy.dispose()


def test_to_sql(engine_testaccount_with_numpy, db_parameters):
    with engine_testaccount_with_numpy.connect() as conn:
        total_rows = 10000
        conn.exec_driver_sql(
            textwrap.dedent(
                f"""\
                create or replace table src(c1 float)
                as select random(123) from table(generator(timelimit=>1))
                limit {total_rows}
                """
            )
        )
        conn.exec_driver_sql("create or replace table dst(c1 float)")
        tbl = pd.read_sql_query(text("select * from src"), conn)

        tbl.to_sql(
            "dst",
            engine_testaccount_with_numpy,
            if_exists="append",
            chunksize=1000,
            index=False,
        )
        df = pd.read_sql_query(text("select count(*) as cnt from dst"), conn)
        assert df.cnt.values[0] == total_rows


def test_no_indexes(engine_testaccount, db_parameters):
    with engine_testaccount.connect() as conn:
        data = pd.DataFrame([("1.0.0",), ("1.0.1",)])
        with pytest.raises(NotImplementedError) as exc:
            data.to_sql(
                "versions",
                schema=db_parameters["schema"],
                index=True,
                index_label="col1",
                con=conn,
                if_exists="replace",
            )
        assert str(exc.value) == "Snowflake does not support indexes"


def test_timezone(db_parameters, engine_testaccount, engine_testaccount_with_numpy):

    test_table_name = "".join([random.choice(string.ascii_letters) for _ in range(5)])

    sa_engine = engine_testaccount_with_numpy
    sa_engine2_raw_conn = engine_testaccount.raw_connection()

    with sa_engine.connect() as conn:

        with conn.begin():
            conn.exec_driver_sql(
                textwrap.dedent(
                    f"""\
                CREATE OR REPLACE TABLE {test_table_name}(
                    tz_col timestamp_tz,
                    ntz_col timestamp_ntz,
                    decimal_col decimal(10,1),
                    float_col float
                );
                """
                )
            )

            conn.exec_driver_sql(
                textwrap.dedent(
                    f"""\
                INSERT INTO {test_table_name}
                    SELECT
                        current_timestamp(),
                        current_timestamp()::timestamp_ntz,
                        to_decimal(.1, 10, 1),
                        .10;
                """
                )
            )
        try:
            qry = textwrap.dedent(
                f"""\
            SELECT
                tz_col,
                ntz_col,
                CONVERT_TIMEZONE('America/Los_Angeles', tz_col) AS tz_col_converted,
                CONVERT_TIMEZONE('America/Los_Angeles', ntz_col) AS ntz_col_converted,
                decimal_col,
                float_col
            FROM {test_table_name};
            """
            )

            result = pd.read_sql_query(text(qry), conn)
            result2 = pd.read_sql_query(qry, sa_engine2_raw_conn)
            # Check sqlalchemy engine result
            assert pd.api.types.is_datetime64tz_dtype(result.tz_col)
            assert not pd.api.types.is_datetime64tz_dtype(result.ntz_col)
            assert pd.api.types.is_datetime64tz_dtype(result.tz_col_converted)
            assert pd.api.types.is_datetime64tz_dtype(result.ntz_col_converted)
            assert np.issubdtype(result.decimal_col, np.float64)
            assert np.issubdtype(result.float_col, np.float64)
            # Check sqlalchemy raw connection result
            assert pd.api.types.is_datetime64tz_dtype(result2.TZ_COL)
            assert not pd.api.types.is_datetime64tz_dtype(result2.NTZ_COL)
            assert pd.api.types.is_datetime64tz_dtype(result2.TZ_COL_CONVERTED)
            assert pd.api.types.is_datetime64tz_dtype(result2.NTZ_COL_CONVERTED)
            assert np.issubdtype(result2.DECIMAL_COL, np.float64)
            assert np.issubdtype(result2.FLOAT_COL, np.float64)
        finally:
            conn.exec_driver_sql(f"DROP TABLE {test_table_name};")


def test_pandas_writeback(engine_testaccount):
    if IS_VERSION_20 and sys.version_info < (3, 8):
        pytest.skip(
            "In Python 3.7, this test depends on pandas features of which the implementation is incompatible with sqlachemy 2.0, and pandas does not support Python 3.7 anymore."
        )

    with engine_testaccount.connect() as conn:
        sf_connector_version_data = [
            ("snowflake-connector-python", "1.2.23"),
            ("snowflake-sqlalchemy", "1.1.1"),
            ("snowflake-connector-go", "0.0.1"),
            ("snowflake-go", "1.0.1"),
            ("snowflake-odbc", "3.12.3"),
        ]
        table_name = "driver_versions"
        # Note: column names have to be all upper case because our sqlalchemy connector creates it in a case insensitive way
        sf_connector_version_df = pd.DataFrame(
            sf_connector_version_data, columns=["NAME", "NEWEST_VERSION"]
        )
        sf_connector_version_df.to_sql(
            table_name, conn, index=False, method=pd_writer, if_exists="replace"
        )
        results = pd.read_sql_table(table_name, conn).rename(
            columns={"newest_version": "NEWEST_VERSION", "name": "NAME"}
        )
        assert results.equals(sf_connector_version_df)


@pytest.mark.parametrize("chunk_size", [5, 1])
@pytest.mark.parametrize(
    "compression",
    [
        "gzip",
    ],
)
@pytest.mark.parametrize("parallel", [1, 4])
@pytest.mark.parametrize("quote_identifiers", [True, False])
@pytest.mark.parametrize("auto_create_table", [True, False])
def test_pandas_make_pd_writer(
    engine_testaccount,
    chunk_size: int,
    compression: str,
    parallel: int,
    quote_identifiers: bool,
    auto_create_table: bool,
):
    table_name = f"test_table_{uuid.uuid4().hex}".upper()
    test_df = pd.DataFrame({"a": range(10), "b": range(10, 20)})

    def write_to_db():
        test_df.to_sql(
            table_name,
            engine_testaccount,
            index=False,
            method=make_pd_writer(
                chunk_size=chunk_size,
                compression=compression,
                parallel=parallel,
                quote_identifiers=quote_identifiers,
                auto_create_table=auto_create_table,
            ),
        )

    with engine_testaccount.connect() as conn:
        try:
            if quote_identifiers:
                with pytest.raises(
                    ProgrammingError,
                    match=r".*SQL compilation error.*\ninvalid identifier '\"a\"'.*",
                ):
                    write_to_db()
            else:
                write_to_db()
                results = sorted(
                    conn.exec_driver_sql(f"SELECT * FROM {table_name}").fetchall(),
                    key=operator.itemgetter(0),
                )
                # Verify that all 10 entries were written to the DB
                for i in range(10):
                    assert results[i] == (i, i + 10)
                assert len(results) == 10
        finally:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")


def test_pandas_invalid_make_pd_writer(engine_testaccount):
    table_name = f"test_table_{uuid.uuid4().hex}".upper()
    test_df = pd.DataFrame({"a": range(10), "b": range(10, 20)})

    with pytest.raises(
        ProgrammingError,
        match="Arguments 'table', 'conn', 'keys', and 'data_iter' are not supported parameters for make_pd_writer.",
    ):
        test_df.to_sql(
            table_name,
            engine_testaccount,
            index=False,
            method=make_pd_writer(conn=engine_testaccount),
        )

    with pytest.raises(
        ProgrammingError,
        match="Arguments 'conn', 'df', 'table_name', and 'schema' are not supported parameters for pd_writer.",
    ):
        test_df.to_sql(
            table_name,
            engine_testaccount,
            index=False,
            method=make_pd_writer(df=test_df),
        )


def test_percent_signs(engine_testaccount):
    if IS_VERSION_20 and sys.version_info < (3, 8):
        pytest.skip(
            "In Python 3.7, this test depends on pandas features of which the implementation is incompatible with sqlachemy 2.0, and pandas does not support Python 3.7 anymore."
        )

    table_name = f"test_table_{uuid.uuid4().hex}".upper()
    with engine_testaccount.connect() as conn:
        with conn.begin():
            conn.exec_driver_sql(
                f"CREATE OR REPLACE TEMP TABLE {table_name}(c1 int, c2 string)"
            )
            conn.exec_driver_sql(
                f"""
                INSERT INTO {table_name}(c1, c2) values
                (1, 'abc'),
                (2, 'def'),
                (3, 'ghi')
                """
            )

            not_like_sql = f"select * from {table_name} where c2 not like '%b%'"
            like_sql = f"select * from {table_name} where c2 like '%b%'"
            calculate_sql = "SELECT 1600 % 400 AS a, 1599 % 400 as b"
            if IS_VERSION_20:
                not_like_sql = sqlalchemy.text(not_like_sql)
                like_sql = sqlalchemy.text(like_sql)
                calculate_sql = sqlalchemy.text(calculate_sql)

            df = pd.read_sql(not_like_sql, conn)
            assert list(df.itertuples(index=False, name=None)) == [
                (2, "def"),
                (3, "ghi"),
            ]

            df = pd.read_sql(like_sql, conn)
            assert list(df.itertuples(index=False, name=None)) == [
                (1, "abc"),
            ]

            df = pd.read_sql(calculate_sql, conn)
            assert list(df.itertuples(index=False, name=None)) == [
                (0, 399),
            ]

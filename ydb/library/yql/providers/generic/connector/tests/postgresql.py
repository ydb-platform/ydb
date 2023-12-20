from pathlib import Path
from typing import Sequence

import ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 as data_source_pb2

from utils.comparator import data_outs_equal
from utils.database import Database
from utils.log import make_logger
from utils.postgresql import Client
from utils.schema import Schema
from utils.settings import Settings
from utils.sql import format_values_for_bulk_sql_insert

import test_cases.select_missing_database
import test_cases.select_missing_table
import test_cases.select_positive_common
import test_cases.select_pg_schema
import utils.dqrun as dqrun

LOGGER = make_logger(__name__)


def prepare_table(
    client: Client,
    database: Database,
    table_name: str,
    schema: Schema,
    data_in: Sequence,
    pg_schema: str = None,
):
    # create database
    with client.get_cursor("postgres") as (conn, cur):
        database_exists_stmt = database.exists(data_source_pb2.POSTGRESQL)
        LOGGER.debug(database_exists_stmt)
        cur.execute(database_exists_stmt)

        # database doesn't exist
        if not cur.fetchone():
            create_database_stmt = database.create(data_source_pb2.POSTGRESQL)
            LOGGER.debug(create_database_stmt)
            cur.execute(create_database_stmt)

        conn.commit()
        cur.close()

    # write data
    with client.get_cursor(database.name) as (conn, cur):
        # check if table exists
        if not pg_schema:
            check_table_stmt = f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='{table_name}')"
        else:
            check_table_stmt = f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='{table_name}' AND table_schema='{pg_schema}')"

        LOGGER.debug(check_table_stmt)
        cur.execute(check_table_stmt)

        if cur.fetchone()[0]:
            # no need to create table
            return

        if pg_schema:
            # create schema
            create_schema_stmt = f"CREATE SCHEMA IF NOT EXISTS {pg_schema}"
            LOGGER.debug(create_schema_stmt)
            cur.execute(create_schema_stmt)
            table_name = f"{pg_schema}.{table_name}"

        create_table_stmt = f"CREATE TABLE {table_name} ({schema.yql_column_list(data_source_pb2.POSTGRESQL)})"
        LOGGER.debug(create_table_stmt)
        cur.execute(create_table_stmt)

        values = format_values_for_bulk_sql_insert(data_in)

        insert_stmt = f"INSERT INTO {table_name} ({schema.columns.names_with_commas}) VALUES {values}"
        # TODO: these logs may be too big when working with big tables,
        # dump insert statement via yatest into file.
        LOGGER.debug(insert_stmt)
        cur.execute(insert_stmt)

        conn.commit()
        cur.close()


def select_positive(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    client: Client,
    test_case: test_cases.select_positive_common.TestCase,
):
    prepare_table(
        client=client,
        database=test_case.database,
        table_name=test_case.sql_table_name,
        schema=test_case.schema,
        data_in=test_case.data_in,
    )

    # read data
    where_statement = ""
    if test_case.select_where is not None:
        where_statement = f"WHERE {test_case.select_where.filter_expression}"
    yql_script = f"""
        {test_case.pragmas_sql_string}
        SELECT {test_case.select_what.yql_select_names}
        FROM {settings.postgresql.cluster_name}.{test_case.qualified_table_name}
        {where_statement}
    """
    LOGGER.debug(yql_script)
    result = dqrun_runner.run(
        test_dir=tmp_path,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert result.returncode == 0, result.stderr

    assert data_outs_equal(test_case.data_out, result.data_out_with_types), (
        test_case.data_out,
        result.data_out_with_types,
    )
    if test_case.check_output_schema:
        assert test_case.schema == result.schema, (test_case.schema, result.schema)


def select_missing_database(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    test_case: test_cases.select_missing_database.TestCase,
):
    # select table from database that does not exist

    yql_script = f"""
        SELECT *
        FROM {settings.postgresql.cluster_name}.{test_case.qualified_table_name}
    """
    LOGGER.debug(yql_script)
    result = dqrun_runner.run(
        test_dir=tmp_path,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert test_case.database.missing_database_msg(data_source_pb2.POSTGRESQL) in result.stderr, result.stderr


def select_missing_table(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    client: Client,
    test_case: test_cases.select_missing_table.TestCase,
):
    # create database but don't create table
    with client.get_cursor("postgres") as (conn, cur):
        database_exists_stmt = test_case.database.exists(data_source_pb2.POSTGRESQL)
        LOGGER.debug(database_exists_stmt)
        cur.execute(database_exists_stmt)

        # database doesn't exist
        if not cur.fetchone():
            create_database_stmt = test_case.database.create(data_source_pb2.POSTGRESQL)
            LOGGER.debug(create_database_stmt)
            cur.execute(create_database_stmt)

        conn.commit()
        cur.close()

    # read data
    yql_script = f"""
        SELECT *
        FROM {settings.postgresql.cluster_name}.{test_case.qualified_table_name}
    """
    LOGGER.debug(yql_script)
    result = dqrun_runner.run(
        test_dir=tmp_path,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert test_case.database.missing_table_msg(data_source_pb2.POSTGRESQL) in result.stderr, result.stderr


def select_pg_schema(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    client: Client,
    test_case: test_cases.select_pg_schema.TestCase,
):
    prepare_table(
        client=client,
        database=test_case.database,
        table_name=test_case.sql_table_name,
        schema=test_case.schema,
        data_in=test_case.data_in,
        pg_schema=test_case.pg_schema,
    )

    # read data
    yql_script = f"""
        SELECT {test_case.select_what.yql_select_names}
        FROM {settings.postgresql.cluster_name}.{test_case.qualified_table_name}
    """
    LOGGER.debug(yql_script)
    result = dqrun_runner.run(
        test_dir=tmp_path,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert result.returncode == 0, result.stderr

    assert data_outs_equal(test_case.data_out, result.data_out_with_types), (
        test_case.data_out,
        result.data_out_with_types,
    )

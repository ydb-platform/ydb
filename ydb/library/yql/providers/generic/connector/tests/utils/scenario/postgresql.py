from typing import Sequence

import ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 as data_source_pb2

import ydb.library.yql.providers.generic.connector.tests.utils.artifacts as artifacts
from ydb.library.yql.providers.generic.connector.tests.utils.comparator import assert_data_outs_equal
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger, debug_with_limit
from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.run.parent import Runner
from ydb.library.yql.providers.generic.connector.tests.utils.sql import format_values_for_bulk_sql_insert
from ydb.library.yql.providers.generic.connector.tests.utils.clients.postgresql import Client

import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as tc_select_positive_common
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_database as tc_select_missing_database
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_table as tc_select_missing_table

LOGGER = make_logger(__name__)


def prepare_table(
    test_name: str,
    client: Client,
    database: Database,
    table_name: str,
    schema: Schema,
    data_in: Sequence,
    pg_schema: str = None,
):
    # create database
    with client.get_cursor("postgres") as (conn, cur):
        database_exists_stmt = database.query_exists()
        debug_with_limit(LOGGER, database_exists_stmt)
        cur.execute(database_exists_stmt)

        # database doesn't exist
        if not cur.fetchone():
            create_database_stmt = database.query_create()
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
            debug_with_limit(LOGGER, create_schema_stmt)
            cur.execute(create_schema_stmt)
            table_name = f"{pg_schema}.{table_name}"

        create_table_stmt = f"CREATE TABLE {table_name} ({schema.yql_column_list(data_source_pb2.POSTGRESQL)})"
        LOGGER.debug(create_table_stmt)
        cur.execute(create_table_stmt)

        values = format_values_for_bulk_sql_insert(data_in)
        insert_stmt = f"INSERT INTO {table_name} ({schema.columns.names_with_commas}) VALUES {values}"
        # NOTE: these statement may be too big when working with big tables,
        # so with truncate logs and put full statement into directory with artifacts
        debug_with_limit(LOGGER, insert_stmt)
        artifacts.dump_str(insert_stmt, test_name, 'insert.sql')

        cur.execute(insert_stmt)
        conn.commit()
        cur.close()

    LOGGER.debug("Test table data initialized")


def select_positive(
    test_name: str,
    test_case: tc_select_positive_common.TestCase,
    settings: Settings,
    runner: Runner,
    client: Client,
):
    prepare_table(
        test_name=test_name,
        client=client,
        database=test_case.database,
        table_name=test_case.table_name,
        schema=test_case.schema,
        data_in=test_case.data_in,
    )

    # read data
    where_statement = ""
    if test_case.select_where is not None:
        where_statement = "WHERE " + test_case.select_where.render(
            cluster_name=settings.postgresql.cluster_name,
            table_name=test_case.table_name,
        )
    yql_script = f"""
        {test_case.pragmas_sql_string}
        SELECT {test_case.select_what.yql_select_names}
        FROM {settings.postgresql.cluster_name}.{test_case.table_name}
        {where_statement}
    """
    result = runner.run(
        test_name=test_name,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert result.returncode == 0, result.output

    assert_data_outs_equal(test_case.data_out, result.data_out_with_types), (
        test_case.data_out,
        result.data_out_with_types,
    )

    if test_case.check_output_schema:
        assert test_case.schema == result.schema, (test_case.schema, result.schema)


def select_missing_database(
    test_name: str,
    test_case: tc_select_missing_database.TestCase,
    settings: Settings,
    runner: Runner,
):
    # select table from database that does not exist

    yql_script = f"""
        SELECT *
        FROM {settings.postgresql.cluster_name}.{test_case.table_name}
    """
    result = runner.run(
        test_name=test_name,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert test_case.database.missing_database_msg() in result.output, result.output


def select_missing_table(
    test_name: str,
    test_case: tc_select_missing_table.TestCase,
    settings: Settings,
    runner: Runner,
    client: Client,
):
    # create database but don't create table
    with client.get_cursor("postgres") as (conn, cur):
        database_exists_stmt = test_case.database.query_exists()
        debug_with_limit(LOGGER, database_exists_stmt)
        cur.execute(database_exists_stmt)

        # database doesn't exist
        if not cur.fetchone():
            create_database_stmt = test_case.database.query_create()
            debug_with_limit(LOGGER, create_database_stmt)
            cur.execute(create_database_stmt)

        conn.commit()
        cur.close()

    # read data
    yql_script = f"""
        SELECT *
        FROM {settings.postgresql.cluster_name}.{test_case.table_name}
    """
    result = runner.run(
        test_name=test_name,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert test_case.database.missing_table_msg() in result.output, result.output


def select_pg_schema(
    test_name: str,
    test_case: tc_select_positive_common.TestCase,
    settings: Settings,
    runner: Runner,
    client: Client,
):
    prepare_table(
        test_name=test_name,
        client=client,
        database=test_case.database,
        table_name=test_case.table_name,
        schema=test_case.schema,
        data_in=test_case.data_in,
        pg_schema=test_case.pg_schema,
    )

    # read data
    yql_script = f"""
        SELECT {test_case.select_what.yql_select_names}
        FROM {settings.postgresql.cluster_name}.{test_case.table_name}
    """
    result = runner.run(
        test_name=test_name,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert result.returncode == 0, result.output

    assert_data_outs_equal(test_case.data_out, result.data_out_with_types), (
        test_case.data_out,
        result.data_out_with_types,
    )

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
from ydb.library.yql.providers.generic.connector.tests.utils.clients.clickhouse import Client

import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_database as tc_select_missing_database
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_table as tc_select_missing_table
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as tc_select_positive_common

LOGGER = make_logger(__name__)


def prepare_table(
    test_name: str,
    client: Client,
    database: Database,
    table_name: str,
    schema: Schema,
    data_in: Sequence,
):
    dbTable = f"{database.name}.{table_name}"

    # create database
    create_database_stmt = database.query_create()
    LOGGER.debug(create_database_stmt)
    client.command(create_database_stmt)

    # check if table exists
    check_table_stmt = f"EXISTS TABLE {dbTable}"
    LOGGER.debug(check_table_stmt)
    res = client.command(check_table_stmt)
    assert res in (0, 1), res
    if res == 1:
        # no need to create table
        return

    # create table
    create_table_stmt = f"CREATE TABLE {dbTable} ({schema.yql_column_list(data_source_pb2.CLICKHOUSE)}) ENGINE = Memory"
    LOGGER.debug(create_table_stmt)
    client.command(create_table_stmt)

    # write data
    values = format_values_for_bulk_sql_insert(data_in)
    insert_stmt = f"INSERT INTO {dbTable} (*) VALUES {values}"
    # NOTE: these statement may be too big when working with big tables,
    # so with truncate logs and put full statement into directory with artifacts
    debug_with_limit(LOGGER, insert_stmt)
    artifacts.dump_str(insert_stmt, test_name, 'insert.sql')
    client.command(insert_stmt)


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

    where_statement = ""
    if test_case.select_where is not None:
        where_statement = "WHERE " + test_case.select_where.render(
            cluster_name=settings.clickhouse.cluster_name,
            table_name=test_case.table_name,
        )

    # NOTE: to assert equivalence we have to add explicit ORDER BY,
    # because Clickhouse's output will be randomly ordered otherwise.
    order_by_expression = ""
    order_by_column_name = test_case.select_what.order_by_column_name
    if order_by_column_name:
        order_by_expression = f"ORDER BY {order_by_column_name}"

    yql_script = f"""
        {test_case.pragmas_sql_string}
        SELECT {test_case.select_what.yql_select_names}
        FROM {settings.clickhouse.cluster_name}.{test_case.table_name}
        {where_statement}
        {order_by_expression}
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
    # select table from the database that does not exist
    yql_script = f"""
        SELECT *
        FROM {settings.clickhouse.cluster_name}.{test_case.table_name}
    """
    result = runner.run(
        test_name=test_name,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert test_case.database.missing_database_msg() in result.output, (
        test_case.database.missing_database_msg(),
        result.output,
    )


def select_missing_table(
    test_name: str,
    test_case: tc_select_missing_table.TestCase,
    settings: Settings,
    runner: Runner,
    client: Client,
):
    # create database, but don't create table
    create_database_stmt = test_case.database.query_create()
    LOGGER.debug(create_database_stmt)
    client.command(create_database_stmt)

    yql_script = f"""
        SELECT *
        FROM {settings.clickhouse.cluster_name}.{test_case.table_name}
    """
    result = runner.run(
        test_name=test_name,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )

    assert test_case.database.missing_table_msg() in result.output, result.output

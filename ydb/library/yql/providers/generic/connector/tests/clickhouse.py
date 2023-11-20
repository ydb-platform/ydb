from pathlib import Path
from typing import Sequence

import ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 as data_source_pb2

from utils.clickhouse import Client
from utils.database import Database
from utils.log import make_logger
from utils.schema import Schema
from utils.settings import Settings
import test_cases.select_missing_database
import test_cases.select_missing_table
import test_cases.select_positive
import utils.dqrun as dqrun

LOGGER = make_logger(__name__)


def prepare_table(
    client: Client,
    database: Database,
    table_name: str,
    schema: Schema,
    data_in: Sequence,
):
    dbTable = f'{database.name}.{table_name}'

    # create database
    create_database_stmt = database.create(data_source_pb2.CLICKHOUSE)
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
    create_table_stmt = f'CREATE TABLE {dbTable} ({schema.yql_column_list(data_source_pb2.CLICKHOUSE)}) ENGINE = Memory'
    LOGGER.debug(create_table_stmt)
    client.command(create_table_stmt)

    # write data
    for row in data_in:
        # prepare string with serialized data
        values_dump = []
        for val in row:
            if isinstance(val, str):
                values_dump.append(f"'{val}'")
            elif val is None:
                values_dump.append('NULL')
            else:
                values_dump.append(str(val))
        values = ", ".join(values_dump)

        insert_stmt = f"INSERT INTO {dbTable} (*) VALUES ({values})"
        LOGGER.debug(insert_stmt)
        client.command(insert_stmt)


def select_positive(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    client: Client,
    test_case: test_cases.select_positive.TestCase,
):
    prepare_table(
        client=client,
        database=test_case.database,
        table_name=test_case.sql_table_name,
        schema=test_case.schema,
        data_in=test_case.data_in,
    )

    # NOTE: to assert equivalence we have to add explicit ORDER BY,
    # because Clickhouse's output will be randomly ordered otherwise.
    where_statement = ''
    if test_case.select_where is not None:
        where_statement = f'WHERE {test_case.select_where.filter_expression}'
    order_by_expression = ''
    order_by_column_name = test_case.select_what.order_by_column_name
    if order_by_column_name:
        order_by_expression = f'ORDER BY {order_by_column_name}'
    yql_script = f'''
        {test_case.pragmas_sql_string}
        SELECT {test_case.select_what.yql_select_names}
        FROM {settings.clickhouse.cluster_name}.{test_case.qualified_table_name}
        {where_statement}
        {order_by_expression}
    '''
    result = dqrun_runner.run(test_dir=tmp_path, script=yql_script, generic_settings=test_case.generic_settings)

    assert test_case.data_out == result.data_out_with_types, (test_case.data_out, result.data_out_with_types)
    if test_case.check_output_schema:
        assert test_case.schema == result.schema, (test_case.schema, result.schema)


def select_missing_database(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    test_case: test_cases.select_missing_database.TestCase,
):
    # select table from the database that does not exist
    yql_script = f'''
        SELECT *
        FROM {settings.clickhouse.cluster_name}.{test_case.qualified_table_name}
    '''
    result = dqrun_runner.run(test_dir=tmp_path, script=yql_script, generic_settings=test_case.generic_settings)

    assert test_case.database.missing_database_msg(data_source_pb2.CLICKHOUSE) in result.stderr, result.stderr


def select_missing_table(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    client: Client,
    test_case: test_cases.select_missing_table.TestCase,
):
    # create database, but don't create table
    create_database_stmt = test_case.database.create(data_source_pb2.CLICKHOUSE)
    LOGGER.debug(create_database_stmt)
    client.command(create_database_stmt)

    yql_script = f'''
        SELECT *
        FROM {settings.clickhouse.cluster_name}.{test_case.qualified_table_name}
    '''
    result = dqrun_runner.run(test_dir=tmp_path, script=yql_script, generic_settings=test_case.generic_settings)

    assert test_case.database.missing_table_msg(data_source_pb2.CLICKHOUSE) in result.stderr, result.stderr

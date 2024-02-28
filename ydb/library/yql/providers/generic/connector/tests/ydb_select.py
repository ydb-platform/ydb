from pathlib import Path
from typing import Sequence

import ydb
import ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 as data_source_pb2

from utils.ydb import Client
from utils.comparator import data_outs_equal
from utils.database import Database
from utils.log import make_logger
from utils.schema import Schema
from utils.settings import Settings
from utils.sql import format_values_for_bulk_sql_insert

import test_cases.select_missing_database
import test_cases.select_missing_table
import test_cases.select_positive_common
import utils.dqrun as dqrun

LOGGER = make_logger(__name__)

def prepare_table(
    client: Client,
    table_name: str,
    schema: Schema,
    data_in: Sequence,
):
    with ydb.SessionPool(client) as pool:

        # create table
        def create_table(session):
            create_table_stmt = f"CREATE TABLE `{table_name}` ({schema.yql_column_list(data_source_pb2.YDB)}, PRIMARY KEY ({schema.columns.names[1]}))"

            LOGGER.debug(create_table_stmt)
            session.execute_scheme(create_table_stmt)
        LOGGER.debug("create_table")
        pool.retry_operation_sync(create_table)

        values = format_values_for_bulk_sql_insert(data_in)

        # write data
        def insert(session):
            insert_stmt = f"UPSERT INTO `{table_name}` ({schema.columns.names_with_commas}) VALUES {values};"
            LOGGER.debug(insert_stmt)
            session.transaction().execute(insert_stmt,
                                          commit_tx=True,
                                          )

        LOGGER.debug("insert start")
        pool.retry_operation_sync(insert)
        LOGGER.debug("insert end")


def select_positive(
    test_name: str,
    settings: Settings,
    runner: dqrun.Runner,
    client: Client,
    test_case: test_cases.select_positive_common.TestCase,
):
    prepare_table(
        client=client,
        table_name=test_case.sql_table_name,
        schema=test_case.schema,
        data_in=test_case.data_in,
    )

    yql_script = f"""
        SELECT {test_case.select_what.yql_select_names}
        FROM {settings.ydb.cluster_name}.{test_case.qualified_table_name}
    """
    LOGGER.debug('runner.run')
    result = runner.run(
        test_name=test_name,
        script=yql_script,
        generic_settings=test_case.generic_settings,
    )
    LOGGER.debug("assert1")
    assert result.returncode == 0, result.stderr

    LOGGER.debug("assert2")
    assert data_outs_equal(test_case.data_out, result.data_out_with_types), (
        test_case.data_out,
        result.data_out_with_types,
    )
    if test_case.check_output_schema:
        assert test_case.schema == result.schema, (test_case.schema, result.schema)

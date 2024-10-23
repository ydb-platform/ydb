from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger
from ydb.library.yql.providers.generic.connector.tests.utils.comparator import (
    assert_data_outs_equal,
    assert_schemas_equal,
)
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.run.parent import Runner

import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_database as tc_select_missing_database
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_table as tc_select_missing_table
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as tc_select_positive_common

LOGGER = make_logger(__name__)


def select_positive(
    test_name: str,
    test_case: tc_select_positive_common.TestCase,
    settings: Settings,
    runner: Runner,
):
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
        assert_schemas_equal(test_case.schema, result.schema)


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
):
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

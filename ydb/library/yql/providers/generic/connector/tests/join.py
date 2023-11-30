from pathlib import Path
import utils.postgresql

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind

from utils.comparator import data_outs_equal
from utils.log import make_logger
from utils.runner import Runner
from utils.settings import Settings

import clickhouse
import postgresql
import test_cases.join

LOGGER = make_logger(__name__)


def join(
    tmp_path: Path,
    settings: Settings,
    runner: Runner,
    clickhouse_client: clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.join.TestCase,
):
    # prepare tables
    for data_source in test_case.data_sources:
        match data_source.kind:
            case EDataSourceKind.CLICKHOUSE:
                clickhouse.prepare_table(
                    client=clickhouse_client,
                    database=data_source.database,
                    table_name=data_source.database.sql_table_name(data_source.table.name),
                    data_in=data_source.table.data_in,
                    schema=data_source.table.schema,
                )
            case EDataSourceKind.POSTGRESQL:
                postgresql.prepare_table(
                    client=postgresql_client,
                    database=data_source.database,
                    table_name=data_source.database.sql_table_name(data_source.table.name),
                    data_in=data_source.table.data_in,
                    schema=data_source.table.schema,
                )
            case _:
                raise Exception(f'invalid data source: {test_case.data_source_kind}')

    # run join
    yql_script = test_case.make_sql(settings)

    result = runner.run(test_dir=tmp_path, script=yql_script, generic_settings=test_case.generic_settings)

    assert result.returncode == 0, result.stderr

    assert data_outs_equal(test_case.data_out, result.data_out_with_types), (
        test_case.data_out,
        result.data_out_with_types,
    )

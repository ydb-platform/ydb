from pathlib import Path
import utils.postgresql

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind

from utils.settings import Settings
from utils.log import make_logger
import clickhouse
import postgresql
import test_cases.join
import utils.dqrun as dqrun

LOGGER = make_logger(__name__)


def join(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
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
    dss = test_case.data_sources
    ds_head, ds_tail = dss[0], dss[1:]

    yql_script_parts = [
        '\nSELECT',
        test_case.yql_select_what,
        f'FROM {ds_head.yql_aliased_name(settings.get_cluster_name(ds_head.kind))} ',
        '\n'.join(
            f'JOIN {ds.yql_aliased_name(settings.get_cluster_name(ds.kind))} '
            + f'ON {ds_head.alias}.id = {ds.alias}.id'
            for ds in ds_tail
        ),
        f'ORDER BY {ds_head.alias}_id',
    ]
    yql_script = "\n".join(yql_script_parts)

    result = dqrun_runner.run(test_dir=tmp_path, script=yql_script, generic_settings=test_case.generic_settings)

    assert test_case.data_out == result.data_out_with_types, (test_case.data_out, result.data_out_with_types)

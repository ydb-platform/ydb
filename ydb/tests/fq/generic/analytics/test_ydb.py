import logging

import ydb.public.api.protos.ydb_value_pb2 as ydb

from ydb.library.yql.providers.generic.connector.tests.utils.one_time_waiter import OneTimeWaiter
from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind
import conftest

one_time_waiter = OneTimeWaiter(
    data_source_kind=EGenericDataSourceKind.YDB,
    docker_compose_file_path=conftest.docker_compose_file_path,
    expected_tables=["simple_table", "dummy_table"],
)


class TestYdb:
    def test_simple(self, client, create_source):
        table_name = 'simple_table'
        conn_name = f'conn_{table_name}'

        one_time_waiter.wait()

        create_source(conn_name, "ydb")

        sql = fR'''
            SELECT *
            FROM {conn_name}.{table_name};
            '''

        result_set = client.query(sql)[0]
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "number"
        assert result_set.columns[0].type == ydb.Type(
            optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.INT32))
        )
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 1
        assert result_set.rows[1][0] == 2
        assert result_set.rows[2][0] == 3

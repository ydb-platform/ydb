import logging
import pytest

import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v2

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.fq.generic.utils.settings import Settings


class TestPostgreSQL:
    @yq_v2
    @pytest.mark.parametrize("fq_client", [{"folder_id": "my_folder"}], indirect=True)
    def test_simple(self, fq_client: FederatedQueryClient, settings: Settings):
        table_name = 'simple_table'
        conn_name = f'conn_{table_name}'
        query_name = f'query_{table_name}'

        fq_client.create_postgresql_connection(
            name=conn_name,
            database_name=settings.postgresql.dbname,
            database_id='postgresql_cluster_id',
            login=settings.postgresql.username,
            password=settings.postgresql.password,
        )

        sql = fR'''
            SELECT *
            FROM {conn_name}.{table_name};
            '''

        query_id = fq_client.create_query(query_name, sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        fq_client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = fq_client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "number"
        assert result_set.columns[0].type == ydb.Type(
            optional_type=ydb.OptionalType(item=ydb.Type(type_id=ydb.Type.INT32))
        )
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].int32_value == 1
        assert result_set.rows[1].items[0].int32_value == 2
        assert result_set.rows[2].items[0].int32_value == 3

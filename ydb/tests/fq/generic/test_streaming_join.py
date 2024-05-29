import pytest
import os

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.fq.generic.utils.settings import Settings


class TestStreamingJoin(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": "tests-fq-generic-ydb:2136"}], indirect=True)
    @pytest.mark.parametrize("fq_client", [{"folder_id": "my_folder"}], indirect=True)
    def test_simple(self, kikimr, fq_client: FederatedQueryClient, settings: Settings, yq_version):
        self.init_topics(f"pq_yq_streaming_test_simple{yq_version}")
        fq_client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        table_name = 'join_table'
        ydb_conn_name = f'ydb_conn_{table_name}'

        fq_client.create_ydb_connection(
            name=ydb_conn_name,
            database_id=settings.ydb.dbname,
        )

        sql = R'''
            $input = SELECT * FROM myyds.`{input_topic}`;

            $enriched = select e.Data as Data
                from
                    $input as e
                left join
                    ydb_conn_{table_name}.{table_name} as u
                on(e.Data = CAST(u.id as String))
            ;

            insert into myyds.`{output_topic}`
            select * from $enriched;
            '''.format(
            input_topic=self.input_topic, output_topic=self.output_topic, table_name=table_name
        )

        query_id = fq_client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        fq_client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        messages = ['A', 'B', 'C']
        self.write_stream(messages)

        read_data = self.read_stream(len(messages))
        assert read_data == messages

        fq_client.abort_query(query_id)
        fq_client.wait_query(query_id)

        describe_response = fq_client.describe_query(query_id)
        status = describe_response.result.query.meta.status
        assert not describe_response.issues, str(describe_response.issues)
        assert status == fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.ComputeStatus.Name(status)

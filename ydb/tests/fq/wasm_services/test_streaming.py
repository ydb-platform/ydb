"""Streaming enrichment uses DQ checkpoints and recreates WASM after recovery."""
import os
import time

import pytest

from ydb.public.api.protos.draft import fq_pb2 as fq
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestStreamingServices(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize('protocol', ['http', 'grpc'])
    @pytest.mark.parametrize('batch_config', [{'streaming': True, 'rows': 1}], indirect=True)
    def test_checkpoint_and_worker_recovery(self, client, kikimr, services, protocol, batch_config):
        self.init_topics('wasm_stream_' + protocol)
        services.delay_timeout = 180
        client.create_yds_connection('events', os.getenv('YDB_DATABASE'), os.getenv('YDB_ENDPOINT'))
        sql = f"""
            PRAGMA dq.MaxTasksPerStage='1';
            $input = SELECT STREAM Unwrap(CAST(Data AS Uint64)) AS id FROM events.`{self.input_topic}`;
            $profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
                WITH CONNECTION='profiles_{protocol}', INPUT_TYPE=Struct<id:Uint64>,
                     OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
            INSERT INTO events.`{self.output_topic}`
            SELECT STREAM CAST(id AS String) || ':' || CAST(score AS String) AS Data FROM $profiles;
        """
        query_id = client.create_query('wasm_streaming', sql, type=fq.QueryContent.STREAMING).result.query_id
        try:
            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
            plane = kikimr.compute_plane
            plane.wait_zero_checkpoint(query_id)
            self.write_stream(['42'])
            assert self.read_stream(1) == ['42:97']
            plane.wait_completed_checkpoints(query_id, plane.get_completed_checkpoints(query_id) + 1)

            # Hold a real RPC across a checkpoint attempt. A barrier must not
            # overtake its result, even though the source offset was consumed.
            self.write_stream(['999'])
            assert services.started.wait(30)
            before = plane.get_completed_checkpoints(query_id)
            deadline = time.monotonic() + 15
            while plane.get_inprogress_checkpoints(query_id) == 0:
                assert time.monotonic() < deadline
                time.sleep(0.1)
            time.sleep(2)  # two checkpoint periods while the response is withheld
            assert plane.get_completed_checkpoints(query_id) <= before + 1

            # Restart the actual compute process while its compartment owns an
            # uncheckpointed call. DQ must replay 999, but not checkpointed 42.
            nodes = list(plane.kikimr_cluster.nodes.values())
            for node in nodes:
                node.kill()
            for node in nodes:
                node.start()
            deadline = time.monotonic() + 120
            while True:
                with services.lock:
                    repeated = sum(kind == protocol and ids == 999 for kind, ids, _ in services.requests)
                if repeated >= 2:
                    break
                assert time.monotonic() < deadline, client.describe_query(query_id).result.query
                time.sleep(0.2)
            services.release.set()
            self.write_stream(['43'])
            assert sorted(self.read_stream(2)) == ['43:97', '999:97']
            plane.wait_completed_checkpoints(query_id, plane.get_completed_checkpoints(query_id) + 1, timeout=60)
            with services.lock:
                assert sum(kind == protocol and ids == 42 for kind, ids, _ in services.requests) == 1
        finally:
            services.release.set()
            client.abort_query(query_id)

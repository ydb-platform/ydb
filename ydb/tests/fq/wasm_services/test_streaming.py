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

    @yq_v1
    @pytest.mark.parametrize(
        'batch_config', [
            {'streaming': True, 'rows': 1, 'buffered_rows': 2},
            {'streaming': True, 'rows': 2, 'buffered_rows': 2},
        ], indirect=True,
    )
    def test_slow_downstream_checkpoint_recovery(self, client, kikimr, services, downstream_service, batch_config):
        self.init_topics('wasm_slow_downstream_' + str(batch_config['rows']))
        client.create_yds_connection('events', os.getenv('YDB_DATABASE'), os.getenv('YDB_ENDPOINT'))
        client.create_external_service_connection(
            'slow_profiles', f'http://127.0.0.1:{downstream_service.http.server_port}', insecure=True,
        )
        sql = f"""
            PRAGMA dq.MaxTasksPerStage='1';
            $input = SELECT STREAM Unwrap(CAST(Data AS Uint64)) AS id FROM events.`{self.input_topic}`;
            $first = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
                WITH CONNECTION='profiles_http', INPUT_TYPE=Struct<id:Uint64>,
                     OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
            $middle = SELECT STREAM id FROM $first;
            $second = PROCESS $middle USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
                WITH CONNECTION='slow_profiles', INPUT_TYPE=Struct<id:Uint64>,
                     OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
            INSERT INTO events.`{self.output_topic}` SELECT STREAM CAST(id AS String) AS Data FROM $second;
        """
        query_id = client.create_query('slow_downstream', sql, type=fq.QueryContent.STREAMING).result.query_id
        plane = kikimr.compute_plane
        try:
            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
            plane.wait_zero_checkpoint(query_id)
            self.write_stream(['42'])
            assert self.read_stream(1) == ['42']
            plane.wait_completed_checkpoints(query_id, plane.get_completed_checkpoints(query_id) + 1)

            # A second WASM stage stops consuming while the first stage keeps
            # producing. A two-row transform buffer propagates backpressure;
            # exact exhausted-channel-credit ordering is covered by the native test.
            downstream_service.pause_all = True
            rows = [str(i) for i in range(1000, 1200)]
            before = plane.get_completed_checkpoints(query_id)
            self.write_stream(rows)
            assert downstream_service.started.wait(30), client.describe_query(query_id).result.query
            deadline = time.monotonic() + 30
            while plane.get_inprogress_checkpoints(query_id) == 0:
                assert time.monotonic() < deadline, client.describe_query(query_id).result.query
                time.sleep(0.1)
            time.sleep(2)
            assert plane.get_completed_checkpoints(query_id) <= before + 1
            downstream_service.release.set()
            assert sorted(self.read_stream(len(rows), timeout=120)) == rows
            plane.wait_completed_checkpoints(query_id, plane.get_completed_checkpoints(query_id) + 1, timeout=60)

            # Restore the completed checkpoint after relieving backpressure.
            # Reading through the sentinel detects a replayed row in the sink.
            nodes = list(plane.kikimr_cluster.nodes.values())
            for node in nodes:
                node.kill()
            for node in nodes:
                node.start()
            self.write_stream(['43'])
            assert self.read_stream(1, timeout=120) == ['43']
            plane.wait_completed_checkpoints(query_id, plane.get_completed_checkpoints(query_id) + 1, timeout=60)
        finally:
            downstream_service.release.set()
            client.abort_query(query_id)

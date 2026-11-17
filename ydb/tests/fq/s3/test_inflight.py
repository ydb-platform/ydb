import time

import boto3
import pytest

from ydb.tests.fq.streaming_common.common import counter_nodes, get_sensors


class TestS3Inflight:
    @staticmethod
    def run(kikimr, client, sql, limit, sensor):
        future = kikimr.ydb_client.query_async(sql, timeout=120)
        deadline = time.monotonic() + 130
        observed_counter = False
        while True:
            for node_id in counter_nodes(kikimr.cluster):
                value = get_sensors(kikimr.cluster, node_id, 'utils').find_sensor(
                    {'subcomponent': 'http_gateway', 'sensor': sensor}
                )
                if value is None:
                    continue
                observed_counter = True
                assert value <= limit, f'{sensor}={value} exceeds {limit} on node {node_id}'
            if future.done():
                result = future.result(timeout=1)[0]
                assert observed_counter, f'HTTP gateway counter {sensor} was not exposed'
                return result
            assert time.monotonic() < deadline, 'S3 SQL query did not complete'
            time.sleep(0.1)

    @staticmethod
    def prepare(s3, client, unique_prefix, count):
        resource = boto3.resource(
            's3', endpoint_url=s3.s3_url, aws_access_key_id='key', aws_secret_access_key='secret_key'
        )
        bucket = resource.Bucket('inflight')
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()
        for i in range(count):
            bucket.put_object(Key=f'fruits{i}.csv', Body='Fruit,Price,Weight\nBanana,3,100\nApple,2,22\nPear,15,33')
        source = client.create_external_data_source(unique_prefix + 'source', 'inflight').name
        return f'`{client.path(source)}`.`*`'

    @pytest.mark.parametrize('kikimr_params', [{'inflight': 1}, {'inflight': 2}, {'inflight': 5}], indirect=True)
    def test_inflight(self, kikimr, s3, client, unique_prefix, kikimr_params):
        # HTTP requests are serialized at limit=1. This is enough files to
        # exercise all limits without turning the test into a throughput run.
        file_count = 30
        source = self.prepare(s3, client, unique_prefix, file_count)
        sql = f'''PRAGMA ydb.MaxTasksPerStage='1';
            SELECT COUNT(*) AS cnt FROM {source}
            WITH (FORMAT=csv_with_names, SCHEMA=(Fruit String NOT NULL, Price Int32 NOT NULL, Weight Int32 NOT NULL));'''
        result = self.run(kikimr, client, sql, kikimr_params.param['inflight'], 'InFlightStreams')
        assert result.rows[0]['cnt'] == file_count * 3
        sql = f'''SELECT COUNT(*) AS cnt FROM {source} WITH (FORMAT=raw, SCHEMA=(Data String NOT NULL));'''
        result = self.run(kikimr, client, sql, kikimr_params.param['inflight'], 'InFlight')
        assert result.rows[0]['cnt'] == file_count

    @pytest.mark.parametrize('kikimr_params', [{'inflight': 1, 'data_inflight': 1}], indirect=True)
    def test_data_inflight(self, kikimr, s3, client, unique_prefix):
        source = self.prepare(s3, client, unique_prefix, 10)
        sql = f'''SELECT COUNT(*) AS cnt FROM {source}
            WITH (FORMAT=csv_with_names, SCHEMA=(Fruit String NOT NULL, Price Int32 NOT NULL, Weight Int32 NOT NULL));'''
        result = self.run(kikimr, client, sql, 1, 'InFlightStreams')
        assert result.rows[0]['cnt'] == 30

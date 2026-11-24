import boto3

from ydb.tests.fq.s3.s3_helpers import create_bucket_and_put_object
from ydb.tests.fq.streaming_common.common import StreamingTestBase


class TestEarlyFinish(StreamingTestBase):
    def test_early_finish(self, kikimr, s3, client, unique_prefix):
        create_bucket_and_put_object(s3.s3_url, 'early-finish', 'a.txt', 'A')
        resource = boto3.resource(
            's3', endpoint_url=s3.s3_url, aws_access_key_id='key', aws_secret_access_key='secret_key'
        )
        resource.Bucket('early-finish').put_object(Key='c.txt', Body='C')
        source = client.create_external_data_source(unique_prefix + 'source', 'early-finish').name
        topic, endpoint = self.get_input_name(kikimr, unique_prefix + 'early_finish', True, lambda name: name)
        sql = f'''
            SELECT input_rows.Data AS Data1, dictionary.Data AS Data2
            FROM {topic} WITH (STREAMING='TRUE', FORMAT=raw, SCHEMA=(Data String NOT NULL)) AS input_rows
            INNER JOIN (
                SELECT Data FROM `{client.path(source)}`.`*`
                WITH (FORMAT=raw, SCHEMA=(Data String NOT NULL))
                ORDER BY Data DESC
            ) AS dictionary ON input_rows.Data = dictionary.Data
            LIMIT 2;
        '''
        try:
            future = client.start_topic_query(
                sql, kikimr.get_database_name() + '/' + self.input_topic, self.consumer_name
            )
            self.write_stream(['A', 'B', 'C'], endpoint=endpoint)
            result = future.result(timeout=70)[0]
            assert [(row['Data1'], row['Data2']) for row in result.rows] == [(b'A', b'A'), (b'C', b'C')]
        finally:
            kikimr.ydb_client.query(f'DROP TOPIC {topic};')

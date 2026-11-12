from ydb.tests.fq.s3.s3_helpers import create_bucket_and_put_object
from ydb.tests.fq.streaming_common.common import StreamingTestBase


class TestTopicS3Join(StreamingTestBase):
    def test_grace_join(self, kikimr, s3, client, unique_prefix):
        create_bucket_and_put_object(s3.s3_url, 'grace-join', 'fruits.csv', 'Fruit,Price\nBanana,3\nApple,2\nPear,15')
        source = client.create_external_data_source(unique_prefix + 'source', 'grace-join').name
        topic, endpoint = self.get_input_name(kikimr, unique_prefix + 'grace_join', True, lambda name: name)
        sql = f'''
            PRAGMA ydb.HashJoinMode='grace';
            SELECT input_rows.Data AS Fruit, dictionary.Price AS Price
            FROM {topic} WITH (STREAMING='TRUE', FORMAT=raw, SCHEMA=(Data String NOT NULL)) AS input_rows
            LEFT JOIN (
                SELECT Fruit, Price FROM `{client.path(source)}`.`fruits.csv`
                WITH (FORMAT=csv_with_names, SCHEMA=(Fruit String NOT NULL, Price Int32 NOT NULL))
            ) AS dictionary ON input_rows.Data = dictionary.Fruit
            LIMIT 1;
        '''
        try:
            future = client.start_topic_query(
                sql, kikimr.get_database_name() + '/' + self.input_topic, self.consumer_name
            )
            self.write_stream(['Banana'], endpoint=endpoint)
            result = future.result(timeout=70)[0]
            assert len(result.rows) == 1
            assert result.rows[0]['Fruit'] == b'Banana'
            assert result.rows[0]['Price'] == 3
        finally:
            kikimr.ydb_client.query(f'DROP TOPIC {topic};')

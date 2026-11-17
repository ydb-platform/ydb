import pytest

from ydb.tests.fq.s3.s3_helpers import create_bucket_and_put_object


class TestS3Connection:
    def test_s3_connection(self, s3, client, unique_prefix):
        create_bucket_and_put_object(s3.s3_url, 'connection-test', 'data.txt', 'connected')
        name = client.create_external_data_source(unique_prefix + 'source', 'connection-test').name
        result = client.query(
            f'SELECT Data FROM `{name}`.`data.txt` WITH (FORMAT=raw, SCHEMA=(Data String NOT NULL));'
        )[0]
        assert result.rows[0]['Data'] == b'connected'

    @pytest.mark.parametrize('bucket', [' connection-test', 'non-existent-bucket'])
    def test_s3_connection_error(self, client, unique_prefix, bucket):
        name = client.create_external_data_source(unique_prefix + 'source', bucket).name
        result = client.execute(
            f'SELECT Data FROM `{name}`.`*` WITH (FORMAT=raw, SCHEMA=(Data String NOT NULL));'
        ).check(False)
        assert 'The specified bucket does not exist' in result.error

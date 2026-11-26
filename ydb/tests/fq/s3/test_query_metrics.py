from ydb.tests.fq.s3.s3_helpers import create_bucket_and_put_object


class TestQueryMetrics:
    def test_query_metrics(self, s3, client, unique_prefix):
        create_bucket_and_put_object(s3.s3_url, 'query-metrics', 'data.csv', 'value\n1\n2\n3\n')
        source = client.create_external_data_source(unique_prefix + 'source', 'query-metrics').name
        result = client.execute(f'''
            SELECT SUM(value) AS value FROM `{source}`.`data.csv`
            WITH (FORMAT=csv_with_names, SCHEMA=(value Int32 NOT NULL));
        ''').check()
        assert result.result_set().rows[0]['value'] == 6
        assert result.metric('IngressRows') == 3
        assert result.metric('IngressBytes') > 0
        assert result.stats.total_duration_us > 0
        assert result.stats.total_cpu_time_us > 0

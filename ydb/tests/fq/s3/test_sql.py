import pytest
import ydb
import ydb.public.api.protos.ydb_value_pb2 as ydb_value

from ydb.tests.fq.s3.s3_helpers import create_bucket_and_put_object


class TestS3Sql:
    @pytest.mark.parametrize('runtime_listing', [False, True])
    def test_external_table_format_settings(self, s3, client, unique_prefix, runtime_listing):
        create_bucket_and_put_object(
            s3.s3_url, 'sql-format-settings', 'fruits.csv', 'Fruit;Price;Duration\nBanana;3;100\nApple;2;22\nPear;15;33'
        )
        source = client.create_external_data_source(unique_prefix + 'source', 'sql-format-settings').name
        columns = [
            ydb_value.Column(name=name, type=ydb_value.Type(type_id=kind))
            for name, kind in [
                ('Fruit', ydb_value.Type.STRING),
                ('Price', ydb_value.Type.INT32),
                ('Duration', ydb_value.Type.INTERVAL),
            ]
        ]
        name = unique_prefix + 'table'
        client.create_external_table(
            name,
            'fruits.csv',
            'csv_with_names',
            source,
            columns,
            format_setting={'data.interval.unit': 'SECONDS', 'csv_delimiter': ';'},
        )
        result = client.query(
            f'PRAGMA s3.UseRuntimeListing="{str(runtime_listing).lower()}"; SELECT Duration, Fruit, Price FROM `{name}`;'
        )[0]
        assert [(row[0], row[1], row[2]) for row in result.rows] == [
            (100000000, b'Banana', 3),
            (22000000, b'Apple', 2),
            (33000000, b'Pear', 15),
        ]

    @pytest.mark.parametrize('sql', ['SELECT * FROM `non-existing-table`;', 'SELECT 1 FROM foo.bar;'])
    def test_unknown_table(self, client, sql):
        result = client.execute(sql).check(False)
        assert 'Cannot find table' in result.error

    def test_query_parameters(self, client):
        result = client.execute(
            'DECLARE $x AS Int64; SELECT 2 * $x;', parameters={'$x': (10, ydb.PrimitiveType.Int64)}
        ).check()
        assert result.result_set().rows[0][0] == 20

    def test_select_sys_view(self, kikimr):
        result = kikimr.ydb_client.query('SELECT COUNT(*) AS count FROM `.sys/top_queries_by_cpu_time_one_hour`;')
        assert result[0].rows[0]['count'] >= 0

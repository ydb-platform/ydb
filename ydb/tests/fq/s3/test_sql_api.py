import json

import pytest
import ydb
import ydb.public.api.protos.ydb_value_pb2 as ydb_value

from ydb.tests.fq.s3.s3_helpers import create_bucket_and_put_object


class TestS3SqlApi:
    @staticmethod
    def prepare(s3, client, unique_prefix):
        create_bucket_and_put_object(
            s3.s3_url, 'sql-api', 'fruits.csv', 'Fruit,Price,Weight\nBanana,3,100\nApple,2,22\nPear,15,33'
        )
        source = client.create_external_data_source(unique_prefix + 'source', 'sql-api').name
        name = unique_prefix + 'table'
        columns = [
            ydb_value.Column(name=n, type=ydb_value.Type(type_id=t))
            for n, t in [
                ('Fruit', ydb_value.Type.STRING),
                ('Price', ydb_value.Type.INT32),
                ('Weight', ydb_value.Type.INT32),
            ]
        ]
        client.create_external_table(name, 'fruits.csv', 'csv_with_names', source, columns)
        return name, columns

    def test_list_directory(self, s3, client, unique_prefix):
        name, columns = self.prepare(s3, client, unique_prefix)
        source = unique_prefix + 'source'
        for i in range(1, 1002):
            client.create_external_table(
                unique_prefix + f'table_{i:04}', 'fruits.csv', 'csv_with_names', source, columns
            )
        entries = client.list_directory().children
        tables = sorted(entry.name for entry in entries if entry.type == ydb.scheme.SchemeEntryType.EXTERNAL_TABLE)
        assert len(tables) == 1002
        assert name in tables
        assert len(entries) == 1003

    def test_list_directory_with_topic(self, s3, client, unique_prefix):
        name, _ = self.prepare(s3, client, unique_prefix)
        topic = unique_prefix + 'topic'
        client.query(f'CREATE TOPIC `{client.path(topic)}`;')
        client.objects.append(('TOPIC', client.path(topic)))
        entries = {entry.name: entry.type for entry in client.list_directory().children}
        assert entries[name] == ydb.scheme.SchemeEntryType.EXTERNAL_TABLE
        assert entries[topic] == ydb.scheme.SchemeEntryType.TOPIC

    def test_execute_query(self, s3, client, unique_prefix):
        name, _ = self.prepare(s3, client, unique_prefix)
        rows = client.query(f'SELECT * FROM `{name}` ORDER BY Fruit;')[0].rows
        assert rows == [
            {'Fruit': b'Apple', 'Price': 2, 'Weight': 22},
            {'Fruit': b'Banana', 'Price': 3, 'Weight': 100},
            {'Fruit': b'Pear', 'Price': 15, 'Weight': 33},
        ]

    def test_stream_execute_query(self, s3, client, unique_prefix):
        name, _ = self.prepare(s3, client, unique_prefix)
        with client.pool.checkout() as session:
            with session.execute(f'SELECT Fruit FROM `{client.path(name)}` ORDER BY Fruit;') as stream:
                rows = [row for part in stream if part is not None for row in part.rows]
        assert [row['Fruit'] for row in rows] == [b'Apple', b'Banana', b'Pear']

    def test_execute_query_results(self, client):
        result = client.query('SELECT 41; SELECT 42; SELECT 43;')
        assert len(result) == 3
        assert [part.rows[0][0] for part in result] == [41, 42, 43]

    @pytest.mark.parametrize(
        'sql, message', [('BAD QUERY', 'no viable alternative'), ('SELECT * FROM `missing_table`', 'Cannot find table')]
    )
    def test_execute_query_error(self, client, sql, message):
        result = client.execute(sql).check(False)
        assert message in result.error, result.error

    def test_explain_query(self, s3, client, unique_prefix):
        name, _ = self.prepare(s3, client, unique_prefix)
        result = client.execute(f'SELECT * FROM `{name}`;', explain=True).check()
        assert result.ast
        assert result.statistics
        json.dumps(result.statistics)

    def test_describe_external_table(self, s3, client, unique_prefix):
        name, columns = self.prepare(s3, client, unique_prefix)
        description = client.ydb_client.driver.scheme_client.describe_path(client.path(name))
        assert description.type == ydb.scheme.SchemeEntryType.EXTERNAL_TABLE
        result = client.query(f'SELECT * FROM `{name}` LIMIT 0;')[0]
        assert list(result.columns) == columns

    def test_insert_query(self, s3, client, unique_prefix):
        name, columns = self.prepare(s3, client, unique_prefix)
        target = unique_prefix + 'output'
        client.create_external_table(target, 'output/', 'csv_with_names', unique_prefix + 'source', columns)
        assert client.query(f'INSERT INTO `{target}` SELECT * FROM `{name}`;') == []
        assert client.query(f'SELECT COUNT(*) AS count FROM `{target}`;')[0].rows[0]['count'] == 3

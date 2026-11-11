import boto3
import pytest
import ydb

from ydb.tests.fq.s3.sql_client import sql_identifier, sql_string


class TestExternalTables:
    @staticmethod
    def prepare(s3):
        resource = boto3.resource(
            's3', endpoint_url=s3.s3_url, aws_access_key_id='key', aws_secret_access_key='secret_key'
        )
        bucket = resource.Bucket('external-tables')
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()
        bucket.put_object(Key='first.json', Body='{"a": 42, "b": "first"}')
        bucket.put_object(Key='second.json', Body='{"a": 43, "b": "second"}')

    @staticmethod
    def create_table(client, name, source, location):
        client.query(f'''
            CREATE EXTERNAL TABLE {sql_identifier(client.path(name))} (a Int32 NOT NULL, b Utf8 NOT NULL)
            WITH (DATA_SOURCE={sql_string(client.path(source))}, LOCATION={sql_string(location)}, FORMAT='json_each_row');
        ''')
        client.objects.append(('EXTERNAL TABLE', client.path(name)))

    @pytest.mark.parametrize(
        'kikimr_settings', [{'is_replace_if_exists': True}, {'is_replace_if_exists': False}], indirect=True
    )
    def test_external_table_operations(self, s3, client, unique_prefix, kikimr_settings):
        self.prepare(s3)
        source, table = unique_prefix + 'source', unique_prefix + 'table'
        client.create_external_data_source(source, 'external-tables')
        self.create_table(client, table, source, 'first.json')
        entries = {entry.name: entry.type for entry in client.list_directory().children}
        assert entries == {
            source: ydb.scheme.SchemeEntryType.EXTERNAL_DATA_SOURCE,
            table: ydb.scheme.SchemeEntryType.EXTERNAL_TABLE,
        }
        assert client.query(f'SELECT * FROM `{table}`;')[0].rows == [{'a': 42, 'b': 'first'}]

        replacement = f'''
            CREATE OR REPLACE EXTERNAL TABLE `{client.path(table)}` (a Int32 NOT NULL, b Utf8 NOT NULL)
            WITH (DATA_SOURCE='{client.path(source)}', LOCATION='second.json', FORMAT='json_each_row');
        '''
        result = client.execute(replacement)
        result.check(kikimr_settings['is_replace_if_exists'])
        expected = {'a': 43, 'b': 'second'} if kikimr_settings['is_replace_if_exists'] else {'a': 42, 'b': 'first'}
        assert client.query(f'SELECT * FROM `{table}`;')[0].rows == [expected]
        client.drop('EXTERNAL TABLE', table)
        client.drop('EXTERNAL DATA SOURCE', source)
        assert not client.list_directory().children

    @pytest.mark.parametrize('kikimr_settings', [{'is_replace_if_exists': True}], indirect=True)
    def test_replace_source_with_many_external_tables(self, s3, client, unique_prefix):
        self.prepare(s3)
        source = unique_prefix + 'source'
        client.create_external_data_source(source, 'external-tables')
        names = [unique_prefix + f'table_{i}' for i in range(100)]
        for name in names:
            self.create_table(client, name, source, 'first.json')
        union = ' UNION ALL '.join(f'SELECT * FROM `{name}`' for name in names)
        assert client.query(f'SELECT COUNT(*) AS count FROM ({union});')[0].rows[0]['count'] == 100
        client.query(f'''
            CREATE OR REPLACE EXTERNAL DATA SOURCE `{client.path(source)}` WITH (
                SOURCE_TYPE='ObjectStorage', LOCATION={sql_string(s3.s3_url + '/external-tables/')}, AUTH_METHOD='NONE'
            );
        ''')
        assert client.query(f'SELECT COUNT(*) AS count FROM ({union});')[0].rows[0]['count'] == 100
        assert {entry.name for entry in client.list_directory().children} == {source, *names}

    def test_name_uniqueness_constraint(self, s3, client, unique_prefix):
        self.prepare(s3)
        source, table = unique_prefix + 'source', unique_prefix + 'table'
        client.create_external_data_source(source, 'external-tables')
        self.create_table(client, table, source, 'first.json')
        result = client.create_external_data_source(table, 'external-tables', expect_error=True)
        assert result.error_code == 'GENERIC_ERROR', result.error
        assert 'unexpected path type' in result.error, result.error
        result = client.execute(f'''
            CREATE EXTERNAL TABLE `{client.path(source)}` (a Int32)
            WITH (DATA_SOURCE='{client.path(source)}', LOCATION='first.json', FORMAT='json_each_row');
        ''').check(False)
        assert result.error_code == 'GENERIC_ERROR', result.error
        assert 'unexpected path type' in result.error, result.error
        entries = {entry.name: entry.type for entry in client.list_directory().children}
        assert entries == {
            source: ydb.scheme.SchemeEntryType.EXTERNAL_DATA_SOURCE,
            table: ydb.scheme.SchemeEntryType.EXTERNAL_TABLE,
        }
        assert client.query(f'SELECT * FROM `{table}`;')[0].rows == [{'a': 42, 'b': 'first'}]

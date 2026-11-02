#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest
import ydb.public.api.protos.ydb_value_pb2 as ydb
from ydb.tests.fq.streaming_common.common import StreamingTestBase


class TestS3:
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_csv(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT32
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == b"Banana"
        assert result_set.rows[0][1] == 3
        assert result_set.rows[0][2] == 100
        assert result_set.rows[1][0] == b"Apple"
        assert result_set.rows[1][1] == 2
        assert result_set.rows[1][2] == 22
        assert result_set.rows[2][0] == b"Pear"
        assert result_set.rows[2][1] == 15
        assert result_set.rows[2][2] == 33

    def test_json_list_formats(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''[
    { "date" : "", "datetime" : "", "timestamp" : "", "interval" : "", "date32" : "", "datetime64" : "", "timestamp64" : "", "interval64" : "", "tzDate" : "", "tzDateTime" : "", "tzTimestamp" : "" },
    { "date" : "", "datetime" : "", "timestamp" : "", "interval" : "", "date32" : "", "datetime64" : "", "timestamp64" : "", "interval64" : "", "tzDate" : "", "tzDateTime" : "", "tzTimestamp" : "" },
    { "date" : "", "datetime" : "", "timestamp" : "", "interval" : "", "date32" : "", "datetime64" : "", "timestamp64" : "", "interval64" : "", "tzDate" : "", "tzDateTime" : "", "tzTimestamp" : "" }
]'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='timestamp.json', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`/timestamp.json`
            WITH (
                format="json_list",
                schema=(
                    `date` date,
                    `datetime` datetime,
                    `timestamp` timestamp,
                    `interval` interval,
                    `date32` date32,
                    `datetime64` datetime64,
                    `timestamp64` timestamp64,
                    `interval64` interval64,
                    `tzDate` tzDate,
                    `tzDateTime` tzDateTime,
                    `tzTimestamp` tzTimestamp
                ));
            '''

        query = client.execute(sql)
        query.check(False)

        error_message = str(query)
        assert "Date, Timestamp and Interval types are not allowed in json_list format" in error_message
        assert "Date" in error_message
        assert "Datetime" in error_message
        assert "Timestamp" in error_message
        assert "Interval" in error_message
        assert "Date32" in error_message
        assert "Datetime64" in error_message
        assert "Timestamp64" in error_message
        assert "Interval64" in error_message
        assert "TzDate" in error_message
        assert "TzDatetime" in error_message
        assert "TzTimestamp" in error_message

    def test_csv_with_hopping(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Time,Fruit,Price
0,Banana,3
1,Apple,2
2,Pear,15'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = fR'''
            SELECT COUNT(*) as count,
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Time UInt64 NOT NULL,
                Fruit String NOT NULL,
                Price Int NOT NULL
            ))
            GROUP BY HOP(CAST(Time AS Timestamp?), "PT1M", "PT1M", "PT1M")
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 3

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_raw(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("rbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="text1", Bucket='rbucket', Key='file1.txt', ContentType='text/plain')
        s3_client.put_object(Body="text3", Bucket='rbucket', Key='file3.txt', ContentType='text/plain')
        s3_client.put_object(Body="text2", Bucket='rbucket', Key='file2.txt', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "rawbucket")
        client.create_external_data_source(storage_connection_name, "rbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT Data
            FROM `{storage_connection_name}`.`*`
            WITH (format=raw, SCHEMA (
                Data String NOT NULL
            ))
            ORDER BY Data DESC
            '''

        sql = 'pragma ydb.MaxTasksPerStage="10"; ' + sql

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "Data"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == b"text3"
        assert result_set.rows[1][0] == b"text2"
        assert result_set.rows[2][0] == b"text1"

    @pytest.mark.parametrize("kikimr_params", [{"raw": 3, "": 4}], indirect=True)
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_limit(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("lbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="text1", Bucket='lbucket', Key='file1.txt', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "limbucket")
        client.create_external_data_source(storage_connection_name, "lbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT Data
            FROM `{storage_connection_name}`.`*`
            WITH (format=raw, SCHEMA (
                Data String
            ))
            ORDER BY Data DESC
            '''

        query = client.execute(sql)
        query.check(False)
        assert "Size of object file1.txt = 5 and exceeds limit = 3 specified for format raw" in str(query)

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM `{storage_connection_name}`.`*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        assert "Size of object file1.txt = 5 and exceeds limit = 4 specified for format csv_with_names" in str(query)

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_bad_format(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="blah blah blah", Bucket='bbucket', Key='file1.txt', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "badbucket")
        client.create_external_data_source(storage_connection_name, "bbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select * from `{storage_connection_name}`.`*.*` with (format=json_list, schema (data string)) limit 1;
            '''

        query = client.execute(sql)
        query.check(False)

    def test_bad_request_on_invalid_parquet(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body='not a parquet file', Bucket='bbucket', Key='file.txt', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "badbucket")
        client.create_external_data_source(storage_connection_name, "bbucket")

        sql = f'''
            select * from `{storage_connection_name}`.`file.txt` with (format=parquet, schema (data string));
            '''

        query = client.execute(sql)
        query.check(False)

        error_message = str(query)
        assert (query.error_code == "BAD_REQUEST") and ("Parquet magic bytes not found in footer." in error_message)

    def test_bad_request_on_compression(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(
            Body="invalid uncompressed json file", Bucket='bbucket', Key='file.json', ContentType='text/plain'
        )

        storage_connection_name = client.path(unique_prefix + "badbucket")
        client.create_external_data_source(storage_connection_name, "bbucket")

        sql = f'''
            select * from `{storage_connection_name}`.`file.json` with (
                format=csv_with_names,
                compression='gzip',
                schema (data timestamp)
            );
            '''

        query = client.execute(sql)
        query.check(False)
        assert query.error_code == "BAD_REQUEST"

    def test_double_optional_types_validation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format='csv_with_names', SCHEMA (
                Name Int32??,
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)

        assert "double optional types are not supported" in issues, "Incorrect issues: " + issues

        sql = f'''
            INSERT INTO `{storage_connection_name}`.`insert/`
            WITH
            (
                FORMAT="csv_with_names"
            )
            SELECT CAST(42 AS Int32??) as Weight;'''

        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)

        assert "double optional types are not supported" in issues, "Incorrect issues: " + issues

    def test_json_list_validation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )
        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT * FROM `{storage_connection_name}`.`fruits.csv`
            WITH (
                FORMAT='json_list',
                SCHEMA (
                    Value Dict<Int32, String>,
                )
            );
        '''
        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)
        assert "unsupported dict key type, it should be String or Utf8" in issues, "Incorrect issues: " + issues

        sql = f"INSERT INTO `{storage_connection_name}`.`/test/`\n" + '''
            WITH (FORMAT = 'json_list')
            SELECT * FROM AS_TABLE([
                <|foo:{123: "abc"}, bar:"xxx"u|>
            ]);
        '''
        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)
        assert "unsupported dict key type, it should be String or Utf8" in issues, "Incorrect Issues: " + issues

    def test_schema_validation(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )
        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "ibucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            SELECT * FROM `{storage_connection_name}`.`fruits.csv`
            WITH (
                FORMAT='csv_with_names',
                SCHEMA (
                    Value Date32,
                )
            );
        '''
        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)
        assert "big dates is not supported" in issues, "Incorrect issues: " + issues

        sql = f'''
            INSERT INTO `{storage_connection_name}`.`/test/`
            WITH (FORMAT = 'csv_with_names')
            SELECT * FROM AS_TABLE([
                <|foo:CAST(CurrentUtcDate() AS Date32), bar:"xxx"u|>
            ]);
        '''
        query = client.execute(sql)
        query.check(False)
        issues = str(query.error)
        assert "big dates is not supported" in issues, "Incorrect Issues: " + issues

    def test_runtime_listing_disabled(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            PRAGMA s3.UseRuntimeListing="true";

            SELECT * FROM `{storage_connection_name}`.`fruits.csv`
            WITH (
                FORMAT = csv_with_names,
                SCHEMA (
                    Fruit String NOT NULL,
                    Price Int NOT NULL,
                    Weight Int NOT NULL
                )
            );
        '''

        query = client.execute(sql)
        query.check()

        describe_string = "{}".format(query)
        assert (
            "Runtime listing is not allowed for federated queries, pragma value was ignored" in describe_string
        ), describe_string
        assert len(query.result_set().rows) == 3

    def test_precompute_with_different_result_types(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''f1,f2,f3
Banana,3,100'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='file1.csv', ContentType='text/plain')
        fruits = '''f3,f4
Banana,3'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='file2.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            $input1 =
                SELECT AGGREGATE_LIST( AsStruct(f1 AS dns_mining_pool))
                FROM `{storage_connection_name}`.`file1.csv`
                WITH (format=csv_with_names, SCHEMA (
                    f1 String NOT NULL,
                    f2 Int NOT NULL,
                    f3 Int NOT NULL
                ));

            $input2 =
                SELECT AGGREGATE_LIST( AsStruct(f3 AS dns_f_query, f4 AS dns_query1wewqwer) )
                FROM `{storage_connection_name}`.`file2.csv`
                WITH (format=csv_with_names, SCHEMA (
                    f3 String NOT NULL,
                    f4 Int NOT NULL
                ));

            $f1 = () -> {{
                RETURN ListHead(ListMap(
                    $input1,
                    ($r) -> (
                        AsStruct("1" AS dns_mining_pool)
                    )
                ))
            }};

            $f2 = () -> {{
                RETURN ListHead(ListMap(
                    $input2,
                    ($r) -> (
                        AsStruct("2" AS dns_f_query)
                    )
                ))
            }};

            $parsed = SELECT $f1() AS f1,  $f2() AS f2;

            $parsed =
                SELECT
                    p.f1.dns_mining_pool AS f1, p.f2.dns_f_query AS f2
                FROM $parsed AS p;

            SELECT SOME("MinersPoolsViaDNS") AS event_class FROM $parsed
            WHERE (f1 == f2 )
            '''

        query = client.execute(sql)
        query.check()

    def test_hopping_some_tablerow_flatten_alias_projection_yq5620(self, kikimr, s3, client, unique_prefix):
        bucket_name = "yq5620"
        resource = boto3.resource(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key",
        )
        bucket = resource.Bucket(bucket_name)
        bucket.create(ACL="public-read")

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3.s3_url,
            aws_access_key_id="key",
            aws_secret_access_key="secret_key",
        )

        s3_client.put_object(
            Body=("class"),
            Bucket=bucket_name,
            Key="lookup.txt",
            ContentType="text/plain",
        )

        storage_connection_name = client.path("s3")
        client.create_external_data_source(storage_connection_name, bucket_name)

        sql = Rf'''
                $s3 = (
                    SELECT *
                    FROM `{storage_connection_name}`.`lookup.txt`
                    WITH (
                        FORMAT=raw,
                        SCHEMA (class String)
                    )
                );

                $parsed = SELECT * FROM AS_TABLE([<|hostid:"123", time:"xxx"|>]);
                $parsed = (
                    SELECT
                        $s3 AS s3,
                        p.*
                    FROM
                        $parsed AS p
                );

                SELECT
                    SOME(AsStruct(hostid AS hostid)) AS hostid
                FROM
                    $parsed
                GROUP BY
                    HoppingWindow(CurrentUtcTimestamp(time), "PT900S", "PT900S")
        '''

        query = client.execute(sql)

        query.check()


class TestS3TopicJoin(StreamingTestBase):
    def test_join_s3_with_topic(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            's3', endpoint_url=s3.s3_url, aws_access_key_id='key', aws_secret_access_key='secret_key'
        )
        bucket = resource.Bucket('join-s3-with-topic')
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()
        for key, value in [(1, 'one'), (2, 'two'), (3, 'three')]:
            bucket.put_object(Key=f'{key}.json', Body=f'{{"key": {key}, "value": "{value}"}}')
        source = client.create_external_data_source(unique_prefix + 'source', 'join-s3-with-topic').name
        topic, endpoint = self.get_input_name(kikimr, unique_prefix + 'join_s3_with_topic', True, lambda name: name)
        words = ['January', 'February', 'March', 'Monday', 'Tuesday', 'Wednesday', 'Gold', 'Silver', 'Bronze']
        sql = f'''
            SELECT dictionary.value AS num, stream.word AS word
            FROM {topic} WITH (STREAMING='TRUE', FORMAT=json_each_row, SCHEMA=(key Int64 NOT NULL, word String NOT NULL)) AS stream
            INNER JOIN (
                SELECT key, value FROM `{client.path(source)}`.`*`
                WITH (FORMAT=json_each_row, SCHEMA=(key Int64 NOT NULL, value String NOT NULL))
            ) AS dictionary ON stream.key = dictionary.key
            LIMIT 9;
        '''
        try:
            future = kikimr.ydb_client.query_async(sql, timeout=60)
            self.write_stream(
                [f'{{"key": {i % 3 + 1}, "word": "{word}"}}' for i, word in enumerate(words)], endpoint=endpoint
            )
            result = future.result(timeout=70)[0]
            expected = [(number.encode(), word.encode()) for number, word in zip(['one', 'two', 'three'] * 3, words)]
            assert [(row['num'], row['word']) for row in result.rows] == expected
        finally:
            kikimr.ydb_client.query(f'DROP TOPIC {topic};')

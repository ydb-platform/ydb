#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest
import time
import ydb.public.api.protos.ydb_value_pb2 as ydb


class TestS3:
    @pytest.mark.parametrize("kikimr_params", [{"compute": 3}], indirect=True)
    def test_write_result(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("wbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        for i in range(100):
            j = i % 10
            fruit = "Fruit,Price,Weight\n"
            for k in range(j):
                fruit += "A" + str(j) + ",1,1\n"
            s3_client.put_object(Body=fruit, Bucket='wbucket', Key='fruits' + str(j) + '.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "wbucket")

        time.sleep(10)  # 2 x node info update period

        sql = f'''
            SELECT Fruit, sum(Price) as Price, sum(Weight) as Weight
            FROM `{storage_connection_name}`.`fruits*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            )) group by Fruit;
            '''

        query = client.execute(sql)
        query.check()
        describe_result = query
        assert len(describe_result.result_set().rows) == 9

        data = query.result_set(index=0, limit=1000)
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.STRING
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.type_id == ydb.Type.INT64
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 9

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_precompute(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("pbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        s3_client.put_object(Body="text1", Bucket='pbucket', Key='file1.txt', ContentType='text/plain')
        s3_client.put_object(Body="text3", Bucket='pbucket', Key='file3.txt', ContentType='text/plain')
        s3_client.put_object(Body="text2", Bucket='pbucket', Key='file2.txt', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "prebucket")
        client.create_external_data_source(storage_connection_name, "pbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select count(*) as Cnt from `{storage_connection_name}`.`file1.txt` with (format=raw, schema(
                Data String NOT NULL
            ))
            union all
            select count(*) as Cnt from `{storage_connection_name}`.`file2.txt` with (format=raw, schema(
                Data String NOT NULL
            ))
            union all
            select count(*) as Cnt from `{storage_connection_name}`.`file3.txt` with (format=raw, schema(
                Data String NOT NULL
            ))
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "Cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 1
        assert result_set.rows[1][0] == 1
        assert result_set.rows[2][0] == 1

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_failed_precompute(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fpbucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "fp")
        client.create_external_data_source(storage_connection_name, "fpbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            insert into `{storage_connection_name}`.`path/` with (format=json_each_row)
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select count(*) from `{storage_connection_name}`.`path/` with (format=json_each_row, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            union all
            select count(*) from `{storage_connection_name}`.`path/` with (format=json_each_row, schema(
                foo String NOT NULL,
                bar Int NOT NULL
            ))
            '''

        query = client.execute(sql)
        query.check(False)

        issues = str(query.error)
        assert "Error while reading file path" in issues, "Incorrect Issues: " + issues
        assert "Cannot parse input" in issues, "Incorrect Issues: " + issues
        assert "while reading the value of key bar" in issues, "Incorrect Issues: " + issues

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_missed(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
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
                Weight Int NOT NULL,
                Intellect String NOT NULL
            ));
            '''

        query = client.execute(sql)
        query.check(False)
        assert "Column `Intellect` is marked as not null, but was not found in the csv file" in "{}".format(query)

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_simple_hits_47(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            $data = SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));

            $a = select CurrentUtcDate() as _date, Just(1.0) as parsed_lag from $data;

            SELECT
                SUM(parsed_lag)
            FROM  $a
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "column0"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DOUBLE
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 3

    @pytest.mark.parametrize("raw", [True, False])
    @pytest.mark.parametrize("path_pattern", ["exact_file", "directory_scan"])
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_i18n_unpartitioned(self, kikimr, s3, client, raw, path_pattern, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("ibucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        i18n_directory = 'dataset/こんにちは/'
        i18n_name = i18n_directory + 'fruitand&+ %непечатное.csv'

        fruits = '''Data
101
102
103'''
        s3_client.put_object(Body=fruits, Bucket='ibucket', Key=i18n_name, ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "i18nbucket")
        client.create_external_data_source(storage_connection_name, "ibucket")

        if path_pattern == "exact_file":
            path = i18n_name
        elif path_pattern == "directory_scan":
            path = i18n_directory
        else:
            raise ValueError(f"Unknown path_pattern {path_pattern}")

        format = "raw" if raw else "csv_with_names"
        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT count(*) as cnt
            FROM `{storage_connection_name}`.`{path}`
            WITH (format={format}, SCHEMA (
                Data String
            ));
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 1 if raw else 3

    @pytest.mark.parametrize("raw", [False, True])
    @pytest.mark.parametrize("partitioning", ["hive", "projection"])
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_i18n_partitioning(self, kikimr, s3, client, raw, partitioning, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("ibucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        i18n_name = 'fruit and &{+}% непечатное.csv'

        fruits = '''Data
    101
    102
    103'''
        s3_client.put_object(
            Body=fruits, Bucket='ibucket', Key=f"dataset/folder=%こん/{i18n_name}", ContentType='text/plain'
        )
        s3_client.put_object(
            Body=fruits, Bucket='ibucket', Key=f"dataset/folder=に ちは/{i18n_name}", ContentType='text/plain'
        )
        s3_client.put_object(
            Body=fruits, Bucket='ibucket', Key=f"dataset/folder=に/{i18n_name}", ContentType='text/plain'
        )

        storage_connection_name = client.path(unique_prefix + "i18nbucket")
        client.create_external_data_source(storage_connection_name, "ibucket")

        format = "raw" if raw else "csv_with_names"
        if partitioning == "projection":
            sql = (
                f'''
                pragma s3.UseRuntimeListing="{runtime_listing}";
                '''
                + R'''
                $projection = @@ {
                    "projection.enabled" : "true",
                    "storage.location.template" : "/folder=${folder}",
                    "projection.folder.type" : "enum",
                    "projection.folder.values" : "%こん,に ちは,に"
                } @@;'''
                + f'''
                SELECT count(*) as cnt
                FROM `{storage_connection_name}`.`dataset`
                WITH (
                    format={format},
                    SCHEMA (
                        Data String,
                        folder String NOT NULL
                    ),
                    partitioned_by=(folder),
                    projection=$projection
                )
                WHERE folder = 'に ちは' or folder = '%こん';
                '''
            )
        elif partitioning == "hive":
            sql = f'''
                pragma s3.UseRuntimeListing="{runtime_listing}";

                SELECT count(*) as cnt
                FROM `{storage_connection_name}`.`dataset`
                WITH (
                    format={format},
                    SCHEMA (
                        Data String,
                        folder String NOT NULL
                    ),
                    partitioned_by=(folder)
                )
                WHERE folder = 'に ちは' or folder = '%こん';
                '''
        else:
            raise ValueError(f"Unknown partitioning {partitioning}")

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 2 if raw else 6

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_huge_source(self, kikimr, s3, client, runtime_listing, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("hbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        storage_connection_name = client.path(unique_prefix + "hugebucket")
        client.create_external_data_source(storage_connection_name, "hbucket")

        long_literal = "*" * 1024
        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            insert into `{storage_connection_name}`.`path/` with (format=csv_with_names)
            select * from AS_TABLE(ListReplicate(<|s:"{long_literal}"u|>, 1024 * 10));
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            select count(*) from `{storage_connection_name}`.`path/` with (format=csv_with_names, schema(
                s String NOT NULL
            ))
            '''

        query = client.execute(sql)
        query.check()

        data = query.result_set()
        result_set = data
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1
        assert result_set.rows[0][0] == 1024 * 10
        # 1024 x 1024 x 10 = 10 MB of raw data + little overhead for header, eols etc

    # it looks like the runtime_listing for v1 doesn't work in case of
    # restart of query because the v1 keeps the compiled query in the cache
    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    def test_top_level_listing(self, kikimr, s3, client, runtime_listing, unique_prefix):
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
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2024-08-09.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2024-08-08.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "test_top_level_listing")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM `{storage_connection_name}`.`/2024-08-*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            )
            );
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
        assert len(result_set.rows) == 6
        assert result_set.rows[0][0] == b"Banana"
        assert result_set.rows[0][1] == 3
        assert result_set.rows[0][2] == 100
        assert result_set.rows[1][0] == b"Apple"
        assert result_set.rows[1][1] == 2
        assert result_set.rows[1][2] == 22
        assert result_set.rows[2][0] == b"Pear"
        assert result_set.rows[2][1] == 15
        assert result_set.rows[2][2] == 33
        assert result_set.rows[3][0] == b"Banana"
        assert result_set.rows[3][1] == 3
        assert result_set.rows[3][2] == 100
        assert result_set.rows[4][0] == b"Apple"
        assert result_set.rows[4][1] == 2
        assert result_set.rows[4][2] == 22
        assert result_set.rows[5][0] == b"Pear"
        assert result_set.rows[5][1] == 15
        assert result_set.rows[5][2] == 33

    @pytest.mark.parametrize("runtime_listing", ["false", "true"])
    @pytest.mark.parametrize("kikimr_params", [{"allow_concurrent_listings": True}], indirect=True)
    def test_top_level_listing_2(self, kikimr, s3, client, runtime_listing, unique_prefix):
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
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2024-08-09.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2024-09-08.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='2024-08-08.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='/a/2024-08-08.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='/b/test.csv', ContentType='text/plain')

        storage_connection_name = client.path(unique_prefix + "test_top_level_listing_2")
        client.create_external_data_source(storage_connection_name, "fbucket")

        sql = f'''
            pragma s3.UseRuntimeListing="{runtime_listing}";

            SELECT *
            FROM `{storage_connection_name}`.`/2024-08-*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            )
            );
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
        assert len(result_set.rows) == 6
        assert result_set.rows[0][0] == b"Banana"
        assert result_set.rows[0][1] == 3
        assert result_set.rows[0][2] == 100
        assert result_set.rows[1][0] == b"Apple"
        assert result_set.rows[1][1] == 2
        assert result_set.rows[1][2] == 22
        assert result_set.rows[2][0] == b"Pear"
        assert result_set.rows[2][1] == 15
        assert result_set.rows[2][2] == 33
        assert result_set.rows[3][0] == b"Banana"
        assert result_set.rows[3][1] == 3
        assert result_set.rows[3][2] == 100
        assert result_set.rows[4][0] == b"Apple"
        assert result_set.rows[4][1] == 2
        assert result_set.rows[4][2] == 22
        assert result_set.rows[5][0] == b"Pear"
        assert result_set.rows[5][1] == 15
        assert result_set.rows[5][2] == 33

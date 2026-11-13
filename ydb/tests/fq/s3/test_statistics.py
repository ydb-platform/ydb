#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3

import pytest


class TestS3(object):
    @pytest.mark.parametrize("format", ["json_list", "json_each_row", "csv_with_names", "parquet"])
    def test_egress(self, kikimr, s3, client, format, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("egress_bucket")
        bucket.create(ACL='public-read-write')

        storage_connection_name = client.path(unique_prefix + "sbucket")
        client.create_external_data_source(storage_connection_name, "egress_bucket")

        sql = f'''
            insert into `{storage_connection_name}`.`{format}_sql/` with (format={format})
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        egress_bytes = query.metric('EgressBytes')

        file_size = 0
        for file in bucket.objects.all():
            if file.key.startswith(f"{format}_sql/"):
                file_size += bucket.Object(file.key).content_length

        assert file_size == egress_bytes, f"File size {file_size} mistmatches egress bytes {egress_bytes}"

    @pytest.mark.parametrize("format1", ["json_list", "json_each_row", "csv_with_names", "parquet"])
    @pytest.mark.parametrize("format2", ["json_list", "json_each_row", "csv_with_names", "parquet"])
    def test_convert(self, kikimr, s3, client, format1, format2, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("convert_bucket")
        bucket.create(ACL='public-read-write')

        storage_connection_name = client.path(unique_prefix + "sbucket")
        client.create_external_data_source(storage_connection_name, "convert_bucket")

        sql = f'''
            insert into `{storage_connection_name}`.`{format1}_1_{format2}_sql/` with (format={format1})
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        egress_bytes_1 = query.metric('EgressBytes')

        sql = f'''
            insert into `{storage_connection_name}`.`{format1}_2_{format2}_sql/` with (format={format2})
            select foo, bar from `{storage_connection_name}`.`{format1}_1_{format2}_sql/*` with (format={format1}, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            '''

        query = client.execute(sql)
        query.check()

        ingress_bytes_1 = query.metric('IngressBytes')
        egress_bytes_2 = query.metric('EgressBytes')

        sql = f'''
            select foo, bar from `{storage_connection_name}`.`{format1}_2_{format2}_sql/*` with (format={format2}, schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            '''

        query = client.execute(sql)
        query.check()

        ingress_bytes_2 = query.metric('IngressBytes')

        file_size_1 = 0
        file_size_2 = 0
        for file in bucket.objects.all():
            if file.key.startswith(f"{format1}_1_{format2}_sql/"):
                file_size_1 += bucket.Object(file.key).content_length
            if file.key.startswith(f"{format1}_2_{format2}_sql/"):
                file_size_2 += bucket.Object(file.key).content_length

        assert (
            file_size_1 == egress_bytes_1
        ), f"File {format1} size {file_size_1} mistmatches egress bytes {egress_bytes_1}"
        assert (
            file_size_2 == egress_bytes_2
        ), f"File {format2} size {file_size_2} mistmatches egress bytes {egress_bytes_2}"
        if format1 != "parquet":
            assert (
                file_size_1 == ingress_bytes_1
            ), f"File {format1} size {file_size_1} mistmatches ingress bytes {egress_bytes_1}"
        if format2 != "parquet":
            assert (
                file_size_2 == ingress_bytes_2
            ), f"File {format2} size {file_size_2} mistmatches ingress bytes {egress_bytes_2}"

    def test_precompute(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("pbucket")
        bucket.create(ACL='public-read-write')

        storage_connection_name = client.path(unique_prefix + "pb")
        client.create_external_data_source(storage_connection_name, "pbucket")

        sql = f'''
            insert into `{storage_connection_name}`.`path1/` with (format=json_list)
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            insert into `{storage_connection_name}`.`path2/` with (format=csv_with_names)
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query = client.execute(sql)
        query.check()

        sql = f'''
            $c1 =
                SELECT
                count(*) as `count`
                FROM
                `{storage_connection_name}`.`path1/`
                with (format=json_list, schema(
                    foo Int NOT NULL,
                    bar String NOT NULL
                ));

            $c2 =
                SELECT
                count(*) as `count`
                FROM
                `{storage_connection_name}`.`path2/`
                with (format=csv_with_names, schema(
                    foo Int NOT NULL,
                    bar String NOT NULL
                ));

            select $c1 + $c2;
            '''

        query = client.execute(sql)
        query.check()

        ingress = query.metric("IngressBytes")

        file_size = 0
        for file in bucket.objects.all():
            file_size += bucket.Object(file.key).content_length

        assert file_size == ingress, f"Total size {file_size} mistmatches ingress bytes {ingress}"

    def test_sum(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket_name = "sum_bucket_sql"
        bucket = resource.Bucket(bucket_name)
        bucket.create(ACL='public-read-write')

        storage_connection_name = client.path(unique_prefix + "sbucket")
        client.create_external_data_source(storage_connection_name, bucket_name)

        sql = f'''
            insert into `{storage_connection_name}`.`file/` with (format="csv_with_names")
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        for i in range(0, 10):
            query = client.execute(sql)
            query.check()

        files_size = 0
        for file in bucket.objects.all():
            files_size += bucket.Object(file.key).content_length

        sql = fR'''
            select foo, bar from `{storage_connection_name}`.`file/*` with (format="csv_with_names", schema(
                foo Int NOT NULL,
                bar String NOT NULL
            ))
            '''

        query = client.execute(sql)
        query.check()

        ingress_bytes = query.metric('IngressBytes')

        assert files_size == ingress_bytes, f"Files size {files_size} mistmatches ingress bytes {ingress_bytes}"

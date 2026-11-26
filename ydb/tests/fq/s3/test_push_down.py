#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json

BUCKET_NAME = 'fbucket'


class TestS3PushDown:
    @staticmethod
    def create_s3_client(s3):
        resource = boto3.resource(
            's3', endpoint_url=s3.s3_url, aws_access_key_id='key', aws_secret_access_key='secret_key'
        )

        bucket = resource.Bucket(BUCKET_NAME)
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        return boto3.client(
            's3',
            endpoint_url=s3.s3_url,
            aws_access_key_id='key',
            aws_secret_access_key='secret_key',
        )

    def test_simple_case(self, kikimr, s3, client, unique_prefix):
        file_head = 'Fruit,Price,Weight\n'
        file_rows = [file_head]
        for i in range(0, 10_000):
            file_rows.append(f'Fruit#{i + 1},{i},{i + 3}\n')
        file_content = ''.join(file_rows)
        file_size = len(file_content)

        s3_client = TestS3PushDown.create_s3_client(s3)
        s3_client.put_object(Body=file_content, Bucket=BUCKET_NAME, Key='fruits.csv')

        storage_connection_name = client.path(unique_prefix + "fruitbucket")
        client.create_external_data_source(storage_connection_name, 'fbucket')

        sql = f'''
            select *
            from `{storage_connection_name}`.`fruits.csv`
            with (format=csv_with_names, schema (
                Fruit String not null,
                Price Int not null,
                Weight Int not null
            ))
            limit 1;
        '''

        query = client.execute(sql)
        query.check()

        stat = query.statistics
        ingress_bytes = None
        ingress_bytes = query.metric("IngressBytes")

        assert ingress_bytes < file_size, f'loaded too much for a pushed down query: {json.dumps(stat)}'

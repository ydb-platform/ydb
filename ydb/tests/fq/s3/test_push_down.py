#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json
import pytest

import ydb.public.api.protos.draft.fq_pb2 as fq

from ydb.tests.tools.fq_runner.kikimr_utils import yq_all

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

    @yq_all
    @pytest.mark.parametrize('client', [{'folder_id': 'my_folder'}], indirect=True)
    def test_simple_case(self, kikimr, s3, client, yq_version, unique_prefix):
        file_head = 'Fruit,Price,Weight\n'
        file_rows = [file_head]
        for i in range(0, 10_000):
            file_rows.append(f'Fruit#{i + 1},{i},{i + 3}\n')
        file_content = ''.join(file_rows)
        file_size = len(file_content)

        s3_client = TestS3PushDown.create_s3_client(s3)
        s3_client.put_object(Body=file_content, Bucket=BUCKET_NAME, Key='fruits.csv')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, 'fbucket')

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

        query_id = client.create_query('test', sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)
        ingress_bytes = None
        if yq_version == 'v1':
            ingress_bytes = stat['Graph=0']['IngressBytes']
        else:
            assert yq_version == 'v2'
            ingress_bytes = stat['ResultSet']['IngressBytes']

        assert ingress_bytes['sum'] < file_size, f'loaded too much for a pushed down query: {json.dumps(stat)}'

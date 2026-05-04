#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import pytest
import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v2
from google.protobuf.struct_pb2 import NullValue

import ydb.tests.fq.s3.s3_helpers as s3_helpers


class TestS3Inference(TestYdsBase):
    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight,Date
Banana,3,100,2024-01-02
Apple,2,22,2024-03-04
Pear,15,33,2024-05-06'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Date"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DATE
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].uint32_value == 19724
        assert result_set.rows[0].items[1].text_value == "Banana"
        assert result_set.rows[0].items[2].int64_value == 3
        assert result_set.rows[0].items[3].int64_value == 100
        assert result_set.rows[1].items[0].uint32_value == 19786
        assert result_set.rows[1].items[1].text_value == "Apple"
        assert result_set.rows[1].items[2].int64_value == 2
        assert result_set.rows[1].items[3].int64_value == 22
        assert result_set.rows[2].items[0].uint32_value == 19849
        assert result_set.rows[2].items[1].text_value == "Pear"
        assert result_set.rows[2].items[2].int64_value == 15
        assert result_set.rows[2].items[3].int64_value == 33
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_csv_no_header(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        # No header row — columns should be inferred as column0..columnN.
        fruits = '''Banana,3,100,2024-01-02
Apple,2,22,2024-03-04
Pear,15,33,2024-05-06'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits_noheader.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits_noheader.csv`
            WITH (format=csv, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        # Columns are inferred with synthetic names column0..column3, ordered alphabetically by name.
        assert result_set.columns[0].name == "column0"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "column1"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[2].name == "column2"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "column3"
        assert result_set.columns[3].type.optional_type.item.type_id == ydb.Type.DATE
        assert len(result_set.rows) == 3

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_parquet_all_types(self, kikimr, s3, client, unique_prefix):
        # Generate a parquet file in-memory with one column per primitive arrow
        # type from kPrimitiveTypeTable (arrow_inferencinator.cpp) that survives
        # a parquet round-trip, then verify schema inference maps each column
        # to the expected YDB type. Column names are numerically prefixed so
        # they come back in a deterministic alphabetical order.
        import decimal
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq
        from datetime import date, datetime

        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )
        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        # (name, pyarrow_array, expected_ydb_type, expected_optional)
        # Reflects current kPrimitiveTypeTable in arrow_inferencinator.cpp.
        # ShouldBeOptional() returns false for STRING / BINARY / LARGE_BINARY / NA,
        # and true for everything else. DATE64, LARGE_*, and unsigned integer
        # types are excluded because they don't survive a parquet round-trip
        # cleanly (parquet's default reader widens unsigned ints to signed int64).
        columns = [
            ("c01_bool",       pa.array([True],                          type=pa.bool_()),                          ydb.Type.BOOL,      True),
            ("c02_int8",       pa.array([1],                             type=pa.int8()),                           ydb.Type.INT8,      True),
            ("c03_int16",      pa.array([1],                             type=pa.int16()),                          ydb.Type.INT16,     True),
            ("c04_int32",      pa.array([1],                             type=pa.int32()),                          ydb.Type.INT32,     True),
            ("c05_int64",      pa.array([1],                             type=pa.int64()),                          ydb.Type.INT64,     True),
            ("c06_float",      pa.array([1.0],                           type=pa.float32()),                        ydb.Type.FLOAT,     True),
            ("c07_double",     pa.array([1.0],                           type=pa.float64()),                        ydb.Type.DOUBLE,    True),
            ("c08_string",     pa.array(["s"],                           type=pa.string()),                         ydb.Type.UTF8,      False),
            ("c09_binary",     pa.array([b"b"],                          type=pa.binary()),                         ydb.Type.STRING,    False),
            ("c10_date32",     pa.array([date(2024, 1, 2)],              type=pa.date32()),                         ydb.Type.DATE,      True),
            ("c11_timestamp",  pa.array([datetime(2024, 1, 2, 3, 4, 5)], type=pa.timestamp('us')),                  ydb.Type.TIMESTAMP, True),
            ("c12_decimal",    pa.array([decimal.Decimal("1.5")],        type=pa.decimal128(precision=5, scale=2)), ydb.Type.DOUBLE,    True),
        ]

        table = pa.table([col[1] for col in columns], names=[col[0] for col in columns])
        buf = io.BytesIO()
        pq.write_table(table, buf)
        s3_client.put_object(
            Body=buf.getvalue(),
            Bucket='fbucket',
            Key='all_types.parquet',
            ContentType='application/octet-stream',
        )

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`all_types.parquet`
            WITH (format=parquet, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))

        assert len(result_set.columns) == len(columns)
        for actual, (name, _arr, expected_type, expected_optional) in zip(result_set.columns, columns):
            assert actual.name == name, f"unexpected column name: got {actual.name}, want {name}"
            if expected_optional:
                assert actual.type.optional_type.item.type_id == expected_type, \
                    f"column {name}: got {actual.type}, want Optional<{expected_type}>"
            else:
                assert actual.type.type_id == expected_type, \
                    f"column {name}: got {actual.type}, want {expected_type}"

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    @pytest.mark.parametrize("kikimr_settings", [{"enable_schema_inference": False}], indirect=True)
    def test_inference_disabled_by_feature_flag(self, kikimr, s3, client, unique_prefix):
        # When the EnableExternalSourceSchemaInference feature flag is disabled,
        # a query with WITH (with_infer="true") must fail with a clear error
        # rather than silently falling through to a cryptic type-annotation
        # failure later in the pipeline.
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )
        bucket = resource.Bucket("ibucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )
        s3_client.put_object(
            Body="name,value\nfoo,1\n",
            Bucket='ibucket',
            Key='data.csv',
            ContentType='text/plain',
        )

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "infer_disabled_bucket"
        client.create_storage_connection(storage_connection_name, "ibucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`data.csv`
            WITH (format="csv_with_names", with_infer="true");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "Schema inference" in issues and "with_infer" in issues, \
            f"expected a clear schema-inference-disabled error in issues, got: {issues}"

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_error_propagates_to_user(self, kikimr, s3, client, unique_prefix):
        # When inference cannot produce any usable columns (here every JSON
        # field has an unsupported nested-array/object type), the inference
        # actor must surface a clear error to the user instead of silently
        # producing an empty schema that fails later with a cryptic
        # type-annotation error.
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        # Every column has a non-primitive (unsupported by inference) type.
        data = '''{ "a" : [10, 20, 30] , "b" : { "key" : "value" } }
{ "a" : [10, 20, 30] , "b" : { "key" : "value" } }
{ "a" : [10, 20, 30] , "b" : { "key" : "value" } }'''
        s3_client.put_object(Body=data, Bucket='fbucket', Key='unsupported.json', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "unsupported_bucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`unsupported.json`
            WITH (format=json_each_row, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "couldn't infer schema" in issues or "no usable columns" in issues, \
            f"expected a clear inference-failure error in issues, got: {issues}"

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_null_column(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Missing column
Banana,3,
Apple,2,
Pear,15,'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "Missing column"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].text_value == ""
        assert result_set.rows[0].items[2].int64_value == 3
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].text_value == ""
        assert result_set.rows[1].items[2].int64_value == 2
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].text_value == ""
        assert result_set.rows[2].items[2].int64_value == 15
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_optional_types(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit,Price,Weight,Date
Banana,,,2024-01-02
Apple,2,22,
,15,33,2024-05-06'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Date"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DATE
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].uint32_value == 19724
        assert result_set.rows[0].items[1].text_value == "Banana"
        assert result_set.rows[0].items[2].null_flag_value == NullValue.NULL_VALUE
        assert result_set.rows[0].items[3].null_flag_value == NullValue.NULL_VALUE
        assert result_set.rows[1].items[0].null_flag_value == NullValue.NULL_VALUE
        assert result_set.rows[1].items[1].text_value == "Apple"
        assert result_set.rows[1].items[2].int64_value == 2
        assert result_set.rows[1].items[3].int64_value == 22
        assert result_set.rows[2].items[0].uint32_value == 19849
        assert result_set.rows[2].items[1].text_value == ""
        assert result_set.rows[2].items[2].int64_value == 15
        assert result_set.rows[2].items[3].int64_value == 33

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_multiple_files(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        read_data_1 = '''a,b,c
1,2,3
1,2,3'''
        read_data_2 = '''a,b,c
1,2,3'''

        s3_client.put_object(Body=read_data_1, Bucket='fbucket', Key='test/sub dir/1.csv', ContentType='text/plain')
        s3_client.put_object(Body=read_data_2, Bucket='fbucket', Key='test/sub dir/2.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "multiple_files_bucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`/test/`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "a"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[1].name == "b"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[2].name == "c"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].int64_value == 1
        assert result_set.rows[0].items[1].int64_value == 2
        assert result_set.rows[0].items[2].int64_value == 3
        assert result_set.rows[1].items[0].int64_value == 1
        assert result_set.rows[1].items[1].int64_value == 2
        assert result_set.rows[1].items[2].int64_value == 3
        assert result_set.rows[2].items[0].int64_value == 1
        assert result_set.rows[2].items[1].int64_value == 2
        assert result_set.rows[2].items[2].int64_value == 3
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_file_error(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        read_data = '''{"a" : [10, 20, 30]}'''
        s3_client.put_object(Body=read_data, Bucket='fbucket', Key='data.json', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "json_bucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`data.json`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "couldn\\'t open csv/tsv file, check format and compression parameters:" in str(
            client.describe_query(query_id).result
        )

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_parameters(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''Fruit|Price|Weight|Date
Banana|3|100|2024-01-02
Apple|2|22|2024-03-04
Pear|15|33|2024-05-06'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='year=10/month=5/test1.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='year=10/month=5/test2.csv', ContentType='text/plain')
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='year=10/month=5/test3.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`/`
            WITH (format=csv_with_names,
                with_infer='true',
                partitioned_by=(`year`, `month`),
                file_pattern='test*',
                csv_delimiter='|')
            limit 3;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 6
        assert result_set.columns[0].name == "Date"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DATE
        assert result_set.columns[1].name == "Fruit"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "Price"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "Weight"
        assert result_set.columns[3].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[4].name == "month"
        assert result_set.columns[4].type.type_id == ydb.Type.INT64
        assert result_set.columns[5].name == "year"
        assert result_set.columns[5].type.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].uint32_value == 19724
        assert result_set.rows[0].items[1].text_value == "Banana"
        assert result_set.rows[0].items[2].int64_value == 3
        assert result_set.rows[0].items[3].int64_value == 100
        assert result_set.rows[0].items[4].int64_value == 5
        assert result_set.rows[0].items[5].int64_value == 10
        assert result_set.rows[1].items[0].uint32_value == 19786
        assert result_set.rows[1].items[1].text_value == "Apple"
        assert result_set.rows[1].items[2].int64_value == 2
        assert result_set.rows[1].items[3].int64_value == 22
        assert result_set.rows[1].items[4].int64_value == 5
        assert result_set.rows[1].items[5].int64_value == 10
        assert result_set.rows[2].items[0].uint32_value == 19849
        assert result_set.rows[2].items[1].text_value == "Pear"
        assert result_set.rows[2].items[2].int64_value == 15
        assert result_set.rows[2].items[3].int64_value == 33
        assert result_set.rows[2].items[4].int64_value == 5
        assert result_set.rows[2].items[5].int64_value == 10
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_timestamp(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        csv_data = '''a,b,c
2024-08-29,2024-08-29 10:20:30,2024-08-29T10:20:30.01
2024-08-29,2024-08-29 10:20:30,2024-08-29T10:20:30.01
2024-08-29,2024-08-29 10:20:30,2024-08-29T10:20:30.01'''
        json_data = '''{ "a" : "2024-08-29", "b" : "2024-08-29 10:20:30", "c" : "2024-08-29T10:20:30.01" }
{ "a" : "2024-08-29", "b" : "2024-08-29 10:20:30", "c" : "2024-08-29T10:20:30.01" }
{ "a" : "2024-08-29", "b" : "2024-08-29 10:20:30", "c" : "2024-08-29T10:20:30.01" }'''
        s3_client.put_object(Body=csv_data, Bucket='fbucket', Key='timestamp.csv', ContentType='text/plain')
        s3_client.put_object(Body=json_data, Bucket='fbucket', Key='timestamp.json', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`timestamp.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "a"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.DATE
        assert result_set.columns[1].name == "b"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.TIMESTAMP
        assert result_set.columns[2].name == "c"
        assert result_set.columns[2].type.type_id == ydb.Type.UTF8

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`timestamp.json`
            WITH (format=json_each_row, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "a"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "b"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert result_set.columns[2].name == "c"
        assert result_set.columns[2].type.type_id == ydb.Type.UTF8

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`timestamp.csv`
            WITH (format=csv_with_names, with_infer='true', `data.timestamp.formatname`='ISO');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        assert "parameter is not supported with type inference" in str(
            client.describe_query(query_id).result
        )

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_projection(self, kikimr, s3, client, unique_prefix):
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
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='year=2023/fruits.csv', ContentType='text/plain')

        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = '''$projection = @@ {
                "projection.enabled" : "true",
                "storage.location.template" : "/${date}",
                "projection.date.type" : "date",
                "projection.date.min" : "2022-11-02",
                "projection.date.max" : "2024-12-02",
                "projection.date.interval" : "1",
                "projection.date.format" : "/year=%Y",
                "projection.date.unit" : "YEARS"
            } @@;''' + f'''

            SELECT *
            FROM `{storage_connection_name}`.`/`
            WITH (format=csv_with_names,
                with_infer='true',
                partitioned_by=(`date`),
                projection=$projection);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 4
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[3].name == "date"
        assert result_set.columns[3].type.type_id == ydb.Type.DATE
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].int64_value == 3
        assert result_set.rows[0].items[2].int64_value == 100
        assert result_set.rows[0].items[3].uint32_value == 19663
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].int64_value == 2
        assert result_set.rows[1].items[2].int64_value == 22
        assert result_set.rows[1].items[3].uint32_value == 19663
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].int64_value == 15
        assert result_set.rows[2].items[2].int64_value == 33
        assert result_set.rows[2].items[3].uint32_value == 19663
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_null_column_name(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = ''',Fruit,Price
1,Banana,3
2,Apple,2
3,Pear,15'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].int64_value == 3
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].int64_value == 2
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].int64_value == 15
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inference_unsupported_types(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = '''{ "a" : [10, 20, 30] , "b" : { "key" : "value" }, "c" : 10 }
{ "a" : [10, 20, 30] , "b" : { "key" : "value" }, "c" : 20 }
{ "a" : [10, 20, 30] , "b" : { "key" : "value" }, "c" : 30 }'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.json', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.json`
            WITH (format=json_each_row, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "c"
        assert result_set.columns[0].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].int64_value == 10
        assert result_set.rows[1].items[0].int64_value == 20
        assert result_set.rows[2].items[0].int64_value == 30

    def _create_bucket_and_upload_file(self, filename, s3, kikimr):
        s3_helpers.create_bucket_and_upload_file(
            filename, s3.s3_url, "fbucket", "ydb/tests/fq/s3/test_format_data"
        )
        kikimr.control_plane.wait_bootstrap(1)

    def _create_bucket_and_upload_file_body(
        self, body, object_key, s3, kikimr, content_type="text/plain", bucket_name="fbucket"
    ):
        s3_helpers.create_bucket_and_put_object(s3.s3_url, bucket_name, object_key, body, content_type)
        kikimr.control_plane.wait_bootstrap(1)

    def _validate_result_inference(self, result_set):
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "Fruit"
        assert result_set.columns[0].type.type_id == ydb.Type.UTF8
        assert result_set.columns[1].name == "Price"
        assert result_set.columns[1].type.optional_type.item.type_id == ydb.Type.INT64
        assert result_set.columns[2].name == "Weight"
        assert result_set.columns[2].type.optional_type.item.type_id == ydb.Type.INT64
        assert len(result_set.rows) == 3
        assert result_set.rows[0].items[0].text_value == "Banana"
        assert result_set.rows[0].items[1].int64_value == 3
        assert result_set.rows[0].items[2].int64_value == 100
        assert result_set.rows[1].items[0].text_value == "Apple"
        assert result_set.rows[1].items[1].int64_value == 2
        assert result_set.rows[1].items[2].int64_value == 22
        assert result_set.rows[2].items[0].text_value == "Pear"
        assert result_set.rows[2].items[1].int64_value == 15
        assert result_set.rows[2].items[2].int64_value == 33

    @yq_v2
    @pytest.mark.parametrize(
        "filename, type_format",
        [
            ("test.csv", "csv_with_names"),
            ("test.tsv", "tsv_with_names"),
            ("test_each_row.json", "json_each_row"),
            ("test_list.json", "json_list"),
            ("test.parquet", "parquet"),
        ],
    )
    def test_format_inference(self, kikimr, s3, client, filename, type_format, unique_prefix):
        self._create_bucket_and_upload_file(filename, s3, kikimr)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`{filename}`
            WITH (format=`{type_format}`, with_infer='true');
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        self._validate_result_inference(result_set)

    @yq_v2
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_with_infer_and_unsupported_option(self, kikimr, s3, client, unique_prefix):
        fruits = '''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        self._create_bucket_and_upload_file_body(fruits, "fruits.csv", s3, kikimr)

        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        # XXX replace with other unsupported parameter when/if this one become supported
        sql = f'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format="csv_with_names", with_infer="true", `data.datetime.format`="%Y-%m-%dT%H-%M");
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert (
            "couldn\\'t load table metadata: parameter is not supported with type inference: data.datetime.format"
            in describe_string
        )

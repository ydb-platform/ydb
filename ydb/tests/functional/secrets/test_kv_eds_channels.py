import os
import time
import uuid

import boto3
import grpc

from ydb.public.api.grpc.ydb_keyvalue_v1_pb2_grpc import KeyValueServiceStub
from ydb.public.api.protos import ydb_keyvalue_pb2 as kv
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.functional.secrets.lib.secrets_plugin import DATABASE, create_secrets, run_with_assert


CLUSTER_CONFIG = dict(extra_feature_flags=[
    "enable_external_data_sources", "enable_external_data_source_storage_pools", "enable_schema_secrets",
])


def test_two_eds_channels_after_partition_growth(ydb_cluster, db_fixture):
    endpoint = os.environ["S3_ENDPOINT"]
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="minio", aws_secret_access_key="minio123")
    create_secrets(db_fixture, [f"{DATABASE}/access", f"{DATABASE}/secret"], ["minio", "minio123"])
    buckets = []
    for name in ("hot", "cold"):
        bucket = f"kv-{name}-{uuid.uuid4().hex}"
        s3.create_bucket(Bucket=bucket)
        buckets.append(bucket)
        run_with_assert(db_fixture, f"""
            CREATE EXTERNAL DATA SOURCE `{name}_source` WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="{endpoint}/{bucket}", AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_PATH="{DATABASE}/access",
                AWS_SECRET_ACCESS_KEY_SECRET_PATH="{DATABASE}/secret",
                AWS_REGION="us-east-1", STORAGE_POOL_NAME="{name}"
            );
        """)

    volume = f"{DATABASE}/volume"
    meta = (("x-ydb-database", DATABASE),)
    node = ydb_cluster.nodes[1]
    with grpc.insecure_channel(f"{node.host}:{node.port}") as channel:
        stub = KeyValueServiceStub(channel)

        def call(method, request):
            op = method(request, metadata=meta, timeout=120).operation
            assert op.status == StatusIds.SUCCESS, op
            return op.result

        create = kv.CreateVolumeRequest(path=volume, partition_count=1)
        create.storage_config.channel.add(media="hdd")
        create.storage_config.channel.add(media="hdd")
        create.storage_config.channel.add(external_data_source=f"{DATABASE}/hot_source")
        create.storage_config.channel.add(storage_pool="cold")
        call(stub.CreateVolume, create)

        call(stub.AlterVolume, kv.AlterVolumeRequest(path=volume, alter_partition_count=2))
        described = kv.DescribeVolumeResult()
        assert call(stub.DescribeVolume, kv.DescribeVolumeRequest(path=volume)).Unpack(described)
        assert [ch.external_data_source for ch in described.storage_config.channel[2:]] == [
            f"{DATABASE}/hot_source", f"{DATABASE}/cold_source",
        ]

        def write_and_read(key, payload, storage_channel):
            write = kv.ExecuteTransactionRequest(path=volume, partition_id=1)
            write.commands.add().write.CopyFrom(kv.ExecuteTransactionRequest.Command.Write(
                key=key, value=payload, storage_channel=storage_channel,
            ))
            call(stub.ExecuteTransaction, write)
            result = kv.ReadResult()
            assert call(stub.Read, kv.ReadRequest(path=volume, partition_id=1, key=key)).Unpack(result)
            assert result.value == payload

        values = [b"hot-payload", b"cold-payload"]
        for i, payload in enumerate(values):
            write_and_read(f"key{i}", payload, 2 + i)

        for bucket, payload in zip(buckets, values):
            objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
            assert [s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read() for obj in objects] == [payload]

        # Moto accepts arbitrary signing keys. An empty secret exercises the router's
        # fail-closed path even with this backend and proves it refreshes without a restart.
        read = kv.ReadRequest(path=volume, partition_id=1, key="key1")

        def wait_read(expected_status):
            deadline = time.monotonic() + 120
            last = None
            while (remaining := deadline - time.monotonic()) > 0:
                try:
                    op = stub.Read(read, metadata=meta, timeout=min(remaining, 15)).operation
                except grpc.RpcError as err:
                    if err.code() not in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                        raise
                    last = err
                else:
                    last = op
                    if op.status == StatusIds.INTERNAL_ERROR:
                        assert any("S3 credentials are unavailable" in issue.message for issue in op.issues), op
                    elif op.status not in (StatusIds.SUCCESS, StatusIds.UNAVAILABLE, StatusIds.TIMEOUT, StatusIds.OVERLOADED):
                        raise AssertionError(op)
                    if op.status == expected_status:
                        return op
                time.sleep(1)
            raise AssertionError(f"KV Read did not reach status {expected_status}, last response was {last}")

        run_with_assert(db_fixture, f"ALTER SECRET `{DATABASE}/secret` WITH (VALUE='');")
        wait_read(StatusIds.INTERNAL_ERROR)
        run_with_assert(db_fixture, f"ALTER SECRET `{DATABASE}/secret` WITH (VALUE='replacement');")
        recovered = kv.ReadResult()
        assert wait_read(StatusIds.SUCCESS).result.Unpack(recovered)
        assert recovered.value == values[1]
        write_and_read("after-rotation", b"rotated-payload", 3)

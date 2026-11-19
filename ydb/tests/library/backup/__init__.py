from contextlib import contextmanager
import os
from threading import Thread
import time
import uuid
from urllib.parse import urlsplit

import boto3
import grpc
from library.python.port_manager import PortManager

from ydb.public.api.grpc import ydb_export_v1_pb2_grpc, ydb_import_v1_pb2_grpc, ydb_operation_v1_pb2_grpc
from ydb.public.api.protos import ydb_export_pb2, ydb_import_pb2, ydb_operation_pb2, ydb_status_codes_pb2
from ydb.tests.library.common.s3_upload_proxy import S3UploadProxy

SUCCESS = ydb_status_codes_pb2.StatusIds.SUCCESS
CANCELLED = ydb_status_codes_pb2.StatusIds.CANCELLED


def checksum_metadata(row):
    hash_value = row["hash"]
    if isinstance(hash_value, bytes):
        hash_value = hash_value.decode("ascii")
    return {
        "version": row["version"],
        "algorithm": row["algorithm"],
        "count": str(row["count"]),
        "hash": hash_value,
    }


class BackupStorage:
    def __init__(self, kind, directory, channel, database):
        self.kind = kind
        self.directory = directory
        self.metadata = (("x-ydb-database", database),)
        self.exporter = ydb_export_v1_pb2_grpc.ExportServiceStub(channel)
        self.importer = ydb_import_v1_pb2_grpc.ImportServiceStub(channel)
        self.operations = ydb_operation_v1_pb2_grpc.OperationServiceStub(channel)
        if kind == "s3":
            self.endpoint = os.environ["S3_ENDPOINT"]
            self.bucket = f"backup-table-checksum-{uuid.uuid4().hex}"
            self.s3 = boto3.client(
                "s3", endpoint_url=self.endpoint, aws_access_key_id="minio", aws_secret_access_key="minio123"
            )
            self.s3.create_bucket(Bucket=self.bucket)

    def wait(self, response, expected=SUCCESS, issue=None):
        operation = response.operation
        operation_id = operation.id
        deadline = time.monotonic() + 300
        while not operation.ready and time.monotonic() < deadline:
            time.sleep(0.2)
            try:
                result = self.operations.GetOperation(
                    ydb_operation_pb2.GetOperationRequest(id=operation_id), metadata=self.metadata, timeout=60
                ).operation
            except grpc.RpcError as error:
                if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    raise
                continue
            if result.status != ydb_status_codes_pb2.StatusIds.UNAVAILABLE:
                operation = result
        assert operation.ready, f"Backup operation did not finish: {operation_id}"
        assert operation.status == expected, str(operation.issues)
        if issue is not None:
            assert issue in str(operation.issues)
        return operation

    def s3_settings(self):
        return dict(
            endpoint=urlsplit(self.endpoint).netloc,
            scheme=ydb_export_pb2.ExportToS3Settings.HTTP,
            bucket=self.bucket,
            access_key="minio",
            secret_key="minio123",
        )

    def wait_for_transfer(self, response):
        operation = response.operation
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            assert not operation.ready, str(operation.issues)
            metadata = ydb_export_pb2.ExportToS3Metadata()
            operation.metadata.Unpack(metadata)
            if metadata.progress == ydb_export_pb2.ExportProgress.PROGRESS_TRANSFER_DATA:
                return
            time.sleep(0.2)
            operation = self.operations.GetOperation(
                ydb_operation_pb2.GetOperationRequest(id=operation.id), metadata=self.metadata, timeout=60
            ).operation
        raise AssertionError("Export did not finish creating its table copy")

    def start_export(self, items, endpoint=None, data_format="csv", include_index_data=False):
        if self.kind == "fs":
            assert data_format == "csv", "ExportToFs has no Parquet format setting"
            request = ydb_export_pb2.ExportToFsRequest()
            request.settings.base_path = str(self.directory)
            request.settings.include_index_data = include_index_data
            for source, prefix in items:
                request.settings.items.add(source_path=source, destination_path=prefix)
            response = self.exporter.ExportToFs(request, metadata=self.metadata, timeout=60)
        else:
            request = ydb_export_pb2.ExportToS3Request()
            request.settings.CopyFrom(ydb_export_pb2.ExportToS3Settings(**self.s3_settings()))
            if endpoint is not None:
                request.settings.endpoint = endpoint
            request.settings.include_index_data = include_index_data
            if data_format == "parquet":
                request.settings.parquet.row_group_size = 64
            for source, prefix in items:
                request.settings.items.add(source_path=source, destination_prefix=prefix)
            response = self.exporter.ExportToS3(request, metadata=self.metadata, timeout=60)
        return response

    def export(self, source, data_format="csv"):
        return self.wait(self.start_export([(source, "table")], data_format=data_format))

    @contextmanager
    def pause_uploads(self):
        assert self.kind == "s3"
        with PortManager() as ports:
            port = ports.get_port()
            with S3UploadProxy(port, paused=True, discard_paused=False) as proxy:
                server = Thread(target=proxy.serve_forever, daemon=True)
                server.start()
                try:
                    yield proxy, f"127.0.0.1:{port}"
                finally:
                    proxy.release.set()
                    proxy.shutdown()
                    server.join(timeout=60)

    def start_import(self, destination, skip=False, prefix="table", index_population_mode=None):
        if self.kind == "fs":
            request = ydb_import_pb2.ImportFromFsRequest()
            request.settings.base_path = str(self.directory)
            request.settings.skip_checksum_validation = skip
            if index_population_mode is not None:
                request.settings.index_population_mode = index_population_mode
            request.settings.items.add(source_path=prefix, destination_path=destination)
            response = self.importer.ImportFromFs(request, metadata=self.metadata, timeout=60)
        else:
            request = ydb_import_pb2.ImportFromS3Request()
            request.settings.CopyFrom(ydb_import_pb2.ImportFromS3Settings(**self.s3_settings()))
            request.settings.skip_checksum_validation = skip
            if index_population_mode is not None:
                request.settings.index_population_mode = index_population_mode
            request.settings.items.add(source_prefix=prefix, destination_path=destination)
            response = self.importer.ImportFromS3(request, metadata=self.metadata, timeout=60)
        return response

    def restore(
        self, destination, skip=False, expected=SUCCESS, prefix="table", issue="table checksum mismatch",
        index_population_mode=None,
    ):
        response = self.start_import(destination, skip, prefix, index_population_mode)
        return self.wait(response, expected, issue if expected == CANCELLED else None)

    def read(self, name, prefix="table"):
        if self.kind == "fs":
            return (self.directory / prefix / name).read_bytes()
        return self.s3.get_object(Bucket=self.bucket, Key=f"{prefix}/{name}")["Body"].read()

    def write(self, name, content):
        if self.kind == "fs":
            (self.directory / "table" / name).write_bytes(content)
        else:
            self.s3.put_object(Bucket=self.bucket, Key=f"table/{name}", Body=content)

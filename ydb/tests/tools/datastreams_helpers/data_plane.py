#!/usr/bin/env python
# -*- coding: utf-8 -*-

import grpc
import logging
import os
import uuid

import yatest.common
import ydb

from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.public.api.grpc.draft import ydb_datastreams_v1_pb2_grpc
from ydb.public.api.protos.draft import datastreams_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


READ_TOOL_TIMEOUT = plain_or_under_sanitizer(30, 300)


def write_stream(path, data, partition_key=None, database=None, endpoint=None):
    if database is None:
        database = os.getenv("YDB_DATABASE")
    if endpoint is None:
        endpoint = os.getenv("YDB_ENDPOINT")
    request_metadata = [("x-ydb-database", database)]
    channel = grpc.insecure_channel(endpoint)
    stub = ydb_datastreams_v1_pb2_grpc.DataStreamsServiceStub(channel)

    request = datastreams_pb2.PutRecordsRequest()
    request.stream_name = path
    for d in data:
        rec = request.records.add()
        rec.data = d if isinstance(d, bytes) else str(d).encode("utf8")
        if partition_key is None:
            rec.partition_key = (d if isinstance(d, str) else str(d))[:100]
        else:
            rec.partition_key = partition_key
    response = stub.PutRecords(request, metadata=request_metadata)
    logging.debug("Write topic {}. Response: {}".format(path, response))
    assert response.operation.status == StatusIds.SUCCESS
    result = datastreams_pb2.GetRecordsResult()
    response.operation.result.Unpack(result)
    logging.info("Data was written to {}: {}".format(path, data))


#  Data plane grpc API is not implemented in datastreams.
def read_stream(path, messages_count, commit_after_processing=True, consumer_name="test_client", timeout=None, database=None, endpoint=None):
    result_file_name = "{}-{}-read-result-{}-{}-out".format(
        os.getenv("PYTEST_CURRENT_TEST").rsplit('/', 1)[-1].replace(":", "_").replace(" (call)", ""),
        path.replace("/", "_"),
        consumer_name,
        uuid.uuid4()
    )
    if database is None:
        database = os.getenv("YDB_DATABASE")
    if endpoint is None:
        endpoint = os.getenv("YDB_ENDPOINT")
    if timeout is None:
        timeout = READ_TOOL_TIMEOUT
    result_file = yatest.common.output_path(result_file_name)
    cmd = [
        yatest.common.binary_path("ydb/tests/tools/pq_read/pq_read"),
        "--endpoint", endpoint,
        "--database", database,
        "--topic-path", path,
        "--consumer-name", consumer_name,
        "--disable-cluster-discovery",
        "--messages-count", str(messages_count),
        "--timeout", "{}ms".format(int(timeout * 1000))
    ]
    if commit_after_processing:
        cmd += ["--commit-after-processing"]

    execute_timeout = timeout + max(timeout, plain_or_under_sanitizer(10, 30))
    with open(result_file, "w") as outfile:
        yatest.common.execute(cmd, timeout=execute_timeout, stdout=outfile)

    ret = []
    with open(result_file, "r") as result:
        for msg in result:
            if msg.endswith("\n"):
                msg = msg[:-1]
            ret.append(msg)

    logging.info("Data was read from {}: {}".format(path, ret))
    return ret


def read_stream_with_codec(topic_path, count, consumer_name, endpoint, database, timeout=10):
    """Read *count* messages without decompression.

    Uses identity decoders so that the SDK does not decompress the payload and
    ``batch._codec`` retains the original wire codec value (normally the SDK
    overwrites it to CODEC_RAW=1 after decompression).

    Returns (wire_codec_int, list_of_raw_bytes) where wire_codec_int is the
    codec tag observed on the first batch (e.g. 2 for GZIP, 4 for ZSTD).
    """
    identity = lambda data: data  # noqa: E731 тАФ keep as lambda for clarity
    # Map all known non-RAW codecs to identity so we receive compressed bytes
    GZIP_CODEC = 2
    ZSTD_CODEC = 4
    decoders = {GZIP_CODEC: identity, ZSTD_CODEC: identity}

    grpc_endpoint = endpoint if endpoint.startswith("grpc") else f"grpc://{endpoint}"
    driver_config = ydb.DriverConfig(grpc_endpoint, database, auth_token="root@builtin")
    driver = ydb.Driver(driver_config)
    driver.wait(timeout=5)

    wire_codec = None
    raw_messages = []
    try:
        with driver.topic_client.reader(
            topic_path,
            consumer=consumer_name,
            decoders=decoders,
        ) as reader:
            while len(raw_messages) < count:
                batch = reader.receive_batch(timeout=timeout)
                if batch is None:
                    break
                if wire_codec is None:
                    wire_codec = int(batch._codec)
                raw_messages.extend(msg.data for msg in batch.messages)
    finally:
        driver.stop()

    logging.info("Read %d messages with codec=%s from %s", len(raw_messages), wire_codec, topic_path)
    return wire_codec, raw_messages

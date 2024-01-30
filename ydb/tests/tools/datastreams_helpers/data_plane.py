#!/usr/bin/env python
# -*- coding: utf-8 -*-

import grpc
import logging
import os
import uuid

import yatest.common

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.public.api.grpc.draft import ydb_datastreams_v1_pb2_grpc
from ydb.public.api.protos.draft import datastreams_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


READ_TOOL_TIMEOUT = yatest_common.plain_or_under_sanitizer(20, 300)


def write_stream(path, data, partition_key=None):
    request_metadata = [("x-ydb-database", os.getenv("YDB_DATABASE"))]
    channel = grpc.insecure_channel(os.getenv("YDB_ENDPOINT"))
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
def read_stream(path, messages_count, commit_after_processing=True, consumer_name="test_client", timeout=READ_TOOL_TIMEOUT):
    result_file_name = "{}-{}-read-result-{}-{}-out".format(
        os.getenv("PYTEST_CURRENT_TEST").replace(":", "_").replace(" (call)", ""),
        path.replace("/", "_"),
        consumer_name,
        uuid.uuid4()
    )
    result_file = yatest.common.output_path(result_file_name)
    cmd = [
        yatest.common.binary_path("ydb/tests/tools/pq_read/pq_read"),
        "--endpoint", os.getenv("YDB_ENDPOINT"),
        "--database", os.getenv("YDB_DATABASE"),
        "--topic-path", path,
        "--consumer-name", consumer_name,
        "--disable-cluster-discovery",
        "--messages-count", str(messages_count),
        "--timeout", "{}ms".format(int(timeout * 1000))
    ] + ["--commit-after-processing"] if commit_after_processing else []

    with open(result_file, "w") as outfile:
        yatest.common.execute(cmd, timeout=timeout * 2, stdout=outfile)

    ret = []
    with open(result_file, "r") as result:
        for msg in result:
            if msg.endswith("\n"):
                msg = msg[:-1]
            ret.append(msg)

    logging.info("Data was read from {}: {}".format(path, ret))
    return ret

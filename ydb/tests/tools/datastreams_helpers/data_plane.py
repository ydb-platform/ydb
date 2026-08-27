#!/usr/bin/env python
# -*- coding: utf-8 -*-

import grpc
import logging
import os

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
    if database is None:
        database = os.getenv("YDB_DATABASE")
    if endpoint is None:
        endpoint = os.getenv("YDB_ENDPOINT")
    if timeout is None:
        timeout = READ_TOOL_TIMEOUT

    driver_config = ydb.DriverConfig(endpoint, database, disable_discovery=True)
    driver = ydb.Driver(driver_config)
    driver.wait(timeout=max(timeout, plain_or_under_sanitizer(10, 30)))
    try:
        ret = []
        with driver.topic_client.reader(path, consumer=consumer_name) as reader:
            while len(ret) < messages_count:
                try:
                    msg = reader.receive_message(timeout=timeout or None)
                except TimeoutError:
                    break

                data = msg.data
                ret.append(data.decode("utf-8") if isinstance(data, bytes) else str(data))
                if commit_after_processing:
                    reader.commit_with_ack(msg, timeout=timeout or None)
    finally:
        driver.stop()

    logging.info("Data was read from {}: {}".format(path, ret))
    return ret

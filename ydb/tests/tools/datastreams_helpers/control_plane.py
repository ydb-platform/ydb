#!/usr/bin/env python
# -*- coding: utf-8 -*-

import grpc
import logging
import os

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.protos import ydb_persqueue_v1_pb2
from ydb.public.api.protos.draft import datastreams_pb2
from ydb.public.api.grpc.draft import ydb_persqueue_v1_pb2_grpc
from ydb.public.api.grpc.draft import ydb_datastreams_v1_pb2_grpc


def _get_database():
    return os.getenv("YDB_DATABASE")


def _get_endpoint():
    return os.getenv("YDB_ENDPOINT")


def _build_stream_path(path):
    database_with_leading_slash = _get_database()
    if not database_with_leading_slash.startswith("/"):
        database_with_leading_slash = "/" + database_with_leading_slash
    return "{}/{}".format(database_with_leading_slash, path)


def _build_request_metadata():
    return [("x-ydb-database", _get_database())]


def _new_persqueue_service():
    channel = grpc.insecure_channel(_get_endpoint())
    return ydb_persqueue_v1_pb2_grpc.PersQueueServiceStub(channel)


def _new_datastreams_service():
    channel = grpc.insecure_channel(_get_endpoint())
    return ydb_datastreams_v1_pb2_grpc.DataStreamsServiceStub(channel)


def _get_and_check_result(response, result_class):
    logging.debug("Response: {}".format(response))
    assert response.operation.status == StatusIds.SUCCESS, response
    result = result_class()
    response.operation.result.Unpack(result)
    logging.debug("Result: {}".format(result))
    return result


def create_stream(path, partitions_count=1):
    stub = _new_datastreams_service()

    request = datastreams_pb2.CreateStreamRequest()
    request.stream_name = _build_stream_path(path)
    request.shard_count = partitions_count
    request.retention_period_hours = 1
    request.write_quota_kb_per_sec = 1024
    logging.debug("Requesting CreateStream.\nDatabase: \"{}\".\nRequest:\n{}".format(_get_database(), request))
    response = stub.CreateStream(request, metadata=_build_request_metadata())
    _get_and_check_result(response, datastreams_pb2.CreateStreamResult)


def delete_stream(path):
    stub = _new_datastreams_service()

    request = datastreams_pb2.DeleteStreamRequest()
    request.stream_name = _build_stream_path(path)
    request.enforce_consumer_deletion = True
    logging.debug("Requesting DeleteStream.\nDatabase: \"{}\".\nRequest:\n{}".format(_get_database(), request))
    response = stub.DeleteStream(request, metadata=_build_request_metadata())
    _get_and_check_result(response, datastreams_pb2.DeleteStreamResult)


def create_read_rule(path, consumer_name="test_client"):
    stub = _new_persqueue_service()

    request = ydb_persqueue_v1_pb2.AddReadRuleRequest()
    request.path = path
    request.read_rule.service_type = "yandex-query"
    request.read_rule.consumer_name = consumer_name
    request.read_rule.supported_format = ydb_persqueue_v1_pb2.TopicSettings.FORMAT_BASE
    response = stub.AddReadRule(request, metadata=_build_request_metadata())
    _get_and_check_result(response, ydb_persqueue_v1_pb2.AddReadRuleResult)


def describe_topic(path):
    stub = _new_persqueue_service()

    request = ydb_persqueue_v1_pb2.DescribeTopicRequest()
    request.path = path
    response = stub.DescribeTopic(request, metadata=_build_request_metadata())
    return _get_and_check_result(response, ydb_persqueue_v1_pb2.DescribeTopicResult)


def list_read_rules(path):
    describe_result = describe_topic(path)
    return [read_rule_desc.consumer_name for read_rule_desc in describe_result.settings.read_rules]

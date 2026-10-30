"""Boto3 equivalents of SqsJsonFederationCompatibilityTests."""

from collections import Counter
from contextlib import ExitStack
import logging
import os
import time
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import grpc
import pytest

from ydb.public.api.grpc import ydb_topic_v1_pb2_grpc
from ydb.public.api.protos import ydb_operation_pb2, ydb_status_codes_pb2, ydb_topic_pb2


CM_DATABASE = '/logbroker-federation/prod'
DATABASE = '/Root/logbroker-federation/prod'
CONSUMER = 'sqs-consumer'
CONSUMER_PATH = 'prod/' + CONSUMER
CLUSTERS = ('cluster_a', 'cluster_b')
logger = logging.getLogger(__name__)


def _topic_request(endpoint, database, method, request, timeout=10):
    request.operation_params.operation_mode = ydb_operation_pb2.OperationParams.SYNC
    with grpc.insecure_channel(endpoint) as channel:
        stub = ydb_topic_v1_pb2_grpc.TopicServiceStub(channel)
        return getattr(stub, method)(
            request,
            metadata=(('x-ydb-database', database), ('x-ydb-auth-ticket', 'root@builtin')),
            timeout=timeout,
        ).operation


def _assert_success(operation):
    assert operation.ready, operation
    assert operation.status == ydb_status_codes_pb2.StatusIds.SUCCESS, operation


@pytest.fixture
def clients():
    with ExitStack() as stack:
        result = {}
        for cluster in CLUSTERS:
            port = os.environ[cluster + '_http_proxy_port']
            client = boto3.session.Session().client(
                'sqs',
                aws_access_key_id='unused',
                aws_secret_access_key='unused',
                aws_session_token='root@builtin',
                endpoint_url='http://localhost:' + port + DATABASE,
                region_name='ru-central1',
                config=Config(connect_timeout=5, read_timeout=10, retries={'total_max_attempts': 1}),
            )
            stack.callback(client.close)
            assert client.meta.service_model.protocol == 'json'
            result[cluster] = client
        yield result


def _wait_for_topic(cluster, name):
    endpoint = 'localhost:' + os.environ[cluster + '_port']
    deadline = time.monotonic() + 60
    last_error_details = None
    while time.monotonic() < deadline:
        try:
            operation = _topic_request(
                endpoint, DATABASE, 'DescribeTopic', ydb_topic_pb2.DescribeTopicRequest(path=name),
                timeout=5)
        except grpc.RpcError as error:
            if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                raise
            last_error_details = error
        else:
            last_error_details = operation
            if operation.ready and operation.status == ydb_status_codes_pb2.StatusIds.SUCCESS:
                description = ydb_topic_pb2.DescribeTopicResult()
                assert operation.result.Unpack(description)
                if any(c.name == CONSUMER_PATH and c.HasField('shared_consumer_type')
                       for c in description.consumers):
                    return
        time.sleep(0.1)
    pytest.fail('Topic {} is not ready on {}: {}'.format(name, cluster, last_error_details))


@pytest.fixture
def topic():
    name = 'sqs-' + uuid.uuid4().hex
    endpoint = 'localhost:' + os.environ['CM_PORT']
    request = ydb_topic_pb2.CreateTopicRequest(path=name)
    request.partitioning_settings.min_active_partitions = 1
    request.partitioning_settings.max_active_partitions = 1
    consumer = request.consumers.add(name=CM_DATABASE + '/' + CONSUMER)
    consumer.shared_consumer_type.keep_messages_order = False
    consumer.shared_consumer_type.default_processing_timeout.seconds = 30
    try:
        _assert_success(_topic_request(endpoint, CM_DATABASE, 'CreateTopic', request, timeout=60))
        for cluster in CLUSTERS:
            _wait_for_topic(cluster, name)
        yield name
    finally:
        try:
            _assert_success(_topic_request(
                endpoint, CM_DATABASE, 'DropTopic', ydb_topic_pb2.DropTopicRequest(path=name)))
        except Exception:
            logger.exception('Failed to drop federation topic %s', name)


def _queues(clients, topic):
    for cluster, client in clients.items():
        url = client.get_queue_url(QueueName=topic + '@' + CONSUMER_PATH)['QueueUrl']
        assert url
        yield cluster, client, url


def _assert_batch(response, count):
    assert not response.get('Failed'), response
    entries = response.get('Successful', [])
    assert len(entries) == count, response
    assert {entry['Id'] for entry in entries} == {str(i) for i in range(count)}, response
    return entries


def _send(client, url, bodies):
    response = client.send_message_batch(
        QueueUrl=url, Entries=[{'Id': str(i), 'MessageBody': body} for i, body in enumerate(bodies)])
    assert all(entry['MessageId'] for entry in _assert_batch(response, len(bodies)))


def _receive(client, url, count, visibility=60):
    messages = []
    deadline = time.monotonic() + 30
    while len(messages) < count and time.monotonic() < deadline:
        response = client.receive_message(
            QueueUrl=url, MaxNumberOfMessages=min(10, count - len(messages)),
            WaitTimeSeconds=1, VisibilityTimeout=visibility)
        for message in response.get('Messages', []):
            assert message['MessageId'] and message['ReceiptHandle']
            messages.append(message)
    assert len(messages) == count, messages
    return messages


def _receive_and_delete(client, url, bodies):
    messages = _receive(client, url, len(bodies), visibility=30)
    assert Counter(m['Body'] for m in messages) == Counter(bodies)
    response = client.delete_message_batch(
        QueueUrl=url,
        Entries=[{'Id': str(i), 'ReceiptHandle': m['ReceiptHandle']} for i, m in enumerate(messages)],
    )
    _assert_batch(response, len(messages))


def _assert_empty(client, url):
    assert not client.receive_message(QueueUrl=url, WaitTimeSeconds=1).get('Messages')


def test_reports_missing_queue(clients):
    for client in clients.values():
        with pytest.raises(ClientError) as error:
            client.get_queue_url(QueueName='missing-' + uuid.uuid4().hex)
        error_code = error.value.response['Error']['Code']
        assert error_code in (
            'AWS.SimpleQueueService.NonExistentQueue', 'QueueDoesNotExist'
            ), f'Unexpected error code: {error_code}'
        assert error.value.response['Error']['Message']


def test_batch_write_and_read(clients, topic):
    for cluster, client, url in _queues(clients, topic):
        bodies = [cluster + '-' + str(i) for i in range(3)]
        _send(client, url, bodies)
        _receive_and_delete(client, url, bodies)
        _assert_empty(client, url)


def test_single_write_read_and_delete(clients, topic):
    for cluster, client, url in _queues(clients, topic):
        body = cluster + '-single'
        sent = client.send_message(QueueUrl=url, MessageBody=body)
        assert sent['MessageId']
        message, = _receive(client, url, 1, visibility=1)
        assert message['Body'] == body
        assert message['MessageId'] == sent['MessageId']
        client.delete_message(QueueUrl=url, ReceiptHandle=message['ReceiptHandle'])
        time.sleep(2)
        _assert_empty(client, url)


@pytest.mark.parametrize('batch', [False, True], ids=['single', 'batch'])
def test_change_message_visibility(clients, topic, batch):
    for cluster, client, url in _queues(clients, topic):
        bodies = [cluster + '-first', cluster + '-second'] if batch else [cluster + '-single']
        _send(client, url, bodies)
        messages = _receive(client, url, len(bodies))
        _assert_empty(client, url)  # because messages visibility equals to 60s
        if batch:
            response = client.change_message_visibility_batch(
                QueueUrl=url, Entries=[
                    {'Id': str(i), 'ReceiptHandle': m['ReceiptHandle'], 'VisibilityTimeout': 0}
                    for i, m in enumerate(messages)])
            _assert_batch(response, len(messages))
        else:
            client.change_message_visibility(
                QueueUrl=url, ReceiptHandle=messages[0]['ReceiptHandle'], VisibilityTimeout=0)
        redelivered = _receive(client, url, len(bodies))
        assert {m['MessageId'] for m in redelivered} == {m['MessageId'] for m in messages}
        assert Counter(m['Body'] for m in redelivered) == Counter(bodies)
        for message in redelivered:
            client.delete_message(QueueUrl=url, ReceiptHandle=message['ReceiptHandle'])
        _assert_empty(client, url)


def test_get_queue_attributes(clients, topic):
    for _, client, url in _queues(clients, topic):
        response = client.get_queue_attributes(QueueUrl=url, AttributeNames=['VisibilityTimeout'])
        assert response['Attributes']['VisibilityTimeout'] == '30'


def test_list_queues(clients, topic):
    for _, client, url in _queues(clients, topic):
        assert url in client.list_queues(QueueNamePrefix=topic).get('QueueUrls', [])


def test_purge_queue(clients, topic):
    for cluster, client, url in _queues(clients, topic):
        _send(client, url, [cluster + '-purged-0', cluster + '-purged-1'])
        _receive(client, url, 1, visibility=1)
        client.purge_queue(QueueUrl=url)
        time.sleep(2)
        _assert_empty(client, url)
        bodies = [cluster + '-after-purge']
        _send(client, url, bodies)
        _receive_and_delete(client, url, bodies)

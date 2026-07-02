#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time
import uuid

import boto3
from hamcrest import assert_that, equal_to, has_item, has_length, not_none, raises

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

DEFAULT_REGION = 'ru-central1'
DEFAULT_SECURITY_TOKEN = 'root@builtin'
DEFAULT_SQS_CONSUMER = 'ydb-sqs-consumer'


class KikimrSqsTopicTestBase(object):
    database = '/Root/SqsTopic'
    use_in_memory_pdisks = True

    @classmethod
    def _setup_config_generator(cls):
        return KikimrConfigGenerator(
            use_in_memory_pdisks=cls.use_in_memory_pdisks,
            additional_log_configs={
                'HTTP_PROXY': LogLevels.DEBUG,
                'SQS': LogLevels.TRACE,
            },
            http_proxy_config={
                'enabled': True,
                'sqs_topic_enabled': True,
                'ymq_enabled': False,
                'yandex_cloud_service_region': [DEFAULT_REGION, 'ru-central-1'],
            },
            extra_feature_flags=['enable_topic_message_level_parallelism'],
        )

    @classmethod
    def _init_database(cls):
        cls.cluster.create_database(
            cls.database,
            storage_pool_units_count={'hdd': 1},
        )
        cls.cluster.register_and_start_slots(cls.database, count=1)
        cls.cluster.wait_tenant_up(cls.database)

    @classmethod
    def setup_class(cls):
        config_generator = cls._setup_config_generator()
        cls.cluster = KiKiMR(config_generator)
        cls.cluster.start()
        cls._init_database()

        node = cls.cluster.nodes[1]
        cls.sqs_endpoint = 'http://{}:{}{}'.format(
            node.host,
            node.http_proxy_port,
            cls.database,
        )

    def setup_method(self, method=None):
        logger.debug('Test started: %s', method.__name__)

        self._boto_client = self._make_boto_client()
        self._queue_url = None

    def teardown_method(self, method=None):
        if self._queue_url is not None:
            try:
                self._boto_client.delete_queue(QueueUrl=self._queue_url)
            except Exception as exc:
                logger.warning('Failed to delete queue %s: %s', self._queue_url, exc)

        logger.debug('Test finished: %s', method.__name__)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            if hasattr(cls, 'database'):
                cls.cluster.remove_database(cls.database)
            cls.cluster.stop()

    def _make_queue_name(self, test_name):
        return 'queue_{}_{}'.format(test_name[:60], uuid.uuid1())

    def _make_fifo_queue_name(self, test_name):
        return '{}.fifo'.format(self._make_queue_name(test_name))

    def _create_fifo_queue(self, test_name):
        queue_name = self._make_fifo_queue_name(test_name)
        self._queue_url = self._boto_client.create_queue(
            QueueName=queue_name,
            Attributes={
                'FifoQueue': 'true',
            },
        )['QueueUrl']
        return queue_name

    def _make_boto_client(self):
        session = boto3.session.Session()
        return session.client(
            service_name='sqs',
            aws_access_key_id='unused',
            aws_secret_access_key='unused',
            aws_session_token=DEFAULT_SECURITY_TOKEN,
            endpoint_url=self.sqs_endpoint,
            region_name=DEFAULT_REGION,
        )

    def _make_ydb_driver(self):
        node = self.cluster.nodes[1]
        config = ydb.DriverConfig(
            endpoint='{}:{}'.format(node.host, node.port),
            database=self.database,
        )
        driver = ydb.Driver(config)
        driver.wait()
        return driver

    def _assert_topic_exists(self, topic_path, expected_name, parent_directory=None):
        driver = self._make_ydb_driver()
        try:
            scheme_entry = driver.scheme_client.describe_path(topic_path)
            assert_that(scheme_entry.type, equal_to(ydb.SchemeEntryType.TOPIC))
            assert_that(scheme_entry.name, equal_to(expected_name))

            if parent_directory is not None:
                parent_entry = driver.scheme_client.describe_path(parent_directory)
                assert_that(parent_entry.type, equal_to(ydb.SchemeEntryType.DIRECTORY))

                children = driver.scheme_client.list_directory(parent_directory).children
                matching_children = [
                    child for child in children
                    if child.name == expected_name and child.type == ydb.SchemeEntryType.TOPIC
                ]
                assert_that(len(matching_children), equal_to(1))
        finally:
            driver.stop()

    def _assert_topic_not_exists(self, topic_path):
        driver = self._make_ydb_driver()
        try:
            def describe_topic():
                driver.scheme_client.describe_path(topic_path)

            assert_that(describe_topic, raises(ydb.SchemeError))
        finally:
            driver.stop()

    def _read_message_from_topic_without_consumer(self, topic_path, timeout=10):
        messages = self._read_messages_from_topic_without_consumer(topic_path, 1, timeout=timeout)
        return messages[0]

    def _read_messages_from_topic_without_consumer(self, topic_path, messages_count, timeout=10):
        driver = self._make_ydb_driver()
        try:
            topic_description = driver.topic_client.describe_topic(topic_path, include_stats=False)
            partition_ids = [partition.partition_id for partition in topic_description.partitions]

            topic_selector = ydb.TopicReaderSelector(
                path=topic_path,
                partitions=partition_ids,
            )
            messages = []
            with driver.topic_client.reader(
                topic_selector,
                consumer=None,
                event_handler=ydb.TopicReaderEvents.EventHandler(),
            ) as reader:
                deadline = time.time() + timeout
                while len(messages) < messages_count and time.time() < deadline:
                    try:
                        messages.append(reader.receive_message(timeout=1))
                    except TimeoutError:
                        continue

            assert_that(messages, has_length(messages_count))
            return messages
        finally:
            driver.stop()

    def _assert_topic_has_consumer(self, topic_path, consumer_name=DEFAULT_SQS_CONSUMER):
        driver = self._make_ydb_driver()
        try:
            topic_description = driver.topic_client.describe_topic(topic_path, include_stats=False)
            consumer_names = [consumer.name for consumer in topic_description.consumers]
            assert_that(consumer_names, has_item(equal_to(consumer_name)))
        finally:
            driver.stop()

    def _assert_consumer_keep_message_order(self, topic_path, expected, consumer_name=DEFAULT_SQS_CONSUMER):
        try:
            from ydb.public.api.protos import ydb_topic_pb2
        except ImportError:
            from contrib.ydb.public.api.protos import ydb_topic_pb2

        from ydb import _apis, issues
        from ydb._grpc.grpcwrapper import ydb_topic_public_types as _ydb_topic_public_types

        driver = self._make_ydb_driver()
        try:
            request = _ydb_topic_public_types.DescribeConsumerRequestParams(
                path=topic_path,
                consumer=consumer_name,
                include_stats=False,
                include_location=False,
            )

            def extract_proto(rpc_state, response_pb, driver=None):
                issues._process_response(response_pb.operation)
                result = ydb_topic_pb2.DescribeConsumerResult()
                response_pb.operation.result.Unpack(result)
                return result

            consumer_description = driver(
                request.to_proto(),
                _apis.TopicService.Stub,
                _apis.TopicService.DescribeConsumer,
                extract_proto,
            )
            assert_that(consumer_description.consumer.HasField('shared_consumer_type'), equal_to(True))
            assert_that(
                consumer_description.consumer.shared_consumer_type.keep_messages_order,
                equal_to(expected),
            )
        finally:
            driver.stop()

    def _get_consumer_uncommitted_messages_count(self, topic_path, consumer_name=DEFAULT_SQS_CONSUMER):
        driver = self._make_ydb_driver()
        try:
            consumer_description = driver.topic_client.describe_consumer(
                topic_path,
                consumer_name,
                include_stats=True,
            )
            uncommitted_count = 0
            for partition in consumer_description.partitions:
                assert_that(partition.partition_stats, not_none())
                assert_that(partition.partition_consumer_stats, not_none())
                uncommitted_count += (
                    partition.partition_stats.partition_end
                    - partition.partition_consumer_stats.committed_offset
                )
            return uncommitted_count
        finally:
            driver.stop()

    def _send_and_receive_message_attribute(self, test_name, message_attributes, attribute_name):
        queue_name = self._make_queue_name(test_name)
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody='message body',
            MessageAttributes=message_attributes,
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        return messages[0]['MessageAttributes'][attribute_name]

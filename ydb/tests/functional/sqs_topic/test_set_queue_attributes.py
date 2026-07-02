#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

import botocore

from hamcrest import assert_that, equal_to, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicSetQueueAttributes(KikimrSqsTopicTestBase):
    def _create_queue(self, test_name):
        queue_name = self._make_queue_name(test_name)
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']
        return queue_name

    def _get_queue_attribute(self, attribute_name):
        response = self._boto_client.get_queue_attributes(
            QueueUrl=self._queue_url,
            AttributeNames=[attribute_name],
        )
        return response['Attributes'][attribute_name]

    def _set_queue_attribute(self, attribute_name, attribute_value):
        self._boto_client.set_queue_attributes(
            QueueUrl=self._queue_url,
            Attributes={
                attribute_name: attribute_value,
            },
        )

    def _assert_set_queue_attribute_fails(self, attribute_name, attribute_value):
        def set_queue_attribute():
            self._set_queue_attribute(attribute_name, attribute_value)

        assert_that(
            set_queue_attribute,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidParameterValue',
            ),
        )

    def _assert_get_queue_attribute_fails(self, attribute_name):
        def get_queue_attribute():
            self._get_queue_attribute(attribute_name)

        assert_that(
            get_queue_attribute,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidParameterValue',
            ),
        )

    def test_set_queue_attributes_delay_seconds(self):
        self._create_queue('set_queue_attributes_delay_seconds')

        self._set_queue_attribute('DelaySeconds', '5')
        assert_that(self._get_queue_attribute('DelaySeconds'), equal_to('5'))

    def test_set_queue_attributes_maximum_message_size(self):
        self._create_queue('set_queue_attributes_maximum_message_size')

        # TODO: MaximumMessageSize is silently ignored, the value should be applied. Fix it.
        self._set_queue_attribute('MaximumMessageSize', '111111')
        assert_that(self._get_queue_attribute('MaximumMessageSize'), equal_to('262144'))

    def test_set_queue_attributes_message_retention_period(self):
        queue_name = self._make_queue_name('set_queue_attributes_message_retention_period')
        self._queue_url = self._boto_client.create_queue(
            QueueName=queue_name,
            Attributes={
                'MessageRetentionPeriod': '3600',
            },
        )['QueueUrl']

        self._set_queue_attribute('MessageRetentionPeriod', '7200')
        assert_that(self._get_queue_attribute('MessageRetentionPeriod'), equal_to('7200'))

    def test_set_queue_attributes_policy(self):
        self._create_queue('set_queue_attributes_policy')

        # Policy is not supported.
        self._assert_set_queue_attribute_fails(
            'Policy',
            '{"Version":"2012-10-17","Statement":[]}',
        )

    def test_set_queue_attributes_receive_message_wait_time_seconds(self):
        self._create_queue('set_queue_attributes_receive_message_wait_time_seconds')

        self._set_queue_attribute('ReceiveMessageWaitTimeSeconds', '10')
        assert_that(self._get_queue_attribute('ReceiveMessageWaitTimeSeconds'), equal_to('10'))

    def test_set_queue_attributes_visibility_timeout(self):
        self._create_queue('set_queue_attributes_visibility_timeout')

        self._set_queue_attribute('VisibilityTimeout', '60')
        assert_that(self._get_queue_attribute('VisibilityTimeout'), equal_to('60'))

    def test_set_queue_attributes_redrive_policy(self):
        dlq_name = self._make_queue_name('set_queue_attributes_redrive_policy_dlq')
        dlq_url = self._boto_client.create_queue(QueueName=dlq_name)['QueueUrl']
        dlq_arn = self._boto_client.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=['QueueArn'],
        )['Attributes']['QueueArn']

        self._create_queue('set_queue_attributes_redrive_policy')
        redrive_policy = json.dumps({
            'deadLetterTargetArn': dlq_arn,
            'maxReceiveCount': 5,
        })
        self._set_queue_attribute('RedrivePolicy', redrive_policy)

        actual_redrive_policy = json.loads(self._get_queue_attribute('RedrivePolicy'))
        assert_that(actual_redrive_policy['maxReceiveCount'], equal_to(5))
        assert_that(actual_redrive_policy['deadLetterTargetArn'], equal_to(dlq_arn))

        self._boto_client.delete_queue(QueueUrl=dlq_url)

    def test_set_queue_attributes_redrive_allow_policy(self):
        self._create_queue('set_queue_attributes_redrive_allow_policy')

        self._assert_set_queue_attribute_fails(
            'RedriveAllowPolicy',
            '{"redrivePermission":"allowAll"}',
        )

    def test_set_queue_attributes_kms_master_key_id(self):
        self._create_queue('set_queue_attributes_kms_master_key_id')

        # KmsMasterKeyId is not supported: it is accepted on set but not returned by get.
        self._set_queue_attribute('KmsMasterKeyId', 'alias/aws/sqs')
        self._assert_get_queue_attribute_fails('KmsMasterKeyId')

    def test_set_queue_attributes_kms_data_key_reuse_period_seconds(self):
        self._create_queue('set_queue_attributes_kms_data_key_reuse_period_seconds')

        # KmsDataKeyReusePeriodSeconds is not supported: it is accepted on set but not returned by get.
        self._set_queue_attribute('KmsDataKeyReusePeriodSeconds', '300')
        self._assert_get_queue_attribute_fails('KmsDataKeyReusePeriodSeconds')

    def test_set_queue_attributes_sqs_managed_sse_enabled(self):
        self._create_queue('set_queue_attributes_sqs_managed_sse_enabled')

        # SqsManagedSseEnabled is not supported: it is accepted on set but not returned by get.
        self._set_queue_attribute('SqsManagedSseEnabled', 'true')
        self._assert_get_queue_attribute_fails('SqsManagedSseEnabled')

    def test_set_queue_attributes_content_based_deduplication(self):
        self._create_fifo_queue('set_queue_attributes_content_based_deduplication')

        self._set_queue_attribute('ContentBasedDeduplication', 'true')
        assert_that(self._get_queue_attribute('ContentBasedDeduplication'), equal_to('true'))

    def test_set_queue_attributes_deduplication_scope(self):
        self._create_fifo_queue('set_queue_attributes_deduplication_scope')

        # DeduplicationScope is not supported.
        self._assert_set_queue_attribute_fails('DeduplicationScope', 'messageGroup')

    def test_set_queue_attributes_fifo_throughput_limit(self):
        self._create_fifo_queue('set_queue_attributes_fifo_throughput_limit')

        # TODO: FifoThroughputLimit should be supported. Fix it.
        self._assert_set_queue_attribute_fails('FifoThroughputLimit', 'perMessageGroupId')

    def test_set_queue_attributes_fifo_queue(self):
        self._create_fifo_queue('set_queue_attributes_fifo_queue')

        # FifoQueue is immutable: changing it is expected to fail.
        self._assert_set_queue_attribute_fails('FifoQueue', 'false')

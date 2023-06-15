#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from hamcrest import assert_that, equal_to

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS


class TestQueueAttributesInCompatibilityMode(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestQueueAttributesInCompatibilityMode, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['enable_queue_attributes_validation'] = False
        return config_generator

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_set_queue_attributes_no_validation(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo, use_http=True, attributes={'MaximumMessageSize': '1000000'})
        assert_that(self._sqs_api.get_queue_attributes(queue_url)['MaximumMessageSize'], equal_to(str(256 * 1024)))

        self._sqs_api.set_queue_attributes(queue_url, {'MaximumMessageSize': '1'})
        assert_that(self._sqs_api.get_queue_attributes(queue_url)['MaximumMessageSize'], equal_to('1024'))

        # create queue again, this should yield no error
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo, use_http=True)
        # previously set attributes should be right there
        assert_that(self._sqs_api.get_queue_attributes(queue_url)['MaximumMessageSize'], equal_to('1024'))
        try:
            queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo, use_http=True, attributes={'MaximumMessageSize': 'troll'}, retries=1)
        except Exception:
            pass
        else:
            raise Exception('only parseable attributes are valid')


class TestQueueAttributesValidation(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestQueueAttributesValidation, cls)._setup_config_generator()
        return config_generator

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_set_queue_attributes(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo, use_http=True)

        attributes = self._sqs_api.get_queue_attributes(queue_url)
        queue_attributes_list = ['VisibilityTimeout', 'MaximumMessageSize', 'DelaySeconds', 'MessageRetentionPeriod', 'ReceiveMessageWaitTimeSeconds']
        attributes_to_set_and_check = {}
        for attr in queue_attributes_list:
            val = int(attributes[attr])
            # change attributes slightly
            attributes_to_set_and_check[attr] = str(val - 1 if val > 0 else val + 1)
            assert attributes_to_set_and_check[attr] != attributes[attr]

        content_based_dedup_key = 'ContentBasedDeduplication'
        if is_fifo:
            val = attributes[content_based_dedup_key]
            attributes_to_set_and_check[content_based_dedup_key] = 'true' if val == 'false' else 'false'
            assert attributes_to_set_and_check[content_based_dedup_key] != attributes[content_based_dedup_key]

        self._sqs_api.set_queue_attributes(queue_url, attributes_to_set_and_check)
        # match set attributes
        attributes = self._sqs_api.get_queue_attributes(queue_url)
        for attr in attributes_to_set_and_check:
            assert_that(attributes_to_set_and_check[attr], equal_to(attributes[attr]))

        # okay, now we'll try to break it
        for attr in attributes_to_set_and_check:
            try:
                self._sqs_api.set_queue_attributes(queue_url, {attr: '2.5'})
            except Exception as e:
                assert 'InvalidAttributeValue' in str(e)
                assert attr in str(e)
            else:
                assert_that(False)  # expected InvalidAttributeValue exception

            try:
                self._sqs_api.set_queue_attributes(queue_url, {attr: '2147483647'})
            except Exception as e:
                assert 'InvalidAttributeValue' in str(e)
                assert attr in str(e)
            else:
                assert_that(False)  # expected InvalidAttributeValue exception

        try:
            self._sqs_api.set_queue_attributes(queue_url, {'trololo': 'ololo'})
        except Exception as e:
            assert 'InvalidAttributeName' in str(e)
        else:
            assert_that(False)  # expected InvalidAttributeName exception

        # check setting FifoQueue attribute
        if is_fifo:
            self._sqs_api.set_queue_attributes(queue_url, {'FifoQueue': 'true'})  # ok, do nothing
            try:
                self._sqs_api.set_queue_attributes(queue_url, {'FifoQueue': 'omg'})
            except Exception as e:
                assert 'InvalidAttributeValue' in str(e)
                assert 'FifoQueue' in str(e)
            else:
                assert_that(False)  # expected InvalidAttributeValue exception

            # special case: queue type mismatch
            try:
                self._sqs_api.set_queue_attributes(queue_url, {'FifoQueue': 'false'})
            except Exception as e:
                assert 'InvalidAttributeValue' in str(e)
                assert 'Modifying queue type is not supported' in str(e)
            else:
                assert_that(False)  # expected InvalidAttributeValue exception
        else:
            try:
                self._sqs_api.set_queue_attributes(queue_url, {'FifoQueue': 'false'})
            except Exception as e:
                assert 'InvalidAttributeName' in str(e)
            else:
                assert_that(False)  # expected InvalidAttributeName exception

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_create_queue_with_default_attributes(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        # check default queue creation
        queue_url = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)

        expected_attributes = {
            'DelaySeconds': '0',
            'MaximumMessageSize': '262144',
            'MessageRetentionPeriod': '345600',
            'ReceiveMessageWaitTimeSeconds': '0',
            'VisibilityTimeout': '30'
        }

        if is_fifo:
            expected_attributes['ContentBasedDeduplication'] = 'false'
            expected_attributes['FifoQueue'] = 'true'

        attributes = self._sqs_api.get_queue_attributes(queue_url)
        for attr in expected_attributes:
            assert_that(attributes[attr], equal_to(expected_attributes[attr]))

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_create_queue_with_custom_attributes(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        custom_attributes = {
            'DelaySeconds': '1',
            'MaximumMessageSize': '262143',
            'MessageRetentionPeriod': '345599',
            'ReceiveMessageWaitTimeSeconds': '1',
            'VisibilityTimeout': '29'
        }

        if is_fifo:
            custom_attributes['ContentBasedDeduplication'] = 'true'

        queue_url = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo, attributes=custom_attributes)

        attributes = self._sqs_api.get_queue_attributes(queue_url)
        for attr in custom_attributes:
            assert_that(attributes[attr], equal_to(custom_attributes[attr]))

        # get arn by default
        assert_that(attributes['QueueArn'], equal_to('yrn:ya:sqs:ru-central1:' + self._username + ':' + self.queue_name))

        # okay, now we'll try to break it
        for attr in custom_attributes:
            try:
                queue_url = self._sqs_api.create_queue('new_' + self.queue_name, is_fifo=is_fifo, attributes={attr: '2147483647'})
            except Exception as e:
                assert attr in str(e)
            else:
                assert_that(False)  # expected some exception

            try:
                queue_url = self._sqs_api.create_queue('new2_' + self.queue_name, is_fifo=is_fifo, attributes={attr: '2.5'})
            except Exception as e:
                assert attr in str(e)
            else:
                assert_that(False)  # expected some exception

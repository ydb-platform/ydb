#!/usr/bin/env python
# -*- coding: utf-8 -*-
import boto3
import botocore

import random
import logging
import requests
import time
import uuid

import pytest
import ydb.tests.library.common.yatest_common as yatest_common

import ydb
from hamcrest import assert_that, equal_to, not_none, has_item, has_items, is_not, contains_string
from hamcrest import raises, greater_than, not_, less_than
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_tenant_installation
from ydb.tests.library.sqs.test_base import IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS


ANOTHER_TABLES_FORMAT_PARAMS = {
    'argnames': 'another_tables_format',
    'argvalues': [0, 1],
    'ids': ['tables_format_v0', 'tables_format_v1'],
}


class TestSqsYandexCloudMode(get_test_with_sqs_tenant_installation(KikimrSqsTestBase)):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsYandexCloudMode, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['yandex_cloud_mode'] = True
        config_generator.yaml_config['sqs_config']['enable_queue_master'] = True
        config_generator.yaml_config['sqs_config']['enable_dead_letter_queues'] = True
        config_generator.yaml_config['sqs_config']['account_settings_defaults'] = {'max_queues_count': 40}
        config_generator.yaml_config['sqs_config']['background_metrics_update_time_ms'] = 1000

        cls.event_output_file = yatest_common.output_path("events-%s.txt" % random.randint(1, 10000000))
        config_generator.yaml_config['sqs_config']['yc_search_events_config'] = {
            'enable_yc_search': True,
            'output_file_name': cls.event_output_file,
        }
        temp_token_file = yatest_common.work_path("tokenfile")
        with open(temp_token_file, "w") as fl:
            fl.write("root@builtin")

        config_generator.yaml_config['sqs_config']['auth_config'] = {'oauth_token': {'token_file': temp_token_file}}
        return config_generator

    def _before_test_start(self):
        self.cloud_account = f'acc_{uuid.uuid1()}'
        self.iam_token = f'usr_{self.cloud_account}'
        self.folder_id = f'folder_{self.cloud_account}'
        self.cloud_id = f'CLOUD_FOR_{self.folder_id}'

        self._username = self.cloud_id

        logging.info(f'run test with cloud_id={self.cloud_id} folder_id={self.folder_id}')

    def _setup_user(self, _username, retries_count=3):
        pass  # account should be created automatically

    @classmethod
    def create_metauser(cls, cluster, config_generator):
        pass

    def teardown_method(self, method=None):
        self.check_all_users_queues_tables_consistency()
        super(TestSqsYandexCloudMode, self).teardown_method(method)

    def _read_single_message_no_wait(self, queue_url):
        read_result = self._read_messages_and_assert(
            queue_url, messages_count=1, visibility_timeout=0, wait_timeout=0,
            matcher=None
        )
        return read_result

    def _get_queue_arn(self, queue_url):
        return self._sqs_api.get_queue_attributes(queue_url, ['QueueArn'])['QueueArn']

    def _make_boto_client(self, access_key_id, secret_access_key, url):
        session = boto3.session.Session()
        return session.client(
            service_name='sqs',
            aws_access_key_id=access_key_id,  # service account
            aws_secret_access_key=secret_access_key,
            aws_session_token='unused',
            endpoint_url=url,
            region_name='ru-central1'
        )

    def _make_server_url(self):
        host = self.cluster.nodes[1].host
        port = self.cluster_nodes[0].sqs_port
        return 'http://{}:{}'.format(host, port)

    def test_empty_auth_header(self):
        url = self._make_server_url()
        r = requests.post(url, data='Action=ListQueues', headers={'Authorization': 'trollface'})
        assert_that(r.status_code, equal_to(400))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_retryable_iam_error(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        url = self._make_server_url()

        boto_client = self._make_boto_client('TEST_ID_FOR_RETRYIES', 'SECRET', url)

        def list_queues():
            boto_client.list_queues()

        assert_that(
            list_queues,
            raises(
                botocore.exceptions.ClientError,
                pattern='ServiceUnavailable.+IAM authorization error'
            )
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_empty_access_key_id(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        url = self._make_server_url()

        boto_client = self._make_boto_client('sa_' + self._username, 'SECRET', url)

        # basic sanity check
        queue_url = boto_client.create_queue(QueueName=self.queue_name)['QueueUrl']
        assert_that(boto_client.list_queues()['QueueUrls'], has_item(queue_url))
        boto_client.send_message(QueueUrl=queue_url, MessageBody='awesome')

        unknown_boto_client = self._make_boto_client('', 'SECRET', url)

        def list_queues_for_empty_access_key_id():
            unknown_boto_client.list_queues()

        assert_that(
            list_queues_for_empty_access_key_id,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidClientTokenId'
            )
        )

        def send_message_for_empty_access_key_id():
            unknown_boto_client.send_message(QueueUrl=queue_url, MessageBody='psst, wanna some weed?')

        assert_that(
            send_message_for_empty_access_key_id,
            raises(
                botocore.exceptions.ClientError,
                pattern='AccessDenied'
            )
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_list_queues_for_unknown_cloud(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        account_path = '{}/{}'.format(self.sqs_root, self.cloud_id)

        def touch_account_dir():
            assert_that(self._driver.scheme_client.describe_path(account_path).is_directory())

        # no such account
        assert_that(
            touch_account_dir,
            raises(
                ydb.SchemeError,
                pattern='Path not found'
            )
        )

        existing_queues = self._sqs_api.list_queues()
        assert_that(
            len(existing_queues) == 0
        )

        # no such account after list_queues
        assert_that(
            touch_account_dir,
            raises(
                ydb.SchemeError,
                pattern='Path not found'
            )
        )

        self._sqs_api.create_queue(self.queue_name, is_fifo=False)
        touch_account_dir()  # should be ok now

        # wait for user list update
        time.sleep(1.1)

        existing_queues = self._sqs_api.list_queues()
        assert_that(
            len(existing_queues) == 1
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_fifo_groups_with_dlq_in_cloud(self, tables_format):
        is_fifo = True
        self._init_with_params(is_fifo=is_fifo, tables_format=tables_format)
        dlq_name = 'dlq_' + self.queue_name

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        queue_url = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)
        dlq_url = self._sqs_api.create_queue(dlq_name, is_fifo=is_fifo)

        # wait for user list update
        time.sleep(1.1)

        dlq_arn = self._get_queue_arn(dlq_url)

        max_receive_count = 3
        redrive_policy = '{\"deadLetterTargetArn\":\"' + dlq_arn + '\",\"maxReceiveCount\":{}}}'.format(max_receive_count)
        self._sqs_api.set_queue_attributes(queue_url, {'RedrivePolicy': redrive_policy})

        # check one policy
        attributes = self._sqs_api.get_queue_attributes(queue_url)
        assert_that(attributes, has_items('RedrivePolicy'))

        groups = ['fus', 'ro', 'dah']
        for group_id in groups:
            self._send_message_and_assert(queue_url, 'john doe', seq_no=group_id, group_id=group_id)

        for i in range(max_receive_count):
            read_result = self._read_messages_and_assert(
                queue_url, messages_count=len(groups), visibility_timeout=0, wait_timeout=0,
                matcher=None
            )
            assert_that(len(read_result), equal_to(len(groups)))

        self._send_message_and_assert(queue_url, 'jane doe', seq_no='secret', group_id='secret')

        read_result = self._read_messages_and_assert(
            queue_url, messages_count=len(groups) + 1, visibility_timeout=0, wait_timeout=0,
            matcher=None
        )
        assert_that(len(read_result), equal_to(1))  # three messages were moved hence we expect only one
        assert_that(read_result[0]['Body'], 'jane doe')

        # delete secret msg
        receipt_handle = read_result[0]['ReceiptHandle']
        self._sqs_api.delete_message(queue_url, receipt_handle)

        # check dlq contents
        read_result = self._read_messages_and_assert(
            dlq_url, messages_count=len(groups), visibility_timeout=0, wait_timeout=0,
            matcher=None
        )
        assert_that(len(read_result), equal_to(len(groups)))
        for i in range(len(groups)):
            assert_that(read_result[i]['Body'], equal_to('john doe'))

        # check deduplication (this may break if test lasts longer than 5 minutes)
        for group_id in groups:
            self._send_message_and_assert(queue_url, 'john doe', seq_no=group_id, group_id=group_id)

        for i in range(max_receive_count):
            read_result = self._read_messages_and_assert(
                queue_url, messages_count=1, visibility_timeout=0, wait_timeout=0,
                matcher=None
            )
            assert_that(len(read_result), equal_to(0))  # receive nothing

        # check moved messages counter
        counters = self._get_sqs_counters(counters_format='text')
        assert_that(counters, contains_string('MessagesMovedToDLQ'))

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_dlq_message_counters_in_cloud(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        queue_url = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)

        dlq_url = self._sqs_api.create_queue('dlq_' + self.queue_name, is_fifo=is_fifo)

        # wait for user list update
        time.sleep(1.1)

        dlq_arn = self._get_queue_arn(dlq_url)

        max_receive_count = 1
        redrive_policy = '{\"deadLetterTargetArn\":\"' + dlq_arn + '\",\"maxReceiveCount\":{}}}'.format(max_receive_count)
        self._sqs_api.set_queue_attributes(queue_url, {'RedrivePolicy': redrive_policy})

        seq_no = 0

        def get_dlq_message_count():
            return int(self._sqs_api.get_queue_attributes(dlq_url)['ApproximateNumberOfMessages'])

        expected_dlq_message_count = 0

        def wait_for_dlq_message_count(expected_message_count):
            retries = 20
            while retries > 0:
                message_count = get_dlq_message_count()
                if message_count == expected_message_count:
                    return

                time.sleep(0.5)
                retries -= 1
                if retries == 0:
                    # fails the test
                    assert_that(message_count, equal_to(expected_message_count))

        for msg_body in 'test messages famous quartet'.split(' '):
            wait_for_dlq_message_count(expected_dlq_message_count)

            self._send_message_and_assert(queue_url, msg_body, seq_no=str(seq_no) if is_fifo else None, group_id='group' if is_fifo else None)

            for i in range(max_receive_count):
                assert_that(self._read_single_message_no_wait(queue_url)[0]['Body'], equal_to(msg_body))

            # message is moved to DLQ during the call, so we get nothing in response
            assert_that(len(self._read_single_message_no_wait(queue_url)), equal_to(0))

            expected_dlq_message_count += 1
            wait_for_dlq_message_count(expected_dlq_message_count)

            # send message to dlq manually
            self._send_message_and_assert(dlq_url, msg_body, seq_no=str(seq_no) if is_fifo else None, group_id='group2' if is_fifo else None)

            expected_dlq_message_count += 1
            wait_for_dlq_message_count(expected_dlq_message_count)

            # delete the extra msg

            msg_from_dlq = self._read_single_message_no_wait(dlq_url)[0]

            receipt_handle = msg_from_dlq['ReceiptHandle']
            self._sqs_api.delete_message(dlq_url, receipt_handle)

            expected_dlq_message_count -= 1
            wait_for_dlq_message_count(expected_dlq_message_count)

            seq_no += 1

    def test_ymq_expiring_counters(self):
        def get_counters():
            return self._get_ymq_counters(cloud=self.cloud_id, folder=self.folder_id)

        def check_counter(labels, expected):
            if expected > 0:
                value = self._get_counter_value(
                    get_counters(), labels
                )
                assert_that(value, equal_to(expected))
            else:
                assert_that(
                    lambda : self._get_counter_value(get_counters(), labels),
                    raises(KeyError)
                )

        def check_total_count(expected):
            return check_counter({"sensor": "queue.total_count"}, expected)

        def check_messages_sent(q_name, expected):
            return check_counter({"name": "queue.messages.sent_count_per_second", "queue": q_name}, expected)

        def try_check_total_count(expected):
            retry_count = 3
            while retry_count > 0:
                try:
                    check_total_count(expected)
                except AssertionError:
                    retry_count -= 1
                    time.sleep(2)
                else:
                    return
            check_total_count(expected)

        self._sqs_api = self._create_api_for_user(
            self._username, raise_on_error=True, force_private=False,
            iam_token=self.iam_token, folder_id=self.folder_id
        )
        queue_url1 = self._sqs_api.create_queue(self.queue_name, is_fifo=False)
        cloud_q_name = queue_url1.split('/')[4]

        logging.debug("Queue url1: {}".format(queue_url1))
        try_check_total_count(1)

        queue_url2 = self._sqs_api.create_queue('second_' + self.queue_name, is_fifo=False)
        try_check_total_count(2)

        self._send_message_and_assert(queue_url1, "test1")

        check_messages_sent(cloud_q_name, 1)
        self._sqs_api.delete_queue(queue_url1)
        try_check_total_count(1)
        self._sqs_api.delete_queue(queue_url2)
        try_check_total_count(0)
        check_messages_sent(cloud_q_name, 0)

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    @pytest.mark.parametrize(**ANOTHER_TABLES_FORMAT_PARAMS)
    def test_dlq_mechanics_in_cloud(self, is_fifo, tables_format, another_tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        queue1_url = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)

        self._init_with_params(is_fifo, another_tables_format)
        queue2_name = 'second_' + self.queue_name
        queue2_url = self._sqs_api.create_queue(queue2_name, is_fifo=is_fifo)

        # wait for user list update
        time.sleep(1.1)

        queue_1_arn = self._get_queue_arn(queue1_url)
        queue_2_arn = self._get_queue_arn(queue2_url)

        max_receive_count = 2
        redrive_policy = '{\"deadLetterTargetArn\":\"' + queue_2_arn + '\",\"maxReceiveCount\":{}}}'.format(max_receive_count)
        self._sqs_api.set_queue_attributes(queue1_url, {'RedrivePolicy': redrive_policy})
        redrive_policy_2 = '{\"deadLetterTargetArn\":\"' + queue_1_arn + '\",\"maxReceiveCount\":{}}}'.format(max_receive_count)
        self._sqs_api.set_queue_attributes(queue2_url, {'RedrivePolicy': redrive_policy_2})

        # check one policy
        attributes = self._sqs_api.get_queue_attributes(queue1_url)
        assert_that(attributes, has_items('RedrivePolicy'))
        assert_that(attributes['RedrivePolicy'], equal_to(redrive_policy))

        # wait for dead letter queue notification
        time.sleep(self.config_generator.yaml_config['sqs_config']['masters_describer_update_time_ms'] * 1.2 / 1000.0)

        def get_messages_count(queue_url):
            attrs = self._sqs_api.get_queue_attributes(queue_url)
            msg_count = int(attrs['ApproximateNumberOfMessages'])
            infly_msg_count = int(attrs['ApproximateNumberOfMessagesNotVisible'])
            assert_that(infly_msg_count, not_(greater_than(msg_count)))
            return msg_count

        # check that messages are actually moved to dlq and this doesn't break both queues
        lst = [queue1_url, queue2_url]
        seq_no = 0
        for i in range(3):
            lst = lst[::-1]
            q1, q2 = lst
            for msg_body in 'test messages'.split(' '):
                self._send_message_and_assert(q1, msg_body, seq_no=str(seq_no) if is_fifo else None, group_id='group' if is_fifo else None)
                seq_no += 1

                messages_count_metric_update_attempts = 20
                while messages_count_metric_update_attempts:
                    messages_count_metric_update_attempts -= 1

                    messages_count_before = get_messages_count(q1)
                    assert_that(messages_count_before, not_(less_than(0)))
                    if messages_count_before == 0:
                        logging.debug('Wait for proper messages count metric and retry. Attempts left: {}'.format(messages_count_metric_update_attempts))
                        time.sleep(1)
                        continue

                assert_that(messages_count_before, greater_than(0))

                for i in range(max_receive_count):
                    assert_that(self._read_single_message_no_wait(q1)[0]['Body'], equal_to(msg_body))

                # message is moved to DLQ during the call, so we get nothing in response
                assert_that(len(self._read_single_message_no_wait(q1)), equal_to(0))

                messages_count_after = get_messages_count(q1)
                assert_that(messages_count_after, equal_to(messages_count_before - 1))

                # this is our msg
                msg_from_dlq = self._read_single_message_no_wait(q2)[0]
                assert_that(msg_from_dlq['Body'], equal_to(msg_body))

                receipt_handle = msg_from_dlq['ReceiptHandle']
                self._sqs_api.delete_message(q2, receipt_handle)

        # what happens when the dlq is removed? we expect one error on receive at most
        msg_body = 'not gonna leave the source queue'
        self._send_message_and_assert(queue1_url, msg_body, seq_no=str(seq_no) if is_fifo else None, group_id='group' if is_fifo else None)

        for i in range(max_receive_count):
            assert_that(self._read_single_message_no_wait(queue1_url)[0]['Body'], equal_to(msg_body))

        delete_result = self._sqs_api.delete_queue(queue2_url)
        assert_that(
            delete_result, not_none()
        )

        # waiting until the message appears in queue1 again
        result_list = self._read_while_not_empty(
            queue_url=queue1_url,
            messages_count=1,
            visibility_timeout=0,
            wait_timeout=10
        )
        assert_that(result_list[0]['Body'], equal_to(msg_body))

        # getting the message until it's moved to dlq
        for i in range(max_receive_count):
            self._read_single_message_no_wait(queue1_url)

        # check moved messages counter
        counters = self._get_sqs_counters(counters_format='text')
        assert_that(counters, contains_string('MessagesMovedToDLQ'))

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_dlq_setup_in_cloud(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        queue_url = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)
        dlq_name = 'dlq_for_' + self.queue_name
        dlq_url = self._sqs_api.create_queue(dlq_name, is_fifo=is_fifo)

        # wait for user list update
        time.sleep(1.1)

        for url in [queue_url, dlq_url]:
            attributes = self._sqs_api.get_queue_attributes(url)
            assert_that(attributes, has_items(
                'CreatedTimestamp',
                'MaximumMessageSize',
                'VisibilityTimeout',
                'QueueArn'
            ))
            assert_that(attributes, is_not(has_items('RedrivePolicy')))

        dlq_arn = self._get_queue_arn(dlq_url)
        assert_that(dlq_arn, equal_to('yrn:yc:ymq:ru-central1:{}:{}'.format(self.folder_id, dlq_name)))

        # set policy
        def test_policy(arn_to_set, max_receive_count, expected_error):
            def call():
                self._sqs_api.set_queue_attributes(queue_url, {'RedrivePolicy': '{\"deadLetterTargetArn\":\"' + arn_to_set + '\",\"maxReceiveCount\":\"' + max_receive_count + '\"}'})

            assert_that(
                call,
                raises(
                    RuntimeError,
                    pattern=expected_error
                )
            )

        # bad max receive count
        test_policy('yrn:yc:ymq:ru-central1:{}:{}'.format(self.folder_id, dlq_name), '0', 'InvalidParameterValue')
        # bad prefix
        test_policy('yarr:yc:ymq:ru-central1:{}:{}'.format(self.folder_id, dlq_name), '1', 'InvalidParameterValue')
        # bad region
        test_policy('yrn:yc:ymq:ru-central42:{}:{}'.format(self.folder_id, dlq_name), '1', 'InvalidParameterValue')
        # using queue itself as a dlq is prohibited
        test_policy('yrn:yc:ymq:ru-central1:{}:{}'.format(self.folder_id, self.queue_name), '1', 'InvalidParameterValue')
        # inexistent queue
        test_policy('yrn:yc:ymq:ru-central1:{}:omg_{}'.format(self.folder_id, dlq_name), '1', 'AWS.SimpleQueueService.NonExistentQueue')

        def make_redrive_policy(target_queue_name):
            return '{\"deadLetterTargetArn\":\"yrn:yc:ymq:ru-central1:' + self.folder_id + ':' + target_queue_name + '\",\"maxReceiveCount\":847}'

        redrive_policy = make_redrive_policy(dlq_name)
        self._sqs_api.set_queue_attributes(queue_url, {'RedrivePolicy': redrive_policy})

        # check policy
        attributes = self._sqs_api.get_queue_attributes(queue_url)
        assert_that(attributes, has_items('RedrivePolicy'))
        assert_that(attributes['RedrivePolicy'], equal_to(redrive_policy))

        attributes = self._sqs_api.get_queue_attributes(dlq_url)
        assert_that(attributes, is_not(has_items('RedrivePolicy')))
        # list source queues
        result = self._sqs_api.list_dead_letter_source_queues(dlq_url)
        assert_that(len(result), equal_to(1))
        assert_that(result, has_item(queue_url))

        result = self._sqs_api.list_dead_letter_source_queues(queue_url)
        assert_that(result is None)

        # remove policy
        self._sqs_api.set_queue_attributes(queue_url, {'RedrivePolicy': ''})

        # check policy
        attributes = self._sqs_api.get_queue_attributes(queue_url)
        assert_that(attributes, is_not(has_items('RedrivePolicy')))

        # check dlq
        result = self._sqs_api.list_dead_letter_source_queues(dlq_url)
        assert_that(result is None)

        result = self._sqs_api.list_dead_letter_source_queues(queue_url)
        assert_that(result is None)

        # check attributed queue creation

        # use the queue itself as dlq
        queue_with_dlq = 'with_dlq_' + self.queue_name
        bad_redrive_policy = make_redrive_policy(queue_with_dlq)

        def create_ill_attributed_queue():
            self._sqs_api.create_queue(queue_with_dlq, is_fifo=is_fifo, attributes={'RedrivePolicy': bad_redrive_policy})

        assert_that(
            create_ill_attributed_queue,
            raises(
                RuntimeError,
                pattern='Using the queue itself as a dead letter queue is not allowed.'
            )
        )

        # normal attributed queue creation
        queue_with_dlq_url = self._sqs_api.create_queue(queue_with_dlq, is_fifo=is_fifo, attributes={'RedrivePolicy': redrive_policy})

        # wait for user list update
        time.sleep(1.1)

        # check dlq
        result = self._sqs_api.list_dead_letter_source_queues(dlq_url)
        assert_that(len(result), equal_to(1))
        assert_that(result, has_item(queue_with_dlq_url))

        # check policy
        attributes = self._sqs_api.get_queue_attributes(queue_with_dlq_url)
        assert_that(attributes, has_items('RedrivePolicy'))
        assert_that(attributes['RedrivePolicy'], equal_to(redrive_policy))

    def test_list_clouds(self):
        def call():
            self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=False, iam_token=self.iam_token, folder_id=self.folder_id)
            result = self._sqs_api.list_users()

            assert_that(len(result), equal_to(1))

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_cloud_queues_with_iam_token(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        queue_url = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)

        # wait for user list update
        time.sleep(1.1)

        assert_that(
            (self.cloud_id in queue_url) and (self.queue_name in queue_url)
        )
        received_queue_url = self._sqs_api.get_queue_url(self.queue_name)
        assert_that(
            received_queue_url, equal_to(queue_url)
        )

        # check broken queue url
        try:
            parts = received_queue_url.split('/')
            parts[-2] = ''
            self._sqs_api.set_queue_attributes('/'.join(parts), {'RedrivePolicy': ''})
        except Exception as e:
            assert 'InvalidParameterValue' in str(e)
        else:
            assert_that(False)  # expected InvalidParameterValue

        if is_fifo:
            msg_id = self._sqs_api.send_message(received_queue_url, 'fifo test', deduplication_id='42', group_id='666')
        else:
            msg_id = self._sqs_api.send_message(received_queue_url, 'std test')

        assert msg_id is not None

        res = self._read_while_not_empty(received_queue_url, 1)
        assert_that(res[0]['MessageId'], equal_to(msg_id))

        # berkanavt_5 and his queue
        existing_queues = self._sqs_api.list_queues()
        assert_that(
            received_queue_url in existing_queues
        )
        # change folder - should be empty
        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=f'{self.folder_id}_other')
        existing_queues = self._sqs_api.list_queues()
        assert_that(
            received_queue_url not in existing_queues
        )
        # alkoberkanavt_5 is a friend, so he has access to self.folder_id
        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token='usr_alkoberkanavt_5', folder_id=self.folder_id)
        existing_queues = self._sqs_api.list_queues()
        assert_that(
            received_queue_url in existing_queues
        )

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=False, iam_token=self.iam_token, folder_id=self.folder_id)
        existing_queues = self._sqs_api.list_queues()
        assert_that(
            received_queue_url in existing_queues
        )

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        existing_queues = self._sqs_api.list_queues()
        assert_that(
            received_queue_url in existing_queues
        )

        # there is no folder for alkonavt
        def call():
            self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id='FOLDER_alkonavt')
            self._sqs_api.list_queues()

        assert_that(
            call,
            raises(
                RuntimeError,
                pattern='Request id to report: '
            )
        )

        # Check counters
        counters = self._get_sqs_counters(counters_format='text')
        assert_that(counters, contains_string('AuthorizeDuration'))
        assert_that(counters, contains_string('GetFolderIdDuration'))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_count_queues(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        folder_api_1 = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        folder_api_2 = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=f'{self.folder_id}_other')

        assert_that(folder_api_1.private_count_queues(), equal_to('0'))
        assert_that(folder_api_2.private_count_queues(), equal_to('0'))

        url_1 = None
        for i in range(4):
            url_1 = folder_api_1.create_queue('queue_{}'.format(i))

        url_2 = None
        for i in range(2):
            url_2 = folder_api_2.create_queue('queue_{}'.format(i))

        # wait for update
        time.sleep(2.1)

        assert_that(folder_api_1.private_count_queues(), equal_to('4'))
        assert_that(folder_api_2.private_count_queues(), equal_to('2'))

        folder_api_1.delete_queue(url_1)
        folder_api_2.delete_queue(url_2)

        # wait for update
        time.sleep(2.1)

        assert_that(folder_api_1.private_count_queues(), equal_to('3'))
        assert_that(folder_api_2.private_count_queues(), equal_to('1'))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_queues_count_over_limit(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        # Separate test in cloud tests, because cloud queue creation differ from yandex one.
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        urls = []
        for i in range(40):
            urls.append(self._sqs_api.create_queue('queue_{}'.format(i)))

        def call():
            self._sqs_api.create_queue('extra_queue')

        assert_that(
            call,
            raises(
                RuntimeError,
                pattern='OverLimit'
            )
        )

        self._sqs_api.delete_queue(urls[5])
        self._sqs_api.create_queue('extra_queue')

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_queue_counters_are_in_folder(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_name = 'queue'
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        queue_url = self._sqs_api.create_queue(queue_name)
        logging.debug('Queue url: {}'.format(queue_url))

        # wait for user list update
        time.sleep(1.1)

        self._sqs_api.send_message(queue_url, 'data')

        def get_queue_resource_id(queue_url, cloud_id, queue_name):
            # queue url == http://<host:port>/<cloud_id>/<resource_id>/<queue_name>
            folder_index = queue_url.find(cloud_id)
            assert folder_index != -1
            resource_id_start_index = folder_index + len(cloud_id) + 1
            resource_id_end_index = len(queue_url) - len(queue_name) - 1
            return queue_url[resource_id_start_index:resource_id_end_index]

        counters = self._get_sqs_counters()
        labels = {
            'subsystem': 'core',
            'user': self.cloud_id,
            'folder': self.folder_id,
            'queue': get_queue_resource_id(queue_url, self.cloud_id, queue_name),
            'sensor': 'SendMessage_Count',
        }
        assert_that(self._get_counter_value(counters, labels), equal_to(1))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_yc_events_processor(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_name1 = 'queue1'
        queue_name2 = 'queue2'
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        self._sqs_api.create_queue(queue_name1)
        self._sqs_api.create_queue(queue_name2)

        attempts_count = 10
        lines = []
        # Simply check that records get to file. Content may be variable as several tests run upon one cluster
        while attempts_count:
            attempts_count -= 1

            with open(self.event_output_file, "r") as fl:
                lines = fl.readlines()
            if len(lines) < 2:
                time.sleep(5)
                continue
            else:
                break

        assert len(lines) >= 2, "Got only %s event lines after all attempts" % len(lines)

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    def test_cloud_double_create_queue(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)
        queue_url1 = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)
        time.sleep(1)
        queue_url2 = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)
        assert queue_url1 == queue_url2, f'{queue_url1} vs {queue_url2}'

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    def test_not_throttling_with_custom_queue_name(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._sqs_api = self._create_api_for_user(
            user_name='ignored',
            raise_on_error=True,
            force_private=True,
            iam_token=self.iam_token,
            folder_id=self.folder_id,
        )

        custom_queue_name = 'MyCustomQueue'
        queue_url = self._sqs_api.create_queue(
            queue_name=self.queue_name,
            private_api=True,
            custom_name=custom_queue_name,
        )

        nonexistent_queue_url = queue_url.replace(self.queue_name, self.queue_name + '_nonex')

        def get_attributes_of_nonexistent_queue():
            self._sqs_api.get_queue_attributes(nonexistent_queue_url)

        # Draining budget
        for _ in range(16):
            try:
                get_attributes_of_nonexistent_queue()
            except Exception:
                pass

        # Check that there is no more budget
        assert_that(
            get_attributes_of_nonexistent_queue,
            raises(
                RuntimeError,
                pattern=".*<Code>ThrottlingException</Code>.*"
            )
        )

        received_queue_url = self._sqs_api.get_queue_url(custom_queue_name)
        assert received_queue_url == queue_url

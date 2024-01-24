#!/usr/bin/env python
# -*- coding: utf-8 -*-
import random
import logging
import uuid

import ydb.tests.library.common.yatest_common as yatest_common

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_tenant_installation


class TestYmqQueueCounters(get_test_with_sqs_tenant_installation(KikimrSqsTestBase)):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestYmqQueueCounters, cls)._setup_config_generator()
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
        super(TestYmqQueueCounters, self).teardown_method(method)

    def _get_queue_resource_id(self, queue_url, queue_name):
        folder_index = queue_url.find(self.cloud_id)
        assert folder_index != -1
        resource_id_start_index = folder_index + len(self.cloud_id) + 1
        resource_id_end_index = len(queue_url) - len(queue_name) - 1
        return queue_url[resource_id_start_index:resource_id_end_index]

    def test_ymq_send_read_delete(self):
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        queue_url = self._sqs_api.create_queue(self.queue_name)
        queue_resource_id = self._get_queue_resource_id(queue_url, self.queue_name)

        message_payload = "foobar"

        self._sqs_api.send_message(queue_url, message_payload)
        handle = self._read_while_not_empty(queue_url, 1)[0]["ReceiptHandle"]
        self._sqs_api.delete_message(queue_url, handle)

        ymq_counters = self._get_ymq_counters(cloud=self.cloud_id, folder=self.folder_id)

        send_message_count = self._get_counter_value(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.sent_count_per_second',
        })
        assert send_message_count == 1

        send_message_bytes_written = self._get_counter_value(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.sent_bytes_per_second',
        })
        assert send_message_bytes_written == len(message_payload)

        message_receive_attempts = self._get_counter(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.receive_attempts_count_rate',
        })
        assert message_receive_attempts['hist']['buckets'][0] == 1

        receive_message_count = self._get_counter_value(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.received_count_per_second',
        })
        assert receive_message_count == 1

        recide_duration_buckets = self._get_counter(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.reside_duration_milliseconds',
        })['hist']['buckets']
        assert any(map(lambda x: x > 0, recide_duration_buckets))

        receive_message_bytes_read = self._get_counter_value(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.received_bytes_per_second',
        })
        assert receive_message_bytes_read > 0

        client_message_processing_duration_buckets = self._get_counter(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.client_processing_duration_milliseconds',
        })['hist']['buckets']
        assert any(map(lambda x: x > 0, client_message_processing_duration_buckets))

    def test_counters_when_sending_duplicates(self):
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        fifo_queue_name = self.queue_name + ".fifo"
        queue_url = self._sqs_api.create_queue(queue_name=fifo_queue_name, is_fifo=True)
        queue_resource_id = self._get_queue_resource_id(queue_url, fifo_queue_name)

        for _ in range(2):
            self._sqs_api.send_message(
                queue_url=queue_url,
                message_body="foobar",
                deduplication_id="deduplication_id",
                group_id="group_id",
            )

        ymq_counters = self._get_ymq_counters(cloud=self.cloud_id, folder=self.folder_id)

        send_message_deduplication_count = self._get_counter_value(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.deduplicated_count_per_second',
        })
        assert send_message_deduplication_count == 1

    def test_counters_when_reading_from_empty_queue(self):
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        queue_url = self._sqs_api.create_queue(self.queue_name)
        queue_resource_id = self._get_queue_resource_id(queue_url, self.queue_name)

        self._read_while_not_empty(queue_url, 1)

        ymq_counters = self._get_ymq_counters(cloud=self.cloud_id, folder=self.folder_id)
        receive_message_empty_count = self._get_counter_value(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.empty_receive_attempts_count_per_second',
        })
        assert receive_message_empty_count == 1

    def test_sqs_action_counters(self):

        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        queue_url = self._sqs_api.create_queue(self.queue_name)
        queue_resource_id = self._get_queue_resource_id(queue_url, self.queue_name)

        message_payload = "foobar"

        self._sqs_api.send_message(queue_url, message_payload)
        self._read_while_not_empty(queue_url, 1)

        ymq_counters = self._get_ymq_counters(cloud=self.cloud_id, folder=self.folder_id)

        successes = self._get_counter_value(ymq_counters, {
            'queue': queue_resource_id,
            'method': 'receive_message',
            'name': 'api.http.requests_count_per_second',
        })
        assert successes == 1

        durations = self._get_counter(ymq_counters, {
            'queue': queue_resource_id,
            'method': 'receive_message',
            'name': 'api.http.request_duration_milliseconds',
        })
        duration_buckets = durations['hist']['buckets']
        assert any(map(lambda x: x > 0, duration_buckets))

    def test_purge_queue_counters(self):

        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        queue_url = self._sqs_api.create_queue(self.queue_name)
        queue_resource_id = self._get_queue_resource_id(queue_url, self.queue_name)

        for _ in range(20):
            self._sqs_api.send_message(queue_url, "foobar")
            self._sqs_api.purge_queue(queue_url)

        ymq_counters = self._get_ymq_counters(cloud=self.cloud_id, folder=self.folder_id)
        purged_derivative = self._get_counter_value(ymq_counters, {
            'queue': queue_resource_id,
            'name': 'queue.messages.purged_count_per_second',
        })
        assert purged_derivative > 0

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import pytest
from hamcrest import assert_that, equal_to, none, not_, greater_than, raises

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, TABLES_FORMAT_PARAMS


class TestSqsCountersFeatures(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsCountersFeatures, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['create_lazy_counters'] = False
        config_generator.yaml_config['sqs_config']['queue_attributes_cache_time_ms'] = 1000
        config_generator.yaml_config['sqs_config']['user_settings_update_time_ms'] = 1000  # Checking settings table.
        return config_generator

    def enable_detailed_queue_counters(self):
        version = self._get_queue_version_number(self._username, self.queue_name)
        attributes_path = self._smart_make_table_path(self._username, self.queue_name, version, None, 'Attributes')

        deadline = int((time.time() + 600) * 1000)
        query_key = {'name': 'State', 'value': 0}
        if self.get_tables_format() != 0:
            query_key = {'name': 'QueueIdNumber', 'value': self._get_queue_version_number(self._username, self.queue_name)}

        self._execute_yql_query(
            f'''UPDATE `{attributes_path}` SET ShowDetailedCountersDeadline = {deadline}
                WHERE {query_key['name']} = {query_key['value']}'''
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_creates_counter(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name)
        self._sqs_api.get_queue_attributes(queue_url)  # Ensure that counters structure is initialized.

        counters = self._get_sqs_counters()
        send_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'SendMessage_Count',
        }
        counter = self._get_counter_value(counters, send_counter_labels, None)
        assert_that(counter, equal_to(0))

    @pytest.mark.parametrize('switch_user', argvalues=[True, False], ids=['user', 'queue'])
    def test_detailed_counters(self, switch_user):
        queue_url = self._create_queue_and_assert(self.queue_name)
        self._sqs_api.get_queue_attributes(queue_url)

        counters = self._get_sqs_counters()
        counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'TransactionsCount',
        }
        counter = self._get_counter_value(counters, counter_labels, None)
        assert_that(counter, none())

        if switch_user:
            deadline = int((time.time() + 600) * 1000)
            self._execute_yql_query('UPSERT INTO `{}/.Settings` (Account, Name, Value) VALUES (\'{}\', \'ShowDetailedCountersDeadlineMs\', \'{}\')'
                                    .format(self.sqs_root, self._username, deadline))
        else:
            self.enable_detailed_queue_counters()

        time.sleep(2)  # Wait attributes/settings cache time
        self._sqs_api.send_message(queue_url, 'data')

        counters = self._get_sqs_counters()
        counter = self._get_counter_value(counters, counter_labels, None)
        assert_that(counter, not_(none()))
        assert_that(counter, greater_than(0))

    def test_disables_user_counters(self):
        self._sqs_api.list_queues()  # init user's structure in server
        counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'sensor': 'RequestTimeouts',
        }
        counters = self._get_sqs_counters()
        counter = self._get_counter_value(counters, counter_labels, None)
        assert_that(counter, equal_to(0))

        def disable_user_counters(disable):
            self._execute_yql_query('UPSERT INTO `{}/.Settings` (Account, Name, Value) VALUES (\'{}\', \'DisableCounters\', \'{}\')'
                                    .format(self.sqs_root, self._username, '1' if disable else '0'))

        disable_user_counters(True)

        attempts = 50
        while attempts:
            attempts -= 1

            counters = self._get_sqs_counters()
            counter = self._get_counter_value(counters, counter_labels, None)

            if counter is not None and attempts:
                time.sleep(0.5)
                continue

            assert_that(counter, none())
            break

        disable_user_counters(False)

        attempts = 50
        while attempts:
            attempts -= 1

            counters = self._get_sqs_counters()
            counter = self._get_counter_value(counters, counter_labels, None)

            if counter is None and attempts:
                time.sleep(0.5)
                continue

            assert_that(counter, equal_to(0))
            break

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_removes_user_counters_after_user_deletion(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        self._sqs_api.list_queues()  # init user's structure in server
        counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'sensor': 'RequestTimeouts',
        }
        counters = self._get_sqs_counters()
        counter = self._get_counter_value(counters, counter_labels, None)
        assert_that(counter, equal_to(0))

        self._sqs_api.delete_user(self._username)

        attempts = 50
        while attempts:
            attempts -= 1

            counters = self._get_sqs_counters()
            counter = self._get_counter_value(counters, counter_labels, None)

            if counter is not None and attempts:
                time.sleep(0.2)
                continue

            assert_that(counter, none())
            break

    @pytest.mark.parametrize('switch_user', argvalues=[True, False], ids=['user', 'queue'])
    def test_aggregates_transaction_counters(self, switch_user):
        queue_url = self._create_queue_and_assert(self.queue_name)
        self._sqs_api.send_message(queue_url, 'data')

        counters = self._get_sqs_counters()
        total_counter_labels = {
            'subsystem': 'core',
            'user': 'total',
            'queue': 'total',
            'sensor': 'TransactionsByType',
            'query_type': 'WRITE_MESSAGE_ID',
        }
        user_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': 'total',
            'sensor': 'TransactionsByType',
            'query_type': 'WRITE_MESSAGE_ID',
        }
        queue_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'TransactionsByType',
            'query_type': 'WRITE_MESSAGE_ID',
        }
        total_counter = self._get_counter_value(counters, total_counter_labels)
        assert_that(total_counter, not_(none()))

        if switch_user:
            self._execute_yql_query('UPSERT INTO `{}/.Settings` (Account, Name, Value) VALUES (\'{}\', \'ExportTransactionCounters\', \'1\')'
                                    .format(self.sqs_root, self._username))
        else:
            self.enable_detailed_queue_counters()

        attempts = 50
        while attempts:
            attempts -= 1

            self._sqs_api.send_message(queue_url, 'data')

            counters = self._get_sqs_counters()
            new_total_counter = self._get_counter_value(counters, total_counter_labels)
            assert_that(new_total_counter, equal_to(total_counter + 1))
            total_counter = new_total_counter

            user_counter = self._get_counter_value(counters, user_counter_labels)
            queue_counter = self._get_counter_value(counters, queue_counter_labels)
            if (user_counter is None or queue_counter is None and not switch_user) and attempts:
                time.sleep(0.2)
                continue

            if not switch_user:
                assert_that(user_counter, not_(none()))
                assert_that(queue_counter, not_(none()))
            break

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_updates_status_code_counters_when_parsing_errors_occur(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        self._sqs_api.list_queues()  # init user's structure in server
        counter_labels = {
            'subsystem': 'core',
            'user': 'total',
            'sensor': 'StatusesByType',
            'status_code': 'InvalidParameterValue',
        }
        counters = self._get_sqs_counters()
        prev_counter = self._get_counter_value(counters, counter_labels, 0)

        def call_receive():
            self._sqs_api.receive_message('http://sqs.yandex.net/user/queue_url', max_number_of_messages=-42)

        assert_that(
            call_receive,
            raises(
                RuntimeError,
                pattern='.*MaxNumberOfMessages.*'
            )
        )

        counters = self._get_sqs_counters()
        counter = self._get_counter_value(counters, counter_labels, 0)

        assert_that(counter, equal_to(prev_counter + 1))


class TestSqsCountersExportDelay(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsCountersExportDelay, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['create_lazy_counters'] = False
        config_generator.yaml_config['sqs_config']['queue_counters_export_delay_ms'] = 2000
        return config_generator

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_export_delay(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'RequestTimeouts',
        }

        queue_url = self._create_queue_and_assert(self.queue_name)
        attributes = self._sqs_api.get_queue_attributes(queue_url, attributes=['CreatedTimestamp'])

        counters = self._get_sqs_counters()
        time_counters_got = time.time()

        time_passed = time_counters_got - int(attributes['CreatedTimestamp'])
        if time_passed < 2:
            logging.debug('Time passed: {}'.format(time_passed))
            counter = self._get_counter_value(counters, counter_labels, None)
            assert_that(counter, none())
        else:
            logging.debug('Too much time passed: {}'.format(time_passed))

        attempts = 50
        while attempts:
            attempts -= 1

            counters = self._get_sqs_counters()
            counter = self._get_counter_value(counters, counter_labels, None)
            if counter is None and attempts:
                time.sleep(0.2)
                continue

            assert_that(counter, equal_to(0))
            break

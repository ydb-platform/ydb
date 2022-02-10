#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
 
import pytest 
import yatest 
 
from sqs_test_base import KikimrSqsTestBase, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation, IS_FIFO_PARAMS 
 
 
class MultiplexingTablesFormatTest(KikimrSqsTestBase): 
    def _set_new_format_settings(self, username=None, tables_format=1): 
        if username is None: 
            username = self._username 
        self._execute_yql_query( 
            f'UPSERT INTO `{self.sqs_root}/.Settings` (Account, Name, Value) \ 
                VALUES ("{username}", "CreateQueuesWithTabletFormat", "{tables_format}")' 
        ) 
 
    def create_queue(self, is_fifo): 
        if is_fifo: 
            self.queue_name = self.queue_name + '.fifo' 
        self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo) 
 
    def create_queue_must_fail(self, is_fifo): 
        try: 
            self.create_queue(is_fifo) 
        except yatest.common.process.ExecutionError: 
            return 
        assert(False) 
 
    def create_queue_with_wrong_tables_format(self, tables_format): 
        self._set_new_format_settings(tables_format='qwerty') 
        self.create_queue_must_fail(True) 
        self.create_queue_must_fail(False) 
 
    @pytest.mark.parametrize(**IS_FIFO_PARAMS) 
    def test_create_queue(self, is_fifo): 
        self._set_new_format_settings() 
        self.create_queue(is_fifo) 
 
    def test_create_queue_with_incorrect_tables_format(self): 
        self.create_queue_with_wrong_tables_format('qwerty') 
 
    def test_create_queue_with_empty_tables_format(self): 
        self.create_queue_with_wrong_tables_format('') 
 
    def test_create_queue_with_unsupported_tables_format(self): 
        self.create_queue_with_wrong_tables_format(2) 
 
 
class TestMultiplexingTablesFormatWithTenant(get_test_with_sqs_tenant_installation(MultiplexingTablesFormatTest)): 
    pass 
 
 
class TestMultiplexingTablesFormatWithPath(get_test_with_sqs_installation_by_path(MultiplexingTablesFormatTest)): 
    pass 

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import pytest
from hamcrest import assert_that, none, is_not, is_, raises

import re
import yatest

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation
from ydb.tests.library.sqs.test_base import TABLES_FORMAT_PARAMS


class SqsACLTest(KikimrSqsTestBase):
    def _modify_permissions(self, resource, action, subject, permissions_string):
        cmd = [
            'curl',
            '-v',
            'localhost:'+str(self._http_port)+'?Action='+action+'Permissions&UserName=metauser&Subject='+subject+permissions_string+'&Path='+resource,
            '-H',
            'authorization: aaa credential=abacaba/20220830/ec2/aws4_request'
        ]
        retries_count = 1
        while retries_count:
            logging.debug("Running {}".format(' '.join(cmd)))
            try:
                yatest.common.execute(cmd)
            except yatest.common.ExecutionError as ex:
                logging.debug("Modify permissions failed: {}. Retrying".format(ex))
                retries_count -= 1
                time.sleep(3)
            else:
                return
        raise RuntimeError("Failed to modify permissions")

    def _list_permissions(self, path, retries_count=1):
        cmd = [
            'curl',
            '-v',
            'localhost:'+str(self._http_port)+'?Action=ListPermissions&UserName=metauser&Path='+path,
            '-H',
            'authorization: aaa credential=abacaba/20220830/ec2/aws4_request'
        ]

        while retries_count:
            logging.debug("Running {}".format(' '.join(cmd)))
            try:
                execute = yatest.common.execute(cmd)
            except yatest.common.ExecutionError as ex:
                logging.debug("List permissions failed: {}. Retrying".format(ex))
                retries_count -= 1
                time.sleep(3)
            else:
                return execute.std_out
        raise RuntimeError("Failed to list permissions")

    def _extract_permissions_for(self, sid, message):
        permissions = set()
        for part in message.decode('utf-8').split('<Subject>'):
            if sid not in part:
                continue

            for probably_name in re.split('<|>', part):
                logging.debug("PARSED {}".format(probably_name))
                if 'Subject' in probably_name:
                    continue
                if 'Resource' in probably_name:
                    break
                if 'Permission' in probably_name:
                    continue
                if 'Response' in probably_name:
                    continue
                if sid in probably_name:
                    continue

                probably_name = "".join(list(filter(lambda x: x.isalnum(), probably_name)))
                if probably_name:
                    permissions.add(probably_name)

        return list(permissions)

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_modify_permissions(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        self._send_message_and_assert(queue_url, 'data')

        alkonavt_sid = 'alkonavt@builtin'

        create_queue_permission = 'CreateQueue'
        send_message_permission = 'SendMessage'

        # no permissions expected
        description = self._extract_permissions_for(alkonavt_sid, self._list_permissions(self._username))
        assert description == []

        # two permissions expected
        self._modify_permissions(
            self._username,
            'Grant',
            alkonavt_sid,
            '&Permission.1=CreateQueue&Permission.2=SendMessage'
        )
        description = self._list_permissions(self._username)
        assert sorted(self._extract_permissions_for(alkonavt_sid, description)) == [create_queue_permission, send_message_permission]

        # single permission expected
        self._modify_permissions(self._username, 'Revoke', alkonavt_sid, '&Permission.1=CreateQueue')
        description = self._list_permissions(self._username)
        assert self._extract_permissions_for(alkonavt_sid, description) == [send_message_permission]

        receive_message_permission = 'ReceiveMessage'

        # other single permission expected
        self._modify_permissions(self._username, 'Set', alkonavt_sid, '&Permission.1=ReceiveMessage')
        description = self._list_permissions(self._username)
        assert self._extract_permissions_for(alkonavt_sid, description) == [receive_message_permission]

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_apply_permissions(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        self._send_message_and_assert(queue_url, 'data')

        berkanavt_sid = 'berkanavt@builtin'

        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=False, security_token=berkanavt_sid)

        def __send_message_with_retries(_queue_url, data, result_predicate):
            retries = 3

            while retries > 0:
                retries = retries - 1
                try:
                    result = self._sqs_api.send_message(_queue_url, data)
                    assert_that(result, result_predicate)
                    return result
                except Exception:
                    if retries == 0:
                        raise
                    time.sleep(0.1)

        __send_message_with_retries(queue_url, 'megadata', is_(none()))  # no access

        self._modify_permissions(
            self._username,
            'Grant',
            berkanavt_sid,
            '&Permission.1=ModifyPermissions'
        )

        result = self._sqs_api.modify_permissions('Grant', berkanavt_sid, self._username, ['SendMessage', 'DescribePath'])
        assert_that(result, is_not(none()))

        __send_message_with_retries(queue_url, 'utradata', is_not(none()))  # has access

        result = self._sqs_api.modify_permissions('Revoke', berkanavt_sid, self._username, ['SendMessage', 'AlterQueue'])
        assert_that(result, is_not(none()))

        __send_message_with_retries(queue_url, 'superdata', is_(none()))  # no access again. that's a pity

        result = self._sqs_api.list_permissions(self._username)
        assert 'Account' in str(result)
        assert berkanavt_sid in str(result)
        assert 'Permissions' in str(result)


class SqsWithForceAuthorizationTest(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(SqsWithForceAuthorizationTest, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['force_access_control'] = True
        return config_generator

    def _setup_user(self, _username, retries_count=3):
        pass

    @classmethod
    def create_metauser(cls, cluster, config_generator):
        pass

    @pytest.mark.parametrize(argnames='token,pattern',
                             argvalues=[('invalid_token', 'AccessDeniedException'), ('', 'No security token was provided.'), (None, 'InvalidClientTokenId')],
                             ids=['invalid', 'empty', 'no'])
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_invalid_token(self, token, pattern, tables_format):
        self._init_with_params(tables_format=tables_format)
        sqs_api = self._create_api_for_user(self._username, raise_on_error=True, security_token=token)

        def call_list():
            sqs_api.list_queues()

        def call_get_queue_url():
            sqs_api.get_queue_url('queue_name')

        assert_that(
            call_list,
            raises(
                RuntimeError,
                pattern=pattern
            )
        )

        assert_that(
            call_get_queue_url,
            raises(
                RuntimeError,
                pattern=pattern
            )
        )


class TestSqsACLWithTenant(get_test_with_sqs_tenant_installation(SqsACLTest)):
    pass


class TestSqsACLWithPath(get_test_with_sqs_installation_by_path(SqsACLTest)):
    pass


class TestSqsWithForceAuthorizationWithTenant(get_test_with_sqs_tenant_installation(SqsWithForceAuthorizationTest)):
    pass


class TestSqsWithForceAuthorizationWithPath(get_test_with_sqs_installation_by_path(SqsWithForceAuthorizationTest)):
    pass

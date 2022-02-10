#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import pytest 
from hamcrest import assert_that, equal_to, none, is_not, is_, raises 

import ydb.tests.library.common.yatest_common as yatest_common

from sqs_test_base import KikimrSqsTestBase, get_sqs_client_path, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation, to_bytes
 
 
class SqsACLTest(KikimrSqsTestBase): 
    def _modify_permissions(self, resource, action, permissions_string, clear_acl_flag=False):
        cmd = [
            get_sqs_client_path(),
            'permissions',
            '--resource', resource,
            '--{}'.format(action),
            '{}'.format(permissions_string)
        ]
        if clear_acl_flag:
            cmd.append('--clear-acl')

        cmd += self._sqs_server_opts

        retries_count = 1
        while retries_count:
            logging.debug("Running {}".format(' '.join(cmd))) 
            try:
                yatest_common.execute(cmd)
            except yatest_common.ExecutionError as ex:
                logging.debug("Modify permissions failed: {}. Retrying".format(ex))
                retries_count -= 1
                time.sleep(3)
            else:
                return
        raise RuntimeError("Failed to modify permissions")

    def _list_permissions(self, path, retries_count=1):
        cmd = [
            get_sqs_client_path(),
            'list-permissions',
            '--path', path,
        ] + self._sqs_server_opts

        while retries_count:
            logging.debug("Running {}".format(' '.join(cmd))) 
            try:
                execute = yatest_common.execute(cmd)
            except yatest_common.ExecutionError as ex:
                logging.debug("List permissions failed: {}. Retrying".format(ex))
                retries_count -= 1
                time.sleep(3)
            else:
                return execute.std_out
        raise RuntimeError("Failed to list permissions")

    def __generate_expected_simplified_permissions_response(self, sid=None, permission_names=[]):
        resp = 'AccountPermissions {'
        if not permission_names:
            resp += '}'
        else:
            resp += 'Permissions {{ Subject: \"{}\"'.format(sid)
            for permission_name in permission_names:
                resp += 'PermissionNames: \"{}\"'.format(permission_name)
            resp += '}} EffectivePermissions {{ Subject: \"{}\"'.format(sid)
            for permission_name in permission_names:
                resp += 'PermissionNames: \"{}\"'.format(permission_name)
            resp += '}}'

        return resp

    def __clean_and_compare(self, s1, s2):
        s1 = to_bytes(s1)[to_bytes(s1).find(to_bytes('AccountPermissions')):].replace(to_bytes('\n'), to_bytes('')).replace(to_bytes(' '), to_bytes(''))
        s2 = to_bytes(s2)[to_bytes(s2).find(to_bytes('AccountPermissions')):].replace(to_bytes('\n'), to_bytes('')).replace(to_bytes(' '), to_bytes(''))

        assert_that(s1, equal_to(s2))

    def test_modify_permissions(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        self._send_message_and_assert(queue_url, 'data')

        alkonavt_sid = 'alkonavt@builtin'
        berkanavt_sid = 'berkanavt@builtin'

        create_queue_permission = 'CreateQueue'
        send_message_permission = 'SendMessage'

        # no permissions expected
        description = self._list_permissions(self._username)
        expected_description = self.__generate_expected_simplified_permissions_response()
        self.__clean_and_compare(description, expected_description)

        # two permissions expected
        self._modify_permissions(
            self._username,
            'grant',
            alkonavt_sid + ':' + ','.join([create_queue_permission, send_message_permission])
        )
        description = self._list_permissions(self._username)
        expected_description = self.__generate_expected_simplified_permissions_response(alkonavt_sid, [create_queue_permission, send_message_permission])
        self.__clean_and_compare(description, expected_description)

        # single permission expected
        self._modify_permissions(self._username, 'revoke', alkonavt_sid + ':' + create_queue_permission)
        description = self._list_permissions(self._username)
        expected_description = self.__generate_expected_simplified_permissions_response(alkonavt_sid, [send_message_permission])
        self.__clean_and_compare(description, expected_description)

        receive_message_permission = 'ReceiveMessage'

        # other single permission expected
        self._modify_permissions(self._username, 'set', alkonavt_sid + ':' + receive_message_permission)
        description = self._list_permissions(self._username)
        expected_description = self.__generate_expected_simplified_permissions_response(alkonavt_sid, [receive_message_permission])
        self.__clean_and_compare(description, expected_description)

        # clear all permissions
        self._modify_permissions(self._username, 'set', berkanavt_sid + ':' + receive_message_permission)
        self._modify_permissions(self._username, 'revoke', alkonavt_sid + ':' + create_queue_permission, True)
        description = self._list_permissions(self._username)
        expected_description = self.__generate_expected_simplified_permissions_response()
        self.__clean_and_compare(description, expected_description)

    def test_apply_permissions(self):
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
                except:
                    if retries == 0:
                        raise
                    time.sleep(0.1)

        __send_message_with_retries(queue_url, 'megadata', is_(none()))  # no access

        self._modify_permissions(
            self._username,
            'grant',
            berkanavt_sid + ':' + ','.join(['ModifyPermissions'])
        )

        result = self._sqs_api.modify_permissions('Grant', berkanavt_sid, self._username, ['SendMessage', 'DescribePath'])
        assert_that(result, is_not(none()))

        __send_message_with_retries(queue_url, 'utradata', is_not(none()))  # has access

        result = self._sqs_api.modify_permissions('Revoke', berkanavt_sid, self._username, ['SendMessage', 'AlterQueue'])
        assert_that(result, is_not(none()))

        __send_message_with_retries(queue_url, 'superdata', is_(none()))  # no access again. that's a pity

        result = self._sqs_api.list_permissions(self._username)
        assert('Account' in str(result))
        assert(berkanavt_sid in str(result))
        assert('Permissions' in str(result))
 
 
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
    def test_invalid_token(self, token, pattern): 
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

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import abc
import logging
import time

from ydb.tests.library.common.wait_for import wait_for
from .kikimr_client import kikimr_client_factory
from ydb.tests.library.common.protobuf_console import (
    CreateTenantRequest, AlterTenantRequest, GetTenantStatusRequest,
    RemoveTenantRequest, GetOperationRequest)
import ydb.public.api.protos.ydb_cms_pb2 as cms_tenants_pb
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class KiKiMRClusterInterface(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.__client = None
        self.__clients = None
        self.__monitors = None
        self.__ready_timeout_seconds = 60

    @property
    def monitors(self):
        if self.__monitors is None:
            self.__monitors = [node.monitor for node in self.nodes.values()]
        return self.__monitors

    @abc.abstractproperty
    def nodes(self):
        """
        :return: dictionary for node_id -> KikimrNodeInterface
        """
        pass

    @abc.abstractproperty
    def slots(self):
        pass

    @abc.abstractmethod
    def start(self):
        pass

    @property
    def hostnames(self):
        return sorted(
            map(
                lambda node: node.host,
                self.nodes.values(),
            )
        )

    @abc.abstractmethod
    def stop(self):
        pass

    @abc.abstractproperty
    def config(self):
        pass

    @abc.abstractmethod
    def add_storage_pool(self, name=None, kind="rot", pdisk_user_kind=0, erasure=None):
        """
        Adds storage pool to the cluster
        :param erasure: Erasure for storage pool
        :return:
        """

    @property
    def client(self):
        # TODO(gvit): this a legacy method, please remove it
        if self.__client is None:
            self.__client = kikimr_client_factory(
                server=self.nodes[1].host,
                port=self.nodes[1].grpc_port,
                cluster=self,
                retry_count=10,
            )
        return self.__client

    def _send_get_tenant_status_request(self, database_name, token=None):
        req = GetTenantStatusRequest(database_name)

        if token is not None:
            req.set_user_token(token)

        return self.client.send_request(req.protobuf, method='ConsoleRequest').GetTenantStatusResponse

    def get_database_status(self, database_name, token=None):
        response = self._send_get_tenant_status_request(database_name, token=token)

        if response.Response.operation.status != StatusIds.SUCCESS:
            logger.critical("Console response status: %s", str(response.Response.operation.status))
            assert False
            return False

        result = cms_tenants_pb.GetDatabaseStatusResult()
        response.Response.operation.result.Unpack(result)
        return result

    def wait_tenant_up(self, database_name, token=None):
        self.__wait_tenant_up(
            database_name,
            expected_computational_units=1,
            token=token,
        )

    def __wait_tenant_up(
            self,
            database_name,
            expected_computational_units=None,
            timeout_seconds=120,
            token=None
    ):
        def predicate():
            result = self.get_database_status(database_name, token=token)

            if expected_computational_units is None:
                expected = set([2])
            else:
                expected = set([2]) if expected_computational_units > 0 else set([4])

            return result.state in expected

        tenant_running = wait_for(
            predicate=predicate,
            timeout_seconds=timeout_seconds,
            step_seconds=1
        )
        assert tenant_running

    def __get_console_op(self, op_id, token=None):
        req = GetOperationRequest(op_id)

        if token is not None:
            req.set_user_token(token)

        response = self.client.send_request(req.protobuf, method='ConsoleRequest')
        operation = response.GetOperationResponse.operation
        if not operation.ready and response.Status.Code != StatusIds.STATUS_CODE_UNSPECIFIED:
            raise RuntimeError('get_console_op failed: %s: %s' % (response.Status.Code, response.Status.Reason))
        return operation

    def __wait_console_op(self, op_id, timeout_seconds, step_seconds=0.5, token=None):
        deadline = time.time() + timeout_seconds
        while True:
            time.sleep(step_seconds)
            if time.time() >= deadline:
                raise RuntimeError('wait_console_op: deadline exceeded')
            operation = self.__get_console_op(op_id, token=token)
            if operation.ready:
                return operation

    def create_database(
            self,
            database_name,
            storage_pool_units_count,
            disable_external_subdomain=False,
            timeout_seconds=120,
            token=None,
    ):
        req = CreateTenantRequest(database_name)
        for storage_pool_type_name, units_count in storage_pool_units_count.items():
            req = req.add_storage_pool(
                storage_pool_type_name,
                units_count,
            )

        if disable_external_subdomain:
            req.disable_external_subdomain()

        if token is not None:
            req.set_user_token(token)

        response = self.client.send_request(req.protobuf, method='ConsoleRequest')
        operation = response.CreateTenantResponse.Response.operation
        if not operation.ready and response.Status.Code != StatusIds.STATUS_CODE_UNSPECIFIED:
            raise RuntimeError('create_database failed: %s: %s' % (response.Status.Code, response.Status.Reason))
        if not operation.ready:
            operation = self.__wait_console_op(operation.id, timeout_seconds=timeout_seconds, token=token)
        if operation.status != StatusIds.SUCCESS:
            raise RuntimeError('create_database failed: %s, %s' % (operation.status, ydb.issues._format_issues(operation.issues)))

        self.__wait_tenant_up(
            database_name,
            expected_computational_units=0,
            timeout_seconds=timeout_seconds,
            token=token,
        )
        return database_name

    def create_hostel_database(
            self,
            database_name,
            storage_pool_units_count,
            timeout_seconds=120
    ):
        req = CreateTenantRequest(database_name)
        for storage_pool_type_name, units_count in storage_pool_units_count.items():
            req = req.add_shared_storage_pool(
                storage_pool_type_name,
                units_count,
            )

        response = self.client.send_request(req.protobuf, method='ConsoleRequest')
        operation = response.CreateTenantResponse.Response.operation
        if not operation.ready and response.Status.Code != StatusIds.STATUS_CODE_UNSPECIFIED:
            raise RuntimeError('create_hostel_database failed: %s: %s' % (response.Status.Code, response.Status.Reason))
        if not operation.ready:
            operation = self.__wait_console_op(operation.id, timeout_seconds=timeout_seconds)
        if operation.status != StatusIds.SUCCESS:
            raise RuntimeError('create_hostel_database failed: %s' % (operation.status,))

        self.__wait_tenant_up(
            database_name,
            expected_computational_units=0,
            timeout_seconds=timeout_seconds
        )
        return database_name

    def create_serverless_database(
            self,
            database_name,
            hostel_db,
            timeout_seconds=120,
            schema_quotas=None,
            disk_quotas=None,
            attributes=None
    ):
        req = CreateTenantRequest(database_name)

        req.share_resources_with(hostel_db)

        if schema_quotas is not None:
            req.set_schema_quotas(schema_quotas)

        if disk_quotas is not None:
            req.set_disk_quotas(disk_quotas)

        if attributes is not None:
            for name, value in attributes.items():
                req.set_attribute(name, value)

        response = self.client.send_request(req.protobuf, method='ConsoleRequest')
        operation = response.CreateTenantResponse.Response.operation
        if not operation.ready and response.Status.Code != StatusIds.STATUS_CODE_UNSPECIFIED:
            raise RuntimeError('create_serverless_database failed: %s: %s' % (response.Status.Code, response.Status.Reason))
        if not operation.ready:
            operation = self.__wait_console_op(operation.id, timeout_seconds=timeout_seconds)
        if operation.status != StatusIds.SUCCESS:
            raise RuntimeError('create_serverless_database failed: %s' % (operation.status,))

        self.__wait_tenant_up(
            database_name,
            timeout_seconds=timeout_seconds
        )
        return database_name

    def alter_serverless_database(
            self,
            database_name,
            schema_quotas=None,
            disk_quotas=None,
            timeout_seconds=120,
    ):
        req = AlterTenantRequest(database_name)

        assert schema_quotas is not None or disk_quotas is not None

        if schema_quotas is not None:
            req.set_schema_quotas(schema_quotas)

        if disk_quotas is not None:
            req.set_disk_quotas(disk_quotas)

        response = self.client.send_request(req.protobuf, method='ConsoleRequest')
        operation = response.AlterTenantResponse.Response.operation
        if not operation.ready and response.Status.Code != StatusIds.STATUS_CODE_UNSPECIFIED:
            raise RuntimeError('alter_serverless_database failed: %s: %s' % (response.Status.Code, response.Status.Reason))
        if not operation.ready:
            operation = self.__wait_console_op(operation.id, timeout_seconds=timeout_seconds)
        if operation.status != StatusIds.SUCCESS:
            raise RuntimeError('alter_serverless_database failed: %s' % (operation.status,))

        self.__wait_tenant_up(
            database_name,
            timeout_seconds=timeout_seconds
        )
        return database_name

    def remove_database(
            self,
            database_name,
            timeout_seconds=20,
            token=None,
    ):
        logger.debug(database_name)

        operation_id = self._remove_database_send_op(database_name, token=token)
        self._remove_database_wait_op(database_name, operation_id, timeout_seconds=timeout_seconds, token=token)
        self._remove_database_wait_tenant_gone(database_name, timeout_seconds=timeout_seconds, token=token)

        return database_name

    def _remove_database_send_op(self, database_name, token=None):
        logger.debug('%s: send console operation, token %s', database_name, token)

        req = RemoveTenantRequest(database_name)

        if token is not None:
            req.set_user_token(token)

        response = self.client.send_request(req.protobuf, method='ConsoleRequest')
        operation = response.RemoveTenantResponse.Response.operation
        logger.debug('%s: response from console: %s', database_name, response)

        if not operation.ready and response.Status.Code != StatusIds.STATUS_CODE_UNSPECIFIED:
            raise RuntimeError('remove_database failed: %s: %s' % (response.Status.Code, response.Status.Reason))

        return operation.id

    def _remove_database_wait_op(self, database_name, operation_id, timeout_seconds=20, token=None):
        logger.debug('%s: wait console operation done', database_name)
        operation = self.__wait_console_op(operation_id, timeout_seconds=timeout_seconds, token=token)
        logger.debug('%s: console operation done', database_name)

        if operation.status not in (StatusIds.SUCCESS, StatusIds.NOT_FOUND):
            raise RuntimeError('remove_database failed: %s' % (operation.status,))

    def _remove_database_wait_tenant_gone(self, database_name, timeout_seconds=20, token=None):
        logger.debug('%s: wait tenant gone', database_name)

        def predicate():
            response = self._send_get_tenant_status_request(database_name, token=token)
            return response.Response.operation.status == StatusIds.NOT_FOUND

        tenant_not_found = wait_for(
            predicate=predicate,
            timeout_seconds=timeout_seconds,
            step_seconds=1
        )
        assert tenant_not_found

        logger.debug('%s: tenant gone', database_name)

        return database_name

    def __str__(self):
        return "Cluster type: {ntype}: \nCluster nodes: {nodes}".format(
            ntype=type(self), nodes=map(
                str, self.nodes.values()
            )
        )

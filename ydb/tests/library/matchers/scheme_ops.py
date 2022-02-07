#!/usr/bin/env python
# -*- coding: utf-8 -*-
import six
import abc
import logging

from hamcrest import assert_that, has_properties, equal_to, anything, any_of, instance_of, greater_than

from ydb.tests.library.common.msgbus_types import MessageBusStatus, SchemeStatus
from ydb.tests.library.common.path_types import PathType
from ydb.tests.library.common.protobuf import build_protobuf_if_necessary
from ydb.tests.library.common.protobuf_ss import SchemeOperationStatus, SchemeDescribeRequest
from ydb.tests.library.common.protobuf_ss import CreatePath, DropPathRequest
from ydb.tests.library.common.protobuf_ss import CreateTableRequest, DropTableRequest
from ydb.tests.library.common.protobuf_ss import RegisterTenant, DropTenantRequest, ForceDropTenantRequest
from ydb.tests.library.common.protobuf_ss import CreateTopicRequest, DropTopicRequest, AlterTopicRequest
from ydb.tests.library.matchers.response_matchers import DynamicFieldsProtobufMatcher

import ydb.core.protos.flat_scheme_op_pb2 as flat_scheme_op

logger = logging.getLogger(__name__)


class Assertions(object):
    def __init__(self, response):
        self.__actual_response = response
        self.status_is_ok()
        self.domain_is_present()

    def status_is_ok(self):
        assert_that(
            self.__actual_response,
            DynamicFieldsProtobufMatcher()
                .Status(MessageBusStatus.MSTATUS_OK)
                .SchemeStatus(SchemeStatus.StatusSuccess)
        )
        return self

    def domain_is_present(self):
        assert_that(
            self.__actual_response,
            DynamicFieldsProtobufMatcher()
                .PathDescription(
                    has_properties(
                        DomainDescription=has_properties(
                            ProcessingParams=anything(),
                            DomainKey=anything()
                        )
                    )
                )
        )
        return self

    def is_finished(self):
        assert_that(
            self.__actual_response,
            DynamicFieldsProtobufMatcher()
                .PathDescription(
                    has_properties(
                        Self=has_properties(
                            CreateFinished=equal_to(True)
                        )
                    )
                )
        )
        return self

    def check_path_type(self, ptype):
        assert_that(
            self.__actual_response,
            DynamicFieldsProtobufMatcher()
                .PathDescription(
                has_properties(
                    Self=has_properties(
                        PathType=ptype
                    )
                )
            )
        )
        return self

    def is_pers_queue(self):
        self.check_path_type(PathType.PersQueue)
        return self

    def is_dir(self):
        self.check_path_type(PathType.Dir)
        return self

    def is_tenant_root(self):
        self.check_path_type(PathType.SubDomain)
        return self

    def is_table(self):
        self.check_path_type(PathType.Table)
        return self

    def pers_queue_properties(self,
                              name=None, total_partition_count=None, partition_per_tablet=None,
                              balancer=None, alter_version=None):
        assert_that(
            self.__actual_response,
            DynamicFieldsProtobufMatcher()
                .PathDescription(
                has_properties(
                    Self=has_properties(
                        Name=name or anything()
                    ),
                    PersQueueGroup=has_properties(
                        Name=name or anything(),
                        TotalGroupCount=total_partition_count or greater_than(0),
                        PartitionPerTablet=partition_per_tablet or greater_than(0),
                        AlterVersion=alter_version or greater_than(0),
                        BalancerTabletID=balancer or greater_than(0),
                    )
                )
            )
        )


class _BaseSchemeOperation(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, client):
        assert hasattr(client, 'send_request')
        assert callable(getattr(client, 'send_request'))

        super(_BaseSchemeOperation, self).__init__()
        self._client = client

    def _send_request(self, protobuf, method='SchemeOperation'):
        return self._client.send_request(
            build_protobuf_if_necessary(protobuf),
            method
        )

    def _and_assert_scheme_status(self, response):
        assert_that(
            response,
            DynamicFieldsProtobufMatcher().Status(MessageBusStatus.MSTATUS_OK).SchemeStatus(SchemeStatus.StatusSuccess)
        )
        return response


class _WaitOperation(_BaseSchemeOperation):
    __metaclass__ = abc.ABCMeta

    def __init__(self, client):
        super(_WaitOperation, self).__init__(client)

    @staticmethod
    def __looks_like_scheme_response(response):
        assert_that(
            response,
            DynamicFieldsProtobufMatcher()
            .Status(any_of(*[x.value for x in MessageBusStatus]))
            .SchemeStatus(any_of(*[x.value for x in SchemeStatus]))
            .FlatTxId(
                has_properties(
                    TxId=instance_of(six.integer_types),
                    SchemeShardTabletId=instance_of(six.integer_types)
                )
            )
        )

    def wait(self, response, timeout_seconds=120):
        self.__looks_like_scheme_response(response)

        if response.Status != MessageBusStatus.MSTATUS_INPROGRESS:
            return response

        request = SchemeOperationStatus(
            response.FlatTxId.TxId,
            response.FlatTxId.SchemeShardTabletId,
            timeout_seconds
        )
        return self._send_request(
            request,
            method='SchemeOperationStatus'
        )

    def wait_and_assert(self, response, timeout_seconds=120):
        response = self.wait(response, timeout_seconds)
        return self._and_assert_scheme_status(response)


class _CreateOperation(_WaitOperation):
    __metaclass__ = abc.ABCMeta

    def __init__(self, client):
        super(_CreateOperation, self).__init__(client)
        self.__creation_options = None

    def create_and_wait(self, *args, **kwargs):
        if "timeout_seconds" in kwargs:
            timeout_seconds = kwargs.pop("timeout_seconds")
            response = self.create(*args, **kwargs)
            return self.wait(response, timeout_seconds)

        response = self.create(*args, **kwargs)
        return self.wait(response)

    def create_and_wait_and_assert(self, *args, **kwargs):
        response = self.create_and_wait(*args, **kwargs)
        return self._and_assert_scheme_status(response)

    @property
    def creation_options(self):
        return self.__creation_options

    @abc.abstractmethod
    def create(self, *args, **kwargs):
        pass


class _RemoveOperation(_WaitOperation):
    __metaclass__ = abc.ABCMeta

    def __init__(self, client):
        super(_RemoveOperation, self).__init__(client)
        self.__removing_options = None

    def remove_and_wait_and_assert(self, *args, **kwargs):
        response = self.remove_and_wait(*args, **kwargs)
        return self._and_assert_scheme_status(response)

    def remove_and_wait(self, *args, **kwargs):
        if "timeout_seconds" in kwargs:
            timeout_seconds = kwargs.pop("timeout_seconds")
            response = self.remove(*args, **kwargs)
            return self.wait(response, timeout_seconds)

        response = self.remove(*args, **kwargs)
        return self.wait(response)

    @property
    def removing_options(self):
        return self.__removing_options

    @abc.abstractmethod
    def remove(self, *args, **kwargs):
        pass


class _AlterOperation(_WaitOperation):
    __metaclass__ = abc.ABCMeta

    def __init__(self, client):
        super(_AlterOperation, self).__init__(client)
        self.__altering_options = None

    def alter_and_wait_and_assert(self, *args, **kwargs):
        response = self.alter_and_wait(*args, **kwargs)
        return self._and_assert_scheme_status(response)

    def alter_and_wait(self, *args, **kwargs):
        response = self.alter(*args, **kwargs)
        return self.wait(response)

    @property
    def altering_options(self):
        return self.__altering_options

    @abc.abstractmethod
    def alter(self, *args, **kwargs):
        pass


class _DescribeOperation(_BaseSchemeOperation):
    __metaclass__ = abc.ABCMeta

    def __init__(self, client):
        super(_DescribeOperation, self).__init__(client)
        self.__describe_opts = SchemeDescribeRequest.Options()

    @property
    def describe_options(self):
        return self.__describe_opts

    def describe(self, path):
        return self._send_request(
            SchemeDescribeRequest(path, self.describe_options),
            method='SchemeDescribe'
        )


class TenantOperations(_CreateOperation, _RemoveOperation, _DescribeOperation):
    def __init__(self, client):
        super(TenantOperations, self).__init__(client)
        self.__creation_options = RegisterTenant.Options()
        self.__removing_options = DropTenantRequest.Options()

    @property
    def creation_options(self):
        return self.__creation_options

    def create(self, path, options=None):
        options = options or self.creation_options
        return self._send_request(
            RegisterTenant(path, options=options)
        )

    @property
    def removing_options(self):
        return self.__removing_options

    def remove(self, path, options=None):
        options = options or self.removing_options
        # it's a trick
        # because we have the different transaction in SS for that case
        if options.drop_policy == flat_scheme_op.EDropAbortChanges:
            return self._send_request(
                ForceDropTenantRequest(path, options=options)
            )
        else:
            return self._send_request(
                DropTenantRequest(path, options=options)
            )


class PathOperations(_CreateOperation, _RemoveOperation, _DescribeOperation):
    def __init__(self, client):
        super(PathOperations, self).__init__(client)

    def create(self, path):
        return self._send_request(
            CreatePath(path)
        )

    def remove(self, path):
        return self._send_request(
            DropPathRequest(path)
        )


class TableOperations(_CreateOperation, _RemoveOperation, _DescribeOperation):
    def __init__(self, client):
        super(TableOperations, self).__init__(client)
        self.describe_options.return_partition_info = True
        self.describe_options.return_partition_config = True
        self.__creation_options = CreateTableRequest.Options()
        self.__removing_options = DropTableRequest.Options()

    @property
    def creation_options(self):
        return self.__creation_options

    def create(self, path, options=None):
        options = options or self.creation_options
        return self._send_request(
            CreateTableRequest(path, options=options)
        )
        pass

    @property
    def removing_options(self):
        return self.__removing_options

    def remove(self, path, options=None):
        options = options or self.removing_options
        return self._send_request(
            DropTableRequest(path, options=options)
        )


class PersQueueOperations(_CreateOperation, _RemoveOperation, _AlterOperation, _DescribeOperation):
    def __init__(self, client):
        super(PersQueueOperations, self).__init__(client)
        self.__creation_options = CreateTopicRequest.Options()
        self.__removing_options = DropTopicRequest.Options()
        self.__altering_options = AlterTopicRequest.Options()

    @property
    def creation_options(self):
        return self.__creation_options

    def create(self, path, options=None):
        options = options or self.creation_options
        return self._send_request(
            CreateTopicRequest(path, options=options)
        )

    @property
    def removing_options(self):
        return self.__removing_options

    def remove(self, path, options=None):
        options = options or self.removing_options
        return self._send_request(
            DropTopicRequest(path, options=options)
        )

    @property
    def altering_options(self):
        return self.__altering_options

    def alter(self, path, options=None):
        options = options or self.__altering_options
        return self._send_request(
            AlterTopicRequest(path, options=options)
        )

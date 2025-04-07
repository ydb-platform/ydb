#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os

from ydb.tests.library.common.msgbus_types import MessageBusStatus
from ydb.tests.library.common.protobuf_ss import TSchemeOperationStatus

import grpc
import six
import functools

from google.protobuf.text_format import Parse
from ydb.core.protos import blobstorage_config_pb2
import ydb.core.protos.msgbus_pb2 as msgbus
import ydb.core.protos.grpc_pb2_grpc as grpc_server
from ydb.core.protos.schemeshard import operations_pb2 as schemeshard_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.grpc.draft import ydb_tablet_v1_pb2_grpc as grpc_tablet_service
from ydb.public.api.protos.draft.ydb_tablet_pb2 import RestartTabletRequest
from collections import namedtuple


class FlatTxId(namedtuple('FlatTxId', ['tx_id', 'schemeshard_tablet_id'])):
    @staticmethod
    def from_response(response):
        return FlatTxId(
            tx_id=response.FlatTxId.TxId,
            schemeshard_tablet_id=response.FlatTxId.SchemeShardTabletId
        )


def kikimr_client_factory(server, port, cluster=None, retry_count=1):
    return KiKiMRMessageBusClient(
        server, port, cluster=cluster,
        retry_count=retry_count
    )


def channels_list():
    return os.getenv('CHANNELS_LIST', '')


def to_bytes(v):
    if v is None:
        return None

    if not isinstance(v, six.binary_type):
        try:
            return bytes(v, 'utf-8')
        except Exception as e:
            raise ValueError(str(e), type(v))
    return v


class StubWithRetries(object):
    __slots__ = ('_stub', '_retry_count', '_retry_min_sleep', '_retry_max_sleep', '__dict__')

    def __init__(self, stub, retry_count=4, retry_min_sleep=0.1, retry_max_sleep=5):
        self._stub = stub
        self._retry_count = retry_count
        self._retry_min_sleep = retry_min_sleep
        self._retry_max_sleep = retry_max_sleep

    def __getattr__(self, method):
        target = getattr(self._stub, method)

        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            retries = self._retry_count
            next_sleep = self._retry_min_sleep
            while True:
                try:
                    return target(*args, **kwargs)
                except (RuntimeError, grpc.RpcError):
                    retries -= 1
                    if retries <= 0:
                        raise
                    time.sleep(next_sleep)
                    next_sleep = min(next_sleep * 2, self._retry_max_sleep)

        setattr(self, method, wrapper)
        return wrapper


class KiKiMRMessageBusClient(object):
    def __init__(self, server, port, cluster=None, retry_count=1):
        self.server = server
        self.port = port
        self._cluster = cluster
        self.__domain_id = 1
        self.__retry_count = retry_count
        self.__retry_sleep_seconds = 10
        self._options = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6)
        ]
        self._channel = grpc.insecure_channel("%s:%s" % (self.server, self.port), options=self._options)
        self._stub = grpc_server.TGRpcServerStub(self._channel)
        self.tablet_service = StubWithRetries(grpc_tablet_service.TabletServiceStub(self._channel))

    def describe(self, path, token):
        request = msgbus.TSchemeDescribe()
        request.Path = path
        request.SecurityToken = token
        request.Options.ReturnPartitioningInfo = False
        return self.send(request, 'SchemeDescribe')

    def _get_invoke_callee(self, method):
        return getattr(self._stub, method)

    def invoke(self, request, method):
        retry = self.__retry_count
        while True:
            try:
                callee = self._get_invoke_callee(method)
                return callee(request)
            except (RuntimeError, grpc.RpcError):
                retry -= 1

                if not retry:
                    raise

                time.sleep(self.__retry_sleep_seconds)

    def close(self):
        self._channel.close()

    def update_self_heal(self, enable, domain=1):
        request = msgbus.TBlobStorageConfigRequest()
        request.Domain = domain
        command = request.Request.Command.add()
        command.EnableSelfHeal.Enable = enable
        return self.send(request, 'BlobStorageConfig')

    def read_drive_status(self, hostname, interconnect_port, drive_path=None, domain=1):
        request = msgbus.TBlobStorageConfigRequest()
        request.Domain = domain
        command = request.Request.Command.add()
        host_key = blobstorage_config_pb2.THostKey(Fqdn=hostname, IcPort=interconnect_port)
        command.ReadDriveStatus.HostKey.MergeFrom(host_key)
        if drive_path is not None:
            command.ReadDriveStatus.Path = drive_path
        response = self.send(request, 'BlobStorageConfig')
        return response

    def update_drive_status(self, hostname, interconnect_port, drive_path, status, domain=1):
        request = msgbus.TBlobStorageConfigRequest()
        command = request.Request.Command.add()
        request.Domain = domain
        host_key = blobstorage_config_pb2.THostKey(Fqdn=hostname, IcPort=interconnect_port)
        command.UpdateDriveStatus.Path = drive_path
        command.UpdateDriveStatus.HostKey.MergeFrom(host_key)
        command.UpdateDriveStatus.Status = status
        return self.send(request, 'BlobStorageConfig')

    def send_request(self, protobuf_request, method=None):
        return self.send(protobuf_request, method)

    def send_and_poll_request(self, protobuf_request, method='SchemeOperation'):
        response = self.send_request(protobuf_request, method)
        return self.__poll(response)

    def __poll(self, flat_transaction_response):
        if not MessageBusStatus.is_ok_status(flat_transaction_response.Status):
            return flat_transaction_response

        return self.send_request(
            TSchemeOperationStatus(
                flat_transaction_response.FlatTxId.TxId,
                flat_transaction_response.FlatTxId.SchemeShardTabletId
            ).protobuf,
            'SchemeOperationStatus'
        )

    def bind_storage_pools(self, domain_name, spools):
        request = msgbus.TSchemeOperation()
        scheme_transaction = request.Transaction
        scheme_operation = scheme_transaction.ModifyScheme
        scheme_operation.WorkingDir = '/'
        scheme_operation.OperationType = schemeshard_pb2.ESchemeOpAlterSubDomain
        domain_description = scheme_operation.SubDomain
        domain_description.Name = domain_name
        for name, kind in spools.items():
            domain_description.StoragePools.add(Name=name, Kind=kind)
        return self.ddl_exec_status(
            FlatTxId.from_response(
                self.invoke(
                    request, 'SchemeOperation'
                )
            )
        )

    def flat_transaction_status(self, tx_id, tablet_id, timeout=120):
        request = msgbus.TSchemeOperationStatus()
        request.FlatTxId.TxId = tx_id
        request.FlatTxId.SchemeShardTabletId = tablet_id
        request.PollOptions.Timeout = timeout * 1000
        return self.invoke(request, 'SchemeOperationStatus')

    def send(self, request, method):
        return self.invoke(request, method)

    def ddl_exec_status(self, flat_tx_id):
        return self.flat_transaction_status(flat_tx_id.tx_id, flat_tx_id.schemeshard_tablet_id)

    def add_attr(self, working_dir, name, attributes, token=None):
        request = msgbus.TSchemeOperation()
        request.Transaction.ModifyScheme.OperationType = schemeshard_pb2.ESchemeOpAlterUserAttributes
        request.Transaction.ModifyScheme.WorkingDir = working_dir
        request.Transaction.ModifyScheme.AlterUserAttributes.PathName = name

        if attributes is not None:
            for name, value in attributes.items():
                request.Transaction.ModifyScheme.AlterUserAttributes.UserAttributes.add(Key=name, Value=value)

        if token:
            request.SecurityToken = token
        return self.send_and_poll_request(request, 'SchemeOperation')

    def hive_create_tablets(self, list_of_tablets):
        request = msgbus.THiveCreateTablet()
        for tablet in list_of_tablets:
            create_tablet_cmd = request.CmdCreateTablet.add()
            create_tablet_cmd.OwnerId = tablet.owner_id
            create_tablet_cmd.OwnerIdx = tablet.owner_idx
            create_tablet_cmd.TabletType = int(tablet.type)
            create_tablet_cmd.ChannelsProfile = tablet.channels_profile
            channels = None
            if tablet.binded_channels:
                channels = tablet.binded_channels
            elif self._cluster is not None and hasattr(self._cluster, 'default_channel_bindings'):
                channels = self._cluster.default_channel_bindings
            elif channels_list() != '':
                channels = {idx: value for idx, value in enumerate(channels_list().split(";"))}
            if channels:
                for channel, storage_pool in channels.items():
                    create_tablet_cmd.BindedChannels.add(
                        StoragePoolName=storage_pool)

            if tablet.allowed_node_ids:
                create_tablet_cmd.AllowedNodeIDs.extend(tablet.allowed_node_ids)
        request.DomainUid = self.__domain_id
        return self.invoke(
            request,
            'HiveCreateTablet'
        )

    def console_request(self, request_text, raise_on_error=True):
        request = msgbus.TConsoleRequest()
        Parse(request_text, request)
        response = self.invoke(request, 'ConsoleRequest')
        if raise_on_error and response.Status.Code != StatusIds.SUCCESS:
            raise RuntimeError('console_request failed: %s: %s' % (response.Status.Code, response.Status.Reason))
        return response

    def add_config_item(self, config, cookie=None, raise_on_error=True, token=None):
        request = msgbus.TConsoleRequest()
        if token is not None:
            request.SecurityToken = token
        action = request.ConfigureRequest.Actions.add()
        item = action.AddConfigItem.ConfigItem
        if isinstance(config, str) or isinstance(config, bytes):
            Parse(config, item.Config)
        else:
            item.Config.MergeFrom(config)
        response = self.invoke(request, 'ConsoleRequest')
        if raise_on_error and response.Status.Code != StatusIds.SUCCESS:
            raise RuntimeError('add_config_item failed: %s: %s' % (response.Status.Code, response.Status.Reason))
        return response

    def tablet_kill(self, tablet_id, assert_success=False):
        request = RestartTabletRequest(tablet_id=tablet_id)
        response = self.tablet_service.RestartTablet(request)
        if assert_success:
            assert response.status == StatusIds.SUCCESS
            if response.status != StatusIds.SUCCESS:
                raise RuntimeError('ERROR: {status} {issues}'.format(status=response.status, issues=response.issues))
        return response

    def tablet_state(self, tablet_type=None, tablet_ids=()):
        request = msgbus.TTabletStateRequest()
        if tablet_type is not None:
            request.TabletType = int(tablet_type)
        if tablet_ids:
            request.TabletIDs.extend(tablet_ids)
        request.Alive = True
        return self.invoke(request, 'TabletStateRequest')

    def __del__(self):
        self.close()

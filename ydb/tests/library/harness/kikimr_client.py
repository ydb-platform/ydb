#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os

from ydb.tests.library.common.msgbus_types import MessageBusStatus
from ydb.tests.library.common.protobuf_ss import TSchemeOperationStatus

import grpc
import six

from google.protobuf.text_format import Parse
from ydb.core.protos import blobstorage_config_pb2
import ydb.core.protos.msgbus_pb2 as msgbus
import ydb.core.protos.msgbus_kv_pb2 as msgbus_kv
import ydb.core.protos.flat_scheme_op_pb2 as flat_scheme_op_pb2
import ydb.core.protos.grpc_pb2_grpc as grpc_server
from ydb.core.protos import flat_scheme_op_pb2 as flat_scheme_op
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
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
        return self.__poll(response, protobuf_request.SecurityToken)

    def __poll(self, flat_transaction_response, token):
        if not MessageBusStatus.is_ok_status(flat_transaction_response.Status):
            return flat_transaction_response

        request = TSchemeOperationStatus(
            flat_transaction_response.FlatTxId.TxId,
            flat_transaction_response.FlatTxId.SchemeShardTabletId
        ).protobuf

        if token:
            request.SecurityToken = token

        return self.send_request(request, 'SchemeOperationStatus')

    def bind_storage_pools(self, domain_name, spools, token=None):
        request = msgbus.TSchemeOperation()
        if token:
            request.SecurityToken = token
        scheme_transaction = request.Transaction
        scheme_operation = scheme_transaction.ModifyScheme
        scheme_operation.WorkingDir = '/'
        scheme_operation.OperationType = flat_scheme_op.ESchemeOpAlterSubDomain
        domain_description = scheme_operation.SubDomain
        domain_description.Name = domain_name
        for name, kind in spools.items():
            domain_description.StoragePools.add(Name=name, Kind=kind)
        return self.ddl_exec_status(
            FlatTxId.from_response(
                self.invoke(
                    request, 'SchemeOperation'
                )
            ),
            token
        )

    def flat_transaction_status(self, tx_id, tablet_id, timeout=120, token=None):
        request = msgbus.TSchemeOperationStatus()
        request.FlatTxId.TxId = tx_id
        request.FlatTxId.SchemeShardTabletId = tablet_id
        request.PollOptions.Timeout = timeout * 1000
        if token:
            request.SecurityToken = token
        return self.invoke(request, 'SchemeOperationStatus')

    def send(self, request, method):
        return self.invoke(request, method)

    def ddl_exec_status(self, flat_tx_id, token=None):
        return self.flat_transaction_status(flat_tx_id.tx_id, flat_tx_id.schemeshard_tablet_id, token=token)

    def add_attr(self, working_dir, name, attributes, token=None):
        request = msgbus.TSchemeOperation()
        request.Transaction.ModifyScheme.OperationType = flat_scheme_op_pb2.ESchemeOpAlterUserAttributes
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

    def local_enumerate_tablets(self, tablet_type, node_id=1):
        request = msgbus.TLocalEnumerateTablets()
        request.DomainUid = self.__domain_id
        request.NodeId = node_id
        request.TabletType = int(tablet_type)

        return self.invoke(request, 'LocalEnumerateTablets')

    def kv_cmd_write(self, tablet_id, cmd_writes, generation=None):
        request = msgbus_kv.TKeyValueRequest()
        for cmd in cmd_writes:
            write_cmd = request.CmdWrite.add()
            write_cmd.Key = to_bytes(cmd.key)
            write_cmd.Value = to_bytes(cmd.value)
        request.TabletId = tablet_id
        if generation is not None:
            request.Generation = generation

        response = self.invoke(request, 'KeyValue')
        return response

    def kv_cmd_read(self, tablet_id, cmd_reads, generation=None):

        request = msgbus_kv.TKeyValueRequest()
        for cmd in cmd_reads:
            read_cmd = request.CmdRead.add()
            read_cmd.Key = to_bytes(cmd.key)
            if cmd.offset is not None:
                read_cmd.Offset = cmd.offset
            if cmd.size is not None:
                read_cmd.Size = cmd.size
        request.TabletId = tablet_id
        if generation is not None:
            request.Generation = generation

        response = self.invoke(request, 'KeyValue')
        return response

    def increment_generation(self, tablet_id):
        request = msgbus_kv.TKeyValueRequest()
        request.CmdIncrementGeneration.CopyFrom(request.TCmdIncrementGeneration())
        request.TabletId = tablet_id

        return self.invoke(request, 'KeyValue')

    def kv_cmd_read_range(self, tablet_id, cmd_range_reads, generation=None):

        request = msgbus_kv.TKeyValueRequest()
        for cmd in cmd_range_reads:
            read_range = request.CmdReadRange.add()
            read_range.IncludeData = cmd.include_data
            if cmd.limit_bytes:
                read_range.LimitBytes = cmd.limit_bytes
            read_range.Range.From = to_bytes(cmd.from_key)
            read_range.Range.IncludeFrom = cmd.include_from
            read_range.Range.To = to_bytes(cmd.to_key)
            read_range.Range.IncludeTo = cmd.include_to
        request.TabletId = tablet_id
        if generation is not None:
            request.Generation = generation

        return self.invoke(request, 'KeyValue')

    def kv_cmd_rename(self, tablet_id, cmd_renames, generation=None):

        request = msgbus_kv.TKeyValueRequest()
        for cmd in cmd_renames:
            rename_cmd = request.CmdRename.add()
            rename_cmd.OldKey = to_bytes(cmd.old_key)
            rename_cmd.NewKey = to_bytes(cmd.new_key)
        request.TabletId = tablet_id
        if generation is not None:
            request.Generation = generation

        return self.invoke(request, 'KeyValue')

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

    def kv_cmd_delete_range(self, tablet_id, cmd_range_delete, generation=None):

        request = msgbus_kv.TKeyValueRequest()
        for cmd in cmd_range_delete:
            delete_range = request.CmdDeleteRange.add()
            delete_range.Range.From = to_bytes(cmd.from_key)
            delete_range.Range.IncludeFrom = cmd.include_from
            delete_range.Range.To = to_bytes(cmd.to_key)
            delete_range.Range.IncludeTo = cmd.include_to
        request.TabletId = tablet_id
        if generation is not None:
            request.Generation = generation

        return self.invoke(request, 'KeyValue')

    def kv_copy_range(self, tablet_id, key_range, prefix_to_add, prefix_to_remove=None):
        request = msgbus_kv.TKeyValueRequest()
        request.TabletId = tablet_id

        clone_range = request.CmdCopyRange.add()
        clone_range.Range.From = to_bytes(key_range.from_key)
        clone_range.Range.IncludeFrom = key_range.include_from
        clone_range.Range.To = to_bytes(key_range.to_key)
        clone_range.Range.IncludeTo = key_range.include_to
        clone_range.PrefixToAdd = to_bytes(prefix_to_add)
        if prefix_to_remove is not None:
            clone_range.PrefixToRemove = to_bytes(prefix_to_remove)

        return self.invoke(request, 'KeyValue')

    def kv_request(self, tablet_id, kv_request, generation=None, deadline_ms=None):
        request = kv_request.protobuf
        request.TabletId = tablet_id
        if generation is not None:
            request.Generation = generation
        if deadline_ms is not None:
            request.DeadlineInstantMs = deadline_ms
        return self.invoke(request, 'KeyValue')

    def tablet_kill(self, tablet_id):
        request = msgbus.TTabletKillRequest(TabletID=tablet_id)
        return self.invoke(request, 'TabletKillRequest')

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

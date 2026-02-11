#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os

import grpc
import six
import logging

from ydb.public.api.grpc import ydb_keyvalue_v1_pb2_grpc as grpc_server
from ydb.public.api.grpc import ydb_keyvalue_v2_pb2_grpc as grpc_server_v2
from ydb.public.api.protos import ydb_keyvalue_pb2 as keyvalue_api

logger = logging.getLogger()


def keyvalue_client_factory(server, port, cluster=None, retry_count=1):
    return KeyValueClient(
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


class KeyValueClient(object):
    def __init__(self, server, port, cluster=None, retry_count=1, sleep_retry_seconds=10):
        self.server = server
        self.port = port
        self._cluster = cluster
        self.__domain_id = 1
        self.__retry_count = retry_count
        self.__retry_sleep_seconds = sleep_retry_seconds
        self._options = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6)
        ]
        self._channel = grpc.insecure_channel("%s:%s" % (self.server, self.port), options=self._options)
        self._stub = grpc_server.KeyValueServiceStub(self._channel)
        self._stub_v2 = grpc_server_v2.KeyValueServiceStub(self._channel)

    def _get_invoke_callee(self, method, version):
        if version == 'v2':
            return getattr(self._stub_v2, method)
        return getattr(self._stub, method)

    def invoke(self, request, method, version):
        retry = self.__retry_count
        while True:
            try:
                callee = self._get_invoke_callee(method, version)
                result = callee(request)
                return result
            except (RuntimeError, grpc.RpcError):
                retry -= 1

                if not retry:
                    raise

                time.sleep(self.__retry_sleep_seconds)

    def make_write_request(self, path, partition_id, kv_pairs, channel=None):
        request = keyvalue_api.ExecuteTransactionRequest()
        request.path = path
        request.partition_id = partition_id
        for key, value in kv_pairs:
            write = request.commands.add().write
            write.key = key
            write.value = to_bytes(value)
            if channel is not None:
                write.storage_channel = channel
        return request

    def kv_write(self, path, partition_id, key, value, channel=None, version='v1'):
        request = self.make_write_request(path, partition_id, [(key, value)], channel)
        return self.invoke(request, 'ExecuteTransaction', version)

    def kv_writes(self, path, partition_id, kv_pairs, channel=None, version='v1'):
        request = self.make_write_request(path, partition_id, kv_pairs, channel)
        return self.invoke(request, 'ExecuteTransaction', version)

    def make_delete_range_request(self, path, partition_id, from_key=None, to_key=None,
                                  from_inclusive=True, to_inclusive=False):
        request = keyvalue_api.ExecuteTransactionRequest()
        request.path = path
        request.partition_id = partition_id
        delete_range = request.commands.add().delete_range
        if from_key is not None:
            if from_inclusive:
                delete_range.range.from_key_inclusive = from_key
            else:
                delete_range.range.from_key_exclusive = from_key
        if to_key is not None:
            if to_inclusive:
                delete_range.range.to_key_inclusive = to_key
            else:
                delete_range.range.to_key_exclusive = to_key
        return request

    def kv_delete_range(self, path, partition_id, from_key=None, to_key=None,
                        from_inclusive=True, to_inclusive=False, version='v1'):
        request = self.make_delete_range_request(path, partition_id, from_key, to_key,
                                                 from_inclusive, to_inclusive)
        return self.invoke(request, 'ExecuteTransaction', version)

    def make_read_request(self, path, partition_id, key, offset=None, size=None):
        request = keyvalue_api.ReadRequest()
        request.path = path
        request.partition_id = partition_id
        request.key = key
        if offset is not None:
            request.offset = offset
        if size is not None:
            request.size = size
        return request

    def kv_read(self, path, partition_id, key, offset=None, size=None, version='v1'):
        request = self.make_read_request(path, partition_id, key, offset, size)
        return self.invoke(request, 'Read', version)

    def kv_get_tablets_read_state(self, path, partition_ids):
        for id in partition_ids:
            response = self.kv_read(path, id, "key")
            return response

    def kv_get_tablets_write_state(self, path, partition_ids):
        states = list()
        for id in partition_ids:
            response = self.kv_write(path, id, "key", "value")
            states.append(response)
        return states

    def create_tablets(self, number_of_tablets, path, binded_channels=None):
        request = keyvalue_api.CreateVolumeRequest()
        request.path = path
        request.partition_count = number_of_tablets
        channels = None
        if binded_channels:
            channels = binded_channels
        elif self._cluster is not None and hasattr(self._cluster, 'default_channel_bindings'):
            channels = self._cluster.default_channel_bindings
        elif channels_list() != '':
            channels = {idx: value for idx, value in enumerate(channels_list().split(';'))}
        if channels:
            if isinstance(channels, dict):
                for _ in range(len(channels.items())):
                    new_channel = request.storage_config.channel.add()
                    new_channel.media = 'hdd'
            elif isinstance(channels, list):
                for media in channels:
                    new_channel = request.storage_config.channel.add()
                    new_channel.media = media
        return self.invoke(request, 'CreateVolume', version='v1')

    def drop_tablets(self, path):
        request = keyvalue_api.DropVolumeRequest()
        request.path = path
        return self.invoke(request, 'DropVolume', version='v1')

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()

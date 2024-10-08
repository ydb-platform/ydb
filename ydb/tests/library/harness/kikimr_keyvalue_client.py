#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os


import grpc
import six
import logging

from ydb.public.api.grpc import ydb_keyvalue_v1_pb2_grpc as grpc_server
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
        self._stub = grpc_server.KeyValueServiceStub(self._channel)

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

    def kv_write(self, path, partition_id, key, value):
        request = keyvalue_api.ExecuteTransactionRequest()
        request.path = path
        request.partition_id = partition_id
        write = request.commands.add().write
        write.key = key
        write.value = to_bytes(value)
        return self.invoke(request, 'ExecuteTransaction')

    def kv_read(self, path, partition_id, key):
        request = keyvalue_api.ReadRequest()
        request.path = path
        request.partition_id = partition_id
        request.key = key
        return self.invoke(request, 'Read')

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
            for _ in range(len(channels.items())):
                new_channel = request.storage_config.channel.add()
                new_channel.media = 'hdd'
        return self.invoke(request, 'CreateVolume')

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()

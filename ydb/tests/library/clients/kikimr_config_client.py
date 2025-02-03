#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os


import grpc
import logging

from ydb.public.api.grpc import ydb_bsconfig_v1_pb2_grpc as grpc_server
from ydb.public.api.protos import ydb_bsconfig_pb2 as bsconfig_api

logger = logging.getLogger()


def bsconfig_client_factory(server, port, cluster=None, retry_count=1):
    return BSConfigClient(
        server, port, cluster=cluster,
        retry_count=retry_count
    )


def channels_list():
    return os.getenv('CHANNELS_LIST', '')


class BSConfigClient(object):
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
        self._stub = grpc_server.BSConfigServiceStub(self._channel)

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

    def replace_storage_config(self, yaml_config, storage_yaml_config=None):
        request = bsconfig_api.ReplaceStorageConfigRequest()
        request.yaml_config = yaml_config
        if storage_yaml_config is not None:
            request.storage_yaml_config = storage_yaml_config
        return self.invoke(request, 'ReplaceStorageConfig')

    def fetch_storage_config(self, dedicated_storage_section=False, dedicated_cluster_section=False):
        request = bsconfig_api.FetchStorageConfigRequest()
        request.dedicated_storage_section = dedicated_storage_section
        request.dedicated_cluster_section = dedicated_cluster_section
        return self.invoke(request, 'FetchStorageConfig')

    def bootstrap_cluster(self, self_assembly_uuid):
        request = bsconfig_api.BootstrapClusterRequest()
        request.self_assembly_uuid = self_assembly_uuid
        return self.invoke(request, 'BootstrapCluster')

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()

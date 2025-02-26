#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os
from enum import Enum


import grpc
import logging

from ydb.public.api.grpc import ydb_config_v1_pb2_grpc as grpc_server
from ydb.public.api.protos import ydb_config_pb2 as config_api

logger = logging.getLogger()


def config_client_factory(server, port, cluster=None, retry_count=1):
    return ConfigClient(
        server, port, cluster=cluster,
        retry_count=retry_count
    )


def channels_list():
    return os.getenv('CHANNELS_LIST', '')


class ConfigClient(object):
    class FetchTransform(Enum):
        NONE = 1
        DETACH_STORAGE_CONFIG_SECTION = 2
        ATTACH_STORAGE_CONFIG_SECTION = 3

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
        self._stub = grpc_server.ConfigServiceStub(self._channel)

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

    def replace_config(self, main_config):
        request = config_api.ReplaceConfigRequest()
        request.replace = main_config
        return self.invoke(request, 'ReplaceConfig')

    def fetch_all_configs(self, transform=None):
        request = config_api.FetchConfigRequest()
        settings = config_api.FetchConfigRequest.FetchModeAll()

        if transform == ConfigClient.FetchTransform.DETACH_STORAGE_CONFIG_SECTION:
            settings.set_detach_storage_config_section()
        elif transform == ConfigClient.FetchTransform.ATTACH_STORAGE_CONFIG_SECTION:
            settings.set_attach_storage_config_section()

        request.all.CopyFrom(settings)

        return self.invoke(request, 'FetchConfig')

    def bootstrap_cluster(self, self_assembly_uuid):
        request = config_api.BootstrapClusterRequest()
        request.self_assembly_uuid = self_assembly_uuid
        return self.invoke(request, 'BootstrapCluster')

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()

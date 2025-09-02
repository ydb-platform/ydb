#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os


import grpc
import logging

from ydb.public.api.grpc.draft import ydb_dynamic_config_v1_pb2_grpc as grpc_server
from ydb.public.api.protos.draft import ydb_dynamic_config_pb2 as dynamic_config_api


logger = logging.getLogger()


def dynconfig_client_factory(server, port, cluster=None, retry_count=1):
    return DynConfigClient(
        server, port, cluster=cluster,
        retry_count=retry_count
    )


def channels_list():
    return os.getenv('CHANNELS_LIST', '')


class DynConfigClient(object):
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
        self._stub = grpc_server.DynamicConfigServiceStub(self._channel)

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

    def fetch_startup_config(self):
        request = dynamic_config_api.FetchStartupConfigRequest()
        return self.invoke(request, 'FetchStartupConfig')

    def fetch_config(self):
        request = dynamic_config_api.GetConfigRequest()
        return self.invoke(request, 'GetConfig')

    def get_configuration_version(self, list_nodes=False):
        request = dynamic_config_api.GetConfigurationVersionRequest(
            list_nodes=list_nodes
        )
        return self.invoke(request, 'GetConfigurationVersion')

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()

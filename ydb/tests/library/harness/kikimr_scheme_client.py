#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os


import grpc

from ydb.public.api.grpc import ydb_scheme_v1_pb2_grpc as grpc_server
from ydb.public.api.protos import ydb_scheme_pb2 as scheme_api


def scheme_client_factory(server, port, cluster=None, retry_count=1):
    return SchemeClient(
        server, port, cluster=cluster,
        retry_count=retry_count
    )


def channels_list():
    return os.getenv('CHANNELS_LIST', '')


class SchemeClient(object):
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
        self._stub = grpc_server.SchemeServiceStub(self._channel)

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

    def make_directory(self, path):
        request = scheme_api.MakeDirectoryRequest()
        request.path = path
        return self.invoke(request, 'MakeDirectory')

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import logging

import grpc

from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc as grpc_server
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge_api

logger = logging.getLogger()


def bridge_client_factory(server, port, cluster=None, retry_count=1):
    return BridgeClient(
        server, port, cluster=cluster,
        retry_count=retry_count
    )


class BridgeClient(object):
    def __init__(self, server, port, cluster=None, retry_count=1):
        self.server = server
        self.port = port
        self._cluster = cluster
        self.__retry_count = retry_count
        self.__retry_sleep_seconds = 10
        self._options = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6)
        ]
        self._channel = grpc.insecure_channel("%s:%s" % (self.server, self.port), options=self._options)
        self._stub = grpc_server.BridgeServiceStub(self._channel)
        self._auth_token = None

    def set_auth_token(self, token):
        self._auth_token = token

    def _get_invoke_callee(self, method):
        return getattr(self._stub, method)

    def invoke(self, request, method):
        retry = self.__retry_count
        while True:
            try:
                callee = self._get_invoke_callee(method)
                metadata = []
                if self._auth_token:
                    metadata.append(('x-ydb-auth-ticket', self._auth_token))
                return callee(request, metadata=metadata)
            except (RuntimeError, grpc.RpcError):
                retry -= 1

                if not retry:
                    raise

                time.sleep(self.__retry_sleep_seconds)

    def get_cluster_state(self):
        request = bridge_api.GetClusterStateRequest()
        return self.invoke(request, 'GetClusterState')

    def update_cluster_state(self, updates):
        request = bridge_api.UpdateClusterStateRequest()
        request.updates.extend(updates)
        return self.invoke(request, 'UpdateClusterState')

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()

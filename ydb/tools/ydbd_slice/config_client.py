#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import grpc
import logging
from typing import Optional, Any

from ydb.public.api.grpc import ydb_config_v1_pb2_grpc as grpc_server
from ydb.public.api.protos import ydb_config_pb2 as config_api
from ydb.public.api.protos import ydb_status_codes_pb2 as ydb_status_codes

logger = logging.getLogger(__name__)


class ConfigClientError(Exception):
    pass


class ConfigClient:
    """
    gRPC client for YDB config service.
    """

    def __init__(
        self,
        server: str,
        port: int,
        cluster: Optional[str] = None,
        retry_count: int = 1
    ):
        self.server: str = server
        self.port: int = port
        self._cluster: Optional[str] = cluster
        self._retry_count: int = retry_count
        self._retry_sleep_seconds: int = 10
        self._options: list[tuple[str, int]] = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6)
        ]
        self._channel: grpc.Channel = grpc.insecure_channel(f"{self.server}:{self.port}", options=self._options)
        self._stub: grpc_server.ConfigServiceStub = grpc_server.ConfigServiceStub(self._channel)
        self._auth_token: Optional[str] = None

    @property
    def auth_token(self) -> Optional[str]:
        return self._auth_token

    @auth_token.setter
    def auth_token(self, token: Optional[str]) -> None:
        self._auth_token = token

    def _get_invoke_callee(self, method: str) -> Any:
        return getattr(self._stub, method)

    def invoke(self, request: Any, method: str) -> Any:
        """
        Call gRPC method with retries.
        """
        retry = self._retry_count
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
                time.sleep(self._retry_sleep_seconds)

    def _check_success(self, response: Any, action: str = "") -> Any:
        if getattr(response.operation, "status", None) != ydb_status_codes.StatusIds.SUCCESS:
            raise ConfigClientError(f"ConfigClient: {action} failed: {response}")
        return response

    def replace_config(self, main_config: Any) -> Any:
        request = config_api.ReplaceConfigRequest()
        request.replace = main_config
        response = self.invoke(request, 'ReplaceConfig')
        return self._check_success(response, "replace_config")

    def bootstrap_cluster(self, self_assembly_uuid: str) -> Any:
        request = config_api.BootstrapClusterRequest()
        request.self_assembly_uuid = self_assembly_uuid
        response = self.invoke(request, 'BootstrapCluster')
        return self._check_success(response, "bootstrap_cluster")

    def close(self) -> None:
        self._channel.close()

    def __del__(self):
        self.close()

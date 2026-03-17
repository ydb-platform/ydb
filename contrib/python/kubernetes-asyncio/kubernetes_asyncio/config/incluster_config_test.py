# Copyright 2016 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import datetime
import os
import tempfile
import unittest
from collections.abc import Generator

import kubernetes_asyncio.config
from kubernetes_asyncio.client import Configuration
from kubernetes_asyncio.config.config_exception import ConfigException
from kubernetes_asyncio.config.incluster_config import (
    SERVICE_HOST_ENV_NAME,
    SERVICE_PORT_ENV_NAME,
    InClusterConfigLoader,
    _join_host_port,
)
from kubernetes_asyncio.config.kube_config_test import FakeConfig

_TEST_TOKEN = "temp_token"
_TEST_NEW_TOKEN = "temp_new_token"
_TEST_CERT = "temp_cert"
_TEST_HOST = "127.0.0.1"
_TEST_PORT = "80"
_TEST_HOST_PORT = "127.0.0.1:80"
_TEST_IPV6_HOST = "::1"
_TEST_IPV6_HOST_PORT = "[::1]:80"

_TEST_ENVIRON = {SERVICE_HOST_ENV_NAME: _TEST_HOST, SERVICE_PORT_ENV_NAME: _TEST_PORT}
_TEST_IPV6_ENVIRON = {
    SERVICE_HOST_ENV_NAME: _TEST_IPV6_HOST,
    SERVICE_PORT_ENV_NAME: _TEST_PORT,
}


@contextlib.contextmanager
def monkeypatch_kube_config_path() -> Generator[None, None, None]:
    old_kube_config_path = kubernetes_asyncio.config.KUBE_CONFIG_DEFAULT_LOCATION
    kubernetes_asyncio.config.KUBE_CONFIG_DEFAULT_LOCATION = "/path-does-not-exist"
    try:
        yield
    finally:
        kubernetes_asyncio.config.KUBE_CONFIG_DEFAULT_LOCATION = old_kube_config_path


class InClusterConfigTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._temp_files: list[str] = []

    def tearDown(self) -> None:
        for f in self._temp_files:
            os.remove(f)
        Configuration.set_default(None)

    def _create_file_with_temp_content(self, content: str = "") -> str:
        handler, name = tempfile.mkstemp()
        self._temp_files.append(name)
        os.write(handler, str.encode(content))
        os.close(handler)
        return name

    def get_test_loader(
        self,
        token_filename: str | None = None,
        cert_filename: str | None = None,
        environ: os._Environ | dict[str, str] = _TEST_ENVIRON,
    ) -> InClusterConfigLoader:
        if not token_filename:
            token_filename = self._create_file_with_temp_content(_TEST_TOKEN)
        if not cert_filename:
            cert_filename = self._create_file_with_temp_content(_TEST_CERT)
        return InClusterConfigLoader(
            token_filename=token_filename,
            cert_filename=cert_filename,
            try_refresh_token=True,
            environ=environ,
        )

    def test_join_host_port(self) -> None:
        self.assertEqual(_TEST_HOST_PORT, _join_host_port(_TEST_HOST, _TEST_PORT))
        self.assertEqual(
            _TEST_IPV6_HOST_PORT, _join_host_port(_TEST_IPV6_HOST, _TEST_PORT)
        )

    def test_load_config(self) -> None:
        cert_filename = self._create_file_with_temp_content(_TEST_CERT)
        loader = self.get_test_loader(cert_filename=cert_filename)
        loader._load_config()
        self.assertEqual("https://" + _TEST_HOST_PORT, loader.host)
        self.assertEqual(cert_filename, loader.ssl_ca_cert)
        self.assertEqual("Bearer " + _TEST_TOKEN, loader.token)

    async def test_refresh_token(self) -> None:
        loader = self.get_test_loader()
        config = Configuration()
        loader.load_and_set(config)

        self.assertEqual(
            "Bearer " + _TEST_TOKEN, await config.get_api_key_with_prefix("BearerToken")
        )
        self.assertEqual("Bearer " + _TEST_TOKEN, loader.token)
        self.assertIsNotNone(loader.token_expires_at)

        old_token_expires_at = loader.token_expires_at
        loader._token_filename = self._create_file_with_temp_content(_TEST_NEW_TOKEN)
        self.assertEqual(
            "Bearer " + _TEST_TOKEN, await config.get_api_key_with_prefix("BearerToken")
        )

        loader.token_expires_at = datetime.datetime.now()
        self.assertEqual(
            "Bearer " + _TEST_NEW_TOKEN,
            await config.get_api_key_with_prefix("BearerToken"),
        )
        self.assertEqual("Bearer " + _TEST_NEW_TOKEN, loader.token)
        self.assertGreater(loader.token_expires_at, old_token_expires_at)

    async def test_refresh_token_default_config_with_copies(self) -> None:
        loader = self.get_test_loader()
        loader.load_and_set()

        configs = [
            Configuration.get_default_copy(),
            Configuration.get_default_copy(),
        ]

        for config in configs:
            self.assertEqual(
                "Bearer " + _TEST_TOKEN,
                await config.get_api_key_with_prefix("BearerToken"),
            )
        self.assertEqual("Bearer " + _TEST_TOKEN, loader.token)
        self.assertIsNotNone(loader.token_expires_at)

        old_token_expires_at = loader.token_expires_at
        loader._token_filename = self._create_file_with_temp_content(_TEST_NEW_TOKEN)

        for config in configs:
            self.assertEqual(
                "Bearer " + _TEST_TOKEN,
                await config.get_api_key_with_prefix("BearerToken"),
            )

        loader.token_expires_at = datetime.datetime.now()

        for config in configs:
            self.assertEqual(
                "Bearer " + _TEST_NEW_TOKEN,
                await config.get_api_key_with_prefix("BearerToken"),
            )

        self.assertEqual("Bearer " + _TEST_NEW_TOKEN, loader.token)
        self.assertGreater(loader.token_expires_at, old_token_expires_at)

    def _should_fail_load(self, config_loader, reason) -> None:
        try:
            config_loader.load_and_set()
            self.fail(f"Should fail because {reason}")
        except ConfigException:
            # expected
            pass

    def test_no_port(self) -> None:
        loader = self.get_test_loader(environ={SERVICE_HOST_ENV_NAME: _TEST_HOST})
        self._should_fail_load(loader, "no port specified")

    def test_empty_port(self) -> None:
        loader = self.get_test_loader(
            environ={SERVICE_HOST_ENV_NAME: _TEST_HOST, SERVICE_PORT_ENV_NAME: ""}
        )
        self._should_fail_load(loader, "empty port specified")

    def test_no_host(self) -> None:
        loader = self.get_test_loader(environ={SERVICE_PORT_ENV_NAME: _TEST_PORT})
        self._should_fail_load(loader, "no host specified")

    def test_empty_host(self) -> None:
        loader = self.get_test_loader(
            environ={SERVICE_HOST_ENV_NAME: "", SERVICE_PORT_ENV_NAME: _TEST_PORT}
        )
        self._should_fail_load(loader, "empty host specified")

    def test_no_cert_file(self) -> None:
        loader = self.get_test_loader(cert_filename="not_exists_file_1123")
        self._should_fail_load(loader, "cert file does not exists")

    def test_empty_cert_file(self) -> None:
        loader = self.get_test_loader(
            cert_filename=self._create_file_with_temp_content()
        )
        self._should_fail_load(loader, "empty cert file provided")

    def test_no_token_file(self) -> None:
        loader = self.get_test_loader(token_filename="not_exists_file_1123")
        self._should_fail_load(loader, "token file does not exists")

    def test_empty_token_file(self) -> None:
        loader = self.get_test_loader(
            token_filename=self._create_file_with_temp_content()
        )
        self._should_fail_load(loader, "empty token file provided")

    def test_client_config(self) -> None:
        cert_filename = self._create_file_with_temp_content(_TEST_CERT)
        loader = self.get_test_loader(cert_filename=cert_filename)
        loader._load_config()
        client_config = Configuration()
        loader._set_config(client_config)
        self.assertEqual("https://" + _TEST_HOST_PORT, client_config.host)
        self.assertEqual(cert_filename, client_config.ssl_ca_cert)
        self.assertEqual("Bearer " + _TEST_TOKEN, client_config.api_key["BearerToken"])

    async def test_load_config_helper(self) -> None:
        token_filename = self._create_file_with_temp_content(_TEST_TOKEN)
        cert_filename = self._create_file_with_temp_content(_TEST_CERT)
        expected = FakeConfig(
            host="https://" + _TEST_HOST_PORT,
            token="Bearer " + _TEST_TOKEN,
            ssl_ca_cert=cert_filename,
        )
        actual = FakeConfig()
        with monkeypatch_kube_config_path():
            await kubernetes_asyncio.config.load_config(
                client_configuration=actual,
                try_refresh_token=None,
                token_filename=token_filename,
                cert_filename=cert_filename,
                environ=_TEST_ENVIRON,
            )
        self.assertEqual(expected, actual)


if __name__ == "__main__":
    unittest.main()

# Copyright 2018 The Kubernetes Authors.
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

import json
import sys
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, patch

from kubernetes_asyncio.config.config_exception import ConfigException
from kubernetes_asyncio.config.exec_provider import ExecProvider
from kubernetes_asyncio.config.kube_config import ConfigNode


class ExecProviderTest(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.input_ok = ConfigNode(
            "test",
            {
                "command": "aws-iam-authenticator",
                "args": ["token", "-i", "dummy"],
                "apiVersion": "client.authentication.k8s.io/v1beta1",
                "env": None,
            },
        )
        self.output_ok = """
        {
            "apiVersion": "client.authentication.k8s.io/v1beta1",
            "kind": "ExecCredential",
            "status": {
                "token": "dummy"
            }
        }
        """

        process_patch = patch(
            "kubernetes_asyncio.config.exec_provider.asyncio.create_subprocess_exec"
        )
        self.exec_mock = process_patch.start()
        self.process_mock = self.exec_mock.return_value
        self.process_mock.stdout.read = AsyncMock(return_value=self.output_ok)
        self.process_mock.stderr.read = AsyncMock(return_value="")
        self.process_mock.wait = AsyncMock(return_value=0)

    def tearDown(self) -> None:
        patch.stopall()

    def test_missing_input_keys(self) -> None:
        exec_configs = [{}, {"command": ""}, {"apiVersion": ""}]
        for exec_config in exec_configs:
            with self.assertRaises(ConfigException) as context:
                ExecProvider(ConfigNode("dummy-1", exec_config))
            self.assertIn(
                "exec: malformed request. missing key", context.exception.args[0]
            )

    async def test_error_code_returned(self) -> None:
        self.process_mock.stdout.read.return_value = ""
        self.process_mock.wait.return_value = 1
        with self.assertRaisesRegex(ConfigException, "exec: process returned 1"):
            ep = ExecProvider(self.input_ok)
            await ep.run()

    async def test_nonjson_output_returned(self) -> None:
        self.process_mock.stdout.read.return_value = ""
        with self.assertRaisesRegex(
            ConfigException, "exec: failed to decode process output"
        ):
            ep = ExecProvider(self.input_ok)
            await ep.run()

    async def test_missing_output_keys(self) -> None:
        outputs = [
            """
            {
                "kind": "ExecCredential",
                "status": {
                    "token": "dummy"
                }
            }
            """,
            """
            {
                "apiVersion": "client.authentication.k8s.io/v1beta1",
                "status": {
                    "token": "dummy"
                }
            }
            """,
            """
            {
                "apiVersion": "client.authentication.k8s.io/v1beta1",
                "kind": "ExecCredential"
            }
            """,
        ]
        for output in outputs:
            self.process_mock.stdout.read.return_value = output
            with self.assertRaisesRegex(
                ConfigException, "exec: malformed response. missing key"
            ):
                ep = ExecProvider(self.input_ok)
                await ep.run()

    async def test_mismatched_api_version(self) -> None:
        wrong_api_version = "client.authentication.k8s.io/v1"
        output = f"""
        {{
            "apiVersion": "{wrong_api_version}",
            "kind": "ExecCredential",
            "status": {{
                "token": "dummy"
            }}
        }}
        """
        self.process_mock.stdout.read.return_value = output
        with self.assertRaisesRegex(
            ConfigException,
            f"exec: plugin api version {wrong_api_version} does not match",
        ):
            ep = ExecProvider(self.input_ok)
            await ep.run()

    async def test_ok_01(self) -> None:
        ep = ExecProvider(self.input_ok)
        result = await ep.run()
        self.assertTrue(isinstance(result, dict))
        self.assertTrue("token" in result)
        self.exec_mock.assert_called_once_with(
            "aws-iam-authenticator",
            "token",
            "-i",
            "dummy",
            env=ANY,
            stderr=-1,
            stdin=None,
            stdout=-1,
        )
        self.process_mock.stdout.read.assert_awaited_once()
        self.process_mock.stderr.read.assert_awaited_once()
        self.process_mock.wait.assert_awaited_once()

    async def test_ok_with_args(self) -> None:
        input_ok = ConfigNode(
            "test",
            {
                "command": "aws-iam-authenticator",
                "apiVersion": "client.authentication.k8s.io/v1beta1",
                "args": ["token", "-i", "dummy", "--mock", "90"],
            },
        )
        ep = ExecProvider(input_ok)
        result = await ep.run()
        self.assertTrue(isinstance(result, dict))
        self.assertTrue("token" in result)
        self.exec_mock.assert_called_once_with(
            "aws-iam-authenticator",
            "token",
            "-i",
            "dummy",
            "--mock",
            "90",
            env=ANY,
            stderr=-1,
            stdin=None,
            stdout=-1,
        )
        self.process_mock.stdout.read.assert_awaited_once()
        self.process_mock.stderr.read.assert_awaited_once()
        self.process_mock.wait.assert_awaited_once()

    async def test_ok_with_env(self) -> None:
        input_ok = ConfigNode(
            "test",
            {
                "command": "aws-iam-authenticator",
                "apiVersion": "client.authentication.k8s.io/v1beta1",
                "args": ["token", "-i", "dummy"],
                "env": [
                    {
                        "name": "EXEC_PROVIDER_ENV_NAME",
                        "value": "EXEC_PROVIDER_ENV_VALUE",
                    }
                ],
            },
        )

        ep = ExecProvider(input_ok)
        result = await ep.run()
        self.assertTrue(isinstance(result, dict))
        self.assertTrue("token" in result)

        env_used = self.exec_mock.await_args_list[0][1]["env"]
        self.assertEqual(env_used["EXEC_PROVIDER_ENV_NAME"], "EXEC_PROVIDER_ENV_VALUE")
        self.assertEqual(
            json.loads(env_used["KUBERNETES_EXEC_INFO"]),
            {
                "apiVersion": "client.authentication.k8s.io/v1beta1",
                "kind": "ExecCredential",
                "spec": {"interactive": sys.stdout.isatty()},
            },
        )
        self.exec_mock.assert_called_once_with(
            "aws-iam-authenticator",
            "token",
            "-i",
            "dummy",
            env=ANY,
            stderr=-1,
            stdin=None,
            stdout=-1,
        )
        self.process_mock.stdout.read.assert_awaited_once()
        self.process_mock.stderr.read.assert_awaited_once()
        self.process_mock.wait.assert_awaited_once()

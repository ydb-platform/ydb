# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2021 Chris Rose
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

"""
Handling of sensitive data stored in AWS Secrets Manager
"""

from __future__ import annotations

import logging

from typing import Any, Callable

import attr
import boto3

from environ._environ_config import CNF_KEY, RAISE, _ConfigEntry

from ._utils import _get_default_secret


log = logging.getLogger(__name__)


def convert_secret(key):
    def converter(value):
        if isinstance(value, str):
            return value
        return value[key]

    return converter


def _build_secretsmanager_client():
    client = boto3.client("secretsmanager")
    log.debug("Created a secretsmanager client %s", client)
    return client


@attr.s(auto_attribs=True)
class SecretsManagerSecrets:
    """
    Load secrets from the AWS Secrets Manager.

    The secret name should be stored in the environment variable

    .. warning::

       Requires `boto3 <https://pypi.org/project/boto3/>`_! Please install
       *environ-config* with the ``aws`` extra: ``python -Im pip install
       environ-config[aws]``

    .. versionadded:: 21.4.0
    """

    _client: boto3.client | None = None

    @property
    def client(self) -> boto3.client:
        if self._client is None:
            self._client = _build_secretsmanager_client()

        return self._client

    def secret(
        self,
        default: Any = RAISE,
        converter: Callable = convert_secret("SecretString"),
        name: str | None = None,
        help: str | None = None,
    ):
        """
        Declare a secrets manager secret on an `environ.config`-decorated class

        All parameters work just like in `environ.var`.
        """
        return attr.ib(
            default=default,
            metadata={
                CNF_KEY: _ConfigEntry(name, default, None, self._get, help)
            },
            converter=converter,
        )

    def _get(self, environ, metadata, prefix, name):
        ce = metadata[CNF_KEY]
        if ce.name:
            secret_name_envvar = ce.name
            log.debug(
                "override env variable with explicit name %s",
                secret_name_envvar,
            )
        else:
            parts = (*prefix, name)
            secret_name_envvar = "_".join(parts).upper()
            log.debug(
                "secret name environment variable %s", secret_name_envvar
            )

        try:
            secret_name = environ[secret_name_envvar]
        except KeyError:
            # missing the environment; let's try to get the default
            log.debug(
                "no key %s in environment, using default=%s",
                secret_name_envvar,
                ce.default,
            )
            return _get_default_secret(secret_name_envvar, ce.default)
        log.debug("secret name: %s", secret_name)

        return self.client.get_secret_value(SecretId=secret_name)

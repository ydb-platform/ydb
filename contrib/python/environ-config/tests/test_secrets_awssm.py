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

import os
import uuid

from unittest.mock import patch

import attr
import boto3
import pytest

from moto import mock_aws

import environ

from environ.exceptions import MissingSecretError
from environ.secrets import SecretsManagerSecrets, _SecretStr


@pytest.fixture(name="shut_boto_up", autouse=True, scope="session")
def _shut_boto_up():
    import logging

    for name in logging.Logger.manager.loggerDict:
        if ("boto" in name) or ("urllib3" in name):
            logging.getLogger(name).setLevel(logging.WARNING)


@pytest.fixture(name="force_region", autouse=True, scope="session")
def _force_region():
    with patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-east-1"}):
        yield


@pytest.fixture(name="mock_aws_credentials")
def _mock_aws_credentials(force_region):
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_SECURITY_TOKEN": "testing",
            "AWS_SESSION_TOKEN": "testing",
        },
    ):
        yield


@pytest.fixture(name="secretsmanager")
def _secretsmanager():
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="us-east-2")


@pytest.fixture(name="secret")
def _secret():
    return str(uuid.uuid4())


@pytest.fixture(name="sm")
def _sm(secretsmanager, secret):
    secretsmanager.create_secret(Name=secret)
    secretsmanager.put_secret_value(SecretId=secret, SecretString="foobar")
    return SecretsManagerSecrets(client=secretsmanager)


class TestAWSSMSecret:
    def test_missing_default_raises(self, sm):
        """
        Missing values without a default raise an MissingSecretError.
        """

        @environ.config
        class Cfg:
            pw = sm.secret()

        with pytest.raises(MissingSecretError):
            environ.to_config(Cfg, {})

    def test_no_default_resolves_secret(self, sm, secretsmanager):
        """
        Verify that, when no default is provided, resolution should not happen
        until the configuration is needed.
        """

        @environ.config
        class Cfg:
            pw = sm.secret()

        secretsmanager.create_secret(Name="SecretName")
        secretsmanager.put_secret_value(
            SecretId="SecretName", SecretString="no-default"
        )
        assert (
            environ.to_config(Cfg, {"APP_PW": "SecretName"}).pw == "no-default"
        )

    def test_secret_works_with_default_client_overridden(
        self, mock_aws_credentials
    ):
        """
        Assert that the SM type can function without a custom client override
        when testing
        """
        sm = SecretsManagerSecrets()

        @environ.config
        class Cfg:
            pw = sm.secret()

        with mock_aws():
            # we need to make sure we're using the same region. It doesn't
            # matter which -- moto _and_ boto will try figure it out from the
            # environment -- but it has to be the same.
            sm.client.create_secret(Name="SecretName")
            sm.client.put_secret_value(
                SecretId="SecretName", SecretString="no-default"
            )
            conf = environ.to_config(Cfg, {"APP_PW": "SecretName"})
            assert conf.pw == "no-default"

    def test_default(self, sm, secret):
        """
        Defaults are used iff the key is missing.
        """

        @environ.config
        class Cfg:
            password = sm.secret(default="not used")
            secret = sm.secret(default="used!")

        cfg = environ.to_config(Cfg, {"APP_PASSWORD": secret})

        assert Cfg("foobar", "used!") == cfg

    def test_default_factory(self, sm, secret):
        """
        Defaults are used iff the key is missing.
        """

        def getpass():
            return "a default"

        @environ.config
        class Cfg:
            password = sm.secret(default=attr.Factory(getpass))
            secret = sm.secret(default=attr.Factory(getpass))

        cfg = environ.to_config(Cfg, {"APP_PASSWORD": secret})

        assert Cfg("foobar", "a default") == cfg

    def test_name_overwrite(self, sm, secret):
        """
        Passing a specific key name is respected.
        """
        sm.client.put_secret_value(SecretId=secret, SecretString="foobar")

        @environ.config
        class Cfg:
            pw = sm.secret(name="password")

        cfg = environ.to_config(Cfg, {"password": secret})

        assert _SecretStr("foobar") == cfg.pw

    def test_nested(self, sm, secret):
        """
        Prefix building works.
        """
        sm.client.put_secret_value(SecretId=secret, SecretString="nested!")

        @environ.config
        class Cfg:
            @environ.config
            class DB:
                password = sm.secret()

            db = environ.group(DB)

        cfg = environ.to_config(Cfg, {"APP_DB_PASSWORD": secret})

        assert _SecretStr("nested!") == cfg.db.password

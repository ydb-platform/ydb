# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2017 Hynek Schlawack
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

import attr
import pytest

import environ

from environ.exceptions import MissingSecretError
from environ.secrets import (
    DirectorySecrets,
    INISecrets,
    VaultEnvSecrets,
    _SecretStr,
)


class TestSecretStr:
    def test_secret_str_no_repr(self):
        """
        Outside of reprs, _SecretStr behaves normally.
        """
        s = _SecretStr("abc")

        assert "'abc'" == repr(s)

    def test_secret_str_censors(self):
        """
        _SecretStr censors its __repr__ if it's called from another __repr__.
        """
        s = _SecretStr("abc")

        @attr.s
        class Cfg:
            s = attr.ib()

        assert "Cfg(s=<SECRET>)" == repr(Cfg(s))


@pytest.fixture()
def ini_file(tmpdir):
    f = tmpdir.join("foo.ini")
    f.write(
        """\
[secrets]
password = foobar
db_password = nested!
[other_secrets]
password = bar%foo
[yet_another_section]
secret = qux
"""
    )
    return f


@pytest.fixture()
def ini(ini_file):
    return INISecrets.from_path(str(ini_file))


class TestIniSecret:
    def test_missing_default_raises(self, ini):
        """
        Missing values without a default raise an MissingSecretError.
        """

        @environ.config
        class Cfg:
            pw = ini.secret()

        with pytest.raises(MissingSecretError):
            environ.to_config(Cfg, {})

    def test_default(self, ini):
        """
        Defaults are used iff the key is missing.
        """

        @environ.config
        class Cfg:
            password = ini.secret(default="not used")
            secret = ini.secret(default="used!")

        cfg = environ.to_config(Cfg, {})

        assert Cfg("foobar", "used!") == cfg

    def test_default_factory(self, ini):
        """
        Defaults are used iff the key is missing.
        """

        def getpass():
            return "thesecret"

        @environ.config
        class Cfg:
            password = ini.secret(default=attr.Factory(getpass))
            secret = ini.secret(default=attr.Factory(getpass))

        cfg = environ.to_config(Cfg, {})

        assert Cfg("foobar", "thesecret") == cfg

    def test_name_overwrite(self, ini):
        """
        Passing a specific key name is respected.
        """

        @environ.config
        class Cfg:
            pw = ini.secret(name="password")

        cfg = environ.to_config(Cfg, {})

        assert _SecretStr("foobar") == cfg.pw

    def test_overwrite_sections(self, ini):
        """
        The source section can be overwritten.
        """
        ini.section = "yet_another_section"

        @environ.config
        class Cfg:
            password = ini.secret(section="other_secrets")
            secret = ini.secret()

        cfg = environ.to_config(Cfg, {})

        assert _SecretStr("bar%foo") == cfg.password

    def test_nested(self, ini):
        """
        Prefix building works.
        """

        @environ.config
        class Cfg:
            @environ.config
            class DB:
                password = ini.secret()

            db = environ.group(DB)

        cfg = environ.to_config(Cfg, {})

        assert _SecretStr("nested!") == cfg.db.password

    def test_from_path_in_env_delayed(self, ini_file):
        """
        `from_path_in_env` prepares for loading but doesn't load until
        `to_config` runs.
        """
        secret = INISecrets.from_path_in_env("APP_SECRETS_INI").secret

        @environ.config
        class Cfg:
            password = secret()

        cfg = environ.to_config(Cfg, {"APP_SECRETS_INI": str(ini_file)})

        assert "foobar" == cfg.password


@pytest.fixture()
def vault():
    return VaultEnvSecrets(vault_prefix="SECRET")


class TestVaultEnvSecrets:
    def test_returns_secret_str(self, vault):
        """
        The returned strings are `_SecretStr`.
        """

        @environ.config
        class Cfg:
            x = vault.secret()

        cfg = environ.to_config(Cfg, {"SECRET_X": "foo"})

        assert isinstance(cfg.x, _SecretStr)
        assert "foo" == cfg.x

    def test_overwrite_name(self, vault):
        """
        The variable name can be overwritten.
        """

        @environ.config
        class Cfg:
            password = vault.secret(name="not_password")

        cfg = environ.to_config(
            Cfg, {"SECRET_PASSWORD": "wrong", "not_password": "correct"}
        )

        assert "correct" == cfg.password

    def test_missing_raises_missing_secret(self, vault):
        """
        Missing values without a default raise an MissingSecretError.
        """

        @environ.config
        class Cfg:
            pw = vault.secret()

        with pytest.raises(MissingSecretError):
            environ.to_config(Cfg, {})

    def test_prefix_callable(self):
        """
        vault_prefix can also be a callable that is called on each entry.
        """
        fake_environ = {"ABC_PW": "foo"}

        def extract(env):
            assert env == fake_environ
            return "ABC"

        vault = VaultEnvSecrets(vault_prefix=extract)

        @environ.config
        class Cfg:
            pw = vault.secret()

        cfg = environ.to_config(Cfg, fake_environ)

        assert _SecretStr("foo") == cfg.pw


@pytest.fixture()
def secrets_dir(tmp_path):
    def make_secrets_file(name, content):
        secret_file = tmp_path / name
        secret_file.write_text(content)

    make_secrets_file("empty", "")
    make_secrets_file("apples", "apples\n")
    make_secrets_file("oranges", "oranges")

    return str(tmp_path)


class TestDirectorySecrets:
    def test_secret_content(self, secrets_dir):
        """
        Reading secrets with different content.
        """
        dir = DirectorySecrets.from_path(secrets_dir)

        @environ.config
        class Cfg:
            empty = dir.secret()
            apples = dir.secret()
            oranges = dir.secret()

        cfg = environ.to_config(Cfg, {})

        assert _SecretStr("") == cfg.empty
        assert _SecretStr("apples\n") == cfg.apples
        assert _SecretStr("oranges") == cfg.oranges

    def test_secret_different_name(self, secrets_dir):
        """
        Attribute names can be different than filenames,
        the name argument can be used to select the filename.
        """
        dir = DirectorySecrets.from_path(secrets_dir)

        @environ.config
        class Cfg:
            nothing = dir.secret(name="empty")
            has_newline = dir.secret(name="apples")
            no_newline = dir.secret(name="oranges")

        cfg = environ.to_config(Cfg, {})

        assert _SecretStr("") == cfg.nothing
        assert _SecretStr("apples\n") == cfg.has_newline
        assert _SecretStr("oranges") == cfg.no_newline

    def test_secret_from_env(self, secrets_dir):
        """
        A directory can be specified in an environment variable.
        """
        dir = DirectorySecrets.from_path_in_env("SECRETS_DIR", None)

        @environ.config
        class Cfg:
            apples = dir.secret()

        cfg = environ.to_config(Cfg, {"SECRETS_DIR": secrets_dir})
        assert _SecretStr("apples\n") == cfg.apples

    def test_missing_default_value(self, tmpdir):
        """
        Missing secrets should raise Exceptions when loading the config.
        """
        dir = DirectorySecrets.from_path(str(tmpdir))

        @environ.config
        class Cfg:
            doesnt_exist = dir.secret()

        with pytest.raises(MissingSecretError):
            environ.to_config(Cfg, {})

    def test_default_value_specified(self, tmpdir):
        """
        Default values work for DirectorySecrets too.
        """
        dir = DirectorySecrets.from_path(str(tmpdir))

        @environ.config
        class Cfg:
            doesnt_exist = dir.secret(default="Test default value")

        cfg = environ.to_config(Cfg, {})
        assert "Test default value" == cfg.doesnt_exist

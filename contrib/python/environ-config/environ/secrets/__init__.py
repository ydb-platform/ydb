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

"""
Handling of sensitive data.
"""

from __future__ import annotations

import logging
import sys

from configparser import NoOptionError, RawConfigParser
from pathlib import Path
from typing import Any, Callable

import attr

from environ._environ_config import CNF_KEY, RAISE, _ConfigEntry
from environ.exceptions import MissingSecretImplementationError

from ._utils import _get_default_secret


try:
    from environ.secrets.awssm import SecretsManagerSecrets
except ImportError:  # pragma: no cover

    class SecretsManagerSecrets:
        def secret(self, *args, **kwargs):
            msg = "AWS secrets manager requires boto3"
            raise MissingSecretImplementationError(msg)


__all__ = [
    "DirectorySecrets",
    "INISecrets",
    "SecretsManagerSecrets",
    "VaultEnvSecrets",
]

log = logging.getLogger(__name__)


FileOpenError = OSError


@attr.s
class INISecrets:
    """
    Load secrets from an `INI file <https://en.wikipedia.org/wiki/INI_file>`_
    using `configparser.RawConfigParser`.
    """

    section: str = attr.ib()
    _cfg: RawConfigParser = attr.ib(default=None)
    _env_name: str | None = attr.ib(default=None)
    _env_default: Any = attr.ib(default=None)

    @classmethod
    def from_path(cls, path: str | Path, section="secrets") -> INISecrets:
        """
        Look for secrets in *section* of *path*.

        Args:
            path: A path to an INI file.

            section: The section in the INI file to read the secrets from.
        """
        return cls(section, _load_ini(str(path)), None, None)

    @classmethod
    def from_path_in_env(
        cls,
        env_name: str,
        default: str | None = None,
        section: str = "secrets",
    ) -> INISecrets:
        """
        Get the path from the environment variable *env_name* **at runtime**
        and then load the secrets from it.

        This allows you to overwrite the path to the secrets file in
        development.

        Args:
            env_name:
                Environment variable that is used to determine the path of the
                secrets file.

            default: The default path to load from.

            section: The section in the INI file to read the secrets from.
        """
        return cls(section, None, env_name, default)

    def secret(
        self,
        default: Any = RAISE,
        converter: Callable | None = None,
        name: str | None = None,
        section: str | None = None,
        help: str | None = None,
    ) -> Any:
        """
        Declare a secret on an `environ.config`-decorated class.

        Args:
            section: Overwrite the section where to look for the values.

        Other parameters work just like in `environ.var`.
        """
        if section is None:
            section = self.section

        return attr.ib(
            default=default,
            metadata={
                CNF_KEY: _ConfigEntry(name, default, None, self._get, help),
                CNF_INI_SECRET_KEY: _INIConfig(section),
            },
            converter=converter,
        )

    def _get(self, environ, metadata, prefix, name) -> _SecretStr:
        # Delayed loading.
        if self._cfg is None and self._env_name is not None:
            log.debug("looking for env var '%s'.", self._env_name)
            self._cfg = _load_ini(
                environ.get(self._env_name, self._env_default)
            )

        ce = metadata[CNF_KEY]
        ic = metadata[CNF_INI_SECRET_KEY]
        section = ic.section

        var = (
            ce.name if ce.name is not None else "_".join(prefix[1:] + (name,))
        )
        try:
            log.debug("looking for '%s' in section '%s'.", var, section)
            val = self._cfg.get(section, var)

            return _SecretStr(val)
        except NoOptionError:
            return _get_default_secret(var, ce.default)


@attr.s
class DirectorySecrets:
    """
    Load secrets from a directory containing secrets in separate files.
    Suitable for reading Docker or Kubernetes secrets from the filesystem
    inside a container.

    .. versionadded:: 21.1.0
    """

    secrets_dir: str | Path = attr.ib()
    _env_name: str | None = attr.ib(default=None)

    @classmethod
    def from_path(cls, path: str | Path) -> DirectorySecrets:
        """
        Look for secrets in *path* directory.

        Args:
            path: A path to directory containing secrets as files.
        """
        return cls(path)

    @classmethod
    def from_path_in_env(
        cls, env_name: str, default: str | Path | None
    ) -> DirectorySecrets:
        """
        Get the path from the environment variable *env_name* and then load the
        secrets from that directory at runtime.

        This allows you to overwrite the path to the secrets directory in
        development.

        Args:
            env_name:
                Environment variable that is used to determine the path of the
                secrets directory.

            default: The default path to load from.
        """
        return cls(default, env_name)

    def secret(
        self,
        default: Any = RAISE,
        converter: Callable | None = None,
        name: str | None = None,
        help: str | None = None,
    ) -> Any:
        return attr.ib(
            default=default,
            metadata={
                CNF_KEY: _ConfigEntry(name, default, None, self._get, help)
            },
            converter=converter,
        )

    def _get(self, environ, metadata, prefix, name) -> _SecretStr:
        ce = metadata[CNF_KEY]
        # conventions for file naming might be different
        # than for environment variables, so we don't call .upper()
        filename = ce.name or "_".join(prefix[1:] + (name,))

        # Looking up None in os.environ is an error.
        if self._env_name:
            secrets_dir = environ.get(self._env_name, self.secrets_dir)
        else:
            secrets_dir = self.secrets_dir

        secret_path = Path(secrets_dir) / filename
        log.debug("looking for secret in file '%s'.", secret_path)

        try:
            return _SecretStr(secret_path.read_text())
        except FileOpenError:
            return _get_default_secret(filename, ce.default)


@attr.s
class VaultEnvSecrets:
    """
    Loads secrets from environment variables that follow the naming style from
    `envconsul <https://github.com/hashicorp/envconsul>`_.
    """

    vault_prefix: str = attr.ib()

    def secret(
        self,
        default: Any = RAISE,
        converter: Callable | None = None,
        name: str | None = None,
        help: str | None = None,
    ) -> Any:
        """
        Almost identical to `environ.var` except that it takes *envconsul*
        naming into account.
        """
        return attr.ib(
            default=default,
            metadata={
                CNF_KEY: _ConfigEntry(name, default, None, self._get, help)
            },
            converter=converter,
        )

    def _get(self, environ, metadata, prefix, name) -> _SecretStr:
        ce = metadata[CNF_KEY]

        if ce.name is not None:
            var = ce.name
        else:
            if callable(self.vault_prefix):
                vp = self.vault_prefix(environ)
            else:
                vp = self.vault_prefix
            var = "_".join((vp,) + prefix[1:] + (name,)).upper()

        log.debug("looking for env var '%s'.", var)
        try:
            val = environ[var]
            return _SecretStr(val)
        except KeyError:
            return _get_default_secret(var, ce.default)


class _SecretStr(str):
    """
    String that censors its __repr__ if called from an attrs repr.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        # The frame numbers varies across attrs versions. Use this convoluted
        # form to make the call lazy.
        if (
            sys._getframe(1).f_code.co_name == "__repr__"
            or sys._getframe(2).f_code.co_name == "__repr__"
        ):
            return "<SECRET>"

        return str.__repr__(self)


CNF_INI_SECRET_KEY = CNF_KEY + "_ini_secret"


@attr.s
class _INIConfig:
    section: str = attr.ib()


def _load_ini(path: str) -> RawConfigParser:
    """
    Load an INI file from *path*.
    """
    cfg = RawConfigParser()
    with Path(path).open() as f:
        cfg.read_file(f)

    return cfg

# Copyright 2010-2025 The pygit2 contributors
#
# This file is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2,
# as published by the Free Software Foundation.
#
# In addition to the permissions in the GNU General Public License,
# the authors give you unlimited permission to link the compiled
# version of this file into combinations with other programs,
# and to distribute those combinations without any restriction
# coming from the use of this file.  (The General Public License
# restrictions do apply in other respects; for example, they cover
# modification of the file, and distribution when not linked into
# a combined executable.)
#
# This file is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING.  If not, write to
# the Free Software Foundation, 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301, USA.

from __future__ import annotations

from typing import TYPE_CHECKING

from .enums import CredentialType

if TYPE_CHECKING:
    from pathlib import Path


class Username:
    """Username credentials

    This is an object suitable for passing to a remote's credentials
    callback and for returning from said callback.
    """

    def __init__(self, username: str):
        self._username = username

    @property
    def credential_type(self) -> CredentialType:
        return CredentialType.USERNAME

    @property
    def credential_tuple(self) -> tuple[str]:
        return (self._username,)

    def __call__(
        self, _url: str, _username: str | None, _allowed: CredentialType
    ) -> Username:
        return self


class UserPass:
    """Username/Password credentials

    This is an object suitable for passing to a remote's credentials
    callback and for returning from said callback.
    """

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    @property
    def credential_type(self) -> CredentialType:
        return CredentialType.USERPASS_PLAINTEXT

    @property
    def credential_tuple(self) -> tuple[str, str]:
        return (self._username, self._password)

    def __call__(
        self, _url: str, _username: str | None, _allowed: CredentialType
    ) -> UserPass:
        return self


class Keypair:
    """
    SSH key pair credentials.

    This is an object suitable for passing to a remote's credentials
    callback and for returning from said callback.

    Parameters:

    username : str
        The username being used to authenticate with the remote server.

    pubkey : str
        The path to the user's public key file.

    privkey : str
        The path to the user's private key file.

    passphrase : str
        The password used to decrypt the private key file, or empty string if
        no passphrase is required.
    """

    def __init__(
        self,
        username: str,
        pubkey: str | Path | None,
        privkey: str | Path | None,
        passphrase: str | None,
    ):
        self._username = username
        self._pubkey = pubkey
        self._privkey = privkey
        self._passphrase = passphrase

    @property
    def credential_type(self) -> CredentialType:
        return CredentialType.SSH_KEY

    @property
    def credential_tuple(
        self,
    ) -> tuple[str, str | Path | None, str | Path | None, str | None]:
        return (self._username, self._pubkey, self._privkey, self._passphrase)

    def __call__(
        self, _url: str, _username: str | None, _allowed: CredentialType
    ) -> Keypair:
        return self


class KeypairFromAgent(Keypair):
    def __init__(self, username: str):
        super().__init__(username, None, None, None)


class KeypairFromMemory(Keypair):
    @property
    def credential_type(self) -> CredentialType:
        return CredentialType.SSH_MEMORY

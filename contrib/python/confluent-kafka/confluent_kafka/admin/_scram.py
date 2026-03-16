# Copyright 2023 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .. import cimpl

from enum import Enum


class ScramMechanism(Enum):
    """
    Enumerates SASL/SCRAM mechanisms.
    """
    UNKNOWN = cimpl.SCRAM_MECHANISM_UNKNOWN  #: Unknown SASL/SCRAM mechanism
    SCRAM_SHA_256 = cimpl.SCRAM_MECHANISM_SHA_256  #: SCRAM-SHA-256 mechanism
    SCRAM_SHA_512 = cimpl.SCRAM_MECHANISM_SHA_512  #: SCRAM-SHA-512 mechanism

    def __lt__(self, other):
        if self.__class__ != other.__class__:
            return NotImplemented
        return self.value < other.value


class ScramCredentialInfo:
    """
    Contains mechanism and iterations for a
    SASL/SCRAM credential associated with a user.

    Parameters
    ----------
    mechanism: ScramMechanism
        SASL/SCRAM mechanism.
    iterations: int
        Positive number of iterations used when creating the credential.
    """
    def __init__(self, mechanism, iterations):
        self.mechanism = mechanism
        self.iterations = iterations


class UserScramCredentialsDescription:
    """
    Represent all SASL/SCRAM credentials
    associated with a user that can be retrieved.

    Parameters
    ----------
    user: str
        The user name.
    scram_credential_infos: list(ScramCredentialInfo)
        SASL/SCRAM credential representations for the user.
    """
    def __init__(self, user, scram_credential_infos):
        self.user = user
        self.scram_credential_infos = scram_credential_infos


class UserScramCredentialAlteration:
    """
    Base class for SCRAM credential alterations.

    Parameters
    ----------
    user: str
        The user name.
    """
    def __init__(self, user: str):
        self.user = user


class UserScramCredentialUpsertion(UserScramCredentialAlteration):
    """
    A request to update/insert a SASL/SCRAM credential for a user.

    Parameters
    ----------
    user: str
        The user name.
    scram_credential_info: ScramCredentialInfo
        The mechanism and iterations.
    password: bytes
        Password to HMAC before storage.
    salt: bytes
        Salt to use. Will be generated randomly if None. (optional)
    """
    def __init__(self, user, scram_credential_info, password, salt=None):
        super(UserScramCredentialUpsertion, self).__init__(user)
        self.scram_credential_info = scram_credential_info
        self.password = password
        self.salt = salt


class UserScramCredentialDeletion(UserScramCredentialAlteration):
    """
    A request to delete a SASL/SCRAM credential for a user.

    Parameters
    ----------
    user: str
        The user name.
    mechanism: ScramMechanism
        SASL/SCRAM mechanism.
    """
    def __init__(self, user, mechanism):
        super(UserScramCredentialDeletion, self).__init__(user)
        self.mechanism = mechanism

# Copyright (c) 2018 Yubico AB
# All rights reserved.
#
#   Redistribution and use in source and binary forms, with or
#   without modification, are permitted provided that the following
#   conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from __future__ import annotations

from ..webauthn import AuthenticatorData, AttestationObject
from enum import IntEnum, unique
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding, ec, rsa
from cryptography.exceptions import InvalidSignature as _InvalidSignature
from dataclasses import dataclass
from functools import wraps
from typing import List, Type, Mapping, Sequence, Optional, Any

import abc


class InvalidAttestation(Exception):
    pass


class InvalidData(InvalidAttestation):
    pass


class InvalidSignature(InvalidAttestation):
    pass


class UntrustedAttestation(InvalidAttestation):
    pass


class UnsupportedType(InvalidAttestation):
    def __init__(self, auth_data, fmt=None):
        super().__init__(
            f'Attestation format "{fmt}" is not supported'
            if fmt
            else "This attestation format is not supported!"
        )
        self.auth_data = auth_data
        self.fmt = fmt


@unique
class AttestationType(IntEnum):
    BASIC = 1
    SELF = 2
    ATT_CA = 3
    ANON_CA = 4
    NONE = 0


@dataclass
class AttestationResult:
    attestation_type: AttestationType
    trust_path: List[bytes]


def catch_builtins(f):
    @wraps(f)
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except (ValueError, KeyError, IndexError) as e:
            raise InvalidData(e)

    return inner


@catch_builtins
def verify_x509_chain(chain: List[bytes]) -> None:
    """Verifies a chain of certificates.

    Checks that the first item in the chain is signed by the next, and so on.
    The first item is the leaf, the last is the root.
    """
    certs = [x509.load_der_x509_certificate(der, default_backend()) for der in chain]
    cert = certs.pop(0)
    while certs:
        child = cert
        cert = certs.pop(0)
        pub = cert.public_key()
        try:
            if isinstance(pub, rsa.RSAPublicKey):
                pub.verify(
                    child.signature,
                    child.tbs_certificate_bytes,
                    padding.PKCS1v15(),
                    child.signature_hash_algorithm,
                )
            elif isinstance(pub, ec.EllipticCurvePublicKey):
                pub.verify(
                    child.signature,
                    child.tbs_certificate_bytes,
                    ec.ECDSA(child.signature_hash_algorithm),
                )
        except _InvalidSignature:
            raise InvalidSignature()


class Attestation(abc.ABC):
    @abc.abstractmethod
    def verify(
        self,
        statement: Mapping[str, Any],
        auth_data: AuthenticatorData,
        client_data_hash: bytes,
    ) -> AttestationResult:
        """Verifies attestation statement.

        :return: An AttestationResult if successful.
        """

    @staticmethod
    def for_type(fmt: str) -> Type[Attestation]:
        for cls in Attestation.__subclasses__():
            if getattr(cls, "FORMAT", None) == fmt:
                return cls

        class TypedUnsupportedAttestation(UnsupportedAttestation):
            def __init__(self):
                super().__init__(fmt)

        return TypedUnsupportedAttestation


class UnsupportedAttestation(Attestation):
    def __init__(self, fmt=None):
        self.fmt = fmt

    def verify(self, statement, auth_data, client_data_hash):
        raise UnsupportedType(auth_data, self.fmt)


class NoneAttestation(Attestation):
    FORMAT = "none"

    def verify(self, statement, auth_data, client_data_hash):
        if statement != {}:
            raise InvalidData("None Attestation requires empty statement.")
        return AttestationResult(AttestationType.NONE, [])


def _validate_cert_common(cert):
    if cert.version != x509.Version.v3:
        raise InvalidData("Attestation certificate must use version 3!")

    try:
        bc = cert.extensions.get_extension_for_class(x509.BasicConstraints)
        if bc.value.ca:
            raise InvalidData("Attestation certificate must have CA=false!")
    except x509.ExtensionNotFound:
        raise InvalidData("Attestation certificate must have Basic Constraints!")


def _default_attestations():
    return [
        cls()  # type: ignore
        for cls in Attestation.__subclasses__()
        if getattr(cls, "FORMAT", "none") != "none"
    ]


class AttestationVerifier(abc.ABC):
    """Base class for verifying attestation.

    Override the ca_lookup method to provide a trusted root certificate used
    to verify the trust path from the attestation.
    """

    def __init__(self, attestation_types: Optional[Sequence[Attestation]] = None):
        self._attestation_types = attestation_types or _default_attestations()

    @abc.abstractmethod
    def ca_lookup(
        self, attestation_result: AttestationResult, auth_data: AuthenticatorData
    ) -> Optional[bytes]:
        """Lookup a CA certificate to be used to verify a trust path.

        :param attestation_result: The result of the attestation
        :param auth_data: The AuthenticatorData from the registration
        """
        raise NotImplementedError()

    def verify_attestation(
        self, attestation_object: AttestationObject, client_data_hash: bytes
    ) -> None:
        """Verify attestation.

        :param attestation_object: dict containing attestation data.
        :param client_data_hash: SHA256 hash of the ClientData bytes.
        """
        att_verifier: Attestation = UnsupportedAttestation(attestation_object.fmt)
        for at in self._attestation_types:
            if getattr(at, "FORMAT", None) == attestation_object.fmt:
                att_verifier = at
                break
        # An unsupported format causes an exception to be thrown, which
        # includes the auth_data. The caller may choose to handle this case
        # and allow the registration.
        result = att_verifier.verify(
            attestation_object.att_stmt,
            attestation_object.auth_data,
            client_data_hash,
        )

        # Lookup CA to use for trust path verification
        ca = self.ca_lookup(result, attestation_object.auth_data)
        if not ca:
            raise UntrustedAttestation("No root found for Authenticator")

        # Validate the trust chain
        try:
            verify_x509_chain(result.trust_path + [ca])
        except InvalidSignature as e:
            raise UntrustedAttestation(e)

    def __call__(self, *args):
        """Allows passing an instance to Fido2Server as verify_attestation"""
        self.verify_attestation(*args)

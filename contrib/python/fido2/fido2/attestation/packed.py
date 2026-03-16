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

from .base import (
    Attestation,
    AttestationType,
    AttestationResult,
    InvalidData,
    InvalidSignature,
    catch_builtins,
    _validate_cert_common,
)
from ..cose import CoseKey

from cryptography import x509
from cryptography.exceptions import InvalidSignature as _InvalidSignature
from cryptography.hazmat.backends import default_backend


OID_AAGUID = x509.ObjectIdentifier("1.3.6.1.4.1.45724.1.1.4")


def _validate_packed_cert(cert, aaguid):
    # https://www.w3.org/TR/webauthn/#packed-attestation-cert-requirements
    _validate_cert_common(cert)

    c = cert.subject.get_attributes_for_oid(x509.NameOID.COUNTRY_NAME)
    if not c:
        raise InvalidData("Subject must have C set!")
    o = cert.subject.get_attributes_for_oid(x509.NameOID.ORGANIZATION_NAME)
    if not o:
        raise InvalidData("Subject must have O set!")
    ous = cert.subject.get_attributes_for_oid(x509.NameOID.ORGANIZATIONAL_UNIT_NAME)
    if not ous:
        raise InvalidData('Subject must have OU = "Authenticator Attestation"!')

    ou = ous[0]
    if ou.value != "Authenticator Attestation":
        raise InvalidData('Subject must have OU = "Authenticator Attestation"!')
    cn = cert.subject.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
    if not cn:
        raise InvalidData("Subject must have CN set!")

    try:
        ext = cert.extensions.get_extension_for_oid(OID_AAGUID)
        if ext.critical:
            raise InvalidData("AAGUID extension must not be marked as critical")
        ext_aaguid = ext.value.value[2:]
        if ext_aaguid != aaguid:
            raise InvalidData(
                "AAGUID in Authenticator data does not "
                "match attestation certificate!"
            )
    except x509.ExtensionNotFound:
        pass  # If missing, ignore


class PackedAttestation(Attestation):
    FORMAT = "packed"

    @catch_builtins
    def verify(self, statement, auth_data, client_data_hash):
        if "ecdaaKeyId" in statement:
            raise NotImplementedError("ECDAA not implemented")
        alg = statement["alg"]
        x5c = statement.get("x5c")
        if x5c:
            cert = x509.load_der_x509_certificate(x5c[0], default_backend())
            _validate_packed_cert(cert, auth_data.credential_data.aaguid)

            pub_key = CoseKey.for_alg(alg).from_cryptography_key(cert.public_key())
            att_type = AttestationType.BASIC
        else:
            pub_key = CoseKey.parse(auth_data.credential_data.public_key)
            if pub_key.ALGORITHM != alg:
                raise InvalidData("Wrong algorithm of public key!")
            att_type = AttestationType.SELF
        try:
            pub_key.verify(auth_data + client_data_hash, statement["sig"])
            return AttestationResult(att_type, x5c or [])
        except _InvalidSignature:
            raise InvalidSignature()

# Copyright (c) 2020 Yubico AB
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
    catch_builtins,
)
from ..utils import sha256

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.constant_time import bytes_eq


OID_APPLE = x509.ObjectIdentifier("1.2.840.113635.100.8.2")


class AppleAttestation(Attestation):
    FORMAT = "apple"

    @catch_builtins
    def verify(self, statement, auth_data, client_data_hash):
        x5c = statement["x5c"]
        expected_nonce = sha256(auth_data + client_data_hash)
        cert = x509.load_der_x509_certificate(x5c[0], default_backend())
        ext = cert.extensions.get_extension_for_oid(OID_APPLE)
        ext_nonce = ext.value.value[6:]  # Sequence of single element of octet string
        if not bytes_eq(expected_nonce, ext_nonce):
            raise InvalidData("Nonce does not match!")
        return AttestationResult(AttestationType.ANON_CA, x5c)

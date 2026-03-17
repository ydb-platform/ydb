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
    InvalidSignature,
    catch_builtins,
)
from ..cose import ES256

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature as _InvalidSignature


class FidoU2FAttestation(Attestation):
    FORMAT = "fido-u2f"

    @catch_builtins
    def verify(self, statement, auth_data, client_data_hash):
        cd = auth_data.credential_data
        pk = b"\x04" + cd.public_key[-2] + cd.public_key[-3]
        x5c = statement["x5c"]
        FidoU2FAttestation.verify_signature(
            auth_data.rp_id_hash,
            client_data_hash,
            cd.credential_id,
            pk,
            x5c[0],
            statement["sig"],
        )
        return AttestationResult(AttestationType.BASIC, x5c)

    @staticmethod
    def verify_signature(
        app_param, client_param, key_handle, public_key, cert_bytes, signature
    ):
        m = b"\0" + app_param + client_param + key_handle + public_key
        cert = x509.load_der_x509_certificate(cert_bytes, default_backend())
        try:
            ES256.from_cryptography_key(cert.public_key()).verify(m, signature)
        except _InvalidSignature:
            raise InvalidSignature()

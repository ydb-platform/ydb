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
    catch_builtins,
)
from ..cose import CoseKey
from ..utils import sha256, websafe_decode

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.constant_time import bytes_eq

import json


class AndroidSafetynetAttestation(Attestation):
    FORMAT = "android-safetynet"

    def __init__(self, allow_rooted: bool = False):
        self.allow_rooted = allow_rooted

    @catch_builtins
    def verify(self, statement, auth_data, client_data_hash):
        jwt = statement["response"]
        header, payload, sig = (websafe_decode(x) for x in jwt.split(b"."))
        data = json.loads(payload.decode("utf8"))
        if not self.allow_rooted and data["ctsProfileMatch"] is not True:
            raise InvalidData("ctsProfileMatch must be true!")
        expected_nonce = sha256(auth_data + client_data_hash)
        if not bytes_eq(expected_nonce, websafe_decode(data["nonce"])):
            raise InvalidData("Nonce does not match!")

        data = json.loads(header.decode("utf8"))
        x5c = [websafe_decode(x) for x in data["x5c"]]
        cert = x509.load_der_x509_certificate(x5c[0], default_backend())

        cn = cert.subject.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
        if cn[0].value != "attest.android.com":
            raise InvalidData("Certificate not issued to attest.android.com!")

        CoseKey.for_name(data["alg"]).from_cryptography_key(cert.public_key()).verify(
            jwt.rsplit(b".", 1)[0], sig
        )
        return AttestationResult(AttestationType.BASIC, x5c)

# Copyright 2016 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os

from cryptography.hazmat.primitives.asymmetric import rsa
import pytest

from google.auth import _helpers
from google.auth.crypt import _cryptography_rsa
from google.auth.crypt import base

import yatest.common
DATA_DIR = os.path.join(yatest.common.test_source_path(), "data")

# To generate privatekey.pem, privatekey.pub, and public_cert.pem:
#   $ openssl req -new -newkey rsa:1024 -x509 -nodes -out public_cert.pem \
#   >    -keyout privatekey.pem
#   $ openssl rsa -in privatekey.pem -pubout -out privatekey.pub

with open(os.path.join(DATA_DIR, "privatekey.pem"), "rb") as fh:
    PRIVATE_KEY_BYTES = fh.read()
    PKCS1_KEY_BYTES = PRIVATE_KEY_BYTES

with open(os.path.join(DATA_DIR, "privatekey.pub"), "rb") as fh:
    PUBLIC_KEY_BYTES = fh.read()

with open(os.path.join(DATA_DIR, "public_cert.pem"), "rb") as fh:
    PUBLIC_CERT_BYTES = fh.read()

# To generate pem_from_pkcs12.pem and privatekey.p12:
#   $ openssl pkcs12 -export -out privatekey.p12 -inkey privatekey.pem \
#   >    -in public_cert.pem
#   $ openssl pkcs12 -in privatekey.p12 -nocerts -nodes \
#   >   -out pem_from_pkcs12.pem

with open(os.path.join(DATA_DIR, "pem_from_pkcs12.pem"), "rb") as fh:
    PKCS8_KEY_BYTES = fh.read()

with open(os.path.join(DATA_DIR, "privatekey.p12"), "rb") as fh:
    PKCS12_KEY_BYTES = fh.read()

# The service account JSON file can be generated from the Google Cloud Console.
SERVICE_ACCOUNT_JSON_FILE = os.path.join(DATA_DIR, "service_account.json")

with open(SERVICE_ACCOUNT_JSON_FILE, "r") as fh:
    SERVICE_ACCOUNT_INFO = json.load(fh)


class TestRSAVerifier(object):
    def test_verify_success(self):
        to_sign = b"foo"
        signer = _cryptography_rsa.RSASigner.from_string(PRIVATE_KEY_BYTES)
        actual_signature = signer.sign(to_sign)

        verifier = _cryptography_rsa.RSAVerifier.from_string(PUBLIC_KEY_BYTES)
        assert verifier.verify(to_sign, actual_signature)

    def test_verify_unicode_success(self):
        to_sign = u"foo"
        signer = _cryptography_rsa.RSASigner.from_string(PRIVATE_KEY_BYTES)
        actual_signature = signer.sign(to_sign)

        verifier = _cryptography_rsa.RSAVerifier.from_string(PUBLIC_KEY_BYTES)
        assert verifier.verify(to_sign, actual_signature)

    def test_verify_failure(self):
        verifier = _cryptography_rsa.RSAVerifier.from_string(PUBLIC_KEY_BYTES)
        bad_signature1 = b""
        assert not verifier.verify(b"foo", bad_signature1)
        bad_signature2 = b"a"
        assert not verifier.verify(b"foo", bad_signature2)

    def test_from_string_pub_key(self):
        verifier = _cryptography_rsa.RSAVerifier.from_string(PUBLIC_KEY_BYTES)
        assert isinstance(verifier, _cryptography_rsa.RSAVerifier)
        assert isinstance(verifier._pubkey, rsa.RSAPublicKey)

    def test_from_string_pub_key_unicode(self):
        public_key = _helpers.from_bytes(PUBLIC_KEY_BYTES)
        verifier = _cryptography_rsa.RSAVerifier.from_string(public_key)
        assert isinstance(verifier, _cryptography_rsa.RSAVerifier)
        assert isinstance(verifier._pubkey, rsa.RSAPublicKey)

    def test_from_string_pub_cert(self):
        verifier = _cryptography_rsa.RSAVerifier.from_string(PUBLIC_CERT_BYTES)
        assert isinstance(verifier, _cryptography_rsa.RSAVerifier)
        assert isinstance(verifier._pubkey, rsa.RSAPublicKey)

    def test_from_string_pub_cert_unicode(self):
        public_cert = _helpers.from_bytes(PUBLIC_CERT_BYTES)
        verifier = _cryptography_rsa.RSAVerifier.from_string(public_cert)
        assert isinstance(verifier, _cryptography_rsa.RSAVerifier)
        assert isinstance(verifier._pubkey, rsa.RSAPublicKey)


class TestRSASigner(object):
    def test_from_string_pkcs1(self):
        signer = _cryptography_rsa.RSASigner.from_string(PKCS1_KEY_BYTES)
        assert isinstance(signer, _cryptography_rsa.RSASigner)
        assert isinstance(signer._key, rsa.RSAPrivateKey)

    def test_from_string_pkcs1_unicode(self):
        key_bytes = _helpers.from_bytes(PKCS1_KEY_BYTES)
        signer = _cryptography_rsa.RSASigner.from_string(key_bytes)
        assert isinstance(signer, _cryptography_rsa.RSASigner)
        assert isinstance(signer._key, rsa.RSAPrivateKey)

    def test_from_string_pkcs8(self):
        signer = _cryptography_rsa.RSASigner.from_string(PKCS8_KEY_BYTES)
        assert isinstance(signer, _cryptography_rsa.RSASigner)
        assert isinstance(signer._key, rsa.RSAPrivateKey)

    def test_from_string_pkcs8_unicode(self):
        key_bytes = _helpers.from_bytes(PKCS8_KEY_BYTES)
        signer = _cryptography_rsa.RSASigner.from_string(key_bytes)
        assert isinstance(signer, _cryptography_rsa.RSASigner)
        assert isinstance(signer._key, rsa.RSAPrivateKey)

    def test_from_string_pkcs12(self):
        with pytest.raises(ValueError):
            _cryptography_rsa.RSASigner.from_string(PKCS12_KEY_BYTES)

    def test_from_string_bogus_key(self):
        key_bytes = "bogus-key"
        with pytest.raises(ValueError):
            _cryptography_rsa.RSASigner.from_string(key_bytes)

    def test_from_service_account_info(self):
        signer = _cryptography_rsa.RSASigner.from_service_account_info(
            SERVICE_ACCOUNT_INFO
        )

        assert signer.key_id == SERVICE_ACCOUNT_INFO[base._JSON_FILE_PRIVATE_KEY_ID]
        assert isinstance(signer._key, rsa.RSAPrivateKey)

    def test_from_service_account_info_missing_key(self):
        with pytest.raises(ValueError) as excinfo:
            _cryptography_rsa.RSASigner.from_service_account_info({})

        assert excinfo.match(base._JSON_FILE_PRIVATE_KEY)

    def test_from_service_account_file(self):
        signer = _cryptography_rsa.RSASigner.from_service_account_file(
            SERVICE_ACCOUNT_JSON_FILE
        )

        assert signer.key_id == SERVICE_ACCOUNT_INFO[base._JSON_FILE_PRIVATE_KEY_ID]
        assert isinstance(signer._key, rsa.RSAPrivateKey)

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib

from libcloud.utils.py3 import b, hexadigits, base64_decode_string

__all__ = [
    "get_pubkey_openssh_fingerprint",
    "get_pubkey_ssh2_fingerprint",
    "get_pubkey_comment",
]

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    cryptography_available = True
except ImportError:
    cryptography_available = False


def _to_md5_fingerprint(data):
    hashed = hashlib.md5(data).digest()  # nosec

    return ":".join(hexadigits(hashed))


def get_pubkey_openssh_fingerprint(pubkey):
    # We import and export the key to make sure it is in OpenSSH format

    if not cryptography_available:
        raise RuntimeError("cryptography is not available")
    public_key = serialization.load_ssh_public_key(b(pubkey), backend=default_backend())
    pub_openssh = public_key.public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH,
    )[
        7:
    ]  # strip ssh-rsa prefix

    return _to_md5_fingerprint(base64_decode_string(pub_openssh))


def get_pubkey_ssh2_fingerprint(pubkey):
    # This is the format that EC2 shows for public key fingerprints in its
    # KeyPair mgmt API

    if not cryptography_available:
        raise RuntimeError("cryptography is not available")
    public_key = serialization.load_ssh_public_key(b(pubkey), backend=default_backend())
    pub_der = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    return _to_md5_fingerprint(pub_der)


def get_pubkey_comment(pubkey, default=None):
    if pubkey.startswith("ssh-"):
        # This is probably an OpenSSH key

        return pubkey.strip().split(" ", 3)[2]

    if default:
        return default
    raise ValueError("Public key is not in a supported format")

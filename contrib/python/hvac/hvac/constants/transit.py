#!/usr/bin/env python
"""Constants related to the Transit secrets engine."""

import re

ALLOWED_KEY_TYPES = [
    "aes256-gcm96",
    "chacha20-poly1305",
    "ed25519",
    "ecdsa-p256",
    "ecdsa-p384",
    "ecdsa-p521",
    "rsa-2048",
    "rsa-3072",
    "rsa-4096",
]

ALLOWED_EXPORT_KEY_TYPES = [
    "encryption-key",
    "signing-key",
    "hmac-key",
]

ALLOWED_DATA_KEY_TYPES = [
    "plaintext",
    "wrapped",
]

ALLOWED_DATA_KEY_BITS = [128, 256, 512]

ALLOWED_HASH_DATA_ALGORITHMS = [
    "sha2-224",
    "sha2-256",
    "sha2-384",
    "sha2-512",
]

ALLOWED_HASH_DATA_FORMATS = ["hex", "base64"]

ALLOWED_SIGNATURE_ALGORITHMS = [
    "pss",
    "pkcs1v15",
]

ALLOWED_MARSHALING_ALGORITHMS = [
    "asn1",
    "jws",
]

# https://github.com/hashicorp/vault/pull/16549
# Either 'auto', 'hash', '-1', or any nonnegative integer.
ALLOWED_SALT_LENGTHS = re.compile(r"auto|hash|-1|\d+")

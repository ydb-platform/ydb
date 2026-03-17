from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL RC4 API.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

from typing import Optional

from M2Crypto.m2 import rc4_free, rc4_new, rc4_set_key, rc4_update


class RC4(object):
    """Object interface to the stream cipher RC4."""

    rc4_free = rc4_free

    def __init__(self, key: Optional[bytes] = None) -> None:
        self.cipher = rc4_new()
        if key:
            rc4_set_key(self.cipher, key)

    def __del__(self) -> None:
        if getattr(self, 'cipher', None):
            self.rc4_free(self.cipher)

    def set_key(self, key: bytes) -> None:
        rc4_set_key(self.cipher, key)

    def update(self, data: bytes) -> bytes:
        return rc4_update(self.cipher, data)

    def final(self) -> str:
        return ''

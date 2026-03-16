from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL RC4 API.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

from M2Crypto.m2 import rc4_free, rc4_new, rc4_set_key, rc4_update


class RC4(object):
    """Object interface to the stream cipher RC4."""

    rc4_free = rc4_free

    def __init__(self, key=None):
        # type: (bytes) -> None
        self.cipher = rc4_new()
        if key:
            rc4_set_key(self.cipher, key)

    def __del__(self):
        # type: () -> None
        if getattr(self, 'cipher', None):
            self.rc4_free(self.cipher)

    def set_key(self, key):
        # type: (bytes) -> None
        rc4_set_key(self.cipher, key)

    def update(self, data):
        # type: (bytes) -> bytes
        return rc4_update(self.cipher, data)

    def final(self):
        # type: () -> str
        return ''

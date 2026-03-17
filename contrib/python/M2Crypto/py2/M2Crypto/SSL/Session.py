"""SSL Session

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

__all__ = ['Session', 'load_session']

from M2Crypto import BIO, Err, m2
from M2Crypto.SSL import SSLError
from typing import AnyStr  # noqa


class Session(object):

    m2_ssl_session_free = m2.ssl_session_free

    def __init__(self, session, _pyfree=0):
        # type: (bytes, int) -> None
        assert session is not None
        self.session = session
        self._pyfree = _pyfree

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_ssl_session_free(self.session)

    def _ptr(self):
        # type: () -> bytes
        return self.session

    def as_text(self):
        # type: () -> bytes
        buf = BIO.MemoryBuffer()
        m2.ssl_session_print(buf.bio_ptr(), self.session)
        return buf.read_all()

    def as_der(self):
        # type: () -> bytes
        buf = BIO.MemoryBuffer()
        m2.i2d_ssl_session(buf.bio_ptr(), self.session)
        return buf.read_all()

    def write_bio(self, bio):
        # type: (BIO.BIO) -> int
        return m2.ssl_session_write_bio(bio.bio_ptr(), self.session)

    def get_time(self):
        # type: () -> int
        return m2.ssl_session_get_time(self.session)

    def set_time(self, t):
        # type: (int) -> int
        return m2.ssl_session_set_time(self.session, t)

    def get_timeout(self):
        # type: () -> int
        return m2.ssl_session_get_timeout(self.session)

    def set_timeout(self, t):
        # type: (int) -> int
        return m2.ssl_session_set_timeout(self.session, t)


def load_session(pemfile):
    # type: (AnyStr) -> Session
    with BIO.openfile(pemfile) as f:
        cptr = m2.ssl_session_read_pem(f.bio_ptr())

    return Session(cptr, 1)

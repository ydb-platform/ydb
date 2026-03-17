from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL BIO API.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved."""

import io
import logging
from typing import Any, AnyStr, Callable, Iterable, Optional, Union  # noqa

from M2Crypto import m2, six

log = logging.getLogger('BIO')


class BIOError(ValueError):
    pass


m2.bio_init(BIOError)


class BIO(object):
    """Abstract object interface to the BIO API."""

    m2_bio_free = m2.bio_free

    def __init__(self, bio=None, _pyfree=0, _close_cb=None):
        # type: (Optional[BIO], int, Optional[Callable]) -> None
        self.bio = bio
        self._pyfree = _pyfree
        self._close_cb = _close_cb
        self.closed = 0
        self.write_closed = 0

    def __del__(self):
        if self._pyfree:
            self.m2_bio_free(self.bio)

    def _ptr(self):
        return self.bio

    # Deprecated.
    bio_ptr = _ptr

    def fileno(self):
        # type: () -> int
        return m2.bio_get_fd(self.bio)

    def readable(self):
        # type: () -> bool
        return not self.closed

    def read(self, size=None):
        # type: (int) -> Union[bytes, bytearray]
        if not self.readable():
            raise IOError('cannot read')
        if size is None:
            buf = bytearray()
            while 1:
                data = m2.bio_read(self.bio, 4096)
                if not data:
                    break
                buf += data
            return buf
        elif size == 0:
            return b''
        elif size < 0:
            raise ValueError('read count is negative')
        else:
            return bytes(m2.bio_read(self.bio, size))

    def readline(self, size=4096):
        # type: (int) -> bytes
        if not self.readable():
            raise IOError('cannot read')
        buf = m2.bio_gets(self.bio, size)
        buf = '' if buf is None else buf
        return six.ensure_binary(buf)

    def readlines(self, sizehint='ignored'):
        # type: (Union[AnyStr, int]) -> Iterable[bytes]
        if not self.readable():
            raise IOError('cannot read')
        lines = []
        while 1:
            buf = m2.bio_gets(self.bio, 4096)
            if buf is None:
                break
            lines.append(six.ensure_binary(buf))
        return lines

    def writeable(self):
        # type: () -> bool
        return (not self.closed) and (not self.write_closed)

    def write(self, data):
        # type: (AnyStr) -> int
        """Write data to BIO.

        :return: either data written, or [0, -1] for nothing written,
                 -2 not implemented
        """
        if not self.writeable():
            raise IOError('cannot write')
        if isinstance(data, six.text_type):
            data = data.encode('utf8')
        return m2.bio_write(self.bio, data)

    def write_close(self):
        # type: () -> None
        self.write_closed = 1

    def flush(self):
        # type: () -> None
        """Flush the buffers.

        :return: 1 for success, and 0 or -1 for failure
        """
        m2.bio_flush(self.bio)

    def reset(self):
        # type: () -> int
        """Set the bio to its initial state.

        :return: 1 for success, and 0 or -1 for failure
        """
        return m2.bio_reset(self.bio)

    def close(self):
        # type: () -> None
        self.closed = 1
        if self._close_cb:
            self._close_cb()

    def should_retry(self):
        # type: () -> int
        """
        Can the call be attempted again, or was there an error
        ie do_handshake

        """
        return m2.bio_should_retry(self.bio)

    def should_read(self):
        # type: () -> int
        """Should we read more data?"""

        return m2.bio_should_read(self.bio)

    def should_write(self):
        # type: () -> int
        """Should we write more data?"""
        return m2.bio_should_write(self.bio)

    def tell(self):
        """Return the current offset."""
        return m2.bio_tell(self.bio)

    def seek(self, off):
        """Seek to the specified absolute offset."""
        return m2.bio_seek(self.bio, off)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        # type: (*Any) -> int
        self.close()


class MemoryBuffer(BIO):
    """Object interface to BIO_s_mem.

    Empirical testing suggests that this class performs less well than
    cStringIO, because cStringIO is implemented in C, whereas this class
    is implemented in Python. Thus, the recommended practice is to use
    cStringIO for regular work and convert said cStringIO object to
    a MemoryBuffer object only when necessary.
    """

    def __init__(self, data=None):
        # type: (Optional[bytes]) -> None
        super(MemoryBuffer, self).__init__(self)
        if data is not None and not isinstance(data, bytes):
            raise TypeError(
                "data must be bytes or None, not %s" % (type(data).__name__, ))
        self.bio = m2.bio_new(m2.bio_s_mem())
        self._pyfree = 1
        if data is not None:
            m2.bio_write(self.bio, data)

    def __len__(self):
        # type: () -> int
        return m2.bio_ctrl_pending(self.bio)

    def read(self, size=0):
        # type: (int) -> bytes
        m2.err_clear_error();
        if not self.readable():
            raise IOError('cannot read')
        if size:
            return m2.bio_read(self.bio, size)
        else:
            return m2.bio_read(self.bio, m2.bio_ctrl_pending(self.bio))

    # Backwards-compatibility.
    getvalue = read_all = read

    def write_close(self):
        # type: () -> None
        super(MemoryBuffer, self).write_close()
        m2.bio_set_mem_eof_return(self.bio, 0)

    close = write_close


class File(BIO):
    """Object interface to BIO_s_pyfd.

    This class interfaces Python to OpenSSL functions that expect BIO. For
    general file manipulation in Python, use Python's builtin file object.
    """

    def __init__(self, pyfile, close_pyfile=1, mode='rb'):
        # type: (Union[io.BytesIO, AnyStr], int, AnyStr) -> None
        super(File, self).__init__(self, _pyfree=1)

        if isinstance(pyfile, six.string_types):
            pyfile = open(pyfile, mode)

        # This is for downward compatibility, but I don't think, that it is
        # good practice to have two handles for the same file. Whats about
        # concurrent write access? Last write, last wins? Especially since Py3
        # has its own buffer management. See:
        #
        #  https://docs.python.org/3.3/c-api/file.html
        #
        pyfile.flush()
        self.fname = pyfile.name
        self.pyfile = pyfile
        # Be wary of https://github.com/openssl/openssl/pull/1925
        # BIO_new_fd is NEVER to be used before OpenSSL 1.1.1
        if hasattr(m2, "bio_new_pyfd"):
            self.bio = m2.bio_new_pyfd(pyfile.fileno(), m2.bio_noclose)
        else:
            self.bio = m2.bio_new_pyfile(pyfile, m2.bio_noclose)

        self.close_pyfile = close_pyfile
        self.closed = False

    def flush(self):
        # type: () -> None
        super(File, self).flush()
        self.pyfile.flush()

    def close(self):
        # type: () -> None
        self.flush()
        super(File, self).close()
        if self.close_pyfile:
            self.pyfile.close()

    def reset(self):
        # type: () -> int
        """Set the bio to its initial state.

        :return: 0 for success, and -1 for failure
        """
        return super(File, self).reset()

def openfile(filename, mode='rb'):
    # type: (AnyStr, AnyStr) -> File
    try:
        f = open(filename, mode)
    except IOError as ex:
        raise BIOError(ex.args)

    return File(f)


class IOBuffer(BIO):
    """Object interface to BIO_f_buffer.

    Its principal function is to be BIO_push()'ed on top of a BIO_f_ssl, so
    that makefile() of said underlying SSL socket works.
    """

    m2_bio_pop = m2.bio_pop
    m2_bio_free = m2.bio_free

    def __init__(self, under_bio, mode='rwb', _pyfree=1):
        # type: (BIO, str, int) -> None
        super(IOBuffer, self).__init__(self, _pyfree=_pyfree)
        self.io = m2.bio_new(m2.bio_f_buffer())
        self.bio = m2.bio_push(self.io, under_bio._ptr())
        # This reference keeps the underlying BIO alive while we're not closed.
        self._under_bio = under_bio
        if 'w' in mode:
            self.write_closed = 0
        else:
            self.write_closed = 1

    def __del__(self):
        # type: () -> None
        if getattr(self, '_pyfree', 0):
            self.m2_bio_pop(self.bio)
        self.m2_bio_free(self.io)

    def close(self):
        # type: () -> None
        BIO.close(self)


class CipherStream(BIO):
    """Object interface to BIO_f_cipher."""

    SALT_LEN = m2.PKCS5_SALT_LEN

    m2_bio_pop = m2.bio_pop
    m2_bio_free = m2.bio_free

    def __init__(self, obio):
        # type: (BIO) -> None
        super(CipherStream, self).__init__(self, _pyfree=1)
        self.obio = obio
        self.bio = m2.bio_new(m2.bio_f_cipher())
        self.closed = 0

    def __del__(self):
        # type: () -> None
        if not getattr(self, 'closed', 1):
            self.close()

    def close(self):
        # type: () -> None
        self.m2_bio_pop(self.bio)
        self.m2_bio_free(self.bio)
        self.closed = 1

    def write_close(self):
        # type: () -> None
        self.obio.write_close()

    def set_cipher(self, algo, key, iv, op):
        # type: (str, AnyStr, AnyStr, int) -> None
        cipher = getattr(m2, algo, None)
        if cipher is None:
            raise ValueError('unknown cipher', algo)
        else:
            if not isinstance(key, bytes):
                key = key.encode('utf8')
            if not isinstance(iv, bytes):
                iv = iv.encode('utf8')
        m2.bio_set_cipher(self.bio, cipher(), key, iv, int(op))
        m2.bio_push(self.bio, self.obio._ptr())


class SSLBio(BIO):
    """Object interface to BIO_f_ssl."""

    def __init__(self, _pyfree=1):
        # type: (int) -> None
        super(SSLBio, self).__init__(self, _pyfree=_pyfree)
        self.bio = m2.bio_new(m2.bio_f_ssl())
        self.closed = 0

    def set_ssl(self, conn, close_flag=m2.bio_noclose):
        ## type: (Connection, int) -> None
        """
        Sets the bio to the SSL pointer which is
        contained in the connection object.
        """
        self._pyfree = 0
        m2.bio_set_ssl(self.bio, conn.ssl, close_flag)
        if close_flag == m2.bio_noclose:
            conn.set_ssl_close_flag(m2.bio_close)

    def do_handshake(self):
        # type: () -> int
        """Do the handshake.

        Return 1 if the handshake completes
        Return 0 or a negative number if there is a problem
        """
        return m2.bio_do_handshake(self.bio)

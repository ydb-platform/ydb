# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading from http."""

import io
import logging
import os.path
import urllib.parse

try:
    import requests
except ImportError:
    MISSING_DEPS = True

from smart_open import bytebuffer, constants
import smart_open.utils

DEFAULT_BUFFER_SIZE = 128 * 1024
SCHEMES = ('http', 'https')

logger = logging.getLogger(__name__)


_HEADERS = {'Accept-Encoding': 'identity'}
"""The headers we send to the server with every HTTP request.

For now, we ask the server to send us the files as they are.
Sometimes, servers compress the file for more efficient transfer, in which case
the client (us) has to decompress them with the appropriate algorithm.
"""


def parse_uri(uri_as_string):
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES

    uri_path = split_uri.netloc + split_uri.path
    uri_path = "/" + uri_path.lstrip("/")
    return dict(scheme=split_uri.scheme, uri_path=uri_path)


def open_uri(uri, mode, transport_params):
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(uri, mode, **kwargs)


def open(uri, mode, kerberos=False, user=None, password=None, cert=None,
         headers=None, timeout=None, session=None, buffer_size=DEFAULT_BUFFER_SIZE):
    """Implement streamed reader from a web site.

    Supports Kerberos and Basic HTTP authentication.

    Parameters
    ----------
    url: str
        The URL to open.
    mode: str
        The mode to open using.
    kerberos: boolean, optional
        If True, will attempt to use the local Kerberos credentials
    user: str, optional
        The username for authenticating over HTTP
    password: str, optional
        The password for authenticating over HTTP
    cert: str/tuple, optional
        if String, path to ssl client cert file (.pem). If Tuple, (‘cert’, ‘key’)
    headers: dict, optional
        Any headers to send in the request. If ``None``, the default headers are sent:
        ``{'Accept-Encoding': 'identity'}``. To use no headers at all,
        set this variable to an empty dict, ``{}``.
    session: object, optional
        The requests Session object to use with http get requests.
        Can be used for OAuth2 clients.
    buffer_size: int, optional
        The buffer size to use when performing I/O.

    Note
    ----
    If neither kerberos or (user, password) are set, will connect
    unauthenticated, unless set separately in headers.

    """
    if mode == constants.READ_BINARY:
        fobj = SeekableBufferedInputBase(
            uri, mode, buffer_size=buffer_size, kerberos=kerberos,
            user=user, password=password, cert=cert,
            headers=headers, session=session, timeout=timeout,
        )
        fobj.name = os.path.basename(urllib.parse.urlparse(uri).path)
        return fobj
    else:
        raise NotImplementedError('http support for mode %r not implemented' % mode)


class BufferedInputBase(io.BufferedIOBase):
    response = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(self, url, mode='r', buffer_size=DEFAULT_BUFFER_SIZE,
                 kerberos=False, user=None, password=None, cert=None,
                 headers=None, session=None, timeout=None):

        self.url = url
        self.cert = cert
        self.session = session or requests

        if kerberos:
            import requests_kerberos
            self.auth = requests_kerberos.HTTPKerberosAuth()
        elif user is not None and password is not None:
            self.auth = (user, password)
        else:
            self.auth = None

        self.buffer_size = buffer_size
        self.mode = mode

        if headers is None:
            self.headers = _HEADERS.copy()
        else:
            self.headers = headers

        self.timeout = timeout

        self.response = self.session.get(
            self.url,
            auth=self.auth,
            cert=self.cert,
            stream=True,
            headers=self.headers,
            timeout=self.timeout,
        )

        if not self.response.ok:
            self.response.raise_for_status()

        self._read_buffer = bytebuffer.ByteBuffer(buffer_size)
        self._current_pos = 0

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        if not self.closed:
            self.response = None
            self._read_buffer = None

    @property
    def closed(self):
        return self.response is None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        return False

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """
        Mimics the read call to a filehandle object.
        """
        if size < -1:
            raise ValueError(f'size must be >= -1, got {size}')

        logger.debug("reading with size: %d", size)
        if self.closed or size == 0:
            return b""

        if size == -1:
            if len(self._read_buffer):
                retval = self._read_buffer.read() + self.response.raw.read()
            else:  # Avoid unnecessary +
                retval = self.response.raw.read()
        else:
            # Fill _read_buffer until it contains enough bytes
            while len(self._read_buffer) < size:
                if self._read_buffer.fill(self.response.raw) == 0:
                    break  # EOF reached
            retval = self._read_buffer.read(size)

        self._current_pos += len(retval)
        return retval

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[:len(data)] = data
        return len(data)


class SeekableBufferedInputBase(BufferedInputBase):
    """
    Implement seekable streamed reader from a web site.
    Supports Kerberos, client certificate and Basic HTTP authentication.
    """

    def __init__(self, url, mode='r', buffer_size=DEFAULT_BUFFER_SIZE,
                 kerberos=False, user=None, password=None, cert=None,
                 headers=None, session=None, timeout=None):
        """
        If Kerberos is True, will attempt to use the local Kerberos credentials.
        If cert is set, will try to use a client certificate
        Otherwise, will try to use "basic" HTTP authentication via username/password.

        If none of those are set, will connect unauthenticated.
        """
        super().__init__(url, mode, buffer_size, kerberos, user, password, cert, headers, session, timeout)
        self.content_length = int(self.response.headers.get("Content-Length", -1))
        #
        # We assume the HTTP stream is seekable unless the server explicitly
        # tells us it isn't.  It's better to err on the side of "seekable"
        # because we don't want to prevent users from seeking a stream that
        # does not appear to be seekable but really is.
        #
        self._seekable = self.response.headers.get("Accept-Ranges", "").lower() != "none"

    def seek(self, offset, whence=0):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        logger.debug('seeking to offset: %r whence: %r', offset, whence)
        if whence not in constants.WHENCE_CHOICES:
            raise ValueError('invalid whence, expected one of %r' % constants.WHENCE_CHOICES)

        if not self.seekable():
            raise OSError('stream is not seekable')

        if whence == constants.WHENCE_START:
            new_pos = offset
        elif whence == constants.WHENCE_CURRENT:
            new_pos = self._current_pos + offset
        elif whence == constants.WHENCE_END:
            new_pos = self.content_length + offset

        if self.content_length == -1:
            new_pos = smart_open.utils.clamp(new_pos, maxval=None)
        else:
            new_pos = smart_open.utils.clamp(new_pos, maxval=self.content_length)

        if self._current_pos == new_pos:
            return self._current_pos

        logger.debug("http seeking from current_pos: %d to new_pos: %d", self._current_pos, new_pos)

        self._current_pos = new_pos

        if new_pos == self.content_length:
            self.response = None
            self._read_buffer.empty()
        else:
            response = self._partial_request(new_pos)
            if response.ok:
                self.response = response
                self._read_buffer.empty()
            else:
                self.response = None

        return self._current_pos

    def tell(self):
        return self._current_pos

    def seekable(self, *args, **kwargs):
        return self._seekable

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def _partial_request(self, start_pos=None):
        headers = self.headers.copy()
        if start_pos is not None:
            headers["range"] = smart_open.utils.make_range_string(start_pos)

        response = self.session.get(
            self.url,
            auth=self.auth,
            stream=True,
            cert=self.cert,
            headers=headers,
            timeout=self.timeout,
        )
        return response

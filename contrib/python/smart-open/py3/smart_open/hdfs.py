# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements reading and writing to/from HDFS."""

import io
import logging
import subprocess
import urllib.parse

from smart_open import utils

logger = logging.getLogger(__name__)

SCHEMES = ('hdfs', 'viewfs')

URI_EXAMPLES = (
    'hdfs:///path/file',
    'hdfs://path/file',
    'viewfs:///path/file',
    'viewfs://path/file',
)


def parse_uri(uri_as_string):
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES

    uri_path = split_uri.netloc + split_uri.path
    uri_path = "/" + uri_path.lstrip("/")
    if not uri_path:
        raise RuntimeError("invalid HDFS URI: %r" % uri_as_string)

    return dict(scheme=split_uri.scheme, uri_path=uri_path)


def open_uri(uri, mode, transport_params):
    utils.check_kwargs(open, transport_params)

    parsed_uri = parse_uri(uri)
    fobj = open(parsed_uri['uri_path'], mode)
    fobj.name = parsed_uri['uri_path'].split('/')[-1]
    return fobj


def open(uri, mode):
    if mode == 'rb':
        return CliRawInputBase(uri)
    elif mode == 'wb':
        return CliRawOutputBase(uri)
    else:
        raise NotImplementedError('hdfs support for mode %r not implemented' % mode)


class CliRawInputBase(io.RawIOBase):
    """Reads bytes from HDFS via the "hdfs dfs" command-line interface.

    Implements the io.RawIOBase interface of the standard library.
    """
    _sub = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(self, uri):
        self._uri = uri
        self._sub = subprocess.Popen(["hdfs", "dfs", '-cat', self._uri], stdout=subprocess.PIPE)

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        if not self.closed:
            self._sub.terminate()
            self._sub = None

    @property
    def closed(self):
        return self._sub is None

    def readable(self):
        """Return True if the stream can be read from."""
        return self._sub is not None

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError."""
        return False

    #
    # io.RawIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        return self._sub.stdout.read(size)

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


class CliRawOutputBase(io.RawIOBase):
    """Writes bytes to HDFS via the "hdfs dfs" command-line interface.

    Implements the io.RawIOBase interface of the standard library.
    """
    _sub = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(self, uri):
        self._uri = uri
        self._sub = subprocess.Popen(["hdfs", "dfs", '-put', '-f', '-', self._uri],
                                     stdin=subprocess.PIPE)

    def close(self):
        logger.debug("close: called")
        if not self.closed:
            self.flush()
            self._sub.stdin.close()
            self._sub.wait()
            self._sub = None

    @property
    def closed(self):
        return self._sub is None

    def flush(self):
        self._sub.stdin.flush()

    def writeable(self):
        """Return True if this object is writeable."""
        return self._sub is not None

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError."""
        return False

    def write(self, b):
        self._sub.stdin.write(b)

    #
    # io.IOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements the majority of smart_open's top-level API."""

import collections
import locale
import logging
import os
import os.path as P
import pathlib
import urllib.parse
import warnings

#
# This module defines a function called smart_open so we cannot use
# smart_open.submodule to reference to the submodules.
#
import smart_open.local_file as so_file
import smart_open.compression as so_compression
import smart_open.utils as so_utils

from smart_open import doctools
from smart_open import transport

#
# For backwards compatibility and keeping old unit tests happy.
#
from smart_open.compression import register_compressor  # noqa: F401
from smart_open.utils import check_kwargs as _check_kwargs  # noqa: F401
from smart_open.utils import inspect_kwargs as _inspect_kwargs  # noqa: F401

logger = logging.getLogger(__name__)

DEFAULT_ENCODING = locale.getpreferredencoding(do_setlocale=False)


def _sniff_scheme(uri_as_string):
    """Returns the scheme of the URL only, as a string."""
    #
    # urlsplit doesn't work on Windows -- it parses the drive as the scheme...
    # no protocol given => assume a local file
    #
    if os.name == 'nt' and '://' not in uri_as_string:
        uri_as_string = 'file://' + uri_as_string

    return urllib.parse.urlsplit(uri_as_string).scheme


def parse_uri(uri_as_string):
    """
    Parse the given URI from a string.

    Parameters
    ----------
    uri_as_string: str
        The URI to parse.

    Returns
    -------
    collections.namedtuple
        The parsed URI.

    Notes
    -----
    smart_open/doctools.py magic goes here
    """
    scheme = _sniff_scheme(uri_as_string)
    submodule = transport.get_transport(scheme)
    as_dict = submodule.parse_uri(uri_as_string)

    #
    # The conversion to a namedtuple is just to keep the old tests happy while
    # I'm still refactoring.
    #
    Uri = collections.namedtuple('Uri', sorted(as_dict.keys()))
    return Uri(**as_dict)


#
# To keep old unit tests happy while I'm refactoring.
#
_parse_uri = parse_uri

_builtin_open = open


def open(
        uri,
        mode='r',
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
        compression=so_compression.INFER_FROM_EXTENSION,
        transport_params=None,
        ):
    r"""Open the URI object, returning a file-like object.

    The URI is usually a string in a variety of formats.
    For a full list of examples, see the :func:`parse_uri` function.

    The URI may also be one of:

    - an instance of the pathlib.Path class
    - a stream (anything that implements io.IOBase-like functionality)

    Parameters
    ----------
    uri: str or object
        The object to open.
    mode: str, optional
        Mimicks built-in open parameter of the same name.
    buffering: int, optional
        Mimicks built-in open parameter of the same name.
    encoding: str, optional
        Mimicks built-in open parameter of the same name.
    errors: str, optional
        Mimicks built-in open parameter of the same name.
    newline: str, optional
        Mimicks built-in open parameter of the same name.
    closefd: boolean, optional
        Mimicks built-in open parameter of the same name.  Ignored.
    opener: object, optional
        Mimicks built-in open parameter of the same name.  Ignored.
    compression: str, optional (see smart_open.compression.get_supported_compression_types)
        Explicitly specify the compression/decompression behavior.
    transport_params: dict, optional
        Additional parameters for the transport layer (see notes below).

    Returns
    -------
    A file-like object.

    Notes
    -----
    smart_open has several implementations for its transport layer (e.g. S3, HTTP).
    Each transport layer has a different set of keyword arguments for overriding
    default behavior.  If you specify a keyword argument that is *not* supported
    by the transport layer being used, smart_open will ignore that argument and
    log a warning message.

    smart_open/doctools.py magic goes here

    See Also
    --------
    - `Standard library reference <https://docs.python.org/3.14/library/functions.html#open>`__
    - `smart_open README.rst
      <https://github.com/piskvorky/smart_open/blob/master/README.rst>`__

    """
    logger.debug('%r', locals())

    if not isinstance(mode, str):
        raise TypeError('mode should be a string')

    if compression not in so_compression.get_supported_compression_types():
        raise ValueError(f'invalid compression type: {compression}')

    if transport_params is None:
        transport_params = {}

    fobj = _shortcut_open(
        uri,
        mode,
        compression=compression,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    )
    if fobj is not None:
        return fobj

    #
    # This is a work-around for the problem described in Issue #144.
    # If the user has explicitly specified an encoding, then assume they want
    # us to open the destination in text mode, instead of the default binary.
    #
    # If we change the default mode to be text, and match the normal behavior
    # of Py2 and 3, then the above assumption will be unnecessary.
    #
    if encoding is not None and 'b' in mode:
        mode = mode.replace('b', '')

    if isinstance(uri, pathlib.Path):
        uri = str(uri)

    explicit_encoding = encoding
    encoding = explicit_encoding if explicit_encoding else DEFAULT_ENCODING

    #
    # This is how we get from the filename to the end result.  Decompression is
    # optional, but it always accepts bytes and returns bytes.
    #
    # Decoding is also optional, accepts bytes and returns text.  The diagram
    # below is for reading, for writing, the flow is from right to left, but
    # the code is identical.
    #
    #           open as binary         decompress?          decode?
    # filename ---------------> bytes -------------> bytes ---------> text
    #                          binary             decompressed       decode
    #

    try:
        binary_mode = _get_binary_mode(mode)
    except ValueError as ve:
        raise NotImplementedError(ve.args[0])

    binary = _open_binary_stream(uri, binary_mode, transport_params)
    filename = (
        binary.name
        # if name attribute is not string-like (e.g. ftp socket fileno)...
        if isinstance(getattr(binary, "name", None), (str, bytes))
        # ...fall back to uri
        else uri
    )
    decompressed = so_compression.compression_wrapper(
        binary,
        binary_mode,
        compression,
        filename=filename,
    )

    if 'b' not in mode or explicit_encoding is not None:
        decoded = _encoding_wrapper(
            decompressed,
            mode,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
    else:
        decoded = decompressed

    #
    # There are some useful methods in the binary readers, e.g. to_boto3, that get
    # hidden by the multiple layers of wrapping we just performed.  Promote
    # them so they are visible to the user.
    #
    if decoded != binary:
        promoted_attrs = ['to_boto3']
        for attr in promoted_attrs:
            try:
                setattr(decoded, attr, getattr(binary, attr))
            except AttributeError:
                pass

    return so_utils.FileLikeProxy(decoded, binary)


def _get_binary_mode(mode_str):
    #
    # https://docs.python.org/3/library/functions.html#open
    #
    # The order of characters in the mode parameter appears to be unspecified.
    # The implementation follows the examples, just to be safe.
    #
    mode = list(mode_str)
    binmode = []

    if 't' in mode and 'b' in mode:
        raise ValueError("can't have text and binary mode at once")

    counts = [mode.count(x) for x in 'rwa']
    if sum(counts) > 1:
        raise ValueError("must have exactly one of create/read/write/append mode")

    def transfer(char):
        binmode.append(mode.pop(mode.index(char)))

    if 'a' in mode:
        transfer('a')
    elif 'w' in mode:
        transfer('w')
    elif 'r' in mode:
        transfer('r')
    else:
        raise ValueError(
            "Must have exactly one of create/read/write/append "
            "mode and at most one plus"
        )

    if 'b' in mode:
        transfer('b')
    elif 't' in mode:
        mode.pop(mode.index('t'))
        binmode.append('b')
    else:
        binmode.append('b')

    if '+' in mode:
        transfer('+')

    #
    # There shouldn't be anything left in the mode list at this stage.
    # If there is, then either we've missed something and the implementation
    # of this function is broken, or the original input mode is invalid.
    #
    if mode:
        raise ValueError('invalid mode: %r' % mode_str)

    return ''.join(binmode)


def _shortcut_open(
        uri,
        mode,
        compression,
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        ):
    """Try to open the URI using the standard library io.open function.

    This can be much faster than the alternative of opening in binary mode and
    then decoding.

    This is only possible under the following conditions:

        1. Opening a local file; and
        2. Compression is disabled

    If it is not possible to use the built-in open for the specified URI, returns None.

    :param str uri: A string indicating what to open.
    :param str mode: The mode to pass to the open function.
    :param str compression: The compression type selected.
    :returns: The opened file
    :rtype: file
    """
    if not isinstance(uri, str):
        return None

    scheme = _sniff_scheme(uri)
    if scheme not in (transport.NO_SCHEME, so_file.SCHEME):
        return None

    local_path = so_file.extract_local_path(uri)
    if compression == so_compression.INFER_FROM_EXTENSION:
        _, extension = P.splitext(local_path)
        if extension in so_compression.get_supported_extensions():
            return None
    elif compression != so_compression.NO_COMPRESSION:
        return None

    open_kwargs = {}
    if encoding is not None:
        open_kwargs['encoding'] = encoding
        mode = mode.replace('b', '')
    if newline is not None:
        open_kwargs['newline'] = newline

    #
    # binary mode of the builtin/stdlib open function doesn't take an errors argument
    #
    if errors and 'b' not in mode:
        open_kwargs['errors'] = errors

    return _builtin_open(local_path, mode, buffering=buffering, **open_kwargs)


def _open_binary_stream(uri, mode, transport_params):
    """Open an arbitrary URI in the specified binary mode.

    Not all modes are supported for all protocols.

    :arg uri: The URI to open.  May be a string, or something else.
    :arg str mode: The mode to open with.  Must be rb, wb or ab.
    :arg transport_params: Keyword argumens for the transport layer.
    :returns: A named file object
    :rtype: file-like object with a .name attribute
    """
    if mode not in ('rb', 'rb+', 'wb', 'wb+', 'ab', 'ab+'):
        #
        # This should really be a ValueError, but for the sake of compatibility
        # with older versions, which raise NotImplementedError, we do the same.
        #
        raise NotImplementedError('unsupported mode: %r' % mode)

    if isinstance(uri, int):
        #
        # We're working with a file descriptor.  If we open it, its name is
        # just the integer value, which isn't helpful.  Unfortunately, there's
        # no easy cross-platform way to go from a file descriptor to the filename,
        # so we just give up here.  The user will have to handle their own
        # compression, etc. explicitly.
        #
        fobj = _builtin_open(uri, mode, closefd=False)
        return fobj

    if not isinstance(uri, str):
        raise TypeError("don't know how to handle uri %s" % repr(uri))

    scheme = _sniff_scheme(uri)
    submodule = transport.get_transport(scheme)
    fobj = submodule.open_uri(uri, mode, transport_params)
    if not hasattr(fobj, 'name'):
        fobj.name = uri

    return fobj


def _encoding_wrapper(fileobj, mode, encoding=None, errors=None, newline=None):
    """Decode bytes into text, if necessary.

    If mode specifies binary access, does nothing, unless the encoding is
    specified.  A non-null encoding implies text mode.

    :arg fileobj: must quack like a filehandle object.
    :arg str mode: is the mode which was originally requested by the user.
    :arg str encoding: The text encoding to use.  If mode is binary, overrides mode.
    :arg str errors: The method to use when handling encoding/decoding errors.
    :returns: a file object
    """
    logger.debug('encoding_wrapper: %r', locals())

    #
    # If the mode is binary, but the user specified an encoding, assume they
    # want text.  If we don't make this assumption, ignore the encoding and
    # return bytes, smart_open behavior will diverge from the built-in open:
    #
    #   open(filename, encoding='utf-8') returns a text stream in Py3
    #   smart_open(filename, encoding='utf-8') would return a byte stream
    #       without our assumption, because the default mode is rb.
    #
    if 'b' in mode and encoding is None:
        return fileobj

    if encoding is None:
        encoding = DEFAULT_ENCODING

    fileobj = so_utils.TextIOWrapper(
        fileobj,
        encoding=encoding,
        errors=errors,
        newline=newline,
        write_through=True,
    )
    return fileobj


class patch_pathlib(object):
    """Replace `Path.open` with `smart_open.open`"""

    def __init__(self):
        self.old_impl = _patch_pathlib(open)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _patch_pathlib(self.old_impl)


def _patch_pathlib(func):
    """Replace `Path.open` with `func`"""
    old_impl = pathlib.Path.open
    pathlib.Path.open = func
    return old_impl


def smart_open(
        uri,
        mode='rb',
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
        ignore_extension=False,
        **kwargs
    ):
    #
    # This is a thin wrapper of smart_open.open.  It's here for backward
    # compatibility.  It works exactly like smart_open.open when the passed
    # parameters are identical.  Otherwise, it raises a DeprecationWarning.
    #
    # For completeness, the main differences of the old smart_open function:
    #
    # 1. Default mode was read binary (mode='rb')
    # 2. compression parameter was called ignore_extension
    # 3. Transport parameters were passed directly as kwargs
    #
    url = 'https://github.com/piskvorky/smart_open/blob/develop/MIGRATING_FROM_OLDER_VERSIONS.rst'
    if kwargs:
        raise DeprecationWarning(
            'The following keyword parameters are not supported: %r. '
            'See  %s for more information.' % (sorted(kwargs), url)
        )
    message = 'This function is deprecated.  See %s for more information' % url
    warnings.warn(message, category=DeprecationWarning)

    if ignore_extension:
        compression = so_compression.NO_COMPRESSION
    else:
        compression = so_compression.INFER_FROM_EXTENSION
    del kwargs, url, message, ignore_extension
    return open(**locals())


#
# Prevent failures with doctools from messing up the entire library.  We don't
# expect such failures, but contributed modules (e.g. new transport mechanisms)
# may not be as polished.
#
try:
    doctools.tweak_open_docstring(open)
    doctools.tweak_parse_uri_docstring(parse_uri)
except Exception as ex:
    logger.error(
        'Encountered a non-fatal error while building docstrings (see below). '
        'help(smart_open) will provide incomplete information as a result. '
        'For full help text, see '
        '<https://github.com/piskvorky/smart_open/blob/master/help.txt>.'
    )
    logger.exception(ex)

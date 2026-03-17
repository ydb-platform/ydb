# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements the compression layer of the `smart_open` library."""
import io
import logging
import os.path

logger = logging.getLogger(__name__)

_COMPRESSOR_REGISTRY = {}

NO_COMPRESSION = 'disable'
"""Use no compression. Read/write the data as-is."""
INFER_FROM_EXTENSION = 'infer_from_extension'
"""Determine the compression to use from the file extension.

See get_supported_extensions().
"""


def get_supported_compression_types():
    """Return the list of supported compression types available to open.

    See compression paratemeter to smart_open.open().
    """
    return [NO_COMPRESSION, INFER_FROM_EXTENSION] + get_supported_extensions()


def get_supported_extensions():
    """Return the list of file extensions for which we have registered compressors."""
    return sorted(_COMPRESSOR_REGISTRY.keys())


def register_compressor(ext, callback):
    """Register a callback for transparently decompressing files with a specific extension.

    Parameters
    ----------
    ext: str
        The extension.  Must include the leading period, e.g. `.gz`.
    callback: callable
        The callback.  It must accept two position arguments, file_obj and mode.
        This function will be called when `smart_open` is opening a file with
        the specified extension.

    Examples
    --------

    Instruct smart_open to use the `lzma` module whenever opening a file
    with a .xz extension (see README.rst for the complete example showing I/O):

    >>> def _handle_xz(file_obj, mode):
    ...     import lzma
    ...     return lzma.LZMAFile(filename=file_obj, mode=mode)
    >>>
    >>> register_compressor('.xz', _handle_xz)

    This is just an example: `lzma` is in the standard library and is registered by default.

    """
    if not (ext and ext[0] == '.'):
        raise ValueError('ext must be a string starting with ., not %r' % ext)
    ext = ext.lower()
    if ext in _COMPRESSOR_REGISTRY:
        logger.warning('overriding existing compression handler for %r', ext)
    _COMPRESSOR_REGISTRY[ext] = callback


def tweak_close(outer, inner):
    """Ensure that closing the `outer` stream closes the `inner` stream as well.

    Deprecated: `smart_open.open().__exit__` now always calls `__exit__` on the
    underlying filestream.

    Use this when your compression library's `close` method does not
    automatically close the underlying filestream.  See
    https://github.com/piskvorky/smart_open/issues/630 for an
    explanation why that is a problem for smart_open.
    """
    outer_close = outer.close

    def close_both(*args):
        nonlocal inner
        try:
            outer_close()
        finally:
            if inner:
                inner, fp = None, inner
                fp.close()

    outer.close = close_both


def _maybe_wrap_buffered(file_obj, mode):
    # https://github.com/piskvorky/smart_open/issues/760#issuecomment-1553971657
    result = file_obj
    if "b" in mode and "w" in mode:
        result = io.BufferedWriter(result)
    elif "b" in mode and "r" in mode:
        result = io.BufferedReader(result)
    return result


def _handle_bz2(file_obj, mode):
    import bz2
    result = bz2.open(filename=file_obj, mode=mode)
    return _maybe_wrap_buffered(result, mode)


def _handle_gzip(file_obj, mode):
    import gzip
    result = gzip.open(filename=file_obj, mode=mode)
    return _maybe_wrap_buffered(result, mode)


def _handle_zstd(file_obj, mode):
    import sys
    if sys.version_info >= (3, 14):
        from compression import zstd
    else:
        from backports import zstd
    result = zstd.open(file_obj, mode=mode)
    return _maybe_wrap_buffered(result, mode)


def _handle_xz(file_obj, mode):
    import lzma
    result = lzma.open(filename=file_obj, mode=mode)
    return _maybe_wrap_buffered(result, mode)


def compression_wrapper(file_obj, mode, compression=INFER_FROM_EXTENSION, filename=None):
    """
    Wrap `file_obj` with an appropriate [de]compression mechanism based on its file extension.

    If the filename extension isn't recognized, simply return the original `file_obj` unchanged.

    `file_obj` must either be a filehandle object, or a class which behaves like one.

    If `filename` is specified, it will be used to extract the extension.
    If not, the `file_obj.name` attribute is used as the filename.

    """
    if compression == NO_COMPRESSION:
        return file_obj
    elif compression == INFER_FROM_EXTENSION:
        try:
            filename = (filename or file_obj.name).lower()
        except (AttributeError, TypeError):
            logger.warning(
                'unable to transparently decompress %r because it '
                'seems to lack a string-like .name', file_obj
            )
            return file_obj
        _, compression = os.path.splitext(filename)

    if compression in _COMPRESSOR_REGISTRY and mode.endswith('+'):
        raise ValueError('transparent (de)compression unsupported for mode %r' % mode)

    try:
        callback = _COMPRESSOR_REGISTRY[compression]
    except KeyError:
        return file_obj
    else:
        return callback(file_obj, mode)


#
# NB. avoid using lambda here to make stack traces more readable.
#
register_compressor('.bz2', _handle_bz2)
register_compressor('.gz', _handle_gzip)
register_compressor('.zst', _handle_zstd)
register_compressor('.xz', _handle_xz)

# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements the majority of smart_open's top-level API.

The main functions are:

  * `open()`
  * `register_compressor()`

"""

import codecs
import collections
import logging
import io
import importlib
import inspect
import os
import os.path as P
import warnings
import sys

import boto3
import six

from six.moves.urllib import parse as urlparse

#
# This module defines a function called smart_open so we cannot use
# smart_open.submodule to reference to the submodules.
#
import smart_open.s3 as smart_open_s3
import smart_open.hdfs as smart_open_hdfs
import smart_open.webhdfs as smart_open_webhdfs
import smart_open.http as smart_open_http
import smart_open.ssh as smart_open_ssh

from smart_open import doctools

# Import ``pathlib`` if the builtin ``pathlib`` or the backport ``pathlib2`` are
# available. The builtin ``pathlib`` will be imported with higher precedence.
for pathlib_module in ('pathlib', 'pathlib2'):
    try:
        pathlib = importlib.import_module(pathlib_module)
        PATHLIB_SUPPORT = True
        break
    except ImportError:
        PATHLIB_SUPPORT = False

logger = logging.getLogger(__name__)

SYSTEM_ENCODING = sys.getdefaultencoding()

_ISSUE_189_URL = 'https://github.com/RaRe-Technologies/smart_open/issues/189'

_DEFAULT_S3_HOST = 's3.amazonaws.com'

_COMPRESSOR_REGISTRY = {}


def register_compressor(ext, callback):
    """Register a callback for transparently decompressing files with a specific extension.

    Parameters
    ----------
    ext: str
        The extension.
    callback: callable
        The callback.  It must accept two position arguments, file_obj and mode.

    Examples
    --------

    Instruct smart_open to use the identity function whenever opening a file
    with a .xz extension (see README.rst for the complete example showing I/O):

    >>> def _handle_xz(file_obj, mode):
    ...     import lzma
    ...     return lzma.LZMAFile(filename=file_obj, mode=mode, format=lzma.FORMAT_XZ)
    >>>
    >>> register_compressor('.xz', _handle_xz)

    """
    if not (ext and ext[0] == '.'):
        raise ValueError('ext must be a string starting with ., not %r' % ext)
    if ext in _COMPRESSOR_REGISTRY:
        logger.warning('overriding existing compression handler for %r', ext)
    _COMPRESSOR_REGISTRY[ext] = callback


def _handle_bz2(file_obj, mode):
    if six.PY2:
        from bz2file import BZ2File
    else:
        from bz2 import BZ2File
    return BZ2File(file_obj, mode)


def _handle_gzip(file_obj, mode):
    import gzip
    return gzip.GzipFile(fileobj=file_obj, mode=mode)


#
# NB. avoid using lambda here to make stack traces more readable.
#
register_compressor('.bz2', _handle_bz2)
register_compressor('.gz', _handle_gzip)


Uri = collections.namedtuple(
    'Uri',
    (
        'scheme',
        'uri_path',
        'bucket_id',
        'key_id',
        'blob_id',
        'port',
        'host',
        'ordinary_calling_format',
        'access_id',
        'access_secret',
        'user',
        'password',
    )
)
"""Represents all the options that we parse from user input.

Some of the above options only make sense for certain protocols, e.g.
bucket_id is only for S3 and GCS.
"""
#
# Set the default values for all Uri fields to be None.  This allows us to only
# specify the relevant fields when constructing a Uri.
#
# https://stackoverflow.com/questions/11351032/namedtuple-and-default-values-for-optional-keyword-arguments
#
Uri.__new__.__defaults__ = (None,) * len(Uri._fields)


def _inspect_kwargs(kallable):
    #
    # inspect.getargspec got deprecated in Py3.4, and calling it spews
    # deprecation warnings that we'd prefer to avoid.  Unfortunately, older
    # versions of Python (<3.3) did not have inspect.signature, so we need to
    # handle them the old-fashioned getargspec way.
    #
    try:
        signature = inspect.signature(kallable)
    except AttributeError:
        args, varargs, keywords, defaults = inspect.getargspec(kallable)
        if not defaults:
            return {}
        supported_keywords = args[-len(defaults):]
        return dict(zip(supported_keywords, defaults))
    else:
        return {
            name: param.default
            for name, param in signature.parameters.items()
            if param.default != inspect.Parameter.empty
        }


def _check_kwargs(kallable, kwargs):
    """Check which keyword arguments the callable supports.

    Parameters
    ----------
    kallable: callable
        A function or method to test
    kwargs: dict
        The keyword arguments to check.  If the callable doesn't support any
        of these, a warning message will get printed.

    Returns
    -------
    dict
        A dictionary of argument names and values supported by the callable.
    """
    supported_keywords = sorted(_inspect_kwargs(kallable))
    unsupported_keywords = [k for k in sorted(kwargs) if k not in supported_keywords]
    supported_kwargs = {k: v for (k, v) in kwargs.items() if k in supported_keywords}

    if unsupported_keywords:
        logger.warning('ignoring unsupported keyword arguments: %r', unsupported_keywords)

    return supported_kwargs


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
        ignore_ext=False,
        transport_params=None,
        ):
    r"""Open the URI object, returning a file-like object.

    The URI is usually a string in a variety of formats:

    1. a URI for the local filesystem: `./lines.txt`, `/home/joe/lines.txt.gz`,
       `file:///home/joe/lines.txt.bz2`
    2. a URI for HDFS: `hdfs:///some/path/lines.txt`
    3. a URI for Amazon's S3 (can also supply credentials inside the URI):
       `s3://my_bucket/lines.txt`, `s3://my_aws_key_id:key_secret@my_bucket/lines.txt`

    The URI may also be one of:

    - an instance of the pathlib.Path class
    - a stream (anything that implements io.IOBase-like functionality)

    This function supports transparent compression and decompression using the
    following codec:

    - ``.gz``
    - ``.bz2``

    The function depends on the file extension to determine the appropriate codec.

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
    ignore_ext: boolean, optional
        Disable transparent compression/decompression based on the file extension.
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

    S3 (for details, see :mod:`smart_open.s3` and :func:`smart_open.s3.open`):

%(s3)s
    HTTP (for details, see :mod:`smart_open.http` and :func:`smart_open.http.open`):

%(http)s
    WebHDFS (for details, see :mod:`smart_open.webhdfs` and :func:`smart_open.webhdfs.open`):

%(webhdfs)s
    SSH (for details, see :mod:`smart_open.ssh` and :func:`smart_open.ssh.open`):

%(ssh)s

    Examples
    --------
%(examples)s

    See Also
    --------
    - `Standard library reference <https://docs.python.org/3.7/library/functions.html#open>`__
    - `smart_open README.rst
      <https://github.com/RaRe-Technologies/smart_open/blob/master/README.rst>`__

    """
    logger.debug('%r', locals())

    if not isinstance(mode, six.string_types):
        raise TypeError('mode should be a string')

    if transport_params is None:
        transport_params = {}

    fobj = _shortcut_open(
        uri,
        mode,
        ignore_ext=ignore_ext,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
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

    # Support opening ``pathlib.Path`` objects by casting them to strings.
    if PATHLIB_SUPPORT and isinstance(uri, pathlib.Path):
        uri = str(uri)

    explicit_encoding = encoding
    encoding = explicit_encoding if explicit_encoding else SYSTEM_ENCODING

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
        binary_mode = {'r': 'rb', 'r+': 'rb+',
                       'rt': 'rb', 'rt+': 'rb+',
                       'w': 'wb', 'w+': 'wb+',
                       'wt': 'wb', "wt+": 'wb+',
                       'a': 'ab', 'a+': 'ab+',
                       'at': 'ab', 'at+': 'ab+'}[mode]
    except KeyError:
        binary_mode = mode
    binary, filename = _open_binary_stream(uri, binary_mode, transport_params)
    if ignore_ext:
        decompressed = binary
    else:
        decompressed = _compression_wrapper(binary, filename, mode)

    if 'b' not in mode or explicit_encoding is not None:
        decoded = _encoding_wrapper(decompressed, mode, encoding=encoding, errors=errors)
    else:
        decoded = decompressed

    return decoded


#
# The docstring can be None if -OO was passed to the interpreter.
#
open.__doc__ = None if open.__doc__ is None else open.__doc__ % {
    's3': doctools.to_docstring(
        doctools.extract_kwargs(smart_open_s3.open.__doc__),
        lpad=u'    ',
    ),
    'http': doctools.to_docstring(
        doctools.extract_kwargs(smart_open_http.open.__doc__),
        lpad=u'    ',
    ),
    'webhdfs': doctools.to_docstring(
        doctools.extract_kwargs(smart_open_webhdfs.open.__doc__),
        lpad=u'    ',
    ),
    'ssh': doctools.to_docstring(
        doctools.extract_kwargs(smart_open_ssh.open.__doc__),
        lpad=u'    ',
    ),
    'examples': doctools.extract_examples_from_readme_rst(),
}


_MIGRATION_NOTES_URL = (
    'https://github.com/RaRe-Technologies/smart_open/blob/master/README.rst'
    '#migrating-to-the-new-open-function'
)


def smart_open(uri, mode="rb", **kw):
    """Deprecated, use smart_open.open instead.

    See the migration instructions: %s

    """ % _MIGRATION_NOTES_URL

    warnings.warn(
        'This function is deprecated, use smart_open.open instead. '
        'See the migration notes for details: %s' % _MIGRATION_NOTES_URL
    )

    #
    # The new function uses a shorter name for this parameter, handle it separately.
    #
    ignore_extension = kw.pop('ignore_extension', False)

    expected_kwargs = _inspect_kwargs(open)
    scrubbed_kwargs = {}
    transport_params = {}

    #
    # Handle renamed keyword arguments.  This is required to maintain backward
    # compatibility.  See test_smart_open_old.py for tests.
    #
    if 'host' in kw or 's3_upload' in kw:
        transport_params['multipart_upload_kwargs'] = {}
        transport_params['resource_kwargs'] = {}

    if 'host' in kw:
        url = kw.pop('host')
        if not url.startswith('http'):
            url = 'http://' + url
        transport_params['resource_kwargs'].update(endpoint_url=url)

    if 's3_upload' in kw and kw['s3_upload']:
        transport_params['multipart_upload_kwargs'].update(**kw.pop('s3_upload'))

    #
    # Providing the entire Session object as opposed to just the profile name
    # is more flexible and powerful, and thus preferable in the case of
    # conflict.
    #
    if 'profile_name' in kw and 's3_session' in kw:
        logger.error('profile_name and s3_session are mutually exclusive, ignoring the former')

    if 'profile_name' in kw:
        transport_params['session'] = boto3.Session(profile_name=kw.pop('profile_name'))

    if 's3_session' in kw:
        transport_params['session'] = kw.pop('s3_session')

    for key, value in kw.items():
        if key in expected_kwargs:
            scrubbed_kwargs[key] = value
        else:
            #
            # Assume that anything not explicitly supported by the new function
            # is a transport layer keyword argument.  This is safe, because if
            # the argument ends up being unsupported in the transport layer,
            # it will only cause a logging warning, not a crash.
            #
            transport_params[key] = value

    return open(uri, mode, ignore_ext=ignore_extension,
                transport_params=transport_params, **scrubbed_kwargs)


def _shortcut_open(
        uri,
        mode,
        ignore_ext=False,
        buffering=-1,
        encoding=None,
        errors=None,
        ):
    """Try to open the URI using the standard library io.open function.

    This can be much faster than the alternative of opening in binary mode and
    then decoding.

    This is only possible under the following conditions:

        1. Opening a local file
        2. Ignore extension is set to True

    If it is not possible to use the built-in open for the specified URI, returns None.

    :param str uri: A string indicating what to open.
    :param str mode: The mode to pass to the open function.
    :param dict kw:
    :returns: The opened file
    :rtype: file
    """
    if not isinstance(uri, six.string_types):
        return None

    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme != 'file':
        return None

    _, extension = P.splitext(parsed_uri.uri_path)
    if extension in _COMPRESSOR_REGISTRY and not ignore_ext:
        return None

    open_kwargs = {}

    if encoding is not None:
        open_kwargs['encoding'] = encoding
        mode = mode.replace('b', '')

    #
    # binary mode of the builtin/stdlib open function doesn't take an errors argument
    #
    if errors and 'b' not in mode:
        open_kwargs['errors'] = errors

    #
    # Under Py3, the built-in open accepts kwargs, and it's OK to use that.
    # Under Py2, the built-in open _doesn't_ accept kwargs, but we still use it
    # whenever possible (see issue #207).  If we're under Py2 and have to use
    # kwargs, then we have no option other to use io.open.
    #
    if six.PY3:
        return _builtin_open(parsed_uri.uri_path, mode, buffering=buffering, **open_kwargs)
    elif not open_kwargs:
        return _builtin_open(parsed_uri.uri_path, mode, buffering=buffering)
    return io.open(parsed_uri.uri_path, mode, buffering=buffering, **open_kwargs)


def _open_binary_stream(uri, mode, transport_params):
    """Open an arbitrary URI in the specified binary mode.

    Not all modes are supported for all protocols.

    :arg uri: The URI to open.  May be a string, or something else.
    :arg str mode: The mode to open with.  Must be rb, wb or ab.
    :arg transport_params: Keyword argumens for the transport layer.
    :returns: A file object and the filename
    :rtype: tuple
    """
    if mode not in ('rb', 'rb+', 'wb', 'wb+', 'ab', 'ab+'):
        #
        # This should really be a ValueError, but for the sake of compatibility
        # with older versions, which raise NotImplementedError, we do the same.
        #
        raise NotImplementedError('unsupported mode: %r' % mode)

    if isinstance(uri, six.string_types):
        # this method just routes the request to classes handling the specific storage
        # schemes, depending on the URI protocol in `uri`
        filename = uri.split('/')[-1]
        parsed_uri = _parse_uri(uri)

        if parsed_uri.scheme == "file":
            fobj = io.open(parsed_uri.uri_path, mode)
            return fobj, filename
        elif parsed_uri.scheme in smart_open_ssh.SCHEMES:
            fobj = smart_open_ssh.open(
                parsed_uri.uri_path,
                mode,
                host=parsed_uri.host,
                user=parsed_uri.user,
                port=parsed_uri.port,
                password=parsed_uri.password,
                transport_params=transport_params,
            )
            return fobj, filename
        elif parsed_uri.scheme in smart_open_s3.SUPPORTED_SCHEMES:
            return _s3_open_uri(parsed_uri, mode, transport_params), filename
        elif parsed_uri.scheme == "hdfs":
            _check_kwargs(smart_open_hdfs.open, transport_params)
            return smart_open_hdfs.open(parsed_uri.uri_path, mode), filename
        elif parsed_uri.scheme == "webhdfs":
            kw = _check_kwargs(smart_open_webhdfs.open, transport_params)
            http_uri = smart_open_webhdfs.convert_to_http_uri(parsed_uri)
            return smart_open_webhdfs.open(http_uri, mode, **kw), filename
        elif parsed_uri.scheme.startswith('http'):
            #
            # The URI may contain a query string and fragments, which interfere
            # with our compressed/uncompressed estimation, so we strip them.
            #
            filename = P.basename(urlparse.urlparse(uri).path)
            kw = _check_kwargs(smart_open_http.open, transport_params)
            return smart_open_http.open(uri, mode, **kw), filename
        else:
            raise NotImplementedError("scheme %r is not supported", parsed_uri.scheme)
    elif hasattr(uri, 'read'):
        # simply pass-through if already a file-like
        # we need to return something as the file name, but we don't know what
        # so we probe for uri.name (e.g., this works with open() or tempfile.NamedTemporaryFile)
        # if the value ends with COMPRESSED_EXT, we will note it in _compression_wrapper()
        # if there is no such an attribute, we return "unknown" - this
        # effectively disables any compression
        filename = getattr(uri, 'name', 'unknown')
        return uri, filename
    else:
        raise TypeError("don't know how to handle uri %r" % uri)


def _s3_open_uri(uri, mode, transport_params):
    logger.debug('s3_open_uri: %r', locals())
    if mode in ('r', 'w'):
        raise ValueError('this function can only open binary streams. '
                         'Use smart_open.smart_open() to open text streams.')
    elif mode not in ('rb', 'wb'):
        raise NotImplementedError('unsupported mode: %r', mode)

    #
    # There are two explicit ways we can receive session parameters from the user.
    #
    # 1. Via the session keyword argument (transport_params)
    # 2. Via the URI itself
    #
    # They are not mutually exclusive, but we have to pick one of the two.
    # Go with 1).
    #
    if transport_params.get('session') is not None and (uri.access_id or uri.access_secret):
        logger.warning(
            'ignoring credentials parsed from URL because they conflict with '
            'transport_params.session. Set transport_params.session to None '
            'to suppress this warning.'
        )
    elif (uri.access_id and uri.access_secret):
        transport_params['session'] = boto3.Session(
            aws_access_key_id=uri.access_id,
            aws_secret_access_key=uri.access_secret,
        )

    #
    # There are two explicit ways the user can provide the endpoint URI:
    #
    # 1. Via the URL.  The protocol is implicit, and we assume HTTPS in this case.
    # 2. Via the resource_kwargs and multipart_upload_kwargs endpoint_url parameter.
    #
    # Again, these are not mutually exclusive: the user can specify both.  We
    # have to pick one to proceed, however, and we go with 2.
    #
    if uri.host != _DEFAULT_S3_HOST:
        endpoint_url = 'https://%s:%d' % (uri.host, uri.port)
        _override_endpoint_url(transport_params, endpoint_url)

    kwargs = _check_kwargs(smart_open_s3.open, transport_params)
    return smart_open_s3.open(uri.bucket_id, uri.key_id, mode, **kwargs)


def _override_endpoint_url(tp, url):
    try:
        resource_kwargs = tp['resource_kwargs']
    except KeyError:
        resource_kwargs = tp['resource_kwargs'] = {}

    if resource_kwargs.get('endpoint_url'):
        logger.warning(
            'ignoring endpoint_url parsed from URL because it conflicts '
            'with transport_params.resource_kwargs.endpoint_url. '
        )
    else:
        resource_kwargs.update(endpoint_url=url)


def _my_urlsplit(url):
    """This is a hack to prevent the regular urlsplit from splitting around question marks.

    A question mark (?) in a URL typically indicates the start of a
    querystring, and the standard library's urlparse function handles the
    querystring separately.  Unfortunately, question marks can also appear
    _inside_ the actual URL for some schemas like S3.

    Replaces question marks with newlines prior to splitting.  This is safe because:

    1. The standard library's urlsplit completely ignores newlines
    2. Raw newlines will never occur in innocuous URLs.  They are always URL-encoded.

    See Also
    --------
    https://github.com/python/cpython/blob/3.7/Lib/urllib/parse.py
    https://github.com/RaRe-Technologies/smart_open/issues/285
    """
    parsed_url = urlparse.urlsplit(url, allow_fragments=False)
    if parsed_url.scheme not in smart_open_s3.SUPPORTED_SCHEMES or '?' not in url:
        return parsed_url

    sr = urlparse.urlsplit(url.replace('?', '\n'), allow_fragments=False)
    return urlparse.SplitResult(sr.scheme, sr.netloc, sr.path.replace('\n', '?'), '', '')


def _parse_uri(uri_as_string):
    """
    Parse the given URI from a string.

    Supported URI schemes are:

      * file
      * gs
      * hdfs
      * http
      * https
      * s3
      * s3a
      * s3n
      * s3u
      * webhdfs

    .s3, s3a and s3n are treated the same way.  s3u is s3 but without SSL.

    Valid URI examples::

      * s3://my_bucket/my_key
      * s3://my_key:my_secret@my_bucket/my_key
      * s3://my_key:my_secret@my_server:my_port@my_bucket/my_key
      * hdfs:///path/file
      * hdfs://path/file
      * webhdfs://host:port/path/file
      * ./local/path/file
      * ~/local/path/file
      * local/path/file
      * ./local/path/file.gz
      * file:///home/user/file
      * file:///home/user/file.bz2
      * [ssh|scp|sftp]://username@host//path/file
      * [ssh|scp|sftp]://username@host/path/file
      * gs://my_bucket/my_blob

    """
    if os.name == 'nt':
        # urlsplit doesn't work on Windows -- it parses the drive as the scheme...
        if '://' not in uri_as_string:
            # no protocol given => assume a local file
            uri_as_string = 'file://' + uri_as_string

    parsed_uri = _my_urlsplit(uri_as_string)

    if parsed_uri.scheme == "hdfs":
        return _parse_uri_hdfs(parsed_uri)
    elif parsed_uri.scheme == "webhdfs":
        return parsed_uri
    elif parsed_uri.scheme in smart_open_s3.SUPPORTED_SCHEMES:
        return _parse_uri_s3x(parsed_uri)
    elif parsed_uri.scheme == 'file':
        return _parse_uri_file(parsed_uri.netloc + parsed_uri.path)
    elif parsed_uri.scheme in ('', None):
        return _parse_uri_file(uri_as_string)
    elif parsed_uri.scheme.startswith('http'):
        return Uri(scheme=parsed_uri.scheme, uri_path=uri_as_string)
    elif parsed_uri.scheme in smart_open_ssh.SCHEMES:
        return _parse_uri_ssh(parsed_uri)
    else:
        raise NotImplementedError(
            "unknown URI scheme %r in %r" % (parsed_uri.scheme, uri_as_string)
        )


def _parse_uri_hdfs(parsed_uri):
    assert parsed_uri.scheme == 'hdfs'
    uri_path = parsed_uri.netloc + parsed_uri.path
    uri_path = "/" + uri_path.lstrip("/")
    if not uri_path:
        raise RuntimeError("invalid HDFS URI: %s" % str(parsed_uri))

    return Uri(scheme='hdfs', uri_path=uri_path)


def _parse_uri_s3x(parsed_uri):
    #
    # Restrictions on bucket names and labels:
    #
    # - Bucket names must be at least 3 and no more than 63 characters long.
    # - Bucket names must be a series of one or more labels.
    # - Adjacent labels are separated by a single period (.).
    # - Bucket names can contain lowercase letters, numbers, and hyphens.
    # - Each label must start and end with a lowercase letter or a number.
    #
    # We use the above as a guide only, and do not perform any validation.  We
    # let boto3 take care of that for us.
    #
    assert parsed_uri.scheme in smart_open_s3.SUPPORTED_SCHEMES

    port = 443
    host = _DEFAULT_S3_HOST
    ordinary_calling_format = False
    #
    # These defaults tell boto3 to look for credentials elsewhere
    #
    access_id, access_secret = None, None

    #
    # Common URI template [secret:key@][host[:port]@]bucket/object
    #
    # The urlparse function doesn't handle the above schema, so we have to do
    # it ourselves.
    #
    uri = parsed_uri.netloc + parsed_uri.path

    if '@' in uri and ':' in uri.split('@')[0]:
        auth, uri = uri.split('@', 1)
        access_id, access_secret = auth.split(':')

    head, key_id = uri.split('/', 1)
    if '@' in head and ':' in head:
        ordinary_calling_format = True
        host_port, bucket_id = head.split('@')
        host, port = host_port.split(':', 1)
        port = int(port)
    elif '@' in head:
        ordinary_calling_format = True
        host, bucket_id = head.split('@')
    else:
        bucket_id = head

    return Uri(
        scheme=parsed_uri.scheme, bucket_id=bucket_id, key_id=key_id,
        port=port, host=host, ordinary_calling_format=ordinary_calling_format,
        access_id=access_id, access_secret=access_secret
    )


def _parse_uri_file(input_path):
    # '~/tmp' may be expanded to '/Users/username/tmp'
    uri_path = os.path.expanduser(input_path)

    if not uri_path:
        raise RuntimeError("invalid file URI: %s" % input_path)

    return Uri(scheme='file', uri_path=uri_path)


def _parse_uri_ssh(unt):
    """Parse a Uri from a urllib namedtuple."""
    return Uri(
        scheme=unt.scheme,
        uri_path=_unquote(unt.path),
        user=_unquote(unt.username),
        host=unt.hostname,
        port=int(unt.port or smart_open_ssh.DEFAULT_PORT),
        password=_unquote(unt.password),
    )


def _unquote(text):
    return text and urlparse.unquote(text)


def _need_to_buffer(file_obj, mode, ext):
    """Returns True if we need to buffer the whole file in memory in order to proceed."""
    try:
        is_seekable = file_obj.seekable()
    except AttributeError:
        #
        # Under Py2, built-in file objects returned by open do not have
        # .seekable, but have a .seek method instead.
        #
        is_seekable = hasattr(file_obj, 'seek')
    return six.PY2 and mode.startswith('r') and ext in _COMPRESSOR_REGISTRY and not is_seekable


def _compression_wrapper(file_obj, filename, mode):
    """
    This function will wrap the file_obj with an appropriate
    [de]compression mechanism based on the extension of the filename.

    file_obj must either be a filehandle object, or a class which behaves
        like one.

    If the filename extension isn't recognized, will simply return the original
    file_obj.
    """
    _, ext = os.path.splitext(filename)

    if _need_to_buffer(file_obj, mode, ext):
        warnings.warn('streaming gzip support unavailable, see %s' % _ISSUE_189_URL)
        file_obj = io.BytesIO(file_obj.read())
    if ext in _COMPRESSOR_REGISTRY and mode.endswith('+'):
        raise ValueError('transparent (de)compression unsupported for mode %r' % mode)

    try:
        callback = _COMPRESSOR_REGISTRY[ext]
    except KeyError:
        return file_obj
    else:
        return callback(file_obj, mode)


def _encoding_wrapper(fileobj, mode, encoding=None, errors=None):
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
        encoding = SYSTEM_ENCODING

    kw = {'errors': errors} if errors else {}
    if mode[0] == 'r' or mode.endswith('+'):
        fileobj = codecs.getreader(encoding)(fileobj, **kw)
    if mode[0] in ('w', 'a') or mode.endswith('+'):
        fileobj = codecs.getwriter(encoding)(fileobj, **kw)
    return fileobj

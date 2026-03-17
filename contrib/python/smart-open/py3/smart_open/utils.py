# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Helper functions for documentation, etc."""

import inspect
import io
import logging
import urllib.parse

import wrapt

logger = logging.getLogger(__name__)

WORKAROUND_SCHEMES = ['s3', 's3n', 's3u', 's3a', 'gs']
QUESTION_MARK_PLACEHOLDER = '///smart_open.utils.QUESTION_MARK_PLACEHOLDER///'


def inspect_kwargs(kallable):
    #
    # inspect.getargspec got deprecated in Py3.4, and calling it spews
    # deprecation warnings that we'd prefer to avoid.  Unfortunately, older
    # versions of Python (<3.3) did not have inspect.signature, so we need to
    # handle them the old-fashioned getargspec way.
    #
    try:
        signature = inspect.signature(kallable)
    except AttributeError:
        try:
            args, varargs, keywords, defaults = inspect.getargspec(kallable)
        except TypeError:
            #
            # Happens under Py2.7 with mocking.
            #
            return {}

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


def check_kwargs(kallable, kwargs):
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
    supported_keywords = sorted(inspect_kwargs(kallable))
    unsupported_keywords = [k for k in sorted(kwargs) if k not in supported_keywords]
    supported_kwargs = {k: v for (k, v) in kwargs.items() if k in supported_keywords}

    if unsupported_keywords:
        logger.warning('ignoring unsupported keyword arguments: %r', unsupported_keywords)

    return supported_kwargs


def clamp(value, minval=0, maxval=None):
    """Clamp a numeric value to a specific range.

    Parameters
    ----------
    value: numeric
        The value to clamp.

    minval: numeric
        The lower bound.

    maxval: numeric
        The upper bound.

    Returns
    -------
    numeric
        The clamped value.  It will be in the range ``[minval, maxval]``.

    """
    if maxval is not None:
        value = min(value, maxval)
    value = max(value, minval)
    return value


def make_range_string(start=None, stop=None):
    """Create a byte range specifier in accordance with RFC-2616.

    Parameters
    ----------
    start: int, optional
        The start of the byte range.  If unspecified, stop indicated offset from EOF.

    stop: int, optional
        The end of the byte range.  If unspecified, indicates EOF.

    Returns
    -------
    str
        A byte range specifier.

    """
    #
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    #
    if start is None and stop is None:
        raise ValueError("make_range_string requires either a stop or start value")
    start_str = '' if start is None else str(start)
    stop_str = '' if stop is None else str(stop)
    return 'bytes=%s-%s' % (start_str, stop_str)


def parse_content_range(content_range):
    """Extract units, start, stop, and length from a content range header like "bytes 0-846981/846982".

    Assumes a properly formatted content-range header from S3.
    See werkzeug.http.parse_content_range_header for a more robust version.

    Parameters
    ----------
    content_range: str
        The content-range header to parse.

    Returns
    -------
    tuple (units: str, start: int, stop: int, length: int)
        The units and three integers from the content-range header.

    """
    units, numbers = content_range.split(' ', 1)
    range, length = numbers.split('/', 1)
    start, stop = range.split('-', 1)
    return units, int(start), int(stop), int(length)


def safe_urlsplit(url):
    """This is a hack to prevent the regular urlsplit from splitting around question marks.

    A question mark (?) in a URL typically indicates the start of a
    querystring, and the standard library's urlparse function handles the
    querystring separately.  Unfortunately, question marks can also appear
    _inside_ the actual URL for some schemas like S3, GS.

    Replaces question marks with a special placeholder substring prior to
    splitting.  This work-around behavior is disabled in the unlikely event the
    placeholder is already part of the URL.  If this affects you, consider
    changing the value of QUESTION_MARK_PLACEHOLDER to something more suitable.

    See Also
    --------
    https://bugs.python.org/issue43882
    https://github.com/python/cpython/blob/3.14/Lib/urllib/parse.py
    https://github.com/piskvorky/smart_open/issues/285
    https://github.com/piskvorky/smart_open/issues/458
    smart_open/utils.py:QUESTION_MARK_PLACEHOLDER
    """
    sr = urllib.parse.urlsplit(url, allow_fragments=False)

    placeholder = None
    if sr.scheme in WORKAROUND_SCHEMES and '?' in url and QUESTION_MARK_PLACEHOLDER not in url:
        #
        # This is safe because people will _almost never_ use the below
        # substring in a URL.  If they do, then they're asking for trouble,
        # and this special handling will simply not happen for them.
        #
        placeholder = QUESTION_MARK_PLACEHOLDER
        url = url.replace('?', placeholder)
        sr = urllib.parse.urlsplit(url, allow_fragments=False)

    if placeholder is None:
        return sr

    path = sr.path.replace(placeholder, '?')
    return urllib.parse.SplitResult(sr.scheme, sr.netloc, path, '', '')


class TextIOWrapper(io.TextIOWrapper):
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Call close on underlying buffer only when there was no exception.

        Without this patch, TextIOWrapper would call self.buffer.close() during
        exception handling, which is unwanted for e.g. s3 and azure. They only call
        self.close() when there was no exception (self.terminate() otherwise) to avoid
        committing unfinished/failed uploads.
        """
        if exc_type is None:
            self.close()


class FileLikeProxy(wrapt.ObjectProxy):
    __inner = ...  # initialized before wrapt disallows __setattr__ on certain objects

    def __init__(self, outer, inner):
        super().__init__(outer)
        self.__inner = inner

    def __enter__(self):
        """This explicit proxy method is only required for pylance ref #916."""
        return self.__wrapped__.__enter__()

    def __exit__(self, *args, **kwargs):
        """Exit inner after exiting outer."""
        try:
            return super().__exit__(*args, **kwargs)
        finally:
            self.__inner.__exit__(*args, **kwargs)

    def __next__(self):
        return self.__wrapped__.__next__()

    def close(self):
        try:
            return self.__wrapped__.close()
        finally:
            if self.__inner != self.__wrapped__:  # Don't close again if inner and wrapped are the same
                self.__inner.close()

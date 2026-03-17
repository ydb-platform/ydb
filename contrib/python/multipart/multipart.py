# -*- coding: utf-8 -*-
"""
This module provides multiple parsers for RFC-7578 `multipart/form-data`, both
low-level for framework authors and high-level for WSGI or ASGI application
developers.

https://multipart.readthedocs.io/

Copyright (c) 2010-2025, Marcel Hellkamp
License: MIT (see LICENSE file)
"""


__author__ = "Marcel Hellkamp"
__version__ = '1.3.1'
__license__ = "MIT"
__all__ = [
    "MultipartError",
    "ParserLimitReached",
    "ParserError",
    "StrictParserError",
    "ParserStateError",
    "is_form_request",
    "parse_form_data",
    "MultipartParser",
    "MultipartPart",
    "PushMultipartParser",
    "MultipartSegment",
]


import re
from io import BufferedRandom, BytesIO
from typing import (
    Generator,
    AsyncGenerator,
    Union,
    Optional,
    Tuple,
    List,
    Callable,
    Awaitable,
)

from urllib.parse import parse_qs
from wsgiref.headers import Headers
from collections.abc import MutableMapping as DictMixin
import tempfile
import functools
import typing
from math import inf

# Type Aliases (internal use only)

t_ParserEvent: "typing.TypeAlias" = Union["MultipartSegment", bytearray, None]
t_ByteString: "typing.TypeAlias" = Union[bytes, bytearray]
t_BlockingReader: "typing.TypeAlias" = Callable[[int], t_ByteString]
t_AsyncReader: "typing.TypeAlias" = Callable[[int], Awaitable[t_ByteString]]


##
### Exceptions
##


class MultipartError(ValueError):
    """Base class for all parser errors or warnings"""

    #: Suitable HTTP status code for this exception
    http_status = 500  # Internal Error


class ParserError(MultipartError):
    """Detected invalid input"""

    http_status = 415  # Unsupported Media Type


class StrictParserError(ParserError):
    """Detected unusual input while parsing in strict mode"""

    http_status = 415  # Unsupported Media Type


class ParserLimitReached(MultipartError):
    """Parser reached one of the configured limits"""

    http_status = 413  # Request Entity Too Large


class ParserStateError(MultipartError):
    """Parser reachend an invalid state (e.g. use after close)"""

    http_status = 500  # Internal Error


##############################################################################
################################ Helper & Misc ###############################
##############################################################################
# Some of these were copied from bottle: https://bottlepy.org


class MultiDict(DictMixin):
    """A dict that stores multiple values per key. Most dict methods return the
    last value by default. There are special methods to get all values.
    """

    def __init__(self, *args, **kwargs):
        self.dict = {}
        for arg in args:
            if hasattr(arg, "items"):
                for k, v in arg.items():
                    self[k] = v
            else:
                for k, v in arg:
                    self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def __len__(self):
        return len(self.dict)

    def __iter__(self):
        return iter(self.dict)

    def __contains__(self, key):
        return key in self.dict

    def __delitem__(self, key):
        del self.dict[key]

    def __str__(self):
        return str(self.dict)

    def __repr__(self):
        return repr(self.dict)

    def keys(self):
        return self.dict.keys()

    def __getitem__(self, key):
        return self.dict[key][-1]

    def __setitem__(self, key, value):
        self.append(key, value)

    def append(self, key, value):
        """Add an additional value to a key."""
        self.dict.setdefault(key, []).append(value)

    def replace(self, key, value):
        """Replace all values for a key with a single value."""
        self.dict[key] = [value]

    def getall(self, key):
        """Return a list with all values for a key. The list may be empty."""
        return self.dict.get(key) or []

    def get(self, key, default=None, index=-1):
        try:
            return self.dict[key][index]
        except (KeyError, IndexError):
            return default

    def iterallitems(self):
        """Yield (key, value) pairs with repeating keys for each value."""
        for key, values in self.dict.items():
            for value in values:
                yield key, value


def to_bytes(data, enc="utf8"):
    if isinstance(data, str):
        data = data.encode(enc)

    return data


def copy_file(stream, target, maxread=-1, buffer_size=2**16):
    """Read from :stream and write to :target until :maxread or EOF."""
    size, read = 0, stream.read

    while True:
        to_read = buffer_size if maxread < 0 else min(buffer_size, maxread - size)
        part = read(to_read)

        if not part:
            return size

        target.write(part)
        size += len(part)


class _cached_property:
    """A property that is only computed once per instance and then replaces
    itself with an ordinary attribute. Deleting the attribute resets the
    property."""

    def __init__(self, func):
        functools.update_wrapper(self, func)  # type: ignore
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:  # pragma: no cover
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


# -------------
# Header Parser
# -------------


# ASCII minus control or special chars
_token = "[a-zA-Z0-9-!#$%&'*+.^_`|~]+"
_re_token = re.compile("^%s$" % _token, re.ASCII)
# A token or quoted-string (simple qs | token | slow qs)
_value = r'"[^\\"]*"|%s|"(?:\\.|[^\\"])*"' % _token
# A "; key=value" pair from content-disposition header
_option = r"; *(%s) *= *(%s)" % (_token, _value)
_re_option = re.compile(_option)


def header_quote(val):
    """Quote header option values if necessary.

    Note: This is NOT the way modern browsers quote field names or filenames
    in Content-Disposition headers. See :func:`content_disposition_quote`
    """
    if _re_token.match(val):
        return val

    return '"' + val.replace("\\", "\\\\").replace('"', '\\"') + '"'


def header_unquote(val, filename=False):
    """Unquote header option values.

    Note: This is NOT the way modern browsers quote field names or filenames
    in Content-Disposition headers. See :func:`content_disposition_unquote`
    """
    if val[0] == val[-1] == '"':
        val = val[1:-1]

        # fix ie6 bug: full path --> filename
        if filename and (val[1:3] == ":\\" or val[:2] == "\\\\"):
            val = val.split("\\")[-1]

        return val.replace("\\\\", "\\").replace('\\"', '"')

    return val


def content_disposition_quote(val):
    """Quote field names or filenames for Content-Disposition headers the same
    way modern browsers do it (see WHATWG HTML5 specification).
    """
    val = val.replace("\r", "%0D").replace("\n", "%0A").replace('"', "%22")
    return '"' + val + '"'


def content_disposition_unquote(val, filename=False):
    """Unquote field names or filenames from Content-Disposition headers.

    Legacy quoting mechanisms are detected to some degree and also supported,
    but there are rare ambiguous edge cases where we have to guess. If in doubt,
    this function assumes a modern browser and follows the WHATWG HTML5
    specification (limited percent-encoding, no backslash-encoding).
    """

    if '"' == val[0] == val[-1]:
        val = val[1:-1]
        if '\\"' in val:  # Legacy backslash-escaped quoted strings
            val = val.replace("\\\\", "\\").replace('\\"', '"')
        elif "%" in val:  # Modern (HTML5) limited percent-encoding
            val = val.replace("%0D", "\r").replace("%0A", "\n").replace("%22", '"')
        # ie6/windows bug: full path instead of just filename
        if filename and (val[1:3] == ":\\" or val[:2] == "\\\\"):
            val = val.rpartition("\\")[-1]
    elif "%" in val:  # Modern (HTML5) limited percent-encoding
        val = val.replace("%0D", "\r").replace("%0A", "\n").replace("%22", '"')
    return val


def parse_options_header(header, options=None, unquote=header_unquote):
    """Parse Content-Type (or similar) headers into a primary value and an
    options-dict.

    Note: For Content-Disposition headers you need a different unquote function.
    See `content_disposition_unquote`.

    """
    i = header.find(";")
    if i < 0:
        return header.lower().strip(), {}

    options = options or {}
    for key, val in _re_option.findall(header, i):
        key = key.lower()
        options[key] = unquote(val, key == "filename")

    return header[:i].lower().strip(), options


##############################################################################
################################## SansIO Parser #############################
##############################################################################

# Constants used by the parser
_HEADER_EXPECTED = frozenset(["Content-Disposition", "Content-Type", "Content-Length"])
# Parser states as constants
_PREAMBLE = "PREAMBLE"
_HEADER = "HEADER"
_BODY = "BODY"
_COMPLETE = "END"


class PushMultipartParser:
    """An incremental non-blocking parser for multipart/form-data.

    This class provides a non-blocking :meth:`parse` method as well as several
    convenience methods to parse blocking or async data streams.

    **Strict mode**: In `strict` mode, the parser will be less forgiving and
    bail out more quickly when presented with strange or invalid input,
    avoiding unnecessary work caused by broken or malicious clients. Fatal
    errors will always trigger exceptions, even in non-strict mode.

    **Limits**: The various limits are meant as safeguards and exceeding any
    of those limit will trigger :exc:`ParserLimitReached` exceptions.

    Parser instances can be used as context managers in a ``with`` statement
    to ensure that :meth:`close` is called after leaving the parser loop. This
    is important to detect incomplete multipart streams.
    """

    def __init__(
        self,
        boundary: Union[str, t_ByteString],
        content_length=-1,
        max_header_size=4096 + 128,  # 4KB should be enough for everyone
        max_header_count=8,  # RFC 7578 allows just 3
        max_segment_size=inf,  # unlimited
        max_segment_count=inf,  # unlimited
        header_charset="utf8",
        strict=False,
    ):
        """Create a new parser instance.

        :param boundary: The multipart boundary as found in the Content-Type header.
        :param content_length: Expected input size in bytes, or -1 if unknown.
        :param max_header_size: Maximum length of a single header line (name and value).
        :param max_header_count: Maximum number of headers per segment.
        :param max_segment_size: Maximum size of a single segment body.
        :param max_segment_count: Maximum number of segments.
        :param header_charset: Charset for header names and values.
        :param strict: Enables additional format and sanity checks.
        """
        self.boundary = to_bytes(boundary)
        self.content_length = content_length
        self.header_charset = header_charset
        self.max_header_size = max_header_size
        self.max_header_count = max_header_count
        self.max_segment_size = max_segment_size
        self.max_segment_count = max_segment_count
        self.strict = strict

        # Internal parser state
        self._delimiter = b"\r\n--" + self.boundary
        self._parsed = 0
        self._fieldcount = 0
        self._buffer = bytearray()
        self._current = MultipartSegment(self)
        self._state = _PREAMBLE

        #: True if the parser was closed, either successfully by reaching the
        #: end of the multipart stream, or due to an :attr:`error`.
        self.closed = False
        #: A :exc:`MultipartError` instance if parsing failed.
        self.error: Optional[MultipartError] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Close the parser. If the call was caused by an exception, the final
        check for a complete multipart stream is skipped to avoid another
        exception.
        """
        self.close(check_complete=not exc_type)

    def parse(self, chunk: t_ByteString) -> Generator[t_ParserEvent, None, None]:
        """Parse a chunk of data and yield as many parser events as possible
        from the given chunk.

        **Parser Events:** For each multipart segment the parser will emit a
        single instance of :class:`MultipartSegment` with header and meta
        information, followed by zero or more non-empty :class:`bytearray`
        instances with chunks from the segment body, followed by a single
        :data:`None` event to signal the end of the current segment.

        This method does not perform any IO on its own. It stops yielding events
        if more data is needed and should be called again with the next chunk to
        continue. The returned generator must be fully consumed before parsing
        the next chunk. Once the end of the multipart stream is reached and the
        last event was emitted, :attr:`closed` will be true.

        End of input can be signaled by parsing an empty chunk, calling
        :meth:`close` or using the parser as a context manager and leaving the
        context. Closing the parser is important to be notified about incomplete
        or missing segments.

        :param chunk: A non-empty chunk of data, or an empty chunk to signal end
            of input.

        :raises ParserError: Input is not a valid multipart stream.
        :raises StrictParserError: Unusual input while parsing in strict mode.
        :raises ParserLimitReached: One of the configured limits reached.
        :raises ParserStateError: Invalid parser state (e.g. use after close).
        """

        try:
            assert isinstance(chunk, (bytes, bytearray))

            if not chunk:
                self.close()
                return

            if self.closed:
                raise ParserStateError("Parser closed")

            if self.content_length > -1:
                available = self._parsed + len(self._buffer) + len(chunk)
                if self.content_length < available:
                    raise ParserError("Content-Length limit exceeded")

            if self._state is _COMPLETE:
                if self.strict:
                    raise StrictParserError(
                        "Unexpected data after end of multipart stream"
                    )
                return

            delimiter = self._delimiter
            d_len = len(delimiter)
            buffer = self._buffer
            buffer += chunk  # In-place append
            bufferlen = len(buffer)
            offset = 0

            while True:

                if self._state is _PREAMBLE:
                    # Scan for first delimiter (CRLF prefix is optional here)
                    index = buffer.find(delimiter[2:], offset)

                    if index > -1:
                        # Boundary must be at position zero, or start with CRLF
                        if index > 0 and buffer[index - 2 : index] != b"\r\n":
                            raise ParserError(
                                "Unexpected byte in front of first boundary"
                            )

                        next_start = index + d_len
                        tail = buffer[next_start - 2 : next_start]

                        if tail == b"\r\n":  # Normal delimiter found
                            self._state = _HEADER
                            offset = next_start
                            continue
                        elif tail == b"--":  # First is also last delimiter
                            offset = next_start
                            self._state = _COMPLETE
                            break  # parsing complete
                        elif tail[0:1] == b"\n":  # Broken client or legacy test case
                            raise ParserError("Invalid line break after first boundary")
                        elif len(tail) == 2:
                            raise ParserError("Unexpected byte after first boundary")

                    elif self.strict and bufferlen >= d_len:
                        # No boundary in first chunk -> Fail fast in strict mode
                        # and do not waste time consuming a legacy preamble.
                        raise StrictParserError("Boundary not found in first chunk")

                    # Delimiter not found, skip data until we find one
                    offset = bufferlen - (d_len + 2)
                    break  # wait for more data

                elif self._state is _HEADER:
                    # Find end of header line
                    nl = buffer.find(b"\r\n", offset)

                    if nl > offset:  # Non-empty header line
                        self._current._add_headerline(buffer[offset:nl])
                        offset = nl + 2
                        continue
                    elif nl == offset:  # Empty header line -> End of header section
                        self._current._close_headers()
                        yield self._current
                        self._state = _BODY
                        offset += 2
                        continue
                    else:  # No CRLF found -> Ask for more data
                        if buffer.find(b"\n", offset) != -1:
                            raise ParserError("Invalid line break in segment header")
                        if bufferlen - offset > self.max_header_size:
                            raise ParserLimitReached(
                                "Maximum segment header length exceeded"
                            )
                        break  # wait for more data

                elif self._state is _BODY:

                    # Ensure there is enough data in buffer to fit a delimiter
                    if offset + d_len + 2 > bufferlen:
                        break  # wait for more data

                    # Scan for delimiter (CRLF + boundary + (CRLF or '--'))
                    index = buffer.find(delimiter, offset)
                    if index > -1:
                        next_start = index + d_len + 2
                        tail = buffer[next_start - 2 : next_start]

                        if tail == b"\r\n" or tail == b"--":
                            if index > offset:
                                self._current._update_size(index - offset)
                                yield buffer[offset:index]

                            offset = next_start
                            self._current._mark_complete()
                            yield None  # End of segment

                            if tail == b"--":  # Last delimiter
                                self._state = _COMPLETE
                                break
                            else:  # Normal delimiter
                                self._current = MultipartSegment(self)
                                self._state = _HEADER
                                continue

                    # Keep enough in buffer to accout for a partial delimiter at
                    # the end, but emit the rest.
                    chunk_end = bufferlen - (d_len + 1)
                    assert chunk_end > offset  # Always true
                    self._current._update_size(chunk_end - offset)
                    yield buffer[offset:chunk_end]
                    offset = chunk_end
                    break  # wait for more data

                else:  # pragma: no cover
                    raise RuntimeError(f"Unexpected internal state: {self._state}")

            # We ran out of data, or reached the end
            if offset > 0:
                self._parsed += offset
                buffer[:] = buffer[offset:]

        except MultipartError as err:
            if not self.error:
                self.error = err
            self.close(check_complete=False)
            raise

    async def parse_async(
        self, read: t_AsyncReader, chunk_size=1024 * 64
    ) -> AsyncGenerator[t_ParserEvent, None]:
        """Parse the entire multipart stream from an async ``read`` function and
        return an async generator yielding parser events (see :meth:`parse`).
        Should be used with ``async from``.

        This convenience method will try to read and parse chunks of data until
        the end of the multipart stream is reached or the ``read`` function
        returns an empty chunk (signaling EOF). If :attr:`content_length` is
        known, then the parser will only try to read up to this limit.

        :param read: An async function that takes `chunk_size` as a parameter
          and returns a non-empty chunk of data as soon as data is available, or
          an empty chunk if EOF was detected and there is no data to return.
          For example: :meth:`asyncio.StreamReader.read`.
        :param chunk_size: A positive integer limiting maximum size of a single
          read operation.

        :raises Exception: Exceptions raised by ``read`` are not handled.
        :raises MultipartError: Same as :meth:`parse`.

        .. versionadded:: 1.3
        """
        assert chunk_size > 0

        with self:
            readlimit = self.content_length
            while not self.closed:
                if readlimit >= 0:
                    chunk = await read(min(chunk_size, readlimit))
                    readlimit -= len(chunk)
                else:
                    chunk = await read(chunk_size)

                for event in self.parse(chunk):
                    yield event

    def parse_blocking(
        self, read: t_BlockingReader, chunk_size=1024 * 64
    ) -> Generator[t_ParserEvent, None, None]:
        """Parse the entire multipart stream from a blocking ``read`` function
        and return a generator yielding parser events (see :meth:`parse`).

        This convenience method will try to read and parse chunks of data until
        the end of the multipart stream is reached or the ``read`` function
        returns an empty chunk (signaling EOF). If :attr:`content_length` is
        known, then the parser will only try to read up to this limit.

        :param read: A callable that takes `chunk_size` as a parameter
        and returns a non-empty chunk of data as soon as data is available, or an
        empty chunk if EOF was detected and there is no data to return. Most
        blocking read functions work that way.
        :param chunk_size: A positive integer limiting the maximum chunk size.

        :raises Exception: Exceptions raised by ``read`` are not handled.
        :raises MultipartError: Same as :meth:`parse`.

        .. versionadded:: 1.3
        """

        assert chunk_size > 0

        with self:
            readlimit = self.content_length
            while not self.closed:
                if readlimit >= 0:
                    chunk = read(min(chunk_size, readlimit))
                    readlimit -= len(chunk)
                else:
                    chunk = read(chunk_size)

                yield from self.parse(chunk)

    def close(self, check_complete=True):
        """
        Close this parser if not already closed.

        :param check_complete: Raise :exc:`ParserError` if the parser did not
            reach the end of the multipart stream yet.
        """

        self.closed = True
        del self._buffer[:]

        if check_complete and self._state is not _COMPLETE:
            err = ParserError("Unexpected end of multipart stream (parser closed)")
            if not self.error:
                self.error = err
            raise err


class MultipartSegment:
    """A :class:`MultipartSegment` represents the header section of a single
    multipart part and provides convenient access to part headers and other
    details (e.g. :attr:`name` and :attr:`filename`). Each segment also tracks
    its own content :attr:`size` while the :class:`PushMultipartParser`
    processes more data, and is marked as :attr:`complete` as soon as the next
    multipart border is found. Segments do not store or buffer any of their
    content data, though.
    """

    #: List of headers as name/value pairs with normalized (Title-Case) names.
    headerlist: List[Tuple[str, str]]
    #: The 'name' option of the `Content-Disposition` header. Always a string,
    #: but may be empty.
    name: str
    #: The optional 'filename' option of the `Content-Disposition` header.
    filename: Optional[str]
    #: The cleaned up `Content-Type` segment header, if present. The value is
    #: lower-cased and header options (e.g. charset) are removed.
    content_type: Optional[str]
    #: The 'charset' option of the `Content-Type` header, if present.
    charset: Optional[str]

    #: Segment body size (so far). Will be updated during parsing.
    size: int
    #: If true, the segment content was fully parsed and the size value is final.
    complete: bool

    def __init__(self, parser: PushMultipartParser):
        """Private constructor, used by :class:`PushMultipartParser`"""
        self._parser = parser

        if parser._fieldcount + 1 > parser.max_segment_count:
            raise ParserLimitReached("Maximum segment count exceeded")
        parser._fieldcount += 1

        self.headerlist = []
        self.size = 0
        self.complete = False

        self.name = None  # type: ignore
        self.filename = None
        self.content_type = None
        self.charset = None
        self._clen = -1
        self._size_limit = parser.max_segment_size

    def _add_headerline(self, line: t_ByteString):
        assert line and self.name is None
        parser = self._parser

        if line[0] in b" \t":  # Multi-line header value
            if not self.headerlist or parser.strict:
                raise StrictParserError("Unexpected segment header continuation")
            prev = ": ".join(self.headerlist.pop())
            line = prev.encode(parser.header_charset) + b" " + line.strip()

        if len(line) > parser.max_header_size:
            raise ParserLimitReached("Maximum segment header length exceeded")
        if len(self.headerlist) >= parser.max_header_count:
            raise ParserLimitReached("Maximum segment header count exceeded")

        try:
            name, col, value = line.decode(parser.header_charset).partition(":")
            name = name.strip()
            if not col or not name:
                raise ParserError("Malformed segment header")
            if name not in _HEADER_EXPECTED:
                if " " in name or not name.isascii() or not name.isprintable():
                    raise ParserError("Invalid segment header name")
        except UnicodeDecodeError as err:
            raise ParserError("Segment header failed to decode", err)

        self.headerlist.append((name.title(), value.strip()))

    def _close_headers(self):
        assert self.name is None

        for h, v in self.headerlist:
            if h == "Content-Disposition":
                dtype, args = parse_options_header(
                    v, unquote=content_disposition_unquote
                )
                if dtype != "form-data":
                    raise ParserError(
                        "Invalid Content-Disposition segment header: Wrong type"
                    )
                if "name" not in args and self._parser.strict:
                    raise StrictParserError(
                        "Invalid Content-Disposition segment header: Missing name option"
                    )
                self.name = args.get("name", "")
                self.filename = args.get("filename")
            elif h == "Content-Type":
                self.content_type, args = parse_options_header(v)
                self.charset = args.get("charset")
            elif h == "Content-Length":
                try:
                    content_length = int(v)
                    if content_length < 0 or str(content_length) != v:
                        raise ValueError("Not an unsigned ASCII decimal")
                    self._clen = content_length
                except ValueError:
                    pass # Will be an error in 1.4

        if self.name is None:
            raise ParserError("Missing Content-Disposition segment header")

    def _update_size(self, bytecount: int):
        assert self.name is not None and not self.complete
        self.size += bytecount
        if self._clen >= 0 and self.size > self._clen:
            raise ParserError("Segment Content-Length exceeded")
        if self.size > self._size_limit:
            raise ParserLimitReached("Maximum segment size exceeded")

    def _mark_complete(self):
        assert self.name is not None and not self.complete
        if self._clen >= 0 and self.size != self._clen:
            raise ParserError("Segment size does not match Content-Length header")
        self.complete = True

    def header(self, name: str, default=None):
        """Return the value of a header if present, or a default value."""
        compare = name.title()
        for header in self.headerlist:
            if header[0] == compare:
                return header[1]
        if default is KeyError:
            raise KeyError(name)
        return default

    def __getitem__(self, name):
        """Return a header value if present, or raise :exc:`KeyError`."""
        return self.header(name, KeyError)


##############################################################################
################################## Multipart #################################
##############################################################################


class MultipartParser:
    def __init__(
        self,
        stream,
        boundary,
        content_length=-1,
        charset="utf8",
        strict=False,
        buffer_size=1024 * 64,
        header_limit=8,
        headersize_limit=1024 * 4 + 128,  # 4KB
        part_limit=128,
        partsize_limit=inf,  # unlimited
        spool_limit=1024 * 64,  # Keep fields up to 64KB in memory
        memory_limit=1024 * 64 * 128,  # spool_limit * part_limit
        disk_limit=inf,  # unlimited
        mem_limit=0,
        memfile_limit=0,
    ):
        """A parser that reads from a `multipart/form-data` encoded byte stream
        and yields buffered :class:`MultipartPart` instances.

        The parse acts as a lazy iterator and will only read and parse as much
        data as needed to return the next part. Results are cached and the same
        part can be requested multiple times without extra cost.

        :param stream: A readable byte stream or any other object that implements
          a :meth:`read(size) <io.BufferedIOBase.read>` method.
        :param boundary: The multipart boundary as found in the Content-Type header.

        :param charset: Default charset for headers and text fields.
        :param strict: Enables additional format and sanity checks.
        :param buffer_size: Chunk size when reading from the source stream.

        :param header_limit: Maximum number of headers per part.
        :param headersize_limit: Maximum length of a single header line (name and value).
        :param part_limit: Maximum number of parts.
        :param partsize_limit: Maximum content size of a single parts.
        :param spool_limit: Parts up to this size are buffered in memory and count
          towards `memory_limit`. Larger parts are spooled to temporary files on
          disk and count towards `disk_limit`.
        :param memory_limit: Maximum size of all memory-buffered parts. Should
          be smaller than ``spool_limit * part_limit`` to have an effect.
        :param disk_limit: Maximum size of all disk-buffered parts.
        """
        self.stream = stream
        self.boundary = boundary
        self.content_length = content_length
        self.charset = charset
        self.strict = strict
        self.buffer_size = buffer_size
        self.header_limit = header_limit
        self.headersize_limit = headersize_limit
        self.part_limit = part_limit
        self.partsize_limit = partsize_limit
        self.memory_limit = mem_limit or memory_limit
        self.spool_limit = min(memfile_limit or spool_limit, self.memory_limit)
        self.disk_limit = disk_limit

        self._done = []
        self._part_iter = None

    def __iter__(self):
        """Parse the multipart stream and yield :class:`MultipartPart`
        instances as soon as they are available."""
        if not self._part_iter:
            self._part_iter = self._iterparse()

        if self._done:
            yield from self._done

        for part in self._part_iter:
            self._done.append(part)
            yield part

    def parts(self):
        """Parse the entire multipart stream and return all :class:`MultipartPart`
        instances as a list."""
        return list(self)

    def get(self, name, default=None):
        """Return the first part with a given name, or the default value if no
        matching part exists."""
        for part in self:
            if name == part.name:
                return part

        return default

    def get_all(self, name):
        """Return all parts with the given name."""
        return [p for p in self if p.name == name]

    def _iterparse(self):
        bufsize = self.buffer_size
        mem_used = disk_used = 0

        part = None
        parser = PushMultipartParser(
            boundary=self.boundary,
            content_length=self.content_length,
            max_header_count=self.header_limit,
            max_header_size=self.headersize_limit,
            max_segment_count=self.part_limit,
            max_segment_size=self.partsize_limit,
            header_charset=self.charset,
        )

        with parser:
            for event in parser.parse_blocking(self.stream.read, bufsize):
                if isinstance(event, MultipartSegment):
                    part = MultipartPart(
                        event,
                        buffer_size=self.buffer_size,
                        memfile_limit=self.spool_limit,
                        charset=self.charset,
                    )
                elif event:
                    assert part
                    part._write(event)
                    if part.is_buffered():
                        if part.size + mem_used > self.memory_limit:
                            raise ParserLimitReached("Memory limit reached")
                    elif part.size + disk_used > self.disk_limit:
                        raise ParserLimitReached("Disk limit reached")
                else:
                    assert part
                    if part.is_buffered():
                        mem_used += part.size
                    else:
                        disk_used += part.size
                    part._mark_complete()
                    yield part
                    part = None


class MultipartPart:
    """A :class:`MultipartPart` represents a fully parsed multipart part and
    provides convenient access to part headers and other details (e.g.
    :attr:`name` and :attr:`filename`) as well as its memory- or disk-buffered
    binary or text content.
    """

    def __init__(
        self,
        segment: "MultipartSegment",
        buffer_size=2**16,
        memfile_limit=2**18,
        charset="utf8",
    ):
        """Private constructor, used by :class:`MultipartParser`"""

        self._segment = segment
        #: A file-like buffer holding the parts binary content, or None if this
        #: part was :meth:`closed <close>`.
        self.file: Union[BytesIO, BufferedRandom, None] = BytesIO()
        #: Part size in bytes.
        self.size = 0
        #: Part name.
        self.name = segment.name
        #: Part filename (if defined).
        self.filename = segment.filename
        #: Charset as defined in the part header, or the parser default charset.
        self.charset = segment.charset or charset
        #: All part headers as a list of (name, value) pairs.
        self.headerlist = segment.headerlist

        self.memfile_limit = memfile_limit
        self.buffer_size = buffer_size

    @_cached_property
    def headers(self) -> Headers:
        """A convenient dict-like holding all part headers."""
        return Headers(self._segment.headerlist)

    @_cached_property
    def disposition(self) -> str:
        """The value of the `Content-Disposition` part header."""
        return self._segment.header("Content-Disposition")  # type: ignore

    @_cached_property
    def content_type(self) -> str:
        """Cleaned up content type provided for this part, or a sensible
        default (`application/octet-stream` for files and `text/plain` for
        text fields).
        """
        return self._segment.content_type or (
            "application/octet-stream" if self.filename else "text/plain"
        )

    def _write(self, chunk):
        self.size += len(chunk)
        self.file.write(chunk)  # type: ignore
        if self.size > self.memfile_limit:
            old = self.file
            self.file = tempfile.TemporaryFile()
            self.file.write(old.getvalue())  # type: ignore
            self._write = self._write_nocheck

    def _write_nocheck(self, chunk):
        self.size += len(chunk)
        self.file.write(chunk)  # type: ignore

    def _mark_complete(self):
        self.file.seek(0)  # type: ignore

    def is_buffered(self):
        """Return true if :attr:`file` is memory-buffered, or false if the part
        was larger than the `spool_limit` and content was spooled to
        temporary files on disk."""
        return isinstance(self.file, BytesIO)

    @property
    def value(self):
        """Return the entire payload as a decoded text string.

        Warning, this may consume a lot of memory, check :attr:`size` first.
        """

        return self.raw.decode(self.charset)

    @property
    def raw(self):
        """Return the entire payload as a raw byte string.

        Warning, this may consume a lot of memory, check :attr:`size` first.
        """
        if self.file is None:
            raise MultipartError("Cannot read from closed MultipartPart")

        pos = self.file.tell()
        self.file.seek(0)

        val = self.file.read()
        self.file.seek(pos)
        return val

    def save_as(self, path):
        """Save a copy of this part to `path` and return the number of bytes
        written.
        """
        if self.file is None:
            raise MultipartError("Cannot read from closed MultipartPart")

        with open(path, "wb") as fp:
            pos = self.file.tell()
            try:
                self.file.seek(0)
                size = copy_file(self.file, fp, buffer_size=self.buffer_size)
            finally:
                self.file.seek(pos)
        return size

    def close(self):
        """Close :attr:`file` and set it to `None` to free up resources."""
        if self.file:
            self.file.close()
            self.file = None


##############################################################################
#################################### WSGI ####################################
##############################################################################


def is_form_request(environ):
    """Return True if the environ represents a form request that can be parsed
    with :func:`parse_form_data`. Checks for a compatible `Content-Type`
    header.
    """

    content_type = environ.get("CONTENT_TYPE", "")
    return content_type.split(";", 1)[0].strip().lower() in (
        "multipart/form-data",
        "application/x-www-form-urlencoded",
        "application/x-url-encoded",
    )


def parse_form_data(
    environ, charset="utf8", strict=False, ignore_errors=None, **kwargs
):
    """Parses both types of form data (multipart and url-encoded) from a WSGI
    environment and returns two :class:`MultiDict` instances, one for text form
    fields (strings) and one for file uploads (:class:`MultipartPart`
    instances). Text fields that are too big to fit into memory limits are
    treated as file uploads with no filename.

    In case of an url-encoded form request, the total request body size is
    limited by `memory_limit`. Larger requests will trigger an error.

    :param environ: A WSGI environment dictionary. Only `wsgi.input`,
        `CONTENT_TYPE` and `CONTENT_LENGTH` are used.
    :param charset: The default charset used to decode headers and text fields.
    :param strict: Enables additional format and sanity checks.
    :param ignore_errors: If True, suppress all exceptions. The returned
        results may be empty or incomplete. If False, then exceptions are
        not suppressed. A value of None (default) throws exceptions in
        strict mode but suppresses errors in non-strict mode.
    :param kwargs: Additional keyword arguments are forwarded to
        :class:`MultipartParser`. This is particularly useful to change the
        default parser limits.
    :raises MultipartError: See `ignore_errors` parameters.
    """

    forms, files = MultiDict(), MultiDict()

    try:
        stream = environ.get("wsgi.input")
        if not stream:
            if strict:
                raise StrictParserError("No 'wsgi.input' in WSGI environment")
            stream = BytesIO()

        content_type = environ.get("CONTENT_TYPE", "")
        if not content_type:
            if strict:
                raise StrictParserError("Missing Content-Type header")
            return forms, files

        try:
            content_length = int(environ.get("CONTENT_LENGTH", -1))
        except ValueError:
            raise ParserError("Invalid Content-Length header")

        content_type, options = parse_options_header(content_type)
        kwargs["charset"] = charset = options.get("charset", charset)

        if content_type == "multipart/form-data":
            boundary = options.get("boundary", "")

            if not boundary:
                raise ParserError("Missing boundary for multipart/form-data")

            for part in MultipartParser(stream, boundary, content_length, **kwargs):
                if part.filename or not part.is_buffered():
                    files.append(part.name, part)
                else:  # TODO: Big form-fields go into the files dict. Really?
                    forms.append(part.name, part.value)
                    part.close()

        elif content_type in (
            "application/x-www-form-urlencoded",
            "application/x-url-encoded",
        ):
            mem_limit = kwargs.get(
                "memory_limit", kwargs.get("mem_limit", 1024 * 64 * 128)
            )
            if content_length > -1:
                if content_length > mem_limit:
                    raise ParserLimitReached("Memory limit exceeded")
                data = stream.read(min(mem_limit, content_length))
                if len(data) < content_length:
                    raise ParserError("Unexpected end of data stream")
            else:
                data = stream.read(mem_limit + 1)
                if len(data) > mem_limit:
                    raise ParserLimitReached("Memory limit exceeded")

            data = data.decode(charset)
            data = parse_qs(data, keep_blank_values=True, encoding=charset)

            for key, values in data.items():
                for value in values:
                    forms.append(key, value)
        elif strict:
            raise StrictParserError("Unsupported Content-Type")

    except MultipartError:
        if ignore_errors is None:
            ignore_errors = not strict
        if not ignore_errors:
            for _, part in files.iterallitems():
                part.close()
            raise

    return forms, files

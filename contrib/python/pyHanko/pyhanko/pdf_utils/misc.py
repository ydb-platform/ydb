"""
Utility functions for PDF library.
Taken from PyPDF2 with modifications and additions, see
:ref:`here <pypdf2-license>` for the original license of the PyPDF2 project.

Generally, all of these constitute internal API, except for the exception
classes.
"""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from io import BytesIO
from typing import Callable, Generator, Iterable, Optional, TypeVar, Union

from pyhanko_certvalidator.util import CancelableAsyncIterator, ConsList

__all__ = [
    'PdfError',
    'PdfReadError',
    'PdfStrictReadError',
    'PdfWriteError',
    'PdfStreamError',
    'IndirectObjectExpected',
    'get_and_apply',
    'OrderedEnum',
    'StringWithLanguage',
    'is_regular_character',
    'read_non_whitespace',
    'read_until_whitespace',
    'read_until_delimiter',
    'read_until_regex',
    'skip_over_whitespace',
    'skip_over_comment',
    'instance_test',
    'peek',
    'assert_writable_and_random_access',
    'prepare_rw_output_stream',
    'finalise_output',
    'DEFAULT_CHUNK_SIZE',
    'chunked_write',
    'chunked_digest',
    'chunk_stream',
    'ConsList',
    'Singleton',
    'rd',
    'isoparse',
    'lift_iterable_async',
]

DEFAULT_CHUNK_SIZE = 4096
"""
Default chunk size for stream I/O.
"""

rd = lambda x: round(x, 4)


def instance_test(cls):
    return lambda x: isinstance(x, cls)


def pair_iter(lst):
    i = iter(lst)
    while True:
        try:
            x1 = next(i)
        except StopIteration:
            return
        try:
            x2 = next(i)
        except StopIteration:
            raise ValueError('List has odd number of elements')
        yield x1, x2


PDF_WHITESPACE = b' \n\r\t\f\x00'
PDF_DELIMITERS = b'()<>[]{}/%'


def read_until_whitespace(stream, maxchars: Optional[int] = None) -> bytes:
    """
    Reads non-whitespace characters and returns them.
    Stops upon encountering whitespace, or, if ``maxchars`` is not ``None``,
    when maxchars is reached.

    :param stream:
        stream to read
    :param maxchars:
        maximal number of bytes to read before returning
    """

    return _read_until_class(PDF_WHITESPACE, stream, maxchars=maxchars)


def read_until_delimiter(stream) -> bytes:
    """
    Read until a token delimiter (i.e. a delimiter character or a PDF
    whitespace character) is encountered, and rewind the stream to the previous
    character.

    :param stream:
        A stream.
    :return:
        The bytes read.
    """
    result = _read_until_class(PDF_WHITESPACE + PDF_DELIMITERS, stream)
    stream.seek(-1, os.SEEK_CUR)
    return result


def _read_until_class(class_chars: bytes, stream, maxchars=None) -> bytes:
    if maxchars == 0:
        return b''

    def _build():
        stop_at = None if maxchars is None else stream.tell() + maxchars
        while maxchars is None or stream.tell() < stop_at:
            tok = stream.read(1)
            if tok in class_chars or not tok:
                break
            yield tok

    return b''.join(_build())


def is_regular_character(byte_value: int):
    return byte_value not in PDF_WHITESPACE and byte_value not in PDF_DELIMITERS


def read_non_whitespace(stream, seek_back=False, allow_eof=False):
    """
    Finds and reads the next non-whitespace character (ignores whitespace).
    """
    tok = PDF_WHITESPACE[0]
    while True:
        while tok in PDF_WHITESPACE:
            if not tok:
                if allow_eof:
                    return b''
                else:
                    raise PdfStreamError('Stream ended prematurely')
            tok = stream.read(1)
        # Deal with comments
        if tok != b'%':
            break
        else:
            stream.seek(-1, os.SEEK_CUR)
            skip_over_comment(stream)
            tok = PDF_WHITESPACE[0]
    if seek_back:
        stream.seek(-1, os.SEEK_CUR)
    return tok


def skip_over_whitespace(stream, stop_after_eol=False) -> bool:
    """
    Similar to :func:`read_non_whitespace`, but returns a ``bool`` if more than
    one whitespace character was read.

    Will return the cursor to before the first non-whitespace character
    encountered, or after the first end-of-line sequence if one is encountered.
    """
    tok = PDF_WHITESPACE[0]
    cnt = 0
    while tok in PDF_WHITESPACE:
        tok = stream.read(1)
        if not tok:
            raise PdfStreamError("Stream ended prematurely")
        cnt += 1
        if stop_after_eol:
            if tok == b'\n':
                return cnt > 1
            elif tok == b'\r':
                # read the next char and check if it's a LF (or EOF)
                nxt = stream.read(1)
                if nxt == b'\n' or not nxt:
                    return cnt > 1
                # if not, break here; we need to seek back one position
                # (CR by itself also counts as an EOL sequence)
                break

    stream.seek(-1, os.SEEK_CUR)
    return cnt > 1


def skip_over_comment(stream) -> bool:
    """
    Skip over a comment and position the cursor at the first byte after
    the EOL sequence following the comment. If there is no comment under
    the cursor, do nothing.

    :param stream:
        stream to read
    :return:
        ``True`` if a comment was read.
    """
    tok = stream.read(1)
    stream.seek(-1, 1)
    if tok == b'%':
        while tok not in (b'\n', b'\r', b''):
            tok = stream.read(1)
        # read the next char and check if it's a LF (or EOF)
        nxt = stream.read(1)
        if nxt and nxt != b'\n':
            # ...if not, rewind
            stream.seek(-1, os.SEEK_CUR)
        return True
    return False


def read_until_regex(stream, regex, ignore_eof: bool = False):
    """
    Reads until the regular expression pattern matched (ignore the match)
    Raise :class:`PdfStreamError` on premature end-of-file.

    :param stream:
        stream to search
    :param regex:
        regex to match
    :param ignore_eof:
        if true, ignore end-of-line and return immediately
    :raises PdfStreamError:
        on premature EOF
    """
    name = b''
    while True:
        tok = stream.read(16)
        if not tok:
            # stream has truncated prematurely
            if ignore_eof:
                return name
            else:
                raise PdfStreamError("Stream has ended unexpectedly")
        m = regex.search(tok)
        if m is not None:
            name += tok[: m.start()]
            stream.seek(m.start() - len(tok), 1)
            break
        name += tok
    return name


@dataclass(frozen=True)
class StringWithLanguage:
    """
    A string with a language attached to it.
    """

    value: str
    lang_code: Optional[str] = None
    country_code: Optional[str] = None

    def __str__(self):
        return self.value


class PdfError(Exception):
    def __init__(self, msg: str, *args):
        self.msg = msg
        super().__init__(msg, *args)


class PdfReadError(PdfError):
    pass


class PdfStrictReadError(PdfReadError):
    pass


class IndirectObjectExpected(PdfReadError):
    def __init__(self, msg: Optional[str] = None):
        super().__init__(msg=msg or "indirect object expected")


class PdfWriteError(PdfError):
    pass


class PdfStreamError(PdfReadError):
    pass


def peek(itr):
    itr = iter(itr)
    first = next(itr)

    def _itr():
        yield first
        yield from itr

    return first, _itr()


class OrderedEnum(Enum):
    """
    Ordered enum (from the Python documentation)
    """

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        raise NotImplementedError

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        raise NotImplementedError

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        raise NotImplementedError

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        raise NotImplementedError


class VersionEnum(Enum):
    """
    Ordered enum with support for ``None``, for future-proofing version-based
    enums. In such enums, the value ``None`` can be used as a stand-in for
    "any future version".
    """

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            val = self.value
            other_val = other.value
            if val is None:
                return True
            elif other_val is None:
                return False
            else:
                return val >= other_val
        raise NotImplementedError

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            val = self.value
            other_val = other.value
            if val is None:
                return other_val is not None
            elif other_val is None:
                return False
            else:
                return val > other_val
        raise NotImplementedError

    def __le__(self, other):
        if self.__class__ is other.__class__:
            val = self.value
            other_val = other.value
            if other_val is None:
                return True
            elif val is None:
                return False
            else:
                return val <= other_val
        raise NotImplementedError

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            val = self.value
            other_val = other.value
            if other_val is None:
                return val is not None
            elif val is None:
                return False
            else:
                return val < other_val
        raise NotImplementedError


class LazyJoin:
    def __init__(self, sep, iterator):
        self.sep = sep
        self.iterator = iterator

    def __str__(self):
        return self.sep.join(self.iterator)


def get_and_apply(dictionary: dict, key, function: Callable, *, default=None):
    try:
        value = dictionary[key]
    except KeyError:
        return default
    return function(value)


X = TypeVar('X')
Y = TypeVar('Y')
R = TypeVar('R')


def map_with_return(
    gen: Generator[X, None, R], func: Callable[[X], Y]
) -> Generator[Y, None, R]:
    while True:
        try:
            yield func(next(gen))
        except StopIteration as e:
            return e.value


# type checker trick
def _as_gen(x: Iterable[X]) -> Generator[X, None, None]:
    yield from x


def chunk_stream(
    temp_buffer: Union[bytearray, memoryview], stream, max_read=None
) -> Iterable[Union[bytearray, memoryview]]:
    total_read = 0
    while max_read is None or total_read < max_read:
        # clamp the input buffer if necessary
        read_buffer = temp_buffer
        if max_read is not None:
            to_read = max_read - total_read
            if to_read < len(temp_buffer):
                read_buffer = memoryview(temp_buffer)[:to_read]
        bytes_read = stream.readinto(read_buffer)
        total_read += bytes_read
        if not bytes_read:
            return

        # clamp the output as well, if necessary
        to_feed: Union[bytearray, memoryview]
        if bytes_read < len(read_buffer):
            to_feed = memoryview(read_buffer)[:bytes_read]
        else:
            to_feed = read_buffer
        yield to_feed


def chunked_digest(temp_buffer: bytearray, stream, md, max_read=None):
    for chunk in chunk_stream(temp_buffer, stream, max_read=max_read):
        md.update(chunk)


def chunked_write(temp_buffer: bytearray, stream, output, max_read=None):
    for chunk in chunk_stream(temp_buffer, stream, max_read=max_read):
        output.write(chunk)


class Singleton(type):
    def __new__(mcs, name, bases, dct):
        cls = type.__new__(mcs, name, bases, dct)
        instance = type.__call__(cls)
        cls.__new__ = lambda _: instance
        return cls


def assert_writable_and_random_access(output):
    """
    Raise an error if the buffer in question is not writable, and return
    a boolean to indicate whether it supports random-access reading.

    :param output:
    :return:
    """
    # Rationale for the explicit writability check:
    #  If the output buffer is not readable or not seekable, it's
    #  about to be replaced with a BytesIO instance, and in that
    #  case, the write error would only happen *after* the signing/updating
    #  operations are done. We want to avoid that scenario.
    if not output.writable():
        raise IOError("Output buffer is not writable")  # pragma: nocover
    return output.seekable() and output.readable()


def prepare_rw_output_stream(output):
    """
    Prepare an output stream that supports both reading and writing.
    Intended to be used for writing & updating signed files:
    when producing a signature, we render the PDF to a byte buffer with
    placeholder values for the signature data, or straight to the provided
    output stream if possible.

    More precisely: this function will return the original output stream
    if it is writable, readable and seekable.
    If the ``output`` parameter is ``None``, not readable or not seekable,
    this function will return a :class:`.BytesIO` instance instead.
    If the ``output`` parameter is not ``None`` and not writable,
    :class:`.IOError` will be raised.

    :param output:
        A writable file-like object, or ``None``.
    :return:
        A file-like object that supports reading, writing and seeking.
    """
    if output is None:
        output = BytesIO()
    else:
        if not assert_writable_and_random_access(output):
            output = BytesIO()

    return output


def finalise_output(orig_output, returned_output):
    """
    Several internal APIs transparently replaces non-readable/seekable
    buffers with BytesIO for signing operations, but we don't want to
    expose that to the public API user.
    This internal API function handles the unwrapping.
    """

    if orig_output is not None and orig_output is not returned_output:
        # original output is a write-only buffer
        assert isinstance(returned_output, BytesIO)
        raw_buf = returned_output.getbuffer()
        orig_output.write(raw_buf)
        raw_buf.release()
        return orig_output
    return returned_output


def _get_isoparse() -> Callable[[str], datetime]:
    try:
        # Try to import the ISO parser from dateutil, if available
        from dateutil.parser import isoparse as parse

        return parse
    except ImportError:  # pragma: nocover
        # if not, call fromisoformat in the standard library
        # (only implements a subset of ISO 8601)
        return datetime.fromisoformat


def isoparse(dt_str: str) -> datetime:
    parse: Callable[[str], datetime] = _get_isoparse()
    dt = parse(dt_str)
    if dt.tzinfo is None:
        # assume UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class _LiftedIterable(CancelableAsyncIterator[X]):
    async def __anext__(self) -> X:
        return next(self.i)

    async def cancel(self):
        return

    def __init__(self, i: Iterable[X]):
        self.i = iter(i)


def lift_iterable_async(i: Iterable[X]) -> CancelableAsyncIterator[X]:
    return _LiftedIterable(i)


def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg
    return None

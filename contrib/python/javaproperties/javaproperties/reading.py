from __future__ import annotations
from collections.abc import Callable, Iterator
from io import BytesIO, StringIO
import re
from typing import Any, IO, Iterable, TypeVar, overload
from .util import CONTINUED_RGX, ascii_splitlines

T = TypeVar("T")


@overload
def load(fp: IO) -> dict[str, str]: ...


@overload
def load(fp: IO, object_pairs_hook: type[T]) -> T: ...


@overload
def load(fp: IO, object_pairs_hook: Callable[[Iterator[tuple[str, str]]], T]) -> T: ...


def load(fp, object_pairs_hook=dict):  # type: ignore[no-untyped-def]
    """
    Parse the contents of the `~io.IOBase.readline`-supporting file-like object
    ``fp`` as a simple line-oriented ``.properties`` file and return a `dict`
    of the key-value pairs.

    ``fp`` may be either a text or binary filehandle, with or without universal
    newlines enabled.  If it is a binary filehandle, its contents are decoded
    as Latin-1.

    By default, the key-value pairs extracted from ``fp`` are combined into a
    `dict` with later occurrences of a key overriding previous occurrences of
    the same key.  To change this behavior, pass a callable as the
    ``object_pairs_hook`` argument; it will be called with one argument, a
    generator of ``(key, value)`` pairs representing the key-value entries in
    ``fp`` (including duplicates) in order of occurrence.  `load` will then
    return the value returned by ``object_pairs_hook``.

    .. versionchanged:: 0.5.0
        Invalid ``\\uXXXX`` escape sequences will now cause an
        `InvalidUEscapeError` to be raised

    :param IO fp: the file from which to read the ``.properties`` document
    :param callable object_pairs_hook: class or function for combining the
        key-value pairs
    :rtype: `dict` of text strings or the return value of ``object_pairs_hook``
    :raises InvalidUEscapeError: if an invalid ``\\uXXXX`` escape sequence
        occurs in the input
    """
    return object_pairs_hook(
        (kv.key, kv.value) for kv in parse(fp) if isinstance(kv, KeyValue)
    )


@overload
def loads(s: str | bytes) -> dict[str, str]: ...


@overload
def loads(s: str | bytes, object_pairs_hook: type[T]) -> T: ...


@overload
def loads(
    s: str | bytes, object_pairs_hook: Callable[[Iterator[tuple[str, str]]], T]
) -> T: ...


def loads(s, object_pairs_hook=dict):  # type: ignore[no-untyped-def]
    """
    Parse the contents of the string ``s`` as a simple line-oriented
    ``.properties`` file and return a `dict` of the key-value pairs.

    ``s`` may be either a text string or bytes string.  If it is a bytes
    string, its contents are decoded as Latin-1.

    By default, the key-value pairs extracted from ``s`` are combined into a
    `dict` with later occurrences of a key overriding previous occurrences of
    the same key.  To change this behavior, pass a callable as the
    ``object_pairs_hook`` argument; it will be called with one argument, a
    generator of ``(key, value)`` pairs representing the key-value entries in
    ``s`` (including duplicates) in order of occurrence.  `loads` will then
    return the value returned by ``object_pairs_hook``.

    .. versionchanged:: 0.5.0
        Invalid ``\\uXXXX`` escape sequences will now cause an
        `InvalidUEscapeError` to be raised

    :param Union[str,bytes] s: the string from which to read the
        ``.properties`` document
    :param callable object_pairs_hook: class or function for combining the
        key-value pairs
    :rtype: `dict` of text strings or the return value of ``object_pairs_hook``
    :raises InvalidUEscapeError: if an invalid ``\\uXXXX`` escape sequence
        occurs in the input
    """
    fp = BytesIO(s) if isinstance(s, bytes) else StringIO(s)
    return load(fp, object_pairs_hook=object_pairs_hook)


TIMESTAMP_RGX = re.compile(
    r"\A[ \t\f]*[#!][ \t\f]*"
    r"(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat)"
    r" (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    r" (?:[012][0-9]|3[01])"
    r" (?:[01][0-9]|2[0-3]):[0-5][0-9]:(?:[0-5][0-9]|6[01])"
    r" (?:[A-Za-z_0-9]{3})?"
    r" [0-9]{4,}"
    r"[ \t\f]*\r?\n?\Z"
)


class PropertiesElement(Iterable[str]):
    """
    .. versionadded:: 0.7.0

    Superclass of objects returned by `parse()`
    """

    def __init__(self, source: str) -> None:
        #: The raw, unmodified input line (including trailing newlines)
        self.source: str = source

    def __iter__(self) -> Iterator[str]:
        return iter((self.source,))

    def __eq__(self, other: Any) -> bool:
        if type(self) is type(other):
            return tuple(self) == tuple(other)
        else:
            return NotImplemented

    def __repr__(self) -> str:
        return "{0.__module__}.{0.__name__}(source={1.source!r})".format(
            type(self), self
        )

    @property
    def source_stripped(self) -> str:
        """
        Like `source`, but with the final trailing newline and line
        continuation (if any) removed
        """
        s = self.source.rstrip("\r\n")
        if CONTINUED_RGX.search(s):
            s = s[:-1]
        return s

    def _with_source(self, newsource: str) -> PropertiesElement:
        return type(self)(source=newsource)


class Comment(PropertiesElement):
    """
    .. versionadded:: 0.7.0

    Subclass of `PropertiesElement` representing a comment
    """

    @property
    def value(self) -> str:
        """
        Returns the contents of the comment, with the comment marker, any
        whitespace leading up to it, and the trailing newline removed
        """
        s = self.source.lstrip(" \t\f")
        if s.startswith(("#", "!")):
            s = s[1:]
        return s.rstrip("\r\n")

    @property
    def source_stripped(self) -> str:
        """
        Like `source`, but with the final trailing newline (if any) removed
        """
        return self.source.rstrip("\r\n")

    def is_timestamp(self) -> bool:
        """
        Returns `True` iff the comment's value appears to be a valid timestamp
        as produced by Java 8's ``Date.toString()``
        """
        return bool(TIMESTAMP_RGX.fullmatch(self.source))


class Whitespace(PropertiesElement):
    """
    .. versionadded:: 0.7.0

    Subclass of `PropertiesElement` representing a line that is either empty or
    contains only whitespace (and possibly some line continuations)
    """


class KeyValue(PropertiesElement):
    """
    .. versionadded:: 0.7.0

    Subclass of `PropertiesElement` representing a key-value entry
    """

    def __init__(self, key: str, value: str, source: str):
        super().__init__(source=source)
        #: The entry's key, after processing escape sequences
        self.key: str = key
        #: The entry's value, after processing escape sequences
        self.value: str = value

    def __iter__(self) -> Iterator[str]:
        return iter((self.key, self.value, self.source))

    def __repr__(self) -> str:
        return (
            "{0.__module__}.{0.__name__}(key={1.key!r}, value={1.value!r},"
            " source={1.source!r})".format(type(self), self)
        )

    def _with_source(self, newsource: str) -> KeyValue:
        return type(self)(key=self.key, value=self.value, source=newsource)


COMMENT_RGX = re.compile(r"^[ \t\f]*[#!]")
BLANK_RGX = re.compile(r"^[ \t\f]*\r?\n?\Z")
SEPARATOR_RGX = re.compile(r"(?<!\\)(?:\\\\)*([ \t\f]*[=:]|[ \t\f])[ \t\f]*")


def parse(src: IO | str | bytes) -> Iterator[PropertiesElement]:
    """
    Parse the given data as a simple line-oriented ``.properties`` file and
    return a generator of `PropertiesElement` objects representing the
    key-value pairs (as `KeyValue` objects), comments (as `Comment` objects),
    and blank lines (as `Whitespace` objects) in the input in order of
    occurrence.

    If the same key appears multiple times in the input, a separate `KeyValue`
    object is emitted for each entry.

    ``src`` may be a text string, a bytes string, or a text or binary
    filehandle/file-like object supporting the `~io.IOBase.readline` method
    (with or without universal newlines enabled).  Bytes input is decoded as
    Latin-1.

    .. versionchanged:: 0.5.0
        Invalid ``\\uXXXX`` escape sequences will now cause an
        `InvalidUEscapeError` to be raised

    .. versionchanged:: 0.7.0
        `parse()` now accepts strings as input, and it now returns a generator
        of custom objects instead of triples of strings

    :param src: the ``.properties`` document
    :type src: string or file-like object
    :rtype: Iterator[PropertiesElement]
    :raises InvalidUEscapeError: if an invalid ``\\uXXXX`` escape sequence
        occurs in the input
    """
    liter: Iterator[str]
    if isinstance(src, bytes):
        liter = iter(ascii_splitlines(src.decode("iso-8859-1")))
    elif isinstance(src, str):
        liter = iter(ascii_splitlines(src))
    else:
        fp: IO = src

        def lineiter() -> Iterator[str]:
            while True:
                line = fp.readline()
                ll: str
                if isinstance(line, bytes):
                    ll = line.decode("iso-8859-1")
                else:
                    ll = line
                if ll == "":
                    return
                for ln in ascii_splitlines(ll):
                    yield ln

        liter = lineiter()
    for source in liter:
        line = source
        if COMMENT_RGX.match(line):
            yield Comment(source)
            continue
        elif BLANK_RGX.match(line):
            yield Whitespace(source)
            continue
        line = line.lstrip(" \t\f").rstrip("\r\n")
        while CONTINUED_RGX.search(line):
            line = line[:-1]
            nextline = next(liter, "")
            source += nextline
            line += nextline.lstrip(" \t\f").rstrip("\r\n")
        if line == "":  # series of otherwise-blank lines with continuations
            yield Whitespace(source)
            continue
        m = SEPARATOR_RGX.search(line)
        if m:
            yield KeyValue(
                unescape(line[: m.start(1)]),
                unescape(line[m.end() :]),
                source,
            )
        else:
            yield KeyValue(unescape(line), "", source)


SURROGATE_PAIR_RGX = re.compile(r"[\uD800-\uDBFF][\uDC00-\uDFFF]")
ESCAPE_RGX = re.compile(r"\\(u.{0,4}|.)")
U_ESCAPE_RGX = re.compile(r"^u[0-9A-Fa-f]{4}\Z")


def unescape(field: str) -> str:
    """
    Decode escape sequences in a ``.properties`` key or value.  The following
    escape sequences are recognized::

        \\t \\n \\f \\r \\uXXXX \\\\

    If a backslash is followed by any other character, the backslash is
    dropped.

    In addition, any valid UTF-16 surrogate pairs in the string after
    escape-decoding are further decoded into the non-BMP characters they
    represent.  (Invalid & isolated surrogate code points are left as-is.)

    .. versionchanged:: 0.5.0
        Invalid ``\\uXXXX`` escape sequences will now cause an
        `InvalidUEscapeError` to be raised

    :param str field: the string to decode
    :rtype: str
    :raises InvalidUEscapeError: if an invalid ``\\uXXXX`` escape sequence
        occurs in the input
    """
    return SURROGATE_PAIR_RGX.sub(_unsurrogate, ESCAPE_RGX.sub(_unesc, field))


_unescapes = {"t": "\t", "n": "\n", "f": "\f", "r": "\r"}


def _unesc(m: re.Match[str]) -> str:
    esc = m.group(1)
    if esc[0] == "u":
        if not U_ESCAPE_RGX.match(esc):
            # We can't rely on `int` failing, because it succeeds when `esc`
            # has trailing whitespace or a leading minus.
            raise InvalidUEscapeError("\\" + esc)
        return chr(int(esc[1:], 16))
    else:
        return _unescapes.get(esc, esc)


def _unsurrogate(m: re.Match[str]) -> str:
    c, d = map(ord, m.group())
    uord = ((c - 0xD800) << 10) + (d - 0xDC00) + 0x10000
    return chr(uord)


class InvalidUEscapeError(ValueError):
    """
    .. versionadded:: 0.5.0

    Raised when an invalid ``\\uXXXX`` escape sequence (i.e., a ``\\u`` not
    immediately followed by four hexadecimal digits) is encountered in a simple
    line-oriented ``.properties`` file
    """

    def __init__(self, escape: str) -> None:
        #: The invalid ``\uXXXX`` escape sequence encountered
        self.escape: str = escape

    def __str__(self) -> str:
        return "Invalid \\u escape sequence: " + self.escape

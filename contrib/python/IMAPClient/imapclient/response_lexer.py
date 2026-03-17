# Copyright (c) 2014, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

"""
A lexical analyzer class for IMAP responses.

Although Lexer does all the work, TokenSource is the class to use for
external callers.
"""

from typing import Iterator, List, Optional, Tuple, TYPE_CHECKING, Union

from .util import assert_imap_protocol

__all__ = ["TokenSource"]

CTRL_CHARS = frozenset(c for c in range(32))
ALL_CHARS = frozenset(c for c in range(256))
SPECIALS = frozenset(c for c in b' ()%"[')
NON_SPECIALS = ALL_CHARS - SPECIALS - CTRL_CHARS
WHITESPACE = frozenset(c for c in b" \t\r\n")

BACKSLASH = ord("\\")
OPEN_SQUARE = ord("[")
CLOSE_SQUARE = ord("]")
DOUBLE_QUOTE = ord('"')


class TokenSource:
    """
    A simple iterator for the Lexer class that also provides access to
    the current IMAP literal.
    """

    def __init__(self, text: List[bytes]):
        self.lex = Lexer(text)
        self.src = iter(self.lex)

    @property
    def current_literal(self) -> Optional[bytes]:
        if TYPE_CHECKING:
            assert self.lex.current_source is not None
        return self.lex.current_source.literal

    def __iter__(self) -> Iterator[bytes]:
        return self.src


class Lexer:
    """
    A lexical analyzer class for IMAP
    """

    def __init__(self, text: List[bytes]):
        self.sources = (LiteralHandlingIter(chunk) for chunk in text)
        self.current_source: Optional[LiteralHandlingIter] = None

    def read_until(
        self, stream_i: "PushableIterator", end_char: int, escape: bool = True
    ) -> bytearray:
        token = bytearray()
        try:
            for nextchar in stream_i:
                if escape and nextchar == BACKSLASH:
                    escaper = nextchar
                    nextchar = next(stream_i)
                    if nextchar not in (escaper, end_char):
                        token.append(escaper)  # Don't touch invalid escaping
                elif nextchar == end_char:
                    break
                token.append(nextchar)
            else:
                raise ValueError("No closing '%s'" % chr(end_char))
        except StopIteration:
            raise ValueError("No closing '%s'" % chr(end_char))
        token.append(end_char)
        return token

    def read_token_stream(self, stream_i: "PushableIterator") -> Iterator[bytearray]:
        whitespace = WHITESPACE
        wordchars = NON_SPECIALS
        read_until = self.read_until

        while True:
            # Whitespace
            for nextchar in stream_i:
                if nextchar not in whitespace:
                    stream_i.push(nextchar)
                    break  # done skipping over the whitespace

            # Non-whitespace
            token = bytearray()
            for nextchar in stream_i:
                if nextchar in wordchars:
                    token.append(nextchar)
                elif nextchar == OPEN_SQUARE:
                    token.append(nextchar)
                    token.extend(read_until(stream_i, CLOSE_SQUARE, escape=False))
                else:
                    if nextchar in whitespace:
                        yield token
                    elif nextchar == DOUBLE_QUOTE:
                        assert_imap_protocol(not token)
                        token.append(nextchar)
                        token.extend(read_until(stream_i, nextchar))
                        yield token
                    else:
                        # Other punctuation, eg. "(". This ends the current token.
                        if token:
                            yield token
                        yield bytearray([nextchar])
                    break
            else:
                if token:
                    yield token
                break

    def __iter__(self) -> Iterator[bytes]:
        for source in self.sources:
            self.current_source = source
            for tok in self.read_token_stream(iter(source)):
                yield bytes(tok)


# imaplib has poor handling of 'literals' - it both fails to remove the
# {size} marker, and fails to keep responses grouped into the same logical
# 'line'.  What we end up with is a list of response 'records', where each
# record is either a simple string, or tuple of (str_with_lit, literal) -
# where str_with_lit is a string with the {xxx} marker at its end.  Note
# that each element of this list does *not* correspond 1:1 with the
# untagged responses.
# (http://bugs.python.org/issue5045 also has comments about this)
# So: we have a special object for each of these records.  When a
# string literal is processed, we peek into this object to grab the
# literal.
class LiteralHandlingIter:
    def __init__(self, resp_record: Union[Tuple[bytes, bytes], bytes]):
        self.literal: Optional[bytes]
        if isinstance(resp_record, tuple):
            # A 'record' with a string which includes a literal marker, and
            # the literal itself.
            self.src_text = resp_record[0]
            assert_imap_protocol(self.src_text.endswith(b"}"), self.src_text)
            self.literal = resp_record[1]
        else:
            # just a line with no literals.
            self.src_text = resp_record
            self.literal = None

    def __iter__(self) -> "PushableIterator":
        return PushableIterator(self.src_text)


class PushableIterator:
    NO_MORE = object()

    def __init__(self, it: bytes):
        self.it = iter(it)
        self.pushed: List[int] = []

    def __iter__(self) -> "PushableIterator":
        return self

    def __next__(self) -> int:
        if self.pushed:
            return self.pushed.pop()
        return next(self.it)

    # For Python 2 compatibility
    next = __next__

    def push(self, item: int) -> None:
        self.pushed.append(item)

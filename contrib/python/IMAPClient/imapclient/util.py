# Copyright (c) 2015, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

import logging
from typing import Iterator, Optional, Tuple, Union

from . import exceptions

logger = logging.getLogger(__name__)


def to_unicode(s: Union[bytes, str]) -> str:
    if isinstance(s, bytes):
        try:
            return s.decode("ascii")
        except UnicodeDecodeError:
            logger.warning(
                "An error occurred while decoding %s in ASCII 'strict' mode. Fallback to "
                "'ignore' errors handling, some characters might have been stripped",
                s,
            )
            return s.decode("ascii", "ignore")
    return s


def to_bytes(s: Union[bytes, str], charset: str = "ascii") -> bytes:
    if isinstance(s, str):
        return s.encode(charset)
    return s


def assert_imap_protocol(condition: bool, message: Optional[bytes] = None) -> None:
    if not condition:
        msg = "Server replied with a response that violates the IMAP protocol"
        if message:
            # FIXME(jlvillal): This looks wrong as it repeats `msg` twice
            msg += "{}: {}".format(
                msg, message.decode(encoding="ascii", errors="ignore")
            )
        raise exceptions.ProtocolError(msg)


_TupleAtomPart = Union[None, int, bytes]
_TupleAtom = Tuple[Union[_TupleAtomPart, "_TupleAtom"], ...]


def chunk(lst: _TupleAtom, size: int) -> Iterator[_TupleAtom]:
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

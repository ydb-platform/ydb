# Copyright (c) 2014, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

"""
Parsing for IMAP command responses with focus on FETCH responses as
returned by imaplib.

Initially inspired by http://effbot.org/zone/simple-iterator-parser.htm
"""

# TODO more exact error reporting

import datetime
import re
import sys
from collections import defaultdict
from typing import cast, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union

from .datetime_util import parse_to_datetime
from .exceptions import ProtocolError
from .response_lexer import TokenSource
from .response_types import Address, BodyData, Envelope, SearchIds
from .typing_imapclient import _Atom

__all__ = ["parse_response", "parse_message_list"]


def parse_response(data: List[bytes]) -> Tuple[_Atom, ...]:
    """Pull apart IMAP command responses.

    Returns nested tuples of appropriately typed objects.
    """
    if data == [None]:
        return tuple()
    return tuple(gen_parsed_response(data))


_msg_id_pattern = re.compile(r"(\d+(?: +\d+)*)")


def parse_message_list(data: List[Union[bytes, str]]) -> SearchIds:
    """Parse a list of message ids and return them as a list.

    parse_response is also capable of doing this but this is
    faster. This also has special handling of the optional MODSEQ part
    of a SEARCH response.

    The returned list is a SearchIds instance which has a *modseq*
    attribute which contains the MODSEQ response (if returned by the
    server).
    """
    if len(data) != 1:
        raise ValueError("unexpected message list data")

    message_data = data[0]
    if not message_data:
        return SearchIds()

    if isinstance(message_data, bytes):
        message_data = message_data.decode("ascii")

    m = _msg_id_pattern.match(message_data)
    if not m:
        raise ValueError("unexpected message list format")

    ids = SearchIds(int(n) for n in m.group(1).split())

    # Parse any non-numeric part on the end using parse_response (this
    # is likely to be the MODSEQ section).
    extra = message_data[m.end(1) :]
    if extra:
        for item in parse_response([extra.encode("ascii")]):
            if (
                isinstance(item, tuple)
                and len(item) == 2
                and cast(bytes, item[0]).lower() == b"modseq"
            ):
                if TYPE_CHECKING:
                    assert isinstance(item[1], int)
                ids.modseq = item[1]
            elif isinstance(item, int):
                ids.append(item)
    return ids


def gen_parsed_response(text: List[bytes]) -> Iterator[_Atom]:
    if not text:
        return
    src = TokenSource(text)

    token = None
    try:
        for token in src:
            yield atom(src, token)
    except ProtocolError:
        raise
    except ValueError:
        _, err, _ = sys.exc_info()
        raise ProtocolError("%s: %r" % (str(err), token))


_ParseFetchResponseInnerDict = Dict[
    bytes, Optional[Union[datetime.datetime, int, BodyData, Envelope, _Atom]]
]


def parse_fetch_response(
    text: List[bytes], normalise_times: bool = True, uid_is_key: bool = True
) -> "defaultdict[int, _ParseFetchResponseInnerDict]":
    """Pull apart IMAP FETCH responses as returned by imaplib.

    Returns a dictionary, keyed by message ID. Each value a dictionary
    keyed by FETCH field type (eg."RFC822").
    """
    if text == [None]:
        return defaultdict()
    response = gen_parsed_response(text)

    parsed_response: "defaultdict[int, _ParseFetchResponseInnerDict]" = defaultdict(
        dict
    )
    while True:
        try:
            msg_id = seq = _int_or_error(next(response), "invalid message ID")
        except StopIteration:
            break

        try:
            msg_response = next(response)
        except StopIteration:
            raise ProtocolError("unexpected EOF")

        if not isinstance(msg_response, tuple):
            raise ProtocolError("bad response type: %s" % repr(msg_response))
        if len(msg_response) % 2:
            raise ProtocolError(
                "uneven number of response items: %s" % repr(msg_response)
            )

        # always return the sequence of the message, so it is available
        # even if we return keyed by UID.
        msg_data: _ParseFetchResponseInnerDict = {b"SEQ": seq}
        for i in range(0, len(msg_response), 2):
            msg_attribute = msg_response[i]
            if TYPE_CHECKING:
                assert isinstance(msg_attribute, bytes)
            word = msg_attribute.upper()
            value = msg_response[i + 1]

            if word == b"UID":
                uid = _int_or_error(value, "invalid UID")
                if uid_is_key:
                    msg_id = uid
                else:
                    msg_data[word] = uid
            elif word == b"INTERNALDATE":
                msg_data[word] = _convert_INTERNALDATE(value, normalise_times)
            elif word == b"ENVELOPE":
                msg_data[word] = _convert_ENVELOPE(value, normalise_times)
            elif word in (b"BODY", b"BODYSTRUCTURE"):
                if TYPE_CHECKING:
                    assert isinstance(value, tuple)
                msg_data[word] = BodyData.create(value)
            else:
                msg_data[word] = value

        parsed_response[msg_id].update(msg_data)

    return parsed_response


def _int_or_error(value: _Atom, error_text: str) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise ProtocolError("%s: %s" % (error_text, repr(value)))


def _convert_INTERNALDATE(
    date_string: _Atom, normalise_times: bool = True
) -> Optional[datetime.datetime]:
    if date_string is None:
        return None

    try:
        if TYPE_CHECKING:
            assert isinstance(date_string, bytes)
        return parse_to_datetime(date_string, normalise=normalise_times)
    except ValueError:
        return None


def _convert_ENVELOPE(
    envelope_response: _Atom, normalise_times: bool = True
) -> Envelope:
    if TYPE_CHECKING:
        assert isinstance(envelope_response, tuple)
    dt = None
    if envelope_response[0]:
        try:
            if TYPE_CHECKING:
                assert isinstance(envelope_response[0], bytes)
            dt = parse_to_datetime(
                envelope_response[0],
                normalise=normalise_times,
            )
        except ValueError:
            pass

    subject = envelope_response[1]
    in_reply_to = envelope_response[8]
    message_id = envelope_response[9]
    if TYPE_CHECKING:
        assert isinstance(subject, bytes)
        assert isinstance(in_reply_to, bytes)
        assert isinstance(message_id, bytes)

    # addresses contains a tuple of addresses
    # from, sender, reply_to, to, cc, bcc headers
    addresses: List[Optional[Tuple[Address, ...]]] = []
    for addr_list in envelope_response[2:8]:
        addrs = []
        if addr_list:
            if TYPE_CHECKING:
                assert isinstance(addr_list, tuple)
            for addr_tuple in addr_list:
                if TYPE_CHECKING:
                    assert isinstance(addr_tuple, tuple)
                if addr_tuple:
                    if TYPE_CHECKING:
                        addr_tuple = cast(Tuple[bytes, bytes, bytes, bytes], addr_tuple)
                    addrs.append(Address(*addr_tuple))
            addresses.append(tuple(addrs))
        else:
            addresses.append(None)

    return Envelope(
        date=dt,
        subject=subject,
        from_=addresses[0],
        sender=addresses[1],
        reply_to=addresses[2],
        to=addresses[3],
        cc=addresses[4],
        bcc=addresses[5],
        in_reply_to=in_reply_to,
        message_id=message_id,
    )


def atom(src: TokenSource, token: bytes) -> _Atom:
    if token == b"(":
        return parse_tuple(src)
    if token == b"NIL":
        return None
    if token[:1] == b"{":
        literal_len = int(token[1:-1])
        literal_text = src.current_literal
        if literal_text is None:
            raise ProtocolError("No literal corresponds to %r" % token)
        if len(literal_text) != literal_len:
            raise ProtocolError(
                "Expecting literal of size %d, got %d"
                % (literal_len, len(literal_text))
            )
        return literal_text
    if len(token) >= 2 and (token[:1] == token[-1:] == b'"'):
        return token[1:-1]
    if token.isdigit() and (token[:1] != b"0" or len(token) == 1):
        # this prevents converting items like 0123 to 123
        return int(token)
    return token


def parse_tuple(src: TokenSource) -> _Atom:
    out: List[_Atom] = []
    for token in src:
        if token == b")":
            return tuple(out)
        out.append(atom(src, token))
    # no terminator
    raise ProtocolError('Tuple incomplete before "(%s"' % _fmt_tuple(out))


def _fmt_tuple(t: List[_Atom]) -> str:
    return " ".join(str(item) for item in t)

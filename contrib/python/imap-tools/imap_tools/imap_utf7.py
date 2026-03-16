"""
Encode and decode UTF-7 string, as described in the RFC 3501

There are variations, specific to IMAP4rev1, therefore the built-in python UTF-7 codec can't be used.
The main difference is the shift character, used to switch from ASCII to base64 encoding context.
This is "&" in that modified UTF-7 convention, since "+" is considered as mainly used in mailbox names.
Full description at RFC 3501, section 5.1.3.
"""

import binascii
from typing import MutableSequence

AMPERSAND_ORD = ord('&')
HYPHEN_ORD = ord('-')


# ENCODING
# --------
def _modified_base64(value: str) -> bytes:
    return binascii.b2a_base64(value.encode('utf-16be')).rstrip(b'\n=').replace(b'/', b',')


def _do_b64(_in: MutableSequence[str], r: MutableSequence[bytes]):
    if _in:
        r.append(b'&' + _modified_base64(''.join(_in)) + b'-')
    _in.clear()


def utf7_encode(value: str) -> bytes:
    res = []
    _in = []
    for char in value:
        ord_c = ord(char)
        if 0x20 <= ord_c <= 0x25 or 0x27 <= ord_c <= 0x7e:
            _do_b64(_in, res)
            res.append(char.encode())
        elif char == '&':
            _do_b64(_in, res)
            res.append(b'&-')
        else:
            _in.append(char)
    _do_b64(_in, res)
    return b''.join(res)


# DECODING
# --------
def _modified_unbase64(value: bytearray) -> str:
    return binascii.a2b_base64(value.replace(b',', b'/') + b'===').decode('utf-16be')


def utf7_decode(value: bytes) -> str:
    res = []
    encoded_chars = bytearray()
    for char in value:
        if char == AMPERSAND_ORD and not encoded_chars:
            encoded_chars.append(AMPERSAND_ORD)
        elif char == HYPHEN_ORD and encoded_chars:
            if len(encoded_chars) == 1:
                res.append('&')
            else:
                res.append(_modified_unbase64(encoded_chars[1:]))
            encoded_chars = bytearray()
        elif encoded_chars:
            encoded_chars.append(char)
        else:
            res.append(chr(char))
    if encoded_chars:
        res.append(_modified_unbase64(encoded_chars[1:]))
    return ''.join(res)

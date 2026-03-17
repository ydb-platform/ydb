import struct

from pytds import tds_base

from .tds_base import _TdsLogin


def fedauth_packet(login: _TdsLogin, fedauth_required: bool) -> bytes:
    if login.access_token is None:
        raise ValueError("Cannot build fedauth packet without access_token")
    fedauth_token = login.access_token.encode("UTF-16LE")
    tokenlen = len(fedauth_token)
    noncelen = len(login.nonce) if login.nonce else 0
    buffer = bytearray()
    buffer.extend(struct.pack("B", tds_base.TDS_LOGIN_FEATURE_FEDAUTH))
    buffer.extend(struct.pack("<I", tokenlen + noncelen + 1 + 4))
    buffer.extend(struct.pack("B", (tds_base.TDS_FEDAUTH_OPTIONS_LIBRARY_SECURITYTOKEN << 1) |
                             (tds_base.TDS_FEDAUTH_OPTIONS_ECHO_YES if fedauth_required else tds_base.TDS_FEDAUTH_OPTIONS_ECHO_NO)))
    buffer.extend(struct.pack("<I", tokenlen))
    buffer.extend(fedauth_token)
    if login.nonce:
        buffer.extend(login.nonce)
    buffer.extend(struct.pack("B", 0xFF))
    return bytes(buffer)


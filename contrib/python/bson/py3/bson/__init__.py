#!/usr/bin/python -OOOO
# vim: set fileencoding=utf8 shiftwidth=4 tabstop=4 textwidth=80 foldmethod=marker :
# Copyright (c) 2010, Kou Man Tong. All rights reserved.
# For licensing, see LICENSE file included in the package.
"""
BSON serialization and deserialization logic.
Specifications taken from: http://bsonspec.org/#/specification
The following types are unsupported, because for data exchange purposes, they're
over-engineered:
    0x06 (Undefined)
    0x0b (Regex - Exactly which flavor do you want? Better let higher level
        programmers make that decision.)
    0x0c (DBPointer)
    0x0d (JavaScript code)
    0x0e (Symbol)
    0x0f (JS w/ scope)
    0x11 (MongoDB-specific timestamp)

For binaries, only the default 0x0 type is supported.
"""

from .codec import *
from .objectid import ObjectId

__all__ = ["loads", "dumps"]


def dumps(obj, generator=None, on_unknown=None):
    """
    Given a dict, outputs a BSON string.

    generator is an optional function which accepts the dictionary/array being
    encoded, the current DFS traversal stack, and outputs an iterator indicating
    the correct encoding order for keys.
    """
    if isinstance(obj, BSONCoding):
        return encode_object(obj, [],
                             generator_func=generator, on_unknown=on_unknown)
    return encode_document(obj, [],
                           generator_func=generator, on_unknown=on_unknown)


def loads(data):
    """
        Given a BSON string, outputs a dict.
    """
    return decode_document(data, 0)[1]


def patch_socket():
    """
        Patches the Python socket class such that sockets can send and receive BSON
        objects atomically.

        This adds the following functions to socket:

        recvbytes(bytes_needed, sock_buf = None) - reads bytes_needed bytes
        atomically. Returns None if socket closed.

        recvobj() - reads a BSON document from the socket atomically and returns
        the deserialized dictionary. Returns None if socket closed.

        sendobj(obj) - sends a BSON document to the socket atomically.
    """
    from socket import socket
    from .network import recvbytes, recvobj, sendobj
    socket.recvbytes = recvbytes
    socket.recvobj = recvobj
    socket.sendobj = sendobj

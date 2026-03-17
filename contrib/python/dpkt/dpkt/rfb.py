# $Id: rfb.py 47 2008-05-27 02:10:00Z jon.oberheide $
# -*- coding: utf-8 -*-
"""Remote Framebuffer Protocol."""
from __future__ import absolute_import

from . import dpkt

# Remote Framebuffer Protocol
# http://www.realvnc.com/docs/rfbproto.pdf

# Client to Server Messages
CLIENT_SET_PIXEL_FORMAT = 0
CLIENT_SET_ENCODINGS = 2
CLIENT_FRAMEBUFFER_UPDATE_REQUEST = 3
CLIENT_KEY_EVENT = 4
CLIENT_POINTER_EVENT = 5
CLIENT_CUT_TEXT = 6

# Server to Client Messages
SERVER_FRAMEBUFFER_UPDATE = 0
SERVER_SET_COLOUR_MAP_ENTRIES = 1
SERVER_BELL = 2
SERVER_CUT_TEXT = 3


class RFB(dpkt.Packet):
    """Remote Framebuffer Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of RADIUS.
        TODO.
    """

    __hdr__ = (
        ('type', 'B', 0),
    )


class SetPixelFormat(dpkt.Packet):
    __hdr__ = (
        ('pad', '3s', b''),
        ('pixel_fmt', '16s', b'')
    )


class SetEncodings(dpkt.Packet):
    __hdr__ = (
        ('pad', '1s', b''),
        ('num_encodings', 'H', 0)
    )


class FramebufferUpdateRequest(dpkt.Packet):
    __hdr__ = (
        ('incremental', 'B', 0),
        ('x_position', 'H', 0),
        ('y_position', 'H', 0),
        ('width', 'H', 0),
        ('height', 'H', 0)
    )


class KeyEvent(dpkt.Packet):
    __hdr__ = (
        ('down_flag', 'B', 0),
        ('pad', '2s', b''),
        ('key', 'I', 0)
    )


class PointerEvent(dpkt.Packet):
    __hdr__ = (
        ('button_mask', 'B', 0),
        ('x_position', 'H', 0),
        ('y_position', 'H', 0)
    )


class FramebufferUpdate(dpkt.Packet):
    __hdr__ = (
        ('pad', '1s', b''),
        ('num_rects', 'H', 0)
    )


class SetColourMapEntries(dpkt.Packet):
    __hdr__ = (
        ('pad', '1s', b''),
        ('first_colour', 'H', 0),
        ('num_colours', 'H', 0)
    )


class CutText(dpkt.Packet):
    __hdr__ = (
        ('pad', '3s', b''),
        ('length', 'I', 0)
    )

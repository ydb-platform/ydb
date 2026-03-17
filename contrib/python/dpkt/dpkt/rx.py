# $Id: rx.py 23 2006-11-08 15:45:33Z jonojono $
# -*- coding: utf-8 -*-
"""Rx Protocol."""
from __future__ import absolute_import

from . import dpkt

# Types
DATA = 0x01
ACK = 0x02
BUSY = 0x03
ABORT = 0x04
ACKALL = 0x05
CHALLENGE = 0x06
RESPONSE = 0x07
DEBUG = 0x08

# Flags
CLIENT_INITIATED = 0x01
REQUEST_ACK = 0x02
LAST_PACKET = 0x04
MORE_PACKETS = 0x08
SLOW_START_OK = 0x20
JUMBO_PACKET = 0x20

# Security
SEC_NONE = 0x00
SEC_BCRYPT = 0x01
SEC_RXKAD = 0x02
SEC_RXKAD_ENC = 0x03


class Rx(dpkt.Packet):
    """Rx Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of Rx.
        TODO.
    """

    __hdr__ = (
        ('epoch', 'I', 0),
        ('cid', 'I', 0),
        ('call', 'I', 1),
        ('seq', 'I', 0),
        ('serial', 'I', 1),
        ('type', 'B', 0),
        ('flags', 'B', CLIENT_INITIATED),
        ('status', 'B', 0),
        ('security', 'B', 0),
        ('sum', 'H', 0),
        ('service', 'H', 0)
    )

# $Id: hsrp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Cisco Hot Standby Router Protocol."""
from __future__ import absolute_import

from . import dpkt

# Opcodes
HELLO = 0
COUP = 1
RESIGN = 2

# States
INITIAL = 0x00
LEARN = 0x01
LISTEN = 0x02
SPEAK = 0x04
STANDBY = 0x08
ACTIVE = 0x10


class HSRP(dpkt.Packet):
    """Cisco Hot Standby Router Protocol.

    It  is a Cisco proprietary redundancy protocol for establishing a fault-tolerant default gateway. Version 1 of the
    protocol was described in RFC 2281 in 1998. Version 2 of the protocol includes improvements and supports IPv6 but
    there is no corresponding RFC published for this version.

    Attributes:
        __hdr__: Header fields of HSRP.
            version: (int): Version. HSRP version number. (1 byte)
            opcode: (int): Operation code. (Hello - 0, Coup - 1, Resign - 2) (1 byte)
            state: (int): State. This field describes the current state of the router sending the message. (1 byte)
            hello: (int): Hellotime. This field is only meaningful in Hello messages. It contains the approximate period
                between the Hello messages that the router sends. The time is given in seconds.(1 byte)
            hold: (int): Holdtime. This field is only meaningful in Hello messages. It contains the amount of time that
                the current Hello message should be considered valid. The time is given in seconds. (1 byte)
            priority: (int): Priority. This field is used to elect the active and standby routers. (1 byte)
            group: (int): Group. This field identifies the standby group. (1 byte)
            rsvd: (int): Reserved. (1 byte)
            auth: (bytes): Authentication Data. This field contains a clear text 8 character reused password. (8 bytes)
            vip: (bytes): Virtual IP Address. The virtual IP address used by this group. (4 bytes)
    """

    __hdr__ = (
        ('version', 'B', 0),
        ('opcode', 'B', 0),
        ('state', 'B', 0),
        ('hello', 'B', 0),
        ('hold', 'B', 0),
        ('priority', 'B', 0),
        ('group', 'B', 0),
        ('rsvd', 'B', 0),
        ('auth', '8s', b'cisco'),
        ('vip', '4s', b'')
    )

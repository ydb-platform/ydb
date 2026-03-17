# $Id: ntp.py 48 2008-05-27 17:31:15Z yardley $
# -*- coding: utf-8 -*-
"""Network Time Protocol."""
from __future__ import print_function

from . import dpkt

# NTP v4

# Leap Indicator (LI) Codes
NO_WARNING = 0
LAST_MINUTE_61_SECONDS = 1
LAST_MINUTE_59_SECONDS = 2
ALARM_CONDITION = 3

# Mode Codes
RESERVED = 0
SYMMETRIC_ACTIVE = 1
SYMMETRIC_PASSIVE = 2
CLIENT = 3
SERVER = 4
BROADCAST = 5
CONTROL_MESSAGE = 6
PRIVATE = 7


class NTP(dpkt.Packet):
    """Network Time Protocol.

    The Network Time Protocol (NTP) is a networking protocol for clock synchronization between computer systems over
    packet-switched, variable-latency data networks. In operation since before 1985, NTP is one of the oldest Internet
    protocols in current use. NTP was designed by David L. Mills of the University of Delaware.

    Attributes:
        __hdr__: Header fields of NTP.
        TODO.
    """

    __hdr__ = (
        ('flags', 'B', 0),
        ('stratum', 'B', 0),
        ('interval', 'B', 0),
        ('precision', 'B', 0),
        ('delay', 'I', 0),
        ('dispersion', 'I', 0),
        ('id', '4s', 0),
        ('update_time', '8s', 0),
        ('originate_time', '8s', 0),
        ('receive_time', '8s', 0),
        ('transmit_time', '8s', 0)
    )
    __bit_fields__ = {
        'flags': (
            ('li', 2),    # leap indicator, 2 hi bits
            ('v', 3),     # version, 3 bits
            ('mode', 3),  # mode, 3 lo bits
        )
    }


__s = (b'\x24\x02\x04\xef\x00\x00\x00\x84\x00\x00\x33\x27\xc1\x02\x04\x02\xc8\x90\xec\x11\x22\xae'
       b'\x07\xe5\xc8\x90\xf9\xd9\xc0\x7e\x8c\xcd\xc8\x90\xf9\xd9\xda\xc5\xb0\x78\xc8\x90\xf9\xd9\xda\xc6\x8a\x93')


def test_ntp_pack():
    n = NTP(__s)
    assert (__s == bytes(n))


def test_ntp_unpack():
    n = NTP(__s)
    assert (n.li == NO_WARNING)
    assert (n.v == 4)
    assert (n.mode == SERVER)
    assert (n.stratum == 2)
    assert (n.id == b'\xc1\x02\x04\x02')
    # test get/set functions
    n.li = ALARM_CONDITION
    n.v = 3
    n.mode = CLIENT
    assert (n.li == ALARM_CONDITION)
    assert (n.v == 3)
    assert (n.mode == CLIENT)

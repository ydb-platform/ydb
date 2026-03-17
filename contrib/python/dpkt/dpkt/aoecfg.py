# -*- coding: utf-8 -*-
"""ATA over Ethernet ATA command"""
from __future__ import print_function
from __future__ import absolute_import

from . import dpkt


class AOECFG(dpkt.Packet):
    """ATA over Ethernet ATA command.

    See more about the AOE on \
    https://en.wikipedia.org/wiki/ATA_over_Ethernet

    Attributes:
        __hdr__: Header fields of AOECFG.
        data: Message data.
    """

    __hdr__ = (
        ('bufcnt', 'H', 0),
        ('fwver', 'H', 0),
        ('scnt', 'B', 0),
        ('aoeccmd', 'B', 0),
        ('cslen', 'H', 0),
    )


def test_aoecfg():
    s = (b'\x01\x02\x03\x04\x05\x06\x11\x12\x13\x14\x15\x16\x88\xa2\x10\x00\x00\x01\x02\x01\x80'
         b'\x00\x00\x00\x12\x34\x00\x00\x00\x00\x04\x00' + b'\0xed' * 1024)
    aoecfg = AOECFG(s[14 + 10:])
    assert (aoecfg.bufcnt == 0x1234)

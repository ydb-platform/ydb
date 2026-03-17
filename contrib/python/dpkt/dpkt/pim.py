# $Id: pim.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Protocol Independent Multicast."""
from __future__ import absolute_import

from . import dpkt


class PIM(dpkt.Packet):
    """Protocol Independent Multicast.

    Protocol Independent Multicast (PIM) is a collection of multicast routing protocols, each optimized for a different
    environment. There are two main PIM protocols, PIM Sparse Mode and PIM Dense Mode. A third PIM protocol,
    Bi-directional PIM, is less widely used.

    Attributes:
        __hdr__: Header fields of PIM.
            _v_type: (int): Version (4 bits) and type (4 bits). PIM version number and Message type. (1 byte)
            _rsvd: (int): Reserved. Always cleared to zero. (1 byte)
            sum: (int): Checksum. The 16-bit one's complement of the one's complement sum of the entire PIM message,
                excluding the data portion in the Register message.(2 bytes)
    """

    __hdr__ = (
        ('_v_type', 'B', 0x20),
        ('_rsvd', 'B', 0),
        ('sum', 'H', 0)
    )
    __bit_fields__ = {
        '_v_type': (
            ('v', 4),
            ('type', 4),
        )
    }

    def __bytes__(self):
        if not self.sum:
            self.sum = dpkt.in_cksum(dpkt.Packet.__bytes__(self))
        return dpkt.Packet.__bytes__(self)


def test_pim():
    from binascii import unhexlify
    buf = unhexlify(
        '20'            # _v_type
        '00'            # rsvd
        'df93'          # sum

        '000100020069'  # data
    )
    pimdata = PIM(buf)
    assert bytes(pimdata) == buf
    # force checksum recalculation
    pimdata = PIM(buf)
    pimdata.sum = 0
    assert pimdata.sum == 0
    assert bytes(pimdata) == buf

    assert pimdata.v == 2
    assert pimdata.type == 0

    # test setters
    buf_modified = unhexlify(
        '31'            # _v_type
        '00'            # rsvd
        'df93'          # sum

        '000100020069'  # data
    )
    pimdata.v = 3
    pimdata.type = 1
    assert bytes(pimdata) == buf_modified

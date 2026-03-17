"""Extreme Discovery Protocol."""
from __future__ import absolute_import

import dpkt


class EDP(dpkt.Packet):
    __hdr__ = (
        ('version', 'B', 1),
        ('reserved', 'B', 0),
        ('hlen', 'H', 0),
        ('sum', 'H', 0),
        ('seq', 'H', 0),
        ('mid', 'H', 0),
        ('mac', '6s', b'')
    )

    def __bytes__(self):
        if not self.sum:
            self.sum = dpkt.in_cksum(dpkt.Packet.__bytes__(self))
        return dpkt.Packet.__bytes__(self)


class TestEDP(object):
    """
    Test basic EDP functionality.
    """

    @classmethod
    def setup_class(cls):
        from binascii import unhexlify
        cls.buf = unhexlify(
            '01'      # version
            '00'      # reserved
            '013c'    # hlen
            '9e76'    # sum
            '001b'    # seq
            '0000'    # mid
            '080027'  # mac
            '2d90ed990200240000000000000000000000000f020207000000000000000000000000000000009901010445584f532d32000000000000000'
            '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'
            '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'
            '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'
            '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'
            '00000000000000000000000000000000099000004'
        )
        cls.p = EDP(cls.buf)

    def test_version(self):
        assert (self.p.version == 1)

    def test_reserved(self):
        assert (self.p.reserved == 0)

    def test_hlen(self):
        assert (self.p.hlen == 316)

    def test_sum(self):
        assert (self.p.sum == 40566)

    def test_seq(self):
        assert (self.p.seq == 27)

    def test_mid(self):
        assert (self.p.mid == 0)

    def test_mac(self):
        assert (self.p.mac == b"\x08\x00'-\x90\xed")

    def test_bytes(self):
        assert bytes(self.p) == self.buf

        # force recalculation of the checksum
        edp = EDP(self.buf)
        edp.sum = 0
        assert edp.sum == 0
        assert bytes(edp) == self.buf

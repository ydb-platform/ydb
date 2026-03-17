# $Id: stp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Spanning Tree Protocol."""
from __future__ import print_function
from __future__ import absolute_import

from . import dpkt


class STP(dpkt.Packet):
    """Spanning Tree Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of STP.
        TODO.
    """

    __hdr__ = (
        ('proto_id', 'H', 0),
        ('v', 'B', 0),
        ('type', 'B', 0),
        ('flags', 'B', 0),
        ('root_id', '8s', b''),
        ('root_path', 'I', 0),
        ('bridge_id', '8s', b''),
        ('port_id', 'H', 0),
        ('_age', 'H', 0),
        ('_max_age', 'H', 0),
        ('_hello', 'H', 0),
        ('_fd', 'H', 0)
    )

    @property
    def age(self):
        return self._age >> 8

    @age.setter
    def age(self, age):
        self._age = age << 8

    @property
    def max_age(self):
        return self._max_age >> 8

    @max_age.setter
    def max_age(self, max_age):
        self._max_age = max_age << 8

    @property
    def hello(self):
        return self._hello >> 8

    @hello.setter
    def hello(self, hello):
        self._hello = hello << 8

    @property
    def fd(self):
        return self._fd >> 8

    @fd.setter
    def fd(self, fd):
        self._fd = fd << 8


def test_stp():
    buf = (b'\x00\x00\x02\x02\x3e\x80\x00\x08\x00\x27\xad\xa3\x41\x00\x00\x00\x00\x80\x00\x08\x00\x27'
           b'\xad\xa3\x41\x80\x01\x00\x00\x14\x00\x02\x00\x0f\x00\x00\x00\x00\x00\x02\x00\x14\x00')
    stp = STP(buf)

    assert stp.proto_id == 0
    assert stp.port_id == 0x8001
    assert stp.age == 0
    assert stp.max_age == 20
    assert stp.hello == 2
    assert stp.fd == 15

    assert bytes(stp) == buf

    stp.fd = 100
    assert stp.pack_hdr()[-2:] == b'\x64\x00'  # 100 << 8


def test_properties():
    stp = STP()
    stp.age = 10
    assert stp.age == 10

    stp.max_age = 20
    assert stp.max_age == 20

    stp.hello = 1234
    assert stp.hello == 1234

# -*- coding: utf-8 -*-
"""ATA over Ethernet Protocol."""
from __future__ import absolute_import

import struct

from . import dpkt
from .compat import iteritems


class AOE(dpkt.Packet):
    """ATA over Ethernet Protocol.

    See more about the AOE on
    https://en.wikipedia.org/wiki/ATA_over_Ethernet

    Attributes:
        __hdr__: Header fields of AOE.
        data: Message data.
    """

    __hdr__ = (
        ('_ver_fl', 'B', 0x10),
        ('err', 'B', 0),
        ('maj', 'H', 0),
        ('min', 'B', 0),
        ('cmd', 'B', 0),
        ('tag', 'I', 0),
    )
    __bit_fields__ = {
        '_ver_fl': (
            ('ver', 4),
            ('fl', 4),
        )
    }
    _cmdsw = {}

    @classmethod
    def set_cmd(cls, cmd, pktclass):
        cls._cmdsw[cmd] = pktclass

    @classmethod
    def get_cmd(cls, cmd):
        return cls._cmdsw[cmd]

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        try:
            self.data = self._cmdsw[self.cmd](self.data)
            setattr(self, self.data.__class__.__name__.lower(), self.data)
        except (KeyError, struct.error, dpkt.UnpackError):
            pass


AOE_CMD_ATA = 0
AOE_CMD_CFG = 1
AOE_FLAG_RSP = 1 << 3


def _load_cmds():
    prefix = 'AOE_CMD_'
    g = globals()

    for k, v in iteritems(g):
        if k.startswith(prefix):
            name = 'aoe' + k[len(prefix):].lower()
            try:
                mod = __import__(name, g, level=1)
                AOE.set_cmd(v, getattr(mod, name.upper()))
            except (ImportError, AttributeError):
                continue


def _mod_init():
    """Post-initialization called when all dpkt modules are fully loaded"""
    if not AOE._cmdsw:
        _load_cmds()


def test_creation():
    aoe = AOE()
    # hdr fields
    assert aoe._ver_fl == 0x10
    assert aoe.err == 0
    assert aoe.maj == 0
    assert aoe.min == 0
    assert aoe.cmd == 0
    assert aoe.tag == 0
    assert bytes(aoe) == b'\x10' + b'\x00' * 9


def test_properties():
    aoe = AOE()
    # property getters
    assert aoe.ver == 1
    assert aoe.fl == 0

    # property setters
    aoe.ver = 2
    assert aoe.ver == 2
    assert aoe._ver_fl == 0x20

    aoe.fl = 12
    assert aoe.fl == 12
    assert aoe._ver_fl == 0x2C


def test_unpack():
    from binascii import unhexlify
    buf = unhexlify(
        '1000000000'
        '00'          # cmd: AOE_CMD_ATA
        '00000000'    # tag
    )
    aoe = AOE(buf)
    # AOE_CMD_ATA specified, but no data supplied
    assert aoe.data == b''

    buf = unhexlify(
        '1000000000'
        '00'          # cmd: AOE_CMD_ATA
        '00000000'    # tag

        # AOEDATA specification
        '030a6b190000000045000028941f0000e30699b4232b2400de8e8442abd100500035e1'
        '2920d9000000229bf0e204656b'
    )
    aoe = AOE(buf)
    assert aoe.aoeata == aoe.data


def test_cmds():
    import dpkt
    assert AOE.get_cmd(AOE_CMD_ATA) == dpkt.aoeata.AOEATA
    assert AOE.get_cmd(AOE_CMD_CFG) == dpkt.aoecfg.AOECFG


def test_cmd_loading():
    # this test checks that failing to load a module isn't catastrophic
    standard_cmds = AOE._cmdsw
    # delete the existing code->module mappings
    AOE._cmdsw = {}
    assert not AOE._cmdsw
    # create a new global constant pointing to a module which doesn't exist
    globals()['AOE_CMD_FAIL'] = "FAIL"
    _mod_init()
    # check that the same modules were loaded, ignoring the fail
    assert AOE._cmdsw == standard_cmds

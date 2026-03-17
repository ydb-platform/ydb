# -*- coding: utf-8 -*-
"""Radiotap"""
from __future__ import print_function
from __future__ import absolute_import

from . import dpkt
from . import ieee80211
from .compat import compat_ord

# Ref: http://www.radiotap.org
# Fields Ref: http://www.radiotap.org/defined-fields/all

# Present flags
_TSFT_SHIFT = 0
_FLAGS_SHIFT = 1
_RATE_SHIFT = 2
_CHANNEL_SHIFT = 3
_FHSS_SHIFT = 4
_ANT_SIG_SHIFT = 5
_ANT_NOISE_SHIFT = 6
_LOCK_QUAL_SHIFT = 7
_TX_ATTN_SHIFT = 8
_DB_TX_ATTN_SHIFT = 9
_DBM_TX_POWER_SHIFT = 10
_ANTENNA_SHIFT = 11
_DB_ANT_SIG_SHIFT = 12
_DB_ANT_NOISE_SHIFT = 13
_RX_FLAGS_SHIFT = 14
_CHANNELPLUS_SHIFT = 18
_EXT_SHIFT = 31

# Flags elements
_FLAGS_SIZE = 2
_CFP_FLAG_SHIFT = 0
_PREAMBLE_SHIFT = 1
_WEP_SHIFT = 2
_FRAG_SHIFT = 3
_FCS_SHIFT = 4
_DATA_PAD_SHIFT = 5
_BAD_FCS_SHIFT = 6
_SHORT_GI_SHIFT = 7

# Channel type
_CHAN_TYPE_SIZE = 4
_CHANNEL_TYPE_SHIFT = 4
_CCK_SHIFT = 5
_OFDM_SHIFT = 6
_TWO_GHZ_SHIFT = 7
_FIVE_GHZ_SHIFT = 8
_PASSIVE_SHIFT = 9
_DYN_CCK_OFDM_SHIFT = 10
_GFSK_SHIFT = 11
_GSM_SHIFT = 12
_STATIC_TURBO_SHIFT = 13
_HALF_RATE_SHIFT = 14
_QUARTER_RATE_SHIFT = 15

# Flags offsets and masks
_FCS_MASK = 0x10


class Radiotap(dpkt.Packet):
    """Radiotap.

    Attributes:
        __hdr__: Header fields of Radiotap.
            version: (int): Version (1 byte)
            pad: (int): Padding (1 byte)
            length: (int): Length (2 bytes)
    """

    __hdr__ = (
        ('version', 'B', 0),
        ('pad', 'B', 0),
        ('length', 'H', 0),
    )

    __byte_order__ = '<'

    def _is_present(self, bit):
        index = bit // 8
        mask = 1 << (bit % 8)
        return 1 if self.present_flags[index] & mask else 0

    def _set_bit(self, bit, val):
        # present_flags is a bytearray, this gets the element
        index = bit // 8
        # mask retrieves every bit except our one
        mask = ~(1 << (bit % 8) & 0xff)
        # retrieve all of the bits, then or in the val at the appropriate place
        # as the mask does not return the value at `bit`, if `val` is zero, the bit remains zero
        self.present_flags[index] = (self.present_flags[index] & mask) | (val << bit % 8)

    @property
    def tsft_present(self):
        return self._is_present(_TSFT_SHIFT)

    @tsft_present.setter
    def tsft_present(self, val):
        self._set_bit(_TSFT_SHIFT, val)

    @property
    def flags_present(self):
        return self._is_present(_FLAGS_SHIFT)

    @flags_present.setter
    def flags_present(self, val):
        self._set_bit(_FLAGS_SHIFT, val)

    @property
    def rate_present(self):
        return self._is_present(_RATE_SHIFT)

    @rate_present.setter
    def rate_present(self, val):
        self._set_bit(_RATE_SHIFT, val)

    @property
    def channel_present(self):
        return self._is_present(_CHANNEL_SHIFT)

    @channel_present.setter
    def channel_present(self, val):
        self._set_bit(_CHANNEL_SHIFT, val)

    @property
    def fhss_present(self):
        return self._is_present(_FHSS_SHIFT)

    @fhss_present.setter
    def fhss_present(self, val):
        self._set_bit(_FHSS_SHIFT, val)

    @property
    def ant_sig_present(self):
        return self._is_present(_ANT_SIG_SHIFT)

    @ant_sig_present.setter
    def ant_sig_present(self, val):
        self._set_bit(_ANT_SIG_SHIFT, val)

    @property
    def ant_noise_present(self):
        return self._is_present(_ANT_NOISE_SHIFT)

    @ant_noise_present.setter
    def ant_noise_present(self, val):
        self._set_bit(_ANT_NOISE_SHIFT, val)

    @property
    def lock_qual_present(self):
        return self._is_present(_LOCK_QUAL_SHIFT)

    @lock_qual_present.setter
    def lock_qual_present(self, val):
        self._set_bit(_LOCK_QUAL_SHIFT, val)

    @property
    def tx_attn_present(self):
        return self._is_present(_TX_ATTN_SHIFT)

    @tx_attn_present.setter
    def tx_attn_present(self, val):
        self._set_bit(_TX_ATTN_SHIFT, val)

    @property
    def db_tx_attn_present(self):
        return self._is_present(_DB_TX_ATTN_SHIFT)

    @db_tx_attn_present.setter
    def db_tx_attn_present(self, val):
        self._set_bit(_DB_TX_ATTN_SHIFT, val)

    @property
    def dbm_tx_power_present(self):
        return self._is_present(_DBM_TX_POWER_SHIFT)

    @dbm_tx_power_present.setter
    def dbm_tx_power_present(self, val):
        self._set_bit(_DBM_TX_POWER_SHIFT, val)

    @property
    def ant_present(self):
        return self._is_present(_ANTENNA_SHIFT)

    @ant_present.setter
    def ant_present(self, val):
        self._set_bit(_ANTENNA_SHIFT, val)

    @property
    def db_ant_sig_present(self):
        return self._is_present(_DB_ANT_SIG_SHIFT)

    @db_ant_sig_present.setter
    def db_ant_sig_present(self, val):
        self._set_bit(_DB_ANT_SIG_SHIFT, val)

    @property
    def db_ant_noise_present(self):
        return self._is_present(_DB_ANT_NOISE_SHIFT)

    @db_ant_noise_present.setter
    def db_ant_noise_present(self, val):
        self._set_bit(_DB_ANT_NOISE_SHIFT, val)

    @property
    def rx_flags_present(self):
        return self._is_present(_RX_FLAGS_SHIFT)

    @rx_flags_present.setter
    def rx_flags_present(self, val):
        self._set_bit(_RX_FLAGS_SHIFT, val)

    @property
    def chanplus_present(self):
        return self._is_present(_CHANNELPLUS_SHIFT)

    @chanplus_present.setter
    def chanplus_present(self, val):
        self._set_bit(_CHANNELPLUS_SHIFT, val)

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.data = buf[self.length:]

        self.fields = []
        buf = buf[self.__hdr_len__:]

        self.present_flags = bytearray(buf[:4])
        buf = buf[4:]
        ext_bit = _EXT_SHIFT
        while self._is_present(ext_bit):
            self.present_flags += bytearray(buf[:4])
            buf = buf[4:]
            ext_bit += 32

        # decode each field into self.<name> (eg. self.tsft) as well as append it self.fields list
        field_decoder = [
            ('tsft', self.tsft_present, self.TSFT),
            ('flags', self.flags_present, self.Flags),
            ('rate', self.rate_present, self.Rate),
            ('channel', self.channel_present, self.Channel),
            ('fhss', self.fhss_present, self.FHSS),
            ('ant_sig', self.ant_sig_present, self.AntennaSignal),
            ('ant_noise', self.ant_noise_present, self.AntennaNoise),
            ('lock_qual', self.lock_qual_present, self.LockQuality),
            ('tx_attn', self.tx_attn_present, self.TxAttenuation),
            ('db_tx_attn', self.db_tx_attn_present, self.DbTxAttenuation),
            ('dbm_tx_power', self.dbm_tx_power_present, self.DbmTxPower),
            ('ant', self.ant_present, self.Antenna),
            ('db_ant_sig', self.db_ant_sig_present, self.DbAntennaSignal),
            ('db_ant_noise', self.db_ant_noise_present, self.DbAntennaNoise),
            ('rx_flags', self.rx_flags_present, self.RxFlags),
            ('chanplus', self.chanplus_present, self.ChannelPlus)
        ]

        offset = self.__hdr_len__ + len(self.present_flags)

        for name, present_bit, parser in field_decoder:
            if present_bit:
                ali = parser.__alignment__
                if ali > 1 and offset % ali:
                    padding = ali - offset % ali
                    buf = buf[padding:]
                    offset += padding
                field = parser(buf)
                field.data = b''
                setattr(self, name, field)
                self.fields.append(field)
                buf = buf[len(field):]
                offset += len(field)

        if len(self.data) > 0:
            if self.flags_present and self.flags.fcs:
                self.data = ieee80211.IEEE80211(self.data, fcs=self.flags.fcs)
            else:
                self.data = ieee80211.IEEE80211(self.data)

    class RadiotapField(dpkt.Packet):
        __alignment__ = 1
        __byte_order__ = '<'

    class Antenna(RadiotapField):
        __hdr__ = (
            ('index', 'B', 0),
        )

    class AntennaNoise(RadiotapField):
        __hdr__ = (
            ('db', 'b', 0),
        )

    class AntennaSignal(RadiotapField):
        __hdr__ = (
            ('db', 'b', 0),
        )

    class Channel(RadiotapField):
        __alignment__ = 2
        __hdr__ = (
            ('freq', 'H', 0),
            ('flags', 'H', 0),
        )

    class FHSS(RadiotapField):
        __hdr__ = (
            ('set', 'B', 0),
            ('pattern', 'B', 0),
        )

    class Flags(RadiotapField):
        __hdr__ = (
            ('val', 'B', 0),
        )

        @property
        def fcs(self):
            return (self.val & _FCS_MASK) >> _FCS_SHIFT

        @fcs.setter
        def fcs(self, v):
            self.val = (v << _FCS_SHIFT) | (v & ~_FCS_MASK)

    class LockQuality(RadiotapField):
        __alignment__ = 2
        __hdr__ = (
            ('val', 'H', 0),
        )

    class RxFlags(RadiotapField):
        __alignment__ = 2
        __hdr__ = (
            ('val', 'H', 0),
        )

    class Rate(RadiotapField):
        __hdr__ = (
            ('val', 'B', 0),
        )

    class TSFT(RadiotapField):
        __alignment__ = 8
        __hdr__ = (
            ('usecs', 'Q', 0),
        )

    class TxAttenuation(RadiotapField):
        __alignment__ = 2
        __hdr__ = (
            ('val', 'H', 0),
        )

    class DbTxAttenuation(RadiotapField):
        __alignment__ = 2
        __hdr__ = (
            ('db', 'H', 0),
        )

    class DbAntennaNoise(RadiotapField):
        __hdr__ = (
            ('db', 'B', 0),
        )

    class DbAntennaSignal(RadiotapField):
        __hdr__ = (
            ('db', 'B', 0),
        )

    class DbmTxPower(RadiotapField):
        __hdr__ = (
            ('dbm', 'B', 0),
        )

    class ChannelPlus(RadiotapField):
        __alignment__ = 4
        __hdr__ = (
            ('flags', 'I', 0),
            ('freq', 'H', 0),
            ('channel', 'B', 0),
            ('maxpower', 'B', 0),
        )


def test_radiotap_1():
    s = b'\x00\x00\x00\x18\x6e\x48\x00\x00\x00\x02\x6c\x09\xa0\x00\xa8\x81\x02\x00\x00\x00\x00\x00\x00\x00'
    rad = Radiotap(s)
    assert(rad.version == 0)
    assert(rad.present_flags == b'\x6e\x48\x00\x00')
    assert(rad.tsft_present == 0)
    assert(rad.flags_present == 1)
    assert(rad.rate_present == 1)
    assert(rad.channel_present == 1)
    assert(rad.fhss_present == 0)
    assert(rad.ant_sig_present == 1)
    assert(rad.ant_noise_present == 1)
    assert(rad.lock_qual_present == 0)
    assert(rad.db_tx_attn_present == 0)
    assert(rad.dbm_tx_power_present == 0)
    assert(rad.ant_present == 1)
    assert(rad.db_ant_sig_present == 0)
    assert(rad.db_ant_noise_present == 0)
    assert(rad.rx_flags_present == 1)
    assert(rad.channel.freq == 0x096c)
    assert(rad.channel.flags == 0xa0)
    assert(len(rad.fields) == 7)


def test_radiotap_2():
    s = (b'\x00\x00\x30\x00\x2f\x40\x00\xa0\x20\x08\x00\xa0\x20\x08\x00\xa0\x20\x08\x00\x00\x00\x00'
         b'\x00\x00\x08\x84\xbd\xac\x28\x00\x00\x00\x10\x02\x85\x09\xa0\x00\xa5\x00\x00\x00\xa1\x00'
         b'\x9f\x01\xa1\x02')
    rad = Radiotap(s)
    assert(rad.version == 0)
    assert(rad.present_flags == b'\x2f\x40\x00\xa0\x20\x08\x00\xa0\x20\x08\x00\xa0\x20\x08\x00\x00')
    assert(rad.tsft_present)
    assert(rad.flags_present)
    assert(rad.rate_present)
    assert(rad.channel_present)
    assert(not rad.fhss_present)
    assert(rad.ant_sig_present)
    assert(not rad.ant_noise_present)
    assert(not rad.lock_qual_present)
    assert(not rad.db_tx_attn_present)
    assert(not rad.dbm_tx_power_present)
    assert(not rad.ant_present)
    assert(not rad.db_ant_sig_present)
    assert(not rad.db_ant_noise_present)
    assert(rad.rx_flags_present)
    assert(rad.channel.freq == 2437)
    assert(rad.channel.flags == 0x00a0)
    assert(len(rad.fields) == 6)
    assert(rad.flags_present)
    assert(rad.flags.fcs)


def test_fcs():
    s = b'\x00\x00\x1a\x00\x2f\x48\x00\x00\x34\x8f\x71\x09\x00\x00\x00\x00\x10\x0c\x85\x09\xc0\x00\xcc\x01\x00\x00'
    rt = Radiotap(s)
    assert(rt.flags_present == 1)
    assert(rt.flags.fcs == 1)


def test_radiotap_3():  # xchannel aka channel plus field
    s = (
        b'\x00\x00\x20\x00\x67\x08\x04\x00\x84\x84\x66\x25\x00\x00\x00\x00\x22\x0c\xd6\xa0\x01\x00\x00\x00\x40'
        b'\x01\x00\x00\x3c\x14\x24\x11\x08\x02\x00\x00\xff\xff\xff\xff\xff\xff\x06\x03\x7f\x07\xa0\x16\x00\x19'
        b'\xe3\xd3\x53\x52\x00\x8e\xaa\xaa\x03\x00\x00\x00\x08\x06\x00\x01\x08\x00\x06\x04\x00\x01\x00\x19\xe3'
        b'\xd3\x53\x52\xa9\xfe\xf7\x00\x00\x00\x00\x00\x00\x00\x4f\x67\x32\x38'
    )
    rt = Radiotap(s)
    assert rt.ant_noise.db == -96
    assert rt.ant_sig.db == -42
    assert rt.ant.index == 1
    assert rt.chanplus_present
    assert rt.chanplus.flags == 0x140
    assert rt.chanplus.freq == 5180
    assert rt.chanplus.channel == 36
    assert rt.chanplus.maxpower == 17
    assert len(rt.fields) == 7
    assert repr(rt.data).startswith('IEEE80211')


def test_radiotap_properties():
    from binascii import unhexlify
    buf = unhexlify(
        '00'
        '00'
        '0018'

        '0000000000000000000000000000000000000000'
    )
    radiotap = Radiotap(buf)
    property_keys = [
        'tsft', 'flags', 'rate', 'channel', 'fhss', 'ant_sig', 'ant_noise',
        'lock_qual', 'tx_attn', 'db_tx_attn', 'dbm_tx_power', 'ant',
        'db_ant_sig', 'db_ant_noise', 'rx_flags', 'chanplus'
    ]
    for prop in [key + '_present' for key in property_keys]:
        print(prop)
        assert hasattr(radiotap, prop)
        assert getattr(radiotap, prop) == 0

        setattr(radiotap, prop, 1)
        assert getattr(radiotap, prop) == 1

        setattr(radiotap, prop, 0)
        assert getattr(radiotap, prop) == 0


def test_radiotap_unpack_fcs():
    from binascii import unhexlify
    buf = unhexlify(
        '00'     # version
        '00'     # pad
        '1800'   # length

        '6e48000011026c09a000a8810200000000000000'
        'd40000000012f0b61ca4ffffffff'
    )
    radiotap = Radiotap(buf)
    assert radiotap.data.fcs_present == 1


def test_flags():
    flags = Radiotap.Flags(b'\x00')
    assert flags.fcs == 0
    flags.fcs = 1
    assert flags.fcs == 1

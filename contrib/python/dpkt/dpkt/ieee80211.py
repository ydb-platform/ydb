# $Id: 80211.py 53 2008-12-18 01:22:57Z jon.oberheide $
# -*- coding: utf-8 -*-
"""IEEE 802.11."""
from __future__ import print_function
from __future__ import absolute_import

import struct

from . import dpkt
from .compat import ntole, ntole64

# Frame Types
MGMT_TYPE = 0
CTL_TYPE = 1
DATA_TYPE = 2

# Frame Sub-Types
M_ASSOC_REQ = 0
M_ASSOC_RESP = 1
M_REASSOC_REQ = 2
M_REASSOC_RESP = 3
M_PROBE_REQ = 4
M_PROBE_RESP = 5
M_BEACON = 8
M_ATIM = 9
M_DISASSOC = 10
M_AUTH = 11
M_DEAUTH = 12
M_ACTION = 13
C_BLOCK_ACK_REQ = 8
C_BLOCK_ACK = 9
C_PS_POLL = 10
C_RTS = 11
C_CTS = 12
C_ACK = 13
C_CF_END = 14
C_CF_END_ACK = 15
D_DATA = 0
D_DATA_CF_ACK = 1
D_DATA_CF_POLL = 2
D_DATA_CF_ACK_POLL = 3
D_NULL = 4
D_CF_ACK = 5
D_CF_POLL = 6
D_CF_ACK_POLL = 7
D_QOS_DATA = 8
D_QOS_CF_ACK = 9
D_QOS_CF_POLL = 10
D_QOS_CF_ACK_POLL = 11
D_QOS_NULL = 12
D_QOS_CF_POLL_EMPTY = 14

TO_DS_FLAG = 10
FROM_DS_FLAG = 1
INTER_DS_FLAG = 11

# Bitshifts for Frame Control
_VERSION_MASK = 0x0300
_TYPE_MASK = 0x0c00
_SUBTYPE_MASK = 0xf000
_TO_DS_MASK = 0x0001
_FROM_DS_MASK = 0x0002
_MORE_FRAG_MASK = 0x0004
_RETRY_MASK = 0x0008
_PWR_MGT_MASK = 0x0010
_MORE_DATA_MASK = 0x0020
_WEP_MASK = 0x0040
_ORDER_MASK = 0x0080
_FRAGMENT_NUMBER_MASK = 0x000F
_SEQUENCE_NUMBER_MASK = 0XFFF0
_VERSION_SHIFT = 8
_TYPE_SHIFT = 10
_SUBTYPE_SHIFT = 12
_TO_DS_SHIFT = 0
_FROM_DS_SHIFT = 1
_MORE_FRAG_SHIFT = 2
_RETRY_SHIFT = 3
_PWR_MGT_SHIFT = 4
_MORE_DATA_SHIFT = 5
_WEP_SHIFT = 6
_ORDER_SHIFT = 7
_SEQUENCE_NUMBER_SHIFT = 4

# IEs
IE_SSID = 0
IE_RATES = 1
IE_FH = 2
IE_DS = 3
IE_CF = 4
IE_TIM = 5
IE_IBSS = 6
IE_HT_CAPA = 45
IE_ESR = 50
IE_HT_INFO = 61

FCS_LENGTH = 4

FRAMES_WITH_CAPABILITY = [M_BEACON, M_ASSOC_RESP, M_ASSOC_REQ, M_REASSOC_REQ, ]

# Block Ack control constants
_ACK_POLICY_SHIFT = 0
_MULTI_TID_SHIFT = 1
_COMPRESSED_SHIFT = 2
_TID_SHIFT = 12

_ACK_POLICY_MASK = 0x0001
_MULTI_TID_MASK = 0x0002
_COMPRESSED_MASK = 0x0004
_TID_MASK = 0xf000

_COMPRESSED_BMP_LENGTH = 8
_BMP_LENGTH = 128

# Action frame categories
BLOCK_ACK = 3

# Block ack category action codes
BLOCK_ACK_CODE_REQUEST = 0
BLOCK_ACK_CODE_RESPONSE = 1
BLOCK_ACK_CODE_DELBA = 2


class IEEE80211(dpkt.Packet):
    """IEEE 802.11.

    IEEE 802.11 is part of the IEEE 802 set of local area network (LAN) technical standards,
    and specifies the set of media access control (MAC) and physical layer (PHY) protocols
    for implementing wireless local area network (WLAN) computer communication.

    Attributes:
        __hdr__: Header fields of IEEE802.11.
            framectl: (int): Frame control (2 bytes)
            duration: (int): Duration ID (2 bytes)
    """

    __hdr__ = (
        ('framectl', 'H', 0),
        ('duration', 'H', 0)
    )

    # The standard really defines the entire MAC protocol as little-endian on the wire,
    # however there is broken logic in the rest of the module preventing this from working right now
    #  __byte_order__ = '<'

    @property
    def version(self):
        return (self.framectl & _VERSION_MASK) >> _VERSION_SHIFT

    @version.setter
    def version(self, val):
        self.framectl = (val << _VERSION_SHIFT) | (self.framectl & ~_VERSION_MASK)

    @property
    def type(self):
        return (self.framectl & _TYPE_MASK) >> _TYPE_SHIFT

    @type.setter
    def type(self, val):
        self.framectl = (val << _TYPE_SHIFT) | (self.framectl & ~_TYPE_MASK)

    @property
    def subtype(self):
        return (self.framectl & _SUBTYPE_MASK) >> _SUBTYPE_SHIFT

    @subtype.setter
    def subtype(self, val):
        self.framectl = (val << _SUBTYPE_SHIFT) | (self.framectl & ~_SUBTYPE_MASK)

    @property
    def to_ds(self):
        return (self.framectl & _TO_DS_MASK) >> _TO_DS_SHIFT

    @to_ds.setter
    def to_ds(self, val):
        self.framectl = (val << _TO_DS_SHIFT) | (self.framectl & ~_TO_DS_MASK)

    @property
    def from_ds(self):
        return (self.framectl & _FROM_DS_MASK) >> _FROM_DS_SHIFT

    @from_ds.setter
    def from_ds(self, val):
        self.framectl = (val << _FROM_DS_SHIFT) | (self.framectl & ~_FROM_DS_MASK)

    @property
    def more_frag(self):
        return (self.framectl & _MORE_FRAG_MASK) >> _MORE_FRAG_SHIFT

    @more_frag.setter
    def more_frag(self, val):
        self.framectl = (val << _MORE_FRAG_SHIFT) | (self.framectl & ~_MORE_FRAG_MASK)

    @property
    def retry(self):
        return (self.framectl & _RETRY_MASK) >> _RETRY_SHIFT

    @retry.setter
    def retry(self, val):
        self.framectl = (val << _RETRY_SHIFT) | (self.framectl & ~_RETRY_MASK)

    @property
    def pwr_mgt(self):
        return (self.framectl & _PWR_MGT_MASK) >> _PWR_MGT_SHIFT

    @pwr_mgt.setter
    def pwr_mgt(self, val):
        self.framectl = (val << _PWR_MGT_SHIFT) | (self.framectl & ~_PWR_MGT_MASK)

    @property
    def more_data(self):
        return (self.framectl & _MORE_DATA_MASK) >> _MORE_DATA_SHIFT

    @more_data.setter
    def more_data(self, val):
        self.framectl = (val << _MORE_DATA_SHIFT) | (self.framectl & ~_MORE_DATA_MASK)

    @property
    def wep(self):
        return (self.framectl & _WEP_MASK) >> _WEP_SHIFT

    @wep.setter
    def wep(self, val):
        self.framectl = (val << _WEP_SHIFT) | (self.framectl & ~_WEP_MASK)

    @property
    def order(self):
        return (self.framectl & _ORDER_MASK) >> _ORDER_SHIFT

    @order.setter
    def order(self, val):
        self.framectl = (val << _ORDER_SHIFT) | (self.framectl & ~_ORDER_MASK)

    def unpack_ies(self, buf):
        self.ies = []

        ie_decoder = {
            IE_SSID: ('ssid', self.IE),
            IE_RATES: ('rate', self.IE),
            IE_FH: ('fh', self.FH),
            IE_DS: ('ds', self.DS),
            IE_CF: ('cf', self.CF),
            IE_TIM: ('tim', self.TIM),
            IE_IBSS: ('ibss', self.IBSS),
            IE_HT_CAPA: ('ht_capa', self.IE),
            IE_ESR: ('esr', self.IE),
            IE_HT_INFO: ('ht_info', self.IE)
        }

        # each IE starts with an ID and a length
        while len(buf) > FCS_LENGTH:
            ie_id = struct.unpack('B', buf[:1])[0]
            try:
                parser = ie_decoder[ie_id][1]
                name = ie_decoder[ie_id][0]
            except KeyError:
                parser = self.IE
                name = 'ie_' + str(ie_id)
            ie = parser(buf)

            ie.data = buf[2:2 + ie.len]
            setattr(self, name, ie)
            self.ies.append(ie)
            buf = buf[2 + ie.len:]

    class Capability(object):
        def __init__(self, field):
            self.ess = field & 1
            self.ibss = (field >> 1) & 1
            self.cf_poll = (field >> 2) & 1
            self.cf_poll_req = (field >> 3) & 1
            self.privacy = (field >> 4) & 1
            self.short_preamble = (field >> 5) & 1
            self.pbcc = (field >> 6) & 1
            self.hopping = (field >> 7) & 1
            self.spec_mgmt = (field >> 8) & 1
            self.qos = (field >> 9) & 1
            self.short_slot = (field >> 10) & 1
            self.apsd = (field >> 11) & 1
            self.dsss = (field >> 13) & 1
            self.delayed_blk_ack = (field >> 14) & 1
            self.imm_blk_ack = (field >> 15) & 1

    def __init__(self, *args, **kwargs):
        if kwargs and 'fcs' in kwargs:
            self.fcs_present = kwargs.pop('fcs')
        else:
            self.fcs_present = False

        super(IEEE80211, self).__init__(*args, **kwargs)

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.data = buf[self.__hdr_len__:]

        m_decoder = {
            M_BEACON: ('beacon', self.Beacon),
            M_ASSOC_REQ: ('assoc_req', self.Assoc_Req),
            M_ASSOC_RESP: ('assoc_resp', self.Assoc_Resp),
            M_DISASSOC: ('diassoc', self.Disassoc),
            M_REASSOC_REQ: ('reassoc_req', self.Reassoc_Req),
            M_REASSOC_RESP: ('reassoc_resp', self.Assoc_Resp),
            M_AUTH: ('auth', self.Auth),
            M_PROBE_RESP: ('probe_resp', self.Beacon),
            M_DEAUTH: ('deauth', self.Deauth),
            M_ACTION: ('action', self.Action)
        }

        c_decoder = {
            C_RTS: ('rts', self.RTS),
            C_CTS: ('cts', self.CTS),
            C_ACK: ('ack', self.ACK),
            C_BLOCK_ACK_REQ: ('bar', self.BlockAckReq),
            C_BLOCK_ACK: ('back', self.BlockAck),
            C_CF_END: ('cf_end', self.CFEnd),
        }

        d_dsData = {
            0: self.Data,
            FROM_DS_FLAG: self.DataFromDS,
            TO_DS_FLAG: self.DataToDS,
            INTER_DS_FLAG: self.DataInterDS
        }

        # For now decode everything with DATA. Haven't checked about other QoS
        # additions
        d_decoder = {
            # modified the decoder to consider the ToDS and FromDS flags
            # Omitting the 11 case for now
            D_DATA: ('data_frame', d_dsData),
            D_NULL: ('data_frame', d_dsData),
            D_QOS_DATA: ('data_frame', d_dsData),
            D_QOS_NULL: ('data_frame', d_dsData)
        }

        decoder = {
            MGMT_TYPE: m_decoder,
            CTL_TYPE: c_decoder,
            DATA_TYPE: d_decoder
        }

        # Strip off the FCS field
        if self.fcs_present:
            self.fcs = struct.unpack('<I', self.data[-1 * FCS_LENGTH:])[0]
            self.data = self.data[0: -1 * FCS_LENGTH]

        if self.type == MGMT_TYPE:
            self.mgmt = self.MGMT_Frame(self.data)
            self.data = self.mgmt.data
            if self.subtype == M_PROBE_REQ:
                self.unpack_ies(self.data)
                return
            if self.subtype == M_ATIM:
                return

        try:
            parser = decoder[self.type][self.subtype][1]
            name = decoder[self.type][self.subtype][0]
        except KeyError:
            raise dpkt.UnpackError("KeyError: type=%s subtype=%s" % (self.type, self.subtype))

        if self.type == DATA_TYPE:
            # need to grab the ToDS/FromDS info
            parser = parser[self.to_ds * 10 + self.from_ds]

        if self.type == MGMT_TYPE:
            field = parser(self.mgmt.data)
        else:
            field = parser(self.data)
            self.data = field

        setattr(self, name, field)

        if self.type == MGMT_TYPE:
            self.unpack_ies(field.data)
            if self.subtype in FRAMES_WITH_CAPABILITY:
                self.capability = self.Capability(ntole(field.capability))

        if self.type == DATA_TYPE and self.subtype == D_QOS_DATA:
            self.qos_data = self.QoS_Data(field.data)
            field.data = self.qos_data.data

        self.data = field.data

    class BlockAckReq(dpkt.Packet):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6),
            ('ctl', 'H', 0),
            ('seq', 'H', 0),
        )

    class BlockAck(dpkt.Packet):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6),
            ('ctl', 'H', 0),
            ('seq', 'H', 0),
        )

        @property
        def compressed(self):
            return (self.ctl & _COMPRESSED_MASK) >> _COMPRESSED_SHIFT

        @compressed.setter
        def compressed(self, val):
            self.ctl = (val << _COMPRESSED_SHIFT) | (self.ctl & ~_COMPRESSED_MASK)

        @property
        def ack_policy(self):
            return (self.ctl & _ACK_POLICY_MASK) >> _ACK_POLICY_SHIFT

        @ack_policy.setter
        def ack_policy(self, val):
            self.ctl = (val << _ACK_POLICY_SHIFT) | (self.ctl & ~_ACK_POLICY_MASK)

        @property
        def multi_tid(self):
            return (self.ctl & _MULTI_TID_MASK) >> _MULTI_TID_SHIFT

        @multi_tid.setter
        def multi_tid(self, val):
            self.ctl = (val << _MULTI_TID_SHIFT) | (self.ctl & ~_MULTI_TID_MASK)

        @property
        def tid(self):
            return (self.ctl & _TID_MASK) >> _TID_SHIFT

        @tid.setter
        def tid(self, val):
            self.ctl = (val << _TID_SHIFT) | (self.ctl & ~_TID_MASK)

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            self.data = buf[self.__hdr_len__:]
            self.ctl = ntole(self.ctl)

            if self.compressed:
                self.bmp = struct.unpack('8s', self.data[0:_COMPRESSED_BMP_LENGTH])[0]
            else:
                self.bmp = struct.unpack('128s', self.data[0:_BMP_LENGTH])[0]
            self.data = self.data[len(self.__hdr__) + len(self.bmp):]

    class _FragmentNumSeqNumMixin(object):
        @property
        def fragment_number(self):
            return ntole(self.frag_seq) & _FRAGMENT_NUMBER_MASK

        @property
        def sequence_number(self):
            return (ntole(self.frag_seq) & _SEQUENCE_NUMBER_MASK) >> _SEQUENCE_NUMBER_SHIFT

    class RTS(dpkt.Packet):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6)
        )

    class CTS(dpkt.Packet):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
        )

    class ACK(dpkt.Packet):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
        )

    class CFEnd(dpkt.Packet):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6),
        )

    class MGMT_Frame(dpkt.Packet, _FragmentNumSeqNumMixin):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6),
            ('bssid', '6s', '\x00' * 6),
            ('frag_seq', 'H', 0)
        )

    class Beacon(dpkt.Packet):
        __hdr__ = (
            ('timestamp', 'Q', 0),
            ('interval', 'H', 0),
            ('capability', 'H', 0)
        )

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            self.timestamp = ntole64(self.timestamp)
            self.interval = ntole(self.interval)

    class Disassoc(dpkt.Packet):
        __hdr__ = (
            ('reason', 'H', 0),
        )

    class Assoc_Req(dpkt.Packet):
        __hdr__ = (
            ('capability', 'H', 0),
            ('interval', 'H', 0)
        )

    class Assoc_Resp(dpkt.Packet):
        __hdr__ = (
            ('capability', 'H', 0),
            ('status', 'H', 0),
            ('aid', 'H', 0)
        )

    class Reassoc_Req(dpkt.Packet):
        __hdr__ = (
            ('capability', 'H', 0),
            ('interval', 'H', 0),
            ('current_ap', '6s', '\x00' * 6)
        )

    # This obviously doesn't support any of AUTH frames that use encryption
    class Auth(dpkt.Packet):
        __hdr__ = (
            ('algorithm', 'H', 0),
            ('auth_seq', 'H', 0),
        )

    class Deauth(dpkt.Packet):
        __hdr__ = (
            ('reason', 'H', 0),
        )

    class Action(dpkt.Packet):
        __hdr__ = (
            ('category', 'B', 0),
            ('code', 'B', 0),
        )

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)

            action_parser = {
                BLOCK_ACK: {
                    BLOCK_ACK_CODE_REQUEST: ('block_ack_request', IEEE80211.BlockAckActionRequest),
                    BLOCK_ACK_CODE_RESPONSE: ('block_ack_response', IEEE80211.BlockAckActionResponse),
                    BLOCK_ACK_CODE_DELBA: ('block_ack_delba', IEEE80211.BlockAckActionDelba),
                },
            }

            try:
                decoder = action_parser[self.category][self.code][1]
                field_name = action_parser[self.category][self.code][0]
            except KeyError:
                raise dpkt.UnpackError("KeyError: category=%s code=%s" % (self.category, self.code))

            field = decoder(self.data)
            setattr(self, field_name, field)
            self.data = field.data

    class BlockAckActionRequest(dpkt.Packet):
        __hdr__ = (
            ('dialog', 'B', 0),
            ('parameters', 'H', 0),
            ('timeout', 'H', 0),
            ('starting_seq', 'H', 0),
        )

    class BlockAckActionResponse(dpkt.Packet):
        __hdr__ = (
            ('dialog', 'B', 0),
            ('status_code', 'H', 0),
            ('parameters', 'H', 0),
            ('timeout', 'H', 0),
        )

    class BlockAckActionDelba(dpkt.Packet):
        __byte_order__ = '<'
        __hdr__ = (
            ('delba_param_set', 'H', 0),
            ('reason_code', 'H', 0),
            # ('gcr_group_addr', '8s', '\x00' * 8), # Standard says it must be there, but it isn't?
        )

    class Data(dpkt.Packet, _FragmentNumSeqNumMixin):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6),
            ('bssid', '6s', '\x00' * 6),
            ('frag_seq', 'H', 0)
        )

    class DataFromDS(dpkt.Packet, _FragmentNumSeqNumMixin):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
            ('bssid', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6),
            ('frag_seq', 'H', 0)
        )

    class DataToDS(dpkt.Packet, _FragmentNumSeqNumMixin):
        __hdr__ = (
            ('bssid', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6),
            ('dst', '6s', '\x00' * 6),
            ('frag_seq', 'H', 0)
        )

    class DataInterDS(dpkt.Packet, _FragmentNumSeqNumMixin):
        __hdr__ = (
            ('dst', '6s', '\x00' * 6),
            ('src', '6s', '\x00' * 6),
            ('da', '6s', '\x00' * 6),
            ('frag_seq', 'H', 0),
            ('sa', '6s', '\x00' * 6)
        )

    class QoS_Data(dpkt.Packet):
        __hdr__ = (
            ('control', 'H', 0),
        )

    class IE(dpkt.Packet):
        __hdr__ = (
            ('id', 'B', 0),
            ('len', 'B', 0)
        )

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            self.info = buf[2:self.len + 2]

    class FH(dpkt.Packet):
        __hdr__ = (
            ('id', 'B', 0),
            ('len', 'B', 0),
            ('tu', 'H', 0),
            ('hopset', 'B', 0),
            ('hoppattern', 'B', 0),
            ('hopindex', 'B', 0)
        )

    class DS(dpkt.Packet):
        __hdr__ = (
            ('id', 'B', 0),
            ('len', 'B', 0),
            ('ch', 'B', 0)
        )

    class CF(dpkt.Packet):
        __hdr__ = (
            ('id', 'B', 0),
            ('len', 'B', 0),
            ('count', 'B', 0),
            ('period', 'B', 0),
            ('max', 'H', 0),
            ('dur', 'H', 0)
        )

    class TIM(dpkt.Packet):
        __hdr__ = (
            ('id', 'B', 0),
            ('len', 'B', 0),
            ('count', 'B', 0),
            ('period', 'B', 0),
            ('ctrl', 'H', 0)
        )

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            self.bitmap = buf[5:self.len + 2]

    class IBSS(dpkt.Packet):
        __hdr__ = (
            ('id', 'B', 0),
            ('len', 'B', 0),
            ('atim', 'H', 0)
        )


def test_802211_ack():
    s = b'\xd4\x00\x00\x00\x00\x12\xf0\xb6\x1c\xa4\xff\xff\xff\xff'
    ieee = IEEE80211(s, fcs=True)
    assert ieee.version == 0
    assert ieee.type == CTL_TYPE
    assert ieee.subtype == C_ACK
    assert ieee.to_ds == 0
    assert ieee.from_ds == 0
    assert ieee.pwr_mgt == 0
    assert ieee.more_data == 0
    assert ieee.wep == 0
    assert ieee.order == 0
    assert ieee.ack.dst == b'\x00\x12\xf0\xb6\x1c\xa4'
    fcs = struct.unpack('<I', s[-4:])[0]
    assert ieee.fcs == fcs


def test_80211_beacon():
    s = (
        b'\x80\x00\x00\x00\xff\xff\xff\xff\xff\xff\x00\x26\xcb\x18\x6a\x30\x00\x26\xcb\x18\x6a\x30'
        b'\xa0\xd0\x77\x09\x32\x03\x8f\x00\x00\x00\x66\x00\x31\x04\x00\x04\x43\x41\x45\x4e\x01\x08'
        b'\x82\x84\x8b\x0c\x12\x96\x18\x24\x03\x01\x01\x05\x04\x00\x01\x00\x00\x07\x06\x55\x53\x20'
        b'\x01\x0b\x1a\x0b\x05\x00\x00\x6e\x00\x00\x2a\x01\x02\x2d\x1a\x6e\x18\x1b\xff\xff\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x30\x14\x01'
        b'\x00\x00\x0f\xac\x04\x01\x00\x00\x0f\xac\x04\x01\x00\x00\x0f\xac\x01\x28\x00\x32\x04\x30'
        b'\x48\x60\x6c\x36\x03\x51\x63\x03\x3d\x16\x01\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x85\x1e\x05\x00\x8f\x00\x0f\x00\xff\x03\x59\x00'
        b'\x63\x73\x65\x2d\x33\x39\x31\x32\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x36\x96\x06'
        b'\x00\x40\x96\x00\x14\x00\xdd\x18\x00\x50\xf2\x02\x01\x01\x80\x00\x03\xa4\x00\x00\x27\xa4'
        b'\x00\x00\x42\x43\x5e\x00\x62\x32\x2f\x00\xdd\x06\x00\x40\x96\x01\x01\x04\xdd\x05\x00\x40'
        b'\x96\x03\x05\xdd\x05\x00\x40\x96\x0b\x09\xdd\x08\x00\x40\x96\x13\x01\x00\x34\x01\xdd\x05'
        b'\x00\x40\x96\x14\x05'
    )
    ieee = IEEE80211(s, fcs=True)
    assert ieee.version == 0
    assert ieee.type == MGMT_TYPE
    assert ieee.subtype == M_BEACON
    assert ieee.to_ds == 0
    assert ieee.from_ds == 0
    assert ieee.pwr_mgt == 0
    assert ieee.more_data == 0
    assert ieee.wep == 0
    assert ieee.order == 0
    assert ieee.mgmt.dst == b'\xff\xff\xff\xff\xff\xff'
    assert ieee.mgmt.src == b'\x00\x26\xcb\x18\x6a\x30'
    assert ieee.beacon.capability == 0x3104
    assert ieee.capability.privacy == 1
    assert ieee.ssid.data == b'CAEN'
    assert ieee.rate.data == b'\x82\x84\x8b\x0c\x12\x96\x18\x24'
    assert ieee.ds.data == b'\x01'
    assert ieee.tim.data == b'\x00\x01\x00\x00'
    fcs = struct.unpack('<I', s[-4:])[0]
    assert ieee.fcs == fcs


def test_80211_data():
    s = (
        b'\x08\x09\x20\x00\x00\x26\xcb\x17\x3d\x91\x00\x16\x44\xb0\xae\xc6\x00\x02\xb3\xd6\x26\x3c'
        b'\x80\x7e\xaa\xaa\x03\x00\x00\x00\x08\x00\x45\x00\x00\x28\x07\x27\x40\x00\x80\x06\x1d\x39'
        b'\x8d\xd4\x37\x3d\x3f\xf5\xd1\x69\xc0\x5f\x01\xbb\xb2\xd6\xef\x23\x38\x2b\x4f\x08\x50\x10'
        b'\x42\x04\xac\x17\x00\x00'
    )
    ieee = IEEE80211(s, fcs=True)
    assert ieee.type == DATA_TYPE
    assert ieee.subtype == D_DATA
    assert ieee.data_frame.dst == b'\x00\x02\xb3\xd6\x26\x3c'
    assert ieee.data_frame.src == b'\x00\x16\x44\xb0\xae\xc6'
    assert ieee.data_frame.frag_seq == 0x807e
    assert ieee.data_frame.fragment_number == 0
    assert ieee.data_frame.sequence_number == 2024
    assert ieee.data == (b'\xaa\xaa\x03\x00\x00\x00\x08\x00\x45\x00\x00\x28\x07\x27\x40\x00\x80\x06'
                         b'\x1d\x39\x8d\xd4\x37\x3d\x3f\xf5\xd1\x69\xc0\x5f\x01\xbb\xb2\xd6\xef\x23'
                         b'\x38\x2b\x4f\x08\x50\x10\x42\x04')
    assert ieee.fcs == struct.unpack('<I', b'\xac\x17\x00\x00')[0]

    from . import llc
    llc_pkt = llc.LLC(ieee.data_frame.data)
    ip_pkt = llc_pkt.data
    assert ip_pkt.dst == b'\x3f\xf5\xd1\x69'


def test_80211_data_qos():
    s = (
        b'\x88\x01\x3a\x01\x00\x26\xcb\x17\x44\xf0\x00\x23\xdf\xc9\xc0\x93\x00\x26\xcb\x17\x44\xf0'
        b'\x20\x7b\x00\x00\xaa\xaa\x03\x00\x00\x00\x88\x8e\x01\x00\x00\x74\x02\x02\x00\x74\x19\x80'
        b'\x00\x00\x00\x6a\x16\x03\x01\x00\x65\x01\x00\x00\x61\x03\x01\x4b\x4c\xa7\x7e\x27\x61\x6f'
        b'\x02\x7b\x3c\x72\x39\xe3\x7b\xd7\x43\x59\x91\x7f\xaa\x22\x47\x51\xb6\x88\x9f\x85\x90\x87'
        b'\x5a\xd1\x13\x20\xe0\x07\x00\x00\x68\xbd\xa4\x13\xb0\xd5\x82\x7e\xc7\xfb\xe7\xcc\xab\x6e'
        b'\x5d\x5a\x51\x50\xd4\x45\xc5\xa1\x65\x53\xad\xb5\x88\x5b\x00\x1a\x00\x2f\x00\x05\x00\x04'
        b'\x00\x35\x00\x0a\x00\x09\x00\x03\x00\x08\x00\x33\x00\x39\x00\x16\x00\x15\x00\x14\x01\x00'
        b'\xff\xff\xff\xff'
    )
    ieee = IEEE80211(s, fcs=True)
    assert ieee.type == DATA_TYPE
    assert ieee.subtype == D_QOS_DATA
    assert ieee.data_frame.dst == b'\x00\x26\xcb\x17\x44\xf0'
    assert ieee.data_frame.src == b'\x00\x23\xdf\xc9\xc0\x93'
    assert ieee.data_frame.frag_seq == 0x207b
    assert ieee.data_frame.fragment_number == 0
    assert ieee.data_frame.sequence_number == 1970
    assert ieee.data == (b'\xaa\xaa\x03\x00\x00\x00\x88\x8e\x01\x00\x00\x74\x02\x02\x00\x74\x19\x80'
                         b'\x00\x00\x00\x6a\x16\x03\x01\x00\x65\x01\x00\x00\x61\x03\x01\x4b\x4c\xa7'
                         b'\x7e\x27\x61\x6f\x02\x7b\x3c\x72\x39\xe3\x7b\xd7\x43\x59\x91\x7f\xaa\x22'
                         b'\x47\x51\xb6\x88\x9f\x85\x90\x87\x5a\xd1\x13\x20\xe0\x07\x00\x00\x68\xbd'
                         b'\xa4\x13\xb0\xd5\x82\x7e\xc7\xfb\xe7\xcc\xab\x6e\x5d\x5a\x51\x50\xd4\x45'
                         b'\xc5\xa1\x65\x53\xad\xb5\x88\x5b\x00\x1a\x00\x2f\x00\x05\x00\x04\x00\x35'
                         b'\x00\x0a\x00\x09\x00\x03\x00\x08\x00\x33\x00\x39\x00\x16\x00\x15\x00\x14\x01\x00')
    assert ieee.qos_data.control == 0x0
    assert ieee.fcs == struct.unpack('<I', b'\xff\xff\xff\xff')[0]


def test_bug():
    s = (b'\x88\x41\x2c\x00\x00\x26\xcb\x17\x44\xf0\x00\x1e\x52\x97\x14\x11\x00\x1f\x6d\xe8\x18\x00'
         b'\xd0\x07\x00\x00\x6f\x00\x00\x20\x00\x00\x00\x00')
    ieee = IEEE80211(s)
    assert ieee.wep == 1


def test_data_ds():
    # verifying the ToDS and FromDS fields and that we're getting the
    # correct values

    s = (b'\x08\x03\x00\x00\x01\x0b\x85\x00\x00\x00\x00\x26\xcb\x18\x73\x50\x01\x0b\x85\x00\x00\x00'
         b'\x00\x89\x00\x26\xcb\x18\x73\x50')
    ieee = IEEE80211(s)
    assert ieee.type == DATA_TYPE
    assert ieee.to_ds == 1
    assert ieee.from_ds == 1
    assert ieee.data_frame.sa == b'\x00\x26\xcb\x18\x73\x50'
    assert ieee.data_frame.src == b'\x00\x26\xcb\x18\x73\x50'
    assert ieee.data_frame.dst == b'\x01\x0b\x85\x00\x00\x00'
    assert ieee.data_frame.da == b'\x01\x0b\x85\x00\x00\x00'

    s = (b'\x88\x41\x50\x01\x00\x26\xcb\x17\x48\xc1\x00\x24\x2c\xe7\xfe\x8a\xff\xff\xff\xff\xff\xff'
         b'\x80\xa0\x00\x00\x09\x1a\x00\x20\x00\x00\x00\x00')
    ieee = IEEE80211(s)
    assert ieee.type == DATA_TYPE
    assert ieee.to_ds == 1
    assert ieee.from_ds == 0
    assert ieee.data_frame.bssid == b'\x00\x26\xcb\x17\x48\xc1'
    assert ieee.data_frame.src == b'\x00\x24\x2c\xe7\xfe\x8a'
    assert ieee.data_frame.dst == b'\xff\xff\xff\xff\xff\xff'

    s = b'\x08\x02\x02\x01\x00\x02\x44\xac\x27\x70\x00\x1f\x33\x39\x75\x44\x00\x1f\x33\x39\x75\x44\x90\xa4'
    ieee = IEEE80211(s)
    assert ieee.type == DATA_TYPE
    assert ieee.to_ds == 0
    assert ieee.from_ds == 1
    assert ieee.data_frame.bssid == b'\x00\x1f\x33\x39\x75\x44'
    assert ieee.data_frame.src == b'\x00\x1f\x33\x39\x75\x44'
    assert ieee.data_frame.dst == b'\x00\x02\x44\xac\x27\x70'


def test_compressed_block_ack():
    s = (b'\x94\x00\x00\x00\x34\xc0\x59\xd6\x3f\x62\xb4\x75\x0e\x46\x83\xc1\x05\x50\x80\xee\x03\x00'
         b'\x00\x00\x00\x00\x00\x00\xa2\xe4\x98\x45')
    ieee = IEEE80211(s, fcs=True)
    assert ieee.type == CTL_TYPE
    assert ieee.subtype == C_BLOCK_ACK
    assert ieee.back.dst == b'\x34\xc0\x59\xd6\x3f\x62'
    assert ieee.back.src == b'\xb4\x75\x0e\x46\x83\xc1'
    assert ieee.back.compressed == 1
    assert len(ieee.back.bmp) == 8
    assert ieee.back.ack_policy == 1
    assert ieee.back.tid == 5


def test_action_block_ack_request():
    s = (b'\xd0\x00\x3a\x01\x00\x23\x14\x36\x52\x30\xb4\x75\x0e\x46\x83\xc1\xb4\x75\x0e\x46\x83\xc1'
         b'\x70\x14\x03\x00\x0d\x02\x10\x00\x00\x40\x29\x06\x50\x33\x9e')
    ieee = IEEE80211(s, fcs=True)
    assert ieee.type == MGMT_TYPE
    assert ieee.subtype == M_ACTION
    assert ieee.action.category == BLOCK_ACK
    assert ieee.action.code == BLOCK_ACK_CODE_REQUEST
    assert ieee.action.block_ack_request.timeout == 0
    parameters = struct.unpack('<H', b'\x10\x02')[0]
    assert ieee.action.block_ack_request.parameters == parameters


def test_action_block_ack_response():
    s = (b'\xd0\x00\x3c\x00\xb4\x75\x0e\x46\x83\xc1\x00\x23\x14\x36\x52\x30\xb4\x75\x0e\x46\x83\xc1'
         b'\xd0\x68\x03\x01\x0d\x00\x00\x02\x10\x88\x13\x9f\xc0\x0b\x75')
    ieee = IEEE80211(s, fcs=True)
    assert ieee.type == MGMT_TYPE
    assert ieee.subtype == M_ACTION
    assert ieee.action.category == BLOCK_ACK
    assert ieee.action.code == BLOCK_ACK_CODE_RESPONSE
    timeout = struct.unpack('<H', b'\x13\x88')[0]
    assert ieee.action.block_ack_response.timeout == timeout
    parameters = struct.unpack('<H', b'\x10\x02')[0]
    assert ieee.action.block_ack_response.parameters == parameters


def test_action_block_ack_delete():
    s = (b'\xd0\x00\x2c\x00\x00\xc1\x41\x06\x13\x0d\x6c\xb2\xae\xae\xde\x80\x6c\xb2\xae\xae\xde\x80'
         b'\xa0\x52\x03\x02\x00\x08\x01\x00\x74\x5d\x0a\xc6')
    ieee = IEEE80211(s, fcs=True)
    assert ieee.type == MGMT_TYPE
    assert ieee.subtype == M_ACTION
    assert ieee.action.category == BLOCK_ACK
    assert ieee.action.code == BLOCK_ACK_CODE_DELBA
    assert ieee.action.block_ack_delba.delba_param_set == 0x0800
    assert ieee.action.block_ack_delba.reason_code == 1


def test_ieee80211_properties():
    ieee80211 = IEEE80211()
    assert ieee80211.version == 0
    ieee80211.version = 1
    assert ieee80211.version == 1

    assert ieee80211.type == 0
    ieee80211.type = 1
    assert ieee80211.type == 1

    assert ieee80211.subtype == 0
    ieee80211.subtype = 1
    assert ieee80211.subtype == 1

    assert ieee80211.to_ds == 0
    ieee80211.to_ds = 1
    assert ieee80211.to_ds == 1

    assert ieee80211.from_ds == 0
    ieee80211.from_ds = 1
    assert ieee80211.from_ds == 1

    assert ieee80211.more_frag == 0
    ieee80211.more_frag = 1
    assert ieee80211.more_frag == 1

    assert ieee80211.retry == 0
    ieee80211.retry = 0
    assert ieee80211.retry == 0

    assert ieee80211.pwr_mgt == 0
    ieee80211.pwr_mgt = 0
    assert ieee80211.pwr_mgt == 0

    assert ieee80211.more_data == 0
    ieee80211.more_data = 0
    assert ieee80211.more_data == 0

    assert ieee80211.wep == 0
    ieee80211.wep = 1
    assert ieee80211.wep == 1

    assert ieee80211.order == 0
    ieee80211.order = 1
    assert ieee80211.order == 1


def test_blockack_properties():
    blockack = IEEE80211.BlockAck()
    assert blockack.compressed == 0
    blockack.compressed = 1
    assert blockack.compressed == 1

    assert blockack.ack_policy == 0
    blockack.ack_policy = 1
    assert blockack.ack_policy == 1

    assert blockack.multi_tid == 0
    blockack.multi_tid = 1
    assert blockack.multi_tid == 1

    assert blockack.tid == 0
    blockack.tid = 1
    assert blockack.tid == 1


def test_ieee80211_unpack():
    import pytest
    from binascii import unhexlify

    buf = unhexlify(
        '4000'  # subtype set to M_PROBE_REQ
        '0000'

        # MGMT_Frame
        '000000000000'  # dst
        '000000000000'  # src
        '000000000000'  # bssid
        '0000'          # frag_seq
    )
    ieee80211 = IEEE80211(buf)
    assert ieee80211.ies == []

    buf = unhexlify(
        '9000'  # subtype set to M_ATIM
        '0000'

        # MGMT_Frame
        '000000000000'  # dst
        '000000000000'  # src
        '000000000000'  # bssid
        '0000'          # frag_seq
    )
    ieee80211 = IEEE80211(buf)
    assert not hasattr(ieee80211, 'ies')

    buf = unhexlify(
        '0c00'  # type set to invalid value
        '0000'
    )
    with pytest.raises(dpkt.UnpackError, match="KeyError: type=3 subtype=0"):
        IEEE80211(buf)


def test_blockack_unpack():
    from binascii import unhexlify
    # unpack a non-compressed BlockAck
    buf = unhexlify(
        '000000000000'
        '000000000000'
        '0000'   # compressed flag not set
        '0000'
    ) + b'\xff' * 128

    blockack = IEEE80211.BlockAck(buf)
    assert blockack.bmp == b'\xff' * 128
    assert blockack.data == b''


def test_action_unpack():
    import pytest
    from binascii import unhexlify
    buf = unhexlify(
        '01'  # category
        '00'  # code (non-existent)
    )
    with pytest.raises(dpkt.UnpackError, match="KeyError: category=1 code=0"):
        IEEE80211.Action(buf)


def test_beacon_unpack():
    beacon_payload = b"\xb9\x71\xfa\x45\x52\x02\x00\x00\x64\x00\x11\x04"
    beacon = IEEE80211.Beacon(beacon_payload)
    assert beacon.timestamp == 0x0000025245fa71b9
    assert beacon.interval == 100
    assert beacon.capability == 0x1104


def test_fragment_and_sequence_values():
    from binascii import unhexlify
    for raw_frag_seq, (expected_frag_num, expected_seq_num) in [
        ("0000", (0, 0)),
        ("0F00", (15, 0)),
        ("0111", (1, 272)),
        ("B3FF", (3, 4091))
    ]:
        buf = unhexlify(
            '000000000000'  # dst
            '000000000000'  # src
            '000000000000'  # bssid
            + raw_frag_seq
        )
        data = IEEE80211.Data(buf)
        assert data.fragment_number == expected_frag_num
        assert data.sequence_number == expected_seq_num

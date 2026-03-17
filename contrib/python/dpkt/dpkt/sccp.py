# $Id: sccp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Cisco Skinny Client Control Protocol."""
from __future__ import absolute_import

from . import dpkt

KEYPAD_BUTTON = 0x00000003
OFF_HOOK = 0x00000006
ON_HOOK = 0x00000007
OPEN_RECEIVE_CHANNEL_ACK = 0x00000022
START_TONE = 0x00000082
STOP_TONE = 0x00000083
SET_LAMP = 0x00000086
SET_SPEAKER_MODE = 0x00000088
START_MEDIA_TRANSMIT = 0x0000008A
STOP_MEDIA_TRANSMIT = 0x0000008B
CALL_INFO = 0x0000008F
DEFINE_TIME_DATE = 0x00000094
DISPLAY_TEXT = 0x00000099
OPEN_RECEIVE_CHANNEL = 0x00000105
CLOSE_RECEIVE_CHANNEL = 0x00000106
SELECT_SOFTKEYS = 0x00000110
CALL_STATE = 0x00000111
DISPLAY_PROMPT_STATUS = 0x00000112
CLEAR_PROMPT_STATUS = 0x00000113
ACTIVATE_CALL_PLANE = 0x00000116


class ActivateCallPlane(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('line_instance', 'I', 0),
    )


class CallInfo(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('calling_party_name', '40s', b''),
        ('calling_party', '24s', b''),
        ('called_party_name', '40s', b''),
        ('called_party', '24s', b''),
        ('line_instance', 'I', 0),
        ('call_id', 'I', 0),
        ('call_type', 'I', 0),
        ('orig_called_party_name', '40s', b''),
        ('orig_called_party', '24s', b'')
    )


class CallState(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('call_state', 'I', 12),  # 12: Proceed, 15: Connected
        ('line_instance', 'I', 1),
        ('call_id', 'I', 0)
    )


class ClearPromptStatus(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('line_instance', 'I', 1),
        ('call_id', 'I', 0)
    )


class CloseReceiveChannel(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('conference_id', 'I', 0),
        ('passthruparty_id', 'I', 0),
    )


class DisplayPromptStatus(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('msg_timeout', 'I', 0),
        ('display_msg', '32s', b''),
        ('line_instance', 'I', 1),
        ('call_id', 'I', 0)
    )


class DisplayText(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('display_msg', '36s', b''),
    )


class KeypadButton(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('button', 'I', 0),
    )


class OpenReceiveChannel(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('conference_id', 'I', 0),
        ('passthruparty_id', 'I', 0),
        ('ms_packet', 'I', 0),
        ('payload_capability', 'I', 4),  # 4: G.711 u-law 64k
        ('echo_cancel_type', 'I', 4),
        ('g723_bitrate', 'I', 0),
    )


class OpenReceiveChannelAck(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('channel_status', 'I', 0),
        ('ip', '4s', b''),
        ('port', 'I', 0),
        ('passthruparty_id', 'I', 0),
    )


class SelectStartKeys(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('line_id', 'I', 1),
        ('call_id', 'I', 0),
        ('softkey_set', 'I', 8),
        ('softkey_map', 'I', 0xffffffff)
    )


class SetLamp(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('stimulus', 'I', 9),  # 9: Line
        ('stimulus_instance', 'I', 1),
        ('lamp_mode', 'I', 1),
    )


class SetSpeakerMode(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('speaker', 'I', 2),  # 2: SpeakerOff
    )


class StartMediaTransmission(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('conference_id', 'I', 0),
        ('passthruparty_id', 'I', 0),
        ('ipv4_or_ipv6', 'I', 0),
        ('remote_ip', '16s', b''),
        ('remote_port', 'I', 0),
        ('ms_packet', 'I', 0),
        ('payload_capability', 'I', 4),  # 4: G.711 u-law 64k
        ('precedence', 'I', 0),
        ('silence_suppression', 'I', 0),
        ('max_frames_per_pkt', 'I', 1),
        ('g723_bitrate', 'I', 0),
        ('call_reference', 'I', 0)
    )


class StartTone(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('tone', 'I', 0x24),  # 0x24: AlertingTone
    )


class StopMediaTransmission(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('conference_id', 'I', 0),
        ('passthruparty_id', 'I', 0),
    )


class SCCP(dpkt.Packet):
    """Cisco Skinny Client Control Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of SCCP.
        TODO.
    """

    __byte_order__ = '<'
    __hdr__ = (
        ('len', 'I', 0),
        ('rsvd', 'I', 0),
        ('msgid', 'I', 0),
        ('msg', '0s', b''),
    )
    _msgsw = {
        KEYPAD_BUTTON: KeypadButton,
        OPEN_RECEIVE_CHANNEL_ACK: OpenReceiveChannelAck,
        START_TONE: StartTone,
        SET_LAMP: SetLamp,
        START_MEDIA_TRANSMIT: StartMediaTransmission,
        STOP_MEDIA_TRANSMIT: StopMediaTransmission,
        CALL_INFO: CallInfo,
        DISPLAY_TEXT: DisplayText,
        OPEN_RECEIVE_CHANNEL: OpenReceiveChannel,
        CLOSE_RECEIVE_CHANNEL: CloseReceiveChannel,
        CALL_STATE: CallState,
        DISPLAY_PROMPT_STATUS: DisplayPromptStatus,
        CLEAR_PROMPT_STATUS: ClearPromptStatus,
        ACTIVATE_CALL_PLANE: ActivateCallPlane,
    }

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        n = self.len - 4
        if n > len(self.data):
            raise dpkt.NeedData('not enough data')
        self.msg, self.data = self.data[:n], self.data[n:]
        try:
            p = self._msgsw[self.msgid](self.msg)
            setattr(self, p.__class__.__name__.lower(), p)
        except (KeyError, dpkt.UnpackError):
            pass


def test_sccp():
    import pytest
    from binascii import unhexlify
    buf = unhexlify(
        '08000000'  # len
        '00000000'  # rsvd
        '03000000'  # msgid (KEYPAD_BUTTON)

        'abcdef01'  # msg
        '23456789'  # daat
    )
    sccp = SCCP(buf)
    assert sccp.msg == b'\xab\xcd\xef\x01'
    assert sccp.data == b'\x23\x45\x67\x89'
    assert isinstance(sccp.keypadbutton, KeypadButton)

    # len is too long for data, raises NeedData
    buf = unhexlify(
        '88880000'  # len
        '00000000'  # rsvd
        '00000003'  # msgid (KEYPAD_BUTTON)

        'abcdef01'  # msg
    )
    with pytest.raises(dpkt.NeedData):
        SCCP(buf)

    # msgid is invalid, raises KeyError on _msgsw (silently caught)
    buf = unhexlify(
        '08000000'  # len
        '00000000'  # rsvd
        '00000003'  # msgid (invalid)

        'abcdef01'  # msg
    )
    sccp = SCCP(buf)
    assert sccp.msg == b'\xab\xcd\xef\x01'
    assert sccp.data == b''

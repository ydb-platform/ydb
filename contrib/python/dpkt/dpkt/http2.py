# -*- coding: utf-8 -*-
"""Hypertext Transfer Protocol Version 2."""

import struct
import codecs

from . import dpkt


HTTP2_PREFACE = b'\x50\x52\x49\x20\x2a\x20\x48\x54\x54\x50\x2f\x32\x2e\x30\x0d\x0a\x0d\x0a\x53\x4d\x0d\x0a\x0d\x0a'

# Frame types
HTTP2_FRAME_DATA = 0
HTTP2_FRAME_HEADERS = 1
HTTP2_FRAME_PRIORITY = 2
HTTP2_FRAME_RST_STREAM = 3
HTTP2_FRAME_SETTINGS = 4
HTTP2_FRAME_PUSH_PROMISE = 5
HTTP2_FRAME_PING = 6
HTTP2_FRAME_GOAWAY = 7
HTTP2_FRAME_WINDOW_UPDATE = 8
HTTP2_FRAME_CONTINUATION = 9

# Flags
HTTP2_FLAG_END_STREAM = 0x01  # for DATA and HEADERS frames
HTTP2_FLAG_ACK = 0x01  # for SETTINGS and PING frames
HTTP2_FLAG_END_HEADERS = 0x04
HTTP2_FLAG_PADDED = 0x08
HTTP2_FLAG_PRIORITY = 0x20

# Settings
HTTP2_SETTINGS_HEADER_TABLE_SIZE = 0x1
HTTP2_SETTINGS_ENABLE_PUSH = 0x2
HTTP2_SETTINGS_MAX_CONCURRENT_STREAMS = 0x3
HTTP2_SETTINGS_INITIAL_WINDOW_SIZE = 0x4
HTTP2_SETTINGS_MAX_FRAME_SIZE = 0x5
HTTP2_SETTINGS_MAX_HEADER_LIST_SIZE = 0x6

# Error codes
HTTP2_NO_ERROR = 0x0
HTTP2_PROTOCOL_ERROR = 0x1
HTTP2_INTERNAL_ERROR = 0x2
HTTP2_FLOW_CONTROL_ERROR = 0x3
HTTP2_SETTINGS_TIMEOUT = 0x4
HTTP2_STREAM_CLOSED = 0x5
HTTP2_FRAME_SIZE_ERROR = 0x6
HTTP2_REFUSED_STREAM = 0x7
HTTP2_CANCEL = 0x8
HTTP2_COMPRESSION_ERROR = 0x9
HTTP2_CONNECT_ERROR = 0xa
HTTP2_ENHANCE_YOUR_CALM = 0xb
HTTP2_INADEQUATE_SECURITY = 0xc
HTTP2_HTTP_1_1_REQUIRED = 0xd

error_code_str = {
    HTTP2_NO_ERROR: 'NO_ERROR',
    HTTP2_PROTOCOL_ERROR: 'PROTOCOL_ERROR',
    HTTP2_INTERNAL_ERROR: 'INTERNAL_ERROR',
    HTTP2_FLOW_CONTROL_ERROR: 'FLOW_CONTROL_ERROR',
    HTTP2_SETTINGS_TIMEOUT: 'SETTINGS_TIMEOUT',
    HTTP2_STREAM_CLOSED: 'STREAM_CLOSED',
    HTTP2_FRAME_SIZE_ERROR: 'FRAME_SIZE_ERROR',
    HTTP2_REFUSED_STREAM: 'REFUSED_STREAM',
    HTTP2_CANCEL: 'CANCEL',
    HTTP2_COMPRESSION_ERROR: 'COMPRESSION_ERROR',
    HTTP2_CONNECT_ERROR: 'CONNECT_ERROR',
    HTTP2_ENHANCE_YOUR_CALM: 'ENHANCE_YOUR_CALM',
    HTTP2_INADEQUATE_SECURITY: 'INADEQUATE_SECURITY',
    HTTP2_HTTP_1_1_REQUIRED: 'HTTP_1_1_REQUIRED',
}


class HTTP2Exception(Exception):
    pass


class Preface(dpkt.Packet):
    __hdr__ = (
        ('preface', '24s', HTTP2_PREFACE),
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if self.preface != HTTP2_PREFACE:
            raise HTTP2Exception('Invalid HTTP/2 preface')
        self.data = ''


class Frame(dpkt.Packet):
    """
    An HTTP/2 frame as defined in RFC 7540
    """

    # struct.unpack can't handle the 3-byte int, so we parse it as bytes
    # (and store it as bytes so dpkt doesn't get confused), and turn it into
    # an int in a user-facing property
    __hdr__ = (
        ('length_bytes', '3s', 0),
        ('type', 'B', 0),
        ('flags', 'B', 0),
        ('stream_id', 'I', 0),
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        # only take the right number of bytes
        self.data = self.data[:self.length]
        if len(self.data) != self.length:
            raise dpkt.NeedData

    @property
    def length(self):
        return struct.unpack('!I', b'\x00' + self.length_bytes)[0]


class Priority(dpkt.Packet):
    """
    Payload of a PRIORITY frame, also used in HEADERS frame with FLAG_PRIORITY.

    Also used in the HEADERS frame if the PRIORITY flag is set.
    """

    __hdr__ = (
        ('stream_dep', 'I', 0),
        ('weight', 'B', 0),
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if len(self.data) != 0:
            raise HTTP2Exception('Invalid number of bytes in PRIORITY frame')
        self.exclusive = (self.stream_dep & 0x80000000) != 0
        self.stream_dep &= 0x7fffffff
        self.weight += 1


class Setting(dpkt.Packet):
    """
    A key-value pair used in the SETTINGS frame.
    """

    __hdr__ = (
        ('identifier', 'H', 0),
        ('value', 'I', 0),
    )


class PaddedFrame(Frame):
    """
    Abstract class for frame types that support the FLAG_PADDED flag: DATA,
    HEADERS and PUSH_PROMISE.
    """

    def unpack(self, buf):
        Frame.unpack(self, buf)
        if self.flags & HTTP2_FLAG_PADDED:
            if self.length == 0:
                raise HTTP2Exception('Missing padding length in PADDED frame')
            self.pad_length = struct.unpack('B', self.data[0:1])[0]
            if self.length <= self.pad_length:
                raise HTTP2Exception('Missing padding bytes in PADDED frame')
            self.unpadded_data = self.data[1:-self.pad_length]
        else:
            self.unpadded_data = self.data


class DataFrame(PaddedFrame):
    """
    Frame of type DATA.
    """

    @property
    def payload(self):
        return self.unpadded_data


class HeadersFrame(PaddedFrame):
    """
    Frame of type HEADERS.
    """

    def unpack(self, buf):
        PaddedFrame.unpack(self, buf)
        if self.flags & HTTP2_FLAG_PRIORITY:
            if len(self.unpadded_data) < 5:
                raise HTTP2Exception('Missing stream dependency in HEADERS frame with PRIORITY flag')
            self.priority = Priority(self.unpadded_data[:5])
            self.block_fragment = self.unpadded_data[5:]
        else:
            self.block_fragment = self.unpadded_data


class PriorityFrame(Frame):
    """
    Frame of type PRIORITY.
    """

    def unpack(self, buf):
        Frame.unpack(self, buf)
        self.priority = Priority(self.data)


class RSTStreamFrame(Frame):
    """
    Frame of type RST_STREAM.
    """

    def unpack(self, buf):
        Frame.unpack(self, buf)
        if self.length != 4:
            raise HTTP2Exception('Invalid number of bytes in RST_STREAM frame (must be 4)')
        self.error_code = struct.unpack('!I', self.data)[0]


class SettingsFrame(Frame):
    """
    Frame of type SETTINGS.
    """

    def unpack(self, buf):
        Frame.unpack(self, buf)
        if self.length % 6 != 0:
            raise HTTP2Exception('Invalid number of bytes in SETTINGS frame (must be multiple of 6)')
        self.settings = []
        i = 0
        while i < self.length:
            self.settings.append(Setting(self.data[i:i + 6]))
            i += 6


class PushPromiseFrame(PaddedFrame):
    """
    Frame of type PUSH_PROMISE.
    """

    def unpack(self, buf):
        PaddedFrame.unpack(self, buf)
        if len(self.unpadded_data) < 4:
            raise HTTP2Exception('Missing promised stream ID in PUSH_PROMISE frame')
        self.promised_id = struct.unpack('!I', self.data[:4])[0]
        self.block_fragment = self.unpadded_data[4:]


class PingFrame(Frame):
    """
    Frame of type PING.
    """

    def unpack(self, buf):
        Frame.unpack(self, buf)
        if self.length != 8:
            raise HTTP2Exception('Invalid number of bytes in PING frame (must be 8)')


class GoAwayFrame(Frame):
    """
    Frame of type GO_AWAY.
    """

    def unpack(self, buf):
        Frame.unpack(self, buf)
        if self.length < 8:
            raise HTTP2Exception('Invalid number of bytes in GO_AWAY frame')
        self.last_stream_id = struct.unpack('!I', self.data[:4])[0]
        self.error_code = struct.unpack('!I', self.data[4:8])[0]
        self.debug_data = self.data[8:]


class WindowUpdateFrame(Frame):
    """
    Frame of type WINDOW_UPDATE.
    """

    def unpack(self, buf):
        Frame.unpack(self, buf)
        if self.length != 4:
            raise HTTP2Exception('Invalid number of bytes in WINDOW_UPDATE frame (must be 4)')
        self.window_increment = struct.unpack('!I', self.data)[0]


class ContinuationFrame(Frame):
    """
    Frame of type CONTINUATION.
    """

    def unpack(self, buf):
        Frame.unpack(self, buf)
        self.block_fragment = self.data


FRAME_TYPES = {
    HTTP2_FRAME_DATA: ('DATA', DataFrame),
    HTTP2_FRAME_HEADERS: ('HEADERS', HeadersFrame),
    HTTP2_FRAME_PRIORITY: ('PRIORITY', PriorityFrame),
    HTTP2_FRAME_RST_STREAM: ('RST_STREAM', RSTStreamFrame),
    HTTP2_FRAME_SETTINGS: ('SETTINGS', SettingsFrame),
    HTTP2_FRAME_PUSH_PROMISE: ('PUSH_PROMISE', PushPromiseFrame),
    HTTP2_FRAME_PING: ('PING', PingFrame),
    HTTP2_FRAME_GOAWAY: ('GOAWAY', GoAwayFrame),
    HTTP2_FRAME_WINDOW_UPDATE: ('WINDOW_UPDATE', WindowUpdateFrame),
    HTTP2_FRAME_CONTINUATION: ('CONTINUATION', ContinuationFrame),
}


class FrameFactory(object):
    def __new__(cls, buf):
        if len(buf) < 4:
            raise dpkt.NeedData
        t = struct.unpack('B', buf[3:4])[0]
        frame_type = FRAME_TYPES.get(t, None)
        if frame_type is None:
            raise HTTP2Exception('Invalid frame type: ' + hex(t))
        return frame_type[1](buf)


def frame_multi_factory(buf, preface=False):
    """
    Attempt to parse one or more Frame's out of buf

    Args:
      buf: string containing HTTP/2 frames. May have an incomplete frame at the
        end.
      preface: expect an HTTP/2 preface at the beginning of the buffer.

    Returns:
      [Frame]
      int, total bytes consumed, != len(buf) if an incomplete frame was left at
        the end.
    """
    i = 0
    n = len(buf)
    frames = []

    if preface:
        try:
            p = Preface(buf)
            i += len(p)
        except dpkt.NeedData:
            return [], 0

    while i < n:
        try:
            frame = FrameFactory(buf[i:])
            frames.append(frame)
            i += len(frame)
        except dpkt.NeedData:
            break
    return frames, i


class TestFrame(object):
    """Some data found in real traffic"""

    @classmethod
    def setup_class(cls):
        # First TLS AppData record sent by Firefox (decrypted)
        record = codecs.decode(b'505249202a20485454502f322e300d0a'
                               b'0d0a534d0d0a0d0a00000c0400000000'
                               b'00000400020000000500004000000004'
                               b'08000000000000bf0001000005020000'
                               b'00000300000000c80000050200000000'
                               b'05000000006400000502000000000700'
                               b'00000000000005020000000009000000'
                               b'070000000502000000000b0000000300', 'hex')
        cls.frames, cls.i = frame_multi_factory(record, preface=True)

    def test_frame(self):
        import pytest
        # Too short
        pytest.raises(dpkt.NeedData, Frame, codecs.decode(b'000001'  # length
                                                          b'0000'  # type, flags
                                                          b'deadbeef',  # stream id
                                                          'hex'))

    def test_data(self):
        # Padded DATA frame
        frame_data_padded = FrameFactory(codecs.decode(b'000008'  # length
                                                       b'0008'  # type, flags
                                                       b'12345678'  # stream id
                                                       b'05'  # pad length
                                                       b'abcd'  # data
                                                       b'1122334455',  # padding
                                                       'hex'))
        assert (frame_data_padded.length == 8)
        assert (frame_data_padded.type == HTTP2_FRAME_DATA)
        assert (frame_data_padded.flags == HTTP2_FLAG_PADDED)
        assert (frame_data_padded.stream_id == 0x12345678)
        assert (frame_data_padded.data == b'\x05\xAB\xCD\x11\x22\x33\x44\x55')
        assert (frame_data_padded.pad_length == 5)
        assert (frame_data_padded.unpadded_data == b'\xAB\xCD')
        assert (frame_data_padded.payload == b'\xAB\xCD')

        # empty DATA frame
        frame_data_empty_end = FrameFactory(codecs.decode(b'000000'  # length
                                                          b'0001'  # type, flags
                                                          b'deadbeef',  # stream id
                                                          'hex'))
        assert (frame_data_empty_end.length == 0)
        assert (frame_data_empty_end.type == HTTP2_FRAME_DATA)
        assert (frame_data_empty_end.flags == HTTP2_FLAG_END_STREAM)
        assert (frame_data_empty_end.stream_id == 0xdeadbeef)
        assert (frame_data_empty_end.data == b'')
        assert (frame_data_empty_end.unpadded_data == b'')
        assert (frame_data_empty_end.payload == b'')

        import pytest
        # Invalid padding
        with pytest.raises(HTTP2Exception) as e:
            DataFrame(codecs.decode(b'000000'  # length
                                    b'0008'  # type, flags
                                    b'12345678'  # stream id
                                    b'',  # missing padding
                                    'hex'))
        assert (str(e.value) == 'Missing padding length in PADDED frame')

        with pytest.raises(HTTP2Exception) as e:
            DataFrame(codecs.decode(b'000001'  # length
                                    b'0008'  # type, flags
                                    b'12345678'  # stream id
                                    b'01'
                                    b'',  # missing padding bytes
                                    'hex'))
        assert (str(e.value) == 'Missing padding bytes in PADDED frame')

    def test_headers(self):
        frame_headers = FrameFactory(codecs.decode(b'000003'  # length
                                                   b'0100'  # type, flags
                                                   b'deadbeef'  # stream id
                                                   b'f00baa',  # block fragment
                                                   'hex'))
        assert (frame_headers.length == 3)
        assert (frame_headers.type == HTTP2_FRAME_HEADERS)
        assert (frame_headers.flags == 0)
        assert (frame_headers.stream_id == 0xdeadbeef)
        assert (frame_headers.data == b'\xF0\x0B\xAA')
        assert (frame_headers.unpadded_data == b'\xF0\x0B\xAA')
        assert (frame_headers.block_fragment == b'\xF0\x0B\xAA')

        frame_headers_prio = FrameFactory(codecs.decode(b'000008'  # length
                                                        b'0120'  # type, flags
                                                        b'deadbeef'  # stream id
                                                        b'cafebabe10'  # priority
                                                        b'f00baa',  # block fragment
                                                        'hex'))
        assert (frame_headers_prio.length == 8)
        assert (frame_headers_prio.type == HTTP2_FRAME_HEADERS)
        assert (frame_headers_prio.flags == HTTP2_FLAG_PRIORITY)
        assert (frame_headers_prio.stream_id == 0xdeadbeef)
        assert (frame_headers_prio.data == b'\xCA\xFE\xBA\xBE\x10\xF0\x0B\xAA')
        assert (frame_headers_prio.unpadded_data == b'\xCA\xFE\xBA\xBE\x10\xF0\x0B\xAA')
        assert (frame_headers_prio.priority.exclusive is True)
        assert (frame_headers_prio.priority.stream_dep == 0x4afebabe)
        assert (frame_headers_prio.priority.weight == 0x11)
        assert (frame_headers_prio.block_fragment == b'\xF0\x0B\xAA')

        import pytest
        # Invalid priority
        with pytest.raises(HTTP2Exception) as e:
            HeadersFrame(codecs.decode(b'000002'  # length
                                       b'0120'  # type, flags
                                       b'deadbeef'  # stream id
                                       b'1234',  # invalid priority
                                       'hex'))
        assert (str(e.value) == 'Missing stream dependency in HEADERS frame with PRIORITY flag')

    def test_priority(self):
        frame_priority = FrameFactory(codecs.decode(b'000005'  # length
                                                    b'0200'  # type, flags
                                                    b'deadbeef'  # stream id
                                                    b'cafebabe'  # stream dep
                                                    b'12',  # weight
                                                    'hex'))
        assert (frame_priority.length == 5)
        assert (frame_priority.type == HTTP2_FRAME_PRIORITY)
        assert (frame_priority.flags == 0)
        assert (frame_priority.stream_id == 0xdeadbeef)
        assert (frame_priority.data == b'\xCA\xFE\xBA\xBE\x12')
        assert (frame_priority.priority.data == b'')
        assert (frame_priority.priority.exclusive is True)
        assert (frame_priority.priority.stream_dep == 0x4afebabe)
        assert (frame_priority.priority.weight == 0x13)

        import pytest
        # Invalid length
        with pytest.raises(HTTP2Exception) as e:
            PriorityFrame(codecs.decode(b'000006'  # length
                                        b'0200'  # type, flags
                                        b'deadbeef'  # stream id
                                        b'cafebabe'  # stream dep
                                        b'12'  # weight
                                        b'00',  # unexpected additional payload
                                        'hex'))
        assert (str(e.value) == 'Invalid number of bytes in PRIORITY frame')

    def test_rst_stream(self):
        frame_rst = FrameFactory(codecs.decode(b'000004'  # length
                                               b'0300'  # type, flags
                                               b'deadbeef'  # stream id
                                               b'0000000c',  # error code
                                               'hex'))
        assert (frame_rst.length == 4)
        assert (frame_rst.type == HTTP2_FRAME_RST_STREAM)
        assert (frame_rst.flags == 0)
        assert (frame_rst.stream_id == 0xdeadbeef)
        assert (frame_rst.data == b'\x00\x00\x00\x0c')
        assert (frame_rst.error_code == HTTP2_INADEQUATE_SECURITY)

        import pytest
        # Invalid length
        with pytest.raises(HTTP2Exception) as e:
            RSTStreamFrame(codecs.decode(b'000005'  # length
                                         b'0300'  # type, flags
                                         b'deadbeef'  # stream id
                                         b'0000000c'  # error code
                                         b'00',  # unexpected additional payload
                                         'hex'))
        assert (str(e.value) == 'Invalid number of bytes in RST_STREAM frame (must be 4)')

    def test_settings(self):
        frame_settings = FrameFactory(codecs.decode(b'00000c'  # length
                                                    b'0400'  # type, flags
                                                    b'00000000'  # stream id
                                                    # settings
                                                    b'0004'  # setting id
                                                    b'00020000'  # setting value
                                                    b'0005'  # setting id
                                                    b'00004000',  # setting value
                                                    'hex'))
        assert (frame_settings.length == 12)
        assert (frame_settings.type == HTTP2_FRAME_SETTINGS)
        assert (frame_settings.flags == 0)
        assert (frame_settings.stream_id == 0)
        assert (len(frame_settings.settings) == 2)
        assert (frame_settings.settings[0].identifier == HTTP2_SETTINGS_INITIAL_WINDOW_SIZE)
        assert (frame_settings.settings[0].value == 0x20000)
        assert (frame_settings.settings[1].identifier == HTTP2_SETTINGS_MAX_FRAME_SIZE)
        assert (frame_settings.settings[1].value == 0x4000)

        # Settings ack, with empty payload
        frame_settings_ack = FrameFactory(codecs.decode(b'000000'  # length
                                                        b'0401'  # type, flags
                                                        b'00000000',  # stream id
                                                        'hex'))
        assert (frame_settings_ack.length == 0)
        assert (frame_settings_ack.type == HTTP2_FRAME_SETTINGS)
        assert (frame_settings_ack.flags == HTTP2_FLAG_ACK)
        assert (frame_settings_ack.stream_id == 0)
        assert (len(frame_settings_ack.settings) == 0)

        import pytest
        # Invalid length
        with pytest.raises(HTTP2Exception) as e:
            SettingsFrame(codecs.decode(b'000005'  # length
                                        b'0400'  # type, flags
                                        b'deadbeef'  # stream id
                                        b'1234567890',  # invalid length
                                        'hex'))
        assert (str(e.value) == 'Invalid number of bytes in SETTINGS frame (must be multiple of 6)')

    def test_push_promise(self):
        frame_pp = FrameFactory(codecs.decode(b'000007'  # length
                                              b'0500'  # type, flags
                                              b'deadbeef'  # stream id
                                              b'cafebabe'  # promised id
                                              b'123456',  # some block fragment
                                              'hex'))
        assert (frame_pp.length == 7)
        assert (frame_pp.type == HTTP2_FRAME_PUSH_PROMISE)
        assert (frame_pp.flags == 0)
        assert (frame_pp.stream_id == 0xdeadbeef)
        assert (frame_pp.promised_id == 0xcafebabe)
        assert (frame_pp.block_fragment == b'\x12\x34\x56')

        import pytest
        # Invalid length
        with pytest.raises(HTTP2Exception) as e:
            PushPromiseFrame(codecs.decode(b'000003'  # length
                                           b'0500'  # type, flags
                                           b'deadbeef'  # stream id
                                           b'cafeba',  # missing promised id
                                           'hex'))
        assert (str(e.value) == 'Missing promised stream ID in PUSH_PROMISE frame')

    def test_ping(self):
        frame_ping = FrameFactory(codecs.decode(b'000008'  # length
                                                b'0600'  # type, flags
                                                b'deadbeef'  # stream id
                                                b'cafebabe12345678',  # user data
                                                'hex'))
        assert (frame_ping.length == 8)
        assert (frame_ping.type == HTTP2_FRAME_PING)
        assert (frame_ping.flags == 0)
        assert (frame_ping.stream_id == 0xdeadbeef)
        assert (frame_ping.data == b'\xCA\xFE\xBA\xBE\x12\x34\x56\x78')

        import pytest
        # Invalid length
        with pytest.raises(HTTP2Exception) as e:
            PingFrame(codecs.decode(b'000005'  # length
                                    b'0600'  # type, flags
                                    b'deadbeef'  # stream id
                                    b'1234567890',  # invalid length
                                    'hex'))
        assert (str(e.value) == 'Invalid number of bytes in PING frame (must be 8)')

    def test_goaway(self):
        frame_goaway = FrameFactory(codecs.decode(b'00000a'  # length
                                                  b'0700'  # type, flags
                                                  b'deadbeef'  # stream id
                                                  b'00000000'  # last stream id
                                                  b'00000000'  # error code
                                                  b'cafe',  # debug data
                                                  'hex'))
        assert (frame_goaway.length == 10)
        assert (frame_goaway.type == HTTP2_FRAME_GOAWAY)
        assert (frame_goaway.flags == 0)
        assert (frame_goaway.stream_id == 0xdeadbeef)
        assert (frame_goaway.last_stream_id == 0)
        assert (frame_goaway.error_code == HTTP2_NO_ERROR)
        assert (frame_goaway.debug_data == b'\xCA\xFE')

        import pytest
        # Invalid length
        with pytest.raises(HTTP2Exception) as e:
            GoAwayFrame(codecs.decode(b'000005'  # length
                                      b'0700'  # type, flags
                                      b'deadbeef'  # stream id
                                      b'1234567890',  # invalid length
                                      'hex'))
        assert (str(e.value) == 'Invalid number of bytes in GO_AWAY frame')

    def test_window_update(self):
        frame_wu = FrameFactory(codecs.decode(b'000004'  # length
                                              b'0800'  # type, flags
                                              b'deadbeef'  # stream id
                                              b'12345678',  # window increment
                                              'hex'))
        assert (frame_wu.length == 4)
        assert (frame_wu.type == HTTP2_FRAME_WINDOW_UPDATE)
        assert (frame_wu.flags == 0)
        assert (frame_wu.stream_id == 0xdeadbeef)
        assert (frame_wu.window_increment == 0x12345678)

        import pytest
        # Invalid length
        with pytest.raises(HTTP2Exception) as e:
            WindowUpdateFrame(codecs.decode(b'000005'  # length
                                            b'0800'  # type, flags
                                            b'deadbeef'  # stream id
                                            b'1234567890',  # invalid length
                                            'hex'))
        assert (str(e.value) == 'Invalid number of bytes in WINDOW_UPDATE frame (must be 4)')

    def test_continuation(self):
        frame_cont = FrameFactory(codecs.decode(b'000003'  # length
                                                b'0900'  # type, flags
                                                b'deadbeef'  # stream id
                                                b'f00baa',  # block fragment
                                                'hex'))
        assert (frame_cont.length == 3)
        assert (frame_cont.type == HTTP2_FRAME_CONTINUATION)
        assert (frame_cont.flags == 0)
        assert (frame_cont.stream_id == 0xdeadbeef)
        assert (frame_cont.block_fragment == b'\xF0\x0B\xAA')

    def test_factory(self):
        import pytest
        # Too short
        pytest.raises(dpkt.NeedData, FrameFactory, codecs.decode(b'000000', 'hex'))

        # Invalid type
        with pytest.raises(HTTP2Exception) as e:
            FrameFactory(codecs.decode(b'000000'  # length
                                       b'abcd'  # type, flags
                                       b'deadbeef',  # stream id
                                       'hex'))
        assert (str(e.value) == 'Invalid frame type: 0xab')

    def test_preface(self):
        import pytest
        # Preface
        pytest.raises(dpkt.NeedData, Preface,
                      codecs.decode(b'505249202a20485454502f322e300d0a', 'hex'))
        pytest.raises(dpkt.NeedData, Preface, b'\x00' * 23)
        with pytest.raises(HTTP2Exception) as e:
            Preface(b'\x00' * 24)
        assert (str(e.value) == 'Invalid HTTP/2 preface')

    def test_multi(self):
        assert (self.i == 128)
        assert (len(self.frames) == 7)

        assert (self.frames[0].length == 12)
        assert (self.frames[1].length == 4)
        assert (self.frames[2].length == 5)
        assert (self.frames[3].length == 5)
        assert (self.frames[4].length == 5)
        assert (self.frames[5].length == 5)
        assert (self.frames[6].length == 5)

        assert (self.frames[0].type == HTTP2_FRAME_SETTINGS)
        assert (self.frames[1].type == HTTP2_FRAME_WINDOW_UPDATE)
        assert (self.frames[2].type == HTTP2_FRAME_PRIORITY)
        assert (self.frames[3].type == HTTP2_FRAME_PRIORITY)
        assert (self.frames[4].type == HTTP2_FRAME_PRIORITY)
        assert (self.frames[5].type == HTTP2_FRAME_PRIORITY)
        assert (self.frames[6].type == HTTP2_FRAME_PRIORITY)

        assert (self.frames[0].flags == 0)
        assert (self.frames[1].flags == 0)
        assert (self.frames[2].flags == 0)
        assert (self.frames[3].flags == 0)
        assert (self.frames[4].flags == 0)
        assert (self.frames[5].flags == 0)
        assert (self.frames[6].flags == 0)

        assert (self.frames[0].stream_id == 0)
        assert (self.frames[1].stream_id == 0)
        assert (self.frames[2].stream_id == 3)
        assert (self.frames[3].stream_id == 5)
        assert (self.frames[4].stream_id == 7)
        assert (self.frames[5].stream_id == 9)
        assert (self.frames[6].stream_id == 11)

        frames, i = frame_multi_factory(
            codecs.decode(b'505249202a20485454502f322e300d0a', 'hex'),
            preface=True)
        assert (len(frames) == 0)
        assert (i == 0)

        # Only preface was parsed
        frames, i = frame_multi_factory(
            codecs.decode(b'505249202a20485454502f322e300d0a'
                          b'0d0a534d0d0a0d0a00000c0400000000', 'hex'),
            preface=True)
        assert (len(frames) == 0)
        assert (i == 24)

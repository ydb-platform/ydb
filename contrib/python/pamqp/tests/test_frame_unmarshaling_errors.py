# coding=utf-8
import struct
import unittest

from pamqp import constants, exceptions, frame


class TestCase(unittest.TestCase):

    def test_invalid_protocol_header(self):
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(b'AMQP\x00\x00\t')
            self.assertTrue(str(err).startswith(
                "Could not unmarshal <class 'pamqp.header.ProtocolHeader'> "
                'frame: Data did not match the ProtocolHeader format'))

    def test_invalid_frame_header(self):
        frame_data = struct.pack('>BI', 255, 0)
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(frame_data)
            self.assertEqual(
                str(err), 'Could not unmarshal Unknown frame: No frame size')

    def test_frame_with_no_length(self):
        frame_data = (b'\x01\x00\x01\x00\x00\x00\x00\x00<\x00P\x00\x00\x00\x00'
                      b'\x00\x00\x00\x01\x00\xce')
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(frame_data)
            self.assertEqual(
                str(err), 'Could not unmarshal Unknown frame: No frame size')

    def test_frame_malformed_length(self):
        frame_data = (b'\x01\x00\x01\x00\x00\x00\x0c\x00<\x00P\x00\x00\x00\x00'
                      b'\x00\x00\x00\xce')
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(frame_data)
            self.assertEqual(
                str(err),
                'Could not unmarshal Unknown frame: Not all data received')

    def test_frame_malformed_end_byte(self):
        frame_data = (b'\x01\x00\x01\x00\x00\x00\r\x00<\x00P\x00\x00\x00\x00'
                      b'\x00\x00\x00\x01\x00\x00')
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(frame_data)
            self.assertEqual(
                str(err),
                'Could not unmarshal Unknown frame: Last byte error')

    def test_malformed_frame_content(self):
        payload = struct.pack('>HxxQ', 8192, 32768)
        frame_value = b''.join([struct.pack('>BHI', 5, 0, len(payload)),
                                payload, constants.FRAME_END_CHAR])
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(frame_value)
            self.assertEqual(
                str(err),
                'Could not unmarshal Unknown frame: Unknown frame type: 5')

    def test_invalid_method_frame_index(self):
        payload = struct.pack('>L', 42949)
        frame_value = b''.join([struct.pack('>BHI', 1, 0, len(payload)),
                                payload, constants.FRAME_END_CHAR])
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(frame_value)
            self.assertEqual(
                str(err),
                ('Could not unmarshal Unknown frame: '
                 'Unknown method index: 42949'))

    def test_invalid_method_frame_content(self):
        payload = struct.pack('>L', 0x000A0029)
        frame_value = b''.join([struct.pack('>BHI', 1, 0, len(payload)),
                                payload, constants.FRAME_END_CHAR])
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(frame_value)
            self.assertTrue(str(err).startswith(
                'Could not unmarshal <pamqp.specification.Connection.OpenOk'))

    def test_invalid_content_header_frame(self):
        payload = struct.pack('>L', 0x000A0029)
        frame_value = b''.join([struct.pack('>BHI', 2, 0, len(payload)),
                                payload, constants.FRAME_END_CHAR])
        with self.assertRaises(exceptions.UnmarshalingException) as err:
            frame.unmarshal(frame_value)
            self.assertTrue(str(err).startswith(
                'Could not unmarshal ContentHeader frame:'))

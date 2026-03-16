# -*- encoding: utf-8 -*-
import datetime
import unittest
import uuid

from pamqp import body, commands, frame, header, heartbeat


class MarshalingTests(unittest.TestCase):
    def test_protocol_header(self):
        expectation = b'AMQP\x00\x00\t\x01'
        response = frame.marshal(header.ProtocolHeader(), 0)
        self.assertEqual(response, expectation,
                         'ProtocolHeader did not match expectation')

    def test_basic_ack(self):
        expectation = (b'\x01\x00\x01\x00\x00\x00\r\x00<\x00P\x00\x00\x00'
                       b'\x00\x00\x00\x00\x01\x00\xce')
        frame_obj = commands.Basic.Ack(1, False)
        response = frame.marshal(frame_obj, 1)
        self.assertEqual(response, expectation,
                         'Basic.Ack did not match expectation')

    def test_basic_cancel(self):
        expectation = (b'\x01\x00\x01\x00\x00\x00\r\x00<\x00\x1e\x07'
                       b'ctag1.0\x00\xce')
        frame_obj = commands.Basic.Cancel('ctag1.0', False)
        response = frame.marshal(frame_obj, 1)
        self.assertEqual(response, expectation,
                         'Basic.Cancel did not match expectation')

    def test_basic_cancelok(self):
        expectation = (b'\x01\x00\x01\x00\x00\x00\x0c\x00<\x00\x1f\x07'
                       b'ctag1.0\xce')
        frame_obj = commands.Basic.CancelOk(consumer_tag='ctag1.0')
        response = frame.marshal(frame_obj, 1)
        self.assertEqual(response, expectation,
                         'Basic.Cancel did not match expectation')

    def test_basic_consume(self):
        expectation = (b'\x01\x00\x01\x00\x00\x00\x15\x00<\x00\x14\x00'
                       b'\x00\x03bar\x05ctag0\x00\x00\x00\x00\x00\xce')
        frame_obj = commands.Basic.Consume(0, 'bar', 'ctag0', False,
                                           False, False, False)
        response = frame.marshal(frame_obj, 1)
        self.assertEqual(response, expectation,
                         'Basic.Consume did not match expectation')

    def test_heartbeat(self):
        expectation = b'\x08\x00\x00\x00\x00\x00\x00\xce'
        response = frame.marshal(heartbeat.Heartbeat(), 0)
        self.assertEqual(response, expectation,
                         'Heartbeat did not match expectation')

    def test_content_body(self):
        value = str(uuid.uuid4()).encode('utf-8')
        expectation = b'\x03\x00\x01\x00\x00\x00$' + value + b'\xce'
        self.assertEqual(frame.marshal(body.ContentBody(value), 1),
                         expectation)

    def test_content_header(self):
        expectation = (b'\x02\x00\x01\x00\x00\x00\x0e\x00<\x00\x00\x00\x00'
                       b'\x00\x00\x00\x00\x00\n\x00\x00\xce')
        self.assertEqual(frame.marshal(header.ContentHeader(body_size=10), 1),
                         expectation)

    def test_content_header_with_basic_properties(self):
        props = commands.Basic.Properties(
            app_id='unittest',
            content_type='application/json',
            content_encoding='bzip2',
            correlation_id='d146482a-42dd-4b8b-a620-63d62ef686f3',
            delivery_mode=2,
            expiration='100',
            headers={'foo': 'Test ✈'},
            message_id='4b5baed7-66e3-49da-bfe4-20a9651e0db4',
            message_type='foo',
            priority=10,
            reply_to='q1',
            timestamp=datetime.datetime(2019, 12, 19, 23, 29, 00,
                                        tzinfo=datetime.timezone.utc))
        expectation = (b'\x02\x00\x01\x00\x00\x00\xa2\x00<\x00\x00\x00\x00\x00'
                       b'\x00\x00\x00\x00\n\xff\xe8\x10application/json\x05'
                       b'bzip2\x00\x00\x00\x11\x03fooS\x00\x00\x00\x08Test '
                       b'\xe2\x9c\x88\x02\n$d146482a-42dd-4b8b-a620-63d62ef68'
                       b'6f3\x02q1\x03100$4b5baed7-66e3-49da-bfe4-20a9651e0db4'
                       b'\x00\x00\x00\x00]\xfc\x07\xbc\x03foo\x08unittest\xce')
        self.assertEqual(frame.marshal(header.ContentHeader(0, 10, props), 1),
                         expectation)

    def test_unknown_frame_type(self):
        with self.assertRaises(ValueError):
            frame.marshal(self, 1)

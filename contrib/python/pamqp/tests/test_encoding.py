# -*- encoding: utf-8 -*-
import datetime
import decimal
import unittest

from pamqp import encode


class MarshalingTests(unittest.TestCase):
    def test_encode_bool_wrong_type(self):
        self.assertRaises(TypeError, encode.boolean, 'hi')

    def test_encode_bool_false(self):
        self.assertEqual(encode.boolean(False), b'\x00')

    def test_encode_bool_true(self):
        self.assertEqual(encode.boolean(True), b'\x01')

    def test_encode_byte_array(self):
        self.assertEqual(encode.byte_array(bytearray([65, 66, 67])),
                         b'\x00\x00\x00\x03ABC')

    def test_encode_byte_array_wrong_type(self):
        self.assertRaises(TypeError, encode.byte_array, b'ABC')

    def test_encode_decimal_wrong_type(self):
        self.assertRaises(TypeError, encode.decimal, 3.141597)

    def test_encode_decimal(self):
        self.assertEqual(encode.decimal(decimal.Decimal('3.14159')),
                         b'\x05\x00\x04\xcb/')

    def test_encode_decimal_whole(self):
        self.assertEqual(encode.decimal(decimal.Decimal('314159')),
                         b'\x00\x00\x04\xcb/')

    def test_encode_double_invalid_value(self):
        self.assertRaises(TypeError, encode.double, '1234')

    def test_encode_double(self):
        self.assertEqual(encode.double(float(3.14159)),
                         b'@\t!\xf9\xf0\x1b\x86n')

    def test_encode_floating_point_type(self):
        self.assertRaises(TypeError, encode.floating_point, '1234')

    def test_encode_float(self):
        self.assertEqual(encode.floating_point(float(3.14159)), b'@I\x0f\xd0')

    def test_encode_long_int_wrong_type(self):
        self.assertRaises(TypeError, encode.long_int, 3.141597)

    def test_encode_table_integer_bad_value_error(self):
        self.assertRaises(TypeError, encode.long_int, 9223372036854775808)

    def test_encode_long_int(self):
        self.assertEqual(encode.long_int(2147483647),
                         b'\x7f\xff\xff\xff')

    def test_encode_long_int_error(self):
        self.assertRaises(TypeError, encode.long_int, 21474836449)

    def test_encode_long_uint(self):
        self.assertEqual(encode.long_uint(4294967295),
                         b'\xff\xff\xff\xff')

    def test_encode_long_uint_error(self):
        self.assertRaises(TypeError, encode.long_uint, 4294967296)

    def test_encode_long_uint_wrong_type(self):
        self.assertRaises(TypeError, encode.long_uint, 3.141597)

    def test_encode_long_long_int_wrong_type(self):
        self.assertRaises(TypeError, encode.long_long_int, 3.141597)

    def test_encode_long_long_int_error(self):
        self.assertRaises(TypeError, encode.long_long_int,
                          9223372036854775808)

    def test_encode_octet(self):
        self.assertEqual(encode.octet(1), b'\x01')

    def test_encode_octet_error(self):
        self.assertRaises(TypeError, encode.octet, 'hi')

    def test_encode_short_wrong_type(self):
        self.assertRaises(TypeError, encode.short_int, 3.141597)

    def test_encode_short(self):
        self.assertEqual(encode.short_int(32767), b'\x7f\xff')

    def test_encode_short_error(self):
        self.assertRaises(TypeError, encode.short_int, 32768)

    def test_encode_short_uint(self):
        self.assertEqual(encode.short_uint(65535), b'\xff\xff')

    def test_encode_short_uint_error(self):
        self.assertRaises(TypeError, encode.short_uint, 65536)

    def test_encode_short_uint_type_error(self):
        self.assertRaises(TypeError, encode.short_uint, 'hello')

    def test_encode_table_integer_error(self):
        self.assertRaises(TypeError, encode.table_integer, 9223372036854775808)

    def test_encode_short_string(self):
        self.assertEqual(encode.short_string('Hello'), b'\x05Hello')

    def test_encode_short_string_error(self):
        self.assertRaises(TypeError, encode.short_string, 32768)

    def test_encode_short_string_utf8_python3(self):
        self.assertEqual(encode.short_string('🐰'), b'\x04\xf0\x9f\x90\xb0')

    def test_encode_long_string(self):
        self.assertEqual(encode.long_string('0123456789'),
                         b'\x00\x00\x00\n0123456789')

    def test_encode_long_string_bytes(self):
        self.assertEqual(encode.long_string('rabbitmq'),
                         b'\x00\x00\x00\x08rabbitmq')

    def test_encode_long_string_utf8_python3(self):
        self.assertEqual(encode.long_string('🐰'),
                         b'\x00\x00\x00\x04\xf0\x9f\x90\xb0')

    def test_encode_long_string_error(self):
        self.assertRaises(TypeError, encode.long_string, 100)

    def test_encode_timestamp_from_datetime(self):
        self.assertEqual(
            encode.timestamp(datetime.datetime(
                2006, 11, 21, 16, 30, 10, tzinfo=datetime.timezone.utc)),
            b'\x00\x00\x00\x00Ec)\x92')

    def test_encode_timestamp_from_struct_time(self):
        value = encode.timestamp(
            datetime.datetime(
                2006, 11, 21, 16, 30, 10,
                tzinfo=datetime.timezone.utc).timetuple())
        self.assertEqual(value, b'\x00\x00\x00\x00Ec)\x92')

    def test_encode_timestamp_error(self):
        self.assertRaises(TypeError, encode.timestamp, 'hi')

    def test_encode_field_array(self):
        expectation = (b'\x00\x00\x00:b\x01u\xaf\xc8I\x02bZ\x00S\x00\x00\x00'
                       b'\x04TestT\x00\x00\x00\x00Ec)\x92I\xbb\x9a\xca\x00D'
                       b'\x02\x00\x00\x01:f@H\xf5\xc3i\xc4e5\xffl\x80\x00\x00'
                       b'\x00\x00\x00\x00\x08')
        data = [
            1, 45000, 40000000, 'Test',
            datetime.datetime(
                2006, 11, 21, 16, 30, 10, tzinfo=datetime.timezone.utc),
            -1147483648,
            decimal.Decimal('3.14'), 3.14,
            3294967295, -9223372036854775800
        ]
        self.assertEqual(encode.field_array(data), expectation)

    def test_encode_field_array_error(self):
        self.assertRaises(TypeError, encode.field_array, 'hi')

    def test_encode_field_table_empty(self):
        self.assertEqual(encode.field_table(None), b'\x00\x00\x00\x00')

    def test_encode_field_table_type_error(self):
        self.assertRaises(TypeError, encode.field_table, [1, 2, 3])

    def test_encode_field_table_object(self):
        self.assertRaises(TypeError, encode.field_table,
                          {'key': encode.field_table})

    def test_encode_field_table(self):
        expectation = (b"\x00\x00\x04'\x08arrayvalA\x00\x00\x00\x06b\x01b"
                       b'\x02b\x03\x07boolvalt\x01\tbytearrayx\x00\x00\x00'
                       b'\x03AAA\x06decvalD\x02\x00\x00\x01:\x07dictvalF\x00'
                       b'\x00\x00\x0c\x03fooS\x00\x00\x00\x03bar\x08floatval'
                       b'f@H\xf5\xc3\x06intvalb\x01\x07longstrS\x00\x00\x03t'
                       b'000000000000000000000000000000000000000000000000000'
                       b'011111111111111111111111111111111111111111111111111'
                       b'112222222222222222222222222222222222222222222222222'
                       b'222111111111111111111111111111111111111111111111111'
                       b'111122222222222222222222222222222222222222222222222'
                       b'222221111111111111111111111111111111111111111111111'
                       b'111111222222222222222222222222222222222222222222222'
                       b'222222211111111111111111111111111111111111111111111'
                       b'111111112222222222222222222222222222222222222222222'
                       b'222222222111111111111111111111111111111111111111111'
                       b'111111111122222222222222222222222222222222222222222'
                       b'222222222221111111111111111111111111111111111111111'
                       b'111111111111222222222222222222222222222222222222222'
                       b'222222222222211111111111111111111111111111111111111'
                       b'111111111111112222222222222222222222222222222222222'
                       b'222222222222222111111111111111111111111111111111111'
                       b'111111111111111100000000000000000000000000000000000'
                       b'00000000000000000\x07longvalI6e&U\x04noneV\x06strva'
                       b'lS\x00\x00\x00\x04Test\x0ctimestampvalT\x00\x00\x00'
                       b'\x00Ec)\x92')
        data = {
            'intval': 1,
            'strval': 'Test',
            'boolval': True,
            'timestampval': datetime.datetime(
                2006, 11, 21, 16, 30, 10, tzinfo=datetime.timezone.utc),
            'decval': decimal.Decimal('3.14'),
            'floatval': 3.14,
            'longval': 912598613,
            'dictval': {
                'foo': 'bar'
            },
            'arrayval': [1, 2, 3],
            'none': None,
            'bytearray': bytearray((65, 65, 65)),
            'longstr': ('0000000000000000000000000000000000000000000000000'
                        '0001111111111111111111111111111111111111111111111'
                        '1111112222222222222222222222222222222222222222222'
                        '2222222221111111111111111111111111111111111111111'
                        '1111111111112222222222222222222222222222222222222'
                        '2222222222222221111111111111111111111111111111111'
                        '1111111111111111112222222222222222222222222222222'
                        '2222222222222222222221111111111111111111111111111'
                        '1111111111111111111111112222222222222222222222222'
                        '2222222222222222222222222221111111111111111111111'
                        '1111111111111111111111111111112222222222222222222'
                        '2222222222222222222222222222222221111111111111111'
                        '1111111111111111111111111111111111112222222222222'
                        '2222222222222222222222222222222222222221111111111'
                        '1111111111111111111111111111111111111111112222222'
                        '2222222222222222222222222222222222222222222221111'
                        '1111111111111111111111111111111111111111111111110'
                        '0000000000000000000000000000000000000000000000000'
                        '00')
        }
        self.assertEqual(encode.field_table(data), expectation)

    def test_encode_by_type_field_array(self):
        expectation = (b'\x00\x00\x008b\x01sBhu\xaf\xc8S\x00\x00\x00\x04TestT'
                       b'\x00\x00\x00\x00Ec)\x92I\xbb\x9a\xca\x00D\x02\x00'
                       b'\x00\x01:f@H\xf5\xc3i\xc4e5\xffl\x80\x00\x00\x00\x00'
                       b'\x00\x00\x08')
        data = [
            1, 17000, 45000, 'Test',
            datetime.datetime(
                2006, 11, 21, 16, 30, 10, tzinfo=datetime.timezone.utc),
            -1147483648,
            decimal.Decimal('3.14'), 3.14,
            3294967295, -9223372036854775800
        ]
        self.assertEqual(encode.by_type(data, 'field_array'), expectation)

    def test_encode_by_type_byte_array(self):
        self.assertEqual(encode.by_type(bytearray((65, 66, 67)), 'bytearray'),
                         b'\x00\x00\x00\x03ABC')

    def test_encode_by_type_double(self):
        self.assertEqual(encode.by_type(float(4294967295), 'double'),
                         b'A\xef\xff\xff\xff\xe0\x00\x00')

    def test_encode_by_type_long_uint(self):
        self.assertEqual(encode.by_type(4294967295, 'long'),
                         b'\xff\xff\xff\xff')

    def test_encode_by_type_long_long_int(self):
        self.assertEqual(encode.by_type(9223372036854775800, 'longlong'),
                         b'\x7f\xff\xff\xff\xff\xff\xff\xf8')

    def test_encode_by_type_long_str(self):
        self.assertEqual(encode.by_type('0123456789', 'longstr'),
                         b'\x00\x00\x00\n0123456789')

    def test_encode_by_type_none(self):
        self.assertEqual(encode.by_type(None, 'void'), None)

    def test_encode_by_type_octet(self):
        self.assertEqual(encode.by_type(1, 'octet'), b'\x01')

    def test_encode_by_type_short(self):
        self.assertEqual(encode.by_type(32767, 'short'), b'\x7f\xff')

    def test_encode_by_type_timestamp(self):
        self.assertEqual(
            encode.by_type(
                datetime.datetime(2006, 11, 21, 16, 30, 10,
                                  tzinfo=datetime.timezone.utc), 'timestamp'),
            b'\x00\x00\x00\x00Ec)\x92')

    def test_encode_by_type_field_table(self):
        expectation = (b'\x00\x00\x04B\x08arrayvalA\x00\x00\x00\x08b\x01s\x10'
                       b'`u\xa4\x10\x07boolvalt\x01\x06decvalD\x02\x00\x00\x01'
                       b':\x07dictvalF\x00\x00\x00\x0c\x03fooS\x00\x00\x00\x03'
                       b'bar\x08floatvalf@H\xf5\xc3\x07longstrS\x00\x00\x03t00'
                       b'00000000000000000000000000000000000000000000000000111'
                       b'11111111111111111111111111111111111111111111111112222'
                       b'22222222222222222222222222222222222222222222222211111'
                       b'11111111111111111111111111111111111111111111111222222'
                       b'22222222222222222222222222222222222222222222221111111'
                       b'11111111111111111111111111111111111111111111122222222'
                       b'22222222222222222222222222222222222222222222111111111'
                       b'11111111111111111111111111111111111111111112222222222'
                       b'22222222222222222222222222222222222222222211111111111'
                       b'11111111111111111111111111111111111111111222222222222'
                       b'22222222222222222222222222222222222222221111111111111'
                       b'11111111111111111111111111111111111111122222222222222'
                       b'22222222222222222222222222222222222222111111111111111'
                       b'11111111111111111111111111111111111112222222222222222'
                       b'22222222222222222222222222222222222211111111111111111'
                       b'11111111111111111111111111111111111000000000000000000'
                       b'0000000000000000000000000000000000\x07longvalI6e&U'
                       b'\x06s32intI\xc4e6\x00\x06s64intl7\x82\xda\xce\x9d\x90'
                       b'\x00\x00\x06strvalS\x00\x00\x00\x04Test\x0ctimestamp'
                       b'valT\x00\x00\x00\x00Ec)\x92\x06u16ints \x00\x06u32int'
                       b'i\xeek(\x00\x05u8bitb ')
        data = {
            'u8bit': 32,
            'u16int': 8192,
            's32int': -1000000000,
            'u32int': 4000000000,
            's64int': 4000000000000000000,
            'strval': 'Test',
            'boolval': True,
            'timestampval': datetime.datetime(
                2006, 11, 21, 16, 30, 10, tzinfo=datetime.timezone.utc),
            'decval': decimal.Decimal('3.14'),
            'floatval': 3.14,
            'longval': 912598613,
            'dictval': {
                'foo': 'bar'
            },
            'arrayval': [1, 4192, 42000],
            'longstr': ('0000000000000000000000000000000000000000000000000'
                        '0001111111111111111111111111111111111111111111111'
                        '1111112222222222222222222222222222222222222222222'
                        '2222222221111111111111111111111111111111111111111'
                        '1111111111112222222222222222222222222222222222222'
                        '2222222222222221111111111111111111111111111111111'
                        '1111111111111111112222222222222222222222222222222'
                        '2222222222222222222221111111111111111111111111111'
                        '1111111111111111111111112222222222222222222222222'
                        '2222222222222222222222222221111111111111111111111'
                        '1111111111111111111111111111112222222222222222222'
                        '2222222222222222222222222222222221111111111111111'
                        '1111111111111111111111111111111111112222222222222'
                        '2222222222222222222222222222222222222221111111111'
                        '1111111111111111111111111111111111111111112222222'
                        '2222222222222222222222222222222222222222222221111'
                        '1111111111111111111111111111111111111111111111110'
                        '0000000000000000000000000000000000000000000000000'
                        '00')
        }
        self.assertEqual(encode.by_type(data, 'table'), expectation)

    def test_encode_by_type_error(self):
        self.assertRaises(TypeError, encode.by_type, 12345.12434, 'foo')


class EncodeTableIntegerTestCase(unittest.TestCase):

    def setUp(self):
        encode.support_deprecated_rabbitmq(False)

    def tearDown(self):
        encode.support_deprecated_rabbitmq(False)

    def test_table_integer(self):
        tests = {
            'short-short': (32, b'b '),
            'short': (1024, b's\x04\x00'),
            'short-negative': (-1024, b's\xfc\x00'),
            'short-unsigned': (32768, b'u\x80\x00'),
            'long': (65536, b'I\x00\x01\x00\x00'),
            'long-negative': (65536, b'I\x00\x01\x00\x00'),
            'long-unsigned': (4294967295, b'i\xff\xff\xff\xff'),
            'long-long': (9223372036854775805,
                          b'l\x7f\xff\xff\xff\xff\xff\xff\xfd'),
        }
        for key, value in tests.items():
            result = encode.table_integer(value[0])
            self.assertEqual(result, value[1],
                             'encode {} mismatch ({!r} != {!r})'.format(
                                 key, result, value[1]))

    def test_deprecated_table_integer(self):
        self.assertFalse(encode.DEPRECATED_RABBITMQ_SUPPORT)
        encode.support_deprecated_rabbitmq(True)
        self.assertTrue(encode.DEPRECATED_RABBITMQ_SUPPORT)
        tests = {
            'short-short': (32, b'b '),
            'short': (1024, b's\x04\x00'),
            'short-negative': (-1024, b's\xfc\x00'),
            'long': (65536, b'I\x00\x01\x00\x00'),
            'long-negative': (65536, b'I\x00\x01\x00\x00'),
            'long-long': (2147483648, b'l\x00\x00\x00\x00\x80\x00\x00\x00'),
        }
        for key, value in tests.items():
            result = encode.table_integer(value[0])
            self.assertEqual(
                result, value[1],
                'encode {} mismatch of {!r} ({!r} != {!r})'.format(
                    key, value[0], result, value[1]))

    def test_too_large_int(self):
        self.assertFalse(encode.DEPRECATED_RABBITMQ_SUPPORT)
        with self.assertRaises(TypeError):
            encode.table_integer(9223372036854775809)
        encode.support_deprecated_rabbitmq(True)
        self.assertTrue(encode.DEPRECATED_RABBITMQ_SUPPORT)
        with self.assertRaises(TypeError):
            encode.table_integer(9223372036854775809)

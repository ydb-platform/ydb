import unittest
from infi.clickhouse_orm.fields import *
from datetime import date, datetime
import pytz


class SimpleFieldsTest(unittest.TestCase):

    epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
    # Valid values
    dates = [
        date(1970, 1, 1), datetime(1970, 1, 1), epoch,
        epoch.astimezone(pytz.timezone('US/Eastern')), epoch.astimezone(pytz.timezone('Asia/Jerusalem')),
        '1970-01-01 00:00:00', '1970-01-17 00:00:17', '0000-00-00 00:00:00', 0,
        '2017-07-26T08:31:05', '2017-07-26T08:31:05Z', '2017-07-26 08:31',
        '2017-07-26T13:31:05+05', '2017-07-26 13:31:05+0500'
    ]

    def test_datetime_field(self):
        f = DateTimeField()
        for value in self.dates:
            dt = f.to_python(value, pytz.utc)
            self.assertTrue(dt.tzinfo)
            # Verify that conversion to and from db string does not change value
            dt2 = f.to_python(f.to_db_string(dt, quote=False), pytz.utc)
            self.assertEqual(dt, dt2)
        # Invalid values
        for value in ('nope', '21/7/1999', 0.5,
                      '2017-01 15:06:00', '2017-01-01X15:06:00', '2017-13-01T15:06:00'):
            with self.assertRaises(ValueError):
                f.to_python(value, pytz.utc)

    def test_datetime64_field(self):
        f = DateTime64Field()
        # Valid values
        for value in self.dates + [
            datetime(1970, 1, 1, microsecond=100000),
            pytz.timezone('US/Eastern').localize(datetime(1970, 1, 1, microsecond=100000)),
            '1970-01-01 00:00:00.1', '1970-01-17 00:00:17.1', '0000-00-00 00:00:00.1', 0.1,
            '2017-07-26T08:31:05.1', '2017-07-26T08:31:05.1Z', '2017-07-26 08:31.1',
            '2017-07-26T13:31:05.1+05', '2017-07-26 13:31:05.1+0500'
        ]:
            dt = f.to_python(value, pytz.utc)
            self.assertTrue(dt.tzinfo)
            # Verify that conversion to and from db string does not change value
            dt2 = f.to_python(f.to_db_string(dt, quote=False), pytz.utc)
            self.assertEqual(dt, dt2)
        # Invalid values
        for value in ('nope', '21/7/1999',
                      '2017-01 15:06:00', '2017-01-01X15:06:00', '2017-13-01T15:06:00'):
            with self.assertRaises(ValueError):
                f.to_python(value, pytz.utc)

    def test_datetime64_field_precision(self):
        for precision in range(1, 7):
            f = DateTime64Field(precision=precision, timezone=pytz.utc)
            dt = f.to_python(datetime(2000, 1, 1, microsecond=123456), pytz.utc)
            dt2 = f.to_python(f.to_db_string(dt, quote=False), pytz.utc)
            m = round(123456, precision - 6) # round rightmost microsecond digits according to precision
            self.assertEqual(dt2, dt.replace(microsecond=m))

    def test_date_field(self):
        f = DateField()
        epoch = date(1970, 1, 1)
        # Valid values
        for value in (datetime(1970, 1, 1), epoch, '1970-01-01', '0000-00-00', 0):
            d = f.to_python(value, pytz.utc)
            self.assertEqual(d, epoch)
            # Verify that conversion to and from db string does not change value
            d2 = f.to_python(f.to_db_string(d, quote=False), pytz.utc)
            self.assertEqual(d, d2)
        # Invalid values
        for value in ('nope', '21/7/1999', 0.5):
            with self.assertRaises(ValueError):
                f.to_python(value, pytz.utc)
        # Range check
        for value in (date(1900, 1, 1), date(2900, 1, 1)):
            with self.assertRaises(ValueError):
                f.validate(value)

    def test_date_field_timezone(self):
        # Verify that conversion of timezone-aware datetime is correct
        f = DateField()
        dt = datetime(2017, 10, 5, tzinfo=pytz.timezone('Asia/Jerusalem'))
        self.assertEqual(f.to_python(dt, pytz.utc), date(2017, 10, 4))

    def test_datetime_field_timezone(self):
        # Verify that conversion of timezone-aware datetime is correct
        f = DateTimeField()
        utc_value = datetime(2017, 7, 26, 8, 31, 5, tzinfo=pytz.UTC)
        for value  in (
                '2017-07-26T08:31:05',
                '2017-07-26T08:31:05Z',
                '2017-07-26T11:31:05+03',
                '2017-07-26 11:31:05+0300',
                '2017-07-26T03:31:05-0500',
        ):
            self.assertEqual(f.to_python(value, pytz.utc), utc_value)

    def test_uint8_field(self):
        f = UInt8Field()
        # Valid values
        for value in (17, '17', 17.0):
            self.assertEqual(f.to_python(value, pytz.utc), 17)
        # Invalid values
        for value in ('nope', date.today()):
            with self.assertRaises(ValueError):
                f.to_python(value, pytz.utc)
        # Range check
        for value in (-1, 1000):
            with self.assertRaises(ValueError):
                f.validate(value)

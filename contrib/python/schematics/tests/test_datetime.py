# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import sys

from dateutil.tz import gettz, tzutc
import pytest

from schematics.exceptions import ConversionError, ValidationError
from schematics.types import DateTimeType, UTCDateTimeType, TimestampType


UTC = DateTimeType.UTC
NYC = gettz('US/Eastern')
EPOCH = datetime(1970, 1, 1, 0, 0, tzinfo=UTC)


def test_parse_with_defaults():

    field = DateTimeType()

    dt = field.to_native('2015-11-08T12:34')
    assert dt == datetime(2015, 11, 8, 12, 34)

    dt = field.to_native('2015-11-08T12:34:00.1')
    assert dt == datetime(2015, 11, 8, 12, 34, 0, 100000)

    dt = field.to_native('2015-11-08T12:34:56,0369-0730')
    assert dt.utcoffset() == timedelta(hours=-7, minutes=-30)
    assert dt.replace(tzinfo=None) == datetime(2015, 11, 8, 12, 34, 56, 36900)

    assert dt == field.to_native(u'2015-11-08T12:34:56,0369−0730') # minus U+2212

    dt = field.to_native('2015-11-08 12:34:56.00200+02:00')
    assert dt.utcoffset() == timedelta(hours=2)
    assert dt.replace(tzinfo=None) == datetime(2015, 11, 8, 12, 34, 56, 2000)

    dt = field.to_native('2015-11-08 12:34:56.00Z')
    assert dt.utcoffset() == timedelta(0)
    assert dt.replace(tzinfo=None) == datetime(2015, 11, 8, 12, 34, 56, 0)


def test_parse_convert():

    field = DateTimeType(convert_tz=True)

    dt = field.to_native('2015-11-08T12:34')
    assert dt == datetime(2015, 11, 8, 12, 34)

    dt = field.to_native('2015-11-08T12:34:56.0369-0730')
    assert dt == datetime(2015, 11, 8, 20, 4, 56, 36900, tzinfo=UTC)

    dt = field.to_native('2015-11-08 12:34:56,00Z')
    assert dt == datetime(2015, 11, 8, 12, 34, 56, 0, tzinfo=UTC)


def test_parse_require_tz():

    field = DateTimeType(tzd='require')

    with pytest.raises(ConversionError):
        dt = field.to_native('2015-11-08 12:34:56.00')

    dt = field.to_native('2015-11-08 12:34:56.00Z')
    assert dt == datetime(2015, 11, 8, 12, 34, 56, tzinfo=UTC)


def test_parse_utc():

    field = DateTimeType(tzd='utc')

    dt = field.to_native('2015-11-08 12:34:56.00')
    assert dt == datetime(2015, 11, 8, 12, 34, 56, tzinfo=UTC)


def test_parse_convert_drop_tzinfo():

    field = DateTimeType(tzd='require', convert_tz=True, drop_tzinfo=True)

    dt = field.to_native('2015-11-08T12:34:56.0369-0730')
    assert dt == datetime(2015, 11, 8, 20, 4, 56, 36900)

    dt = field.to_native('2015-11-08 12:34:56.00Z')
    assert dt == datetime(2015, 11, 8, 12, 34, 56, 0)


def test_parse_reject_tzd():

    field = DateTimeType(tzd='reject')

    with pytest.raises(ConversionError):
        field.to_native('2015-11-08T12:34+0200')

    with pytest.raises(ConversionError):
        field.to_native('2015-11-08T12:34Z')

    dt = field.to_native('2015-11-08T12:34')
    assert dt == datetime(2015, 11, 8, 12, 34)


def test_parse_reject_invalid_type():

    field = DateTimeType()

    with pytest.raises(ConversionError):
        field.to_native(['2017-04-30'])


def test_parse_from_timestamp():

    field = DateTimeType()

    dt = field.to_native('1446991200.7777')
    assert dt == datetime(2015, 11, 8, 14, 00, microsecond=777700, tzinfo=UTC)

    field = DateTimeType(convert_tz=True, drop_tzinfo=True)

    dt = field.to_native('1446991200.7777')
    assert dt == datetime(2015, 11, 8, 14, 00, microsecond=777700)

    dt = field.to_native(1446991200.7777)
    assert dt == datetime(2015, 11, 8, 14, 00, microsecond=777700)


def test_parse_using_formats():

    formats = ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f%z')
    field = DateTimeType(formats=formats)

    dt = field.to_native('2015-11-08T12:34:56.99Z')
    assert dt == datetime(2015, 11, 8, 12, 34, 56, 990000)

    if sys.version_info[0] >= 3:
        # strptime() supports the '%z' directive starting with Python 3.2.
        dt = field.to_native('2015-11-08T12:34:56.099-0700')
        assert dt.utcoffset() == timedelta(hours=-7)
        assert dt.replace(tzinfo=None) == datetime(2015, 11, 8, 12, 34, 56, 99000)


def test_to_native_from_datetime():

    dt_naive_utc = datetime(2015, 6, 1, 14, 00)
    dt_utc = datetime(2015, 6, 1, 14, 00, tzinfo=UTC)
    dt_plustwo = datetime(2015, 6, 1, 16, 00, tzinfo=DateTimeType.offset_timezone(2))
    dt_nyc = datetime(2015, 6, 1, 10, 00, tzinfo=NYC)

    field = DateTimeType(tzd='allow')
    assert field.to_native(dt_naive_utc) == dt_naive_utc
    assert field.to_native(dt_utc) == dt_utc
    assert field.to_native(dt_plustwo) == dt_plustwo
    assert field.to_native(dt_nyc) == dt_nyc

    field = DateTimeType(tzd='utc')
    assert field.to_native(dt_naive_utc) == dt_utc

    field = DateTimeType(tzd='reject')
    assert field.to_native(dt_naive_utc) == dt_naive_utc
    with pytest.raises(ConversionError):
        field.to_native(dt_utc)

    field = DateTimeType(tzd='require')
    assert field.to_native(dt_utc) == dt_utc
    with pytest.raises(ConversionError):
        field.to_native(dt_naive_utc)

    field = DateTimeType(tzd='require', convert_tz=True, drop_tzinfo=True)
    assert field.to_native(dt_naive_utc) == dt_naive_utc

    field = DateTimeType(convert_tz=True)
    assert field.to_native(dt_naive_utc) == dt_naive_utc
    assert field.to_native(dt_utc) == dt_utc
    assert field.to_native(dt_plustwo) == dt_utc
    assert field.to_native(dt_nyc) == dt_utc

    field = DateTimeType(convert_tz=True, drop_tzinfo=True)
    assert field.to_native(dt_naive_utc) == dt_naive_utc
    assert field.to_native(dt_utc) == dt_naive_utc
    assert field.to_native(dt_plustwo) == dt_naive_utc
    assert field.to_native(dt_nyc) == dt_naive_utc


def test_to_primitive():

    dt = datetime(2015, 11, 8, 12, 34, 56, 36900, tzinfo=DateTimeType.offset_timezone(-7, -30))

    assert DateTimeType().to_primitive(dt) == '2015-11-08T12:34:56.036900-0730'
    assert DateTimeType(serialized_format='%Y-%m-%d %H:%M:%S').to_primitive(dt) \
        == '2015-11-08 12:34:56'


def test_utc_type():

    field = UTCDateTimeType()

    dt = field.to_native('2015-11-08T12:34:56.0369-0730')
    assert dt == datetime(2015, 11, 8, 20, 4, 56, 36900)

    dt = field.to_native('2015-11-08 12:34:56.00Z')
    assert dt == datetime(2015, 11, 8, 12, 34, 56, 0)

    dt = field.to_native('2015-11-08 12:34:56.00')
    assert dt == datetime(2015, 11, 8, 12, 34, 56, 0)

    UTCDateTimeType(default=datetime.utcnow)


def test_timestamp():

    field = TimestampType()
    field_no_tz = TimestampType(drop_tzinfo=True)

    ts = field.to_primitive(datetime(2015, 11, 8, 14, 00))
    assert ts == 1446991200.0
    assert type(ts) == float

    ts = field_no_tz.to_primitive(datetime(2015, 11, 8, 14, 00))
    assert ts == 1446991200.0
    assert type(ts) == float

    ts = field.to_primitive(datetime(2014, 5, 8, 22, 40, 40, tzinfo=gettz('PST8PDT')))
    assert ts == 1399614040.0

    ts = field_no_tz.to_primitive(datetime(2014, 5, 8, 22, 40, 40, tzinfo=gettz('PST8PDT')))
    assert ts == 1399614040.0

    dt_with_tz = datetime(2015, 11, 8, 16, 00, tzinfo=DateTimeType.offset_timezone(2))
    dt_no_tz = datetime(2015, 11, 8, 14, 00)
    dt_no_tz_usec = datetime(2015, 11, 8, 14, 00, microsecond=777700)

    assert field.to_primitive(dt_with_tz) == 1446991200.0
    assert field_no_tz.to_primitive(dt_with_tz) == 1446991200.0

    assert field.to_native(dt_with_tz) == dt_no_tz.replace(tzinfo=UTC)
    assert field_no_tz.to_native(dt_with_tz) == dt_no_tz

    assert field.to_native(1446991200.7777) == dt_no_tz_usec.replace(tzinfo=UTC)
    assert field_no_tz.to_native(1446991200.7777) == dt_no_tz_usec

    with pytest.raises(ConversionError):
        field.to_native(dt_no_tz)

    ts = field.to_primitive(dt_no_tz_usec)
    assert ts == 1446991200.7777
    assert type(ts) == float

    ts = field.to_primitive(datetime(2014, 5, 8, 22, 40, 40, tzinfo=UTC))
    assert ts == 1399588840.0

    assert field.to_primitive(EPOCH) == 0
    assert field.to_native(0) == EPOCH


def test_validate_tz():

    dt_naive = lambda: datetime(2015, 6, 1, 14, 00)
    dt_utc = lambda: datetime(2015, 6, 1, 14, 00, tzinfo=UTC)
    dt_plustwo = lambda: datetime(2015, 6, 1, 16, 00, tzinfo=DateTimeType.offset_timezone(2))
    dt_nyc = lambda: datetime(2015, 6, 1, 10, 00, tzinfo=NYC)

    all_values = [dt_naive, dt_utc, dt_plustwo, dt_nyc]

    def test_field(field, valid):
        for value in all_values:
            if value in valid:
                field.validate_tz(value())
            else:
                with pytest.raises(ValidationError):
                    field.validate_tz(value())

    test_field(DateTimeType(tzd='allow'), [dt_naive, dt_utc, dt_plustwo, dt_nyc])
    test_field(DateTimeType(convert_tz=True), [dt_naive, dt_utc])
    test_field(DateTimeType(tzd='utc'), [dt_utc, dt_plustwo, dt_nyc])
    test_field(DateTimeType(tzd='utc', convert_tz=True), [dt_utc])
    test_field(DateTimeType(tzd='reject'), [dt_naive])
    test_field(DateTimeType(tzd='require'), [dt_utc, dt_plustwo, dt_nyc])
    test_field(DateTimeType(drop_tzinfo=True), [dt_naive])


@pytest.mark.parametrize('tzd', ('require', 'allow', 'utc', 'reject'))
@pytest.mark.parametrize('convert_tz', (True, False))
@pytest.mark.parametrize('drop_tzinfo', (True, False))
def test_mock(tzd, convert_tz, drop_tzinfo):

    field = DateTimeType(tzd=tzd, convert_tz=convert_tz, drop_tzinfo=drop_tzinfo)
    for _ in range(20):
        field.validate(field._mock())


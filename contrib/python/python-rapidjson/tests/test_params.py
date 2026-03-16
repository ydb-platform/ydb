# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Basic params tests
# :Author:    Ken Robbins <ken@kenrobbins.com>
# :License:   MIT License
# :Copyright: © 2015 Ken Robbins
# :Copyright: © 2015, 2016, 2017, 2018, 2019, 2020, 2025 Lele Gaifax
#

from calendar import timegm
from datetime import date, datetime, time, timezone, timedelta
import io
import math
import uuid

import pytest

import rapidjson as rj


def test_skipkeys():
    o = {True: False, -1: 1, 1.1: 1.1, (1,2): "foo", b"asdf": 1, None: None}

    with pytest.raises(TypeError):
        rj.dumps(o)

    with pytest.raises(TypeError):
        rj.dumps(o, skipkeys=False)

    assert rj.dumps(o, skipkeys=True) == '{}'
    assert rj.dumps(o, mapping_mode=rj.MM_SKIP_NON_STRING_KEYS) == '{}'


def test_coerce_keys():
    o = {True: False, -1: 1, 1.1: 1.1, (1,2): "foo", b"asdf": 1, None: None}
    expected = '{"True":false,"-1":1,"1.1":1.1,"(1, 2)":"foo","b\'asdf\'":1,"None":null}'
    assert rj.dumps(o, mapping_mode=rj.MM_COERCE_KEYS_TO_STRINGS) == expected
    assert rj.Encoder(mapping_mode=rj.MM_COERCE_KEYS_TO_STRINGS)(o) == expected


def test_skip_invalid_keys():
    o = {True: False, -1: 1, 1.1: 1.1, (1,2): "foo", b"asdf": 1, None: None}

    with pytest.raises(TypeError):
        rj.Encoder()(o)

    with pytest.raises(TypeError):
        rj.Encoder(skip_invalid_keys=False)(o)

    assert rj.Encoder(skip_invalid_keys=True)(o) == '{}'
    assert rj.Encoder(mapping_mode=rj.MM_SKIP_NON_STRING_KEYS)(o) == '{}'


def test_ensure_ascii(dumps):
    s = '\N{GREEK SMALL LETTER ALPHA}\N{GREEK CAPITAL LETTER OMEGA}'
    assert dumps(s) == '"\\u03B1\\u03A9"'
    assert dumps(s, ensure_ascii=True) == '"\\u03B1\\u03A9"'
    assert dumps(s, ensure_ascii=False) == '"%s"' % s


def test_allow_nan():
    f = [1.1, float("inf"), 2.2, float("nan"), 3.3, float("-inf"), 4.4]
    expected = '[1.1,Infinity,2.2,NaN,3.3,-Infinity,4.4]'
    assert rj.dumps(f) == expected
    assert rj.dumps(f, number_mode=rj.NM_NAN) == expected
    assert rj.dumps(f, allow_nan=True) == expected
    with pytest.raises(ValueError):
        rj.dumps(f, number_mode=None)
    with pytest.raises(ValueError):
        rj.dumps(f, allow_nan=False)

    s = "NaN"
    assert math.isnan(rj.loads(s))
    assert math.isnan(rj.loads(s, number_mode=rj.NM_NAN))
    assert math.isnan(rj.loads(s, allow_nan=True))
    with pytest.raises(ValueError):
        rj.loads(s, number_mode=rj.NM_NONE)
    with pytest.raises(ValueError):
        rj.loads(s, allow_nan=False)

    s = "Infinity"
    assert rj.loads(s) == float("inf")
    assert rj.loads(s, number_mode=rj.NM_NAN) == float("inf")
    assert rj.loads(s, allow_nan=True) == float("inf")
    with pytest.raises(ValueError):
        rj.loads(s, number_mode=rj.NM_NONE)
    with pytest.raises(ValueError):
        rj.loads(s, allow_nan=False)

    s = "-Infinity"
    assert rj.loads(s) == float("-inf")
    assert rj.loads(s, number_mode=rj.NM_NAN) == float("-inf")
    assert rj.loads(s, allow_nan=True) == float("-inf")
    with pytest.raises(ValueError):
        rj.loads(s, number_mode=rj.NM_NONE)
    with pytest.raises(ValueError):
        rj.loads(s, allow_nan=False)


def test_native(dumps, loads):
    f = [-1, 1, 1.1, -2.2]
    expected = '[-1,1,1.1,-2.2]'
    assert dumps(f, number_mode=rj.NM_NATIVE) == expected
    assert dumps(f) == expected
    assert loads(expected, number_mode=rj.NM_NATIVE) == f
    assert loads(expected) == f

    trailing_comma = '[-1,1,1.1,-2.2,]'
    pytest.raises(ValueError, loads, trailing_comma, number_mode=rj.NM_NATIVE)
    expected = [-1,1,1.1,-2.2]
    assert loads(trailing_comma, number_mode=rj.NM_NATIVE,
                 parse_mode=rj.PM_TRAILING_COMMAS) == expected

    comments = '[-1,1,/*1.1,*/-2.2,]'
    pytest.raises(ValueError, loads, comments, number_mode=rj.NM_NATIVE)
    pytest.raises(ValueError, loads, comments,
                  number_mode=rj.NM_NATIVE, parse_mode=rj.PM_TRAILING_COMMAS)
    expected = [-1,1,-2.2]
    assert loads(comments, number_mode=rj.NM_NATIVE,
                 parse_mode=rj.PM_COMMENTS | rj.PM_TRAILING_COMMAS) == expected


def test_parse_mode(dumps, loads):
    trailing_comma = '[-1,1,1.1,-2.2,]'
    expected = [-1,1,1.1,-2.2]
    pytest.raises(ValueError, rj.loads, trailing_comma)
    pytest.raises(ValueError, rj.loads, trailing_comma, parse_mode=rj.PM_COMMENTS)
    assert loads(trailing_comma, parse_mode=rj.PM_TRAILING_COMMAS) == expected

    comments = ('[true,  // boolean\n'
                ' false, // idem\n'
                ' // 1.0, // ignored\n'
                ' /*\n'
                '  * ignored\n'
                ' 1,     // integer\n'
                '  */'
                ' "this is the end"]')
    expected = [True, False, "this is the end"]
    pytest.raises(ValueError, loads, comments)
    pytest.raises(ValueError, loads, comments, parse_mode=rj.PM_TRAILING_COMMAS)
    assert loads(comments, parse_mode=rj.PM_COMMENTS) == expected

    c_and_tc = ('[true,  // boolean\n'
                ' false, // idem\n'
                ' // 1.0, // ignored\n'
                ' /*\n'
                '  * ignored\n'
                ' 1,     // integer\n'
                '  */'
                ' {"one": 1 /*, "two": 2 */, "three": 3,},\n'
                ' "this is the end",]'
    )
    expected = [True, False, {"one": 1, "three": 3}, "this is the end"]
    pytest.raises(ValueError, loads, c_and_tc)
    pytest.raises(ValueError, loads, c_and_tc, parse_mode=rj.PM_TRAILING_COMMAS)
    pytest.raises(ValueError, loads, c_and_tc, parse_mode=rj.PM_COMMENTS)
    assert loads(c_and_tc, parse_mode=rj.PM_COMMENTS | rj.PM_TRAILING_COMMAS) == expected


def test_indent(dumps):
    o = {"a": 1, "z": 2, "b": 3}
    expected1 = '{\n    "a": 1,\n    "z": 2,\n    "b": 3\n}'
    expected2 = '{\n    "a": 1,\n    "b": 3,\n    "z": 2\n}'
    expected3 = '{\n    "b": 3,\n    "a": 1,\n    "z": 2\n}'
    expected4 = '{\n    "b": 3,\n    "z": 2,\n    "a": 1\n}'
    expected5 = '{\n    "z": 2,\n    "a": 1,\n    "b": 3\n}'
    expected6 = '{\n    "z": 2,\n    "b": 3,\n    "a": 1\n}'
    expected = (
        expected1,
        expected2,
        expected3,
        expected4,
        expected5,
        expected6)

    assert dumps(o, indent=4) in expected
    assert dumps(o, indent="    ") in expected
    assert dumps(o, indent="\t") in (e.replace("    ", "\t") for e in expected)

    with pytest.raises(TypeError):
        dumps(o, indent="\n\t")

    with pytest.raises(TypeError):
        dumps(o, indent="foo")

    with pytest.raises(TypeError):
        dumps(o, indent=-1)


def test_write_mode(dumps):
    o = {"a": 1, "b": [2, 3, 4]}
    expected_compact = ('{"a":1,"b":[2,3,4]}', '{"b":[2,3,4],"a":1}')
    expected_pretty = ('{\n    "a": 1,\n    "b": [2, 3, 4]\n}',
                       '{\n    "b": [2, 3, 4],\n    "a": 1\n}')

    assert dumps(o, write_mode=rj.WM_COMPACT) in expected_compact
    assert dumps(o, indent=0, write_mode=rj.WM_COMPACT) in expected_compact
    assert dumps(o, write_mode=rj.WM_SINGLE_LINE_ARRAY) in expected_pretty
    assert dumps(o, write_mode=rj.WM_PRETTY|rj.WM_SINGLE_LINE_ARRAY) in expected_pretty

    with pytest.raises(ValueError):
        dumps(o, write_mode=4)


def test_sort_keys(dumps):
    o = {"a": 1, "z": 2, "b": 3}
    expected0 = '{\n"a": 1,\n"b": 3,\n"z": 2\n}'
    expected1 = '{"a":1,"b":3,"z":2}'
    expected2 = '{\n    "a": 1,\n    "b": 3,\n    "z": 2\n}'

    assert dumps(o, sort_keys=True) == expected1
    assert dumps(o, sort_keys=True, indent=4) == expected2
    assert dumps(o, sort_keys=True, indent=0) == expected0
    assert dumps(o, mapping_mode=rj.MM_SORT_KEYS, indent=0) == expected0

    o = {'a0': 'a0', 'a': 'a', 'a1': 'a1', 'a\x00b': 'a\x00b'}
    assert sorted(o.keys()) == ['a', 'a\x00b', 'a0', 'a1']
    assert dumps(o, sort_keys=True) == '{"a":"a","a\\u0000b":"a\\u0000b","a0":"a0","a1":"a1"}'


def test_sort_and_coerce_keys(dumps):
    # Issue 229
    o = {"a": 1, 3: 4.5, date(2025,10,10): 2, date(2025,11,11): 3}
    expected = '{"2025-10-10":2,"2025-11-11":3,"3":4.5,"a":1}'
    assert dumps(o, sort_keys=True, mapping_mode=rj.MM_COERCE_KEYS_TO_STRINGS) == expected

    o = {"ab": 3, "a": 1, "a\x00b": 2}
    expected = '{"a":1,"a\\u0000b":2,"ab":3}'
    assert dumps(o, sort_keys=True, mapping_mode=rj.MM_COERCE_KEYS_TO_STRINGS) == expected


def test_default():
    class Bar:
        pass

    class Foo:
        def __init__(self):
            self.foo = "bar"

    def default(obj):
        if isinstance(obj, Foo):
            return {"foo": obj.foo}

        raise TypeError("default error")

    o = {"asdf": Foo()}
    assert rj.dumps(o, default=default) == '{"asdf":{"foo":"bar"}}'

    o = {"asdf": Foo(), "qwer": Bar()}
    with pytest.raises(TypeError):
        rj.dumps(o, default=default)

    with pytest.raises(TypeError):
        rj.dumps(o)


def test_decimal(dumps, loads):
    import math
    from decimal import Decimal

    dstr = "2.7182818284590452353602874713527"
    d = Decimal(dstr)

    with pytest.raises(TypeError):
        dumps(d)

    assert dumps(float(dstr)) == str(math.e)
    assert dumps(d, number_mode=rj.NM_DECIMAL) == dstr
    assert dumps({"foo": d}, number_mode=rj.NM_DECIMAL) == '{"foo":%s}' % dstr

    assert loads(dumps(d, number_mode=rj.NM_DECIMAL), number_mode=rj.NM_DECIMAL) == d

    assert loads(dumps(d, number_mode=rj.NM_DECIMAL)) == float(dstr)


def test_datetime_mode_dumps(dumps):
    import pytz

    d = datetime(2018, 1, 2, 11, 56, 19, 854440)
    dstr = d.isoformat()

    with pytest.raises(TypeError):
        dumps(d)

    with pytest.raises(TypeError):
        dumps(d, datetime_mode=rj.DM_NONE)

    assert dumps(d, datetime_mode=rj.DM_ISO8601) == '"%s"' % dstr
    assert dumps(d, datetime_mode=(rj.DM_ISO8601 | rj.DM_IGNORE_TZ)) == '"%s"' % dstr

    d = utcd = d.replace(tzinfo=pytz.utc)
    dstr = utcstr = d.isoformat()

    assert dumps(d, datetime_mode=rj.DM_ISO8601) == '"%s"' % dstr
    assert dumps(d, datetime_mode=(rj.DM_ISO8601 | rj.DM_IGNORE_TZ)) == '"%s"' % dstr[:-6]

    d = d.astimezone(pytz.timezone('Pacific/Chatham'))
    dstr = d.isoformat()

    assert dumps(d, datetime_mode=rj.DM_ISO8601) == '"%s"' % dstr
    assert dumps(d, datetime_mode=(rj.DM_ISO8601 | rj.DM_IGNORE_TZ)) == '"%s"' % dstr[:-6]

    d = d.astimezone(pytz.timezone('Asia/Kathmandu'))
    dstr = d.isoformat()

    assert dumps(d, datetime_mode=rj.DM_ISO8601) == '"%s"' % dstr
    assert dumps(d, datetime_mode=(rj.DM_ISO8601 | rj.DM_IGNORE_TZ)) == '"%s"' % dstr[:-6]

    d = d.astimezone(pytz.timezone('America/New_York'))
    dstr = d.isoformat()

    assert dumps(d, datetime_mode=rj.DM_ISO8601) == '"%s"' % dstr
    assert dumps(d, datetime_mode=(rj.DM_ISO8601 | rj.DM_IGNORE_TZ)) == '"%s"' % dstr[:-6]
    assert dumps(d, datetime_mode=(rj.DM_ISO8601 | rj.DM_SHIFT_TO_UTC)) == '"%s"' % utcstr

    assert dumps(d, datetime_mode=rj.DM_UNIX_TIME) == str(d.timestamp())

    assert dumps(
        d, datetime_mode=rj.DM_UNIX_TIME | rj.DM_SHIFT_TO_UTC) == str(utcd.timestamp())

    assert dumps(
        d, datetime_mode= rj.DM_UNIX_TIME | rj.DM_ONLY_SECONDS
    ) == str(d.timestamp()).split('.')[0]

    d = datetime(2018, 1, 2, 12, 56, 19, 854440)

    assert dumps(
        d, datetime_mode=rj.DM_ISO8601 | rj.DM_NAIVE_IS_UTC
    ) == '"%s+00:00"' % d.isoformat()

    assert dumps(
        d, datetime_mode=rj.DM_UNIX_TIME | rj.DM_NAIVE_IS_UTC
    ) == ('%d.%06d' % (timegm(d.timetuple()), d.microsecond)).rstrip('0')

    assert dumps(
        d, datetime_mode=(rj.DM_UNIX_TIME
                          | rj.DM_NAIVE_IS_UTC
                          | rj.DM_ONLY_SECONDS)
    ) == str(timegm(d.timetuple()))

    # This value caused a truncation problem when we were using
    # Writer.SetMaxDecimalPlaces(6) to emit the floating point number
    d = datetime.fromtimestamp(1514893636.276703)

    assert dumps(d, datetime_mode=rj.DM_UNIX_TIME) == str(d.timestamp())


def test_datetime_mode_loads(dumps, loads):
    import pytz

    utc = datetime.now(pytz.utc)
    utcstr = utc.isoformat()

    jsond = dumps(utc, datetime_mode=rj.DM_ISO8601)

    assert jsond == '"%s"' % utcstr
    assert loads(jsond, datetime_mode=rj.DM_ISO8601) == utc

    local = utc.astimezone(pytz.timezone('Europe/Rome'))
    locstr = local.isoformat()

    jsond = dumps(local, datetime_mode=rj.DM_ISO8601)

    assert jsond == '"%s"' % locstr
    assert loads(jsond) == locstr
    assert loads(jsond, datetime_mode=rj.DM_ISO8601) == local

    load_as_utc = loads(jsond, datetime_mode=(rj.DM_ISO8601 | rj.DM_SHIFT_TO_UTC))

    assert load_as_utc == utc
    assert not load_as_utc.utcoffset()

    load_as_naive = loads(jsond, datetime_mode=(rj.DM_ISO8601 | rj.DM_IGNORE_TZ))

    assert load_as_naive == local.replace(tzinfo=None)


@pytest.mark.parametrize(
    'value', [date.today(), datetime.now(), time(10,20,30)])
def test_datetime_values(value, dumps, loads):
    with pytest.raises(TypeError):
        rj.dumps(value)

    dumped = dumps(value, datetime_mode=rj.DM_ISO8601)
    loaded = loads(dumped, datetime_mode=rj.DM_ISO8601)
    assert loaded == value


@pytest.mark.parametrize(
    'value, expected', [
        ('1999-01-03T10:20:30.1',
         datetime(1999, 1, 3, 10, 20, 30, 100000)),
        ('2000-02-03T10:20:30.12',
         datetime(2000, 2, 3, 10, 20, 30, 120000)),
        ('2020-03-03T10:20:30.123',
         datetime(2020, 3, 3, 10, 20, 30, 123000)),
        ('2021-04-03T10:20:30.1234',
         datetime(2021, 4, 3, 10, 20, 30, 123400)),
        ('1999-08-03T10:20:30.12345',
         datetime(1999, 8, 3, 10, 20, 30, 123450)),
        ('1999-09-03T10:20:30.123456',
         datetime(1999, 9, 3, 10, 20, 30, 123456)),
        ('1999-10-03T10:20:30.1234567',
         datetime(1999, 10, 3, 10, 20, 30, 123456)),
        ('1999-11-03T10:20:30.12345678',
         datetime(1999, 11, 3, 10, 20, 30, 123456)),
        ('1999-12-03T10:20:30.123456789',
         datetime(1999, 12, 3, 10, 20, 30, 123456)),
        ('1999-12-03T10:20:30.123456789Z',
         datetime(1999, 12, 3, 10, 20, 30, 123456, tzinfo=timezone.utc)),
        ('1999-12-03T10:20:30.123456789-02:30',
         datetime(1999, 12, 3, 10, 20, 30, 123456,
                  tzinfo=timezone(timedelta(days=-1, seconds=77400)))),
        ('1999-12-03T10:20:30.123456789+02:30',
         datetime(1999, 12, 3, 10, 20, 30, 123456,
                  tzinfo=timezone(timedelta(seconds=9000)))),
    ]
)
def test_datetime_fractional_values(value, expected, dumps, loads):
    dumped = dumps(value, datetime_mode=rj.DM_ISO8601)
    loaded = loads(dumped, datetime_mode=rj.DM_ISO8601)
    assert loaded == expected


def test_uuid_mode(dumps, loads):
    assert rj.UM_NONE == 0
    assert rj.UM_CANONICAL == 1
    assert rj.UM_HEX == 2

    value = uuid.uuid1()
    with pytest.raises(TypeError):
        dumps(value)

    with pytest.raises(ValueError):
        dumps(value, uuid_mode=42)

    with pytest.raises(ValueError):
        loads('""', uuid_mode=42)

    dumped = dumps(value, uuid_mode=rj.UM_CANONICAL)
    loaded = loads(dumped, uuid_mode=rj.UM_CANONICAL)
    assert loaded == value

    # When loading, hex mode implies canonical format
    loaded = loads(dumped, uuid_mode=rj.UM_HEX)
    assert loaded == value

    dumped = dumps(value, uuid_mode=rj.UM_HEX)
    loaded = loads(dumped, uuid_mode=rj.UM_HEX)
    assert loaded == value


def test_uuid_and_datetime_mode_together(dumps, loads):
    value = [date.today(), uuid.uuid1()]
    dumped = dumps(value,
                   datetime_mode=rj.DM_ISO8601,
                   uuid_mode=rj.UM_CANONICAL)
    loaded = loads(dumped,
                   datetime_mode=rj.DM_ISO8601,
                   uuid_mode=rj.UM_CANONICAL)
    assert loaded == value


@pytest.mark.parametrize(
    'value,cls', [
        ('x999-02-03', str),
        ('1999 02 03', str),
        ('x0:02:20', str),
        ('20.02:20', str),
        ('x999-02-03T10:20:30', str),
        ('1999-02-03t10:20:30', str),

        ('0000-01-01', str),
        ('0001-99-99', str),
        ('0001-01-32', str),
        ('0001-02-29', str),

        ('24:02:20', str),
        ('23:62:20', str),
        ('23:02:62', str),
        ('20:02:20.123-25:00', str),
        ('20:02:20.123-05:61', str),

        ('1968-02-29', date),
        ('1999-02-03', date),

        ('20:02:20', time),
        ('20:02:20Z', time),
        ('20:02:20.123', time),
        ('20:02:20.123Z', time),
        ('20:02:20-05:00', time),
        ('20:02:20.123456', time),
        ('20:02:20.123456Z', time),
        ('20:02:20.123-05:00', time),
        ('20:02:20.123456-05:00', time),

        ('1999-02-03T10:20:30', datetime),
        ('1999-02-03T10:20:30Z', datetime),
        ('1999-02-03T10:20:30.1', datetime),
        ('1999-02-03T10:20:30.12', datetime),
        ('1999-02-03T10:20:30.123', datetime),
        ('1999-02-03T10:20:30.123Z', datetime),
        ('1999-02-03T10:20:30.1234', datetime),
        ('1999-02-03T10:20:30.12345', datetime),
        ('1999-02-03T10:20:30.12345Z', datetime),
        ('1999-02-03T10:20:30.12345+05:00', datetime),
        ('1999-02-03T10:20:30-05:00', datetime),
        ('1999-02-03T10:20:30.123456', datetime),
        ('1999-02-03T10:20:30.123456Z', datetime),
        ('1999-02-03T10:20:30.123-05:00', datetime),
        ('1999-02-03T10:20:30.123456-05:00', datetime),
        ('1999-02-03T10:20:30.123456-05:00', datetime),
        ('1999-02-03T10:20:30.1234567', datetime),
        ('1999-02-03T10:20:30.12345678', datetime),
        ('1999-02-03T10:20:30.123456789', datetime),
        ('1999-02-03T10:20:30.1234567Z', datetime),
        ('1999-02-03T10:20:30.12345678Z', datetime),
        ('1999-02-03T10:20:30.123456789Z', datetime),
        ('1999-02-03T10:20:30.1234567-20:10', datetime),
        ('1999-02-03T10:20:30.12345678+20:11', datetime),
        ('1999-02-03T10:20:30.123456789Z', datetime),
        ('1999-02-03T10:20:30.123456789-01:01', datetime),
        ('1999-02-03T10:20:30.123456789+00:00', datetime),
        ('1999-02-03T10:20:30.', str),
        ('1999-02-03T10:20:30.1A', str),
        ('1999-02-03T10:20:30. ', str),
        ('1999-02-03T10:20:30.123456789A', str),
        ('1999-02-03T10:20:30.123456789 ', str),
        ('1999-02-03T10:20:30.12345+00:00Z ', str),
        ('1999-02-03T10:20:30.1A34X6+01:00Z', str),
    ])
def test_datetime_iso8601(value, cls, loads):
    result = loads('"%s"' % value, datetime_mode=rj.DM_ISO8601)
    assert isinstance(result, cls)


@pytest.mark.parametrize(
    'value,cls', [
        ('7a683da49aa011e5972e3085a99ccac7', str),
        ('7a683da4 9aa0-11e5-972e-3085a99ccac7', str),
        ('za683da4-9aa0-11e5-972e-3085a99ccac7', str),

        ('7a683da4-9aa0-11e5-972e-3085a99ccac7', uuid.UUID),
    ])
def test_uuid_canonical(value, cls, loads):
    result = loads('"%s"' % value, uuid_mode=rj.UM_CANONICAL)
    assert isinstance(result, cls), type(result)


@pytest.mark.parametrize(
    'value,cls', [
        ('za683da49aa011e5972e3085a99ccac7', str),

        ('7a683da49aa011e5972e3085a99ccac7', uuid.UUID),
        ('7a683da4-9aa0-11e5-972e-3085a99ccac7', uuid.UUID),
    ])
def test_uuid_hex(value, cls, loads):
    result = loads('"%s"' % value, uuid_mode=rj.UM_HEX)
    assert isinstance(result, cls), type(result)


def test_object_hook():
    class Foo:
        def __init__(self, foo):
            self.foo = foo

    def hook(d):
        if 'foo' in d:
            return Foo(d['foo'])

        return d

    def default(obj):
        return {'foo': obj.foo}

    res = rj.loads('{"foo": 1}', object_hook=hook)
    assert isinstance(res, Foo)
    assert res.foo == 1

    assert rj.dumps(rj.loads('{"foo": 1}', object_hook=hook), default=default) == '{"foo":1}'
    res = rj.loads(rj.dumps(Foo(foo="bar"), default=default), object_hook=hook)
    assert isinstance(res, Foo)
    assert res.foo == "bar"


@pytest.mark.parametrize(
    'posargs,kwargs', (
        ( (), {} ),
        ( (None,), {} ),
        ( (True,), {} ),
        ( ('{}',), { 'this_keyword_arg_shall_never_exist': True } ),
        ( ('[]',), { 'object_hook': True } ),
        ( ('[]',), { 'datetime_mode': 'no' } ),
        ( ('[]',), { 'datetime_mode': 1.0 } ),
        ( ('[]',), { 'datetime_mode': -100 } ),
        ( ('[]',), { 'datetime_mode': rj.DM_UNIX_TIME + 1 } ),
        ( ('[]',), { 'datetime_mode': rj.DM_UNIX_TIME } ),
        ( ('[]',), { 'datetime_mode': rj.DM_SHIFT_TO_UTC } ),
        ( ('[]',), { 'uuid_mode': 'no' } ),
        ( ('[]',), { 'uuid_mode': 1.0 } ),
        ( ('[]',), { 'uuid_mode': -100 } ),
        ( ('[]',), { 'uuid_mode': 100 } ),
        ( ('[]',), { 'parse_mode': 'no' } ),
        ( ('[]',), { 'parse_mode': 1.0 } ),
        ( ('[]',), { 'uuid_mode': -100 } ),
    ))
def test_invalid_loads_params(posargs, kwargs, loads):
    try:
        loads(*posargs, **kwargs)
    except (TypeError, ValueError):
        pass
    else:
        assert False, "Expected either a TypeError or a ValueError"


@pytest.mark.parametrize(
    'posargs,kwargs', (
        ( (), {} ),
        ( ([],), { 'this_keyword_arg_shall_never_exist': True } ),
        ( ([],), { 'default': True } ),
        ( ([],), { 'indent': -1 }),
        ( ([],), { 'datetime_mode': 'no' } ),
        ( ([],), { 'datetime_mode': 1.0 } ),
        ( ([],), { 'datetime_mode': -100 } ),
        ( ([],), { 'datetime_mode': rj.DM_UNIX_TIME + 1 } ),
        ( ([],), { 'datetime_mode': rj.DM_SHIFT_TO_UTC } ),
        ( ([],), { 'uuid_mode': 'no' } ),
        ( ([],), { 'uuid_mode': 1.0 } ),
        ( ([],), { 'uuid_mode': -100 } ),
        ( ([],), { 'uuid_mode': 100 } ),
        ( ([],), { 'iterable_mode': 'no'} ),
        ( ([],), { 'iterable_mode': 1.0} ),
        ( ([],), { 'iterable_mode': -100} ),
        ( ([],), { 'iterable_mode': 100} ),
        ( ([],), { 'mapping_mode': 'no'} ),
        ( ([],), { 'mapping_mode': 1.0} ),
        ( ([],), { 'mapping_mode': -100} ),
        ( ([],), { 'mapping_mode': 100} ),
    ))
def test_invalid_dumps_params(posargs, kwargs, dumps):
    try:
        dumps(*posargs, **kwargs)
    except (TypeError, ValueError):
        pass
    else:
        assert False, "Expected either a TypeError or a ValueError"


def test_explicit_defaults_loads():
    assert rj.loads(
        string='"foo"',
        object_hook=None,
        number_mode=None,
        datetime_mode=None,
        uuid_mode=None,
        parse_mode=None,
        allow_nan=True,
    ) == "foo"


def test_explicit_defaults_load():
    assert rj.load(
        stream=io.StringIO('"foo"'),
        object_hook=None,
        number_mode=None,
        datetime_mode=None,
        uuid_mode=None,
        parse_mode=None,
        chunk_size=None,
        allow_nan=True,
    ) == "foo"


def test_explicit_defaults_decoder():
    assert rj.Decoder(
        number_mode=None,
        datetime_mode=None,
        uuid_mode=None,
        parse_mode=None)('"foo"') == "foo"


def test_explicit_defaults_dumps():
    assert rj.dumps(
        obj='foo',
        skipkeys=False,
        ensure_ascii=True,
        indent=None,
        default=None,
        sort_keys=False,
        number_mode=None,
        datetime_mode=None,
        uuid_mode=None,
        allow_nan=True,
    ) == '"foo"'


def test_explicit_defaults_dump():
    stream = io.StringIO()
    assert rj.dump(
        obj='foo',
        stream=stream,
        skipkeys=False,
        ensure_ascii=True,
        indent=None,
        default=None,
        sort_keys=False,
        number_mode=None,
        datetime_mode=None,
        uuid_mode=None,
        allow_nan=True,
        chunk_size=None) is None
    assert stream.getvalue() == '"foo"'


def test_explicit_defaults_encoder():
    assert rj.Encoder(
        skip_invalid_keys=False,
        ensure_ascii=True,
        indent=None,
        sort_keys=False,
        number_mode=None,
        datetime_mode=None,
        uuid_mode=None)({'foo': 'bar'}) == '{"foo":"bar"}'


def test_encoder_call():
    o = {'foo': 'bar'}

    result = rj.Encoder()(o)
    assert result == '{"foo":"bar"}'
    assert rj.Encoder()(o, None) == result

    stream = io.StringIO()
    assert rj.Encoder()(o, stream) is None
    assert stream.getvalue() == result

    stream = io.StringIO()
    assert rj.Encoder()(o, stream=stream) is None
    assert stream.getvalue() == result


def test_decoder_attrs():
    d = rj.Decoder(
        number_mode=rj.NM_NAN,
        datetime_mode=rj.DM_ISO8601,
        uuid_mode=rj.UM_CANONICAL,
        parse_mode=rj.PM_COMMENTS)
    assert d.number_mode == rj.NM_NAN
    assert d.datetime_mode == rj.DM_ISO8601
    assert d.uuid_mode == rj.UM_CANONICAL
    assert d.parse_mode == rj.PM_COMMENTS


def test_encoder_attrs():
    e = rj.Encoder(
        skip_invalid_keys=True,
        ensure_ascii=True,
        indent='\n',
        sort_keys=True,
        number_mode=rj.NM_NAN,
        datetime_mode=rj.DM_ISO8601,
        uuid_mode=rj.UM_CANONICAL,
        bytes_mode=rj.BM_UTF8)
    assert e.skip_invalid_keys
    assert e.ensure_ascii
    assert e.indent_char == '\n'
    assert e.indent_count == 1
    assert e.write_mode == rj.WM_PRETTY
    assert e.sort_keys
    assert e.number_mode == rj.NM_NAN
    assert e.datetime_mode == rj.DM_ISO8601
    assert e.uuid_mode == rj.UM_CANONICAL
    assert e.bytes_mode == rj.BM_UTF8

    e = rj.Encoder(mapping_mode=rj.MM_SKIP_NON_STRING_KEYS|rj.MM_SORT_KEYS)
    assert e.skip_invalid_keys
    assert e.sort_keys

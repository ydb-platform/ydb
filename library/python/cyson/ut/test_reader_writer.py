# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division

import io
import math
import pytest
import six
import sys

from functools import partial

from cyson import (
    dumps, loads, YsonInt64, YsonUInt64, UInt, Writer, OutputStream,
    UnicodeReader,
)


if six.PY2:
    NativeUInt = long  # noqa: F821
elif six.PY3:
    NativeUInt = UInt
    unicode = str
    long = int
else:
    raise RuntimeError('Unsupported Python version')


def canonize(value, as_unicode=False):
    _canonize = partial(canonize, as_unicode=as_unicode)

    if isinstance(value, (list, tuple)):
        return [_canonize(_) for _ in value]
    elif isinstance(value, dict):
        return {_canonize(k): _canonize(value[k]) for k in value}
    elif isinstance(value, unicode) and not as_unicode:
        return value.encode('utf8')
    elif isinstance(value, bytes) and as_unicode:
        return value.decode('utf8')

    return value


def switch_string_type(string):
    if isinstance(string, bytes):
        return string.decode('utf8')
    elif isinstance(string, unicode):
        return string.encode('utf8')

    raise TypeError('expected unicode or bytes, got {!r}'.format(string))


def coerce(obj, to, via=None):
    if via is None:
        via = to

    if isinstance(obj, to):
        return obj

    return via(obj)


SKIP_PY3 = pytest.mark.skipif(six.PY3, reason='Makes no sense for Python3')


if six.PY3 and sys.platform == 'win32':
    NUMPY_CASES = []
else:
    import numpy as np

    NUMPY_CASES = [
        # numpy int
        np.int8(2 ** 7 - 1), np.int16(2 ** 15 - 1),
        np.int32(2 ** 31 - 1), np.int64(2 ** 63 - 1),
        # numpy uint
        np.uint8(2 ** 8 - 1), np.uint16(2 ** 16 - 1),
        np.uint32(2 ** 32 - 1), np.uint64(2 ** 64 - 1),
        # numpy float
        np.float16(100.0), np.float32(100.0), np.float64(100.0),
    ]


CASES = [
    # NoneType
    None,
    # boolean
    True, False,
    # int
    0, 1, -1, int(2 ** 63 - 1), int(-2 ** 63),
    # float
    0.0, 100.0, -100.0, float('inf'), float('-inf'),
    # bytes
    b'', b'hello', u'Привет'.encode('utf8'),
    # unicode
    u'', u'hello', u'Привет',
    # list
    [], [0], [1, 'hello'], [17, 'q'] * 100, [b'bytes'],
    # tuple
    (), (0,), (1, 'hello'), (17, 'q') * 100, (b'bytes',),
    # dict
    {}, {'a': 'b'}, {'a': 17}, {'a': [1, 2, 3]}, {b'a': 1, u'b': b'a'}
] + NUMPY_CASES


@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
@pytest.mark.parametrize('value', CASES)
def test_roundtrip(value, format):
    encoded = dumps(value, format)
    decoded = loads(encoded)
    assert encoded == dumps(value, switch_string_type(format))
    assert decoded == canonize(value)


# NOTE: roundtrip test doesn't work for NaN (NaN != NaN)
@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
def test_nan(format):
    encoded = dumps(float('nan'), format)
    decoded = loads(encoded)
    assert encoded == dumps(float('nan'), switch_string_type(format))
    assert math.isnan(decoded)


@SKIP_PY3
@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
@pytest.mark.parametrize(
    'value', [long(0), long(1), long(2 ** 63), long(2 ** 64 - 1)]
)
def test_long_roundtrip(value, format):
    encoded = dumps(value, format)
    decoded = loads(encoded)
    assert encoded == dumps(value, switch_string_type(format))
    assert decoded == value


@pytest.mark.parametrize(
    'value', [NativeUInt(0), NativeUInt(111), NativeUInt(2 ** 63), NativeUInt(2 ** 64 - 1)]
)
@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
def test_readwrite_uint64(value, format):
    dumped_uint64 = dumps(coerce(value, YsonUInt64), format=format)
    loaded_uint64 = loads(dumped_uint64)

    assert type(value) is NativeUInt
    assert type(loaded_uint64) is NativeUInt
    assert dumps(value, format=format) == dumped_uint64


@pytest.mark.parametrize('value', [int(-2 ** 63), -111, 0, 111, int(2 ** 63 - 1)])
@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
def test_readwrite_int64(value, format):
    dumped_int64 = dumps(YsonInt64(value), format=format)
    loaded_int64 = loads(dumped_int64)

    assert type(value) is int
    assert type(loaded_int64) is int
    assert dumps(value, format=format) == dumped_int64


@SKIP_PY3
def test_long_overflow():
    with pytest.raises(OverflowError):
        dumps(long(-1))

    with pytest.raises(OverflowError):
        dumps(long(2**64))


@pytest.mark.parametrize('value', [2 ** 63, -2 ** 63 - 1])
def test_int64_overflow(value):
    with pytest.raises(OverflowError):
        int64_value = YsonInt64(value)
        dumps(int64_value)

    if six.PY3:
        with pytest.raises(OverflowError):
            dumps(value)


@pytest.mark.parametrize('value', [2 ** 64, 2 ** 100])
def test_uint64_overflow(value):
    with pytest.raises(OverflowError):
        uint64_value = YsonUInt64(value)
        dumps(uint64_value)


@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
def test_force_write_sequence(format):
    class Sequence(object):
        def __init__(self, seq):
            self._seq = seq

        def __getitem__(self, index):
            return self._seq[index]

        def __len__(self):
            return len(self._seq)

    sequence = [1, 1.1, None, b'xyz']

    sink = io.BytesIO()
    writer = Writer(OutputStream.from_file(sink), format=format)

    writer.begin_stream()
    writer.list(Sequence(sequence))
    writer.end_stream()

    assert sink.getvalue() == dumps(sequence, format)


@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
def test_force_write_mapping(format):
    class Mapping(object):
        def __init__(self, mapping):
            self._mapping = mapping

        def __getitem__(self, key):
            return self._mapping[key]

        def keys(self):
            return self._mapping.keys()

    mapping = {b'a': 1, b'b': 1.1, b'c': None, b'd': b'some'}

    sink = io.BytesIO()
    writer = Writer(OutputStream.from_file(sink), format=format)

    writer.begin_stream()
    writer.map(Mapping(mapping))
    writer.end_stream()

    assert sink.getvalue() == dumps(mapping, format)


@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
@pytest.mark.parametrize('value', CASES)
def test_unicode_reader(value, format):
    expected = canonize(value, as_unicode=True)
    got = loads(dumps(value, format), UnicodeReader)
    assert expected == got


def test_unicode_reader_raises_unicode_decode_error():
    not_decodable = b'\x80\x81'
    with pytest.raises(UnicodeDecodeError):
        loads(dumps(not_decodable, format='binary'), UnicodeReader)


def test_unicode_reader_decodes_object_with_attributes():
    data = b'{"a" = "b"; "c" = <"foo" = "bar">"d"}'
    expected = {u"a": u"b", u"c": u"d"}
    assert loads(data, UnicodeReader) == expected

# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Basic tests
# :Author:    John Anderson <sontek@gmail.com>
# :License:   MIT License
# :Copyright: © 2015 John Anderson
# :Copyright: © 2016, 2017, 2018, 2019, 2020, 2021, 2024 Lele Gaifax
#

import random
import sys

import pytest

import rapidjson as rj


@pytest.mark.parametrize(
    'value', (
        'A', 'cruel\x00world',
        1, -1,
        2.3, -36.973846435546875, 1514893636.276703,
        {'foo': 'bar', '\x00': 'issue57', 'issue57': '\x00'},
        [1, 2, 'a', 1.2, {'foo': 'bar'},],
        sys.maxsize, sys.maxsize**2, 10**1000,
))
def test_base_values(value, dumps, loads):
    dumped = dumps(value)
    loaded = loads(dumped)
    assert loaded == value and type(loaded) is type(value)


def test_float(dumps):
    value = 0.1 + 0.2
    dumped = dumps(value)
    assert dumped == '0.30000000000000004'


def test_tuple(dumps):
    obj = [1, 2, 'a', 1.2, {'foo': 'bar'},]
    assert dumps(obj) == dumps(tuple(obj))


def test_bytes_value(dumps):
    value = b'cruel\x00world'
    dumped = dumps(value)
    assert dumped == r'"cruel\u0000world"'
    dumped = dumps(bytearray(value))
    assert dumped == r'"cruel\u0000world"'


def test_larger_structure(dumps, loads):
    value = {
        'words': """
            Lorem ipsum dolor sit amet, consectetur adipiscing
            elit. Mauris adipiscing adipiscing placerat.
            Vestibulum augue augue,
            pellentesque quis sollicitudin id, adipiscing.
            """,
        'list': list(range(200)),
        'dict': dict((str(i),'a') for i in range(200)),
        'int': 100100100,
        'float': 100999.123456
    }

    dumped = dumps(value)
    loaded = loads(dumped)
    assert loaded == value


def test_object_hook():
    def as_complex(dct):
        if '__complex__' in dct:
            return complex(dct['real'], dct['imag'])

        return dct

    result = rj.loads(
        '{"__complex__": true, "real": 1, "imag": 2}',
        object_hook=as_complex
    )

    assert result == (1+2j)


def test_end_object():
    class ComplexDecoder(rj.Decoder):
        def end_object(self, dct):
            if '__complex__' in dct:
                return complex(dct['real'], dct['imag'])

            return dct

    loads = ComplexDecoder()
    result = loads('{"__complex__": true, "real": 1, "imag": 2}')
    assert result == (1+2j)


def test_dumps_default():
    def encode_complex(obj):
        if isinstance(obj, complex):
            return [obj.real, obj.imag]
        raise TypeError(repr(obj) + " is not JSON serializable")

    result = rj.dumps(2 + 1j, default=encode_complex)
    assert result == '[2.0,1.0]'


def test_encoder_default():
    class ComplexEncoder(rj.Encoder):
        def default(self, obj):
            if isinstance(obj, complex):
                return [obj.real, obj.imag]
            raise TypeError(repr(obj) + " is not JSON serializable")

    dumps = ComplexEncoder()
    result = dumps(2 + 1j)
    assert result == '[2.0,1.0]'


def test_doubles(dumps, loads):
    for _ in range(100000):
        d = sys.maxsize * random.random()
        dumped = dumps(d)
        loaded = loads(dumped)
        assert loaded == d


def test_unicode(dumps, loads):
   arabic='بينهم ان يكون مسلما رشيدا عاقلا ًوابنا شرعيا لابوين عمانيين'
   chinese='本站所提供的資料和服務都不收費，因此網站所需要的資金全來自廣告及捐款。若您願意捐款補助'
   for text in (arabic, chinese):
       dumped = dumps(text)
       loaded = loads(dumped)
       assert text == loaded


def test_serialize_sets_dumps():
    def default_iterable(obj):
        if isinstance(obj, set):
            return list(obj)
        raise TypeError(repr(obj) + " is not JSON serializable")

    rj.dumps([set()], default=default_iterable)

    with pytest.raises(TypeError):
        rj.dumps([set()])


def test_serialize_sets_encoder():
    class SetsEncoder(rj.Encoder):
        def default(self, obj):
            if isinstance(obj, set):
                return list(obj)
            raise TypeError(repr(obj) + " is not JSON serializable")

    dumps = SetsEncoder()
    dumps([set()])

    with pytest.raises(TypeError):
        rj.Encoder()([set()])


def test_constants(dumps, loads):
    for c in [None, True, False]:
        assert loads(dumps(c)) is c
        assert loads(dumps([c]))[0] is c
        assert loads(dumps({'a': c}))['a'] is c


def test_iterables(dumps):
    assert dumps(iter("abc")) == '["a","b","c"]'

    def gen():
        yield 1
        yield 2
        yield 3

    assert dumps(gen()) == '[1,2,3]'


def test_decode_error(loads):
    pytest.raises(rj.JSONDecodeError, loads, '{')


def test_decode_bytes_and_bytearray():
    s = 'FòBàr'
    j = f'"{s}"'
    utf32 = j.encode('utf-32')
    for loader in (rj.loads, rj.Decoder()):
        pytest.raises(UnicodeDecodeError, loader, utf32)

    utf8 = j.encode('utf-8')
    for loader in (rj.loads, rj.Decoder()):
        for data in (utf8, bytearray(utf8)):
            assert loader(data) == s


def test_shared_keys(loads):
    res = loads('[{"key": "value1"}, {"key": "value2"}]')
    key1, = res[0].keys()
    key2, = res[1].keys()
    assert key1 is key2


# TODO: Figure out what we want to do here
bad_tests = """
def test_true_false():
    dumped1 = sorted(rj.dumps({True: False, False: True}))
    dumped2 = sorted(rj.dumps({
        2: 3.0,
        4.0: long_type(5),
        False: 1,
        long_type(6): True,
        "7": 0
    }))

    assert dumped1 == '{"false": true, "true": false}'
    expected = '{"2": 3.0, "4.0": 5, "6": true, "7": 0, "false": 1}'

    assert dumped2 == expected"""

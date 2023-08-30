# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division

import pytest
import six

from cyson import PyWriter, PyReader, dumps, loads, dumps_into


if six.PY3:
    unicode = str


def switch_string_type(string):
    if isinstance(string, unicode):
        return string.encode('utf8')
    elif isinstance(string, bytes):
        return string.decode('utf8')

    raise TypeError('expected bytes or unicode, got {!r}'.format(string))


CASES = [
    None,
    # int
    0, 1, -1, 2**63, -2**63, 2**64 - 1,
    # float
    0.0, 100.0, -100.0,
    # long
    10**100, 2**300, -7**100,
    # bytes
    b'', b'hello', u'Привет'.encode('utf8'),
    # unicode
    u'', u'hello', u'Привет',
    # tuple
    (), (0,), (1, 'hello'), (17, 'q') * 100,
    # list
    [], [0], ['hello', set([1, 2, 3])], [17, 'q'] * 100,
    # dict
    {}, {'a': 'b'}, {'a': 17}, {'a': frozenset([1, 2, 3])}, {b'a': 1, u'b': 2},
    {1: 2, 3: 4, 5: None}, {(1, 2, 3): (1, 4, 9), None: 0},
    # set
    set(), {1, 2, 3}, {'hello', 'world'},
    # frozenset
    frozenset(), frozenset([1, 2, 3]), frozenset(['hello', 'world']),
]


@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
@pytest.mark.parametrize('value', CASES)
def test_roundtrip(value, format):
    encoded = dumps(value, format=format, Writer=PyWriter)
    decoded = loads(encoded, Reader=PyReader)
    assert encoded == dumps(value, format=switch_string_type(format), Writer=PyWriter)
    assert type(decoded) is type(value)
    assert decoded == value


@pytest.mark.parametrize('format', ['binary', 'text', 'pretty'])
@pytest.mark.parametrize('value', CASES)
def test_roundtrip_bytearray(value, format):
    encoded1 = bytearray()
    encoded2 = bytearray()
    dumps_into(encoded1, value, format=format, Writer=PyWriter)
    dumps_into(encoded2, value, format=switch_string_type(format), Writer=PyWriter)
    decoded = loads(encoded1, Reader=PyReader)
    assert decoded == loads(encoded2, Reader=PyReader)
    assert type(decoded) is type(value)
    assert decoded == value

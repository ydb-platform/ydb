import sys

import pytest

from netaddr.compat import _sys_maxint, _is_str, _is_int, _callable, _iter_next
from netaddr.compat import _dict_keys, _dict_items
from netaddr.compat import _bytes_join, _zip, _range
from netaddr.compat import _iter_range


@pytest.mark.skipif(sys.version_info < (3,), reason="requires python 3.x")
def test_compat_py3():
    assert _is_str(''.encode())

    #   byte string join tests.
    str_8bit = _bytes_join(['a'.encode(), 'b'.encode(), 'c'.encode()])

    assert str_8bit == 'abc'.encode()
    assert "b'abc'" == '%r' % str_8bit


@pytest.mark.skipif(sys.version_info > (3,), reason="requires python 2.x")
def test_compat_py2():
    assert _is_str(unicode(''))

    #   Python 2.x - 8 bit strings are just regular strings
    str_8bit = _bytes_join(['a', 'b', 'c'])
    assert str_8bit == 'abc'.encode()
    assert "'abc'" == '%r' % str_8bit


def test_compat_string_and_int_detection():
    assert _is_int(_sys_maxint)
    assert not _is_str(_sys_maxint)
    assert _is_str('')
    assert _is_str(''.encode())


def test_compat_dict_operations():
    d = { 'a' : 0, 'b' : 1, 'c' : 2 }
    assert sorted(_dict_keys(d)) == ['a', 'b', 'c']
    assert sorted(_dict_items(d)) == [('a', 0), ('b', 1), ('c', 2)]


def test_compat_zip():
    l2 = _zip([0], [1])
    assert hasattr(_zip(l2), 'pop')
    assert l2 == [(0, 1)]


def test_compat_range():
    l1 = _range(3)
    assert isinstance(l1, list)
    assert hasattr(l1, 'pop')
    assert l1 == [0, 1, 2]

    it = _iter_range(3)
    assert not isinstance(it, list)
    assert hasattr(it, '__iter__')
    assert it != [0, 1, 2]
    assert list(it) == [0, 1, 2]


def test_compat_callable():
    i = 1

    def f1():
        """docstring"""
        pass

    f2 = lambda x: x
    assert not _callable(i)

    assert _callable(f1)
    assert _callable(f2)


def test_iter_next():
    it = iter([42])
    assert _iter_next(it) == 42

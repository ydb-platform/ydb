import re

import pytest

from dirty_equals import IsAnyStr, IsBytes, IsStr


@pytest.mark.parametrize(
    'value,dirty,match',
    [
        # IsStr tests
        ('foo', IsStr, True),
        ('foo', IsStr(), True),
        (b'foo', IsStr, False),
        ('foo', IsStr(regex='fo{2}'), True),
        ('foo', IsStr(regex=b'fo{2}'), False),
        ('foo', IsStr(regex=re.compile('fo{2}')), True),
        ('Foo', IsStr(regex=re.compile('fo{2}', flags=re.I)), True),
        ('Foo', IsStr(regex=re.compile('fo{2}'), regex_flags=re.I), True),
        ('foo', IsStr(regex='fo'), False),
        ('foo', IsStr(regex='foo', max_length=2), False),
        ('foo\nbar', IsStr(regex='fo.*', regex_flags=re.S), True),
        ('foo\nbar', IsStr(regex='fo.*'), False),
        ('foo', IsStr(min_length=3), True),
        ('fo', IsStr(min_length=3), False),
        ('foo', IsStr(max_length=3), True),
        ('foobar', IsStr(max_length=3), False),
        ('foo', IsStr(case='lower'), True),
        ('FOO', IsStr(case='lower'), False),
        ('FOO', IsStr(case='upper'), True),
        ('foo', IsStr(case='upper'), False),
        # IsBytes tests
        (b'foo', IsBytes, True),
        (b'foo', IsBytes(), True),
        ('foo', IsBytes, False),
        (b'foo', IsBytes(regex=b'fo{2}'), True),
        (b'Foo', IsBytes(regex=re.compile(b'fo{2}', flags=re.I)), True),
        (b'Foo', IsBytes(regex=re.compile(b'fo{2}'), regex_flags=re.I), True),
        (b'foo', IsBytes(regex=b'fo'), False),
        (b'foo', IsBytes(regex=b'foo', max_length=2), False),
        (b'foo\nbar', IsBytes(regex=b'fo.*', regex_flags=re.S), True),
        (b'foo\nbar', IsBytes(regex=b'fo.*'), False),
        (b'foo', IsBytes(min_length=3), True),
        (b'fo', IsBytes(min_length=3), False),
        # IsAnyStr tests
        (b'foo', IsAnyStr, True),
        (b'foo', IsAnyStr(), True),
        ('foo', IsAnyStr, True),
        (b'foo', IsAnyStr(regex=b'fo{2}'), True),
        ('foo', IsAnyStr(regex=b'fo{2}'), True),
        (b'foo', IsAnyStr(regex='fo{2}'), True),
        ('foo', IsAnyStr(regex='fo{2}'), True),
        (b'Foo', IsAnyStr(regex=re.compile(b'fo{2}', flags=re.I)), True),
        ('Foo', IsAnyStr(regex=re.compile(b'fo{2}', flags=re.I)), True),
        (b'Foo', IsAnyStr(regex=re.compile(b'fo{2}'), regex_flags=re.I), True),
        ('Foo', IsAnyStr(regex=re.compile(b'fo{2}'), regex_flags=re.I), True),
        (b'Foo', IsAnyStr(regex=re.compile('fo{2}', flags=re.I)), True),
        ('Foo', IsAnyStr(regex=re.compile('fo{2}', flags=re.I)), True),
        (b'Foo', IsAnyStr(regex=re.compile('fo{2}'), regex_flags=re.I), True),
        ('Foo', IsAnyStr(regex=re.compile('fo{2}'), regex_flags=re.I), True),
        (b'foo\nbar', IsAnyStr(regex=b'fo.*', regex_flags=re.S), True),
        (b'foo\nbar', IsAnyStr(regex=b'fo.*'), False),
        ('foo', IsAnyStr(regex=b'foo', max_length=2), False),
        (b'foo', IsAnyStr(regex=b'foo', max_length=2), False),
        (b'foo', IsAnyStr(min_length=3), True),
        ('foo', IsAnyStr(min_length=3), True),
        (b'fo', IsAnyStr(min_length=3), False),
    ],
)
def test_dirty_equals_true(value, dirty, match):
    if match:
        assert value == dirty
    else:
        assert value != dirty


def test_regex_true():
    assert 'whatever' == IsStr(regex='whatever')
    reg = IsStr(regex='wh.*er')
    assert 'whatever' == reg
    assert str(reg) == "'whatever'"


def test_regex_bytes_true():
    assert b'whatever' == IsBytes(regex=b'whatever')
    assert b'whatever' == IsBytes(regex=b'wh.*er')


def test_regex_false():
    reg = IsStr(regex='wh.*er')
    with pytest.raises(AssertionError):
        assert 'WHATEVER' == reg
    assert str(reg) == "IsStr(regex='wh.*er')"


def test_regex_false_type_error():
    assert 123 != IsStr(regex='wh.*er')

    reg = IsBytes(regex=b'wh.*er')
    with pytest.raises(AssertionError):
        assert 'whatever' == reg
    assert str(reg) == "IsBytes(regex=b'wh.*er')"


def test_is_any_str():
    assert 'foobar' == IsAnyStr
    assert b'foobar' == IsAnyStr
    assert 123 != IsAnyStr
    assert 'foo' == IsAnyStr(regex='foo')
    assert 'foo' == IsAnyStr(regex=b'foo')
    assert b'foo' == IsAnyStr(regex='foo')
    assert b'foo' == IsAnyStr(regex=b'foo')

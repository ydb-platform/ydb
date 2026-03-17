from flex.utils import pluralize


def test_with_list():
    actual = pluralize(['a'])
    assert actual == ['a']


def test_with_tuple():
    actual = pluralize(('a',))
    assert actual == ('a',)


def test_with_dict():
    actual = pluralize({'a': 1})
    assert actual == [{'a': 1}]


def test_with_string():
    actual = pluralize('abc')
    assert actual == ['abc']


def test_with_integer():
    actual = pluralize(1)
    assert actual == [1]


def test_with_float():
    actual = pluralize(1.1)
    assert actual == [1.1]


def test_with_null():
    actual = pluralize(None)
    assert actual == [None]

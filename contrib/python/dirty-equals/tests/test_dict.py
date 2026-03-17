import pytest

from dirty_equals import IsDict, IsIgnoreDict, IsPartialDict, IsPositiveInt, IsStr, IsStrictDict


@pytest.mark.parametrize(
    'input_value,expected',
    [
        ({}, IsDict),
        ({}, IsDict()),
        ({'a': 1}, IsDict(a=1)),
        ({1: 2}, IsDict({1: 2})),
        ({'a': 1, 'b': 2}, IsDict(a=1, b=2)),
        ({'b': 2, 'a': 1}, IsDict(a=1, b=2)),
        ({'a': 1, 'b': None}, IsDict(a=1, b=None)),
        ({'a': 1, 'b': 3}, ~IsDict(a=1, b=2)),
        # partial dict
        ({1: 10, 2: 20}, IsPartialDict({1: 10})),
        ({1: 10}, IsPartialDict({1: 10})),
        ({1: 10, 2: 20}, IsPartialDict({1: 10})),
        ({1: 10, 2: 20}, IsDict({1: 10}).settings(partial=True)),
        ({1: 10}, ~IsPartialDict({1: 10, 2: 20})),
        ({1: 10, 2: None}, ~IsPartialDict({1: 10, 2: 20})),
        # ignore dict
        ({}, IsIgnoreDict()),
        ({'a': 1, 'b': 2}, IsIgnoreDict(a=1, b=2)),
        ({'a': 1, 'b': None}, IsIgnoreDict(a=1)),
        ({1: 10, 2: None}, IsIgnoreDict({1: 10})),
        ({'a': 1, 'b': 2}, ~IsIgnoreDict(a=1)),
        ({1: 10, 2: False}, ~IsIgnoreDict({1: 10})),
        ({1: 10, 2: False}, IsIgnoreDict({1: 10}).settings(ignore={False})),
        # strict dict
        ({}, IsStrictDict()),
        ({'a': 1, 'b': 2}, IsStrictDict(a=1, b=2)),
        ({'a': 1, 'b': 2}, ~IsStrictDict(b=2, a=1)),
        ({1: 10, 2: 20}, IsStrictDict({1: 10, 2: 20})),
        ({1: 10, 2: 20}, ~IsStrictDict({2: 20, 1: 10})),
        ({1: 10, 2: 20}, ~IsDict({2: 20, 1: 10}).settings(strict=True)),
        # combining types
        ({'a': 1, 'b': 2, 'c': 3}, IsStrictDict(a=1, c=3).settings(partial=True)),
        ({'a': 1, 'b': 2, 'c': 3}, IsStrictDict(a=1, b=2).settings(partial=True)),
        ({'a': 1, 'b': 2, 'c': 3}, IsStrictDict(b=2, c=3).settings(partial=True)),
        ({'a': 1, 'c': 3, 'b': 2}, ~IsStrictDict(b=2, c=3).settings(partial=True)),
    ],
)
def test_is_dict(input_value, expected):
    assert input_value == expected


def test_ne_repr_partial_dict():
    v = IsPartialDict({1: 10, 2: 20})

    with pytest.raises(AssertionError):
        assert 1 == v

    assert str(v) == 'IsPartialDict(1=10, 2=20)'


def test_ne_repr_strict_dict():
    v = IsStrictDict({1: 10, 2: 20})

    with pytest.raises(AssertionError):
        assert 1 == v

    assert str(v) == 'IsStrictDict(1=10, 2=20)'


def test_args_and_kwargs():
    with pytest.raises(TypeError, match='IsDict requires either a single argument or kwargs, not both'):
        IsDict(1, x=4)


def test_multiple_args():
    with pytest.raises(TypeError, match='IsDict expected at most 1 argument, got 2'):
        IsDict(1, 2)


def test_arg_not_dict():
    with pytest.raises(TypeError, match="expected_values must be a dict, got <class 'int'>"):
        IsDict(1)


def test_combine_partial_ignore():
    d = IsPartialDict(a=2, b=2, c=3)
    with pytest.raises(TypeError, match='partial and ignore cannot be used together'):
        d.settings(ignore={1})


def ignore_42(value):
    return value == 42


def test_callable_ignore():
    assert {'a': 1} == IsDict(a=1).settings(ignore=ignore_42)
    assert {'a': 1, 'b': 42} == IsDict(a=1).settings(ignore=ignore_42)
    assert {'a': 1, 'b': 43} != IsDict(a=1).settings(ignore=ignore_42)


@pytest.mark.parametrize(
    'd,expected_repr',
    [
        (IsDict, 'IsDict'),
        (IsDict(), 'IsDict()'),
        (IsPartialDict, 'IsPartialDict'),
        (IsPartialDict(), 'IsPartialDict()'),
        (IsDict().settings(partial=True), 'IsDict[partial=True]()'),
        (IsIgnoreDict(), 'IsIgnoreDict()'),
        (IsIgnoreDict().settings(ignore={7}), 'IsIgnoreDict[ignore={7}]()'),
        (IsIgnoreDict().settings(ignore={None}), 'IsIgnoreDict()'),
        (IsIgnoreDict().settings(ignore=None), 'IsIgnoreDict[ignore=None]()'),
        (IsDict().settings(ignore=ignore_42), 'IsDict[ignore=ignore_42]()'),
        (IsDict().settings(ignore={7}), 'IsDict[ignore={7}]()'),
        (IsDict().settings(ignore={None}), 'IsDict[ignore={None}]()'),
        (IsPartialDict().settings(partial=False), 'IsPartialDict[partial=False]()'),
        (IsStrictDict, 'IsStrictDict'),
        (IsStrictDict(), 'IsStrictDict()'),
        (IsDict().settings(strict=True), 'IsDict[strict=True]()'),
        (IsStrictDict().settings(strict=False), 'IsStrictDict[strict=False]()'),
    ],
)
def test_not_equals_repr(d, expected_repr):
    assert repr(d) == expected_repr


def test_ignore():
    def custom_ignore(v: int) -> bool:
        return v % 2 == 0

    assert {'a': 1, 'b': 2, 'c': 3, 'd': 4} == IsDict(a=1, c=3).settings(ignore=custom_ignore)


def test_ignore_with_is_str():
    api_data = {'id': 123, 'token': 't-abc123', 'dob': None, 'street_address': None}

    token_is_str = IsStr(regex=r't\-.+')
    assert api_data == IsIgnoreDict(id=IsPositiveInt, token=token_is_str)
    assert token_is_str.value == 't-abc123'


def test_unhashable_value():
    a = {'a': 1}
    api_data = {'b': a, 'c': None}
    assert api_data == IsIgnoreDict(b=a)

import pytest

from dirty_equals import AnyThing, Contains, HasLen, IsInt, IsList, IsListOrTuple, IsNegative, IsTuple


@pytest.mark.parametrize(
    'other,dirty',
    [
        ([], IsList),
        ((), IsTuple),
        ([], IsList()),
        ([1], IsList(length=1)),
        ((), IsTuple()),
        ([1, 2, 3], IsList(1, 2, 3)),
        ((1, 2, 3), IsTuple(1, 2, 3)),
        ((1, 2, 3), IsListOrTuple(1, 2, 3)),
        ([1, 2, 3], IsListOrTuple(1, 2, 3)),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=5)),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=(4, 6))),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=[4, 6])),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=(4, ...))),
        ([3, 2, 1], IsList(1, 2, 3, check_order=False)),
        ([{1: 2}, 7], IsList(7, {1: 2}, check_order=False)),
        ([1, 2, 3, 4], IsList(positions={0: 1, 2: 3, -1: 4})),
        ([1, 2, 3], IsList(AnyThing, 2, 3)),
        ([1, 2, 3], IsList(1, 2, IsInt)),
        ([3, 2, 1], IsList(1, 2, IsInt, check_order=False)),
        ([1, 2, 2], IsList(2, 2, 1, check_order=False)),
        ([], HasLen(0)),
        ([1, 2, 3], HasLen(3)),
        ('123', HasLen(3)),
        (b'123', HasLen(3)),
        ({'a': 1, 'b': 2, 'c': 3}, HasLen(3)),
        ([1, 2], HasLen(1, 2)),
        ([1, 2], HasLen(2, 3)),
        ([1, 2, 3], HasLen(2, ...)),
        ([1, 2, 3], HasLen(0, ...)),
        ([1, 2, 3], Contains(1)),
        ([1, 2, 3], Contains(1, 2)),
        ((1, 2, 3), Contains(1)),
        ({1, 2, 3}, Contains(1)),
        ('abc', Contains('b')),
        ({'a': 1, 'b': 2}, Contains('a')),
        ([{'a': 1}, {'b': 2}], Contains({'a': 1})),
    ],
)
def test_dirty_equals(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        ([], IsTuple),
        ((), IsList),
        ([1], IsList),
        ([1, 2, 3], IsTuple(1, 2, 3)),
        ((1, 2, 3), IsList(1, 2, 3)),
        ([1, 2, 3, 4], IsList(1, 2, 3)),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=6)),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=(6, 8))),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=(0, 2))),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=[1, 2])),
        ([1, 2, 3, 4, 5], IsList(1, 2, 3, length=(6, ...))),
        ([3, 2, 1, 4], IsList(1, 2, 3, check_order=False)),
        ([1, 2, 3, 4], IsList(positions={0: 1, 2: 3, -1: 5})),
        ([1, 2, 3], IsList(1, 2, IsNegative)),
        ([1, 2, 2], IsList(1, 2, 3, check_order=False)),
        ([1, 2, 3], IsList(1, 2, 2, check_order=False)),
        ([1], HasLen(0)),
        ([], HasLen(1)),
        ('abc', HasLen(2)),
        ([1, 2, 3], HasLen(1, 2)),
        ([1], HasLen(2, 3)),
        ([1], HasLen(2, ...)),
        (123, HasLen(0, ...)),
        ([1, 2, 3], Contains(10)),
        ([1, 2, 3], Contains(1, 'a')),
        ([1, 2, 3], Contains(1, 'a')),
        ([{'a': 1}, {'b': 2}], Contains({'a': 2})),
        ({1, 2, 3}, Contains({1: 2})),
    ],
)
def test_dirty_not_equals(other, dirty):
    assert other != dirty


def test_args_and_positions():
    with pytest.raises(TypeError, match='IsList requires either args or positions, not both'):
        IsList(1, 2, positions={0: 1})


def test_positions_with_check_order():
    with pytest.raises(TypeError, match='check_order=False is not compatible with positions'):
        IsList(check_order=False, positions={0: 1})


def test_wrong_length_length():
    with pytest.raises(TypeError, match='length must be a tuple of length 2, not 3'):
        IsList(1, 2, length=(1, 2, 3))


@pytest.mark.parametrize(
    'dirty,repr_str',
    [
        (IsList, 'IsList'),
        (IsTuple(1, 2, 3), 'IsTuple(1, 2, 3)'),
        (IsList(positions={1: 10, 2: 20}), 'IsList(positions={1: 10, 2: 20})'),
        (IsTuple(1, 2, 3, length=4), 'IsTuple(1, 2, 3, length=4)'),
        (IsTuple(1, 2, 3, length=(6, ...)), 'IsTuple(1, 2, 3, length=(6, ...))'),
        (IsTuple(1, 2, 3, length=(6, 'x')), 'IsTuple(1, 2, 3, length=(6, ...))'),
        (IsTuple(1, 2, 3, length=(6, 10)), 'IsTuple(1, 2, 3, length=(6, 10))'),
        (IsTuple(1, 2, 3, check_order=False), 'IsTuple(1, 2, 3, check_order=False)'),
        (HasLen(42), 'HasLen(42)'),
        (HasLen(0, ...), 'HasLen(0, ...)'),
    ],
)
def test_repr(dirty, repr_str):
    assert repr(dirty) == repr_str


def test_no_contains_value():
    with pytest.raises(TypeError):
        Contains()

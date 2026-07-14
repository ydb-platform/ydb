import pytest
from deepmerge.strategy.list import ListStrategies
from deepmerge import Merger


@pytest.fixture
def custom_merger():
    return Merger(
        [(list, ListStrategies.strategy_append_unique)],
        [],
        [],
    )


def test_strategy_append_unique(custom_merger):
    base = [1, 3, 2]
    nxt = [3, 5, 4, 1, 2]

    expected = [1, 3, 2, 5, 4]
    actual = custom_merger.merge(base, nxt)
    assert actual == expected


def test_strategy_append_unique_nested_dict(custom_merger):
    """append_unique should work even with unhashable objects
    Like dicts.
    """
    base = [{"bar": ["bob"]}]
    nxt = [{"bar": ["baz"]}]

    result = custom_merger.merge(base, nxt)

    assert result == [{"bar": ["bob"]}, {"bar": ["baz"]}]


def test_strategy_append_similar_dict(custom_merger):
    """append_unique should work for identical dicts,
    regardless of insertion order.
    """
    base = [{"bar": "bob", "foo": "baz"}]
    nxt = [{"x": "y"}, {"foo": "baz", "bar": "bob"}]

    result = custom_merger.merge(base, nxt)

    assert result == [{"bar": "bob", "foo": "baz"}, {"x": "y"}]


def test_strategy_append_unique_keeps_hash_colliding_dicts(custom_merger):
    """append_unique must keep distinct dicts even when their lossy
    "key:value" hash collides (e.g. ``{"a": "1", "b": "2"}`` and
    ``{"a": "1,b:2"}`` both hash to ``"a:1,b:2"``), and must not treat an
    ``int`` value as equal to its ``str`` form.
    """
    assert custom_merger.merge([{"a": "1", "b": "2"}], [{"a": "1,b:2"}]) == [
        {"a": "1", "b": "2"},
        {"a": "1,b:2"},
    ]
    assert custom_merger.merge([{"k": 1}], [{"k": "1"}]) == [{"k": 1}, {"k": "1"}]


def test_strategy_append_unique_hashable_hash_collision(custom_merger):
    """A hashable element must not be considered present just because a
    different element shares its hash (e.g. ``hash(-1) == hash(-2)``).
    """
    assert custom_merger.merge([-1], [-2]) == [-1, -2]

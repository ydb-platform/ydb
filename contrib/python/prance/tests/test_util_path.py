"""Test suite for prance.util.path ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2018 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


import pytest

from prance.util.path import path_set, path_get


def test_get_bad_path():
    # Raise with bad path types
    with pytest.raises(TypeError):
        path_get({}, 42)
    with pytest.raises(TypeError):
        path_get({}, 3.14)

    with pytest.raises(KeyError):
        path_get([], ("a", "b"))


def test_get_value_default():
    value = 42

    # No path can be resolved in a value type
    result = None
    with pytest.raises(TypeError):
        result = path_get(value, ("foo", "bar"), 123)
    assert result is None

    # However, we can resolve zero length paths
    result = path_get(value, (), 123)
    assert 42 == result
    result = path_get(None, (), 123)
    assert 123 == result

    # Also we can resolve None-type paths
    result = path_get(value, None, 321)
    assert 42 == result
    result = path_get(None, None, 321)
    assert 321 == result


def test_get_value_no_default():
    value = 42

    # No path can be resolved in a value type
    result = 666
    with pytest.raises(TypeError):
        result = path_get(value, ("foo", "bar"))
    assert result == 666

    # However, we can resolve zero length paths
    result = path_get(value, ())
    assert 42 == result
    result = path_get(None, ())
    assert result is None

    # Also we can resolve None-type paths
    result = path_get(value, None)
    assert 42 == result
    result = path_get(None, None)
    assert result is None


def test_get_collection_default():
    value = (1, 2, 3)

    # String paths in a Sequence should raise KeyError
    result = None
    with pytest.raises(KeyError):
        result = path_get(value, ("foo", "bar"), 123)
    assert result is None

    # A numeric path should work, though
    result = path_get(value, (1,), 123)
    assert 2 == result

    # Zero length paths should return the value or default value
    result = path_get(value, (), 123)
    assert (1, 2, 3) == result
    result = path_get(None, (), 123)
    assert 123 == result

    # And None paths as well
    result = path_get(value, None, 321)
    assert (1, 2, 3) == result
    result = path_get(None, None, 321)
    assert 321 == result


def test_get_collection_no_default():
    value = (1, 2, 3)

    # String paths in a Sequence should raise KeyError
    result = None
    with pytest.raises(KeyError):
        result = path_get(value, ("foo", "bar"))
    assert result is None

    # A numeric path should work, though
    result = path_get(value, (1,))
    assert 2 == result

    # Zero length paths should return the value or default value
    result = path_get(value, ())
    assert (1, 2, 3) == result
    result = path_get(None, ())
    assert result is None

    # And None paths as well
    result = path_get(value, None)
    assert (1, 2, 3) == result
    result = path_get(None, None)
    assert result is None


def test_get_mapping_default():
    value = {"foo": 1, "bar": 2, 3: 3}

    # String paths should work in a Mapping
    result = path_get(value, ("foo",), 123)
    assert 1 == result

    # So should numeric keys
    result = path_get(value, (3,), 123)
    assert 3 == result

    # Zero length paths should return the value or default value
    result = path_get(value, (), 123)
    assert {"foo": 1, "bar": 2, 3: 3} == result
    result = path_get(None, (), 123)
    assert 123 == result

    # And None paths as well
    result = path_get(value, None, 321)
    assert {"foo": 1, "bar": 2, 3: 3} == result
    result = path_get(None, None, 321)
    assert 321 == result


def test_set_bad_path():
    # Raise with bad path types
    with pytest.raises(TypeError):
        path_set({}, 42, None)
    with pytest.raises(TypeError):
        path_set({}, 3.14, None)

    with pytest.raises(KeyError):
        path_set([], ("a", "b"), None)


def test_set_value():
    value = 42

    # Setting on a value type with a path should raise...
    with pytest.raises(TypeError):
        path_set(42, ("foo", "bar"), "something")
    with pytest.raises(TypeError):
        path_set(42, (0, 1), "something")

    # ... and without a path or an empty path should raise an error
    with pytest.raises(KeyError):
        path_set(42, (), "something")
    with pytest.raises(TypeError):
        path_set(42, None, "something")


def test_set_sequence_no_create():
    # Tuples are not mutable - this must raise
    result = None
    with pytest.raises(TypeError):
        result = path_set((42,), (0,), "something")

    # Lists are mutable - this must work
    result = path_set(
        [
            42,
        ],
        (0,),
        "something",
    )
    assert ["something"] == result

    # Try nested paths
    result = path_set([42, [1, 2]], (1, 0), "something")
    assert [42, ["something", 2]] == result

    # Bad paths must raise
    with pytest.raises(IndexError):
        path_set([], (0,), "something")
    with pytest.raises(IndexError):
        path_set([[]], (0, 1), "something")


def test_set_sequence_create():
    # If we want to create entries, then mutable sequences should have further
    # sequences added to it.
    result = path_set([], (1, 0), "something", create=True)
    assert [None, ["something"]] == result


def test_set_mapping_no_create():
    # Setting existing keys must work
    result = path_set({"foo": "bar"}, ("foo",), "new")
    assert {"foo": "new"} == result

    # Nested mappings should work just as well.
    result = path_set(
        {"foo": {"bar": "baz"}},
        (
            "foo",
            "bar",
        ),
        "new",
    )
    assert {"foo": {"bar": "new"}} == result

    # Bad paths must raise
    with pytest.raises(KeyError):
        path_set({}, ("foo",), "something")
    with pytest.raises(KeyError):
        path_set(
            {"foo": {}},
            (
                "foo",
                "bar",
            ),
            "something",
        )


def test_set_mapping_create():
    # Setting missing keys must work
    result = path_set({}, ("foo",), "bar", create=True)
    assert {"foo": "bar"} == result

    # Recursive setting must work
    result = path_set(
        {},
        (
            "foo",
            "bar",
        ),
        "baz",
        create=True,
    )
    assert {"foo": {"bar": "baz"}} == result


def test_set_mixed_no_create():
    # Sequence in Mapping must work
    result = path_set({"foo": [0]}, ("foo", 0), 42)
    assert {"foo": [42]} == result

    # And Mapping in Sequence likewise
    result = path_set([{"foo": "bar"}], (0, "foo"), 42)
    assert [{"foo": 42}] == result


def test_set_mixed_create():
    # Creation should work based on the path element types
    result = path_set({}, ("foo", 0), 42, create=True)
    assert {"foo": [42]} == result

    result = path_set([], (0, "foo"), 42, create=True)
    assert [{"foo": 42}] == result

    # Create int keys in dict
    result = path_set({}, (0,), 42, create=True)
    assert {0: 42} == result


def test_set_mixed_create_no_fill():
    # Creation should not add items that are not necessary
    base = {"foo": [123]}

    result = path_set(base, ("foo", 0), 42, create=True)
    assert {"foo": [42]} == result


def test_get_informative_key_error():
    base = {"foo": {"bar": [123]}}

    # Match that the object being examining has its path printed, as
    # well as that the key is included.
    with pytest.raises(KeyError, match=r'.*"/".*asdf'):
        path_get(base, ("asdf",))

    with pytest.raises(KeyError, match=r'.*"/foo".*asdf'):
        path_get(base, ("foo", "asdf"))


def test_get_informative_index_error():
    base = {"foo": {"bar": [123]}}

    with pytest.raises(IndexError, match=r'.*"/foo/bar".*123'):
        path_get(base, ("foo", "bar", 123))

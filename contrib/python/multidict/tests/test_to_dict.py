"""Test to_dict functionality for all multidict types."""

from collections.abc import Iterator

import pytest

from multidict import (
    CIMultiDict,
    CIMultiDictProxy,
    MultiDict,
    MultiDictProxy,
    MultiMapping,
)


@pytest.mark.parametrize(
    ("items", "expected"),
    (
        pytest.param(
            [("a", "1"), ("b", "2")],
            {"a": ["1"], "b": ["2"]},
            id="unique-keys",
        ),
        pytest.param(
            [("a", "1"), ("b", "2"), ("a", "3")],
            {"a": ["1", "3"], "b": ["2"]},
            id="multi-values",
        ),
    ),
)
def test_to_dict(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
    items: list[tuple[str, str]],
    expected: dict[str, list[str]],
) -> None:
    d = any_multidict_class(items)
    assert d.to_dict() == expected


def test_to_dict_empty(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
) -> None:
    d = any_multidict_class()
    assert d.to_dict() == {}


def test_to_dict_returns_new_dict(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
) -> None:
    d = any_multidict_class([("a", "1")])
    result1 = d.to_dict()
    result2 = d.to_dict()
    assert result1 == result2
    assert result1 is not result2


def test_to_dict_list_is_fresh(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
) -> None:
    d = any_multidict_class([("a", "1")])
    result1 = d.to_dict()
    result2 = d.to_dict()
    assert result1["a"] is not result2["a"]


def test_to_dict_order_preservation(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
) -> None:
    d = any_multidict_class([("x", "3"), ("x", "1"), ("x", "2")])
    assert d.to_dict()["x"] == ["3", "1", "2"]


def test_to_dict_large_data(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
) -> None:
    items = [(f"key{i % 100}", str(i)) for i in range(10000)]
    d = any_multidict_class(items)
    result = d.to_dict()
    assert len(result) == 100
    assert all(len(v) == 100 for v in result.values())


def test_to_dict_mixed_value_types(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
) -> None:
    d = any_multidict_class([("a", "1"), ("a", "two"), ("b", "3.14")])
    result = d.to_dict()
    assert result["a"] == ["1", "two"]
    assert result["b"] == ["3.14"]


def test_to_dict_case_insensitive_grouping(
    case_insensitive_multidict_class: type[CIMultiDict[str]],
) -> None:
    """Every spelling groups under the first one seen."""
    d = case_insensitive_multidict_class([("A", "1"), ("a", "2"), ("B", "3")])
    result = d.to_dict()
    assert len(result) == 2
    key_a = next(k for k in result if k.lower() == "a")
    key_b = next(k for k in result if k.lower() == "b")
    assert result[key_a] == ["1", "2"]
    assert result[key_b] == ["3"]


def test_to_dict_proxy(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]] | type[CIMultiDictProxy[str]],
) -> None:
    md = any_multidict_class([("a", "1"), ("b", "2"), ("a", "3")])
    proxy = any_multidict_proxy_class(md)
    assert proxy.to_dict() == {"a": ["1", "3"], "b": ["2"]}


def test_to_dict_proxy_mutation_isolation(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]] | type[CIMultiDictProxy[str]],
) -> None:
    """The returned lists are copies, not the multidict's own storage."""
    md = any_multidict_class([("a", "1")])
    proxy = any_multidict_proxy_class(md)
    result = proxy.to_dict()
    result["a"].append("999")
    assert proxy.getall("a") == ["1"]


def test_to_dict_ci_proxy_case_insensitive_grouping(
    case_insensitive_multidict_class: type[CIMultiDict[str]],
    case_insensitive_multidict_proxy_class: type[CIMultiDictProxy[str]],
) -> None:
    md = case_insensitive_multidict_class([("A", "1"), ("a", "2"), ("B", "3")])
    proxy = case_insensitive_multidict_proxy_class(md)
    result = proxy.to_dict()
    assert len(result) == 2
    key_a = next(k for k in result if k.lower() == "a")
    key_b = next(k for k in result if k.lower() == "b")
    assert result[key_a] == ["1", "2"]
    assert result[key_b] == ["3"]


def test_to_dict_restores_lookups(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
) -> None:
    """The walk marks each entry it collects; every mark must be cleared.

    A mark left behind reads as a different hash, so the key it belongs to
    would go missing from a later lookup.
    """
    d = any_multidict_class([("a", "1"), ("b", "2"), ("a", "3")])
    assert d.to_dict() == {"a": ["1", "3"], "b": ["2"]}

    assert d.getall("a") == ["1", "3"]
    assert d.getall("b") == ["2"]
    assert list(d.items()) == [("a", "1"), ("b", "2"), ("a", "3")]
    assert d.to_dict() == {"a": ["1", "3"], "b": ["2"]}


def test_to_dict_skips_deleted_entries(
    any_multidict_class: type[MultiDict[str]] | type[CIMultiDict[str]],
) -> None:
    d = any_multidict_class([("a", "1"), ("b", "2"), ("a", "3")])
    del d["b"]
    assert d.to_dict() == {"a": ["1", "3"]}


class _PairsMultiMapping(MultiMapping[str]):
    """A MultiMapping from outside this project, which the ABC has to serve."""

    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        self._pairs = pairs

    def __getitem__(self, key: str) -> str:
        return self.getone(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _ in self._pairs)

    def __len__(self) -> int:
        return len(self._pairs)

    def getall(self, key: str, default: object = None) -> list[str]:
        values = [value for pair_key, value in self._pairs if pair_key == key]
        if not values and isinstance(default, list):
            return default
        return values

    def getone(self, key: str, default: object = None) -> str:
        values = self.getall(key)
        if not values and isinstance(default, str):
            return default
        return values[0]


def test_to_dict_abc_default() -> None:
    """The ABC supplies to_dict() so subclasses outside this project get one."""
    m = _PairsMultiMapping([("a", "1"), ("b", "2"), ("a", "3")])

    assert m.to_dict() == {"a": ["1", "3"], "b": ["2"]}

    assert len(m) == 3
    assert list(m) == ["a", "b", "a"]
    assert m["a"] == "1"
    assert m.getone("a") == "1"
    assert m.getone("zz", "fallback") == "fallback"
    assert m.getall("a") == ["1", "3"]
    assert m.getall("zz", ["fallback"]) == ["fallback"]


@pytest.mark.c_extension
def test_to_dict_key_hash_sees_every_key() -> None:
    """Nothing is left marked while the keys are hashed.

    The walk marks each entry it collects, and a marked entry reads as a
    different hash, so code re-entering the multidict from a key's own
    ``__hash__`` would find the collected keys missing. The marks are
    cleared before any key is built, so it finds them.
    """
    seen: list[list[str]] = []

    class Key(str):
        def __eq__(self, other: object) -> bool:
            return str.__eq__(self, other)

        def __hash__(self) -> int:
            # Only reached from to_dict(): the multidict itself hashes keys
            # through the str hash, not through this.
            seen.append(md.getall("a"))
            return str.__hash__(self)

    assert Key("a") == "a"
    md: MultiDict[str] = MultiDict([(Key("a"), "1"), (Key("a"), "2")])

    assert md.to_dict() == {"a": ["1", "2"]}
    assert seen == [["1", "2"]]


def test_to_dict_refuses_mutation_from_key_hash(
    case_sensitive_multidict_class: type[MultiDict[str]],
) -> None:
    """Mutating from a key's ``__hash__`` is refused, as it is while iterating."""
    armed: list[bool] = []

    class Key(str):
        def __eq__(self, other: object) -> bool:
            return str.__eq__(self, other)

        def __hash__(self) -> int:
            if armed:
                md.add("late", "x")
            return str.__hash__(self)

    assert Key("a") == "a"
    md = case_sensitive_multidict_class([(Key("a"), "1"), (Key("a"), "2"), ("b", "3")])
    armed.append(True)

    with pytest.raises(RuntimeError, match="changed during iteration"):
        md.to_dict()

    armed.clear()
    assert md.getall("a") == ["1", "2"]
    assert md.getall("b") == ["3"]
    assert md.to_dict()["a"] == ["1", "2"]

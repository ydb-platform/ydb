"""Hypothesis property/fuzz tests for MultiDict/CIMultiDict views:
``.items()``, ``.keys()``, ``.values()``.

Runs against both backends and both case-sensitive/case-insensitive classes
via the existing fixtures in ``tests/conftest.py``. See
``tests/test_hypothesis_iters.py`` for the iterator objects themselves.
"""

import sys
from collections.abc import Callable
from typing import Literal

import pytest

pytest.importorskip("hypothesis")

from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402
from hypothesis_helpers import pairs_lists, simple_values, text_keys  # noqa: E402

from multidict import CIMultiDict, MultiDict, MutableMultiMapping  # noqa: E402

if sys.version_info >= (3, 11):
    from typing import assert_never
else:  # pragma: no cover
    # This file only ever runs under >=3.11 (the hypothesis-gil/
    # hypothesis-freethreading CI jobs pin 3.13/3.14t), unlike the
    # multidict package itself, which is tested down to 3.10.
    from typing_extensions import assert_never

pytestmark = pytest.mark.hypothesis

_MD_Classes = type[MultiDict[object]] | type[CIMultiDict[object]]
_Pairs = list[tuple[str, object]]


def _fold_for(any_multidict_class_name: str) -> Callable[[str], str]:
    return str.lower if any_multidict_class_name == "CIMultiDict" else (lambda s: s)


@given(pairs=pairs_lists())
def test_view_len_is_live(any_multidict_class: _MD_Classes, pairs: _Pairs) -> None:
    md = any_multidict_class(pairs)
    items_view = md.items()
    keys_view = md.keys()
    values_view = md.values()

    md.add("__len_marker__", 1)

    assert len(items_view) == len(md)
    assert len(keys_view) == len(md)
    assert len(values_view) == len(md)


@given(pairs=pairs_lists())
def test_forward_iteration_matches_insertion_order(
    any_multidict_class: _MD_Classes, pairs: _Pairs
) -> None:
    md = any_multidict_class(pairs)
    assert [(str(k), v) for k, v in md.items()] == pairs
    assert [str(k) for k in md.keys()] == [str(k) for k, _v in pairs]
    assert list(md.values()) == [v for _k, v in pairs]


@given(pairs=pairs_lists())
def test_reversed_matches_reversed_list(
    any_multidict_class: _MD_Classes, pairs: _Pairs
) -> None:
    md = any_multidict_class(pairs)
    assert list(reversed(md.items())) == list(  # type: ignore[call-overload]
        reversed(list(md.items()))
    )
    assert list(reversed(md.keys())) == list(  # type: ignore[call-overload]
        reversed(list(md.keys()))
    )
    assert list(reversed(md.values())) == list(reversed(list(md.values())))


@given(pairs=pairs_lists())
def test_items_contains_matches_getall(
    any_multidict_class: _MD_Classes, pairs: _Pairs
) -> None:
    md = any_multidict_class(pairs)
    for k, v in pairs:
        assert (k, v) in md.items()
    for k, v in pairs:
        assert v in md.getall(k, [])


@given(pairs=pairs_lists(), other=st.lists(text_keys(), max_size=15))
@settings(max_examples=50)
def test_keys_view_set_algebra_matches_folded_sets(
    any_multidict_class: _MD_Classes,
    any_multidict_class_name: str,
    pairs: _Pairs,
    other: list[str],
) -> None:
    fold = _fold_for(any_multidict_class_name)
    md = any_multidict_class(pairs)
    md_folded = {fold(k) for k in md.keys()}
    other_folded = {fold(k) for k in other}

    assert {fold(k) for k in (md.keys() & other)} == md_folded & other_folded
    assert {fold(k) for k in (md.keys() | other)} == md_folded | other_folded
    assert {fold(k) for k in (md.keys() - other)} == md_folded - other_folded
    assert {fold(k) for k in (md.keys() ^ other)} == md_folded ^ other_folded
    assert md.keys().isdisjoint(other) == md_folded.isdisjoint(other_folded)


@given(
    pairs=pairs_lists(),
    other=st.lists(st.tuples(text_keys(), simple_values()), max_size=15),
)
@settings(max_examples=50)
def test_items_view_set_algebra_matches_folded_sets(
    any_multidict_class: _MD_Classes,
    any_multidict_class_name: str,
    pairs: _Pairs,
    other: list[tuple[str, object]],
) -> None:
    fold = _fold_for(any_multidict_class_name)
    md = any_multidict_class(pairs)
    md_folded = {(fold(k), v) for k, v in md.items()}
    other_folded = {(fold(k), v) for k, v in other}

    assert {(fold(k), v) for k, v in (md.items() & other)} == md_folded & other_folded
    assert {(fold(k), v) for k, v in (md.items() | other)} == md_folded | other_folded
    assert {(fold(k), v) for k, v in (md.items() - other)} == md_folded - other_folded
    assert {(fold(k), v) for k, v in (md.items() ^ other)} == md_folded ^ other_folded
    assert md.items().isdisjoint(other) == md_folded.isdisjoint(other_folded)


_Mutation = Literal["add", "setitem", "delitem", "clear", "popone"]
_MUTATIONS: tuple[_Mutation, ...] = ("add", "setitem", "delitem", "clear", "popone")

# Deliberately never one of `pairs`'s own keys: mutating an unrelated marker
# key (rather than a key already in `pairs`) means the still-unconsumed
# `pairs` entries the iterator hasn't reached yet are never themselves
# touched, so the guard always has a genuine live entry left to check
# against. Longer than `text_keys()`'s max_size so it can never collide with
# a generated key.
_MUTATION_MARKER_KEY = "__iteration_mutation_marker_key__"


def _apply_mutation(md: MutableMultiMapping[object], mutation: _Mutation) -> None:
    match mutation:
        case "add":
            md.add(_MUTATION_MARKER_KEY, "mutated")
        case "setitem":
            md[_MUTATION_MARKER_KEY] = "mutated"
        case "delitem":
            del md[_MUTATION_MARKER_KEY]
        case "clear":
            md.clear()
        case "popone":
            md.popone(_MUTATION_MARKER_KEY, None)
        case _:  # pragma: no cover
            assert_never(mutation)


def test_apply_mutation_covers_every_mutation(
    any_multidict_class: _MD_Classes,
) -> None:
    """Deterministic coverage for every `_apply_mutation` branch: which one
    `test_view_mutation_during_iteration_raises` below exercises on a given
    run depends on Hypothesis's draws, not on a fixed, always-covered set."""
    for mutation in _MUTATIONS:
        md = any_multidict_class([("a", 1)])
        md.add(_MUTATION_MARKER_KEY, "initial")
        _apply_mutation(md, mutation)


@given(
    pairs=pairs_lists(min_size=2), mutation=st.sampled_from(_MUTATIONS), data=st.data()
)
@settings(max_examples=50)
def test_view_mutation_during_iteration_raises(
    any_multidict_class: _MD_Classes,
    pairs: _Pairs,
    mutation: _Mutation,
    data: st.DataObject,
) -> None:
    md = any_multidict_class(pairs)
    md.add(_MUTATION_MARKER_KEY, "initial")
    it = iter(md.items())
    # `n` must be >=1 so the iterator has already pulled its first element
    # (and so pinned itself to the pre-mutation entries) before the mutation
    # happens, and must leave at least one `pairs` entry unconsumed (`pairs`
    # itself is never touched, only the marker) so the guard always has a
    # genuine, untouched entry left to check afterwards.
    n = data.draw(st.integers(min_value=1, max_value=len(pairs) - 1))
    for _ in range(n):
        next(it)

    _apply_mutation(md, mutation)

    with pytest.raises(RuntimeError):
        while True:
            next(it)

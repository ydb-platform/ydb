"""Hypothesis property/fuzz tests for the iterator objects returned by
``iter(md)``, ``iter(md.keys())``, ``iter(md.items())``, ``iter(md.values())``.

Distinct from ``tests/test_hypothesis_views.py``, which covers the view
containers themselves (``.items()``/``.keys()``/``.values()`` as objects,
their ``__len__``/set algebra). This file focuses on the iterator protocol:
``__length_hint__``, determinism, and the mutation-during-iteration guard,
fuzzing far more mutation-kind/timing combinations than the fixed cases in
``tests/test_guard.py``.
"""

import sys
from collections.abc import Callable, Iterator
from typing import Literal

import pytest

pytest.importorskip("hypothesis")

from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402
from hypothesis_helpers import pairs_lists  # noqa: E402

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
_IterFactory = Callable[[MutableMultiMapping[object]], Iterator[object]]

_ITER_FACTORIES: dict[str, _IterFactory] = {
    "raw": iter,
    "keys": lambda md: iter(md.keys()),
    "items": lambda md: iter(md.items()),
    "values": lambda md: iter(md.values()),
}


@given(pairs=pairs_lists(min_size=1))
def test_length_hint_matches_creation_size(
    any_multidict_class: _MD_Classes, pairs: _Pairs
) -> None:
    md = any_multidict_class(pairs)
    for factory in _ITER_FACTORIES.values():
        it = factory(md)
        assert it.__length_hint__() == len(pairs)  # type: ignore[attr-defined]
        next(it)
        # Not required to decrement exactly, but must stay sane.
        assert it.__length_hint__() >= 0  # type: ignore[attr-defined]


@given(pairs=pairs_lists())
def test_unmutated_iteration_is_deterministic(
    any_multidict_class: _MD_Classes, pairs: _Pairs
) -> None:
    md = any_multidict_class(pairs)
    for factory in _ITER_FACTORIES.values():
        first_pass = list(factory(md))
        second_pass = list(factory(md))
        assert first_pass == second_pass


@given(pairs=pairs_lists())
def test_independent_iterators_agree(
    any_multidict_class: _MD_Classes, pairs: _Pairs
) -> None:
    md = any_multidict_class(pairs)
    for factory in _ITER_FACTORIES.values():
        it1 = factory(md)
        it2 = factory(md)
        assert list(it1) == list(it2)


@given(pairs=pairs_lists())
def test_reversed_iterator_matches_reversed_list(
    any_multidict_class: _MD_Classes, pairs: _Pairs
) -> None:
    md = any_multidict_class(pairs)
    # `MultiDict` itself has no `__reversed__` (only its views do).
    assert list(reversed(md.keys())) == list(  # type: ignore[call-overload]
        reversed(list(md.keys()))
    )
    assert list(reversed(md.items())) == list(  # type: ignore[call-overload]
        reversed(list(md.items()))
    )
    assert list(reversed(md.values())) == list(reversed(list(md.values())))


_Mutation = Literal[
    "add",
    "setitem",
    "delitem",
    "clear",
    "popone",
    "popall",
    "update",
    "extend",
    "merge",
    "setdefault_new_key",
]
_MUTATIONS: tuple[_Mutation, ...] = (
    "add",
    "setitem",
    "delitem",
    "clear",
    "popone",
    "popall",
    "update",
    "extend",
    "merge",
    "setdefault_new_key",
)

# See the identical marker technique (and the "why" in detail) in
# tests/test_hypothesis_views.py::test_view_mutation_during_iteration_raises:
# mutating this dedicated key, never one of `pairs`'s own, guarantees `pairs`
# entries are never themselves touched by the mutation.
_MUTATION_MARKER_KEY = "__iters_mutation_marker_key__"


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
        case "popall":
            md.popall(_MUTATION_MARKER_KEY, None)
        case "update":
            # A fresh key, not the marker: update() overwriting an *existing*
            # entry's value in place is a separate, narrower case this file
            # doesn't assert about (see the module docstring note below).
            md.update([("__iters_mutation_new_key__", "mutated")])
        case "extend":
            md.extend([("__iters_mutation_new_key__", "mutated")])
        case "merge":
            # Also a fresh key: merge() on an already-present key is a
            # documented no-op, so it wouldn't change anything for the
            # guard to catch.
            md.merge([("__iters_mutation_new_key__", "mutated")])
        case "setdefault_new_key":
            md.setdefault("__another_marker_key__", "mutated")
        case _:  # pragma: no cover
            assert_never(mutation)


def test_apply_mutation_covers_every_mutation() -> None:
    """Deterministic coverage for every `_apply_mutation` branch: which one
    `test_iterator_raises_on_mutation` below exercises on a given run
    depends on Hypothesis's draws, not on a fixed, always-covered set."""
    for mutation in _MUTATIONS:
        md: MutableMultiMapping[object] = MultiDict([("a", 1)])
        md.add(_MUTATION_MARKER_KEY, "initial")
        _apply_mutation(md, mutation)


@given(
    pairs=pairs_lists(min_size=2),
    kind=st.sampled_from(list(_ITER_FACTORIES)),
    mutation=st.sampled_from(_MUTATIONS),
    data=st.data(),
)
@settings(max_examples=75)
def test_iterator_raises_on_mutation(
    any_multidict_class: _MD_Classes,
    pairs: _Pairs,
    kind: str,
    mutation: _Mutation,
    data: st.DataObject,
) -> None:
    md = any_multidict_class(pairs)
    md.add(_MUTATION_MARKER_KEY, "initial")
    it = _ITER_FACTORIES[kind](md)
    # See test_hypothesis_views.py: >=1 so the iterator has already started
    # (pinning itself to the pre-mutation state), and <=len(pairs)-1 so a
    # genuine untouched `pairs` entry always remains for the guard to find.
    n = data.draw(st.integers(min_value=1, max_value=len(pairs) - 1))
    for _ in range(n):
        next(it)

    _apply_mutation(md, mutation)

    with pytest.raises(RuntimeError):
        while True:
            next(it)

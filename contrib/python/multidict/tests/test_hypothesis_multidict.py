"""Hypothesis property/fuzz tests for MultiDict/CIMultiDict/proxy semantics.

Runs against both the C-extension and pure-Python backends, and both the
case-sensitive and case-insensitive classes, via the existing
``any_multidict_class``-style fixtures in ``tests/conftest.py``.
"""

import pickle

import pytest

pytest.importorskip("hypothesis")

from hypothesis import assume, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402
from hypothesis.stateful import (  # noqa: E402
    RuleBasedStateMachine,
    invariant,
    rule,
    run_state_machine_as_test,
)
from hypothesis_helpers import (  # noqa: E402
    MultiDictModel,
    case_variant,
    pairs_lists,
    simple_values,
    text_keys,
)

from multidict import (  # noqa: E402
    CIMultiDict,
    MultiDict,
    MultiDictProxy,
    MutableMultiMapping,
)

pytestmark = pytest.mark.hypothesis

_MD_Classes = type[MultiDict[object]] | type[CIMultiDict[object]]
_Pairs = list[tuple[str, object]]

# A small, colliding key pool so the stateful machine actually exercises
# duplicate-key handling instead of drawing 25 essentially-unique keys.
_STATEFUL_KEYS = st.sampled_from(["a", "b", "c", "A", "B", "aa", "", "k1", "k2"])
_STATEFUL_VALUES = st.integers(min_value=0, max_value=50)
_STATEFUL_PAIRS = st.lists(st.tuples(_STATEFUL_KEYS, _STATEFUL_VALUES), max_size=5)


def _make_state_machine(cls: _MD_Classes, is_ci: bool) -> type[RuleBasedStateMachine]:
    fold = str.lower if is_ci else (lambda s: s)

    class MultiDictStateMachine(RuleBasedStateMachine):
        def __init__(self) -> None:
            super().__init__()
            self.sut: MutableMultiMapping[object] = cls()
            self.model = MultiDictModel(fold=fold)

        @rule(key=_STATEFUL_KEYS, value=_STATEFUL_VALUES)
        def add(self, key: str, value: int) -> None:
            self.sut.add(key, value)
            self.model.add(key, value)

        @rule(key=_STATEFUL_KEYS, value=_STATEFUL_VALUES)
        def setitem(self, key: str, value: int) -> None:
            self.sut[key] = value
            self.model.setitem(key, value)

        @rule(key=_STATEFUL_KEYS)
        def delitem(self, key: str) -> None:
            sut_raised = model_raised = False
            try:
                del self.sut[key]
            except KeyError:
                sut_raised = True
            try:
                self.model.delitem(key)
            except KeyError:
                model_raised = True
            assert sut_raised == model_raised

        @rule(key=_STATEFUL_KEYS)
        def popone(self, key: str) -> None:
            sut_raised = model_raised = False
            sv = mv = None
            try:
                sv = self.sut.popone(key)
            except KeyError:
                sut_raised = True
            try:
                mv = self.model.popone(key)
            except KeyError:
                model_raised = True
            assert sut_raised == model_raised
            if not sut_raised:
                assert sv == mv

        @rule(key=_STATEFUL_KEYS)
        def popall(self, key: str) -> None:
            sut_raised = model_raised = False
            sv = mv = None
            try:
                sv = self.sut.popall(key)
            except KeyError:
                sut_raised = True
            try:
                mv = self.model.popall(key)
            except KeyError:
                model_raised = True
            assert sut_raised == model_raised
            if not sut_raised:
                assert sv == mv

        @rule()
        def popitem(self) -> None:
            sut_raised = model_raised = False
            sv = mv = None
            try:
                sv = self.sut.popitem()
            except KeyError:
                sut_raised = True
            try:
                mv = self.model.popitem()
            except KeyError:
                model_raised = True
            assert sut_raised == model_raised
            if not sut_raised:
                assert sv is not None
                assert mv is not None
                assert (str(sv[0]), sv[1]) == (str(mv[0]), mv[1])

        @rule(key=_STATEFUL_KEYS, value=_STATEFUL_VALUES)
        def setdefault(self, key: str, value: int) -> None:
            sv = self.sut.setdefault(key, value)
            mv = self.model.setdefault(key, value)
            assert sv == mv

        @rule(pairs=_STATEFUL_PAIRS)
        def update(self, pairs: list[tuple[str, int]]) -> None:
            self.sut.update(pairs)
            self.model.update(pairs)

        @rule(pairs=_STATEFUL_PAIRS)
        def extend(self, pairs: list[tuple[str, int]]) -> None:
            self.sut.extend(pairs)
            self.model.extend(pairs)

        @rule(pairs=_STATEFUL_PAIRS)
        def merge(self, pairs: list[tuple[str, int]]) -> None:
            self.sut.merge(pairs)
            self.model.merge(pairs)

        @rule()
        def clear(self) -> None:
            self.sut.clear()
            self.model.clear()

        @invariant()
        def matches_model(self) -> None:
            sut_items = [(str(k), v) for k, v in self.sut.items()]
            model_items = [(str(k), v) for k, v in self.model.items()]
            assert sut_items == model_items
            assert len(self.sut) == len(self.model)

    return MultiDictStateMachine


def test_stateful_multidict(
    any_multidict_class: _MD_Classes,
    any_multidict_class_name: str,
) -> None:
    is_ci = any_multidict_class_name == "CIMultiDict"
    machine_cls = _make_state_machine(any_multidict_class, is_ci)
    run_state_machine_as_test(  # type: ignore[no-untyped-call]
        machine_cls,
        settings=settings(max_examples=20, stateful_step_count=25),
    )


# -- Targeted properties -----------------------------------------------


@given(pairs=pairs_lists())
def test_add_always_appends(any_multidict_class: _MD_Classes, pairs: _Pairs) -> None:
    md = any_multidict_class()
    for k, v in pairs:
        md.add(k, v)
    assert len(md) == len(pairs)
    assert [(str(k), v) for k, v in md.items()] == pairs


@given(pairs=pairs_lists(), key=text_keys(), v1=simple_values(), v2=simple_values())
@settings(max_examples=50)
def test_setitem_collapses_duplicates(
    any_multidict_class: _MD_Classes,
    pairs: _Pairs,
    key: str,
    v1: object,
    v2: object,
) -> None:
    md = any_multidict_class()
    md.add(key, v1)
    for k, v in pairs:
        md.add(k, v)
    md.add(key, v1)
    md[key] = v2
    assert md.getall(key) == [v2]


@given(pairs=pairs_lists(), key=text_keys(), default=simple_values())
@settings(max_examples=50)
def test_merge_fills_only_missing_keys(
    any_multidict_class: _MD_Classes,
    pairs: _Pairs,
    key: str,
    default: object,
) -> None:
    md = any_multidict_class(pairs)
    was_present = key in md
    before = md.getall(key, None)
    md.merge([(key, default)])
    if was_present:
        assert md.getall(key, None) == before
    else:
        assert md.getall(key) == [default]


@given(pairs=pairs_lists())
@settings(max_examples=50)
def test_popone_vs_popall(any_multidict_class: _MD_Classes, pairs: _Pairs) -> None:
    keys = {k for k, _ in pairs}
    for key in keys:
        expected = any_multidict_class(pairs).getall(key)
        popped = any_multidict_class(pairs).popall(key)
        assert popped == expected

        one = any_multidict_class(pairs)
        first = one.popone(key)
        assert first == expected[0]
        assert one.getall(key, []) == expected[1:]


@given(pairs=pairs_lists(), key=text_keys(), default=simple_values())
@settings(max_examples=50)
def test_default_sentinel_never_raises(
    any_multidict_class: _MD_Classes,
    pairs: _Pairs,
    key: str,
    default: object,
) -> None:
    md = any_multidict_class(pairs)
    present = key in md
    assert md.getall(key, default) == (md.getall(key) if present else default)
    assert md.getone(key, default) == (md.getone(key) if present else default)
    if not present:
        with pytest.raises(KeyError):
            md.getall(key)
        with pytest.raises(KeyError):
            md.getone(key)


@given(pairs=pairs_lists())
def test_hash_raises_typeerror(any_multidict_class: _MD_Classes, pairs: _Pairs) -> None:
    md = any_multidict_class(pairs)
    with pytest.raises(TypeError):
        hash(md)


@given(pairs=pairs_lists())
@settings(max_examples=50)
def test_eq_is_order_sensitive(
    case_sensitive_multidict_class: type[MultiDict[object]], pairs: _Pairs
) -> None:
    # Case-sensitive only: under CI folding, swapping the spelling of two
    # same-identity keys at symmetric positions changes the raw pair list
    # without changing what `__eq__` actually compares (folded identity),
    # so `pairs != reversed(pairs)` would not reliably imply inequality
    # there.
    reversed_pairs = list(reversed(pairs))
    assume(pairs != reversed_pairs)
    md = case_sensitive_multidict_class(pairs)
    reversed_md = case_sensitive_multidict_class(reversed_pairs)
    assert md != reversed_md


@given(pairs=pairs_lists())
def test_copy_is_independent(any_multidict_class: _MD_Classes, pairs: _Pairs) -> None:
    md = any_multidict_class(pairs)
    copy_ = md.copy()
    assert type(copy_) is type(md)
    assert copy_ == md

    copy_.add("__marker_key__", "__marker_value__")
    assert "__marker_key__" not in md

    md.add("__other_marker__", 1)
    assert "__other_marker__" not in copy_


@given(pairs=pairs_lists())
@settings(max_examples=30)
def test_pickle_roundtrip(
    any_multidict_class: _MD_Classes,
    pairs: _Pairs,
    pickle_protocol: int,
) -> None:
    md = any_multidict_class(pairs)
    restored = pickle.loads(pickle.dumps(md, protocol=pickle_protocol))
    assert restored == md
    assert list(restored.items()) == list(md.items())


@given(pairs=pairs_lists())
def test_constructor_matches_extend(
    any_multidict_class: _MD_Classes, pairs: _Pairs
) -> None:
    from_ctor = any_multidict_class(pairs)
    via_extend = any_multidict_class()
    via_extend.extend(pairs)
    assert list(from_ctor.items()) == list(via_extend.items())


# -- Case-insensitivity properties ---------------------------------------


@given(pairs=pairs_lists(), data=st.data())
@settings(max_examples=50)
def test_ci_lookup_ignores_case(
    case_insensitive_multidict_class: type[CIMultiDict[object]],
    pairs: _Pairs,
    data: st.DataObject,
) -> None:
    assume(bool(pairs))
    md = case_insensitive_multidict_class(pairs)
    key = data.draw(st.sampled_from([k for k, _ in pairs]))
    variant = data.draw(case_variant(key))
    assert variant in md
    assert md.getall(variant) == md.getall(key)
    assert md.getone(variant) == md.getone(key)


@given(s=text_keys())
def test_istr_itself_is_case_sensitive(
    case_insensitive_str_class: type[str], s: str
) -> None:
    upper = s.upper()
    assume(upper != s)
    assert (case_insensitive_str_class(upper) == s) is False


@given(pairs=pairs_lists())
def test_ci_keys_preserve_original_spelling(
    case_insensitive_multidict_class: type[CIMultiDict[object]], pairs: _Pairs
) -> None:
    md = case_insensitive_multidict_class(pairs)
    assert [(str(k), v) for k, v in md.items()] == pairs


# -- Proxy properties ------------------------------------------------------


@given(pairs=pairs_lists(), key=text_keys(), value=simple_values())
def test_proxy_is_a_live_view(
    any_multidict_class: _MD_Classes,
    any_multidict_proxy_class: type[MultiDictProxy[object]],
    pairs: _Pairs,
    key: str,
    value: object,
) -> None:
    md = any_multidict_class(pairs)
    proxy = any_multidict_proxy_class(md)
    md.add(key, value)
    assert list(proxy.items())[-1] == (key, value)
    assert len(proxy) == len(md)


@given(pairs=pairs_lists())
def test_proxy_copy_is_mutable_and_independent(
    any_multidict_class: _MD_Classes,
    any_multidict_proxy_class: type[MultiDictProxy[object]],
    pairs: _Pairs,
) -> None:
    md = any_multidict_class(pairs)
    proxy = any_multidict_proxy_class(md)
    copy_ = proxy.copy()
    assert type(copy_) is any_multidict_class
    copy_.add("__proxy_copy_marker__", 1)
    assert "__proxy_copy_marker__" not in md


@given(pairs=pairs_lists())
def test_proxy_pickle_raises(
    any_multidict_class: _MD_Classes,
    any_multidict_proxy_class: type[MultiDictProxy[object]],
    pairs: _Pairs,
) -> None:
    md = any_multidict_class(pairs)
    proxy = any_multidict_proxy_class(md)
    with pytest.raises(TypeError):
        pickle.dumps(proxy)


@given(pairs=pairs_lists())
def test_ci_proxy_rejects_case_sensitive_source(
    case_sensitive_multidict_class: type[MultiDict[object]],
    case_insensitive_multidict_proxy_class: type[CIMultiDict[object]],
    pairs: _Pairs,
) -> None:
    md = case_sensitive_multidict_class(pairs)
    with pytest.raises(TypeError):
        case_insensitive_multidict_proxy_class(md)

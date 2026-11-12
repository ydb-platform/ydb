"""Shared Hypothesis strategies for the ``test_hypothesis_*`` files.

Not a test module itself (no ``test_`` prefix), same convention as
``tests/gen_pickles.py``.
"""

from collections import deque
from collections.abc import Callable, Iterable

import pytest

pytest.importorskip("hypothesis")

from hypothesis import strategies as st  # noqa: E402

# Kept small: every property test here runs across (at least) the four
# c/py x case-sensitive/case-insensitive fixture combinations, so per-example
# cost multiplies fast.
_MAX_TEXT_SIZE = 12


def text_keys(max_size: int = _MAX_TEXT_SIZE) -> st.SearchStrategy[str]:
    """A plain string usable as a multidict key."""
    return st.text(max_size=max_size)


def simple_values(max_size: int = _MAX_TEXT_SIZE) -> st.SearchStrategy[object]:
    """A value with no special comparison semantics of its own."""
    return st.one_of(
        st.text(max_size=max_size),
        st.integers(min_value=-1000, max_value=1000),
        st.booleans(),
        st.none(),
    )


def key_value_pairs() -> st.SearchStrategy[tuple[str, object]]:
    return st.tuples(text_keys(), simple_values())


def pairs_lists(
    max_size: int = 25, min_size: int = 0
) -> st.SearchStrategy[list[tuple[str, object]]]:
    return st.lists(key_value_pairs(), min_size=min_size, max_size=max_size)


@st.composite
def case_variant(draw: st.DrawFn, key: str) -> str:
    """A string equal to ``key`` under ``str.lower()``, with each alphabetic
    character's case independently randomized."""
    chars = []
    for ch in key:
        candidate = ch.upper() if ch == ch.lower() else ch.lower()
        # Only accept a flip that preserves this character's `str.lower()`
        # identity: e.g. "ss".upper() == "SS" changes length instead of just
        # case, and Greek final sigma "ς".upper() == "Σ", whose own
        # `.lower()` is "σ" (plain sigma), not "ς" -- accepting either would
        # make the variant no longer case-fold to the same value as `key`.
        if candidate.lower() == ch.lower() and draw(st.booleans()):
            ch = candidate
        chars.append(ch)
    return "".join(chars)


_Pair = tuple[str, object]


class MultiDictModel:
    """A minimal reference model of ``MultiDict``/``CIMultiDict`` semantics.

    Backed by a plain ``list`` of ``(key, value)`` pairs plus a case-fold
    function, independent of ``multidict``'s own hash table so it can serve
    as an oracle for it. ``add``/``popone``/``popall``/``popitem``/
    ``setdefault``/``clear`` mirror the documented semantics directly.
    ``update``/``merge`` additionally handle within-call duplicate keys the
    same way ``multidict/_multidict_py.py`` does (verified empirically
    against ``tests/test_update.py``'s cases): each incoming pair for a
    given identity claims the next not-yet-claimed *existing* slot for that
    identity, in order; incoming duplicates beyond the existing count are
    appended; existing duplicates beyond the incoming count are dropped.
    ``merge`` only ever checks identities that existed before the call, so
    within-call incoming duplicates for a brand-new identity are all kept.
    """

    def __init__(
        self,
        fold: Callable[[str], str],
        pairs: Iterable[_Pair] = (),
    ) -> None:
        self._fold = fold
        self.entries: list[_Pair] = list(pairs)

    def _indices(self, key: str) -> list[int]:
        folded = self._fold(key)
        return [i for i, (k, _v) in enumerate(self.entries) if self._fold(k) == folded]

    def items(self) -> list[_Pair]:
        return list(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    def add(self, key: str, value: object) -> None:
        self.entries.append((key, value))

    def setitem(self, key: str, value: object) -> None:
        idxs = self._indices(key)
        if not idxs:
            self.entries.append((key, value))
            return
        first, rest = idxs[0], set(idxs[1:])
        self.entries[first] = (key, value)
        if rest:
            self.entries = [e for i, e in enumerate(self.entries) if i not in rest]

    def delitem(self, key: str) -> None:
        idxs = set(self._indices(key))
        if not idxs:
            raise KeyError(key)
        self.entries = [e for i, e in enumerate(self.entries) if i not in idxs]

    def popone(self, key: str) -> object:
        idxs = self._indices(key)
        if not idxs:
            raise KeyError(key)
        idx = idxs[0]
        value = self.entries[idx][1]
        del self.entries[idx]
        return value

    def popall(self, key: str) -> list[object]:
        idxs = self._indices(key)
        if not idxs:
            raise KeyError(key)
        values = [self.entries[i][1] for i in idxs]
        idx_set = set(idxs)
        self.entries = [e for i, e in enumerate(self.entries) if i not in idx_set]
        return values

    def popitem(self) -> _Pair:
        if not self.entries:
            raise KeyError("empty multidict")
        return self.entries.pop()

    def setdefault(self, key: str, default: object) -> object:
        idxs = self._indices(key)
        if idxs:
            return self.entries[idxs[0]][1]
        self.entries.append((key, default))
        return default

    def update(self, pairs: Iterable[_Pair]) -> None:
        available: dict[str, deque[int]] = {}
        for i, (k, _v) in enumerate(self.entries):
            available.setdefault(self._fold(k), deque()).append(i)

        overwritten: dict[int, _Pair] = {}
        appended: list[_Pair] = []
        touched: set[str] = set()
        for k, v in pairs:
            folded = self._fold(k)
            touched.add(folded)
            queue = available.get(folded)
            if queue:
                overwritten[queue.popleft()] = (k, v)
            else:
                appended.append((k, v))

        # Only identities that appeared at least once in `pairs` have their
        # unconsumed existing duplicates dropped; identities `update()` never
        # saw are left completely untouched.
        dropped = {i for folded in touched for i in available.get(folded, ())}
        new_entries = [
            overwritten.get(i, e)
            for i, e in enumerate(self.entries)
            if i not in dropped
        ]
        new_entries.extend(appended)
        self.entries = new_entries

    def extend(self, pairs: Iterable[_Pair]) -> None:
        self.entries.extend(pairs)

    def merge(self, pairs: Iterable[_Pair]) -> None:
        existing = {self._fold(k) for k, _v in self.entries}
        self.entries.extend((k, v) for k, v in pairs if self._fold(k) not in existing)

    def clear(self) -> None:
        self.entries = []

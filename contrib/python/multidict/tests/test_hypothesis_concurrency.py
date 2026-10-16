"""Hypothesis-fuzzed threaded stress tests.

Generalizes the fixed scenarios in tests/test_free_threading.py and the
concurrency section of tests/test_multidict.py by fuzzing the operation
sequence / operation choice / sizes involved, rather than hand-picking one
scenario. Spawning real OS threads per Hypothesis example is expensive, so
each test caps ``max_examples`` well below the suite's default and keeps
per-example thread/iteration counts modest.

Only the C extension has real GIL-release points inside a single bytecode
that these races exploit (see tests/test_free_threading.py); the pure-Python
build's own free-threading races are exercised separately in
tests/test_multidict.py and are out of scope for this file, except for the
reciprocal-lock-ordering deadlock check, which is specific to pure Python's
hand-rolled ``_PairLock`` (the C extension uses ``Py_BEGIN_CRITICAL_SECTION2``,
which CPython itself guarantees is deadlock-free).
"""

import contextlib
import sys
import threading
from typing import Literal

import pytest

pytest.importorskip("hypothesis")

from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

import multidict._multidict_py as _pure  # noqa: E402
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


def _skip_unless_c_extension(cls: _MD_Classes) -> None:
    if getattr(cls, "__module__", "").endswith("_multidict_py"):
        pytest.skip("Test is only applicable to the C extension")


_gil_build_race_skip = pytest.mark.skipif(
    not _pure._FREE_THREADED,
    reason="the two-lock pairing this test exercises is a no-op without it",
)


# -- General op-sequence race fuzz -----------------------------------------

_Op = Literal[
    "add",
    "setitem",
    "delitem",
    "popone",
    "popall",
    "setdefault",
    "get",
    "items",
    "keys",
    "values",
]
_OPS: tuple[_Op, ...] = (
    "add",
    "setitem",
    "delitem",
    "popone",
    "popall",
    "setdefault",
    "get",
    "items",
    "keys",
    "values",
)


def _run_op(md: MutableMultiMapping[object], op: _Op, key: str) -> None:
    match op:
        case "add":
            md.add(key, 1)
        case "setitem":
            md[key] = 1
        case "delitem":
            with contextlib.suppress(KeyError):
                del md[key]
        case "popone":
            md.popone(key, None)
        case "popall":
            md.popall(key, None)
        case "setdefault":
            md.setdefault(key, 1)
        case "get":
            md.get(key)
        case "items":
            list(md.items())
        case "keys":
            list(md.keys())
        case "values":
            list(md.values())
        case _:  # pragma: no cover
            assert_never(op)


def test_run_op_covers_every_op() -> None:
    """Deterministic coverage for every `_run_op` branch: which one
    `test_concurrent_op_sequence_fuzz` below exercises on a given run
    depends on Hypothesis's draws, not on a fixed, always-covered set."""
    md: MutableMultiMapping[object] = MultiDict((f"k-{i}", i) for i in range(8))
    for op in _OPS:
        _run_op(md, op, "k-0")


@pytest.mark.c_extension
@given(op_sequence=st.lists(st.sampled_from(_OPS), min_size=5, max_size=30))
@settings(max_examples=15, deadline=None)
def test_concurrent_op_sequence_fuzz(
    any_multidict_class: _MD_Classes, op_sequence: list[_Op]
) -> None:
    _skip_unless_c_extension(any_multidict_class)
    md: MutableMultiMapping[object] = any_multidict_class(
        (f"k-{i}", i) for i in range(8)
    )
    errors: list[tuple[int, str, str, str]] = []
    barrier = threading.Barrier(4)

    def worker(worker_id: int) -> None:
        barrier.wait()
        for i, op in enumerate(op_sequence):
            key = f"k-{i % 8}"
            try:
                # "changed during iteration" is expected under contention,
                # but real thread scheduling doesn't guarantee hitting it.
                with contextlib.suppress(RuntimeError):
                    _run_op(md, op, key)
            except Exception as e:  # pragma: no cover
                errors.append((worker_id, op, type(e).__name__, str(e)))

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors


# -- Cross-object race fuzz -------------------------------------------------

_ReadOp = Literal["extend", "update", "merge", "copy"]
_READ_OPS: tuple[_ReadOp, ...] = ("extend", "update", "merge", "copy")


def _read_second(
    cls: _MD_Classes, source: MutableMultiMapping[object], op: _ReadOp
) -> int:
    dst: MutableMultiMapping[object] = cls()
    match op:
        case "extend":
            dst.extend(source)
        case "update":
            dst.update(source)
        case "merge":
            dst.merge(source)
        case "copy":
            dst = cls(source)
        case _:  # pragma: no cover
            assert_never(op)
    return len(dst)


def test_read_second_covers_every_op() -> None:
    """Deterministic coverage for every `_read_second` branch; see
    `test_run_op_covers_every_op` above for why this can't rely on
    `test_cross_object_race_fuzz`'s own Hypothesis draws."""
    source: MutableMultiMapping[object] = MultiDict([("a", 1)])
    for op in _READ_OPS:
        assert _read_second(MultiDict, source, op) == 1


@pytest.mark.c_extension
@given(
    read_ops=st.lists(st.sampled_from(_READ_OPS), min_size=3, max_size=10),
    seed_count=st.integers(min_value=8, max_value=64),
)
@settings(max_examples=10, deadline=None)
def test_cross_object_race_fuzz(
    any_multidict_class: _MD_Classes,
    read_ops: list[_ReadOp],
    seed_count: int,
) -> None:
    _skip_unless_c_extension(any_multidict_class)
    source: MutableMultiMapping[object] = any_multidict_class(
        [(f"init-{i}", i) for i in range(seed_count)]
    )
    sizes: list[int] = []
    errors: list[tuple[str, str, str]] = []
    stop = threading.Event()
    ready = threading.Barrier(3)  # 2 readers + 1 mutator

    def reader() -> None:
        ready.wait()
        for op in read_ops:
            try:
                sizes.append(_read_second(any_multidict_class, source, op))
            except Exception as e:  # pragma: no cover
                errors.append((op, type(e).__name__, str(e)))

    def mutator(tag: str) -> None:
        ready.wait()
        while not stop.is_set():
            for i in range(64):
                source[f"{tag}-{i}"] = i
            for i in range(64):
                del source[f"{tag}-{i}"]

    readers = [threading.Thread(target=reader) for _ in range(2)]
    mutators = [threading.Thread(target=mutator, args=("grow",))]

    for t in readers + mutators:
        t.start()
    for t in readers:
        t.join()
    stop.set()
    for t in mutators:
        t.join()

    assert not errors
    # The seeded keys are never removed, so every snapshot must have seen
    # at least those; surviving without a crash is the main point.
    assert not sizes or min(sizes) >= seed_count


# -- Deadlock fuzz (pure-Python free-threaded only) -------------------------

_ReciprocalOp = Literal["and_items", "or_keys", "sub_keys_items", "isdisjoint_keys"]
_RECIPROCAL_OPS: tuple[_ReciprocalOp, ...] = (
    "and_items",
    "or_keys",
    "sub_keys_items",
    "isdisjoint_keys",
)


def _reciprocal(
    a: MutableMultiMapping[object], b: MutableMultiMapping[object], op: _ReciprocalOp
) -> None:
    match op:
        case "and_items":
            a.items() & b.items()
        case "or_keys":
            a.keys() | b.keys()
        case "sub_keys_items":
            a.keys() - b.items()
        case "isdisjoint_keys":
            a.keys().isdisjoint(b.keys())
        case _:  # pragma: no cover
            assert_never(op)


def test_reciprocal_covers_every_op() -> None:
    """Deterministic coverage for every `_reciprocal` branch: unlike
    `test_reciprocal_ops_no_deadlock` below, this always runs, since that
    test itself is skipped outright on a non-free-threaded build."""
    a: _pure.MultiDict[object] = _pure.MultiDict([("a", 1)])
    b: _pure.MultiDict[object] = _pure.MultiDict([("b", 2)])
    for op in _RECIPROCAL_OPS:
        _reciprocal(a, b, op)


@_gil_build_race_skip
@given(ops=st.lists(st.sampled_from(_RECIPROCAL_OPS), min_size=1, max_size=4))
@settings(max_examples=10, deadline=None)
def test_reciprocal_ops_no_deadlock(ops: list[_ReciprocalOp]) -> None:
    a: _pure.MultiDict[object] = _pure.MultiDict((str(i), i) for i in range(50))
    b: _pure.MultiDict[object] = _pure.MultiDict((str(i), i) for i in range(50, 100))

    def worker1() -> None:
        for _ in range(100):
            for op in ops:
                _reciprocal(a, b, op)

    def worker2() -> None:
        for _ in range(100):
            for op in ops:
                _reciprocal(b, a, op)

    t1 = threading.Thread(target=worker1, daemon=True)
    t2 = threading.Thread(target=worker2, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=20)
    t2.join(timeout=20)
    assert not t1.is_alive(), "worker1 still running: deadlock"
    assert not t2.is_alive(), "worker2 still running: deadlock"

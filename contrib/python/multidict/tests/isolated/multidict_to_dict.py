import gc
import sys
import sysconfig

from multidict import CIMultiDict, MultiDict

# sys.getrefcount is not meaningful under the free-threaded build:
# refcounts are biased per-thread and types may be immortalized, so
# the simple baseline/after comparison below does not apply.
FREETHREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


if __name__ == "__main__":
    if FREETHREADED:
        raise SystemExit(0)

    # Distinct, non-interned keys and non-immortal values: a leaked reference
    # to an interned "a" or to a small int would not move any refcount, which
    # is what an RSS-growth check on such objects silently misses.
    key_a = "leak-key-a".swapcase().swapcase()
    key_b = "leak-key-b".swapcase().swapcase()
    value_1 = object()
    value_2 = object()
    value_3 = object()

    # Value leak, duplicate key: exercises both the PyList_SET_ITEM branch
    # (first value for a key) and the PyList_Append branch (later values).
    md = MultiDict([(key_a, value_1), (key_b, value_2), (key_a, value_3)])
    gc.collect()
    baselines = [sys.getrefcount(v) for v in (value_1, value_2, value_3)]
    for _ in range(1000):
        md.to_dict()
    gc.collect()
    after = [sys.getrefcount(v) for v in (value_1, value_2, value_3)]
    assert after == baselines, (
        f"value leaked: {[a - b for a, b in zip(after, baselines)]}"
    )

    # Key leak, same multidict.
    gc.collect()
    key_baselines = [sys.getrefcount(k) for k in (key_a, key_b)]
    for _ in range(1000):
        md.to_dict()
    gc.collect()
    key_after = [sys.getrefcount(k) for k in (key_a, key_b)]
    assert key_after == key_baselines, (
        f"key leaked: {[a - b for a, b in zip(key_after, key_baselines)]}"
    )

    # CIMultiDict takes the only allocating path: _md_ensure_key() builds a
    # fresh istr and stores it back into the entry.
    ci = CIMultiDict([(key_a, value_1), (key_a.upper(), value_2)])
    gc.collect()
    ci_baselines = [sys.getrefcount(v) for v in (value_1, value_2)]
    for _ in range(1000):
        ci.to_dict()
    gc.collect()
    ci_after = [sys.getrefcount(v) for v in (value_1, value_2)]
    assert ci_after == ci_baselines, (
        f"CI value leaked: {[a - b for a, b in zip(ci_after, ci_baselines)]}"
    )

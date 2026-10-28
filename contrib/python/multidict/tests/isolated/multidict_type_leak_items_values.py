import gc
import sys
import sysconfig

from multidict import MultiDict

# sys.getrefcount is not meaningful under the free-threaded build:
# refcounts are biased per-thread and types may be immortalized, so
# the simple baseline/after comparison below does not apply.
FREETHREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


if __name__ == "__main__":
    if FREETHREADED:
        raise SystemExit(0)

    md = MultiDict([("a", "1"), ("b", "2")])

    # items() / values() iterators each have their own PyType_Spec
    # sharing multidict_iter_dealloc; test them independently so a
    # regression in just one spec's slot table is still caught.
    for view_name in ("items", "values"):
        get_view = getattr(md, view_name)
        iter_type = type(iter(get_view()))
        gc.collect()
        baseline = sys.getrefcount(iter_type)
        for _ in range(1000):
            _it = iter(get_view())
            list(_it)
            del _it
        gc.collect()
        after = sys.getrefcount(iter_type)
        assert after == baseline, (
            f"{view_name} iterator type leaked: {after - baseline}"
        )

    # items() / values() views each have their own PyType_Spec
    # sharing multidict_view_dealloc; same rationale as above.
    for view_name in ("items", "values"):
        get_view = getattr(md, view_name)
        view_type = type(get_view())
        gc.collect()
        baseline = sys.getrefcount(view_type)
        for _ in range(1000):
            _v = get_view()
            del _v
        gc.collect()
        after = sys.getrefcount(view_type)
        assert after == baseline, f"{view_name} view type leaked: {after - baseline}"

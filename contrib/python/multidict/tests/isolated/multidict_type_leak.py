import gc
import sys
import sysconfig

from multidict import CIMultiDict, CIMultiDictProxy, MultiDict, MultiDictProxy, istr

# sys.getrefcount is not meaningful under the free-threaded build:
# refcounts are biased per-thread and types may be immortalized, so
# the simple baseline/after comparison below does not apply.
FREETHREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


if __name__ == "__main__":
    if FREETHREADED:
        raise SystemExit(0)

    md = MultiDict([("a", "1"), ("b", "2")])

    # Iterator type leak
    iter_type = type(iter(md.keys()))
    gc.collect()
    baseline = sys.getrefcount(iter_type)
    for _ in range(1000):
        _it = iter(md.keys())
        list(_it)
        del _it
    gc.collect()
    after = sys.getrefcount(iter_type)
    assert after == baseline, f"iterator type leaked: {after - baseline}"

    # MultiDict type leak
    for md_type in (MultiDict, CIMultiDict):
        gc.collect()
        baseline = sys.getrefcount(md_type)
        for _ in range(1000):
            md_type()
        gc.collect()
        after = sys.getrefcount(md_type)
        assert after == baseline, f"{md_type.__name__} type leaked: {after - baseline}"

    # MultiDictProxy type leak
    for proxy_type, proxy_md in (
        (MultiDictProxy, md),
        (CIMultiDictProxy, CIMultiDict(md)),
    ):
        gc.collect()
        baseline = sys.getrefcount(proxy_type)
        for _ in range(1000):
            proxy_type(proxy_md)
        gc.collect()
        after = sys.getrefcount(proxy_type)
        assert after == baseline, (
            f"{proxy_type.__name__} type leaked: {after - baseline}"
        )

    # View type leak
    view_type = type(md.keys())
    gc.collect()
    baseline = sys.getrefcount(view_type)
    for _ in range(1000):
        _v = md.keys()
        del _v
    gc.collect()
    after = sys.getrefcount(view_type)
    assert after == baseline, f"view type leaked: {after - baseline}"

    # istr type leak
    gc.collect()
    baseline = sys.getrefcount(istr)
    for _ in range(1000):
        _s = istr("hello")
        del _s
    gc.collect()
    after = sys.getrefcount(istr)
    assert after == baseline, f"istr type leaked: {after - baseline}"

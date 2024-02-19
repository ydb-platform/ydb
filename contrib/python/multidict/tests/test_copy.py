import copy


def test_copy(any_multidict_class):
    d = any_multidict_class()
    d["foo"] = 6
    d2 = d.copy()
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7


def test_copy_proxy(any_multidict_class, any_multidict_proxy_class):
    d = any_multidict_class()
    d["foo"] = 6
    p = any_multidict_proxy_class(d)
    d2 = p.copy()
    d2["foo"] = 7
    assert d["foo"] == 6
    assert p["foo"] == 6
    assert d2["foo"] == 7


def test_copy_std_copy(any_multidict_class):
    d = any_multidict_class()
    d["foo"] = 6
    d2 = copy.copy(d)
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7


def test_ci_multidict_clone(any_multidict_class):
    d = any_multidict_class(foo=6)
    d2 = any_multidict_class(d)
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7

import copy

import pytest

from multidict._compat import USE_EXTENSIONS
from multidict._multidict_py import CIMultiDict as PyCIMultiDict
from multidict._multidict_py import CIMultiDictProxy as PyCIMultiDictProxy
from multidict._multidict_py import MultiDict as PyMultiDict  # noqa: E402
from multidict._multidict_py import MultiDictProxy as PyMultiDictProxy

if USE_EXTENSIONS:
    from multidict._multidict import (  # type: ignore
        CIMultiDict,
        CIMultiDictProxy,
        MultiDict,
        MultiDictProxy,
    )


@pytest.fixture(
    params=([MultiDict, CIMultiDict] if USE_EXTENSIONS else [])
    + [PyMultiDict, PyCIMultiDict],
    ids=(["MultiDict", "CIMultiDict"] if USE_EXTENSIONS else [])
    + ["PyMultiDict", "PyCIMultiDict"],
)
def cls(request):
    return request.param


@pytest.fixture(
    params=(
        [(MultiDictProxy, MultiDict), (CIMultiDictProxy, CIMultiDict)]
        if USE_EXTENSIONS
        else []
    )
    + [(PyMultiDictProxy, PyMultiDict), (PyCIMultiDictProxy, PyCIMultiDict)],
    ids=(["MultiDictProxy", "CIMultiDictProxy"] if USE_EXTENSIONS else [])
    + ["PyMultiDictProxy", "PyCIMultiDictProxy"],
)
def proxy_classes(request):
    return request.param


def test_copy(cls):
    d = cls()
    d["foo"] = 6
    d2 = d.copy()
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7


def test_copy_proxy(proxy_classes):
    proxy_cls, dict_cls = proxy_classes
    d = dict_cls()
    d["foo"] = 6
    p = proxy_cls(d)
    d2 = p.copy()
    d2["foo"] = 7
    assert d["foo"] == 6
    assert p["foo"] == 6
    assert d2["foo"] == 7


def test_copy_std_copy(cls):
    d = cls()
    d["foo"] = 6
    d2 = copy.copy(d)
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7


def test_ci_multidict_clone(cls):
    d = cls(foo=6)
    d2 = cls(d)
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7

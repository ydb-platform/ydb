import pickle
from pathlib import Path

import pytest

from multidict._compat import USE_EXTENSIONS
from multidict._multidict_py import CIMultiDict as PyCIMultiDict
from multidict._multidict_py import CIMultiDictProxy as PyCIMultiDictProxy
from multidict._multidict_py import MultiDict as PyMultiDict  # noqa: E402
from multidict._multidict_py import MultiDictProxy as PyMultiDictProxy

import yatest.common

if USE_EXTENSIONS:
    from multidict._multidict import (  # type: ignore
        CIMultiDict,
        CIMultiDictProxy,
        MultiDict,
        MultiDictProxy,
    )


here = Path(yatest.common.test_source_path()).resolve()


@pytest.fixture(
    params=(["MultiDict", "CIMultiDict"] if USE_EXTENSIONS else [])
    + ["PyMultiDict", "PyCIMultiDict"]
)
def cls_name(request):
    return request.param


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


def test_pickle(cls, pickle_protocol):
    d = cls([("a", 1), ("a", 2)])
    pbytes = pickle.dumps(d, pickle_protocol)
    obj = pickle.loads(pbytes)
    assert d == obj
    assert isinstance(obj, cls)


def test_pickle_proxy(proxy_classes):
    proxy_cls, dict_cls = proxy_classes
    d = dict_cls([("a", 1), ("a", 2)])
    proxy = proxy_cls(d)
    with pytest.raises(TypeError):
        pickle.dumps(proxy)


def test_load_from_file(pickle_protocol, cls_name):
    cls = globals()[cls_name]
    d = cls([("a", 1), ("a", 2)])
    fname = "{}.pickle.{}".format(cls_name.lower(), pickle_protocol)
    p = here / fname
    with p.open("rb") as f:
        obj = pickle.load(f)
    assert d == obj
    assert isinstance(obj, cls)

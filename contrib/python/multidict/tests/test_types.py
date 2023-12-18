import sys
import types

import pytest


def test_proxies(_multidict):
    assert issubclass(_multidict.CIMultiDictProxy, _multidict.MultiDictProxy)


def test_dicts(_multidict):
    assert issubclass(_multidict.CIMultiDict, _multidict.MultiDict)


def test_proxy_not_inherited_from_dict(_multidict):
    assert not issubclass(_multidict.MultiDictProxy, _multidict.MultiDict)


def test_dict_not_inherited_from_proxy(_multidict):
    assert not issubclass(_multidict.MultiDict, _multidict.MultiDictProxy)


def test_multidict_proxy_copy_type(_multidict):
    d = _multidict.MultiDict(key="val")
    p = _multidict.MultiDictProxy(d)
    assert isinstance(p.copy(), _multidict.MultiDict)


def test_cimultidict_proxy_copy_type(_multidict):
    d = _multidict.CIMultiDict(key="val")
    p = _multidict.CIMultiDictProxy(d)
    assert isinstance(p.copy(), _multidict.CIMultiDict)


def test_create_multidict_proxy_from_nonmultidict(_multidict):
    with pytest.raises(TypeError):
        _multidict.MultiDictProxy({})


def test_create_multidict_proxy_from_cimultidict(_multidict):
    d = _multidict.CIMultiDict(key="val")
    p = _multidict.MultiDictProxy(d)
    assert p == d


def test_create_multidict_proxy_from_multidict_proxy_from_mdict(_multidict):
    d = _multidict.MultiDict(key="val")
    p = _multidict.MultiDictProxy(d)
    assert p == d
    p2 = _multidict.MultiDictProxy(p)
    assert p2 == p


def test_create_cimultidict_proxy_from_cimultidict_proxy_from_ci(_multidict):
    d = _multidict.CIMultiDict(key="val")
    p = _multidict.CIMultiDictProxy(d)
    assert p == d
    p2 = _multidict.CIMultiDictProxy(p)
    assert p2 == p


def test_create_cimultidict_proxy_from_nonmultidict(_multidict):
    with pytest.raises(
        TypeError,
        match=(
            "ctor requires CIMultiDict or CIMultiDictProxy instance, "
            "not <class 'dict'>"
        ),
    ):
        _multidict.CIMultiDictProxy({})


def test_create_ci_multidict_proxy_from_multidict(_multidict):
    d = _multidict.MultiDict(key="val")
    with pytest.raises(
        TypeError,
        match=(
            "ctor requires CIMultiDict or CIMultiDictProxy instance, "
            "not <class 'multidict._multidict.*.MultiDict'>"
        ),
    ):
        _multidict.CIMultiDictProxy(d)


@pytest.mark.skipif(
    sys.version_info >= (3, 9), reason="Python 3.9 uses GenericAlias which is different"
)
def test_generic_exists(_multidict) -> None:
    assert _multidict.MultiDict[int] is _multidict.MultiDict
    assert _multidict.MultiDictProxy[int] is _multidict.MultiDictProxy
    assert _multidict.CIMultiDict[int] is _multidict.CIMultiDict
    assert _multidict.CIMultiDictProxy[int] is _multidict.CIMultiDictProxy


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Python 3.9 is required for GenericAlias"
)
def test_generic_alias(_multidict) -> None:

    assert _multidict.MultiDict[int] == types.GenericAlias(_multidict.MultiDict, (int,))
    assert _multidict.MultiDictProxy[int] == types.GenericAlias(
        _multidict.MultiDictProxy, (int,)
    )
    assert _multidict.CIMultiDict[int] == types.GenericAlias(
        _multidict.CIMultiDict, (int,)
    )
    assert _multidict.CIMultiDictProxy[int] == types.GenericAlias(
        _multidict.CIMultiDictProxy, (int,)
    )

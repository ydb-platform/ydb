import sys
import types

import pytest


def test_proxies(multidict_module):
    assert issubclass(
        multidict_module.CIMultiDictProxy,
        multidict_module.MultiDictProxy,
    )


def test_dicts(multidict_module):
    assert issubclass(multidict_module.CIMultiDict, multidict_module.MultiDict)


def test_proxy_not_inherited_from_dict(multidict_module):
    assert not issubclass(multidict_module.MultiDictProxy, multidict_module.MultiDict)


def test_dict_not_inherited_from_proxy(multidict_module):
    assert not issubclass(multidict_module.MultiDict, multidict_module.MultiDictProxy)


def test_multidict_proxy_copy_type(multidict_module):
    d = multidict_module.MultiDict(key="val")
    p = multidict_module.MultiDictProxy(d)
    assert isinstance(p.copy(), multidict_module.MultiDict)


def test_cimultidict_proxy_copy_type(multidict_module):
    d = multidict_module.CIMultiDict(key="val")
    p = multidict_module.CIMultiDictProxy(d)
    assert isinstance(p.copy(), multidict_module.CIMultiDict)


def test_create_multidict_proxy_from_nonmultidict(multidict_module):
    with pytest.raises(TypeError):
        multidict_module.MultiDictProxy({})


def test_create_multidict_proxy_from_cimultidict(multidict_module):
    d = multidict_module.CIMultiDict(key="val")
    p = multidict_module.MultiDictProxy(d)
    assert p == d


def test_create_multidict_proxy_from_multidict_proxy_from_mdict(multidict_module):
    d = multidict_module.MultiDict(key="val")
    p = multidict_module.MultiDictProxy(d)
    assert p == d
    p2 = multidict_module.MultiDictProxy(p)
    assert p2 == p


def test_create_cimultidict_proxy_from_cimultidict_proxy_from_ci(multidict_module):
    d = multidict_module.CIMultiDict(key="val")
    p = multidict_module.CIMultiDictProxy(d)
    assert p == d
    p2 = multidict_module.CIMultiDictProxy(p)
    assert p2 == p


def test_create_cimultidict_proxy_from_nonmultidict(multidict_module):
    with pytest.raises(
        TypeError,
        match=(
            "ctor requires CIMultiDict or CIMultiDictProxy instance, "
            "not <class 'dict'>"
        ),
    ):
        multidict_module.CIMultiDictProxy({})


def test_create_ci_multidict_proxy_from_multidict(multidict_module):
    d = multidict_module.MultiDict(key="val")
    with pytest.raises(
        TypeError,
        match=(
            "ctor requires CIMultiDict or CIMultiDictProxy instance, "
            "not <class 'multidict._multidict.*.MultiDict'>"
        ),
    ):
        multidict_module.CIMultiDictProxy(d)


@pytest.mark.skipif(
    sys.version_info >= (3, 9), reason="Python 3.9 uses GenericAlias which is different"
)
def test_generic_exists(multidict_module) -> None:
    assert multidict_module.MultiDict[int] is multidict_module.MultiDict
    assert multidict_module.MultiDictProxy[int] is multidict_module.MultiDictProxy
    assert multidict_module.CIMultiDict[int] is multidict_module.CIMultiDict
    assert multidict_module.CIMultiDictProxy[int] is multidict_module.CIMultiDictProxy


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Python 3.9 is required for GenericAlias"
)
def test_generic_alias(multidict_module) -> None:
    assert multidict_module.MultiDict[int] == types.GenericAlias(
        multidict_module.MultiDict, (int,)
    )
    assert multidict_module.MultiDictProxy[int] == types.GenericAlias(
        multidict_module.MultiDictProxy, (int,)
    )
    assert multidict_module.CIMultiDict[int] == types.GenericAlias(
        multidict_module.CIMultiDict, (int,)
    )
    assert multidict_module.CIMultiDictProxy[int] == types.GenericAlias(
        multidict_module.CIMultiDictProxy, (int,)
    )

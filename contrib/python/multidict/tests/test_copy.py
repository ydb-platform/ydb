import copy
from typing import Union

from multidict import CIMultiDict, CIMultiDictProxy, MultiDict, MultiDictProxy

_MD_Classes = Union[type[MultiDict[int]], type[CIMultiDict[int]]]
_MDP_Classes = Union[type[MultiDictProxy[int]], type[CIMultiDictProxy[int]]]


def test_copy(any_multidict_class: _MD_Classes) -> None:
    d = any_multidict_class()
    d["foo"] = 6
    d2 = d.copy()
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7


def test_copy_proxy(
    any_multidict_class: _MD_Classes, any_multidict_proxy_class: _MDP_Classes
) -> None:
    d = any_multidict_class()
    d["foo"] = 6
    p = any_multidict_proxy_class(d)
    d2 = p.copy()
    d2["foo"] = 7
    assert d["foo"] == 6
    assert p["foo"] == 6
    assert d2["foo"] == 7


def test_copy_std_copy(any_multidict_class: _MD_Classes) -> None:
    d = any_multidict_class()
    d["foo"] = 6
    d2 = copy.copy(d)
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7


def test_ci_multidict_clone(any_multidict_class: _MD_Classes) -> None:
    d = any_multidict_class(foo=6)
    d2 = any_multidict_class(d)
    d2["foo"] = 7
    assert d["foo"] == 6
    assert d2["foo"] == 7

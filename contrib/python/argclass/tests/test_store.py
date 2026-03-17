import pytest

import argclass


def test_required_argument():
    class RequiredStore(argclass.Store):
        foo: str

    with pytest.raises(TypeError):
        RequiredStore()


def test_store_repr():
    class Store(argclass.Store):
        foo: str
        bar: int

    store = Store(foo="spam", bar=2)
    r = repr(store)
    assert r == "<Store: bar=2, foo='spam'>"

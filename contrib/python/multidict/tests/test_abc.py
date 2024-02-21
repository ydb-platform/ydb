from collections.abc import Mapping, MutableMapping

import pytest

from multidict import MultiMapping, MutableMultiMapping


def test_abc_inheritance():
    assert issubclass(MultiMapping, Mapping)
    assert not issubclass(MultiMapping, MutableMapping)
    assert issubclass(MutableMultiMapping, Mapping)
    assert issubclass(MutableMultiMapping, MutableMapping)


class A(MultiMapping):
    def __getitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass

    def getall(self, key, default=None):
        super().getall(key, default)

    def getone(self, key, default=None):
        super().getone(key, default)


def test_abc_getall():
    with pytest.raises(KeyError):
        A().getall("key")


def test_abc_getone():
    with pytest.raises(KeyError):
        A().getone("key")


class B(A, MutableMultiMapping):
    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def add(self, key, value):
        super().add(key, value)

    def extend(self, *args, **kwargs):
        super().extend(*args, **kwargs)

    def popall(self, key, default=None):
        super().popall(key, default)

    def popone(self, key, default=None):
        super().popone(key, default)


def test_abc_add():
    with pytest.raises(NotImplementedError):
        B().add("key", "val")


def test_abc_extend():
    with pytest.raises(NotImplementedError):
        B().extend()


def test_abc_popone():
    with pytest.raises(KeyError):
        B().popone("key")


def test_abc_popall():
    with pytest.raises(KeyError):
        B().popall("key")


def test_multidict_inheritance(any_multidict_class):
    assert issubclass(any_multidict_class, MultiMapping)
    assert issubclass(any_multidict_class, MutableMultiMapping)


def test_proxy_inheritance(any_multidict_proxy_class):
    assert issubclass(any_multidict_proxy_class, MultiMapping)
    assert not issubclass(any_multidict_proxy_class, MutableMultiMapping)


def test_generic_type_in_runtime():
    MultiMapping[str]
    MutableMultiMapping[str]

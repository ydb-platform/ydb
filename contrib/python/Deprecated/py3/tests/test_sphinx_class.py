# coding: utf-8
from __future__ import print_function

import inspect
import io
import sys
import warnings

import pytest

import deprecated.sphinx


def test_class_deprecation_using_a_simple_decorator():
    # stream is used to store the deprecation message for testing
    stream = io.StringIO()

    # To deprecated the class, we use a simple decorator
    # which patches the original ``__new__`` method.

    def simple_decorator(wrapped_cls):
        old_new = wrapped_cls.__new__

        def wrapped_new(unused, *args, **kwargs):
            print(u"I am deprecated!", file=stream)
            return old_new(*args, **kwargs)

        wrapped_cls.__new__ = classmethod(wrapped_new)
        return wrapped_cls

    @simple_decorator
    class MyBaseClass(object):
        pass

    class MySubClass(MyBaseClass):
        pass

    obj = MySubClass()
    assert isinstance(obj, MyBaseClass)
    assert inspect.isclass(MyBaseClass)
    assert stream.getvalue().strip() == u"I am deprecated!"


@pytest.mark.skipif(
    sys.version_info < (3, 3), reason="Classes should have mutable docstrings -- resolved in python 3.3"
)
def test_class_deprecation_using_deprecated_decorator():
    @deprecated.sphinx.deprecated(version="7.8.9")
    class MyBaseClass(object):
        pass

    class MySubClass(MyBaseClass):
        pass

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj = MySubClass()

    assert len(warns) == 1
    assert isinstance(obj, MyBaseClass)
    assert inspect.isclass(MyBaseClass)
    assert issubclass(MySubClass, MyBaseClass)


@pytest.mark.skipif(
    sys.version_info < (3, 3), reason="Classes should have mutable docstrings -- resolved in python 3.3"
)
def test_subclass_deprecation_using_deprecated_decorator():
    @deprecated.sphinx.deprecated(version="7.8.9")
    class MyBaseClass(object):
        pass

    @deprecated.sphinx.deprecated(version="7.8.9")
    class MySubClass(MyBaseClass):
        pass

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj = MySubClass()

    assert len(warns) == 2
    assert isinstance(obj, MyBaseClass)
    assert inspect.isclass(MyBaseClass)
    assert issubclass(MySubClass, MyBaseClass)


@pytest.mark.skipif(
    sys.version_info < (3, 3), reason="Classes should have mutable docstrings -- resolved in python 3.3"
)
def test_isinstance_versionadded():
    # https://github.com/laurent-laporte-pro/deprecated/issues/48
    @deprecated.sphinx.versionadded(version="X.Y", reason="some reason")
    class VersionAddedCls:
        pass

    @deprecated.sphinx.versionadded(version="X.Y", reason="some reason")
    class VersionAddedChildCls(VersionAddedCls):
        pass

    instance = VersionAddedChildCls()
    assert isinstance(instance, VersionAddedChildCls)
    assert isinstance(instance, VersionAddedCls)


@pytest.mark.skipif(
    sys.version_info < (3, 3), reason="Classes should have mutable docstrings -- resolved in python 3.3"
)
def test_isinstance_versionchanged():
    @deprecated.sphinx.versionchanged(version="X.Y", reason="some reason")
    class VersionChangedCls:
        pass

    @deprecated.sphinx.versionchanged(version="X.Y", reason="some reason")
    class VersionChangedChildCls(VersionChangedCls):
        pass

    instance = VersionChangedChildCls()
    assert isinstance(instance, VersionChangedChildCls)
    assert isinstance(instance, VersionChangedCls)


@pytest.mark.skipif(
    sys.version_info < (3, 3), reason="Classes should have mutable docstrings -- resolved in python 3.3"
)
def test_isinstance_deprecated():
    @deprecated.sphinx.deprecated(version="X.Y", reason="some reason")
    class DeprecatedCls:
        pass

    @deprecated.sphinx.deprecated(version="Y.Z", reason="some reason")
    class DeprecatedChildCls(DeprecatedCls):
        pass

    instance = DeprecatedChildCls()
    assert isinstance(instance, DeprecatedChildCls)
    assert isinstance(instance, DeprecatedCls)


@pytest.mark.skipif(
    sys.version_info < (3, 3), reason="Classes should have mutable docstrings -- resolved in python 3.3"
)
def test_isinstance_versionadded_versionchanged():
    @deprecated.sphinx.versionadded(version="X.Y")
    @deprecated.sphinx.versionchanged(version="X.Y.Z")
    class AddedChangedCls:
        pass

    instance = AddedChangedCls()
    assert isinstance(instance, AddedChangedCls)

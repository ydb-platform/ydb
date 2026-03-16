# coding: utf-8
from __future__ import print_function

import inspect
import io
import warnings

import deprecated.classic


def test_simple_class_deprecation():
    # stream is used to store the deprecation message for testing
    stream = io.StringIO()

    # To deprecate a class, it is better to emit a message when ``__new__`` is called.
    # The simplest way is to override the ``__new__``method.
    class MyBaseClass(object):
        def __new__(cls, *args, **kwargs):
            print(u"I am deprecated!", file=stream)
            return super(MyBaseClass, cls).__new__(cls, *args, **kwargs)

    # Of course, the subclass will be deprecated too
    class MySubClass(MyBaseClass):
        pass

    obj = MySubClass()
    assert isinstance(obj, MyBaseClass)
    assert inspect.isclass(MyBaseClass)
    assert stream.getvalue().strip() == u"I am deprecated!"


def test_class_deprecation_using_wrapper():
    # stream is used to store the deprecation message for testing
    stream = io.StringIO()

    class MyBaseClass(object):
        pass

    # To deprecated the class, we use a wrapper function which emits
    # the deprecation message and calls ``__new__```.

    original_new = MyBaseClass.__new__

    def wrapped_new(unused, *args, **kwargs):
        print(u"I am deprecated!", file=stream)
        return original_new(*args, **kwargs)

    # Like ``__new__``, this wrapper is a class method.
    # It is used to patch the original ``__new__``method.
    MyBaseClass.__new__ = classmethod(wrapped_new)

    class MySubClass(MyBaseClass):
        pass

    obj = MySubClass()
    assert isinstance(obj, MyBaseClass)
    assert inspect.isclass(MyBaseClass)
    assert stream.getvalue().strip() == u"I am deprecated!"


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


def test_class_deprecation_using_deprecated_decorator():
    @deprecated.classic.deprecated
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


def test_class_respect_global_filter():
    @deprecated.classic.deprecated
    class MyBaseClass(object):
        pass

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("once")
        obj = MyBaseClass()
        obj = MyBaseClass()

    assert len(warns) == 1


def test_subclass_deprecation_using_deprecated_decorator():
    @deprecated.classic.deprecated
    class MyBaseClass(object):
        pass

    @deprecated.classic.deprecated
    class MySubClass(MyBaseClass):
        pass

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj = MySubClass()

    assert len(warns) == 2
    assert isinstance(obj, MyBaseClass)
    assert inspect.isclass(MyBaseClass)
    assert issubclass(MySubClass, MyBaseClass)


def test_simple_class_deprecation_with_args():
    @deprecated.classic.deprecated('kwargs class')
    class MyClass(object):
        def __init__(self, arg):
            super(MyClass, self).__init__()
            self.args = arg

    MyClass(5)
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj = MyClass(5)

    assert len(warns) == 1
    assert isinstance(obj, MyClass)
    assert inspect.isclass(MyClass)

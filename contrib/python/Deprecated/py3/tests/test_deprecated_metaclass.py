# coding: utf-8
import warnings

import deprecated.classic


def with_metaclass(meta, *bases):
    """Create a base class with a metaclass."""

    # This requires a bit of explanation: the basic idea is to make a dummy
    # metaclass for one level of class instantiation that replaces itself with
    # the actual metaclass.
    class metaclass(type):
        def __new__(cls, name, this_bases, d):
            return meta(name, bases, d)

        @classmethod
        def __prepare__(cls, name, this_bases):
            return meta.__prepare__(name, bases)

    return type.__new__(metaclass, 'temporary_class', (), {})


def test_with_init():
    @deprecated.classic.deprecated
    class MyClass(object):
        def __init__(self, a, b=5):
            self.a = a
            self.b = b

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj = MyClass("five")

    assert len(warns) == 1

    assert obj.a == "five"
    assert obj.b == 5


def test_with_new():
    @deprecated.classic.deprecated
    class MyClass(object):
        def __new__(cls, a, b=5):
            obj = super(MyClass, cls).__new__(cls)
            obj.c = 3.14
            return obj

        def __init__(self, a, b=5):
            self.a = a
            self.b = b

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj = MyClass("five")

    assert len(warns) == 1

    assert obj.a == "five"
    assert obj.b == 5
    assert obj.c == 3.14


def test_with_metaclass():
    class Meta(type):
        def __call__(cls, *args, **kwargs):
            obj = super(Meta, cls).__call__(*args, **kwargs)
            obj.c = 3.14
            return obj

    @deprecated.classic.deprecated
    class MyClass(with_metaclass(Meta)):
        def __init__(self, a, b=5):
            self.a = a
            self.b = b

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj = MyClass("five")

    assert len(warns) == 1

    assert obj.a == "five"
    assert obj.b == 5
    assert obj.c == 3.14


def test_with_singleton_metaclass():
    class Singleton(type):
        _instances = {}

        def __call__(cls, *args, **kwargs):
            if cls not in cls._instances:
                cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
            return cls._instances[cls]

    @deprecated.classic.deprecated
    class MyClass(with_metaclass(Singleton)):
        def __init__(self, a, b=5):
            self.a = a
            self.b = b

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj1 = MyClass("five")
        obj2 = MyClass("six", b=6)

    # __new__ is called only once:
    # the instance is constructed only once,
    # so we have only one warning.
    assert len(warns) == 1

    assert obj1.a == "five"
    assert obj1.b == 5
    assert obj2 is obj1

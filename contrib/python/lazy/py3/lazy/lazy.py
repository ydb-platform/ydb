"""Decorator to create lazy attributes."""

import sys
import functools

if sys.version_info >= (3, 9):
    from types import GenericAlias

_marker = object()


class lazy(object):
    """lazy descriptor

    Used as a decorator to create lazy attributes. Lazy attributes
    are evaluated on first use.
    """

    def __init__(self, func):
        self.__func = func
        functools.wraps(self.__func)(self)

    def __set_name__(self, owner, name):
        self.__name__ = name

    def __get__(self, inst, owner):
        if inst is None:
            return self

        if not hasattr(inst, '__dict__'):
            raise AttributeError("'%s' object has no attribute '__dict__'" % (owner.__name__,))

        name = self.__name__
        if name.startswith('__') and not name.endswith('__'):
            name = '_%s%s' % (owner.__name__, name)

        value = inst.__dict__.get(name, _marker)
        if value is _marker:
            inst.__dict__[name] = value = self.__func(inst)
        return value

    @classmethod
    def invalidate(cls, inst, name):
        """Invalidate a lazy attribute.

        This obviously violates the lazy contract. A subclass of lazy
        may however have a contract where invalidation is appropriate.
        """
        owner = inst.__class__

        if not hasattr(inst, '__dict__'):
            raise AttributeError("'%s' object has no attribute '__dict__'" % (owner.__name__,))

        if name.startswith('__') and not name.endswith('__'):
            name = '_%s%s' % (owner.__name__, name)

        if not isinstance(getattr(owner, name), cls):
            raise AttributeError("'%s.%s' is not a %s attribute" % (owner.__name__, name, cls.__name__))

        if name in inst.__dict__:
            del inst.__dict__[name]

    if sys.version_info >= (3, 9):
        __class_getitem__ = classmethod(GenericAlias)


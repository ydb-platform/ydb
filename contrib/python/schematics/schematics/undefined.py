"""
A type and singleton value (like None) to represent fields that
have not been initialized.
"""

from __future__ import unicode_literals, absolute_import


class UndefinedType(object):

    _instance = None

    def __str__(self):
        return 'Undefined'

    def __repr__(self):
        return 'Undefined'

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def __bool__(self):
        return False

    __nonzero__ = __bool__

    def __lt__(self, other):
        self._cmp_err(other, '<')

    def __gt__(self, other):
        self._cmp_err(other, '>')

    def __le__(self, other):
        self._cmp_err(other, '<=')

    def __ge__(self, other):
        self._cmp_err(other, '>=')

    def _cmp_err(self, other, op):
        raise TypeError("unorderable types: {0}() {1} {2}()".format(
                        self.__class__.__name__, op, other.__class__.__name__))

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        elif cls is not UndefinedType:
            raise TypeError("type 'UndefinedType' is not an acceptable base type")
        return cls._instance

    def __init__(self):
        pass

    def __setattr__(self, name, value):
        raise TypeError("'UndefinedType' object does not support attribute assignment")


Undefined = UndefinedType()

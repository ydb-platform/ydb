import sys


__all__ = ['PY2', 'unicode_type', 'unicode_compatible']


PY2 = sys.version_info[0] == 2


def unicode_compatible(cls):
    if PY2:  # pragma: no cover
        __str__ = getattr(cls, '__str__', None)
        __repr__ = getattr(cls, '__repr__', None)
        if __str__ is not None:
            cls.__unicode__ = __str__
            cls.__str__ = lambda self: __str__(self).encode('utf-8')
        if __repr__ is not None:
            cls.__repr__ = lambda self: __repr__(self).encode('utf-8')
    return cls


if PY2:  # pragma: no cover
    unicode_type = unicode
else:  # pragma: no cover
    unicode_type = str

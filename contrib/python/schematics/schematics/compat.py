# pylint: skip-file

from __future__ import absolute_import

import functools
import operator
import sys


__all__ = ['PY2', 'PY3', 'string_type', 'iteritems', 'metaclass', 'py_native_string', 'reraise', 'str_compat']


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


if PY2:
    __all__ += ['bytes', 'str', 'map', 'zip', 'range']
    bytes = str
    str = unicode
    string_type = basestring
    range = xrange
    from itertools import imap as map
    from itertools import izip as zip
    iteritems = operator.methodcaller('iteritems')
    itervalues = operator.methodcaller('itervalues')

    # reraise code taken from werzeug BSD license at https://github.com/pallets/werkzeug/blob/master/LICENSE
    exec('def reraise(tp, value, tb=None):\n raise tp, value, tb')
else:
    string_type = str
    iteritems = operator.methodcaller('items')
    itervalues = operator.methodcaller('values')

    # reraise code taken from werzeug BSD license at https://github.com/pallets/werkzeug/blob/master/LICENSE
    def reraise(tp, value, tb=None):
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value


def metaclass(metaclass):
    def make_class(cls):
        attrs = cls.__dict__.copy()
        if attrs.get('__dict__'):
            del attrs['__dict__']
            del attrs['__weakref__']
        return metaclass(cls.__name__, cls.__bases__, attrs)
    return make_class


def py_native_string(source):
    """
    Converts Unicode strings to bytestrings on Python 2. The intended usage is to
    wrap a function or a string in cases where Python 2 expects a native string.
    """
    if PY2:
        if isinstance(source, str):
            return source.encode('ascii')
        elif callable(source):
            @functools.wraps(source)
            def new_func(*args, **kwargs):
                rv = source(*args, **kwargs)
                if isinstance(rv, str):
                    rv = rv.encode('unicode-escape')
                return rv
            return new_func
    return source


def str_compat(class_):
    """
    On Python 2, patches the ``__str__`` and ``__repr__`` methods on the given class
    so that the class can be written for Python 3 and Unicode.
    """
    if PY2:
        if '__str__' in class_.__dict__ and '__unicode__' not in class_.__dict__:
            class_.__unicode__ = class_.__str__
            class_.__str__ = py_native_string(class_.__unicode__)
    return class_


def repr_compat(class_):
    if PY2:
        if '__repr__' in class_.__dict__:
            class_.__repr__ = py_native_string(class_.__repr__)
    return class_


def _dict(mapping):
    return dict((key, mapping[key]) for key in mapping)

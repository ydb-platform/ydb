# -*- coding: utf-8 -*-
#
# Copyright (C) 2008 John Paulett (john -at- paulett.org)
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""Helper functions for pickling and unpickling.  Most functions assist in
determining the type of an object.
"""
import base64
import collections
import io
import operator
import time
import types
import inspect

from jsonpickle import tags
from jsonpickle.compat import set, unicode, long, bytes, PY3

if not PY3:
    import __builtin__

SEQUENCES = (list, set, tuple)
SEQUENCES_SET = set(SEQUENCES)
PRIMITIVES = set((unicode, bool, float, int, long))


def is_type(obj):
    """Returns True is obj is a reference to a type.

    >>> is_type(1)
    False

    >>> is_type(object)
    True

    >>> class Klass: pass
    >>> is_type(Klass)
    True
    """
    # use "isinstance" and not "is" to allow for metaclasses
    if PY3:
        return isinstance(obj, type)
    else:
        return isinstance(obj, (type, types.ClassType))


def has_method(obj, name):
    # false if attribute doesn't exist
    if not hasattr(obj, name):
        return False
    func = getattr(obj, name)

    # builtin descriptors like __getnewargs__
    if isinstance(func, types.BuiltinMethodType):
        return True

    # note that FunctionType has a different meaning in py2/py3
    if not isinstance(func, (types.MethodType, types.FunctionType)):
        return False

    # need to go through __dict__'s since in py3 methods are essentially descriptors
    base_type = obj if is_type(obj) else obj.__class__  # __class__ for old-style classes
    original = None
    for subtype in inspect.getmro(base_type):  # there is no .mro() for old-style classes
        original = vars(subtype).get(name)
        if original is not None:
            break

    # name not found in the mro
    if original is None:
        return False

    # static methods are always fine
    if isinstance(original, staticmethod):
        return True

    # at this point, the method has to be an instancemthod or a classmethod
    self_attr = '__self__' if PY3 else 'im_self'
    if not hasattr(func, self_attr):
        return False
    bound_to = getattr(func, self_attr)

    # class methods
    if isinstance(original, classmethod):
        return issubclass(base_type, bound_to)

    # bound methods
    return isinstance(obj, type(bound_to))


def is_object(obj):
    """Returns True is obj is a reference to an object instance.

    >>> is_object(1)
    True

    >>> is_object(object())
    True

    >>> is_object(lambda x: 1)
    False
    """
    return (isinstance(obj, object) and
            not isinstance(obj, (type, types.FunctionType)))


def is_primitive(obj):
    """Helper method to see if the object is a basic data type. Unicode strings,
    integers, longs, floats, booleans, and None are considered primitive
    and will return True when passed into *is_primitive()*

    >>> is_primitive(3)
    True
    >>> is_primitive([4,4])
    False
    """
    if obj is None:
        return True
    elif type(obj) in PRIMITIVES:
        return True
    return False


def is_dictionary(obj):
    """Helper method for testing if the object is a dictionary.

    >>> is_dictionary({'key':'value'})
    True

    """
    return type(obj) is dict


def is_sequence(obj):
    """Helper method to see if the object is a sequence (list, set, or tuple).

    >>> is_sequence([4])
    True

    """
    return type(obj) in SEQUENCES_SET


def is_list(obj):
    """Helper method to see if the object is a Python list.

    >>> is_list([4])
    True
    """
    return type(obj) is list


def is_set(obj):
    """Helper method to see if the object is a Python set.

    >>> is_set(set())
    True
    """
    return type(obj) is set


def is_bytes(obj):
    """Helper method to see if the object is a bytestring.

    >>> is_bytes(b'foo')
    True
    """
    return type(obj) is bytes


def is_unicode(obj):
    """Helper method to see if the object is a unicode string"""
    return type(obj) is unicode


def is_tuple(obj):
    """Helper method to see if the object is a Python tuple.

    >>> is_tuple((1,))
    True
    """
    return type(obj) is tuple


def is_dictionary_subclass(obj):
    """Returns True if *obj* is a subclass of the dict type. *obj* must be
    a subclass and not the actual builtin dict.

    >>> class Temp(dict): pass
    >>> is_dictionary_subclass(Temp())
    True
    """
    # TODO: add UserDict
    return (hasattr(obj, '__class__') and
            issubclass(obj.__class__, dict) and not is_dictionary(obj))


def is_sequence_subclass(obj):
    """Returns True if *obj* is a subclass of list, set or tuple.

    *obj* must be a subclass and not the actual builtin, such
    as list, set, tuple, etc..

    >>> class Temp(list): pass
    >>> is_sequence_subclass(Temp())
    True
    """
    return (hasattr(obj, '__class__') and
            (issubclass(obj.__class__, SEQUENCES) or
                is_list_like(obj)) and
            not is_sequence(obj))


def is_noncomplex(obj):
    """Returns True if *obj* is a special (weird) class, that is more complex
    than primitive data types, but is not a full object. Including:

        * :class:`~time.struct_time`
    """
    if type(obj) is time.struct_time:
        return True
    return False


def is_function(obj):
    """Returns true if passed a function

    >>> is_function(lambda x: 1)
    True

    >>> is_function(locals)
    True

    >>> def method(): pass
    >>> is_function(method)
    True

    >>> is_function(1)
    False
    """
    if type(obj) in (types.FunctionType,
                     types.MethodType,
                     types.LambdaType,
                     types.BuiltinFunctionType,
                     types.BuiltinMethodType):
        return True
    if not hasattr(obj, '__class__'):
        return False
    module = translate_module_name(obj.__class__.__module__)
    name = obj.__class__.__name__
    return (module == '__builtin__' and
            name in ('function',
                     'builtin_function_or_method',
                     'instancemethod',
                     'method-wrapper'))


def is_module_function(obj):
    """Return True if `obj` is a module-global function

    >>> import os
    >>> is_module_function(os.path.exists)
    True

    >>> is_module_function(lambda: None)
    False

    """

    return (hasattr(obj, '__class__') and
            isinstance(obj, types.FunctionType) and
            hasattr(obj, '__module__') and
            hasattr(obj, '__name__') and
            obj.__name__ != '<lambda>')


def is_module(obj):
    """Returns True if passed a module

    >>> import os
    >>> is_module(os)
    True

    """
    return isinstance(obj, types.ModuleType)


def is_picklable(name, value):
    """Return True if an object can be pickled

    >>> import os
    >>> is_picklable('os', os)
    True

    >>> def foo(): pass
    >>> is_picklable('foo', foo)
    True

    >>> is_picklable('foo', lambda: None)
    False

    """
    if name in tags.RESERVED:
        return False
    return is_module_function(value) or not is_function(value)


def is_installed(module):
    """Tests to see if ``module`` is available on the sys.path

    >>> is_installed('sys')
    True
    >>> is_installed('hopefullythisisnotarealmodule')
    False

    """
    try:
        __import__(module)
        return True
    except ImportError:
        return False


def is_list_like(obj):
    return hasattr(obj, '__getitem__') and hasattr(obj, 'append')


def is_iterator(obj):
    is_file = False
    if not PY3:
        is_file = isinstance(obj, __builtin__.file)

    return (isinstance(obj, collections.Iterator) and
            not isinstance(obj, io.IOBase) and not is_file)


def is_reducible(obj):
    """
    Returns false if of a type which have special casing, and should not have their
    __reduce__ methods used
    """
    return (not (is_list(obj) or is_list_like(obj) or is_primitive(obj) or
                 is_bytes(obj) or is_unicode(obj) or
                 is_dictionary(obj) or is_sequence(obj) or is_set(obj) or is_tuple(obj) or
                 is_dictionary_subclass(obj) or is_sequence_subclass(obj) or is_noncomplex(obj)
                 or is_function(obj) or is_module(obj) or type(obj) is object or obj is object
                 or (is_type(obj) and obj.__module__ == 'datetime')))


def in_dict(obj, key, default=False):
    """
    Returns true if key exists in obj.__dict__; false if not in.
    If obj.__dict__ is absent, return default
    """
    return (key in obj.__dict__) if getattr(obj, '__dict__', None) else default


def in_slots(obj, key, default=False):
    """
    Returns true if key exists in obj.__slots__; false if not in.
    If obj.__slots__ is absent, return default
    """
    return (key in obj.__slots__) if getattr(obj, '__slots__', None) else default


def has_reduce(obj):
    """
    Tests if __reduce__ or __reduce_ex__ exists in the object dict or
    in the class dicts of every class in the MRO *except object*.

    Returns a tuple of booleans (has_reduce, has_reduce_ex)
    """

    if not is_reducible(obj) or is_type(obj):
        return (False, False)

    has_reduce = False
    has_reduce_ex = False

    REDUCE = '__reduce__'
    REDUCE_EX = '__reduce_ex__'

    # For object instance
    has_reduce = in_dict(obj, REDUCE)
    has_reduce_ex = in_dict(obj, REDUCE_EX)

    has_reduce = has_reduce or in_slots(obj, REDUCE)
    has_reduce_ex = has_reduce_ex or in_slots(obj, REDUCE_EX)

    # turn to the MRO
    for base in type(obj).__mro__:
        if is_reducible(base):
            has_reduce = has_reduce or in_dict(base, REDUCE)
            has_reduce_ex = has_reduce_ex or in_dict(base, REDUCE_EX)
        if has_reduce_ex and has_reduce_ex:
            return (True, True)

    return (has_reduce, has_reduce_ex)


def translate_module_name(module):
    """Rename builtin modules to a consistent (Python2) module name

    This is used so that references to Python's `builtins` module can
    be loaded in both Python 2 and 3.  We remap to the "__builtin__"
    name and unmap it when importing.

    See untranslate_module_name() for the reverse operation.

    """
    if (PY3 and module == 'builtins') or module == 'exceptions':
        # We map the Python2 `exceptions` module to `__builtin__` because
        # `__builtin__` is a superset and contains everything that is
        # available in `exceptions`, which makes the translation simpler.
        return '__builtin__'
    else:
        return module


def untranslate_module_name(module):
    """Rename module names mention in JSON to names that we can import

    This reverses the translation applied by translate_module_name() to
    a module name available to the current version of Python.

    """
    if PY3:
        # remap `__builtin__` and `exceptions` to the `builtins` module
        if module == '__builtin__':
            module = 'builtins'
        elif module == 'exceptions':
            module = 'builtins'
    return module


def importable_name(cls):
    """
    >>> class Example(object):
    ...     pass

    >>> ex = Example()
    >>> importable_name(ex.__class__)
    'jsonpickle.util.Example'

    >>> importable_name(type(25))
    '__builtin__.int'

    >>> importable_name(None.__class__)
    '__builtin__.NoneType'

    >>> importable_name(False.__class__)
    '__builtin__.bool'

    >>> importable_name(AttributeError)
    '__builtin__.AttributeError'

    """
    name = cls.__name__
    module = translate_module_name(cls.__module__)
    return '%s.%s' % (module, name)


def b64encode(data):
    payload = base64.b64encode(data)
    if PY3 and type(payload) is bytes:
        payload = payload.decode('ascii')
    return payload


def b64decode(payload):
    if PY3 and type(payload) is not bytes:
        payload = bytes(payload, 'ascii')
    return base64.b64decode(payload)


def itemgetter(obj, getter=operator.itemgetter(0)):
    return unicode(getter(obj))

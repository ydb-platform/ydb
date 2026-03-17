##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

# This tiny set of safe builtins is extended by users of the module.
# AccessControl.ZopeGuards contains a large set of wrappers for builtins.
# DocumentTemplate.DT_UTil contains a few.

import builtins

from RestrictedPython.transformer import INSPECT_ATTRIBUTES


safe_builtins = {}

_safe_names = [
    '__build_class__',
    'None',
    'False',
    'True',
    'abs',
    'bool',
    'bytes',
    'callable',
    'chr',
    'complex',
    'divmod',
    'float',
    'hash',
    'hex',
    'id',
    'int',
    'isinstance',
    'issubclass',
    'len',
    'oct',
    'ord',
    'pow',
    'range',
    'repr',
    'round',
    'slice',
    'sorted',
    'str',
    'tuple',
    'zip'
]

_safe_exceptions = [
    'ArithmeticError',
    'AssertionError',
    'AttributeError',
    'BaseException',
    'BufferError',
    'BytesWarning',
    'DeprecationWarning',
    'EOFError',
    'EnvironmentError',
    'Exception',
    'FloatingPointError',
    'FutureWarning',
    'GeneratorExit',
    'IOError',
    'ImportError',
    'ImportWarning',
    'IndentationError',
    'IndexError',
    'KeyError',
    'KeyboardInterrupt',
    'LookupError',
    'MemoryError',
    'NameError',
    'NotImplementedError',
    'OSError',
    'OverflowError',
    'PendingDeprecationWarning',
    'ReferenceError',
    'RuntimeError',
    'RuntimeWarning',
    'StopIteration',
    'SyntaxError',
    'SyntaxWarning',
    'SystemError',
    'SystemExit',
    'TabError',
    'TypeError',
    'UnboundLocalError',
    'UnicodeDecodeError',
    'UnicodeEncodeError',
    'UnicodeError',
    'UnicodeTranslateError',
    'UnicodeWarning',
    'UserWarning',
    'ValueError',
    'Warning',
    'ZeroDivisionError',
]

for name in _safe_names:
    safe_builtins[name] = getattr(builtins, name)

for name in _safe_exceptions:
    safe_builtins[name] = getattr(builtins, name)


# Wrappers provided by this module:
# delattr
# setattr

# Wrappers provided by ZopeGuards:
# __import__
# apply
# dict
# enumerate
# filter
# getattr
# hasattr
# iter
# list
# map
# max
# min
# sum
# all
# any

# Builtins that are intentionally disabled
# compile   - don't let them produce new code
# dir       - a general purpose introspector, probably hard to wrap
# execfile  - no direct I/O
# file      - no direct I/O
# globals   - uncontrolled namespace access
# input     - no direct I/O
# locals    - uncontrolled namespace access
# open      - no direct I/O
# raw_input - no direct I/O
# vars      - uncontrolled namespace access

# There are several strings that describe Python.  I think there's no
# point to including these, although they are obviously safe:
# copyright, credits, exit, help, license, quit

# Not provided anywhere.  Do something about these?  Several are
# related to new-style classes, which we are too scared of to support
# <0.3 wink>.  coerce, buffer, and reload are esoteric enough that no
# one should care.

# buffer
# bytearray
# classmethod
# coerce
# eval
# intern
# memoryview
# object
# property
# reload
# staticmethod
# super
# type


def _write_wrapper():
    # Construct the write wrapper class
    def _handler(secattr, error_msg):
        # Make a class method.
        def handler(self, *args):
            try:
                f = getattr(self.ob, secattr)
            except AttributeError:
                raise TypeError(error_msg)
            f(*args)
        return handler

    class Wrapper:
        def __init__(self, ob):
            self.__dict__['ob'] = ob

        __setitem__ = _handler(
            '__guarded_setitem__',
            'object does not support item or slice assignment')

        __delitem__ = _handler(
            '__guarded_delitem__',
            'object does not support item or slice assignment')

        __setattr__ = _handler(
            '__guarded_setattr__',
            'attribute-less object (assign or del)')

        __delattr__ = _handler(
            '__guarded_delattr__',
            'attribute-less object (assign or del)')
    return Wrapper


def _full_write_guard():
    # Nested scope abuse!
    # safetypes and Wrapper variables are used by guard()
    safetypes = {dict, list}
    Wrapper = _write_wrapper()

    def guard(ob):
        # Don't bother wrapping simple types, or objects that claim to
        # handle their own write security.
        if type(ob) in safetypes or hasattr(ob, '_guarded_writes'):
            return ob
        # Hand the object to the Wrapper instance, then return the instance.
        return Wrapper(ob)
    return guard


full_write_guard = _full_write_guard()


def guarded_setattr(object, name, value):
    setattr(full_write_guard(object), name, value)


safe_builtins['setattr'] = guarded_setattr


def guarded_delattr(object, name):
    delattr(full_write_guard(object), name)


safe_builtins['delattr'] = guarded_delattr


raise_ = object()


def safer_getattr(object, name, default=None, getattr=getattr):
    """Getattr implementation which prevents using format on string objects.

    format() is considered harmful:
    http://lucumr.pocoo.org/2016/12/29/careful-with-str-format/

    """
    if type(name) is not str:
        raise TypeError('type(name) must be str')
    if name in ('format', 'format_map') and (
            isinstance(object, str) or
            (isinstance(object, type) and issubclass(object, str))):
        raise NotImplementedError(
            'Using the format*() methods of `str` is not safe')
    if name in INSPECT_ATTRIBUTES:
        raise AttributeError(
            f'"{name}" is a restricted name,'
            ' that is forbidden to access in RestrictedPython.')
    if name.startswith('_'):
        raise AttributeError(
            '"{name}" is an invalid attribute name because it '
            'starts with "_"'.format(name=name)
        )
    args = (object, name) + (() if default is raise_ else (default,))
    return getattr(*args)


safe_builtins['_getattr_'] = safer_getattr


def safer_getattr_raise(object, name, default=raise_):
    """like ``safer_getattr`` but raising ``AttributeError`` if failing."""
    return safer_getattr(object, name, default)


def guarded_iter_unpack_sequence(it, spec, _getiter_):
    """Protect sequence unpacking of targets in a 'for loop'.

    The target of a for loop could be a sequence.
    For example "for a, b in it"
    => Each object from the iterator needs guarded sequence unpacking.
    """
    # The iteration itself needs to be protected as well.
    for ob in _getiter_(it):
        yield guarded_unpack_sequence(ob, spec, _getiter_)


def guarded_unpack_sequence(it, spec, _getiter_):
    """Protect nested sequence unpacking.

    Protect the unpacking of 'it' by wrapping it with '_getiter_'.
    Furthermore for each child element, defined by spec,
    guarded_unpack_sequence is called again.

    Have a look at transformer.py 'gen_unpack_spec' for a more detailed
    explanation.
    """
    # Do the guarded unpacking of the sequence.
    ret = list(_getiter_(it))

    # If the sequence is shorter then expected the interpreter will raise
    # 'ValueError: need more than X value to unpack' anyway
    # => No childs are unpacked => nothing to protect.
    if len(ret) < spec['min_len']:
        return ret

    # For all child elements do the guarded unpacking again.
    for (idx, child_spec) in spec['childs']:
        ret[idx] = guarded_unpack_sequence(ret[idx], child_spec, _getiter_)

    return ret


safe_globals = {'__builtins__': safe_builtins}

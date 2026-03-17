# -*- coding: utf-8 -*-
#
# Copyright (C) 2008 John Paulett (john -at- paulett.org)
# Copyright (C) 2009, 2011, 2013 David Aguilar (davvid -at- gmail.com)
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""Python library for serializing any arbitrary object graph into JSON.

jsonpickle can take almost any Python object and turn the object into JSON.
Additionally, it can reconstitute the object back into Python.

The object must be accessible globally via a module and must
inherit from object (AKA new-style classes).

Create an object::

    class Thing(object):
        def __init__(self, name):
            self.name = name

    obj = Thing('Awesome')

Use jsonpickle to transform the object into a JSON string::

    import jsonpickle
    frozen = jsonpickle.encode(obj)

Use jsonpickle to recreate a Python object from a JSON string::

    thawed = jsonpickle.decode(frozen)

.. warning::

    Loading a JSON string from an untrusted source represents a potential
    security vulnerability.  jsonpickle makes no attempt to sanitize the input.

The new object has the same type and data, but essentially is now a copy of
the original.

.. code-block:: python

    assert obj.name == thawed.name

If you will never need to load (regenerate the Python class from JSON), you can
pass in the keyword unpicklable=False to prevent extra information from being
added to JSON::

    oneway = jsonpickle.encode(obj, unpicklable=False)
    result = jsonpickle.decode(oneway)
    assert obj.name == result['name'] == 'Awesome'

"""
from jsonpickle import pickler
from jsonpickle import unpickler
from jsonpickle.backend import JSONBackend
from jsonpickle.version import VERSION

# ensure built-in handlers are loaded
__import__('jsonpickle.handlers')

__all__ = ('encode', 'decode')
__version__ = VERSION

json = JSONBackend()

# Export specific JSONPluginMgr methods into the jsonpickle namespace
set_preferred_backend = json.set_preferred_backend
set_encoder_options = json.set_encoder_options
load_backend = json.load_backend
remove_backend = json.remove_backend
enable_fallthrough = json.enable_fallthrough


def encode(value,
           unpicklable=True,
           make_refs=True,
           keys=False,
           max_depth=None,
           backend=None,
           warn=False,
           max_iter=None):
    """Return a JSON formatted representation of value, a Python object.

    :param unpicklable: If set to False then the output will not contain the
        information necessary to turn the JSON data back into Python objects,
        but a simpler JSON stream is produced.
    :param max_depth: If set to a non-negative integer then jsonpickle will
        not recurse deeper than 'max_depth' steps into the object.  Anything
        deeper than 'max_depth' is represented using a Python repr() of the
        object.
    :param make_refs: If set to False jsonpickle's referencing support is
        disabled.  Objects that are id()-identical won't be preserved across
        encode()/decode(), but the resulting JSON stream will be conceptually
        simpler.  jsonpickle detects cyclical objects and will break the cycle
        by calling repr() instead of recursing when make_refs is set False.
    :param keys: If set to True then jsonpickle will encode non-string
        dictionary keys instead of coercing them into strings via `repr()`.
    :param warn: If set to True then jsonpickle will warn when it
        returns None for an object which it cannot pickle
        (e.g. file descriptors).
    :param max_iter: If set to a non-negative integer then jsonpickle will
        consume at most `max_iter` items when pickling iterators.

    >>> encode('my string')
    '"my string"'
    >>> encode(36)
    '36'

    >>> encode({'foo': True})
    '{"foo": true}'

    >>> encode({'foo': True}, max_depth=0)
    '"{\\'foo\\': True}"'

    >>> encode({'foo': True}, max_depth=1)
    '{"foo": "True"}'


    """
    if backend is None:
        backend = json
    return pickler.encode(value,
                          backend=backend,
                          unpicklable=unpicklable,
                          make_refs=make_refs,
                          keys=keys,
                          max_depth=max_depth,
                          warn=warn)


def decode(string, backend=None, keys=False):
    """Convert a JSON string into a Python object.

    The keyword argument 'keys' defaults to False.
    If set to True then jsonpickle will decode non-string dictionary keys
    into python objects via the jsonpickle protocol.

    >>> str(decode('"my string"'))
    'my string'
    >>> decode('36')
    36
    """
    if backend is None:
        backend = json
    return unpickler.decode(string, backend=backend, keys=keys)


# json.load(),loads(), dump(), dumps() compatibility
dumps = encode
loads = decode

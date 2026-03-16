# -*- coding: utf-8 -*-
#
# Copyright (C) 2008 John Paulett (john -at- paulett.org)
# Copyright (C) 2009, 2011, 2013 David Aguilar (davvid -at- gmail.com)
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

import warnings
import sys
import quopri
from itertools import chain, islice

import jsonpickle.util as util
import jsonpickle.tags as tags
import jsonpickle.handlers as handlers

from jsonpickle.backend import JSONBackend
from jsonpickle.compat import unicode, PY3, PY2


def encode(value,
           unpicklable=False,
           make_refs=True,
           keys=False,
           max_depth=None,
           reset=True,
           backend=None,
           warn=False,
           context=None,
           max_iter=None):
    backend = _make_backend(backend)
    if context is None:
        context = Pickler(unpicklable=unpicklable,
                          make_refs=make_refs,
                          keys=keys,
                          backend=backend,
                          max_depth=max_depth,
                          warn=warn,
                          max_iter=max_iter)
    return backend.encode(context.flatten(value, reset=reset))


def _make_backend(backend):
    if backend is None:
        return JSONBackend()
    else:
        return backend


class Pickler(object):

    def __init__(self,
                 unpicklable=True,
                 make_refs=True,
                 max_depth=None,
                 backend=None,
                 keys=False,
                 warn=False,
                 max_iter=None):
        self.unpicklable = unpicklable
        self.make_refs = make_refs
        self.backend = _make_backend(backend)
        self.keys = keys
        self.warn = warn
        # The current recursion depth
        self._depth = -1
        # The maximal recursion depth
        self._max_depth = max_depth
        # Maps id(obj) to reference IDs
        self._objs = {}
        # Avoids garbage collection
        self._seen = []
        # maximum amount of items to take from a pickled iterator
        self._max_iter = max_iter

    def reset(self):
        self._objs = {}
        self._depth = -1
        self._seen = []

    def _push(self):
        """Steps down one level in the namespace.
        """
        self._depth += 1

    def _pop(self, value):
        """Step up one level in the namespace and return the value.
        If we're at the root, reset the pickler's state.
        """
        self._depth -= 1
        if self._depth == -1:
            self.reset()
        return value

    def _log_ref(self, obj):
        """
        Log a reference to an in-memory object.
        Return True if this object is new and was assigned
        a new ID. Otherwise return False.
        """
        objid = id(obj)
        is_new = objid not in self._objs
        if is_new:
            new_id = len(self._objs)
            self._objs[objid] = new_id
        return is_new

    def _mkref(self, obj):
        """
        Log a reference to an in-memory object, and return
        if that object should be considered newly logged.
        """
        is_new = self._log_ref(obj)
        # Pretend the object is new
        pretend_new = not self.unpicklable or not self.make_refs
        return pretend_new or is_new

    def _getref(self, obj):
        return {tags.ID: self._objs.get(id(obj))}

    def flatten(self, obj, reset=True):
        """Takes an object and returns a JSON-safe representation of it.

        Simply returns any of the basic builtin datatypes

        >>> p = Pickler()
        >>> p.flatten('hello world') == 'hello world'
        True
        >>> p.flatten(49)
        49
        >>> p.flatten(350.0)
        350.0
        >>> p.flatten(True)
        True
        >>> p.flatten(False)
        False
        >>> r = p.flatten(None)
        >>> r is None
        True
        >>> p.flatten(False)
        False
        >>> p.flatten([1, 2, 3, 4])
        [1, 2, 3, 4]
        >>> p.flatten((1,2,))[tags.TUPLE]
        [1, 2]
        >>> p.flatten({'key': 'value'}) == {'key': 'value'}
        True
        """
        if reset:
            self.reset()
        return self._flatten(obj)

    def _flatten(self, obj):
        self._push()
        return self._pop(self._flatten_obj(obj))

    def _flatten_obj(self, obj):
        self._seen.append(obj)
        max_reached = self._depth == self._max_depth

        if max_reached or (not self.make_refs and id(obj) in self._objs):
            # break the cycle
            flatten_func = repr
        else:
            flatten_func = self._get_flattener(obj)

        if flatten_func is None:
            self._pickle_warning(obj)
            return None

        return flatten_func(obj)

    def _list_recurse(self, obj):
        return [self._flatten(v) for v in obj]

    def _get_flattener(self, obj):

        if PY2 and isinstance(obj, file):
            return self._flatten_file

        if util.is_primitive(obj):
            return lambda obj: obj

        if util.is_bytes(obj):
            return self._flatten_bytestring

        list_recurse = self._list_recurse

        if util.is_list(obj):
            if self._mkref(obj):
                return list_recurse
            else:
                self._push()
                return self._getref

        # We handle tuples and sets by encoding them in a "(tuple|set)dict"
        if util.is_tuple(obj):
            if not self.unpicklable:
                return list_recurse
            return lambda obj: {tags.TUPLE: [self._flatten(v) for v in obj]}

        if util.is_set(obj):
            if not self.unpicklable:
                return list_recurse
            return lambda obj: {tags.SET: [self._flatten(v) for v in obj]}

        if util.is_dictionary(obj):
            return self._flatten_dict_obj

        if util.is_type(obj):
            return _mktyperef

        if util.is_object(obj):
            return self._ref_obj_instance

        if util.is_module_function(obj):
            return self._flatten_function

        # instance methods, lambdas, old style classes...
        self._pickle_warning(obj)
        return None

    def _ref_obj_instance(self, obj):
        """Reference an existing object or flatten if new
        """
        if self._mkref(obj):
            # We've never seen this object so return its
            # json representation.
            return self._flatten_obj_instance(obj)
        # We've seen this object before so place an object
        # reference tag in the data. This avoids infinite recursion
        # when processing cyclical objects.
        return self._getref(obj)

    def _flatten_file(self, obj):
        """
        Special case file objects
        """
        assert not PY3 and isinstance(obj, file)
        return None

    def _flatten_bytestring(self, obj):
        if PY2:
            try:
                return obj.decode('utf-8')
            except:
                pass
        return {tags.BYTES: quopri.encodestring(obj).decode('utf-8')}

    def _flatten_obj_instance(self, obj):
        """Recursively flatten an instance and return a json-friendly dict
        """
        data = {}
        has_class = hasattr(obj, '__class__')
        has_dict = hasattr(obj, '__dict__')
        has_slots = not has_dict and hasattr(obj, '__slots__')
        has_getnewargs = util.has_method(obj, '__getnewargs__')
        has_getnewargs_ex = util.has_method(obj, '__getnewargs_ex__')
        has_getinitargs = util.has_method(obj, '__getinitargs__')
        has_reduce, has_reduce_ex = util.has_reduce(obj)

        # Support objects with __getstate__(); this ensures that
        # both __setstate__() and __getstate__() are implemented
        has_getstate = hasattr(obj, '__getstate__')
        # not using has_method since __getstate__() is handled separately below

        if has_class:
            cls = obj.__class__
        else:
            cls = type(obj)

        # Check for a custom handler
        class_name = util.importable_name(cls)
        handler = handlers.get(cls, handlers.get(class_name))
        if handler is not None:
            if self.unpicklable:
                data[tags.OBJECT] = class_name
            return handler(self).flatten(obj, data)

        reduce_val = None
        if has_class and not util.is_module(obj):
            if self.unpicklable:
                class_name = util.importable_name(cls)
                data[tags.OBJECT] = class_name

            # test for a reduce implementation, and redirect before doing anything else
            # if that is what reduce requests
            if has_reduce_ex:
                try:
                    # we're implementing protocol 2
                    reduce_val = obj.__reduce_ex__(2)
                except TypeError:
                    # A lot of builtin types have a reduce which just raises a TypeError
                    # we ignore those
                    pass

            if has_reduce and not reduce_val:
                try:
                    reduce_val = obj.__reduce__()
                except TypeError:
                    # A lot of builtin types have a reduce which just raises a TypeError
                    # we ignore those
                    pass

            if reduce_val:
                try:
                    # At this stage, we only handle the case where __reduce__ returns a string
                    # other reduce functionality is implemented further down
                    if isinstance(reduce_val, (str, unicode)):
                        varpath = iter(reduce_val.split('.'))
                        # curmod will be transformed by the loop into the value to pickle
                        curmod = sys.modules[next(varpath)]
                        for modname in varpath:
                            curmod = getattr(curmod, modname)
                            # replace obj with value retrieved
                        return self._flatten(curmod)
                except KeyError:
                    # well, we can't do anything with that, so we ignore it
                    pass

            if has_getnewargs_ex:
                data[tags.NEWARGSEX] = list(map(self._flatten, obj.__getnewargs_ex__()))

            if has_getnewargs and not has_getnewargs_ex:
                data[tags.NEWARGS] = self._flatten(obj.__getnewargs__())

            if has_getinitargs:
                data[tags.INITARGS] = self._flatten(obj.__getinitargs__())

        if has_getstate:
            try:
                state = obj.__getstate__()
            except TypeError:
                # Has getstate but it cannot be called, e.g. file descriptors
                # in Python3
                self._pickle_warning(obj)
                return None
            else:
                return self._getstate(state, data)

        if util.is_module(obj):
            if self.unpicklable:
                data[tags.REPR] = '%s/%s' % (obj.__name__,
                                             obj.__name__)
            else:
                data = unicode(obj)
            return data

        if util.is_dictionary_subclass(obj):
            self._flatten_dict_obj(obj, data)
            return data

        if util.is_sequence_subclass(obj):
            return self._flatten_sequence_obj(obj, data)

        if util.is_noncomplex(obj):
            return [self._flatten(v) for v in obj]

        if util.is_iterator(obj):
            # force list in python 3
            data[tags.ITERATOR] = list(map(self._flatten, islice(obj, self._max_iter)))
            return data

        if reduce_val and not isinstance(reduce_val, (str, unicode)):
            # at this point, reduce_val should be some kind of iterable
            # pad out to len 5
            rv_as_list = list(reduce_val)
            insufficiency = 5 - len(rv_as_list)
            if insufficiency:
                rv_as_list += [None] * insufficiency

            if rv_as_list[0].__name__ == '__newobj__':
                rv_as_list[0] = tags.NEWOBJ

            data[tags.REDUCE] = list(map(self._flatten, rv_as_list))

            # lift out iterators, so we don't have to iterator and uniterator their content
            # on unpickle
            if data[tags.REDUCE][3]:
                data[tags.REDUCE][3] = data[tags.REDUCE][3][tags.ITERATOR]

            if data[tags.REDUCE][4]:
                data[tags.REDUCE][4] = data[tags.REDUCE][4][tags.ITERATOR]

            return data

        if has_dict:
            # Support objects that subclasses list and set
            if util.is_sequence_subclass(obj):
                return self._flatten_sequence_obj(obj, data)

            # hack for zope persistent objects; this unghostifies the object
            getattr(obj, '_', None)
            return self._flatten_dict_obj(obj.__dict__, data)

        if has_slots:
            return self._flatten_newstyle_with_slots(obj, data)

        # catchall return for data created above without a return
        # (e.g. __getnewargs__ is not supposed to be the end of the story)
        if data:
            return data

        self._pickle_warning(obj)
        return None

    def _flatten_function(self, obj):
        if self.unpicklable:
            data = {tags.FUNCTION: util.importable_name(obj)}
        else:
            data = None

        return data

    def _flatten_dict_obj(self, obj, data=None):
        """Recursively call flatten() and return json-friendly dict
        """
        if data is None:
            data = obj.__class__()

        flatten = self._flatten_key_value_pair
        for k, v in sorted(obj.items(), key=util.itemgetter):
            flatten(k, v, data)

        # the collections.defaultdict protocol
        if hasattr(obj, 'default_factory') and callable(obj.default_factory):
            factory = obj.default_factory
            if util.is_type(factory):
                # Reference the class/type
                value = _mktyperef(factory)
            else:
                # The factory is not a type and could reference e.g. functions
                # or even the object instance itself, which creates a cycle.
                if self._mkref(factory):
                    # We've never seen this object before so pickle it in-place.
                    # Create an instance from the factory and assume that the
                    # resulting instance is a suitable examplar.
                    value = self._flatten(handlers.CloneFactory(factory()))
                else:
                    # We've seen this object before.
                    # Break the cycle by emitting a reference.
                    value = self._getref(factory)
            data['default_factory'] = value

        return data

    def _flatten_obj_attrs(self, obj, attrs, data):
        flatten = self._flatten_key_value_pair
        ok = False
        for k in attrs:
            try:
                value = getattr(obj, k)
                flatten(k, value, data)
            except AttributeError:
                # The attribute may have been deleted
                continue
            ok = True
        return ok

    def _flatten_newstyle_with_slots(self, obj, data):
        """Return a json-friendly dict for new-style objects with __slots__.
        """
        allslots = [_wrap_string_slot(getattr(cls, '__slots__', tuple()))
                    for cls in obj.__class__.mro()]

        if not self._flatten_obj_attrs(obj, chain(*allslots), data):
            attrs = [x for x in dir(obj)
                     if not x.startswith('__') and not x.endswith('__')]
            self._flatten_obj_attrs(obj, attrs, data)

        return data

    def _flatten_key_value_pair(self, k, v, data):
        """Flatten a key/value pair into the passed-in dictionary."""
        if not util.is_picklable(k, v):
            return data
        if self.keys:
            if not isinstance(k, (str, unicode)) or k.startswith(tags.JSON_KEY):
                k = self._escape_key(k)
        else:
            if k is None:
                k = 'null'  # for compatibility with common json encoders
            if not isinstance(k, (str, unicode)):
                try:
                    k = repr(k)
                except:
                    k = unicode(k)

        data[k] = self._flatten(v)
        return data

    def _flatten_sequence_obj(self, obj, data):
        """Return a json-friendly dict for a sequence subclass."""
        if hasattr(obj, '__dict__'):
            self._flatten_dict_obj(obj.__dict__, data)
        value = [self._flatten(v) for v in obj]
        if self.unpicklable:
            data[tags.SEQ] = value
        else:
            return value
        return data

    def _escape_key(self, k):
        return tags.JSON_KEY + encode(k,
                                      reset=False, keys=True,
                                      context=self, backend=self.backend,
                                      make_refs=self.make_refs)

    def _getstate(self, obj, data):
        state = self._flatten_obj(obj)
        if self.unpicklable:
            data[tags.STATE] = state
        else:
            data = state
        return data

    def _pickle_warning(self, obj):
        if self.warn:
            msg = 'jsonpickle cannot pickle %r: replaced with None' % obj
            warnings.warn(msg)


def _mktyperef(obj):
    """Return a typeref dictionary

    >>> _mktyperef(AssertionError)
    {'py/type': '__builtin__.AssertionError'}

    """
    return {tags.TYPE: util.importable_name(obj)}


def _wrap_string_slot(string):
    """Converts __slots__ = 'a' into __slots__ = ('a',)
    """
    if isinstance(string, (str, unicode)):
        return (string,)
    return string

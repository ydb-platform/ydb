# -*- coding: utf-8 -*-
#
# Copyright (C) 2008 John Paulett (john -at- paulett.org)
# Copyright (C) 2009, 2011, 2013 David Aguilar (davvid -at- gmail.com)
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

import sys
import quopri

import jsonpickle.util as util
import jsonpickle.tags as tags
import jsonpickle.handlers as handlers

from jsonpickle.compat import set
from jsonpickle.backend import JSONBackend


def decode(string, backend=None, context=None, keys=False, reset=True,
           safe=False):
    backend = _make_backend(backend)
    if context is None:
        context = Unpickler(keys=keys, backend=backend, safe=safe)
    return context.restore(backend.decode(string), reset=reset)


def _make_backend(backend):
    if backend is None:
        return JSONBackend()
    else:
        return backend


class _Proxy(object):
    """Proxies are dummy objects that are later replaced by real instances

    The `restore()` function has to solve a tricky problem when pickling
    objects with cyclical references -- the parent instance does not yet
    exist.

    The problem is that `__getnewargs__()`, `__getstate__()`, custom handlers,
    and cyclical objects graphs are allowed to reference the yet-to-be-created
    object via the referencing machinery.

    In other words, objects are allowed to depend on themselves for
    construction!

    We solve this problem by placing dummy Proxy objects into the referencing
    machinery so that we can construct the child objects before constructing
    the parent.  Objects are initially created with Proxy attribute values
    instead of real references.

    We collect all objects that contain references to proxies and run
    a final sweep over them to swap in the real instance.  This is done
    at the very end of the top-level `restore()`.

    The `instance` attribute below is replaced with the real instance
    after `__new__()` has been used to construct the object and is used
    when swapping proxies with real instances.

    """
    def __init__(self):
        self.instance = None

    def get(self):
        return self.instance

    def reset(self, instance):
        self.instance = instance


def _obj_setattr(obj, attr, proxy):
    setattr(obj, attr, proxy.get())


def _obj_setvalue(obj, idx, proxy):
    obj[idx] = proxy.get()


class Unpickler(object):

    def __init__(self, backend=None, keys=False, safe=False):
        self.backend = _make_backend(backend)
        self.keys = keys
        self.safe = safe

        self.reset()

    def reset(self):
        """Resets the object's internal state.
        """
        # Map reference names to object instances
        self._namedict = {}

        # The stack of names traversed for child objects
        self._namestack = []

        # Map of objects to their index in the _objs list
        self._obj_to_idx = {}
        self._objs = []
        self._proxies = []

    def restore(self, obj, reset=True):
        """Restores a flattened object to its original python state.

        Simply returns any of the basic builtin types

        >>> u = Unpickler()
        >>> u.restore('hello world')
        'hello world'
        >>> u.restore({'key': 'value'})
        {'key': 'value'}

        """
        if reset:
            self.reset()
        value = self._restore(obj)
        if reset:
            self._swap_proxies()
        return value

    def _swap_proxies(self):
        """Replace proxies with their corresponding instances"""
        for (obj, attr, proxy, method) in self._proxies:
            method(obj, attr, proxy)
        self._proxies = []

    def _restore(self, obj):
        if has_tag(obj, tags.BYTES):
            restore = self._restore_bytestring
        elif has_tag(obj, tags.ID):
            restore = self._restore_id
        elif has_tag(obj, tags.REF):  # Backwards compatibility
            restore = self._restore_ref
        elif has_tag(obj, tags.ITERATOR):
            restore = self._restore_iterator
        elif has_tag(obj, tags.TYPE):
            restore = self._restore_type
        elif has_tag(obj, tags.REPR):  # Backwards compatibility
            restore = self._restore_repr
        elif has_tag(obj, tags.REDUCE):
            restore = self._restore_reduce
        elif has_tag(obj, tags.OBJECT):
            restore = self._restore_object
        elif has_tag(obj, tags.FUNCTION):
            restore = self._restore_function
        elif util.is_list(obj):
            restore = self._restore_list
        elif has_tag(obj, tags.TUPLE):
            restore = self._restore_tuple
        elif has_tag(obj, tags.SET):
            restore = self._restore_set
        elif util.is_dictionary(obj):
            restore = self._restore_dict
        else:
            restore = lambda x: x
        return restore(obj)

    def _restore_bytestring(self, obj):
        return quopri.decodestring(obj[tags.BYTES].encode('utf-8'))

    def _restore_iterator(self, obj):
        return iter(self._restore_list(obj[tags.ITERATOR]))

    def _restore_reduce(self, obj):
        """
        Supports restoring with all elements of __reduce__ as per pep 307.
        Assumes that iterator items (the last two) are represented as lists
        as per pickler implementation.
        """
        reduce_val = obj[tags.REDUCE]
        f, args, state, listitems, dictitems = map(self._restore, reduce_val)

        if f == tags.NEWOBJ or f.__name__ == '__newobj__':
            # mandated special case
            cls = args[0]
            stage1 = cls.__new__(cls, *args[1:])
        else:
            stage1 = f(*args)

        if state:
            try:
                stage1.__setstate__(state)
            except AttributeError:
                # it's fine - we'll try the prescribed default methods
                try:
                    stage1.__dict__.update(state)
                except AttributeError:
                    # next prescribed default
                    for k, v in state.items():
                        setattr(stage1, k, v)

        if listitems:
            # should be lists if not None
            try:
                stage1.extend(listitems)
            except AttributeError:
                for x in listitems:
                    stage1.append(x)

        if dictitems:
            for k, v in dictitems:
                stage1.__setitem__(k, v)

        return stage1

    def _restore_id(self, obj):
        return self._objs[obj[tags.ID]]

    def _restore_ref(self, obj):
        return self._namedict.get(obj[tags.REF])

    def _restore_type(self, obj):
        typeref = loadclass(obj[tags.TYPE])
        if typeref is None:
            return obj
        return typeref

    def _restore_repr(self, obj):
        if self.safe:
            # eval() is not allowed in safe mode
            return None
        obj = loadrepr(obj[tags.REPR])
        return self._mkref(obj)

    def _restore_object(self, obj):
        class_name = obj[tags.OBJECT]
        cls = loadclass(class_name)
        handler = handlers.get(cls, handlers.get(class_name))
        if handler is not None:  # custom handler
            proxy = _Proxy()
            self._mkref(proxy)
            instance = handler(self).restore(obj)
            proxy.reset(instance)
            self._swapref(proxy, instance)
            return instance

        if cls is None:
            return self._mkref(obj)

        return self._restore_object_instance(obj, cls)

    def _restore_function(self, obj):
        return loadclass(obj[tags.FUNCTION])

    def _loadfactory(self, obj):
        try:
            default_factory = obj['default_factory']
        except KeyError:
            return None
        del obj['default_factory']
        return self._restore(default_factory)

    def _restore_object_instance(self, obj, cls):
        # This is a placeholder proxy object which allows child objects to
        # reference the parent object before it has been instantiated.
        proxy = _Proxy()
        self._mkref(proxy)

        # An object can install itself as its own factory, so load the factory
        # after the instance is available for referencing.
        factory = self._loadfactory(obj)

        if has_tag(obj, tags.NEWARGSEX):
            args, kwargs = obj[tags.NEWARGSEX]
        else:
            args = getargs(obj)
            kwargs = {}
        if args:
            args = self._restore(args)
        if kwargs:
            kwargs = self._restore(kwargs)

        is_oldstyle = not (isinstance(cls, type) or getattr(cls, '__meta__', None))
        try:
            if (not is_oldstyle) and hasattr(cls, '__new__'):  # new style classes
                if factory:
                    instance = cls.__new__(cls, factory, *args, **kwargs)
                    instance.default_factory = factory
                else:
                    instance = cls.__new__(cls, *args, **kwargs)
            else:
                instance = object.__new__(cls)
        except TypeError:  # old-style classes
            is_oldstyle = True

        if is_oldstyle:
            try:
                instance = cls(*args)
            except TypeError:  # fail gracefully
                try:
                    instance = make_blank_classic(cls)
                except:  # fail gracefully
                    return self._mkref(obj)

        proxy.reset(instance)
        self._swapref(proxy, instance)

        if isinstance(instance, tuple):
            return instance

        if (hasattr(instance, 'default_factory') and
                isinstance(instance.default_factory, _Proxy)):
            instance.default_factory = instance.default_factory.get()

        return self._restore_object_instance_variables(obj, instance)

    def _restore_from_dict(self, obj, instance, ignorereserved=True):
        restore_key = self._restore_key_fn()
        method = _obj_setattr

        for k, v in sorted(obj.items(), key=util.itemgetter):
            # ignore the reserved attribute
            if ignorereserved and k in tags.RESERVED:
                continue
            self._namestack.append(k)
            k = restore_key(k)
            # step into the namespace
            value = self._restore(v)
            if (util.is_noncomplex(instance) or
                    util.is_dictionary_subclass(instance)):
                instance[k] = value
            else:
                setattr(instance, k, value)

            # This instance has an instance variable named `k` that is
            # currently a proxy and must be replaced
            if isinstance(value, _Proxy):
                self._proxies.append((instance, k, value, method))

            # step out
            self._namestack.pop()

    def _restore_object_instance_variables(self, obj, instance):
        self._restore_from_dict(obj, instance)

        # Handle list and set subclasses
        if has_tag(obj, tags.SEQ):
            if hasattr(instance, 'append'):
                for v in obj[tags.SEQ]:
                    instance.append(self._restore(v))
            if hasattr(instance, 'add'):
                for v in obj[tags.SEQ]:
                    instance.add(self._restore(v))

        if has_tag(obj, tags.STATE):
            instance = self._restore_state(obj, instance)

        return instance

    def _restore_state(self, obj, instance):
        state = self._restore(obj[tags.STATE])
        has_slots = (isinstance(state, tuple) and len(state) == 2
                     and isinstance(state[1], dict))
        has_slots_and_dict = has_slots and isinstance(state[0], dict)
        if hasattr(instance, '__setstate__'):
            instance.__setstate__(state)
        elif isinstance(state, dict):
            # implements described default handling
            # of state for object with instance dict
            # and no slots
            self._restore_from_dict(state, instance, ignorereserved=False)
        elif has_slots:
            self._restore_from_dict(state[1], instance, ignorereserved=False)
            if has_slots_and_dict:
                self._restore_from_dict(state[0],
                                        instance, ignorereserved=False)
        elif (not hasattr(instance, '__getnewargs__')
              and not hasattr(instance, '__getnewargs_ex__')):
            # __setstate__ is not implemented so that means that the best
            # we can do is return the result of __getstate__() rather than
            # return an empty shell of an object.
            # However, if there were newargs, it's not an empty shell
            instance = state
        return instance

    def _restore_list(self, obj):
        parent = []
        self._mkref(parent)
        children = [self._restore(v) for v in obj]
        parent.extend(children)
        method = _obj_setvalue
        proxies = [(parent, idx, value, method)
                    for idx, value in enumerate(parent)
                        if isinstance(value, _Proxy)]
        self._proxies.extend(proxies)
        return parent

    def _restore_tuple(self, obj):
        return tuple([self._restore(v) for v in obj[tags.TUPLE]])

    def _restore_set(self, obj):
        return set([self._restore(v) for v in obj[tags.SET]])

    def _restore_dict(self, obj):
        data = {}
        restore_key = self._restore_key_fn()
        for k, v in sorted(obj.items(), key=util.itemgetter):
            self._namestack.append(k)
            k = restore_key(k)
            data[k] = self._restore(v)
            self._namestack.pop()
        return data

    def _restore_key_fn(self):
        """Return a callable that restores keys

        This function is responsible for restoring non-string keys
        when we are decoding with `keys=True`.

        """
        # This function is called before entering a tight loop
        # where the returned function will be called.
        # We return a specific function after checking self.keys
        # instead of doing so in the body of the function to
        # avoid conditional branching inside a tight loop.
        if self.keys:
            restore_key = self._restore_pickled_key
        else:
            restore_key = lambda key: key
        return restore_key

    def _restore_pickled_key(self, key):
        if key.startswith(tags.JSON_KEY):
            key = decode(key[len(tags.JSON_KEY):],
                         backend=self.backend, context=self,
                         keys=True, reset=False)
        return key

    def _refname(self):
        """Calculates the name of the current location in the JSON stack.

        This is called as jsonpickle traverses the object structure to
        create references to previously-traversed objects.  This allows
        cyclical data structures such as doubly-linked lists.
        jsonpickle ensures that duplicate python references to the same
        object results in only a single JSON object definition and
        special reference tags to represent each reference.

        >>> u = Unpickler()
        >>> u._namestack = []
        >>> u._refname()
        '/'

        >>> u._namestack = ['a']
        >>> u._refname()
        '/a'

        >>> u._namestack = ['a', 'b']
        >>> u._refname()
        '/a/b'

        """
        return '/' + '/'.join(self._namestack)

    def _mkref(self, obj):
        obj_id = id(obj)
        try:
            self._obj_to_idx[obj_id]
        except KeyError:
            self._obj_to_idx[obj_id] = len(self._objs)
            self._objs.append(obj)
            # Backwards compatibility: old versions of jsonpickle
            # produced "py/ref" references.
            self._namedict[self._refname()] = obj
        return obj

    def _swapref(self, proxy, instance):
        proxy_id = id(proxy)
        instance_id = id(instance)

        instance_index = self._obj_to_idx[proxy_id]
        self._obj_to_idx[instance_id] = instance_index
        del self._obj_to_idx[proxy_id]

        self._objs[instance_index] = instance
        self._namedict[self._refname()] = instance


def loadclass(module_and_name):
    """Loads the module and returns the class.

    >>> cls = loadclass('datetime.datetime')
    >>> cls.__name__
    'datetime'

    >>> loadclass('does.not.exist')

    >>> loadclass('__builtin__.int')()
    0

    """
    try:
        module, name = module_and_name.rsplit('.', 1)
        module = util.untranslate_module_name(module)
        __import__(module)
        return getattr(sys.modules[module], name)
    except:
        return None


def getargs(obj):
    """Return arguments suitable for __new__()"""
    # Let saved newargs take precedence over everything
    if has_tag(obj, tags.NEWARGSEX):
        raise ValueError("__newargs_ex__ returns both args and kwargs")

    if has_tag(obj, tags.NEWARGS):
        return obj[tags.NEWARGS]

    if has_tag(obj, tags.INITARGS):
        return obj[tags.INITARGS]

    try:
        seq_list = obj[tags.SEQ]
        obj_dict = obj[tags.OBJECT]
    except KeyError:
        return []
    typeref = loadclass(obj_dict)
    if not typeref:
        return []
    if hasattr(typeref, '_fields'):
        if len(typeref._fields) == len(seq_list):
            return seq_list
    return []


class _trivialclassic:
    """
    A trivial class that can be instantiated with no args
    """


def make_blank_classic(cls):
    """
    Implement the mandated strategy for dealing with classic classes
    which cannot be instantiated without __getinitargs__ because they
    take parameters
    """
    instance = _trivialclassic()
    instance.__class__ = cls
    return instance


def loadrepr(reprstr):
    """Returns an instance of the object from the object's repr() string.
    It involves the dynamic specification of code.

    >>> obj = loadrepr('datetime/datetime.datetime.now()')
    >>> obj.__class__.__name__
    'datetime'

    """
    module, evalstr = reprstr.split('/')
    mylocals = locals()
    localname = module
    if '.' in localname:
        localname = module.split('.', 1)[0]
    mylocals[localname] = __import__(module)
    return eval(evalstr)


def has_tag(obj, tag):
    """Helper class that tests to see if the obj is a dictionary
    and contains a particular key/tag.

    >>> obj = {'test': 1}
    >>> has_tag(obj, 'test')
    True
    >>> has_tag(obj, 'fail')
    False

    >>> has_tag(42, 'fail')
    False

    """
    return type(obj) is dict and tag in obj

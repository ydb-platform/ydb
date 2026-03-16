"""
This module contains items that are "missing" from the Python standard library,
that do miscelleneous things.
"""

import copy
import functools
import inspect
import re
import weakref
from typing import TypeVar

__all__ = [
    "get_name",
    "classproperty",
    "singleton",
    "hashed_singleton",
    "UNSET",
    "Namespace",
    "unflatten",
]


def get_name(frame) -> str:
    """Gets the name of the passed frame.

    :warning: It's very important to delete a stack frame after you're done
           using it, as it can cause circular references that prevents
           garbage collection.

    :param frame: Stack frame to inspect.
    :returns: Name of the frame in the form *module.class.method*.

    """
    module = inspect.getmodule(frame)

    name = frame.f_code.co_name
    if frame.f_code.co_varnames:
        # Does this method belong to a class?
        try:
            varname = frame.f_code.co_varnames[0]
            # The class or instance should be the first argument,
            # unless it was otherwise munged by a decorator or is a
            # @staticmethod
            maybe_cls = frame.f_locals[varname]

            # Get the actual method, if it exists on the class
            try:
                if isinstance(maybe_cls, type):
                    maybe_func = maybe_cls.__dict__[frame.f_code.co_name]
                else:
                    maybe_func = maybe_cls.__class__.__dict__[frame.f_code.co_name]
            except:  # noqa
                maybe_func = getattr(maybe_cls, frame.f_code.co_name)

            # If we have self, or a classmethod, we need the class name
            if varname in ("self", "cls") or maybe_func.im_self == maybe_cls:
                cls_name = getattr(maybe_cls, "__name__", None) or getattr(
                    getattr(maybe_cls, "__class__", None), "__name__", None
                )

                if cls_name:
                    name = "%s.%s" % (cls_name, name)
                    module = maybe_cls.__module__
        except (KeyError, AttributeError):
            # Probably not a class method, so fuck it
            pass

    if module:
        if not isinstance(module, (str, bytes)):
            module = module.__name__
        if name != "<module>":
            return "%s.%s" % (module, name)
        else:
            return module
    else:
        return name


def classproperty(func):
    """
    Makes a ``@classmethod`` style property (since ``@property`` only works on
    instances).

    ::

        from pytool.lang import classproperty

        class MyClass(object):
            _attr = 'Hello World'

            @classproperty
            def attr(cls):
                return cls._attr

        MyClass.attr # 'Hello World'
        MyClass().attr # Still 'Hello World'

    """

    def __get__(self, instance, owner):
        return func(owner)

    return type(
        func.__name__,
        (object,),
        {
            "__get__": __get__,
            "__module__": func.__module__,
            "__doc__": func.__doc__,
        },
    )()


_Singleton = TypeVar("_Singleton", bound=object)


def singleton(klass: _Singleton) -> _Singleton:
    """Wraps a class to create a singleton version of it.

    :param klass: Class to decorate

    .. versionchanged:: 3.4.2

        `@singleton` wrapped classes now preserve their `@staticmethod`
        functions on the class type as well as the instance.

    Example usage::

        # Make a class directly behave as a singleton
        @singleton
        class Test(object):
            pass

        # Make an imported class behave as a singleton
        Test = singleton(Test)

    """
    cls_dict = {"_singleton": None}

    # Mirror original class
    cls_name = klass.__name__
    for attr in functools.WRAPPER_ASSIGNMENTS:
        if hasattr(klass, attr):
            cls_dict[attr] = getattr(klass, attr)

    # Preserve static methods on the wrapped class type
    for attr in klass.__dict__:
        if isinstance(klass.__dict__[attr], staticmethod):
            cls_dict[attr] = klass.__dict__[attr]

    # Make new method that controls singleton behavior
    def __new__(cls, *args, **kwargs):
        if not cls._singleton:
            cls._singleton = klass(*args, **kwargs)
        return cls._singleton

    # Add new method to singleton class dict
    cls_dict["__new__"] = __new__

    # Build and return new class
    return type(cls_name, (object,), cls_dict)


def hashed_singleton(klass: _Singleton) -> _Singleton:
    """Wraps a class to create a hashed singleton version of it. A hashed
    singleton is like a singleton in that there will be only a single
    instance of the class for each call signature.

    The singleton is kept as a `weak reference
    <http://docs.python.org/2/library/weakref.html>`_, so if your program
    ceases to reference the hashed singleton, you may get a new instance if
    the Python interpreter has garbage collected your original instance.

    This will not work for classes that take arguments that are unhashable
    (e.g. dicts, sets).

    :param klass: Class to decorate

    .. versionadded:: 2.1

    .. versionchanged:: 3.4.2

        `@hashed_singleton` wrapped classes now preserve their
        `@staticmethod` functions on the class type as well as the
        instance.

    Example usage::

        # Make a class directly behave as a hashed singleton
        @hashed_singleton
        class Test(object):
            def __init__(self, *args, **kwargs):
                pass

        # Make an imported class behave as a hashed singleton
        Test = hashed_singleton(Test)

        # The same arguments give you the same class instance back
        test = Test('a', k='k')
        test is Test('a', k='k') # True

        # A different argument signature will give you a new instance
        test is Test('b', k='k') # False
        test is Test('a', k='j') # False

        # Removing all references to a hashed singleton instance will allow
        # it to be garbage collected like normal, because it's only kept
        # as a weak reference
        del test
        test = Test('a', k='k') # If the Python interpreter has garbage
                                # collected, you will get a new instance


    """
    cls_dict = {"_singletons": weakref.WeakValueDictionary()}

    # Mirror original class
    cls_name = klass.__name__
    for attr in functools.WRAPPER_ASSIGNMENTS:
        if hasattr(klass, attr):
            cls_dict[attr] = getattr(klass, attr)

    # Preserve static methods on the wrapped class type
    for attr in klass.__dict__:
        if isinstance(klass.__dict__[attr], staticmethod):
            cls_dict[attr] = klass.__dict__[attr]

    # Make new method that controls singleton behavior
    def __new__(cls, *args, **kwargs):
        hashable_kwargs = tuple(sorted(kwargs.items()))
        signature = (args, hashable_kwargs)

        if signature not in cls._singletons:
            obj = klass(*args, **kwargs)
            cls._singletons[signature] = obj
        else:
            obj = cls._singletons[signature]

        return obj

    # Add new method to singleton class dict
    cls_dict["__new__"] = __new__

    # Build and return new class
    return type(cls_name, (object,), cls_dict)


class _UNSETMeta(type):
    def __nonzero__(cls):
        return False

    def __bool__(cls):
        # Python 3
        return False

    def __len__(cls):
        return 0

    def __eq__(cls, other):
        if cls is other:
            return True
        if not other:
            return True
        return False

    def __iter__(cls):
        return cls

    def next(cls):
        raise StopIteration()

    # Python 3
    __next__ = next

    def __repr__(cls):
        return "UNSET"


class UNSET(object, metaclass=_UNSETMeta):
    """Special class that evaluates to ``bool(False)``, but can be distinctly
    identified as seperate from ``None`` or ``False``. This class can and
    should be used without instantiation.

    ::

        >>> from pytool.lang import UNSET
        >>> # Evaluates to False
        >>> bool(UNSET)
        False
        >>> # Is a class-singleton (cannot become an instance)
        >>> UNSET() is UNSET
        True
        >>> # Is good for checking default values
        >>> if {}.get('example', UNSET) is UNSET:
        ...     print "Key is missing."
        ...
        Key is missing.
        >>> # Has no length
        >>> len(UNSET)
        0
        >>> # Is iterable, but has no iterations
        >>> list(UNSET)
        []
        >>> # It has a repr() equal to itself
        >>> UNSET
        UNSET

    """

    def __new__(cls):
        return cls


class Namespace(object):
    """
    Namespace object used for creating arbitrary data spaces. This can be used
    to create nested namespace objects. It can represent itself as a dictionary
    of dot notation keys.

    .. rubric:: Basic usage:

    ::

        >>> from pytool.lang import Namespace
        >>> # Namespaces automatically nest
        >>> myns = Namespace()
        >>> myns.hello = 'world'
        >>> myns.example.value = True
        >>> # Namespaces can be converted to dictionaries
        >>> myns.as_dict()
        {'hello': 'world', 'example.value': True}
        >>> # Namespaces have container syntax
        >>> 'hello' in myns
        True
        >>> 'example.value' in myns
        True
        >>> 'example.banana' in myns
        False
        >>> 'example' in myns
        True
        >>> # Namespaces are iterable
        >>> for name, value in myns:
        ...     print name, value
        ...
        hello world
        example.value True
        >>> # Namespaces that are empty evaluate as False
        >>> bool(Namespace())
        False
        >>> bool(myns.notset)
        False
        >>> bool(myns)
        True
        >>> # Namespaces allow the __get__ portion of the descriptor protocol
        >>> # to work on instances (normally they would not)
        >>> class MyDescriptor(object):
        ...     def __get__(self, instance, owner):
        ...         return 'Hello World'
        ...
        >>> myns.descriptor = MyDescriptor()
        >>> myns.descriptor
        'Hello World'
        >>> # Namespaces can be created from dictionaries
        >>> newns = Namespace({'foo': {'bar': 1}})
        >>> newns.foo.bar
        1
        >>> # Namespaces will expand dot-notation dictionaries
        >>> dotns = Namespace({'foo.bar': 2})
        >>> dotns.foo.bar
        2
        >>> # Namespaces will coerce list-like dictionaries into lists
        >>> listns = Namespace({'listish': {'0': 'zero', '1': 'one'}})
        >>> listns.listish
        ['zero', 'one']
        >>> # Namespaces can be deepcopied
        >>> a = Namespace({'foo': [[1, 2], 3]}
        >>> b = a.copy()
        >>> b.foo[0][0] = 9
        >>> a.foo
        [[1, 2], 3]
        >>> b.foo
        [[9, 2], 3]
        >>> # You can access keys using dict-like syntax, which is useful
        >>> myns.foo.bar = True
        >>> myns['foo'].bar
        True
        >>> # Dict-like access lets you traverse namespaces
        >>> myns['foo.bar']
        True
        >>> # Dict-like access lets you traverse lists as well
        >>> listns['listish.0']
        'zero'
        >>> listns['listish.1']
        'one'
        >>> # Dict-like access lets you traverse nested lists and namespaces
        >>> nested = Namespace()
        >>> nested.values = []
        >>> nested.values.append(Namespace({'foo': 'bar'}))
        >>> nested['values.0.foo']
        'bar'
        >>> # You can also call the traversal method if you need
        >>> nested.traversal(['values', 0, 'foo'])
        'bar'

    Namespaces are useful!

    .. versionadded:: 3.5.0

        Added the ability to create Namespace instances from dictionaries.

    .. versionadded:: 3.6.0

        Added the ability to handle dot-notation keys and list-like dicts.

    .. versionadded:: 3.7.0

        Added deepcopy capability to Namespaces.

    .. versionadded:: 3.8.0

        Added dict-like access capability to Namespaces.

    .. versionadded:: 3.9.0

        Added traversal by key/index arrays for nested Namespaces and lists

    """

    _VALID_NAME = re.compile("^[a-zA-Z0-9_.]+$")

    def __init__(self, obj=None):
        if obj is not None:
            # Populate the namespace from the give dictionary
            self.from_dict(obj)

    def __getattribute__(self, name):
        # Implement descriptor protocol for reading
        value = object.__getattribute__(self, name)
        if not isinstance(value, Namespace) and hasattr(value, "__get__"):
            value = value.__get__(self, self.__class__)
        return value

    # Allow for dict-like key access and traversal
    def __getitem__(self, item):
        if isinstance(item, (str, bytes)) and "." in item:
            return self.traverse(item.split("."))
        try:
            return self.__getattribute__(item)
        except AttributeError:
            return self.__getattr__(item)

    def __getattr__(self, name):
        # Allow implicit nested namespaces by attribute access
        new_space = type(self)()
        setattr(self, name, new_space)
        return new_space

    def __iter__(self):
        return self.iteritems()

    def __contains__(self, name):
        names = name.split(".")

        obj = self
        for name in names:
            # Easy check for membership without triggering __getattr__ and
            # creating new empty Namespace attributes in the checked object
            if isinstance(obj, Namespace) and name not in obj.__dict__:
                return False

            # Otherwise try to continue down the tree in a normal way
            obj = getattr(obj, name)

        # Check the Namespace object for emptiness
        if isinstance(obj, Namespace):
            return bool(obj.__dict__)

        # Otherwise we found what we wanted
        return True

    def __nonzero__(self):
        return bool(self.__dict__)

    def __bool__(self):
        # For Python 3
        return bool(self.__dict__)

    def iteritems(self, base_name=None):
        """Return generator which returns ``(key, value)`` tuples.

        :param str base_name: Base namespace (optional)

        """
        for name in self.__dict__.keys():
            value = getattr(self, name)

            if base_name:
                name = base_name + "." + name

            # Allow for nested namespaces
            if isinstance(value, Namespace):
                for subkey in value.iteritems(name):
                    yield subkey
            else:
                yield name, value

    def items(self, base_name=None):
        """Return generator which returns ``(key, value)`` tuples.

        Analagous to dict.items() behavior in Python3

        :param str base_name: Base namespace (optional)

        """
        return self.iteritems(base_name)

    def as_dict(self, base_name=None):
        """Return the current namespace as a dictionary.

        :param str base_name: Base namespace (optional)

        """
        space = dict(self.iteritems(base_name))
        for key, value in list(space.items()):
            if isinstance(value, list):
                # We have to copy the list before mutating its items to avoid
                # altering the original namespace
                value = copy.copy(value)
                space[key] = value
                for i in range(len(value)):
                    if isinstance(value[i], Namespace):
                        value[i] = value[i].as_dict()
        return space

    def for_json(self, base_name=None):
        """Return the current namespace as a JSON suitable nested dictionary.

        :param str base_name: Base namespace (optional)

        This is compatible with the :mod:`simplejson` `for_json`
        behavior flag to recursively encode objects.

        Example::

            import simplejson

            json_str = simplejson.dumps(my_namespace, for_json=True)

        """
        target = {}
        obj = target if not base_name else {base_name: target}

        for key in self.__dict__.keys():
            value = getattr(self, key)
            target[key] = value

            if isinstance(value, Namespace):
                value = value.for_json()
                target[key] = value
                continue

            if isinstance(value, list):
                value = copy.copy(value)
                target[key] = value

                for i in range(len(value)):
                    if isinstance(value[i], Namespace):
                        value[i] = value[i].for_json()

        return obj

    def from_dict(self, obj):
        """Populate this Namespace from the given *obj* dictionary.

        :param dict obj: Dictionary object to merge into this Namespace

        .. versionadded:: 3.5.0

        """
        obj = unflatten(obj)

        assert isinstance(obj, dict), "Bad Namespace value: '{!r}'".format(obj)

        def _coerce_value(value):
            """Helps coerce values to Namespaces recursively."""
            if isinstance(value, dict):
                return type(self)(value)
            elif isinstance(value, list):
                # We have to copy the list so we can modify in place without
                # breaking things
                value = copy.copy(value)
                for i in range(len(value)):
                    value[i] = _coerce_value(value[i])
            return value

        for key, value in obj.items():
            assert self._VALID_NAME.match(key), "Invalid name: {!r}".format(key)
            value = _coerce_value(value)
            setattr(self, key, value)

    def __repr__(self):
        return "<{}({})>".format(type(self).__name__, self.as_dict())

    def copy(self, *args, **kwargs):
        """Return a copy of a Namespace by writing it to a dict and then
        writing back to a Namespace.

        Arguments to this method are ignored.

        """
        return type(self)(self.as_dict())

    # Aliases for the stdlib copy module
    __copy__ = copy
    __deepcopy__ = copy

    def traverse(self, path):
        """Traverse the Namespace and any nested elements by following the
        elements in an iterable *path* and return the item found at the end
        of *path*.

        Traversal is achieved using the __getitem__ method, allowing for
        traversal of nested structures such as arrays and dictionaries.

        AttributeError is raised if one of the attributes in *path* does
        not exist at the expected depth.

        :param iterable path: An iterable whose elements specify the keys
            to path over.

        Example usage::

            ns = Namespace({"foo":
                            [Namespace({"name": "john"}),
                            Namespace({"name": "jane"})]})
            ns.traverse(["foo", 1, "name"])  # Returns "jane"
        """
        ns = self
        for key in path:
            try:
                ns = ns[key]
            except TypeError as err:
                # This can happen if key is a str, but ns is a list
                try:
                    # Try type coercion to help list indexing
                    ns = ns[int(key)]
                except ValueError:
                    # Raise the original error, not the coertion error
                    raise err
        return ns


class Keyspace(Namespace):
    """
    Keyspace object which extends Namespaces by allowing item assignment and
    arbitrary key names instead of just python attribute compatible names.

    Example::

        # This would be an error with a Namespace
        my_ns['foobar'] = True

        # This works with a Keyspace
        my_ks.foo['foobar'].bar['you'] = True

        # This would be an error with a Namespace
        Namespace({'key-name': True)

        # This works with a Keyspace
        Keyspace({'key-name': True)

    .. versionadded:: 3.16.0

    """

    _VALID_NAME = re.compile(".*")

    def __init__(self, obj=None):
        super(Keyspace, self).__init__(obj)

    def __setitem__(self, key, value):
        self.__dict__[key] = value


def _split_keys(obj):
    """
    Return a generator that yields 2-tuples of lists representing dot-notation
    keys split on the dots, and their values in *obj*.

    Example::

        {'foo.bar': 0, 'foo.spam': 1, 'parrot': 2}

        ... yields ...

        (['foo', 'bar'], 0)
        (['foo', 'spam'], 1)
        (['parrot'], 2)

    """
    assert isinstance(obj, dict)

    for key, value in obj.items():
        if not isinstance(key, str):
            yield [key], value
        else:
            yield key.split("."), value


def _unflatten(obj):
    """
    Return *obj* having dot-notation keys unflattened.

    :param obj: Arbitrary object (preferably a dict) to unflatten

    """
    # Check if we have something other than a dict
    if not isinstance(obj, dict):
        # If it's a list, we return after _unflattening the list items
        if isinstance(obj, list):
            return [_unflatten(v) for v in obj]
        # If it's anything else we just return the value
        return obj

    # Create the new unflattened dict... could mutate but that'd get messy
    expanded = {}

    # Iterate over our object's keys, looking for dot notation
    for key, value in _split_keys(obj):
        # If there's a single item, then it's a simple key and we recurse
        if len(key) == 1:
            key = key[0]
            expanded[key] = _unflatten(value)
            continue

        # Set the top level dict so we can walk down into it
        current = expanded

        # Get our ending index for the split key, so we know when to assign a
        # value instead of iterating again
        end = len(key) - 1

        # Iterate over the key parts, walking down the dict tree
        for i in range(len(key)):
            # Get the part of the key
            part = key[i]

            # If the part is not in the current walk level ...
            if part not in current:
                # We check if we're at the end, and recursively assign a value
                if i == end:
                    current[part] = _unflatten(value)
                    break

                # Or create a new walk level and continue
                current[part] = {}

            # If we get here, something went very wrong
            if i == end:
                raise ValueError("Value already assigned")

            # Continue walking down the key parts
            current = current[part]

    return expanded


def _join_lists(obj):
    """
    Return *obj* with list-like dictionary objects converted to actual lists.

    :param obj: Arbitrary object

    Example::

        {'0': 'apple', '1': 'pear', '2': 'orange'}

        ... returns ...

        ['apple', 'pear', 'orange']

    """
    # If it's not a dict, it's definitely not a dict-list
    if not isinstance(obj, dict):
        # If it's a list-list then we want to recurse into it
        if isinstance(obj, list):
            return [_join_lists(v) for v in obj]
        # Otherwise just get out
        return obj

    # If there's not a '0' key it's not a possible list
    if "0" not in obj and 0 not in obj:
        # Recurse into it
        for key, value in obj.items():
            obj[key] = _join_lists(value)

        return obj

    # Make sure the all the keys parse into integers, otherwise it's not a list
    try:
        items = [(int(k), v) for k, v in obj.items()]
    except ValueError:
        return obj

    # Sort the integer keys to get the original list order
    items = sorted(items)

    # Check all the keys to ensure there's no missing indexes
    i = 0
    for key, value in items:
        # If the index key is out of sequence we abandon
        if key != i:
            return obj

        # Replace the item with its value
        items[i] = value

        # Increment to check the next one
        i += 1

    return items


def unflatten(obj):
    """
    Return *obj* with dot-notation keys unflattened into nested dictionaries,
    as well as list-like dictionaries converted into list instances.

    :param obj: An arbitrary object, preferably a dict

    """
    obj = _unflatten(obj)
    obj = _join_lists(obj)
    return obj

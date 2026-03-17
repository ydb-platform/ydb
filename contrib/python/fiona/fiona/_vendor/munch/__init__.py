""" Munch is a subclass of dict with attribute-style access.

    >>> b = Munch()
    >>> b.hello = 'world'
    >>> b.hello
    'world'
    >>> b['hello'] += "!"
    >>> b.hello
    'world!'
    >>> b.foo = Munch(lol=True)
    >>> b.foo.lol
    True
    >>> b.foo is b['foo']
    True

    It is safe to import * from this module:

        __all__ = ('Munch', 'munchify','unmunchify')

    un/munchify provide dictionary conversion; Munches can also be
    converted via Munch.to/fromDict().
"""

from collections.abc import Mapping

__version__ = "2.5.0"
VERSION = tuple(map(int, __version__.split('.')[:3]))

__all__ = ('Munch', 'munchify', 'DefaultMunch', 'DefaultFactoryMunch', 'unmunchify')



class Munch(dict):
    """ A dictionary that provides attribute-style access.

        >>> b = Munch()
        >>> b.hello = 'world'
        >>> b.hello
        'world'
        >>> b['hello'] += "!"
        >>> b.hello
        'world!'
        >>> b.foo = Munch(lol=True)
        >>> b.foo.lol
        True
        >>> b.foo is b['foo']
        True

        A Munch is a subclass of dict; it supports all the methods a dict does...

        >>> sorted(b.keys())
        ['foo', 'hello']

        Including update()...

        >>> b.update({ 'ponies': 'are pretty!' }, hello=42)
        >>> print (repr(b))
        Munch({'ponies': 'are pretty!', 'foo': Munch({'lol': True}), 'hello': 42})

        As well as iteration...

        >>> sorted([ (k,b[k]) for k in b ])
        [('foo', Munch({'lol': True})), ('hello', 42), ('ponies', 'are pretty!')]

        And "splats".

        >>> "The {knights} who say {ni}!".format(**Munch(knights='lolcats', ni='can haz'))
        'The lolcats who say can haz!'

        See unmunchify/Munch.toDict, munchify/Munch.fromDict for notes about conversion.
    """
    def __init__(self, *args, **kwargs):  # pylint: disable=super-init-not-called
        self.update(*args, **kwargs)

    # only called if k not found in normal places
    def __getattr__(self, k):
        """ Gets key if it exists, otherwise throws AttributeError.

            nb. __getattr__ is only called if key is not found in normal places.

            >>> b = Munch(bar='baz', lol={})
            >>> b.foo
            Traceback (most recent call last):
                ...
            AttributeError: foo

            >>> b.bar
            'baz'
            >>> getattr(b, 'bar')
            'baz'
            >>> b['bar']
            'baz'

            >>> b.lol is b['lol']
            True
            >>> b.lol is getattr(b, 'lol')
            True
        """
        try:
            # Throws exception if not in prototype chain
            return object.__getattribute__(self, k)
        except AttributeError:
            try:
                return self[k]
            except KeyError:
                raise AttributeError(k)

    def __setattr__(self, k, v):
        """ Sets attribute k if it exists, otherwise sets key k. A KeyError
            raised by set-item (only likely if you subclass Munch) will
            propagate as an AttributeError instead.

            >>> b = Munch(foo='bar', this_is='useful when subclassing')
            >>> hasattr(b.values, '__call__')
            True
            >>> b.values = 'uh oh'
            >>> b.values
            'uh oh'
            >>> b['values']
            Traceback (most recent call last):
                ...
            KeyError: 'values'
        """
        try:
            # Throws exception if not in prototype chain
            object.__getattribute__(self, k)
        except AttributeError:
            try:
                self[k] = v
            except:
                raise AttributeError(k)
        else:
            object.__setattr__(self, k, v)

    def __delattr__(self, k):
        """ Deletes attribute k if it exists, otherwise deletes key k. A KeyError
            raised by deleting the key--such as when the key is missing--will
            propagate as an AttributeError instead.

            >>> b = Munch(lol=42)
            >>> del b.lol
            >>> b.lol
            Traceback (most recent call last):
                ...
            AttributeError: lol
        """
        try:
            # Throws exception if not in prototype chain
            object.__getattribute__(self, k)
        except AttributeError:
            try:
                del self[k]
            except KeyError:
                raise AttributeError(k)
        else:
            object.__delattr__(self, k)

    def toDict(self):
        """ Recursively converts a munch back into a dictionary.

            >>> b = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
            >>> sorted(b.toDict().items())
            [('foo', {'lol': True}), ('hello', 42), ('ponies', 'are pretty!')]

            See unmunchify for more info.
        """
        return unmunchify(self)

    @property
    def __dict__(self):
        return self.toDict()

    def __repr__(self):
        """ Invertible* string-form of a Munch.

            >>> b = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
            >>> print (repr(b))
            Munch({'ponies': 'are pretty!', 'foo': Munch({'lol': True}), 'hello': 42})
            >>> eval(repr(b))
            Munch({'ponies': 'are pretty!', 'foo': Munch({'lol': True}), 'hello': 42})

            >>> with_spaces = Munch({1: 2, 'a b': 9, 'c': Munch({'simple': 5})})
            >>> print (repr(with_spaces))
            Munch({'a b': 9, 1: 2, 'c': Munch({'simple': 5})})
            >>> eval(repr(with_spaces))
            Munch({'a b': 9, 1: 2, 'c': Munch({'simple': 5})})

            (*) Invertible so long as collection contents are each repr-invertible.
        """
        return '{0}({1})'.format(self.__class__.__name__, dict.__repr__(self))

    def __dir__(self):
        return list(self.keys())

    def __getstate__(self):
        """ Implement a serializable interface used for pickling.

        See https://docs.python.org/3.6/library/pickle.html.
        """
        return {k: v for k, v in self.items()}

    def __setstate__(self, state):
        """ Implement a serializable interface used for pickling.

        See https://docs.python.org/3.6/library/pickle.html.
        """
        self.clear()
        self.update(state)

    __members__ = __dir__  # for python2.x compatibility

    @classmethod
    def fromDict(cls, d):
        """ Recursively transforms a dictionary into a Munch via copy.

            >>> b = Munch.fromDict({'urmom': {'sez': {'what': 'what'}}})
            >>> b.urmom.sez.what
            'what'

            See munchify for more info.
        """
        return munchify(d, cls)

    def copy(self):
        return type(self).fromDict(self)

    def update(self, *args, **kwargs):
        """
        Override built-in method to call custom __setitem__ method that may
        be defined in subclasses.
        """
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def get(self, k, d=None):
        """
        D.get(k[,d]) -> D[k] if k in D, else d.  d defaults to None.
        """
        if k not in self:
            return d
        return self[k]

    def setdefault(self, k, d=None):
        """
        D.setdefault(k[,d]) -> D.get(k,d), also set D[k]=d if k not in D
        """
        if k not in self:
            self[k] = d
        return self[k]


class AutoMunch(Munch):
    def __setattr__(self, k, v):
        """ Works the same as Munch.__setattr__ but if you supply
            a dictionary as value it will convert it to another Munch.
        """
        if isinstance(v, Mapping) and not isinstance(v, (AutoMunch, Munch)):
            v = munchify(v, AutoMunch)
        super(AutoMunch, self).__setattr__(k, v)


class DefaultMunch(Munch):
    """
    A Munch that returns a user-specified value for missing keys.
    """

    def __init__(self, *args, **kwargs):
        """ Construct a new DefaultMunch. Like collections.defaultdict, the
            first argument is the default value; subsequent arguments are the
            same as those for dict.
        """
        # Mimic collections.defaultdict constructor
        if args:
            default = args[0]
            args = args[1:]
        else:
            default = None
        super(DefaultMunch, self).__init__(*args, **kwargs)
        self.__default__ = default

    def __getattr__(self, k):
        """ Gets key if it exists, otherwise returns the default value."""
        try:
            return super(DefaultMunch, self).__getattr__(k)
        except AttributeError:
            return self.__default__

    def __setattr__(self, k, v):
        if k == '__default__':
            object.__setattr__(self, k, v)
        else:
            super(DefaultMunch, self).__setattr__(k, v)

    def __getitem__(self, k):
        """ Gets key if it exists, otherwise returns the default value."""
        try:
            return super(DefaultMunch, self).__getitem__(k)
        except KeyError:
            return self.__default__

    def __getstate__(self):
        """ Implement a serializable interface used for pickling.

        See https://docs.python.org/3.6/library/pickle.html.
        """
        return (self.__default__, {k: v for k, v in self.items()})

    def __setstate__(self, state):
        """ Implement a serializable interface used for pickling.

        See https://docs.python.org/3.6/library/pickle.html.
        """
        self.clear()
        default, state_dict = state
        self.update(state_dict)
        self.__default__ = default

    @classmethod
    def fromDict(cls, d, default=None):
        # pylint: disable=arguments-differ
        return munchify(d, factory=lambda d_: cls(default, d_))

    def copy(self):
        return type(self).fromDict(self, default=self.__default__)

    def __repr__(self):
        return '{0}({1!r}, {2})'.format(
            type(self).__name__, self.__undefined__, dict.__repr__(self))


class DefaultFactoryMunch(Munch):
    """ A Munch that calls a user-specified function to generate values for
        missing keys like collections.defaultdict.

        >>> b = DefaultFactoryMunch(list, {'hello': 'world!'})
        >>> b.hello
        'world!'
        >>> b.foo
        []
        >>> b.bar.append('hello')
        >>> b.bar
        ['hello']
    """

    def __init__(self, default_factory, *args, **kwargs):
        super(DefaultFactoryMunch, self).__init__(*args, **kwargs)
        self.default_factory = default_factory

    @classmethod
    def fromDict(cls, d, default_factory):
        # pylint: disable=arguments-differ
        return munchify(d, factory=lambda d_: cls(default_factory, d_))

    def copy(self):
        return type(self).fromDict(self, default_factory=self.default_factory)

    def __repr__(self):
        factory = self.default_factory.__name__
        return '{0}({1}, {2})'.format(
            type(self).__name__, factory, dict.__repr__(self))

    def __setattr__(self, k, v):
        if k == 'default_factory':
            object.__setattr__(self, k, v)
        else:
            super(DefaultFactoryMunch, self).__setattr__(k, v)

    def __missing__(self, k):
        self[k] = self.default_factory()
        return self[k]


# While we could convert abstract types like Mapping or Iterable, I think
# munchify is more likely to "do what you mean" if it is conservative about
# casting (ex: isinstance(str,Iterable) == True ).
#
# Should you disagree, it is not difficult to duplicate this function with
# more aggressive coercion to suit your own purposes.

def munchify(x, factory=Munch):
    """ Recursively transforms a dictionary into a Munch via copy.

        >>> b = munchify({'urmom': {'sez': {'what': 'what'}}})
        >>> b.urmom.sez.what
        'what'

        munchify can handle intermediary dicts, lists and tuples (as well as
        their subclasses), but ymmv on custom datatypes.

        >>> b = munchify({ 'lol': ('cats', {'hah':'i win again'}),
        ...         'hello': [{'french':'salut', 'german':'hallo'}] })
        >>> b.hello[0].french
        'salut'
        >>> b.lol[1].hah
        'i win again'

        nb. As dicts are not hashable, they cannot be nested in sets/frozensets.
    """
    # Munchify x, using `seen` to track object cycles
    seen = dict()

    def munchify_cycles(obj):
        # If we've already begun munchifying obj, just return the already-created munchified obj
        try:
            return seen[id(obj)]
        except KeyError:
            pass

        # Otherwise, first partly munchify obj (but without descending into any lists or dicts) and save that
        seen[id(obj)] = partial = pre_munchify(obj)
        # Then finish munchifying lists and dicts inside obj (reusing munchified obj if cycles are encountered)
        return post_munchify(partial, obj)

    def pre_munchify(obj):
        # Here we return a skeleton of munchified obj, which is enough to save for later (in case
        # we need to break cycles) but it needs to filled out in post_munchify
        if isinstance(obj, Mapping):
            return factory({})
        elif isinstance(obj, list):
            return type(obj)()
        elif isinstance(obj, tuple):
            type_factory = getattr(obj, "_make", type(obj))
            return type_factory(munchify_cycles(item) for item in obj)
        else:
            return obj

    def post_munchify(partial, obj):
        # Here we finish munchifying the parts of obj that were deferred by pre_munchify because they
        # might be involved in a cycle
        if isinstance(obj, Mapping):
            partial.update((k, munchify_cycles(obj[k])) for k in obj.keys())
        elif isinstance(obj, list):
            partial.extend(munchify_cycles(item) for item in obj)
        elif isinstance(obj, tuple):
            for (item_partial, item) in zip(partial, obj):
                post_munchify(item_partial, item)

        return partial

    return munchify_cycles(x)


def unmunchify(x):
    """ Recursively converts a Munch into a dictionary.

        >>> b = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
        >>> sorted(unmunchify(b).items())
        [('foo', {'lol': True}), ('hello', 42), ('ponies', 'are pretty!')]

        unmunchify will handle intermediary dicts, lists and tuples (as well as
        their subclasses), but ymmv on custom datatypes.

        >>> b = Munch(foo=['bar', Munch(lol=True)], hello=42,
        ...         ponies=('are pretty!', Munch(lies='are trouble!')))
        >>> sorted(unmunchify(b).items()) #doctest: +NORMALIZE_WHITESPACE
        [('foo', ['bar', {'lol': True}]), ('hello', 42), ('ponies', ('are pretty!', {'lies': 'are trouble!'}))]

        nb. As dicts are not hashable, they cannot be nested in sets/frozensets.
    """

    # Munchify x, using `seen` to track object cycles
    seen = dict()

    def unmunchify_cycles(obj):
        # If we've already begun unmunchifying obj, just return the already-created unmunchified obj
        try:
            return seen[id(obj)]
        except KeyError:
            pass

        # Otherwise, first partly unmunchify obj (but without descending into any lists or dicts) and save that
        seen[id(obj)] = partial = pre_unmunchify(obj)
        # Then finish unmunchifying lists and dicts inside obj (reusing unmunchified obj if cycles are encountered)
        return post_unmunchify(partial, obj)

    def pre_unmunchify(obj):
        # Here we return a skeleton of unmunchified obj, which is enough to save for later (in case
        # we need to break cycles) but it needs to filled out in post_unmunchify
        if isinstance(obj, Mapping):
            return dict()
        elif isinstance(obj, list):
            return type(obj)()
        elif isinstance(obj, tuple):
            type_factory = getattr(obj, "_make", type(obj))
            return type_factory(unmunchify_cycles(item) for item in obj)
        else:
            return obj

    def post_unmunchify(partial, obj):
        # Here we finish unmunchifying the parts of obj that were deferred by pre_unmunchify because they
        # might be involved in a cycle
        if isinstance(obj, Mapping):
            partial.update((k, unmunchify_cycles(obj[k])) for k in obj.keys())
        elif isinstance(obj, list):
            partial.extend(unmunchify_cycles(v) for v in obj)
        elif isinstance(obj, tuple):
            for (value_partial, value) in zip(partial, obj):
                post_unmunchify(value_partial, value)

        return partial

    return unmunchify_cycles(x)


# Serialization

try:
    try:
        import json
    except ImportError:
        import simplejson as json

    def toJSON(self, **options):
        """ Serializes this Munch to JSON. Accepts the same keyword options as `json.dumps()`.

            >>> b = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
            >>> json.dumps(b) == b.toJSON()
            True
        """
        return json.dumps(self, **options)

    def fromJSON(cls, stream, *args, **kwargs):
        """ Deserializes JSON to Munch or any of its subclasses.
        """
        factory = lambda d: cls(*(args + (d,)), **kwargs)
        return munchify(json.loads(stream), factory=factory)

    Munch.toJSON = toJSON
    Munch.fromJSON = classmethod(fromJSON)

except ImportError:
    pass



"""
This module contains implmentations of proxy-list and proxy-dictionary objects.
"""

# flake8: noqa
import collections.abc as abc


def cmp(a, b):
    return (a > b) - (a < b)


class ListProxy(abc.MutableSequence):
    """
    Proxies all methods for a list instance. This is useful when you want to
    modify a list's behavior without copying the list.

    Methods which do not mutate a list, and instead return a new list will
    return a `list` instance rather than a `ListProxy` instance.

    :param data: A list or list-like object (implements all the \
            :class:`collections.MutableSequence` methods)

    .. versionadded:: 2.2

    :note: If you intend to use a subclass which modifies the apparent indices
           or values of this class with :func:`pytool.json.as_json`, remember
           to override :meth:`for_json` to produce the data you desire.

    Example::

        from pytool.proxy import ListProxy

        class SquaredList(ListProxy):
            def __setitem__(self, index, value):
                if isinstance(value, int):
                    value *= value
                super(SquaredList, self).__setitem__(index, value)

        my_list = range(5)
        my_proxy = SquaredList(my_list)
        my_proxy[3] = 5

        my_proxy[3] # 25
        my_list[3] # 25

    """

    def __init__(self, data):
        self._data = data

    def __repr__(self):
        return repr(self._data)

    def __lt__(self, other):
        return self._data < self.__cast(other)

    def __le__(self, other):
        return self._data <= self.__cast(other)

    def __eq__(self, other):
        return self._data == self.__cast(other)

    def __ne__(self, other):
        return self._data != self.__cast(other)

    def __gt__(self, other):
        return self._data > self.__cast(other)

    def __ge__(self, other):
        return self._data >= self.__cast(other)

    def __cast(self, other):
        if isinstance(other, ListProxy):
            return other._data
        else:
            return other

    def __cmp__(self, other):
        return cmp(self._data, self.__cast(other))

    __hash__ = None  # Mutable sequence, so not hashable

    def __contains__(self, item):
        return item in self._data

    def __len__(self):
        return len(self._data)

    def __getitem__(self, i):
        return self._data[i]

    def __setitem__(self, i, item):
        self._data[i] = item

    def __delitem__(self, i):
        del self._data[i]

    def __getslice__(self, i, j):
        i = max(i, 0)
        j = max(j, 0)
        return self._data[i:j]

    def __setslice__(self, i, j, other):
        i = max(i, 0)
        j = max(j, 0)
        if isinstance(other, ListProxy):
            self._data[i:j] = other._data
        elif isinstance(other, type(self._data)):
            self._data[i:j] = other
        else:
            self._data[i:j] = list(other)

    def __delslice__(self, i, j):
        i = max(i, 0)
        j = max(j, 0)
        del self._data[i:j]

    def __add__(self, other):
        if isinstance(other, ListProxy):
            return self._data + other._data
        elif isinstance(other, type(self._data)):
            return self._data + other
        else:
            return self._data + list(other)

    def __radd__(self, other):
        if isinstance(other, ListProxy):
            return other._data + self._data
        elif isinstance(other, type(self._data)):
            return other + self._data
        else:
            return list(other) + self._data

    def __iadd__(self, other):
        if isinstance(other, ListProxy):
            self._data += other._data
        elif isinstance(other, type(self._data)):
            self._data += other
        else:
            self._data += list(other)
        return self

    def __mul__(self, n):
        return self._data * n

    __rmul__ = __mul__

    def __imul__(self, n):
        self._data *= n
        return self

    def append(self, item):
        self._data.append(item)

    def insert(self, i, item):
        self._data.insert(i, item)

    def pop(self, i=-1):
        return self._data.pop(i)

    def remove(self, item):
        self._data.remove(item)

    def count(self, item):
        return self._data.count(item)

    def index(self, item, *args):
        return self._data.index(item, *args)

    def reverse(self):
        self._data.reverse()

    def sort(self, *args, **kwds):
        self._data.sort(*args, **kwds)

    def extend(self, other):
        if isinstance(other, ListProxy):
            self._data.extend(other._data)
        else:
            self._data.extend(other)

    def for_json(self):
        return self._data


class DictProxy(abc.MutableMapping):
    """
    Proxies all methods for a dict instance.

    This is useful when you want to modify a dictionary's behavior through
    subclassing without copying the dictionary or if you want to be able to
    modify the original dictionary.

    :param data: A dict or dict-like object (implements all the \
        :class:`collections.MutableMapping` methods)

    .. versionadded:: 2.2

    :note: If you intend to use a subclass which modifies the apparent keys
           or values of this class with :func:`pytool.json.as_json`, remember
           to override :meth:`for_json` to produce the data you desire.

    Example::

        from pytool.proxy import DictProxy

        class SquaredDict(DictProxy):
            def __getitem__(self, key):
                value = super(SquaredDict, self).__getitem__(key)
                if isinstance(value, int):
                    value *= value
                return value

        my_dict = {}
        my_proxy = SquaredDict(my_dict)
        my_proxy['val'] = 5

        my_proxy['val'] # 25
        my_dict['val'] # 5

    """

    def __init__(self, data):
        self._data = data

    def __repr__(self):
        return repr(self._data)

    def __cmp__(self, other):
        if isinstance(other, DictProxy):
            return cmp(self._data, other._data)
        else:
            return cmp(self._data, other)

    __hash__ = None  # Avoid Py3k warning

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        if key in self._data:
            return self._data[key]
        if hasattr(self.__class__, "__missing__"):
            return self.__class__.__missing__(self, key)
        raise KeyError(key)

    def __setitem__(self, key, item):
        self._data[key] = item

    def __delitem__(self, key):
        del self._data[key]

    def clear(self):
        self._data.clear()

    def copy(self):
        if self.__class__ is DictProxy:
            return self._data.copy()
        import copy

        data = self._data
        try:
            self._data = {}
            c = copy.copy(self)
        finally:
            self._data = data
        c.update(self)
        return c

    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    def iteritems(self):
        return self._data.iteritems()

    def iterkeys(self):
        return self._data.iterkeys()

    def itervalues(self):
        return self._data.itervalues()

    def values(self):
        return self._data.values()

    def has_key(self, key):
        return key in self._data

    def update(self, other=None, **kwargs):
        if other is None:
            pass
        elif isinstance(other, DictProxy):
            self._data.update(other._data)
        elif isinstance(other, type({})) or not hasattr(other, "items"):
            self._data.update(other)
        else:
            for k, v in other.items():
                self[k] = v
        if len(kwargs):
            self._data.update(kwargs)

    def get(self, key, failobj=None):
        if key not in self:
            return failobj
        return self[key]

    def setdefault(self, key, failobj=None):
        if key not in self:
            self[key] = failobj
        return self[key]

    def pop(self, key, *args):
        return self._data.pop(key, *args)

    def popitem(self):
        return self._data.popitem()

    def __contains__(self, key):
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def for_json(self):
        return self._data

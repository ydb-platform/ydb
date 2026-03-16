import contextlib
from collections.abc import Mapping, MutableMapping

from sortedcontainers import SortedDict

from .const import Bound
from .interval import Interval


def _sortkey(i):
    # Sort by lower bound, closed first
    return (i[0].lower, i[0].left is Bound.OPEN)


class IntervalDict(MutableMapping):
    """
    An IntervalDict is a dict-like data structure that maps from intervals to data,where
    keys can be single values or Interval instances.

    When keys are Interval instances, its behaviour merely corresponds to range queries
    and it returns IntervalDict instances corresponding to the subset of values covered
    by the given interval. If no matching value is found, an empty IntervalDict is
    returned.

    When keys are "single values", its behaviour corresponds to the one of Python
    built-in dict. When no matching value is found, a KeyError is raised.

    Note that this class does not aim to have the best performance, but is provided
    mainly for convenience. Its performance mainly depends on the number of distinct
    values (not keys) that are stored.
    """

    __slots__ = ("_storage",)

    # Class to use when creating Interval instances
    _klass = Interval

    def __init__(self, mapping_or_iterable=None):
        """
        Return a new IntervalDict.

        If no argument is given, an empty IntervalDict is created. If an argument
        is given, and is a mapping object (e.g., another IntervalDict), an
        new IntervalDict with the same key-value pairs is created. If an
        iterable is provided, it has to be a list of (key, value) pairs.

        :param mapping_or_iterable: optional mapping or iterable.
        """
        self._storage = SortedDict(_sortkey)  # Mapping from intervals to values

        if mapping_or_iterable is not None:
            self.update(mapping_or_iterable)

    @classmethod
    def _from_items(cls, items):
        """
        Fast creation of an IntervalDict with the provided items.

        The items have to satisfy the two following properties: (1) all keys
        must be disjoint intervals and (2) all values must be distinct.

        :param items: list of (key, value) pairs.
        :return: an IntervalDict
        """
        d = cls()
        for key, value in items:
            d._storage[key] = value

        return d

    def clear(self):
        """
        Remove all items from the IntervalDict.
        """
        self._storage.clear()

    def copy(self):
        """
        Return a shallow copy.

        :return: a shallow copy.
        """
        return self.__class__._from_items(self.items())

    def get(self, key, default=None):
        """
        Return the values associated to given key.

        If the key is a single value, it returns a single value (if it exists) or
        the default value. If the key is an Interval, it returns a new IntervalDict
        restricted to given interval. In that case, the default value is used to
        "fill the gaps" (if any) w.r.t. given key.

        :param key: a single value or an Interval instance.
        :param default: default value (default to None).
        :return: an IntervalDict, or a single value if key is not an Interval.
        """
        if isinstance(key, Interval):
            d = self[key]
            d[key - d.domain()] = default
            return d
        else:
            try:
                return self[key]
            except KeyError:
                return default

    def find(self, value):
        """
        Return a (possibly empty) Interval i such that self[i] = value, and
        self[~i] != value.

        :param value: value to look for.
        :return: an Interval instance.
        """
        return self._klass(*(i for i, v in self._storage.items() if v == value))

    def items(self):
        """
        Return a view object on the contained items sorted by their key
        (see https://docs.python.org/3/library/stdtypes.html#dict-views).

        :return: a view object.
        """
        return self._storage.items()

    def keys(self):
        """
        Return a view object on the contained keys (sorted)
        (see https://docs.python.org/3/library/stdtypes.html#dict-views).

        :return: a view object.
        """
        return self._storage.keys()

    def values(self):
        """
        Return a view object on the contained values sorted by their key
        (see https://docs.python.org/3/library/stdtypes.html#dict-views).

        :return: a view object.
        """
        return self._storage.values()

    def domain(self):
        """
        Return an Interval corresponding to the domain of this IntervalDict.

        :return: an Interval.
        """
        return self._klass(*self._storage.keys())

    def pop(self, key, default=None):
        """
        Remove key and return the corresponding value if key is not an Interval.
        If key is an interval, it returns an IntervalDict instance.

        This method combines self[key] and del self[key]. If a default value
        is provided and is not None, it uses self.get(key, default) instead of
        self[key].

        :param key: a single value or an Interval instance.
        :param default: optional default value.
        :return: an IntervalDict, or a single value if key is not an Interval.
        """
        if default is None:
            value = self[key]
            del self[key]
            return value
        else:
            value = self.get(key, default)
            with contextlib.suppress(KeyError):
                del self[key]
            return value

    def popitem(self):
        """
        Remove and return some (key, value) pair as a 2-tuple.
        Raise KeyError if D is empty.

        :return: a (key, value) pair.
        """
        return self._storage.popitem()

    def setdefault(self, key, default=None):
        """
        Return given key. If it does not exist, set its value to given default
        and return it.

        :param key: a single value or an Interval instance.
        :param default: default value (default to None).
        :return: an IntervalDict, or a single value if key is not an Interval.
        """
        if isinstance(key, Interval):
            value = self.get(key, default)
            self.update(value)
            return value
        else:
            try:
                return self[key]
            except KeyError:
                self[key] = default
                return default

    def update(self, mapping_or_iterable):
        """
        Update current IntervalDict with provided values.

        If a mapping is provided, it must map Interval instances to values (e.g.,
        another IntervalDict). If an iterable is provided, it must consist of a
        list of (key, value) pairs.

        :param mapping_or_iterable: mapping or iterable.
        """
        if isinstance(mapping_or_iterable, Mapping):
            data = mapping_or_iterable.items()
        else:
            data = mapping_or_iterable

        for i, v in data:
            self[i] = v

    def combine(self, other, how, *, missing=..., pass_interval=False):
        """
        Return a new IntervalDict that combines the values from current and
        provided IntervalDict.

        If d = d1.combine(d2, f), then d contains (1) all values from d1 whose
        keys do not intersect the ones of d2, (2) all values from d2 whose keys
        do not intersect the ones of d1, and (3) f(x, y) for x in d1, y in d2 for
        intersecting keys.

        When missing is set, the how function is called even for non-intersecting
        keys using the value of missing to replace the missing values. This is,
        case (1) corresponds to f(x, missing) and case (2) to f(missing, y).

        If pass_interval is set to True, the current interval will be passed to
        the "how" function as third parameter.

        :param other: another IntervalDict instance.
        :param how: a function combining two values.
        :param missing: if set, use this value for missing values when calling "how".
        :param pass_interval: if set, provide the current interval to the how function.
        :return: a new IntervalDict instance.
        """
        new_items = []

        if not pass_interval:

            def _how(x, y, i):
                return how(x, y)
        else:
            _how = how
        dom1, dom2 = self.domain(), other.domain()

        if missing is Ellipsis:
            new_items.extend(self[dom1 - dom2].items())
            new_items.extend(other[dom2 - dom1].items())
        else:
            for i, v in self[dom1 - dom2].items():
                new_items.append((i, _how(v, missing, i)))
            for i, v in other[dom2 - dom1].items():
                new_items.append((i, _how(missing, v, i)))

        intersection = dom1 & dom2
        d1, d2 = self[intersection], other[intersection]

        for i1, v1 in d1.items():
            for i2, v2 in d2.items():
                if i1.overlaps(i2):
                    i = i1 & i2
                    v = _how(v1, v2, i)
                    new_items.append((i, v))

        return self.__class__(new_items)

    def as_dict(self, atomic=False):
        """
        Return the content as a classical Python dict.

        :param atomic: whether keys are atomic intervals.
        :return: a Python dict.
        """
        if atomic:
            d = {}
            for interval, v in self._storage.items():
                for i in interval:
                    d[i] = v
            return d
        else:
            return dict(self._storage)

    def __getitem__(self, key):
        if isinstance(key, Interval):
            items = []
            for i, v in self._storage.items():
                # Early out
                if key.upper < i.lower:
                    break

                intersection = key & i
                if not intersection.empty:
                    items.append((intersection, v))
            return self.__class__._from_items(items)
        else:
            for i, v in self._storage.items():
                # Early out
                if key < i.lower:
                    break
                if key in i:
                    return v
            raise KeyError(key)

    def __setitem__(self, key, value):
        if isinstance(key, Interval):
            interval = key
        else:
            interval = self._klass.from_atomic(Bound.CLOSED, key, key, Bound.CLOSED)

        if interval.empty:
            return

        removed_keys = []
        added_items = []

        found = False
        for i, v in self._storage.items():
            if value == v:
                found = True
                # Extend existing key
                removed_keys.append(i)
                added_items.append((i | interval, v))
            elif i.overlaps(interval):
                # Reduce existing key
                remaining = i - interval
                removed_keys.append(i)
                if not remaining.empty:
                    added_items.append((remaining, v))

        if not found:
            added_items.append((interval, value))

        # Update storage accordingly
        for key in removed_keys:
            self._storage.pop(key)

        for key, value in added_items:
            self._storage[key] = value

    def __delitem__(self, key):
        if isinstance(key, Interval):
            interval = key
        else:
            interval = self._klass.from_atomic(Bound.CLOSED, key, key, Bound.CLOSED)

        if interval.empty:
            return

        removed_keys = []
        added_items = []

        found = False
        for i, v in self._storage.items():
            # Early out
            if interval.upper < i.lower:
                break

            if i.overlaps(interval):
                found = True
                remaining = i - interval
                removed_keys.append(i)
                if not remaining.empty:
                    added_items.append((remaining, v))

        if not found and not isinstance(key, Interval):
            raise KeyError(key)

        # Update storage accordingly
        for key in removed_keys:
            self._storage.pop(key)

        for key, value in added_items:
            self._storage[key] = value

    def __or__(self, other):
        d = self.copy()
        d.update(other)
        return d

    def __ior__(self, other):
        self.update(other)
        return self

    def __iter__(self):
        return iter(self._storage)

    def __len__(self):
        return len(self._storage)

    def __contains__(self, key):
        return key in self.domain()

    def __repr__(self):
        return "{{{}}}".format(
            ", ".join(f"{i!r}: {v!r}" for i, v in self.items()),
        )

    def __eq__(self, other):
        if isinstance(other, IntervalDict):
            return self.as_dict() == other.as_dict()
        else:
            return NotImplemented

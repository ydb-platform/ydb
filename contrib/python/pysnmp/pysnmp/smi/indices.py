#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import warnings
from bisect import bisect


class OrderedDict(dict):
    """Ordered dictionary used for indices."""

    def __init__(self, *args, **kwargs):
        """Create an ordered dictionary."""
        self.__keys = []
        self.__dirty = True
        super().__init__()
        if args:
            self.update(*args)
        if kwargs:
            self.update(**kwargs)

    def __setitem__(self, key, value):
        """Set an item in the dictionary."""
        if key not in self:
            self.__keys.append(key)
            self.__dirty = True
        super().__setitem__(key, value)

    def __delitem__(self, key):
        """Delete an item from the dictionary."""
        if key in self:
            self.__keys.remove(key)
            self.__dirty = True
        super().__delitem__(key)

    def clear(self):
        """Clear the dictionary."""
        super().clear()
        self.__keys = []
        self.__dirty = True

    def keys(self):
        """Return the keys in the dictionary."""
        if self.__dirty:
            self.__order()
        return list(self.__keys)

    def values(self):
        """Return the values in the dictionary."""
        if self.__dirty:
            self.__order()
        return [self[k] for k in self.__keys]

    def items(self):
        """Return the items in the dictionary."""
        if self.__dirty:
            self.__order()
        return [(k, self[k]) for k in self.__keys]

    def update(self, *args, **kwargs):
        """Update the dictionary."""
        if args:
            iterable = args[0]
            if hasattr(iterable, "keys"):
                for k in iterable:
                    self[k] = iterable[k]
            else:
                for k, v in iterable:
                    self[k] = v

        if kwargs:
            for k in kwargs:
                self[k] = kwargs[k]

    def sorting_function(self, keys):
        """Sort the keys in the dictionary."""
        keys.sort()

    def __order(self):
        self.sorting_function(self.__keys)
        self.__keysLens = sorted({len(k) for k in self.__keys}, reverse=True)
        self.__dirty = False

    def next_key(self, key):
        """Return the next key in the dictionary."""
        if self.__dirty:
            self.__order()

        keys = self.__keys

        nextIdx = bisect(keys, key)

        if nextIdx < len(keys):
            return keys[nextIdx]

        else:
            raise KeyError(key)

    def get_keys_lengths(self):
        """Return the keys lengths in the dictionary."""
        if self.__dirty:
            self.__order()
        return self.__keysLens

    # Compatibility API
    # compatibility with legacy code
    # Old to new attribute mapping
    deprecated_attributes = {
        "nextKey": "next_key",
        "getKeysLens": "get_keys_lengths",
    }

    def __getattr__(self, attr: str):
        """Handle deprecated attributes."""
        if new_attr := self.deprecated_attributes.get(attr):
            warnings.warn(
                f"{attr} is deprecated. Please use {new_attr} instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return getattr(self, new_attr)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{attr}'"
        )


class OidOrderedDict(OrderedDict):
    """OID-ordered dictionary used for indices."""

    def __init__(self, *args, **kwargs):
        """Create an OID-ordered dictionary."""
        self.__keysCache = {}
        OrderedDict.__init__(self, *args, **kwargs)

    def __setitem__(self, key, value):
        """Set an item in the dictionary."""
        OrderedDict.__setitem__(self, key, value)
        if key not in self.__keysCache:
            if isinstance(key, tuple):
                self.__keysCache[key] = key
            else:
                self.__keysCache[key] = [int(x) for x in key.split(".") if x]

    def __delitem__(self, key):
        """Delete an item from the dictionary."""
        OrderedDict.__delitem__(self, key)
        if key in self.__keysCache:
            del self.__keysCache[key]

    def sorting_function(self, keys):
        """Sort the keys in the dictionary."""
        keys.sort(key=lambda k, d=self.__keysCache: d[k])

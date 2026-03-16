""" """

from pytool.lang import UNSET
from pytool.proxy import DictProxy, ListProxy


class NameMap(str):
    """This class is used to map attribute names to document keys internally."""

    def __new__(cls, value=""):
        instance = super().__new__(cls, value)
        instance.__dict__ = {}
        return instance

    def __init__(self, value=""):
        self._key = value.split(".")[-1]
        self._default_value = UNSET

    @property
    def key(self):
        # We don't map leading underscore names, so we cheat by storing our key
        # in a private var, and then get it back out again
        return self._key

    def _default(self, doc, key, reverse_name_map):
        """Return the default value for this name map."""
        if self._default_value is not UNSET:
            return self._default_value
        # Return an empty dict map to allow sub-key assignment
        return DictMap({}, self, doc, key, reverse_name_map)

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __contains__(self, key):
        return key in self.__dict__

    def filtered(self):
        """Return self.__dict__ minus any private keys."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}

    def mapped(self):
        """Return the mapped attributes."""
        return self.filtered().keys()

    def merge(self, other):
        """Merges another `.NameMap` instance into this one."""
        self.__dict__.update(other.filtered())

    def _defaults(self):
        """Return a dict of default values."""
        return {
            n.key: n._default_value
            for n in self.filtered().values()
            if isinstance(n, NameMap) and n._default_value is not UNSET
        }

    def empty(self):
        """
        Return ``True`` if this name map does not contain any sub keys,
        otherwise ``False``.

        .. versionadded: 4.0

        """
        _dict = self.__dict__
        if len(_dict) == 2 and "_key" in _dict and "_default_value" in _dict:
            return True
        return False


class DictMap(DictProxy):
    """This class is used to map embedded documents to their attribute names.
    This class ensures that the original document is kept up to sync with
    the embedded document clones via a reference to the `parent`, which at
    the highest level is the main document.

    """

    def __init__(self, value, name_map, parent, key, reverse_name_map):
        self._parent = parent
        self._key = key
        self._name_map = name_map
        self._reverse_name_map = reverse_name_map
        super(DictMap, self).__init__(value)

    @property
    def _parent_mutable(self):
        return isinstance(self._parent, (dict, DictMap))

    def __getattr__(self, name):
        # Exclude private names from this behavior
        if name.startswith("_"):
            return object.__getattribute__(self, name)

        if name not in self._name_map:
            raise AttributeError("{!r} is not mapped".format(name))

        attr = self._name_map[name]

        # Get the actual key if we are mapped too deep
        if isinstance(attr, NameMap):
            key = attr.key
        else:
            key = attr

        reverse_name_map = self._reverse_name_map[key]

        # Return the value if we have it
        if key in self:
            value = self[key]
            # Only create a new DictMap instance if we map into it
            if isinstance(value, dict) and not attr.empty():
                value = DictMap(value, attr, self, key, reverse_name_map)
            # We always create a new ListMap instance for the .new() method
            elif isinstance(value, list):
                value = ListMap(value, attr, self, key, reverse_name_map)
            return value

        if isinstance(attr, NameMap) and not attr.empty():
            return DictMap({}, attr, self, key, reverse_name_map)
        else:
            return {}

        # TODO: Decide whether to allow non-mapped keys via attribute access
        object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        # Exclude private names from this behavior
        if name.startswith("_"):
            return object.__setattr__(self, name, value)

        if name not in self._name_map:
            raise AttributeError("{!r} is not a mapped attribute".format(name))

        # If it's mapped, let's map it!
        key = self._name_map[name]

        # We want to get just the key, not the dot-notation
        if isinstance(key, NameMap):
            key = key.key

        # Assign the mapped key
        self[key] = value

    def __delattr__(self, name):
        # Exclude private names from this behavior
        if name.startswith("_"):
            return object.__delattr__(self, name)

        # If it's not mapped, let's delete it!
        if name not in self._name_map:
            object.__delattr__(self, name)
            return

        # If it's mapped, let's map it!
        key = self._name_map[name]

        # We want to get just the key, not the dot-notation
        if isinstance(key, NameMap):
            key = key.key

        # Delete the key if we have it
        if key in self:
            del self[key]
            return

        # This will attempt a normal delete, and probably raise an error
        object.__delattr__(self, name)

    def __setitem__(self, key, value):
        # The current dictionary may not exist in the parent yet, so we have
        # to create a new one if it's missing
        if self._key not in self._parent and self._parent_mutable:
            # The parent is empty, so we need a new empty dict
            self._data = {}
            self._parent[self._key] = self._data

        # Assign to self
        super(DictMap, self).__setitem__(key, value)

    def __delitem__(self, key):
        if self._key not in self._parent:
            # Fuck it
            return

        # Delete from self
        if key in self:
            super(DictMap, self).__delitem__(key)
            # If this dict is empty, remove it totally from the parent
            if not self and self._parent_mutable:
                del self._parent[self._key]
        else:
            # Raise an error
            super(DictMap, self).__delitem__(key)

    def for_json(self):
        """Return this suitable for JSON encoding."""
        mapped = {}
        reverse_name_map = self._reverse_name_map
        # We iterate over the keys contained in this. If a key is in the
        # reverse_name_map, which maps key name -> attr name, we use the
        # attribute name instead, since long names are preferred for JSON.
        for key in self:
            if key in reverse_name_map:
                key = reverse_name_map[key]
            # In order to allow recursive mapping, we use getattr() to get the
            # value of the key, so sub documents will be wrapped in their own
            # DictMap, which can handle enconding to JSON.
            value = getattr(self, key)
            mapped[key] = value
        return mapped


class ListMap(ListProxy):
    def __init__(self, value, name_map, parent, key, reverse_name_map):
        self._parent = parent
        self._key = key
        self._name_map = name_map
        self._reverse_name_map = reverse_name_map
        super(ListMap, self).__init__(value)

    def new(self):
        """Create a new embedded document in this list."""
        # We start with a new, empty dictionary
        value = {}
        # Append it to ourselves
        self.append(value)
        # Wrap it in a DictMap if we map into this list
        if not self._name_map.empty():
            # We pass None as the 'key' so that an IndexError would be raised if
            # the dict map tries to modify the parent
            value = DictMap(value, self._name_map, self, None, self._reverse_name_map)
        return value

    def __getitem__(self, index):
        value = super(ListMap, self).__getitem__(index)
        # Only create a new DictMap if we actually map into this list
        if isinstance(value, dict) and not self._name_map.empty():
            value = DictMap(value, self._name_map, self, None, self._reverse_name_map)
        return value

    def for_json(self):
        """Return this suitable for JSON encoding."""
        return list(self)

"""
A subclass of MutableAttr that has defaultdict support.
"""
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

import six

from attrdict.mixins import MutableAttr


__all__ = ['AttrDefault']


class AttrDefault(MutableAttr):
    """
    An implementation of MutableAttr with defaultdict support
    """
    def __init__(self, default_factory=None, items=None, sequence_type=tuple,
                 pass_key=False):
        if items is None:
            items = {}
        elif not isinstance(items, Mapping):
            items = dict(items)

        self._setattr('_default_factory', default_factory)
        self._setattr('_mapping', items)
        self._setattr('_sequence_type', sequence_type)
        self._setattr('_pass_key', pass_key)
        self._setattr('_allow_invalid_attributes', False)

    def _configuration(self):
        """
        The configuration for a AttrDefault instance
        """
        return self._sequence_type, self._default_factory, self._pass_key

    def __getitem__(self, key):
        """
        Access a value associated with a key.

        Note: values returned will not be wrapped, even if recursive
        is True.
        """
        if key in self._mapping:
            return self._mapping[key]
        elif self._default_factory is not None:
            return self.__missing__(key)

        raise KeyError(key)

    def __setitem__(self, key, value):
        """
        Add a key-value pair to the instance.
        """
        self._mapping[key] = value

    def __delitem__(self, key):
        """
        Delete a key-value pair
        """
        del self._mapping[key]

    def __len__(self):
        """
        Check the length of the mapping.
        """
        return len(self._mapping)

    def __iter__(self):
        """
        Iterated through the keys.
        """
        return iter(self._mapping)

    def __missing__(self, key):
        """
        Add a missing element.
        """
        if self._pass_key:
            self[key] = value = self._default_factory(key)
        else:
            self[key] = value = self._default_factory()

        return value

    def __repr__(self):
        """
        Return a string representation of the object.
        """
        return six.u(
            "AttrDefault({default_factory}, {pass_key}, {mapping})"
        ).format(
            default_factory=repr(self._default_factory),
            pass_key=repr(self._pass_key),
            mapping=repr(self._mapping),
        )

    def __getstate__(self):
        """
        Serialize the object.
        """
        return (
            self._default_factory,
            self._mapping,
            self._sequence_type,
            self._pass_key,
            self._allow_invalid_attributes,
        )

    def __setstate__(self, state):
        """
        Deserialize the object.
        """
        (default_factory, mapping, sequence_type, pass_key,
         allow_invalid_attributes) = state

        self._setattr('_default_factory', default_factory)
        self._setattr('_mapping', mapping)
        self._setattr('_sequence_type', sequence_type)
        self._setattr('_pass_key', pass_key)
        self._setattr('_allow_invalid_attributes', allow_invalid_attributes)

    @classmethod
    def _constructor(cls, mapping, configuration):
        """
        A standardized constructor.
        """
        sequence_type, default_factory, pass_key = configuration
        return cls(default_factory, mapping, sequence_type=sequence_type,
                   pass_key=pass_key)

"""
An implementation of MutableAttr.
"""
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

import six

from attrdict.mixins import MutableAttr


__all__ = ['AttrMap']


class AttrMap(MutableAttr):
    """
    An implementation of MutableAttr.
    """
    def __init__(self, items=None, sequence_type=tuple):
        if items is None:
            items = {}
        elif not isinstance(items, Mapping):
            items = dict(items)

        self._setattr('_sequence_type', sequence_type)
        self._setattr('_mapping', items)
        self._setattr('_allow_invalid_attributes', False)

    def _configuration(self):
        """
        The configuration for an attrmap instance.
        """
        return self._sequence_type

    def __getitem__(self, key):
        """
        Access a value associated with a key.
        """
        return self._mapping[key]

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

    def __repr__(self):
        """
        Return a string representation of the object.
        """
        # sequence type seems like more trouble than it is worth.
        # If people want full serialization, they can pickle, and in
        # 99% of cases, sequence_type won't change anyway
        return six.u("AttrMap({mapping})").format(mapping=repr(self._mapping))

    def __getstate__(self):
        """
        Serialize the object.
        """
        return (
            self._mapping,
            self._sequence_type,
            self._allow_invalid_attributes
        )

    def __setstate__(self, state):
        """
        Deserialize the object.
        """
        mapping, sequence_type, allow_invalid_attributes = state
        self._setattr('_mapping', mapping)
        self._setattr('_sequence_type', sequence_type)
        self._setattr('_allow_invalid_attributes', allow_invalid_attributes)

    @classmethod
    def _constructor(cls, mapping, configuration):
        """
        A standardized constructor.
        """
        return cls(mapping, sequence_type=configuration)

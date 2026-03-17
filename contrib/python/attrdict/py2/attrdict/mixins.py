"""
Mixin Classes for Attr-support.
"""
from abc import ABCMeta, abstractmethod
from collections import Mapping, MutableMapping, Sequence
import re

import six

from attrdict.merge import merge


__all__ = ['Attr', 'MutableAttr']


@six.add_metaclass(ABCMeta)
class Attr(Mapping):
    """
    A mixin class for a mapping that allows for attribute-style access
    of values.

    A key may be used as an attribute if:
     * It is a string
     * It matches /^[A-Za-z][A-Za-z0-9_]*$/ (i.e., a public attribute)
     * The key doesn't overlap with any class attributes (for Attr,
        those would be 'get', 'items', 'keys', 'values', 'mro', and
        'register').

    If a values which is accessed as an attribute is a Sequence-type
    (and is not a string/bytes), it will be converted to a
    _sequence_type with any mappings within it converted to Attrs.

    NOTE: This means that if _sequence_type is not None, then a
        sequence accessed as an attribute will be a different object
        than if accessed as an attribute than if it is accessed as an
        item.
    """
    @abstractmethod
    def _configuration(self):
        """
        All required state for building a new instance with the same
        settings as the current object.
        """

    @classmethod
    def _constructor(cls, mapping, configuration):
        """
        A standardized constructor used internally by Attr.

        mapping: A mapping of key-value pairs. It is HIGHLY recommended
            that you use this as the internal key-value pair mapping, as
            that will allow nested assignment (e.g., attr.foo.bar = baz)
        configuration: The return value of Attr._configuration
        """
        raise NotImplementedError("You need to implement this")

    def __call__(self, key):
        """
        Dynamically access a key-value pair.

        key: A key associated with a value in the mapping.

        This differs from __getitem__, because it returns a new instance
        of an Attr (if the value is a Mapping object).
        """
        if key not in self:
            raise AttributeError(
                "'{cls} instance has no attribute '{name}'".format(
                    cls=self.__class__.__name__, name=key
                )
            )

        return self._build(self[key])

    def __getattr__(self, key):
        """
        Access an item as an attribute.
        """
        if key not in self or not self._valid_name(key):
            raise AttributeError(
                "'{cls}' instance has no attribute '{name}'".format(
                    cls=self.__class__.__name__, name=key
                )
            )

        return self._build(self[key])

    def __add__(self, other):
        """
        Add a mapping to this Attr, creating a new, merged Attr.

        other: A mapping.

        NOTE: Addition is not commutative. a + b != b + a.
        """
        if not isinstance(other, Mapping):
            return NotImplemented

        return self._constructor(merge(self, other), self._configuration())

    def __radd__(self, other):
        """
        Add this Attr to a mapping, creating a new, merged Attr.

        other: A mapping.

        NOTE: Addition is not commutative. a + b != b + a.
        """
        if not isinstance(other, Mapping):
            return NotImplemented

        return self._constructor(merge(other, self), self._configuration())

    def _build(self, obj):
        """
        Conditionally convert an object to allow for recursive mapping
        access.

        obj: An object that was a key-value pair in the mapping. If obj
            is a mapping, self._constructor(obj, self._configuration())
            will be called. If obj is a non-string/bytes sequence, and
            self._sequence_type is not None, the obj will be converted
            to type _sequence_type and build will be called on its
            elements.
        """
        if isinstance(obj, Mapping):
            obj = self._constructor(obj, self._configuration())
        elif (isinstance(obj, Sequence) and
              not isinstance(obj, (six.string_types, six.binary_type))):
            sequence_type = getattr(self, '_sequence_type', None)

            if sequence_type:
                obj = sequence_type(self._build(element) for element in obj)

        return obj

    @classmethod
    def _valid_name(cls, key):
        """
        Check whether a key is a valid attribute name.

        A key may be used as an attribute if:
         * It is a string
         * It matches /^[A-Za-z][A-Za-z0-9_]*$/ (i.e., a public attribute)
         * The key doesn't overlap with any class attributes (for Attr,
            those would be 'get', 'items', 'keys', 'values', 'mro', and
            'register').
        """
        return (
            isinstance(key, six.string_types) and
            re.match('^[A-Za-z][A-Za-z0-9_]*$', key) and
            not hasattr(cls, key)
        )


@six.add_metaclass(ABCMeta)
class MutableAttr(Attr, MutableMapping):
    """
    A mixin class for a mapping that allows for attribute-style access
    of values.
    """
    def _setattr(self, key, value):
        """
        Add an attribute to the object, without attempting to add it as
        a key to the mapping.
        """
        super(MutableAttr, self).__setattr__(key, value)

    def __setattr__(self, key, value):
        """
        Add an attribute.

        key: The name of the attribute
        value: The attributes contents
        """
        if self._valid_name(key):
            self[key] = value
        elif getattr(self, '_allow_invalid_attributes', True):
            super(MutableAttr, self).__setattr__(key, value)
        else:
            raise TypeError(
                "'{cls}' does not allow attribute creation.".format(
                    cls=self.__class__.__name__
                )
            )

    def _delattr(self, key):
        """
        Delete an attribute from the object, without attempting to
        remove it from the mapping.
        """
        super(MutableAttr, self).__delattr__(key)

    def __delattr__(self, key, force=False):
        """
        Delete an attribute.

        key: The name of the attribute
        """
        if self._valid_name(key):
            del self[key]
        elif getattr(self, '_allow_invalid_attributes', True):
            super(MutableAttr, self).__delattr__(key)
        else:
            raise TypeError(
                "'{cls}' does not allow attribute deletion.".format(
                    cls=self.__class__.__name__
                )
            )

#!/usr/bin/env python3
"""Creates records, similar to collections.namedtuple.

Creates a record class like namedtuple, but mutable and with optional
attributes.

Optional attributes take a value or a callable (make sure to use a factory
function otherwise the same object will be shared among all the record
instances, like collections.defaultdict).

Example:

    FirstRecord = records.Record('FirstRecord', ['attr1', 'attr2'], attr3=0)
    foo = FirstRecord(1, 2, attr3=3)
    bar = FirstRecord(attr1=1, attr2=2, attr3=5)

    class Second(FirstRecord):
        required_attributes = ['second1']
        optional_attributes = {'second2': 5}

    # Second requires attr1, attr2, and second1.
    baz = Second(1, 2, 3, second2=4)
"""
import copy
import itertools
import sys


class RecordClass(object):
    __slots__ = ()
    required_attributes = ()
    optional_attributes = {}

    def __init__(self, *args, **kwargs):
        # First, check for the maximum number of arguments.
        required_attributes = type(self).required_attributes
        if len(args) + len(kwargs.keys()) < len(required_attributes):
            raise ValueError(
                'Invalid arguments', type(self), args, kwargs, self.__slots__)
        # Second, check if there are any overlapping arguments.
        conflicts = (frozenset(kwargs.keys()) &
                     frozenset(required_attributes[:len(args)]))
        if conflicts:
            raise TypeError(
                'Keyword arguments conflict with positional arguments: %s',
                conflicts)
        # Third, check all required attributes are provided.
        required_kwargs = set(kwargs.keys()) & set(required_attributes)
        num_provided = len(args) + len(required_kwargs)
        if num_provided != len(required_attributes):
            raise TypeError(
                '__init__ takes exactly %d arguments but %d were given: %s' % (
                    len(required_attributes), num_provided,
                    required_attributes))

        for slot, arg in itertools.chain(
                zip(type(self).required_attributes, args), kwargs.items()):
            object.__setattr__(self, slot, arg)
        # Set defaults.
        for attr, value in type(self).optional_attributes.items():
            if attr not in kwargs:
                if callable(value):
                    value = value()
                object.__setattr__(self, attr, value)

    def __str__(self):
        return self._str(type(self).all_attribute_names)

    def _str(self, str_attrs):
        attrs = []
        for attr in str_attrs:
            attrs.append('%s=%s' % (attr, repr(getattr(self, attr))))
        return '%s(%s)' % (type(self).__name__, ', '.join(attrs))
    __repr__ = __str__

    def __eq__(self, other):
        return (
            self is other
            or type(self) == type(other)
            and self._isequal_fields(other, self.__slots__))

    def _isequal_fields(self, other, fields):
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in fields)

    def __copy__(self):
        return type(self)(**{attr: getattr(self, attr)
                             for attr in self.__slots__})

    def __deepcopy__(self, memo):
        return type(self)(**{attr: copy.deepcopy(getattr(self, attr), memo)
                             for attr in self.__slots__})

    def __getstate__(self):
        """Get the current state of all attributes."""
        return {attr: getattr(self, attr) for attr in type(self).__slots__}

    def __setstate__(self, state):
        """Set the state of attributes."""
        for attr, value in state.iteritems():
          setattr(self, attr, value)


class HashableRecordClass(RecordClass):
    """Hashable version of RecordClass.

    Use this when the record is considered immutable enough to be hashable.
    Immutability is not enforced, but is recommended.

    Do not use if the record or any of its fields' values will ever be modified.
    """
    def __hash__(self):
        return hash(
            tuple(hash(getattr(self, attr)) for attr in self.__slots__))


class RecordMeta(type):

    def __new__(cls, name, bases, attrs):
        required_attributes = []  # Combine the bases' req attrs first.
        attrs['optional_attributes'] = attrs.get('optional_attributes', {})
        for base in bases:
            if issubclass(base, RecordClass):
                # Check for repeated attributes first.
                repeats = (set(required_attributes) &
                           set(base.required_attributes))
                assert not repeats, 'Required attributes clash: %s' % repeats
                repeats = (set(attrs['optional_attributes']) &
                           set(base.optional_attributes))
                assert not repeats, 'Optional attributes clash: %s' % repeats

                required_attributes.extend(base.required_attributes)
                attrs['optional_attributes'].update(base.optional_attributes)

                # If this class defines any attributes in a superclass's
                # required attributes, make it an optional attribute with a
                # default with the given value.
                provided = set(base.required_attributes) & set(attrs)
                for attr in provided:
                    required_attributes.remove(attr)
                    attrs['optional_attributes'][attr] = attrs.pop(attr)
                # Allow the class to override optional attribute defaults
                # as well.
                provided = set(base.optional_attributes) & set(attrs)
                for attr in provided:
                    attrs['optional_attributes'][attr] = attrs.pop(attr)

        attrs['required_attributes'] = tuple(
            required_attributes + list(attrs.get('required_attributes', ())))
        attrs['__slots__'] = (tuple(attrs['required_attributes']) +
                              tuple(attrs['optional_attributes'].keys()))
        return super(RecordMeta, cls).__new__(cls, name, bases, attrs)

    def __str__(cls):
        return '<Record: %s>' % cls.__name__
    __repr__ = __str__

    def __eq__(cls, other):
        if not isinstance(other, RecordMeta):
            return False
        return (
            cls is other
            or cls.required_attributes == other.required_attributes
            and cls.optional_attributes == other.optional_attributes)

    def __hash__(cls):
        return hash(
            (cls.required_attributes,
             frozenset(cls.optional_attributes.items())))

    @property
    def all_attribute_names(cls):
        return itertools.chain(
            cls.required_attributes,
            cls.optional_attributes.keys())


def Record(cls_name, required_attributes=(), optional_attributes={}):
    attrs = {'required_attributes': tuple(required_attributes),
             'optional_attributes': dict(optional_attributes)}
    cls = RecordMeta(cls_name, (RecordClass,), attrs)

    # Copied from collections.py, the bottom of namedtuple:
    # For pickling to work, the __module__ variable needs to be set to the frame
    # where the named tuple is created.  Bypass this step in enviroments where
    # sys._getframe is not defined (Jython for example).
    if hasattr(sys, '_getframe'):
        cls.__module__ = sys._getframe(1).f_globals.get('__name__', '__main__')
    return cls


def HashableRecord(cls_name, required_attributes=(), optional_attributes={}):
    attrs = {'required_attributes': tuple(required_attributes),
             'optional_attributes': dict(optional_attributes)}
    return RecordMeta(cls_name, (HashableRecordClass,), attrs)


def CopyRecord(record, **field_overrides):
    """Copies a record and each of its fields, like a Record-only deepcopy.

    Args:
      record: A Record instance to be copied.
      **field_overrides: Fields and their values to override in the new copy.

    Returns: A copy of the given record with any fields overridden.
    """

    fields = field_overrides
    for field in record.__slots__:
        if field in field_overrides:
            continue
        value = getattr(record, field)
        if isinstance(value, RecordClass):
            # Recurse for records.
            new_value = CopyRecord(value)
        else:
            new_value = copy.copy(value)
        fields[field] = new_value

    return type(record)(**fields)

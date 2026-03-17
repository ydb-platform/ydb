##############################################################################
# Copyright (c) 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Schema Fields
"""
__docformat__ = 'restructuredtext'

import re
from collections import abc
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta

from zope.interface import classImplements
from zope.interface import classImplementsFirst
from zope.interface import implementedBy
from zope.interface import implementer
from zope.interface.interfaces import IInterface

from zope.schema._bootstrapfields import Bool
from zope.schema._bootstrapfields import Complex
from zope.schema._bootstrapfields import Container  # API import for __init__
from zope.schema._bootstrapfields import Decimal
from zope.schema._bootstrapfields import Field
from zope.schema._bootstrapfields import Int
from zope.schema._bootstrapfields import Integral
from zope.schema._bootstrapfields import \
    InvalidDecimalLiteral  # noqa: reexport
from zope.schema._bootstrapfields import Iterable
from zope.schema._bootstrapfields import MinMaxLen
from zope.schema._bootstrapfields import Number
from zope.schema._bootstrapfields import Object
from zope.schema._bootstrapfields import Orderable
from zope.schema._bootstrapfields import Password
from zope.schema._bootstrapfields import Rational
from zope.schema._bootstrapfields import Real
from zope.schema._bootstrapfields import Text
from zope.schema._bootstrapfields import TextLine
from zope.schema._bootstrapfields import _NotGiven
from zope.schema.fieldproperty import FieldProperty
from zope.schema.interfaces import IASCII
from zope.schema.interfaces import IURI
from zope.schema.interfaces import ConstraintNotSatisfied
from zope.schema.interfaces import IASCIILine
from zope.schema.interfaces import IBaseVocabulary
from zope.schema.interfaces import IBool
from zope.schema.interfaces import IBytes
from zope.schema.interfaces import IBytesLine
from zope.schema.interfaces import IChoice
from zope.schema.interfaces import ICollection
from zope.schema.interfaces import IComplex
from zope.schema.interfaces import IContainer
from zope.schema.interfaces import IContextSourceBinder
from zope.schema.interfaces import IDate
from zope.schema.interfaces import IDatetime
from zope.schema.interfaces import IDecimal
from zope.schema.interfaces import IDict
from zope.schema.interfaces import IDottedName
from zope.schema.interfaces import IField
from zope.schema.interfaces import IFloat
from zope.schema.interfaces import IFromBytes
from zope.schema.interfaces import IFromUnicode
from zope.schema.interfaces import IFrozenSet
from zope.schema.interfaces import IId
from zope.schema.interfaces import IInt
from zope.schema.interfaces import IIntegral
from zope.schema.interfaces import IInterfaceField
from zope.schema.interfaces import IIterable
from zope.schema.interfaces import IList
from zope.schema.interfaces import IMapping
from zope.schema.interfaces import IMinMaxLen
from zope.schema.interfaces import IMutableMapping
from zope.schema.interfaces import IMutableSequence
from zope.schema.interfaces import INativeString
from zope.schema.interfaces import INativeStringLine
from zope.schema.interfaces import INumber
from zope.schema.interfaces import InvalidDottedName
from zope.schema.interfaces import InvalidId
from zope.schema.interfaces import InvalidURI
from zope.schema.interfaces import InvalidValue
from zope.schema.interfaces import IObject
from zope.schema.interfaces import IPassword
from zope.schema.interfaces import IPythonIdentifier
from zope.schema.interfaces import IRational
from zope.schema.interfaces import IReal
from zope.schema.interfaces import ISequence
from zope.schema.interfaces import ISet
from zope.schema.interfaces import ISource
from zope.schema.interfaces import ISourceText
from zope.schema.interfaces import IText
from zope.schema.interfaces import ITextLine
from zope.schema.interfaces import ITime
from zope.schema.interfaces import ITimedelta
from zope.schema.interfaces import ITuple
from zope.schema.interfaces import NotAnInterface
from zope.schema.interfaces import NotUnique
from zope.schema.interfaces import ValidationError
from zope.schema.interfaces import WrongContainedType
from zope.schema.interfaces import WrongType
from zope.schema.vocabulary import SimpleVocabulary
from zope.schema.vocabulary import getVocabularyRegistry


# Fix up bootstrap field types
Field.title = FieldProperty(IField['title'])
Field.description = FieldProperty(IField['description'])
Field.required = FieldProperty(IField['required'])
Field.readonly = FieldProperty(IField['readonly'])
# Default is already taken care of
classImplements(Field, IField)

MinMaxLen.min_length = FieldProperty(IMinMaxLen['min_length'])
MinMaxLen.max_length = FieldProperty(IMinMaxLen['max_length'])

classImplementsFirst(Text, IText)
classImplementsFirst(TextLine, ITextLine)
classImplementsFirst(Password, IPassword)
classImplementsFirst(Bool, IBool)
classImplementsFirst(Iterable, IIterable)
classImplementsFirst(Container, IContainer)

classImplementsFirst(Number, INumber)
classImplementsFirst(Complex, IComplex)
classImplementsFirst(Real, IReal)
classImplementsFirst(Rational, IRational)
classImplementsFirst(Integral, IIntegral)
classImplementsFirst(Int, IInt)
classImplementsFirst(Decimal, IDecimal)

classImplementsFirst(Object, IObject)


class implementer_if_needed:
    # Helper to make sure we don't redundantly implement
    # interfaces already inherited. Doing so tends to produce
    # problems with the C3 order. This is used when we cannot
    # statically determine if we need the interface or not, e.g,
    # because we're picking different base classes under some circumstances.
    def __init__(self, *ifaces):
        self._ifaces = ifaces

    def __call__(self, cls):
        ifaces_needed = []
        implemented = implementedBy(cls)
        ifaces_needed = [
            iface for iface in self._ifaces
            if not implemented.isOrExtends(iface)
        ]
        return implementer(*ifaces_needed)(cls)


@implementer(ISourceText)
class SourceText(Text):
    __doc__ = ISourceText.__doc__
    _type = str


@implementer(IBytes, IFromUnicode, IFromBytes)
class Bytes(MinMaxLen, Field):
    __doc__ = IBytes.__doc__
    _type = bytes

    def fromUnicode(self, value):
        """ See IFromUnicode.
        """
        return self.fromBytes(value.encode('ascii'))

    def fromBytes(self, value):
        self.validate(value)
        return value


@implementer_if_needed(INativeString, IFromUnicode, IFromBytes)
class NativeString(Text):
    """
    A native string is always the type `str`.

    In addition to :class:`~zope.schema.interfaces.INativeString`,
    this implements :class:`~zope.schema.interfaces.IFromUnicode` and
    :class:`~zope.schema.interfaces.IFromBytes`.

    .. versionchanged:: 4.9.0
       This is now a distinct type instead of an alias for either `Text` or
       `Bytes`, depending on the platform.
    """
    _type = str

    def fromBytes(self, value):
        value = value.decode('utf-8')
        self.validate(value)
        return value


@implementer(IASCII)
class ASCII(NativeString):
    __doc__ = IASCII.__doc__

    def _validate(self, value):
        super()._validate(value)
        if not value:
            return
        if not max(map(ord, value)) < 128:
            raise InvalidValue().with_field_and_value(self, value)


@implementer(IBytesLine)
class BytesLine(Bytes):
    """A `Bytes` field with no newlines."""

    def constraint(self, value):
        # TODO: we should probably use a more general definition of newlines
        return b'\n' not in value


@implementer_if_needed(INativeStringLine, IFromUnicode, IFromBytes)
class NativeStringLine(TextLine):
    """
    A native string is always the type `str`; this field excludes
    newlines.

    In addition to :class:`~zope.schema.interfaces.INativeStringLine`,
    this implements :class:`~zope.schema.interfaces.IFromUnicode` and
    :class:`~zope.schema.interfaces.IFromBytes`.

    .. versionchanged:: 4.9.0
       This is now a distinct type instead of an alias for either `TextLine`
       or `BytesLine`, depending on the platform.
    """
    _type = str

    def fromBytes(self, value):
        value = value.decode('utf-8')
        self.validate(value)
        return value


@implementer(IASCIILine)
class ASCIILine(ASCII):
    __doc__ = IASCIILine.__doc__

    def constraint(self, value):
        # TODO: we should probably use a more general definition of newlines
        return '\n' not in value


class InvalidFloatLiteral(ValueError, ValidationError):
    """Raised by Float fields."""


@implementer(IFloat)
class Float(Real):
    """
    A field representing a native :class:`float` and implementing
    :class:`zope.schema.interfaces.IFloat`.

    The class :class:`zope.schema.Real` is a more general version,
    accepting floats, integers, and fractions.

    The :meth:`fromUnicode` method only accepts values that can be parsed
    by the ``float`` constructor::

        >>> from zope.schema._field import Float
        >>> f = Float()
        >>> f.fromUnicode("1")
        1.0
        >>> f.fromUnicode("125.6")
        125.6
        >>> f.fromUnicode("1+0j") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidFloatLiteral: Invalid literal for float(): 1+0j
        >>> f.fromUnicode("1/2") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidFloatLiteral: invalid literal for float(): 1/2
        >>> f.fromUnicode(str(2**11234) + '.' + str(2**256))
        ... # doctest: +ELLIPSIS
        inf
        >>> f.fromUnicode("not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidFloatLiteral: could not convert string to float: not a number

    Likewise for :meth:`fromBytes`::

        >>> from zope.schema._field import Float
        >>> f = Float()
        >>> f.fromBytes(b"1")
        1.0
        >>> f.fromBytes(b"125.6")
        125.6
        >>> f.fromBytes(b"1+0j") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidFloatLiteral: Invalid literal for float(): 1+0j
        >>> f.fromBytes(b"1/2") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidFloatLiteral: invalid literal for float(): 1/2
        >>> f.fromBytes((str(2**11234) + '.' + str(2**256)).encode('ascii'))
        ... # doctest: +ELLIPSIS
        inf
        >>> f.fromBytes(b"not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidFloatLiteral: could not convert string to float: not a number

    """
    _type = float
    _unicode_converters = (float, )
    _validation_error = InvalidFloatLiteral


@implementer(IDatetime)
class Datetime(Orderable, Field):
    __doc__ = IDatetime.__doc__
    _type = datetime

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)


@implementer(IDate)
class Date(Orderable, Field):
    __doc__ = IDate.__doc__
    _type = date

    def _validate(self, value):
        super()._validate(value)
        if isinstance(value, datetime):
            raise WrongType(value, self._type,
                            self.__name__).with_field_and_value(self, value)


@implementer(ITimedelta)
class Timedelta(Orderable, Field):
    __doc__ = ITimedelta.__doc__
    _type = timedelta


@implementer(ITime)
class Time(Orderable, Field):
    __doc__ = ITime.__doc__
    _type = time


class MissingVocabularyError(ValidationError, ValueError, LookupError):
    """Raised when a named vocabulary cannot be found."""
    # Subclasses ValueError and LookupError for backwards compatibility


class InvalidVocabularyError(ValidationError, ValueError, TypeError):
    """Raised when the vocabulary is not an ISource."""

    # Subclasses TypeError and ValueError for backwards compatibility

    def __init__(self, vocabulary):
        super().__init__(f"Invalid vocabulary {vocabulary!r}")


@implementer(IChoice, IFromUnicode)
class Choice(Field):
    """Choice fields can have a value found in a constant or dynamic set of
    values given by the field definition.
    """

    def __init__(self, values=None, vocabulary=None, source=None, **kw):
        """Initialize object."""
        if vocabulary is not None:
            if (not isinstance(vocabulary, str)
                    and not IBaseVocabulary.providedBy(vocabulary)):
                raise ValueError('vocabulary must be a string or implement '
                                 'IBaseVocabulary')
            if source is not None:
                raise ValueError(
                    "You cannot specify both source and vocabulary.")
        elif source is not None:
            vocabulary = source

        if (values is None and vocabulary is None):
            raise ValueError("You must specify either values or vocabulary.")
        if values is not None and vocabulary is not None:
            raise ValueError("You cannot specify both values and vocabulary.")

        self.vocabulary = None
        self.vocabularyName = None
        if values is not None:
            self.vocabulary = SimpleVocabulary.fromValues(values)
        elif isinstance(vocabulary, str):
            self.vocabularyName = vocabulary
        else:
            if (not ISource.providedBy(vocabulary)
                    and not IContextSourceBinder.providedBy(vocabulary)):
                raise InvalidVocabularyError(vocabulary)
            self.vocabulary = vocabulary
        # Before a default value is checked, it is validated. However, a
        # named vocabulary is usually not complete when these fields are
        # initialized. Therefore signal the validation method to ignore
        # default value checks during initialization of a Choice tied to a
        # registered vocabulary.
        self._init_field = (bool(self.vocabularyName) or
                            IContextSourceBinder.providedBy(self.vocabulary))
        super().__init__(**kw)
        self._init_field = False

    source = property(lambda self: self.vocabulary)

    def _resolve_vocabulary(self, value):
        # Find the vocabulary we should use, raising
        # an exception if this isn't possible, and returning
        # an ISource otherwise.
        vocabulary = self.vocabulary
        if (IContextSourceBinder.providedBy(vocabulary)
                and self.context is not None):
            vocabulary = vocabulary(self.context)
        elif vocabulary is None and self.vocabularyName is not None:
            vr = getVocabularyRegistry()
            try:
                vocabulary = vr.get(self.context, self.vocabularyName)
            except LookupError:
                raise MissingVocabularyError(
                    "Can't validate value without vocabulary named"
                    f" {self.vocabularyName!r}").with_field_and_value(
                        self, value)

        if not ISource.providedBy(vocabulary):
            raise InvalidVocabularyError(vocabulary).with_field_and_value(
                self, value)

        return vocabulary

    def bind(self, context):
        """See zope.schema._bootstrapinterfaces.IField."""
        clone = super().bind(context)
        # Eagerly get registered vocabulary if needed;
        # once that's done, just return it
        vocabulary = clone.vocabulary = clone._resolve_vocabulary(None)
        clone._resolve_vocabulary = lambda value: vocabulary
        return clone

    def fromUnicode(self, value):
        """ See IFromUnicode.
        """
        self.validate(value)
        return value

    def _validate(self, value):
        # Pass all validations during initialization
        if self._init_field:
            return
        super()._validate(value)
        vocabulary = self._resolve_vocabulary(value)
        if value not in vocabulary:
            raise ConstraintNotSatisfied(value,
                                         self.__name__).with_field_and_value(
                                             self, value)


# Both of these are inherited from the parent; re-declaring them
# here messes with the __sro__ of subclasses, causing them to be
# inconsistent with C3.
# @implementer(IFromUnicode, IFromBytes)
class _StrippedNativeStringLine(NativeStringLine):

    _invalid_exc_type = None

    def fromUnicode(self, value):
        v = value.strip()
        # self._type is unicode, but we don't want to allow non-ASCII
        # values, to match the old behaviour on Python 2 (our regexes would
        # reject that anyway).
        try:
            v = v.encode('ascii')  # bytes
        except UnicodeEncodeError:
            raise self._invalid_exc_type(value).with_field_and_value(
                self, value)
        v = v.decode('ascii')
        self.validate(v)
        return v

    def fromBytes(self, value):
        return self.fromUnicode(value.decode('ascii'))


_isuri = r"[a-zA-z0-9+.-]+:"  # scheme
_isuri += r"\S*$"  # non space (should be pickier)
_isuri = re.compile(_isuri).match


@implementer(IURI)
class URI(_StrippedNativeStringLine):
    """
    URI schema field.

    URIs can be validated from both unicode values and bytes values,
    producing a native text string in both cases::

        >>> from zope.schema import URI
        >>> field = URI()
        >>> field.fromUnicode(u'   https://example.com  ')
        'https://example.com'
        >>> field.fromBytes(b'   https://example.com ')
        'https://example.com'

    .. versionchanged:: 4.8.0
        Implement :class:`zope.schema.interfaces.IFromBytes`
    """

    def _validate(self, value):
        super()._validate(value)
        if _isuri(value):
            return

        raise InvalidURI(value).with_field_and_value(self, value)


# An identifier is a letter or underscore, followed by
# any number of letters, underscores, and digits.
_identifier_pattern = r'[a-zA-Z_]+\w*'

# The whole string must match to be an identifier
_is_identifier = re.compile('^' + _identifier_pattern + '$').match

_isdotted = re.compile(
    # The start of the line, followed by an identifier,
    '^' + _identifier_pattern
    # optionally followed by .identifier any number of times
    + r"([.]" + _identifier_pattern + r")*"
    # followed by the end of the line.
    + r"$").match


@implementer(IPythonIdentifier)
class PythonIdentifier(_StrippedNativeStringLine):
    """
    This field describes a python identifier, i.e. a variable name.

    Empty strings are allowed.

    Identifiers can be validated from both unicode values and bytes values,
    producing a native text string in both cases::

        >>> from zope.schema import PythonIdentifier
        >>> field = PythonIdentifier()
        >>> field.fromUnicode(u'zope')
        'zope'
        >>> field.fromBytes(b'_zope')
        '_zope'
        >>> field.fromUnicode(u'   ')
        ''

    .. versionadded:: 4.9.0
    """

    def _validate(self, value):
        super()._validate(value)
        if value and not _is_identifier(value):
            raise InvalidValue(value).with_field_and_value(self, value)


@implementer(IDottedName)
class DottedName(_StrippedNativeStringLine):
    """Dotted name field.

    Values of DottedName fields must be Python-style dotted names.

    Dotted names can be validated from both unicode values and bytes values,
    producing a native text string in both cases::

        >>> from zope.schema import DottedName
        >>> field = DottedName()
        >>> field.fromUnicode(u'zope.schema')
        'zope.schema'
        >>> field.fromBytes(b'zope.schema')
        'zope.schema'
        >>> field.fromUnicode(u'zope._schema')
        'zope._schema'

    .. versionchanged:: 4.8.0
        Implement :class:`zope.schema.interfaces.IFromBytes`
    .. versionchanged:: 4.9.0
        Allow leading underscores in each component.
    """

    _invalid_exc_type = InvalidDottedName

    def __init__(self, *args, **kw):
        self.min_dots = int(kw.pop("min_dots", 0))
        if self.min_dots < 0:
            raise ValueError("min_dots cannot be less than zero")
        self.max_dots = kw.pop("max_dots", None)
        if self.max_dots is not None:
            self.max_dots = int(self.max_dots)
            if self.max_dots < self.min_dots:
                raise ValueError("max_dots cannot be less than min_dots")
        super().__init__(*args, **kw)

    def _validate(self, value):
        """

        """
        super()._validate(value)
        if not _isdotted(value):
            raise InvalidDottedName(value).with_field_and_value(self, value)
        dots = value.count(".")
        if dots < self.min_dots:
            raise InvalidDottedName(
                "too few dots; %d required" % self.min_dots,
                value).with_field_and_value(self, value)
        if self.max_dots is not None and dots > self.max_dots:
            raise InvalidDottedName(
                "too many dots; no more than %d allowed" % self.max_dots,
                value).with_field_and_value(self, value)


@implementer(IId)
class Id(_StrippedNativeStringLine):
    """Id field

    Values of id fields must be either uris or dotted names.

    .. versionchanged:: 4.8.0
        Implement :class:`zope.schema.interfaces.IFromBytes`
    """

    _invalid_exc_type = InvalidId

    def _validate(self, value):
        super()._validate(value)
        if _isuri(value):
            return
        if _isdotted(value) and "." in value:
            return

        raise InvalidId(value).with_field_and_value(self, value)


@implementer(IInterfaceField)
class InterfaceField(Field):
    __doc__ = IInterfaceField.__doc__

    def _validate(self, value):
        super()._validate(value)
        if not IInterface.providedBy(value):
            raise NotAnInterface(value, self.__name__).with_field_and_value(
                self, value)


def _validate_sequence(value_type, value, errors=None):
    """Validates a sequence value.

    Returns a list of validation errors generated during the validation. If
    no errors are generated, returns an empty list.

    value_type is a field. value is the sequence being validated. errors is
    an optional list of errors that will be prepended to the return value.

    To illustrate, we'll use a text value type. All values must be unicode.

       >>> field = TextLine(required=True)

    To validate a sequence of various values:

       >>> errors = _validate_sequence(field, (bytearray(b'foo'), u'bar', 1))
       >>> errors
       [WrongType(bytearray(b'foo'), <...>, ''), WrongType(1, <...>, '')]

    The only valid value in the sequence is the second item. The others
    generated errors.

    We can use the optional errors argument to collect additional errors
    for a new sequence:

       >>> errors = _validate_sequence(field, (2, u'baz'), errors)
       >>> errors # doctest: +NORMALIZE_WHITESPACE
       [WrongType(bytearray(b'foo'), <...>, ''),
        WrongType(1, <...>, ''),
        WrongType(2, <...>, '')]

    """
    if errors is None:
        errors = []
    if value_type is None:
        return errors
    for item in value:
        try:
            value_type.validate(item)
        except ValidationError as error:
            errors.append(error)
    return errors


def _validate_uniqueness(self, value):
    temp_values = []
    for item in value:
        if item in temp_values:
            raise NotUnique(item).with_field_and_value(self, value)

        temp_values.append(item)


@implementer(ICollection)
class Collection(MinMaxLen, Iterable):
    """
    A generic collection implementing
    :class:`zope.schema.interfaces.ICollection`.

    Subclasses can define the attribute ``value_type`` to be a field
    such as an :class:`Object` that will be checked for each member of
    the collection. This can then be omitted from the constructor call.

    They can also define the attribute ``_type`` to be a concrete
    class (or tuple of classes) that the collection itself will
    be checked to be an instance of. This cannot be set in the constructor.

    .. versionchanged:: 4.6.0
       Add the ability for subclasses to specify ``value_type``
       and ``unique``, and allow eliding them from the constructor.
    """
    value_type = None
    unique = False

    def __init__(self, value_type=_NotGiven, unique=_NotGiven, **kw):
        super().__init__(**kw)
        # whine if value_type is not a field
        if value_type is not _NotGiven:
            self.value_type = value_type

        if (self.value_type is not None
                and not IField.providedBy(self.value_type)):
            raise ValueError("'value_type' must be field instance.")
        if unique is not _NotGiven:
            self.unique = unique

    def bind(self, context):
        """See zope.schema._bootstrapinterfaces.IField."""
        clone = super().bind(context)
        # binding value_type is necessary for choices with named vocabularies,
        # and possibly also for other fields.
        if clone.value_type is not None:
            clone.value_type = clone.value_type.bind(context)
        return clone

    def _validate(self, value):
        super()._validate(value)
        errors = _validate_sequence(self.value_type, value)
        if errors:
            try:
                raise WrongContainedType(errors,
                                         self.__name__).with_field_and_value(
                                             self, value)
            finally:
                # Break cycles
                del errors
        if self.unique:
            _validate_uniqueness(self, value)


#: An alternate name for :class:`.Collection`.
#:
#: .. deprecated:: 4.6.0
#:   Use :class:`.Collection` instead.
AbstractCollection = Collection


@implementer(ISequence)
class Sequence(Collection):
    """
    A field representing an ordered sequence.

    .. versionadded:: 4.6.0
    """
    _type = abc.Sequence


@implementer(ITuple)
class Tuple(Sequence):
    """A field representing a Tuple."""
    _type = tuple


@implementer(IMutableSequence)
class MutableSequence(Sequence):
    """
    A field representing a mutable sequence.

    .. versionadded:: 4.6.0
    """
    _type = abc.MutableSequence


@implementer(IList)
class List(MutableSequence):
    """A field representing a List."""
    _type = list


class _AbstractSet(Collection):
    unique = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.unique:  # set members are always unique
            raise TypeError(
                "__init__() got an unexpected keyword argument 'unique'")


@implementer(ISet)
class Set(_AbstractSet):
    """A field representing a set."""
    _type = set


@implementer(IFrozenSet)
class FrozenSet(_AbstractSet):
    _type = frozenset


@implementer(IMapping)
class Mapping(MinMaxLen, Iterable):
    """
    A field representing a mapping.

    .. versionadded:: 4.6.0
    """
    _type = abc.Mapping
    key_type = None
    value_type = None

    def __init__(self, key_type=None, value_type=None, **kw):
        super().__init__(**kw)
        # whine if key_type or value_type is not a field
        if key_type is not None and not IField.providedBy(key_type):
            raise ValueError("'key_type' must be field instance.")
        if value_type is not None and not IField.providedBy(value_type):
            raise ValueError("'value_type' must be field instance.")
        self.key_type = key_type
        self.value_type = value_type

    def _validate(self, value):
        super()._validate(value)
        errors = []
        if self.value_type:
            errors = _validate_sequence(self.value_type, value.values(),
                                        errors)
        errors = _validate_sequence(self.key_type, value, errors)

        if errors:
            try:
                raise WrongContainedType(errors,
                                         self.__name__).with_field_and_value(
                                             self, value)
            finally:
                # Break cycles
                del errors

    def bind(self, object):
        """See zope.schema._bootstrapinterfaces.IField."""
        clone = super().bind(object)
        # binding value_type is necessary for choices with named vocabularies,
        # and possibly also for other fields.
        if clone.key_type is not None:
            clone.key_type = clone.key_type.bind(object)
        if clone.value_type is not None:
            clone.value_type = clone.value_type.bind(object)
        return clone


@implementer(IMutableMapping)
class MutableMapping(Mapping):
    """
    A field representing a mutable mapping.

    .. versionadded:: 4.6.0
    """
    _type = abc.MutableMapping


@implementer(IDict)
class Dict(MutableMapping):
    """A field representing a Dict."""
    _type = dict

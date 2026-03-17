##############################################################################
#
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
"""Bootstrapping fields
"""
__docformat__ = 'restructuredtext'

import decimal
import fractions
import numbers
import sys
import threading
import unicodedata
from math import isinf

from zope.event import notify
from zope.interface import Attribute
from zope.interface import Interface
from zope.interface import Invalid
from zope.interface import implementer
from zope.interface import providedBy
from zope.interface.interface import InterfaceClass
from zope.interface.interfaces import IInterface
from zope.interface.interfaces import IMethod

from zope.schema._bootstrapinterfaces import ConstraintNotSatisfied
from zope.schema._bootstrapinterfaces import IBeforeObjectAssignedEvent
from zope.schema._bootstrapinterfaces import IContextAwareDefaultFactory
from zope.schema._bootstrapinterfaces import IFromBytes
from zope.schema._bootstrapinterfaces import IFromUnicode
from zope.schema._bootstrapinterfaces import IValidatable
from zope.schema._bootstrapinterfaces import NotAContainer
from zope.schema._bootstrapinterfaces import NotAnInterface
from zope.schema._bootstrapinterfaces import NotAnIterator
from zope.schema._bootstrapinterfaces import RequiredMissing
from zope.schema._bootstrapinterfaces import SchemaNotCorrectlyImplemented
from zope.schema._bootstrapinterfaces import SchemaNotFullyImplemented
from zope.schema._bootstrapinterfaces import SchemaNotProvided
from zope.schema._bootstrapinterfaces import StopValidation
from zope.schema._bootstrapinterfaces import TooBig
from zope.schema._bootstrapinterfaces import TooLong
from zope.schema._bootstrapinterfaces import TooShort
from zope.schema._bootstrapinterfaces import TooSmall
from zope.schema._bootstrapinterfaces import ValidationError
from zope.schema._bootstrapinterfaces import WrongType


class _NotGiven:

    def __repr__(self):  # pragma: no cover
        return "<Not Given>"


_NotGiven = _NotGiven()


class ValidatedProperty:

    def __init__(self, name, check=None, allow_none=False):
        self._name = name
        self._check = check
        self._allow_none = allow_none

    def __set__(self, inst, value):
        bypass_validation = (
            (value is None and self._allow_none)
            or value == inst.missing_value
        )
        if not bypass_validation:
            if self._check is not None:
                self._check(inst, value)
            else:
                inst.validate(value)
        inst.__dict__[self._name] = value

    def __get__(self, inst, owner):
        if inst is None:
            return self
        return inst.__dict__[self._name]


class DefaultProperty(ValidatedProperty):

    def __get__(self, inst, owner):
        if inst is None:
            return self
        defaultFactory = inst.__dict__.get('defaultFactory')
        # If there is no default factory, simply return the default.
        if defaultFactory is None:
            return inst.__dict__[self._name]
        # Get the default value by calling the factory. Some factories might
        # require a context to produce a value.
        if IContextAwareDefaultFactory.providedBy(defaultFactory):
            value = defaultFactory(inst.context)
        else:
            value = defaultFactory()
        # Check that the created value is valid.
        if self._check is not None:
            self._check(inst, value)
        elif value != inst.missing_value:
            inst.validate(value)
        return value


def getFields(schema):
    """Return a dictionary containing all the Fields in a schema.
    """
    fields = {}
    for name in schema:
        attr = schema[name]
        if IValidatable.providedBy(attr):
            fields[name] = attr
    return fields


class _DocStringHelpers:
    # Namespace object to hold methods related to ReST formatting
    # docstrings

    @staticmethod
    def docstring_to_lines(docstring):
        # Similar to what sphinx.utils.docstrings.prepare_docstring
        # does. Strip leading equal whitespace, accounting for an initial line
        # that might not have any. Return a list of lines, with a trailing
        # blank line.
        lines = docstring.expandtabs().splitlines()
        # Find minimum indentation of any non-blank lines after ignored lines.

        margin = sys.maxsize
        for line in lines[1:]:
            content = len(line.lstrip())
            if content:
                indent = len(line) - content
                margin = min(margin, indent)
        # Remove indentation from first ignored lines.
        if len(lines) >= 1:
            lines[0] = lines[0].lstrip()

        if margin < sys.maxsize:
            for i in range(1, len(lines)):
                lines[i] = lines[i][margin:]
        # Remove any leading blank lines.
        while lines and not lines[0]:
            lines.pop(0)
        #
        lines.append('')
        return lines

    @staticmethod
    def make_class_directive(kind):
        mod = kind.__module__
        if kind.__module__ in ('__builtin__', 'builtins'):
            mod = ''
        if mod in ('zope.schema._bootstrapfields', 'zope.schema._field'):
            mod = 'zope.schema'
        mod += '.' if mod else ''
        return f':class:`{mod}{kind.__name__}`'

    @classmethod
    def make_field(cls, name, value):
        return f":{name}: {value}"

    @classmethod
    def make_class_field(cls, name, kind):
        if isinstance(kind, (type, InterfaceClass)):
            return cls.make_field(name, cls.make_class_directive(kind))
        if not isinstance(kind, tuple):  # pragma: no cover
            raise TypeError(
                f"make_class_field() can't handle kind {kind!r}")
        return cls.make_field(
            name,
            ', '.join([cls.make_class_directive(t) for t in kind]))


class Field(Attribute):

    # Type restrictions, if any
    _type = None
    context = None

    # If a field has no assigned value, it will be set to missing_value.
    missing_value = None

    # This is the default value for the missing_value argument to the
    # Field constructor.  A marker is helpful since we don't want to
    # overwrite missing_value if it is set differently on a Field
    # subclass and isn't specified via the constructor.
    __missing_value_marker = _NotGiven

    # Note that the "order" field has a dual existance:
    # 1. The class variable Field.order is used as a source for the
    #    monotonically increasing values used to provide...
    # 2. The instance variable self.order which provides a
    #    monotonically increasing value that tracks the creation order
    #    of Field (including Field subclass) instances.
    order = 0

    default = DefaultProperty('default')

    # These were declared as slots in zope.interface, we override them here to
    # get rid of the descriptors so they don't break .bind()
    __name__ = None
    interface = None
    _Element__tagged_values = None

    def __init__(self, title='', description='', __name__='',
                 required=True, readonly=False, constraint=None, default=None,
                 defaultFactory=None, missing_value=__missing_value_marker):
        """Pass in field values as keyword parameters.


        Generally, you want to pass either a title and description, or
        a doc string.  If you pass no doc string, it will be computed
        from the title and description.  If you pass a doc string that
        follows the Python coding style (title line separated from the
        body by a blank line), the title and description will be
        computed from the doc string.  Unfortunately, the doc string
        must be passed as a positional argument.

        Here are some examples:

        >>> from zope.schema._bootstrapfields import Field
        >>> f = Field()
        >>> f.__doc__, str(f.title), str(f.description)
        ('', '', '')

        >>> f = Field(title=u'sample')
        >>> str(f.__doc__), str(f.title), str(f.description)
        ('sample', 'sample', '')

        >>> f = Field(title=u'sample', description=u'blah blah\\nblah')
        >>> str(f.__doc__), str(f.title), str(f.description)
        ('sample\\n\\nblah blah\\nblah', 'sample', 'blah blah\\nblah')
        """
        __doc__ = ''
        # Fix leading whitespace that occurs when using multi-line
        # strings, but don't overwrite the original, we need to
        # preserve it (it could be a MessageID).
        doc_description = '\n'.join(
            _DocStringHelpers.docstring_to_lines(description or '')[:-1]
        )

        if title:
            if doc_description:
                __doc__ = f"{title}\n\n{doc_description}"
            else:
                __doc__ = title
        elif description:
            __doc__ = doc_description

        super().__init__(__name__, __doc__)
        self.title = title
        self.description = description
        self.required = required
        self.readonly = readonly
        if constraint is not None:
            self.constraint = constraint
        self.default = default
        self.defaultFactory = defaultFactory

        # Keep track of the order of field definitions
        Field.order += 1
        self.order = Field.order

        if missing_value is not self.__missing_value_marker:
            self.missing_value = missing_value

    def constraint(self, value):
        return True

    def bind(self, context):
        clone = self.__class__.__new__(self.__class__)
        clone.__dict__.update(self.__dict__)
        clone.context = context
        return clone

    def validate(self, value):
        if value == self.missing_value:
            if self.required:
                raise RequiredMissing(
                    self.__name__
                ).with_field_and_value(self, value)
        else:
            try:
                self._validate(value)
            except StopValidation:
                pass

    def __get_property_names_to_compare(self):
        # Return the set of property names to compare, ignoring
        # order
        names = {}  # used as set of property names, ignoring values
        for interface in providedBy(self):
            names.update(getFields(interface))

        # order will be different always, don't compare it
        names.pop('order', None)
        return names

    def __hash__(self):
        # Equal objects should have equal hashes;
        # equal hashes does not imply equal objects.
        value = (
            (type(self), self.interface) +
            tuple(self.__get_property_names_to_compare())
        )
        return hash(value)

    def __eq__(self, other):
        # should be the same type and in the same interface (or no interface
        # at all)
        if self is other:
            return True

        if type(self) is not type(other) or self.interface != other.interface:
            return False

        # should have the same properties
        names = self.__get_property_names_to_compare()
        # XXX: What about the property names of the other object? Even
        # though it's the same type, it could theoretically have
        # another interface that it `alsoProvides`.

        for name in names:
            if getattr(self, name) != getattr(other, name):
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def _validate(self, value):
        if self._type is not None and not isinstance(value, self._type):
            raise WrongType(
                value, self._type, self.__name__
            ).with_field_and_value(self, value)

        try:
            constraint = self.constraint(value)
        except ValidationError as e:
            if e.field is None:
                e.field = self
            if e.value is None:
                e.value = value
            raise
        if not constraint:
            raise ConstraintNotSatisfied(
                value, self.__name__
            ).with_field_and_value(self, value)

    def get(self, object):
        return getattr(object, self.__name__)

    def query(self, object, default=None):
        return getattr(object, self.__name__, default)

    def set(self, object, value):
        if self.readonly:
            raise TypeError("Can't set values on read-only fields "
                            "(name=%s, class=%s.%s)"
                            % (self.__name__,
                               object.__class__.__module__,
                               object.__class__.__name__))
        setattr(object, self.__name__, value)

    def getExtraDocLines(self):
        """
        Return a list of ReST formatted lines that will be added
        to the docstring returned by :meth:`getDoc`.

        By default, this will include information about the various
        properties of this object, such as required and readonly status,
        required type, and so on.

        This implementation uses a field list for this.

        Subclasses may override or extend.

        .. versionadded:: 4.6.0
        """

        lines = []
        lines.append(_DocStringHelpers.make_class_field(
            'Implementation', type(self)))
        lines.append(_DocStringHelpers.make_field("Read Only", self.readonly))
        lines.append(_DocStringHelpers.make_field("Required", self.required))
        if self.defaultFactory:
            lines.append(_DocStringHelpers.make_field(
                "Default Factory", repr(self.defaultFactory)))
        else:
            lines.append(_DocStringHelpers.make_field(
                "Default Value", repr(self.default)))

        if self._type:
            lines.append(_DocStringHelpers.make_class_field(
                "Allowed Type", self._type))

        # key_type and value_type are commonly used, but don't
        # have a common superclass to add them, so we do it here.
        # Using a rubric produces decent formatting
        for name, rubric in (('key_type', 'Key Type'),
                             ('value_type', 'Value Type')):
            field = getattr(self, name, None)
            if hasattr(field, 'getDoc'):
                lines.append("")
                lines.append(".. rubric:: " + rubric)
                lines.append("")
                lines.append(field.getDoc())

        return lines

    def getDoc(self):
        doc = super().getDoc()
        lines = _DocStringHelpers.docstring_to_lines(doc)
        lines += self.getExtraDocLines()
        lines.append('')

        return '\n'.join(lines)


class Container(Field):

    def _validate(self, value):
        super()._validate(value)

        if not hasattr(value, '__contains__'):
            try:
                iter(value)
            except TypeError:
                raise NotAContainer(value).with_field_and_value(self, value)


# XXX This class violates the Liskov Substituability Principle:  it
#     is derived from Container, but cannot be used everywhere an instance
#     of Container could be, because it's '_validate' is more restrictive.
class Iterable(Container):

    def _validate(self, value):
        super()._validate(value)

        # See if we can get an iterator for it
        try:
            iter(value)
        except TypeError:
            raise NotAnIterator(value).with_field_and_value(self, value)


class Orderable:
    """Values of ordered fields can be sorted.

    They can be restricted to a range of values.

    Orderable is a mixin used in combination with Field.
    """

    min = ValidatedProperty('min', allow_none=True)
    max = ValidatedProperty('max', allow_none=True)

    def __init__(self, min=None, max=None, default=None, **kw):

        # Set min and max to None so that we can validate if
        # one of the super methods invoke validation.
        self.min = None
        self.max = None

        super().__init__(**kw)

        # Now really set min and max
        self.min = min
        self.max = max

        # We've taken over setting default so it can be limited by min
        # and max.
        self.default = default

    def _validate(self, value):
        super()._validate(value)

        if self.min is not None and value < self.min:
            raise TooSmall(value, self.min).with_field_and_value(self, value)

        if self.max is not None and value > self.max:
            raise TooBig(value, self.max).with_field_and_value(self, value)


class MinMaxLen:
    """Expresses constraints on the length of a field.

    MinMaxLen is a mixin used in combination with Field.
    """
    min_length = 0
    max_length = None

    def __init__(self, min_length=0, max_length=None, **kw):
        self.min_length = min_length
        self.max_length = max_length
        super().__init__(**kw)

    def _validate(self, value):
        super()._validate(value)

        if self.min_length is not None and len(value) < self.min_length:
            raise TooShort(value, self.min_length).with_field_and_value(
                self, value)

        if self.max_length is not None and len(value) > self.max_length:
            raise TooLong(value, self.max_length).with_field_and_value(
                self, value)


@implementer(IFromUnicode)
class Text(MinMaxLen, Field):
    """A field containing text used for human discourse."""
    _type = str
    unicode_normalization = 'NFC'

    def __init__(self, *args, **kw):
        self.unicode_normalization = kw.pop(
            'unicode_normalization', self.unicode_normalization)
        super().__init__(*args, **kw)

    def fromUnicode(self, value):
        """
        >>> from zope.schema import Text
        >>> t = Text(constraint=lambda v: 'x' in v)
        >>> t.fromUnicode(b"foo x spam") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        zope.schema._bootstrapinterfaces.WrongType:
            ('foo x spam', <type 'unicode'>, '')
        >>> result = t.fromUnicode(u"foo x spam")
        >>> isinstance(result, bytes)
        False
        >>> str(result)
        'foo x spam'
        >>> t.fromUnicode(u"foo spam") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        zope.schema._bootstrapinterfaces.ConstraintNotSatisfied:
            (u'foo spam', '')
        """
        if isinstance(value, str):
            if self.unicode_normalization:
                value = unicodedata.normalize(
                    self.unicode_normalization, value)
        self.validate(value)
        return value


class TextLine(Text):
    """A text field with no newlines."""

    def constraint(self, value):
        return '\n' not in value and '\r' not in value


class Password(TextLine):
    """A text field containing a text used as a password."""

    UNCHANGED_PASSWORD = object()

    def set(self, context, value):
        """Update the password.

        We use a special marker value that a widget can use
        to tell us that the password didn't change. This is
        needed to support edit forms that don't display the
        existing password and want to work together with
        encryption.

        """
        if value is self.UNCHANGED_PASSWORD:
            return
        super().set(context, value)

    def validate(self, value):
        try:
            existing = bool(self.get(self.context))
        except AttributeError:
            existing = False
        if value is self.UNCHANGED_PASSWORD and existing:
            # Allow the UNCHANGED_PASSWORD value, if a password is set already
            return
        return super().validate(value)


@implementer(IFromUnicode, IFromBytes)
class Bool(Field):
    """
    A field representing a Bool.

    .. versionchanged:: 4.8.0
        Implement :class:`zope.schema.interfaces.IFromBytes`
    """

    _type = bool

    def _validate(self, value):
        # Convert integers to bools to they don't get mis-flagged
        # by the type check later.
        if isinstance(value, int) and not isinstance(value, bool):
            value = bool(value)
        Field._validate(self, value)

    def set(self, object, value):
        if isinstance(value, int) and not isinstance(value, bool):
            value = bool(value)
        Field.set(self, object, value)

    def fromUnicode(self, value):
        """
        >>> from zope.schema._bootstrapfields import Bool
        >>> from zope.schema.interfaces import IFromUnicode
        >>> b = Bool()
        >>> IFromUnicode.providedBy(b)
        True
        >>> b.fromUnicode('True')
        True
        >>> b.fromUnicode('')
        False
        >>> b.fromUnicode('true')
        True
        >>> b.fromUnicode('false') or b.fromUnicode('False')
        False
        >>> b.fromUnicode(u'\u2603')
        False
        """
        # On Python 2, we're relying on the implicit decoding
        # that happens during string comparisons of unicode to native
        # (byte) strings; decoding errors are silently dropped
        v = value == 'True' or value == 'true'
        self.validate(v)
        return v

    def fromBytes(self, value):
        """
        >>> from zope.schema._bootstrapfields import Bool
        >>> from zope.schema.interfaces import IFromBytes
        >>> b = Bool()
        >>> IFromBytes.providedBy(b)
        True
        >>> b.fromBytes(b'True')
        True
        >>> b.fromBytes(b'')
        False
        >>> b.fromBytes(b'true')
        True
        >>> b.fromBytes(b'false') or b.fromBytes(b'False')
        False
        >>> b.fromBytes(u'\u2603'.encode('utf-8'))
        False
        """
        return self.fromUnicode(value.decode("utf-8"))


class InvalidNumberLiteral(ValueError, ValidationError):
    """Invalid number literal."""


@implementer(IFromUnicode, IFromBytes)
class Number(Orderable, Field):
    """
    A field representing a :class:`numbers.Number` and implementing
    :class:`zope.schema.interfaces.INumber`.

    The :meth:`fromUnicode` method will attempt to use the smallest or
    strictest possible type to represent incoming strings::

        >>> from zope.schema._bootstrapfields import Number
        >>> f = Number()
        >>> f.fromUnicode(u"1")
        1
        >>> f.fromUnicode(u"125.6")
        125.6
        >>> f.fromUnicode(u"1+0j")
        (1+0j)
        >>> f.fromUnicode(u"1/2")
        Fraction(1, 2)
        >>> f.fromUnicode(str(2**11234) + '.' + str(2**256))
        ... # doctest: +ELLIPSIS
        Decimal('590...936')
        >>> f.fromUnicode(u"not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidNumberLiteral: Invalid literal for Decimal: 'not a number'

    Similarly, :meth:`fromBytes` will do the same for incoming byte strings::

        >>> from zope.schema._bootstrapfields import Number
        >>> f = Number()
        >>> f.fromBytes(b"1")
        1
        >>> f.fromBytes(b"125.6")
        125.6
        >>> f.fromBytes(b"1+0j")
        (1+0j)
        >>> f.fromBytes(b"1/2")
        Fraction(1, 2)
        >>> f.fromBytes((str(2**11234) + '.' + str(2**256)).encode('ascii'))
        ... # doctest: +ELLIPSIS
        Decimal('590...936')
        >>> f.fromBytes(b"not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidNumberLiteral: Invalid literal for Decimal: 'not a number'

    .. versionadded:: 4.6.0
    .. versionchanged:: 4.8.0
        Implement :class:`zope.schema.interfaces.IFromBytes`

    """
    _type = numbers.Number

    # An ordered sequence of conversion routines. These should accept a
    # native string and produce an object that is an instance of `_type`, or
    # raise a ValueError. The order should be most specific/strictest
    # towards least restrictive (in other words, lowest in the numeric tower
    # towards highest). We break this rule with fractions, though: a
    # floating point number is more generally useful and expected than a
    # fraction, so we attempt to parse as a float before a fraction.
    _unicode_converters = (
        int, float, fractions.Fraction, complex, decimal.Decimal,
    )

    # The type of error we will raise if all conversions fail.
    _validation_error = InvalidNumberLiteral

    def fromUnicode(self, value):
        last_exc = None
        for converter in self._unicode_converters:
            try:
                val = converter(value)
                if (converter is float
                        and isinf(val)
                        and decimal.Decimal in self._unicode_converters):
                    # Pass this on to decimal, if we're allowed
                    val = decimal.Decimal(value)
            except (ValueError, decimal.InvalidOperation) as e:
                last_exc = e
            else:
                self.validate(val)
                return val
        try:
            raise self._validation_error(*last_exc.args).with_field_and_value(
                self, value)
        finally:
            last_exc = None

    def fromBytes(self, value):
        return self.fromUnicode(value.decode('utf-8'))


class Complex(Number):
    """
    A field representing a :class:`numbers.Complex` and implementing
    :class:`zope.schema.interfaces.IComplex`.

    The :meth:`fromUnicode` method is like that for :class:`Number`,
    but doesn't allow Decimals::

        >>> from zope.schema._bootstrapfields import Complex
        >>> f = Complex()
        >>> f.fromUnicode(u"1")
        1
        >>> f.fromUnicode(u"125.6")
        125.6
        >>> f.fromUnicode(u"1+0j")
        (1+0j)
        >>> f.fromUnicode(u"1/2")
        Fraction(1, 2)
        >>> f.fromUnicode(str(2**11234) + '.' + str(2**256))
        ... # doctest: +ELLIPSIS
        inf
        >>> f.fromUnicode(u"not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidNumberLiteral: Invalid literal for Decimal: 'not a number'

    Similarly for :meth:`fromBytes`:

        >>> from zope.schema._bootstrapfields import Complex
        >>> f = Complex()
        >>> f.fromBytes(b"1")
        1
        >>> f.fromBytes(b"125.6")
        125.6
        >>> f.fromBytes(b"1+0j")
        (1+0j)
        >>> f.fromBytes(b"1/2")
        Fraction(1, 2)
        >>> f.fromBytes((str(2**11234) + '.' + str(2**256)).encode('ascii'))
        ... # doctest: +ELLIPSIS
        inf
        >>> f.fromBytes(b"not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidNumberLiteral: Invalid literal for Decimal: 'not a number'

    .. versionadded:: 4.6.0
    """
    _type = numbers.Complex
    _unicode_converters = (int, float, complex, fractions.Fraction)


class Real(Complex):
    """
    A field representing a :class:`numbers.Real` and implementing
    :class:`zope.schema.interfaces.IReal`.

    The :meth:`fromUnicode` method is like that for :class:`Complex`,
    but doesn't allow Decimals or complex numbers::

        >>> from zope.schema._bootstrapfields import Real
        >>> f = Real()
        >>> f.fromUnicode("1")
        1
        >>> f.fromUnicode("125.6")
        125.6
        >>> f.fromUnicode("1+0j") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidNumberLiteral: Invalid literal for Fraction: '1+0j'
        >>> f.fromUnicode("1/2")
        Fraction(1, 2)
        >>> f.fromUnicode(str(2**11234) + '.' + str(2**256))
        ... # doctest: +ELLIPSIS
        inf
        >>> f.fromUnicode("not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidNumberLiteral: Invalid literal for Decimal: 'not a number'

    .. versionadded:: 4.6.0
    """
    _type = numbers.Real
    _unicode_converters = (int, float, fractions.Fraction)


class Rational(Real):
    """
    A field representing a :class:`numbers.Rational` and implementing
    :class:`zope.schema.interfaces.IRational`.

    The :meth:`fromUnicode` method is like that for :class:`Real`,
    but does not allow arbitrary floating point numbers::

        >>> from zope.schema._bootstrapfields import Rational
        >>> f = Rational()
        >>> f.fromUnicode("1")
        1
        >>> f.fromUnicode("1/2")
        Fraction(1, 2)
        >>> f.fromUnicode("125.6")
        Fraction(628, 5)
        >>> f.fromUnicode("1+0j") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidNumberLiteral: Invalid literal for Fraction: '1+0j'
        >>> f.fromUnicode(str(2**11234) + '.' + str(2**256))
        ... # doctest: +ELLIPSIS
        Fraction(195..., 330...)
        >>> f.fromUnicode("not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidNumberLiteral: Invalid literal for Decimal: 'not a number'

    .. versionadded:: 4.6.0
    """
    _type = numbers.Rational
    _unicode_converters = (int, fractions.Fraction)


class InvalidIntLiteral(ValueError, ValidationError):
    """Invalid int literal."""


class Integral(Rational):
    """
    A field representing a :class:`numbers.Integral` and implementing
    :class:`zope.schema.interfaces.IIntegral`.

    The :meth:`fromUnicode` method only allows integral values::

        >>> from zope.schema._bootstrapfields import Integral
        >>> f = Integral()
        >>> f.fromUnicode("125")
        125
        >>> f.fromUnicode("125.6") #doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidIntLiteral: invalid literal for int(): 125.6

    Similarly for :meth:`fromBytes`:

        >>> from zope.schema._bootstrapfields import Integral
        >>> f = Integral()
        >>> f.fromBytes(b"125")
        125
        >>> f.fromBytes(b"125.6") #doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidIntLiteral: invalid literal for int(): 125.6

    .. versionadded:: 4.6.0
    """
    _type = numbers.Integral
    _unicode_converters = (int,)
    _validation_error = InvalidIntLiteral


class Int(Integral):
    """A field representing a native integer type. and implementing
    :class:`zope.schema.interfaces.IInt`.
    """
    _type = int
    _unicode_converters = (int,)


class InvalidDecimalLiteral(ValueError, ValidationError):
    "Raised by decimal fields"


class Decimal(Number):
    """
    A field representing a native :class:`decimal.Decimal` and implementing
    :class:`zope.schema.interfaces.IDecimal`.

    The :meth:`fromUnicode` method only accepts values that can be parsed
    by the ``Decimal`` constructor::

        >>> from zope.schema._field import Decimal
        >>> f = Decimal()
        >>> f.fromUnicode("1")
        Decimal('1')
        >>> f.fromUnicode("125.6")
        Decimal('125.6')
        >>> f.fromUnicode("1+0j") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidDecimalLiteral: Invalid literal for Decimal(): 1+0j
        >>> f.fromUnicode("1/2") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidDecimalLiteral: Invalid literal for Decimal(): 1/2
        >>> f.fromUnicode(str(2**11234) + '.' + str(2**256))
        ... # doctest: +ELLIPSIS
        Decimal('5901...936')
        >>> f.fromUnicode("not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidDecimalLiteral: could not convert string to float: not a number

    Likewise for :meth:`fromBytes`::

        >>> from zope.schema._field import Decimal
        >>> f = Decimal()
        >>> f.fromBytes(b"1")
        Decimal('1')
        >>> f.fromBytes(b"125.6")
        Decimal('125.6')
        >>> f.fromBytes(b"1+0j") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidDecimalLiteral: Invalid literal for Decimal(): 1+0j
        >>> f.fromBytes(b"1/2") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidDecimalLiteral: Invalid literal for Decimal(): 1/2
        >>> f.fromBytes((str(2**11234) + '.' + str(2**256)).encode("ascii"))
        ... # doctest: +ELLIPSIS
        Decimal('5901...936')
        >>> f.fromBytes(b"not a number") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        InvalidDecimalLiteral: could not convert string to float: not a number


    """
    _type = decimal.Decimal
    _unicode_converters = (decimal.Decimal,)
    _validation_error = InvalidDecimalLiteral


class _ObjectsBeingValidated(threading.local):

    def __init__(self):
        super().__init__()
        self.ids_being_validated = set()


def get_schema_validation_errors(schema, value,
                                 _validating_objects=_ObjectsBeingValidated()):
    """
    Validate that *value* conforms to the schema interface *schema*.

    All :class:`zope.schema.interfaces.IField` members of the *schema*
    are validated after being bound to *value*. (Note that we do not check for
    arbitrary :class:`zope.interface.Attribute` members being present.)

    :return: A `dict` mapping field names to `ValidationError` subclasses.
       A non-empty return value means that validation failed.
    """
    errors = {}
    # Interface can be used as schema property for Object fields that plan to
    # hold values of any type.
    # Because Interface does not include any Attribute, it is obviously not
    # worth looping on its methods and filter them all out.
    if schema is Interface:
        return errors
    # if `value` is part of a cyclic graph, we need to break the cycle to avoid
    # infinite recursion. Collect validated objects in a thread local dict by
    # it's python represenation. A previous version was setting a volatile
    # attribute which didn't work with security proxy
    id_value = id(value)
    ids_being_validated = _validating_objects.ids_being_validated
    if id_value in ids_being_validated:
        return errors
    ids_being_validated.add(id_value)
    # (If we have gotten here, we know that `value` provides an interface
    # other than zope.interface.Interface;
    # iow, we can rely on the fact that it is an instance
    # that supports attribute assignment.)

    try:
        for name in schema.names(all=True):
            attribute = schema[name]
            if IMethod.providedBy(attribute):
                continue  # pragma: no cover

            try:
                if IValidatable.providedBy(attribute):
                    # validate attributes that are fields
                    field_value = getattr(value, name)
                    attribute = attribute.bind(value)
                    attribute.validate(field_value)
            except ValidationError as error:
                errors[name] = error
            except AttributeError as error:
                # property for the given name is not implemented
                errors[name] = SchemaNotFullyImplemented(
                    error
                ).with_field_and_value(attribute, None)
    finally:
        ids_being_validated.remove(id_value)
    return errors


def get_validation_errors(schema, value, validate_invariants=True):
    """
    Validate that *value* conforms to the schema interface *schema*.

    This includes checking for any schema validation errors (using
    `get_schema_validation_errors`). If that succeeds, and
    *validate_invariants* is true, then we proceed to check for any
    declared invariants.

    Note that this does not include a check to see if the *value*
    actually provides the given *schema*.

    :return: If there were any validation errors, either schema or
             invariant, return a two tuple (schema_error_dict,
             invariant_error_list). If there were no errors, returns a
             two-tuple where both members are empty.
    """
    schema_error_dict = get_schema_validation_errors(schema, value)
    invariant_errors = []
    # Only validate invariants if there were no previous errors. Previous
    # errors could be missing attributes which would most likely make an
    # invariant raise an AttributeError.

    if validate_invariants and not schema_error_dict:
        try:
            schema.validateInvariants(value, invariant_errors)
        except Invalid:
            # validateInvariants raises a wrapper error around
            # all the errors it got if it got errors, in addition
            # to appending them to the errors list. We don't want
            # that, we raise our own error.
            pass

    return (schema_error_dict, invariant_errors)


class Object(Field):
    """
    Implementation of :class:`zope.schema.interfaces.IObject`.
    """
    schema = None

    def __init__(self, schema=_NotGiven, **kw):
        """
        Object(schema=<Not Given>, *, validate_invariants=True, **kwargs)

        Create an `~.IObject` field. The keyword arguments are as for
        `~.Field`.

        .. versionchanged:: 4.6.0
           Add the keyword argument *validate_invariants*. When true (the
           default), the schema's ``validateInvariants`` method will be
           invoked to check the ``@invariant`` properties of the schema.
        .. versionchanged:: 4.6.0
           The *schema* argument can be ommitted in a subclass
           that specifies a ``schema`` attribute.
        """
        if schema is _NotGiven:
            schema = self.schema

        if not IInterface.providedBy(schema):
            # Note that we don't provide 'self' as the 'field'
            # by calling with_field_and_value(): We're not fully constructed,
            # we don't want this instance to escape.
            raise NotAnInterface(schema, self.__name__)

        self.schema = schema
        self.validate_invariants = kw.pop('validate_invariants', True)
        super().__init__(**kw)

    def getExtraDocLines(self):
        lines = super().getExtraDocLines()
        lines.append(_DocStringHelpers.make_class_field(
            "Must Provide", self.schema))
        return lines

    def _validate(self, value):
        super()._validate(value)

        # schema has to be provided by value
        if not self.schema.providedBy(value):
            raise SchemaNotProvided(self.schema, value).with_field_and_value(
                self, value)

        # check the value against schema
        schema_error_dict, invariant_errors = get_validation_errors(
            self.schema,
            value,
            self.validate_invariants
        )

        if schema_error_dict or invariant_errors:
            errors = list(schema_error_dict.values()) + invariant_errors
            exception = SchemaNotCorrectlyImplemented(
                errors,
                self.__name__,
                schema_error_dict,
                invariant_errors
            ).with_field_and_value(self, value)

            try:
                raise exception
            finally:
                # Break cycles
                del exception
                del invariant_errors
                del schema_error_dict
                del errors

    def set(self, object, value):
        # Announce that we're going to assign the value to the object.
        # Motivation: Widgets typically like to take care of policy-specific
        # actions, like establishing location.
        event = BeforeObjectAssignedEvent(value, self.__name__, object)
        notify(event)
        # The event subscribers are allowed to replace the object, thus we need
        # to replace our previous value.
        value = event.object
        super().set(object, value)


@implementer(IBeforeObjectAssignedEvent)
class BeforeObjectAssignedEvent:
    """An object is going to be assigned to an attribute on another object."""

    def __init__(self, object, name, context):
        self.object = object
        self.name = name
        self.context = context

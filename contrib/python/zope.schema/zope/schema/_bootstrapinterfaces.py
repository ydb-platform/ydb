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
"""Bootstrap schema interfaces and exceptions
"""
from functools import total_ordering

import zope.interface
from zope.interface import Attribute
from zope.interface.interfaces import IInterface

from zope.schema._messageid import _


# pylint:disable=inherit-non-class,keyword-arg-before-vararg,
# pylint:disable=no-self-argument


class StopValidation(Exception):
    """Raised if the validation is completed early.

    Note that this exception should be always caught, since it is just
    a way for the validator to save time.
    """


@total_ordering
class ValidationError(zope.interface.Invalid):
    """Raised if the Validation process fails."""

    #: The field that raised the error, if known.
    field = None

    #: The value that failed validation.
    value = None

    def with_field_and_value(self, field, value):
        self.field = field
        self.value = value
        return self

    def doc(self):
        return self.__class__.__doc__

    def __lt__(self, other):
        # There's no particular reason we choose to sort this way,
        # it's just the way we used to do it with __cmp__.
        if not hasattr(other, 'args'):
            return True
        return self.args < other.args

    def __eq__(self, other):
        if not hasattr(other, 'args'):
            return False
        return self.args == other.args

    # XXX : This is probably inconsistent with __eq__, which is
    # a violation of the language spec.
    __hash__ = zope.interface.Invalid.__hash__  # python3

    def __repr__(self):  # pragma: no cover
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join(repr(arg) for arg in self.args))


class RequiredMissing(ValidationError):
    __doc__ = _("""Required input is missing.""")


class WrongType(ValidationError):
    __doc__ = _("""Object is of wrong type.""")

    #: The type or tuple of types that was expected.
    #:
    #: .. versionadded:: 4.7.0
    expected_type = None

    def __init__(self, value=None, expected_type=None, name=None, *args):
        """
        WrongType(value, expected_type, name)

        .. versionchanged:: 4.7.0
           Added named arguments to the constructor and the `expected_type`
           field.
        """
        ValidationError.__init__(self, value, expected_type, name, *args)
        self.expected_type = expected_type
        self.value = value


class OutOfBounds(ValidationError):
    """
    A value was out of the allowed bounds.

    This is the common superclass for `OrderableOutOfBounds` and
    `LenOutOfBounds`, which in turn are the superclasses for `TooBig`
    and `TooSmall`, and `TooLong` and `TooShort`, respectively.

    .. versionadded:: 4.7.0
    """

    #: The value that was exceeded
    bound = None

    #: A constant for `violation_direction`.
    TOO_LARGE = type('TOO_LARGE', (), {'__slots__': ()})()

    #: A constant for `violation_direction`.
    TOO_SMALL = type('TOO_SMALL', (), {'__slots__': ()})()

    #: Whether the value was too large or
    #: not large enough. One of the values
    #: defined by the constants `TOO_LARGE`
    #: or `TOO_SMALL`
    violation_direction = None

    def __init__(self, value=None, bound=None, *args):
        """
        OutOfBounds(value, bound)
        """
        super().__init__(value, bound, *args)
        self.value = value
        self.bound = bound


class OrderableOutOfBounds(OutOfBounds):
    """
    A value was too big or too small in comparison to another value.

    .. versionadded:: 4.7.0
    """


class TooBig(OrderableOutOfBounds):
    __doc__ = _("""Value is too big""")

    violation_direction = OutOfBounds.TOO_LARGE


class TooSmall(OrderableOutOfBounds):
    __doc__ = _("""Value is too small""")

    violation_direction = OutOfBounds.TOO_SMALL


class LenOutOfBounds(OutOfBounds):
    """
    The length of the value was out of bounds.

    .. versionadded:: 4.7.0
    """


class TooLong(LenOutOfBounds):
    __doc__ = _("""Value is too long""")

    violation_direction = OutOfBounds.TOO_LARGE


class TooShort(LenOutOfBounds):
    __doc__ = _("""Value is too short""")

    violation_direction = OutOfBounds.TOO_SMALL


class InvalidValue(ValidationError):
    __doc__ = _("""Invalid value""")


class ConstraintNotSatisfied(ValidationError):
    __doc__ = _("""Constraint not satisfied""")


class NotAContainer(ValidationError):
    __doc__ = _("""Not a container""")


class NotAnIterator(ValidationError):
    __doc__ = _("""Not an iterator""")


class WrongContainedType(ValidationError):
    __doc__ = _("""Wrong contained type""")

    #: A collection of exceptions raised when validating
    #: the *value*.
    #:
    #: .. versionadded:: 4.7.0
    errors = ()

    def __init__(self, errors=None, name=None, *args):
        """
        WrongContainedType(errors, name)

        .. versionchanged:: 4.7.0
           Added named arguments to the constructor, and the `errors` property.
        """
        super().__init__(errors, name, *args)
        self.errors = errors


class SchemaNotCorrectlyImplemented(WrongContainedType):
    __doc__ = _("""An object failed schema or invariant validation.""")

    #: A dictionary mapping failed attribute names of the
    #: *value* to the underlying exception
    schema_errors = None

    #: A list of exceptions from validating the invariants
    #: of the schema.
    invariant_errors = ()

    def __init__(self, errors=None, name=None, schema_errors=None,
                 invariant_errors=(), *args):
        """
        SchemaNotCorrectlyImplemented(errors, name, schema_errors,
                                      invariant_errors)

        .. versionchanged:: 4.7.0
           Added named arguments to the constructor.
        """
        super().__init__(
            errors, name, *args)
        self.schema_errors = schema_errors
        self.invariant_errors = invariant_errors


class SchemaNotFullyImplemented(ValidationError):
    __doc__ = _("""Schema not fully implemented""")


class SchemaNotProvided(ValidationError):
    __doc__ = _("""Schema not provided""")

    #: The interface that the *value* was supposed to provide,
    #: but does not.
    schema = None

    def __init__(self, schema=None, value=None, *args):
        """
        SchemaNotProvided(schema, value)

        .. versionchanged:: 4.7.0
           Added named arguments to the constructor and the `schema` property.
        """
        super().__init__(schema, value, *args)
        self.schema = schema
        self.value = value


class NotAnInterface(WrongType, SchemaNotProvided):
    """
    Object is not an interface.

    This is a `WrongType` exception for backwards compatibility with
    existing ``except`` clauses, but it is raised when
    ``IInterface.providedBy`` is not true, so it's also a
    `SchemaNotProvided`. The ``expected_type`` field is filled in as
    ``IInterface``; this is not actually a `type`, and
    ``isinstance(thing, IInterface)`` is always false.

    .. versionadded:: 4.7.0
    """

    expected_type = IInterface

    def __init__(self, value, name):
        super().__init__(value, IInterface, name)


class IFromUnicode(zope.interface.Interface):
    """Parse a unicode string to a value

    We will often adapt fields to this interface to support views and
    other applications that need to convert raw data as unicode
    values.
    """

    def fromUnicode(value):
        """Convert a unicode string to a value.
        """


class IFromBytes(zope.interface.Interface):
    """
    Parse a byte string to a value.

    If the string needs to be decoded, decoding is done using UTF-8.

    .. versionadded:: 4.8.0
    """

    def fromBytes(value):
        """Convert a byte string to a value.
        """


class IContextAwareDefaultFactory(zope.interface.Interface):
    """A default factory that requires a context.

    The context is the field context. If the field is not bound, context may
    be ``None``.
    """

    def __call__(context):
        """Returns a default value for the field."""


class IBeforeObjectAssignedEvent(zope.interface.Interface):
    """An object is going to be assigned to an attribute on another object.

    Subscribers to this event can change the object on this event to change
    what object is going to be assigned. This is useful, e.g. for wrapping
    or replacing objects before they get assigned to conform to application
    policy.
    """

    object = Attribute("The object that is going to be assigned.")

    name = Attribute("The name of the attribute under which the object "
                     "will be assigned.")

    context = Attribute("The context object where the object will be "
                        "assigned to.")


class IValidatable(zope.interface.Interface):
    # Internal interface, the base for IField, but used to prevent
    # import recursion. This should *not* be implemented by anything
    # other than IField.
    def validate(value):
        """Validate that the given value is a valid field value.

        Returns nothing but raises an error if the value is invalid.
        It checks everything specific to a Field and also checks
        with the additional constraint.
        """


class NO_VALUE:
    def __repr__(self):  # pragma: no cover
        return '<NO_VALUE>'


NO_VALUE = NO_VALUE()

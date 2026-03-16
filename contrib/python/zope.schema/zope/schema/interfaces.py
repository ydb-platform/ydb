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
"""Schema interfaces and exceptions
"""
__docformat__ = "reStructuredText"

from zope.interface import Attribute
from zope.interface import Interface
from zope.interface.common.mapping import IEnumerableMapping
from zope.interface.interfaces import IInterface

from zope.schema._bootstrapfields import Bool
from zope.schema._bootstrapfields import Complex
from zope.schema._bootstrapfields import Decimal
from zope.schema._bootstrapfields import Field
from zope.schema._bootstrapfields import Int
from zope.schema._bootstrapfields import Integral
from zope.schema._bootstrapfields import Number
from zope.schema._bootstrapfields import Object
from zope.schema._bootstrapfields import Rational
from zope.schema._bootstrapfields import Real
from zope.schema._bootstrapfields import Text
from zope.schema._bootstrapfields import TextLine
# Import from _bootstrapinterfaces only because other packages will expect
# to find these interfaces here.
from zope.schema._bootstrapinterfaces import ConstraintNotSatisfied
from zope.schema._bootstrapinterfaces import IBeforeObjectAssignedEvent
from zope.schema._bootstrapinterfaces import IContextAwareDefaultFactory
from zope.schema._bootstrapinterfaces import IFromBytes
from zope.schema._bootstrapinterfaces import IFromUnicode
from zope.schema._bootstrapinterfaces import InvalidValue
from zope.schema._bootstrapinterfaces import IValidatable
from zope.schema._bootstrapinterfaces import LenOutOfBounds
from zope.schema._bootstrapinterfaces import NotAContainer
from zope.schema._bootstrapinterfaces import NotAnInterface
from zope.schema._bootstrapinterfaces import NotAnIterator
from zope.schema._bootstrapinterfaces import OrderableOutOfBounds
from zope.schema._bootstrapinterfaces import OutOfBounds
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
from zope.schema._bootstrapinterfaces import WrongContainedType
from zope.schema._bootstrapinterfaces import WrongType
from zope.schema._messageid import _


__all__ = [
    # Exceptions
    'ConstraintNotSatisfied',
    'InvalidDottedName',
    'InvalidId',
    'InvalidURI',
    'InvalidValue',
    'LenOutOfBounds',
    'NotAContainer',
    'NotAnInterface',
    'NotAnIterator',
    'NotUnique',
    'OrderableOutOfBounds',
    'OutOfBounds',
    'RequiredMissing',
    'SchemaNotCorrectlyImplemented',
    'SchemaNotFullyImplemented',
    'SchemaNotProvided',
    'StopValidation',
    'TooBig',
    'TooLong',
    'TooShort',
    'TooSmall',
    'Unbound',
    'ValidationError',
    'WrongContainedType',
    'WrongType',


    # Interfaces
    'IASCII',
    'IASCIILine',
    'IAbstractBag',
    'IAbstractSet',
    'IBaseVocabulary',
    'IBeforeObjectAssignedEvent',
    'IBool',
    'IBytes',
    'IBytesLine',
    'IChoice',
    'ICollection',
    'IComplex',
    'IContainer',
    'IContextAwareDefaultFactory',
    'IContextSourceBinder',
    'IDate',
    'IDatetime',
    'IDecimal',
    'IDict',
    'IDottedName',
    'IField',
    'IFieldEvent',
    'IFieldUpdatedEvent',
    'IFloat',
    'IFromBytes',
    'IFromUnicode',
    'IFrozenSet',
    'IId',
    'IInt',
    'IIntegral',
    'IInterfaceField',
    'IIterable',
    'IIterableSource',
    'IIterableVocabulary',
    'ILen',
    'IList',
    'IMapping',
    'IMinMax',
    'IMinMaxLen',
    'IMutableMapping',
    'IMutableSequence',
    'INativeString',
    'INativeStringLine',
    'INumber',
    'IObject',
    'IOrderable',
    'IPassword',
    'IPythonIdentifier',
    'IRational',
    'IReal',
    'ISequence',
    'ISet',
    'ISource',
    'ISourceQueriables',
    'ISourceText',
    'ITerm',
    'IText',
    'ITextLine',
    'ITime',
    'ITimedelta',
    'ITitledTokenizedTerm',
    'ITokenizedTerm',
    'ITreeVocabulary',
    'ITuple',
    'IURI',
    'IUnorderedCollection',
    'IVocabulary',
    'IVocabularyFactory',
    'IVocabularyRegistry',
    'IVocabularyTokenized',
]


class NotUnique(ValidationError):
    __doc__ = _("""One or more entries of sequence are not unique.""")


class InvalidURI(ValidationError):
    __doc__ = _("""The specified URI is not valid.""")


class InvalidId(ValidationError):
    __doc__ = _("""The specified id is not valid.""")


class InvalidDottedName(ValidationError):
    __doc__ = _("""The specified dotted name is not valid.""")


class Unbound(Exception):
    __doc__ = _("""The field is not bound.""")


class IField(IValidatable):
    """Basic Schema Field Interface.

    Fields are used for Interface specifications.  They at least provide
    a title, description and a default value.  You can also
    specify if they are required and/or readonly.

    The Field Interface is also used for validation and specifying
    constraints.

    We want to make it possible for a IField to not only work
    on its value but also on the object this value is bound to.
    This enables a Field implementation to perform validation
    against an object which also marks a certain place.

    Note that many fields need information about the object
    containing a field. For example, when validating a value to be
    set as an object attribute, it may be necessary for the field to
    introspect the object's state. This means that the field needs to
    have access to the object when performing validation::

         bound = field.bind(object)
         bound.validate(value)

    """

    def bind(object):
        """Return a copy of this field which is bound to context.

        The copy of the Field will have the 'context' attribute set
        to 'object'.  This way a Field can implement more complex
        checks involving the object's location/environment.

        Many fields don't need to be bound. Only fields that condition
        validation or properties on an object containing the field
        need to be bound.
        """

    title = TextLine(
        title=_("Title"),
        description=_("A short summary or label"),
        default="",
        required=False,
    )

    description = Text(
        title=_("Description"),
        description=_("A description of the field"),
        default="",
        required=False,
    )

    required = Bool(
        title=_("Required"),
        description=(_("Tells whether a field requires its value to exist.")),
        default=True)

    readonly = Bool(
        title=_("Read Only"),
        description=_("If true, the field's value cannot be changed."),
        required=False,
        default=False)

    default = Field(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )

    missing_value = Field(
        title=_("Missing Value"),
        description=_("""If input for this Field is missing, and that's ok,
                          then this is the value to use""")
    )

    order = Int(
        title=_("Field Order"),
        description=_("""
        The order attribute can be used to determine the order in
        which fields in a schema were defined. If one field is created
        after another (in the same thread), its order will be
        greater.

        (Fields in separate threads could have the same order.)
        """),
        required=True,
        readonly=True,
    )

    def constraint(value):
        """Check a customized constraint on the value.

        You can implement this method with your Field to
        require a certain constraint.  This relaxes the need
        to inherit/subclass a Field you to add a simple constraint.
        Returns true if the given value is within the Field's constraint.
        """

    def validate(value):
        """Validate that the given value is a valid field value.

        Returns nothing but raises an error if the value is invalid.
        It checks everything specific to a Field and also checks
        with the additional constraint.
        """

    def get(object):
        """Get the value of the field for the given object."""

    def query(object, default=None):
        """Query the value of the field for the given object.

        Return the default if the value hasn't been set.
        """

    def set(object, value):
        """Set the value of the field for the object

        Raises a type error if the field is a read-only field.
        """


class IIterable(IField):
    """Fields with a value that can be iterated over.

    The value needs to support iteration; the implementation mechanism
    is not constrained.  (Either `__iter__()` or `__getitem__()` may be
    used.)
    """


class IContainer(IField):
    """Fields whose value allows an ``x in value`` check.

    The value needs to support the `in` operator, but is not
    constrained in how it does so (whether it defines `__contains__()`
    or `__getitem__()` is immaterial).
    """


class IOrderable(IField):
    """Field requiring its value to be orderable.

    The set of value needs support a complete ordering; the
    implementation mechanism is not constrained.  Either `__cmp__()` or
    'rich comparison' methods may be used.
    """


class ILen(IField):
    """A Field requiring its value to have a length.

    The value needs to have a conventional __len__ method.
    """


class IMinMax(IOrderable):
    """Field requiring its value to be between min and max.

    This implies that the value needs to support the IOrderable interface.
    """

    min = Field(
        title=_("Start of the range"),
        required=False,
        default=None
    )

    max = Field(
        title=_("End of the range (including the value itself)"),
        required=False,
        default=None
    )


class IMinMaxLen(ILen):
    """Field requiring the length of its value to be within a range"""

    min_length = Int(
        title=_("Minimum length"),
        description=_("""
        Value after whitespace processing cannot have less than
        `min_length` characters (if a string type) or elements (if
        another sequence type). If `min_length` is ``None``, there is
        no minimum.
        """),
        required=False,
        min=0,  # needs to be a positive number
        default=0)

    max_length = Int(
        title=_("Maximum length"),
        description=_("""
        Value after whitespace processing cannot have greater
        or equal than `max_length` characters (if a string type) or
        elements (if another sequence type). If `max_length` is
        ``None``, there is no maximum."""),
        required=False,
        min=0,  # needs to be a positive number
        default=None)


class IInterfaceField(IField):
    """Fields with a value that is an interface (implementing
    zope.interface.Interface)."""


class IBool(IField):
    """Boolean Field."""

    default = Bool(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )
    required = Bool(
        title=_("Required"),
        description=(_("Tells whether a field requires its value to exist.")),
        required=False,
        default=False)


class IBytes(IMinMaxLen, IIterable, IField):
    """Field containing a byte string (like the python str).

    The value might be constrained to be with length limits.
    """


class IText(IMinMaxLen, IIterable, IField):
    """Field containing a unicode string."""


# for things which are of the str type on both Python 2 and 3
class INativeString(IText):
    """
    A field that always contains the native `str` type.

    .. versionchanged:: 4.9.0
       This is now a distinct type instead of an alias for either `IText`
       or `IBytes`, depending on the platform.
    """


class IASCII(INativeString):
    """Field containing a 7-bit ASCII string. No characters > DEL
    (chr(127)) are allowed

    The value might be constrained to be with length limits.
    """


class IBytesLine(IBytes):
    """Field containing a byte string without newlines."""


class IASCIILine(IASCII):
    """Field containing a 7-bit ASCII string without newlines."""


class ISourceText(IText):
    """Field for source text of object."""


class ITextLine(IText):
    """Field containing a unicode string without newlines."""


class INativeStringLine(ITextLine):
    """
    A field that always contains the native `str` type, without any newlines.

    .. versionchanged:: 4.9.0
       This is now a distinct type instead of an alias for either `ITextLine`
       or `IBytesLine`, depending on the platform.
    """


class IPassword(ITextLine):
    """Field containing a unicode password string without newlines."""


###
# Numbers
###

##
# Abstract numbers
##

class INumber(IMinMax, IField):
    """
    Field containing a generic number: :class:`numbers.Number`.

    .. seealso:: :class:`zope.schema.Number`
    .. versionadded:: 4.6.0
    """
    min = Number(
        title=_("Start of the range"),
        required=False,
        default=None
    )

    max = Number(
        title=_("End of the range (including the value itself)"),
        required=False,
        default=None
    )

    default = Number(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )


class IComplex(INumber):
    """
    Field containing a complex number: :class:`numbers.Complex`.

    .. seealso:: :class:`zope.schema.Real`
    .. versionadded:: 4.6.0
    """
    min = Complex(
        title=_("Start of the range"),
        required=False,
        default=None
    )

    max = Complex(
        title=_("End of the range (including the value itself)"),
        required=False,
        default=None
    )

    default = Complex(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )


class IReal(IComplex):
    """
    Field containing a real number: :class:`numbers.IReal`.

    .. seealso:: :class:`zope.schema.Real`
    .. versionadded:: 4.6.0
    """
    min = Real(
        title=_("Start of the range"),
        required=False,
        default=None
    )

    max = Real(
        title=_("End of the range (including the value itself)"),
        required=False,
        default=None
    )

    default = Real(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )


class IRational(IReal):
    """
    Field containing a rational number: :class:`numbers.IRational`.

    .. seealso:: :class:`zope.schema.Rational`
    .. versionadded:: 4.6.0
    """

    min = Rational(
        title=_("Start of the range"),
        required=False,
        default=None
    )

    max = Rational(
        title=_("End of the range (including the value itself)"),
        required=False,
        default=None
    )

    default = Rational(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )


class IIntegral(IRational):
    """
    Field containing an integral number: class:`numbers.Integral`.

    .. seealso:: :class:`zope.schema.Integral`
    .. versionadded:: 4.6.0
    """
    min = Integral(
        title=_("Start of the range"),
        required=False,
        default=None
    )

    max = Integral(
        title=_("End of the range (including the value itself)"),
        required=False,
        default=None
    )

    default = Integral(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )


##
# Concrete numbers
##

class IInt(IIntegral):
    """
    Field containing exactly the native class :class:`int`.

    .. seealso:: :class:`zope.schema.Int`
    """

    min = Int(
        title=_("Start of the range"),
        required=False,
        default=None
    )

    max = Int(
        title=_("End of the range (including the value itself)"),
        required=False,
        default=None
    )

    default = Int(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )


class IFloat(IReal):
    """
    Field containing exactly the native class :class:`float`.

    :class:`IReal` is a more general interface, allowing all of
    floats, ints, and fractions.

    .. seealso:: :class:`zope.schema.Float`
    """


class IDecimal(INumber):
    """Field containing a :class:`decimal.Decimal`"""

    min = Decimal(
        title=_("Start of the range"),
        required=False,
        default=None
    )

    max = Decimal(
        title=_("End of the range (including the value itself)"),
        required=False,
        default=None
    )

    default = Decimal(
        title=_("Default Value"),
        description=_("""The field default value may be None or a legal
                        field value""")
    )

###
# End numbers
###


class IDatetime(IMinMax, IField):
    """Field containing a datetime."""


class IDate(IMinMax, IField):
    """Field containing a date."""


class ITimedelta(IMinMax, IField):
    """Field containing a timedelta."""


class ITime(IMinMax, IField):
    """Field containing a time."""


def _is_field(value):
    if not IField.providedBy(value):
        return False
    return True


def _fields(values):
    for value in values:
        if not _is_field(value):
            return False
    return True


class IURI(INativeStringLine):
    """A field containing an absolute URI
    """


class IId(INativeStringLine):
    """A field containing a unique identifier

    A unique identifier is either an absolute URI or a dotted name.
    If it's a dotted name, it should have a module/package name as a prefix.
    """


class IDottedName(INativeStringLine):
    """Dotted name field.

    Values of DottedName fields must be Python-style dotted names.
    """

    min_dots = Int(
        title=_("Minimum number of dots"),
        required=True,
        min=0,
        default=0
    )

    max_dots = Int(
        title=_("Maximum number of dots (should not be less than min_dots)"),
        required=False,
        default=None
    )


class IPythonIdentifier(INativeStringLine):
    """
    A single Python identifier, such as a  variable name.

    .. versionadded:: 4.9.0
    """


class IChoice(IField):
    """Field whose value is contained in a predefined set

    Only one, values or vocabulary, may be specified for a given choice.
    """
    vocabulary = Field(
        title=_("Vocabulary or source providing values"),
        description=_("The ISource, IContextSourceBinder or IBaseVocabulary "
                      "object that provides values for this field."),
        required=False,
        default=None
    )

    vocabularyName = TextLine(
        title=_("Vocabulary name"),
        description=_("Vocabulary name to lookup in the vocabulary registry"),
        required=False,
        default=None
    )


# Collections:

# Abstract

class ICollection(IMinMaxLen, IIterable, IContainer):
    """Abstract interface containing a collection value.

    The Value must be iterable and may have a min_length/max_length.
    """

    value_type = Object(
        IField,
        title=_("Value Type"),
        description=_("Field value items must conform to the given type, "
                      "expressed via a Field."))

    unique = Bool(
        title=_('Unique Members'),
        description=_('Specifies whether the members of the collection '
                      'must be unique.'),
        default=False)


class ISequence(ICollection):
    """Abstract interface specifying that the value is ordered"""


class IMutableSequence(ISequence):
    """
    Abstract interface specifying that the value is ordered and
    mutable.

    .. versionadded:: 4.6.0
    """


class IUnorderedCollection(ICollection):
    """Abstract interface specifying that the value cannot be ordered"""


class IAbstractSet(IUnorderedCollection):
    """An unordered collection of unique values."""

    unique = Bool(
        description="This ICollection interface attribute must be True")


class IAbstractBag(IUnorderedCollection):
    """An unordered collection of values, with no limitations on whether
    members are unique"""

    unique = Bool(
        description="This ICollection interface attribute must be False")


# Concrete


class ITuple(ISequence):
    """Field containing a value that implements the API of a conventional
    Python tuple."""


class IList(IMutableSequence):
    """Field containing a value that implements the API of a conventional
    Python list."""


class ISet(IAbstractSet):
    """Field containing a value that implements the API of a Python2.4+ set.
    """


class IFrozenSet(IAbstractSet):
    """Field containing a value that implements the API of a conventional
    Python 2.4+ frozenset."""

# (end Collections)


class IObject(IField):
    """
    Field containing an Object value.

    .. versionchanged:: 4.6.0
       Add the *validate_invariants* attribute.
    """

    schema = Object(
        IInterface,
        description=_("The Interface that defines the Fields comprising the "
                      "Object.")
    )

    validate_invariants = Bool(
        title=_("Validate Invariants"),
        description=_("A boolean that says whether "
                      "``schema.validateInvariants`` is called from "
                      "``self.validate()``. The default is true."),
        default=True,
    )


class IMapping(IMinMaxLen, IIterable, IContainer):
    """
    Field containing an instance of :class:`collections.Mapping`.

    The *key_type* and *value_type* fields allow specification
    of restrictions for keys and values contained in the dict.

    """
    key_type = Object(
        IField,
        description=_("Field keys must conform to the given type, expressed "
                      "via a Field.")
    )

    value_type = Object(
        IField,
        description=_("Field values must conform to the given type, expressed "
                      "via a Field.")
    )


class IMutableMapping(IMapping):
    """
    Field containing an instance of :class:`collections.MutableMapping`.
    """


class IDict(IMutableMapping):
    """Field containing a conventional dict.
    """


class ITerm(Interface):
    """Object representing a single value in a vocabulary."""

    value = Attribute(
        "value", "The value used to represent vocabulary term in a field.")


class ITokenizedTerm(ITerm):
    """Object representing a single value in a tokenized vocabulary.
    """

    # Should be a ``zope.schema.ASCIILine``, but `ASCIILine` is not a bootstrap
    # field. `ASCIILine` is a type of NativeString.
    token = Attribute(
        "token",
        """Token which can be used to represent the value on a stream.

        The value of this attribute must be a non-empty 7-bit ``str``.
        Control characters, including newline, are not allowed.
        """)


class ITitledTokenizedTerm(ITokenizedTerm):
    """A tokenized term that includes a title."""

    title = TextLine(title=_("Title"))


class ISource(Interface):
    """A set of values from which to choose

    Sources represent sets of values. They are used to specify the
    source for choice fields.

    Sources can be large (even infinite), in which case, they need to
    be queried to find out what their values are.
    """

    def __contains__(value):
        """Return whether the value is available in this source
        """


class ISourceQueriables(Interface):
    """A collection of objects for querying sources
    """

    def getQueriables():
        """Return an iterable of objects that can be queried

        The returned obects should be two-tuples with:

        - A unicode id

          The id must uniquely identify the queriable object within
          the set of queriable objects. Furthermore, in subsequent
          calls, the same id should be used for a given queriable
          object.

        - A queriable object

          This is an object for which there is a view provided for
          searching for items.

        """


class IContextSourceBinder(Interface):
    def __call__(context):
        """Return a context-bound instance that implements ISource.
        """


class IBaseVocabulary(ISource):
    """Representation of a vocabulary.

    At this most basic level, a vocabulary only need to support a test
    for containment.  This can be implemented either by __contains__()
    or by sequence __getitem__() (the later only being useful for
    vocabularies which are intrinsically ordered).
    """

    def getTerm(value):
        """Return the ITerm object for the term 'value'.

        If 'value' is not a valid term, this method raises LookupError.
        """


class IIterableSource(ISource):
    """Source which supports iteration over allowed values.

    The objects iteration provides must be values from the source.
    """

    def __iter__():
        """Return an iterator which provides the values from the source."""

    def __len__():
        """Return the number of valid values, or sys.maxint."""


# BBB vocabularies are pending deprecation, hopefully in 3.3
class IIterableVocabulary(Interface):
    """Vocabulary which supports iteration over allowed values.

    The objects iteration provides must conform to the ITerm
    interface.
    """

    def __iter__():
        """Return an iterator which provides the terms from the vocabulary."""

    def __len__():
        """Return the number of valid terms, or sys.maxint."""


class IVocabulary(IIterableVocabulary, IBaseVocabulary):
    """Vocabulary which is iterable."""


class IVocabularyTokenized(IVocabulary):
    """Vocabulary that provides support for tokenized representation.

    Terms returned from getTerm() and provided by iteration must
    conform to ITokenizedTerm.
    """

    def getTermByToken(token):
        """Return an ITokenizedTerm for the passed-in token.

        If `token` is not represented in the vocabulary, `LookupError`
        is raised.
        """


class ITreeVocabulary(IVocabularyTokenized, IEnumerableMapping):
    """A tokenized vocabulary with a tree-like structure.

       The tree is implemented as dictionary, with keys being ITokenizedTerm
       terms and the values being similar dictionaries. Leaf values are empty
       dictionaries.
    """


class IVocabularyRegistry(Interface):
    """
    Registry that provides `IBaseVocabulary` objects for specific
    fields.

    The fields of this package use the vocabulary registry that is
    returned from :func:`~.getVocabularyRegistry`. This is a hook
    function; by default it returns an instance of
    :class:`~.VocabularyRegistry`, but the function
    :func:`~.setVocabularyRegistry` can be used to change this.

    In particular, the package `zope.vocabularyregistry
    <https://pypi.org/project/zope.vocabularyregistry/>`_ can be used
    to install a vocabulary registry that uses the :mod:`zope.component`
    architecture.
    """

    def get(context, name):
        """
        Return the vocabulary named *name* for the content object
        *context*.

        When the vocabulary cannot be found, `LookupError` is raised.
        """


class IVocabularyFactory(Interface):
    """
    An object that can create `IBaseVocabulary`.

    Objects that implement this interface can be registered with the
    default :class:`~.VocabularyRegistry` provided by this package.

    Alternatively, `zope.vocabularyregistry
    <https://pypi.org/project/zope.vocabularyregistry/>`_ can be used
    to install a `IVocabularyRegistry` that looks for named utilities
    using :func:`zope.component.getUtility` which provide this
    interface.
    """

    def __call__(context):
        """The *context* provides a location that vocabulary can make use of.
        """


class IFieldEvent(Interface):

    field = Object(
        IField,
        description="The field that has been changed")

    object = Attribute("The object containing the field")


class IFieldUpdatedEvent(IFieldEvent):
    """
    A field has been modified

    Subscribers will get the old and the new value together with the field
    """

    old_value = Attribute("The value of the field before modification")

    new_value = Attribute("The value of the field after modification")

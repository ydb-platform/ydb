#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
This module contains classes for XML Schema simple data types.
"""
import re
from collections.abc import Callable, Iterator
from decimal import DecimalException, Decimal
from functools import cached_property
from typing import cast, Any, Union
from xml.etree import ElementTree

from elementpath.datatypes import AnyAtomicType, AbstractDateTime, AbstractQName, \
    Duration, UntypedAtomic

import xmlschema.names as nm
from xmlschema.aliases import ElementType, AtomicValueType, ComponentClassType, \
    BaseXsdType, SchemaType, DecodedValueType, NsmapType
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError
from xmlschema.translation import gettext as _
from xmlschema.utils.qnames import local_name, get_extended_qname
from xmlschema.utils.decoding import raw_encode_value
from xmlschema.caching import schema_cache

from .exceptions import XMLSchemaValidationError, XMLSchemaParseError, \
    XMLSchemaCircularityError, XMLSchemaDecodeError, XMLSchemaEncodeError
from .validation import ValidationContext, EncodeContext, ValidationMixin, DecodeContext
from .xsdbase import XsdComponent, XsdType
from .facets import XsdFacet, XsdWhiteSpaceFacet, XsdPatternFacets, \
    XsdEnumerationFacets, XsdAssertionFacet, MULTIPLE_FACETS

FacetsValueType = Union[XsdFacet, Callable[[Any], None], list[XsdAssertionFacet]]
PythonTypeClasses = Union[type[Any], tuple[type[Any]]]


class XsdSimpleType(XsdType, ValidationMixin[str | bytes, DecodedValueType]):
    """
    Base class for simpleTypes definitions. Generally used only for
    instances of xs:anySimpleType.

    ..  <simpleType
          final = (#all | List of (list | union | restriction | extension))
          id = ID
          name = NCName
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (restriction | list | union))
        </simpleType>
    """
    _special_types = {nm.XSD_ANY_TYPE, nm.XSD_ANY_SIMPLE_TYPE}
    _ADMITTED_TAGS: tuple[str, ...] = nm.XSD_SIMPLE_TYPE,
    _REGEX_SPACE = re.compile(r'\s')
    _REGEX_SPACES = re.compile(r'\s+')

    abstract: bool = False
    block: str = ''

    min_length: int | None
    max_length: int | None
    white_space: str | None
    patterns: XsdPatternFacets | None
    validators: tuple[()] | list[XsdFacet | Callable[[Any], None]]
    allow_empty: bool

    datatype: type[Any] = str  # Unicode string as default datatype for XSD simple types
    python_type: type[Any] = str
    instance_types: PythonTypeClasses = str
    to_python: type[Any] | Callable[[str | bytes], AtomicValueType] = str
    from_python: type[str] | Callable[[Any], str] = str

    __slots__ = ('_facets', 'min_length', 'max_length', 'white_space', 'patterns',
                 'validators', 'allow_empty')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: XsdComponent | None = None,
                 name: str | None = None,
                 facets: dict[str | None, FacetsValueType] | None = None) -> None:
        super().__init__(elem, schema, parent, name)
        if not hasattr(self, '_facets'):
            self.facets = facets if facets is not None else {}

    @property
    def facets(self) -> dict[str | None, FacetsValueType]:
        return self._facets

    @facets.setter
    def facets(self, facets: dict[str | None, FacetsValueType]) -> None:
        self._facets = facets
        self.min_length = self.max_length = None
        self.patterns = None
        self.validators = ()

        if not isinstance(self, XsdAtomicBuiltin):
            self._parse_facets(facets)

        if self.min_length:
            self.allow_empty = False
        else:
            self.allow_empty = True

        white_space = getattr(self.get_facet(nm.XSD_WHITE_SPACE), 'value', None)
        if isinstance(self, XsdUnion):
            if not (white_space is None or white_space == 'collapse'):
                msg = _("wrong value %r for facet xs:whiteSpace")
                raise XMLSchemaValueError(msg % white_space)
            self.white_space = 'collapse'

        else:
            self.white_space = white_space

        patterns = self.get_facet(nm.XSD_PATTERN)
        if isinstance(patterns, XsdPatternFacets):
            self.patterns = patterns
            if patterns.re_match('') is None:
                self.allow_empty = False

        enumeration = self.get_facet(nm.XSD_ENUMERATION)
        if isinstance(enumeration, XsdEnumerationFacets) \
                and '' not in enumeration.enumeration:
            self.allow_empty = False

        if facets:
            validators: list[XsdFacet | Callable[[Any], None]]

            if callable(func := facets.get(None)):
                validators = [func]  # a validation function
            else:
                validators = [cast(XsdFacet, v) for k, v in facets.items()
                              if k not in (nm.XSD_WHITE_SPACE, nm.XSD_PATTERN, nm.XSD_ASSERTION)]

            if nm.XSD_ASSERTION in facets:
                assertions = facets[nm.XSD_ASSERTION]
                if isinstance(assertions, list):
                    validators.extend(assertions)
                else:
                    validators.append(assertions)
            if validators:
                self.validators = validators

    def _parse_facets(self, facets: Any) -> None:
        base_type: Any

        if facets and self.base_type is not None:
            if isinstance(self.base_type, XsdSimpleType):
                if self.base_type.name == nm.XSD_ANY_SIMPLE_TYPE:
                    msg = _("facets not allowed for a direct derivation of xs:anySimpleType")
                    self.parse_error(msg)
            elif self.base_type.has_simple_content():
                if self.base_type.content.name == nm.XSD_ANY_SIMPLE_TYPE:
                    msg = _("facets not allowed for a direct content "
                            "derivation of xs:anySimpleType")
                    self.parse_error(msg)

        # Checks the applicability of the facets
        if any(k not in self.admitted_facets for k in facets if k is not None):
            msg = _("one or more facets are not applicable, admitted set is {!r}")
            self.parse_error(msg.format({local_name(e) for e in self.admitted_facets if e}))

        # Check group base_type
        base_type = {t.base_type for t in facets.values() if isinstance(t, XsdFacet)}
        if len(base_type) > 1:
            msg = _("facet group must have the same base type: %r")
            self.parse_error(msg % base_type)
        base_type = base_type.pop() if base_type else None

        # Checks length based facets
        length = getattr(facets.get(nm.XSD_LENGTH), 'value', None)
        min_length = getattr(facets.get(nm.XSD_MIN_LENGTH), 'value', None)
        max_length = getattr(facets.get(nm.XSD_MAX_LENGTH), 'value', None)
        if length is not None:
            if length < 0:
                self.parse_error(_("'length' value must be non a negative integer"))

            if min_length is not None:
                if min_length > length:
                    msg = _("'minLength' value must be less than or equal to 'length'")
                    self.parse_error(msg)
                min_length_facet = base_type.get_facet(nm.XSD_MIN_LENGTH)
                length_facet = base_type.get_facet(nm.XSD_LENGTH)
                if (min_length_facet is None
                        or (length_facet is not None
                            and length_facet.base_type == min_length_facet.base_type)):
                    msg = _("cannot specify both 'length' and 'minLength'")
                    self.parse_error(msg)

            if max_length is not None:
                if max_length < length:
                    msg = _("'maxLength' value must be greater or equal to 'length'")
                    self.parse_error(msg)

                max_length_facet = base_type.get_facet(nm.XSD_MAX_LENGTH)
                length_facet = base_type.get_facet(nm.XSD_LENGTH)
                if max_length_facet is None \
                        or (length_facet is not None
                            and length_facet.base_type == max_length_facet.base_type):
                    msg = _("cannot specify both 'length' and 'maxLength'")
                    self.parse_error(msg)

            min_length = max_length = length
        elif min_length is not None or max_length is not None:
            min_length_facet = base_type.get_facet(nm.XSD_MIN_LENGTH)
            max_length_facet = base_type.get_facet(nm.XSD_MAX_LENGTH)
            if min_length is not None:
                if min_length < 0:
                    msg = _("'minLength' value must be a non negative integer")
                    self.parse_error(msg)
                if max_length is not None and max_length < min_length:
                    msg = _("'maxLength' value is less than 'minLength'")
                    self.parse_error(msg)
                if min_length_facet is not None and min_length_facet.value > min_length:
                    msg = _("'minLength' has a lesser value than parent")
                    self.parse_error(msg)
                if max_length_facet is not None and min_length > max_length_facet.value:
                    msg = _("'minLength' has a greater value than parent 'maxLength'")
                    self.parse_error(msg)

            if max_length is not None:
                if max_length < 0:
                    msg = _("'maxLength' value must be a non negative integer")
                    self.parse_error(msg)
                if min_length_facet is not None and min_length_facet.value > max_length:
                    msg = _("'maxLength' has a lesser value than parent 'minLength'")
                    self.parse_error(msg)
                if max_length_facet is not None and max_length > max_length_facet.value:
                    msg = _("'maxLength' has a greater value than parent")
                    self.parse_error(msg)

        # Checks min/max values
        min_inclusive = getattr(facets.get(nm.XSD_MIN_INCLUSIVE), 'value', None)
        min_exclusive = getattr(facets.get(nm.XSD_MIN_EXCLUSIVE), 'value', None)
        max_inclusive = getattr(facets.get(nm.XSD_MAX_INCLUSIVE), 'value', None)
        max_exclusive = getattr(facets.get(nm.XSD_MAX_EXCLUSIVE), 'value', None)

        if min_inclusive is not None:
            if min_exclusive is not None:
                msg = _("cannot specify both 'minInclusive' and 'minExclusive'")
                self.parse_error(msg)
            if max_inclusive is not None and min_inclusive > max_inclusive:
                msg = _("'minInclusive' must be less or equal to 'maxInclusive'")
                self.parse_error(msg)
            elif max_exclusive is not None and min_inclusive >= max_exclusive:
                msg = _("'minInclusive' must be lesser than 'maxExclusive'")
                self.parse_error(msg)

        elif min_exclusive is not None:
            if max_inclusive is not None and min_exclusive >= max_inclusive:
                msg = _("'minExclusive' must be lesser than 'maxInclusive'")
                self.parse_error(msg)
            elif max_exclusive is not None and min_exclusive > max_exclusive:
                msg = _("'minExclusive' must be less or equal to 'maxExclusive'")
                self.parse_error(msg)

        if max_inclusive is not None and max_exclusive is not None:
            self.parse_error(_("cannot specify both 'maxInclusive' and 'maxExclusive'"))

        # Checks fraction digits
        if nm.XSD_TOTAL_DIGITS in facets:
            if nm.XSD_FRACTION_DIGITS in facets and \
                    facets[nm.XSD_TOTAL_DIGITS].value < facets[nm.XSD_FRACTION_DIGITS].value:
                msg = _("fractionDigits facet value cannot be lesser "
                        "than the value of totalDigits facet")
                self.parse_error(msg)

            total_digits = base_type.get_facet(nm.XSD_TOTAL_DIGITS)
            if total_digits is not None and total_digits.value < facets[nm.XSD_TOTAL_DIGITS].value:
                msg = _("totalDigits facet value cannot be greater than "
                        "the value of the same facet in the base type")
                self.parse_error(msg)

        # Checks XSD 1.1 facets
        if nm.XSD_EXPLICIT_TIMEZONE in facets:
            explicit_tz_facet = base_type.get_facet(nm.XSD_EXPLICIT_TIMEZONE)
            if explicit_tz_facet and explicit_tz_facet.value in ('prohibited', 'required') \
                    and facets[nm.XSD_EXPLICIT_TIMEZONE].value != explicit_tz_facet.value:
                msg = _("the explicitTimezone facet value cannot be changed "
                        "if the base type has the same facet with value %r")
                self.parse_error(msg % explicit_tz_facet.value)

        self.min_length = min_length
        self.max_length = max_length

    @property
    def variety(self) -> str | None:
        return None

    @property
    def simple_type(self) -> 'XsdSimpleType':
        return self

    @cached_property
    def min_value(self) -> AtomicValueType | None:
        min_exclusive: AtomicValueType | None
        min_inclusive: AtomicValueType | None
        min_exclusive = cast(
            AtomicValueType | None,
            getattr(self.get_facet(nm.XSD_MIN_EXCLUSIVE), 'value', None)
        )
        min_inclusive = cast(
            AtomicValueType | None,
            getattr(self.get_facet(nm.XSD_MIN_INCLUSIVE), 'value', None)
        )

        if min_exclusive is None:
            return min_inclusive
        elif min_inclusive is None:
            return min_exclusive
        elif min_inclusive <= min_exclusive:  # type: ignore[operator]
            return min_exclusive
        else:
            return min_inclusive

    @cached_property
    def max_value(self) -> AtomicValueType | None:
        max_exclusive: AtomicValueType | None
        max_inclusive: AtomicValueType | None
        max_exclusive = cast(
            AtomicValueType | None,
            getattr(self.get_facet(nm.XSD_MAX_EXCLUSIVE), 'value', None)
        )
        max_inclusive = cast(
            AtomicValueType | None,
            getattr(self.get_facet(nm.XSD_MAX_INCLUSIVE), 'value', None)
        )

        if max_exclusive is None:
            return max_inclusive
        elif max_inclusive is None:
            return max_exclusive
        elif max_inclusive >= max_exclusive:  # type: ignore[operator]
            return max_exclusive
        else:
            return max_inclusive

    @cached_property
    def enumeration(self) -> list[AtomicValueType | None] | None:
        enumeration = self.get_facet(nm.XSD_ENUMERATION)
        if isinstance(enumeration, XsdEnumerationFacets):
            return enumeration.enumeration
        return None

    @property
    def admitted_facets(self) -> frozenset[str]:
        return self.builders.admitted_facets

    @staticmethod
    def is_simple() -> bool:
        return True

    @staticmethod
    def is_complex() -> bool:
        return False

    @property
    def content_type_label(self) -> str:
        return 'empty' if self.max_length == 0 else 'simple'

    @property
    def root_type(self) -> BaseXsdType:
        if self.base_type is None:
            return self
        elif isinstance(self.base_type, XsdAtomic):
            return self.base_type.primitive_type
        else:
            return self.base_type.root_type

    @property
    def sequence_type(self) -> str:
        if self.is_empty():
            return 'empty-sequence()'

        root_type = self.root_type
        if root_type.name is not None:
            sequence_type = f'xs:{root_type.local_name}'
        else:
            sequence_type = 'xs:untypedAtomic'

        if not self.is_list():
            return sequence_type
        elif self.is_emptiable():
            return f'{sequence_type}*'
        else:
            return f'{sequence_type}+'

    def is_empty(self) -> bool:
        return self.max_length == 0 or \
            self.enumeration is not None and all(v == '' for v in self.enumeration)

    def is_emptiable(self) -> bool:
        return self.allow_empty

    def has_simple_content(self) -> bool:
        return self.max_length != 0

    def has_complex_content(self) -> bool:
        return False

    def has_mixed_content(self) -> bool:
        return False

    def is_element_only(self) -> bool:
        return False

    @schema_cache
    def is_derived(self, other: BaseXsdType, derivation: str | None = None) -> bool:
        if derivation:
            if derivation == self.derivation:
                derivation = None  # derivation mode checked
            elif self.derivation:
                return False

        if other.ref is not None:
            other = other.ref

        if self is other or self.ref is other:
            return True
        elif other.name in self._special_types:
            return derivation != 'extension'
        elif self.base_type is other:
            return True
        elif self.base_type is None:
            if isinstance(other, XsdUnion):
                return any(self.is_derived(m, derivation) for m in other.member_types)
            return False
        elif self.base_type.is_complex():
            if not self.base_type.has_simple_content():
                return False
            return self.base_type.content.is_derived(other, derivation)  # type: ignore
        elif isinstance(other, XsdUnion):
            return any(self.is_derived(m, derivation) for m in other.member_types)
        else:
            return self.base_type.is_derived(other, derivation)

    def is_dynamic_consistent(self, other: BaseXsdType) -> bool:
        return other.name in (nm.XSD_ANY_TYPE, nm.XSD_ANY_SIMPLE_TYPE) \
            or self.is_derived(other) or isinstance(other, XsdUnion) and \
            any(self.is_derived(mt) for mt in other.member_types)

    def normalize(self, text: str | bytes) -> str:
        """
        Normalize and restrict value-space with pre-lexical and lexical facets.

        :param text: text string encoded value.
        :return: a normalized string.
        """
        if isinstance(text, bytes):
            text = text.decode('utf-8')

        match self.white_space:
            case 'replace':
                return self._REGEX_SPACE.sub(' ', text)
            case 'collapse':
                return self._REGEX_SPACES.sub(' ', text).strip()
            case _:
                return text

    def text_decode(self, text: str, validation: str = 'skip',
                    context: ValidationContext | None = None) -> DecodedValueType:
        if context is None:
            self.schema.validation_context.clear()
            return self.raw_decode(text, validation, self.schema.validation_context)
        return self.raw_decode(text, validation, context)

    def text_is_valid(self, text: str, context: ValidationContext | None = None) -> bool:
        if context is None:
            self.schema.validation_context.clear()
            self.raw_decode(text, 'lax', self.schema.validation_context)
            return not self.schema.validation_context.errors
        else:
            try:
                self.raw_decode(text, 'strict', context)
            except XMLSchemaValidationError:
                return False
            else:
                return True

    def get_atomic_value(self, value: AtomicValueType,
                         namespaces: NsmapType | None = None,
                         strict: bool = False) -> AtomicValueType:
        return value

    def raw_decode(self, obj: str | bytes, validation: str,
                   context: ValidationContext) -> DecodedValueType:
        text = self.normalize(obj)
        if self.patterns is not None:
            try:
                self.patterns(text)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err, obj)

        for validator in self.validators:
            try:
                validator(text)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err, obj)

        return text

    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) \
            -> str | None:
        if isinstance(obj, (str, bytes)):
            text = self.normalize(obj)
        else:
            obj = raw_encode_value(obj)
            text = '' if obj is None else obj

        if self.patterns is not None:
            try:
                self.patterns(text)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err)

        for validator in self.validators:
            try:
                validator(text)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err)

        return text if obj is not None else None

    def get_facet(self, tag: str) -> FacetsValueType | None:
        return self.facets.get(tag)


#
# simpleType's derived classes:
class XsdAtomic(XsdSimpleType):
    """
    Class for atomic simpleType definitions. An atomic definition has a base_type
    attribute that refers to primitive or derived atomic built-in type or another
    derived simpleType. The primitive_type here is an extension of XSD definition
    of primitive type, useful for validation.
    """
    _special_types = {nm.XSD_ANY_TYPE, nm.XSD_ANY_SIMPLE_TYPE, nm.XSD_ANY_ATOMIC_TYPE}
    _ADMITTED_TAGS = (nm.XSD_RESTRICTION, nm.XSD_SIMPLE_TYPE)
    primitive_type: XsdSimpleType

    __slots__ = ('primitive_type', 'base_type')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: XsdComponent | None = None,
                 name: str | None = None,
                 facets: dict[str | None, FacetsValueType] | None = None,
                 base_type: BaseXsdType | None = None) -> None:

        if base_type is None:
            self.primitive_type = self
            self.base_type = None
        else:
            self._set_base_type(base_type)
        super().__init__(elem, schema, parent, name, facets)

    def __repr__(self) -> str:
        if self.name is None:
            return '%s(primitive_type=%r)' % (
                self.__class__.__name__, self.primitive_type.local_name
            )
        else:
            return '%s(name=%r)' % (self.__class__.__name__, self.prefixed_name)

    def _set_base_type(self, base_type: BaseXsdType) -> None:
        self.base_type = base_type
        if not hasattr(self, 'white_space') and hasattr(base_type, 'white_space'):
            self.white_space = base_type.white_space

        if hasattr(base_type, 'primitive_type'):
            self.primitive_type = base_type.primitive_type
        elif isinstance(base_type, XsdSimpleType):
            self.primitive_type = base_type  # xs:union, xs:list or a special type
        elif hasattr(base_type.content, 'primitive_type'):
            self.primitive_type = base_type.content.primitive_type
        else:
            assert isinstance(base_type.content, XsdSimpleType)
            self.primitive_type = base_type.content

    @property
    def variety(self) -> str | None:
        return 'atomic'

    @property
    def admitted_facets(self) -> frozenset[str]:
        if self.primitive_type.is_complex():
            return self.builders.admitted_facets
        return self.primitive_type.admitted_facets

    def is_datetime(self) -> bool:
        return issubclass(self.primitive_type.python_type, AbstractDateTime)

    def get_facet(self, tag: str) -> FacetsValueType | None:
        facet = self.facets.get(tag)
        if facet is not None:
            return facet
        elif self.base_type is not None:
            return self.base_type.get_facet(tag)
        else:
            return None

    def get_atomic_value(self, value: AtomicValueType,
                         namespaces: NsmapType | None = None,
                         strict: bool = False) -> AtomicValueType:
        """
        Returns a full decoded atomic value for the given value. Used for ensuring
        that the value is compliant for facets validation. If *strict* is `True`
        keeps the original value unchanged, otherwise raises an error.
        """
        if self.primitive_type is not self:
            return self.primitive_type.get_atomic_value(value, namespaces, strict)
        elif not isinstance(value, self.python_type):
            try:
                return self.to_python(value)  # type: ignore[arg-type]
            except (ValueError, DecimalException, TypeError):
                if strict:
                    raise
        elif self.is_qname():
            if isinstance(value, str):
                return get_extended_qname(value, namespaces)
            elif isinstance(value, AbstractQName):
                return value.expanded_name

        return value

    def is_atomic(self) -> bool:
        return True

    def is_primitive(self) -> bool:
        return self.base_type is None


class XsdAtomicBuiltin(XsdAtomic):
    """
    Class for defining XML Schema built-in simpleType atomic datatypes. An instance
    contains a Python's type transformation and a list of validator functions. The
    'base_type' is not used for validation, but only for reference to the XML Schema
    restriction hierarchy.

    Type conversion methods:
      - to_python(value): Decoding from XML
      - from_python(value): Encoding to XML
    """
    __slots__ = ('datatype', 'instance_types', 'python_type', 'to_python', 'from_python',
                 'post_decode', '_admitted_facets')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 name: str,
                 datatype: type[AnyAtomicType],
                 python_type: PythonTypeClasses,
                 base_type: 'XsdAtomicBuiltin | None' = None,
                 admitted_facets: set[str] | None = None,
                 facets: dict[str | None, FacetsValueType] | None = None,
                 to_python: Callable[[Any], AtomicValueType] | None = None,
                 from_python: Callable[[Any], str] | None = None) -> None:
        """
        :param name: the XSD type's qualified name.
        :param datatype: the XSD datatype.
        :param python_type: the correspondent Python's type. If a tuple of types \
        is provided uses the first and consider the others as compatible types.
        :param base_type: the reference base type, None if it's a primitive type.
        :param admitted_facets: admitted facets tags for type (required for primitive types).
        :param facets: optional facets validators.
        :param to_python: optional decode function.
        :param from_python: optional encode function.
        """
        if isinstance(python_type, tuple):
            self.instance_types, python_type = python_type, python_type[0]
        else:
            self.instance_types = python_type

        if not isinstance(datatype, type):
            raise XMLSchemaTypeError(f"{datatype!r} object is not a type")

        if not isinstance(python_type, type):
            raise XMLSchemaTypeError(f"{python_type!r} object is not a type")

        if base_type is None and not admitted_facets and name != nm.XSD_ERROR:
            raise XMLSchemaValueError("argument 'admitted_facets' must be "
                                      "a not empty set of a primitive type")
        self._admitted_facets = frozenset(admitted_facets) if admitted_facets else None

        super().__init__(elem, schema, None, name, facets, base_type)
        self.datatype = datatype
        self.python_type = python_type
        self.to_python = to_python if to_python is not None else python_type
        self.from_python = from_python if from_python is not None else str

        self.post_decode = name in (nm.XSD_QNAME, nm.XSD_NOTATION, nm.XSD_ID, nm.XSD_IDREF)

    def __repr__(self) -> str:
        return '%s(name=%r)' % (self.__class__.__name__, self.prefixed_name)

    @property
    def admitted_facets(self) -> frozenset[str]:
        return self._admitted_facets or self.primitive_type.admitted_facets

    def raw_decode(self, obj: str | bytes, validation: str,
                   context: ValidationContext) -> DecodedValueType:
        if isinstance(obj, (str, bytes)):
            obj = self.normalize(obj)
        elif not isinstance(obj, self.instance_types):
            msg = _("value is not an instance of {!r}").format(self.instance_types)
            context.decode_error(validation, self, obj, self.to_python, msg)

        if validation == 'skip':
            try:
                return self.to_python(obj)
            except (ValueError, TypeError, DecimalException):
                return raw_encode_value(obj)

        if self.patterns is not None:
            try:
                self.patterns(obj)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err)

        try:
            result: DecodedValueType = self.to_python(obj)
        except (ValueError, DecimalException) as err:
            context.decode_error(validation, self, obj, self.to_python, err)
            return None
        except TypeError:
            # xs:error type (e.g. an XSD 1.1 type alternative used to catch invalid values)
            reason = _("invalid value {!r}").format(obj)
            context.validation_error(validation, self, reason, obj)
            return None

        for validator in self.validators:
            try:
                validator(result)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err)

        if self.post_decode:
            if self.name == nm.XSD_QNAME:
                if ':' not in obj:
                    if default_namespace := context.converter.get(''):
                        result = f"{{{default_namespace}}}{obj}"
                else:
                    try:
                        prefix, name = obj.split(':')
                    except ValueError:
                        pass
                    else:
                        try:
                            result = f"{{{context.namespaces[prefix]}}}{name}"
                        except (TypeError, KeyError):
                            if context.root_namespace != nm.XSD_NAMESPACE:
                                # For a schema is already found by meta-schema validation
                                reason = _("unmapped prefix %r in a QName") % prefix
                                context.validation_error(validation, self, reason, obj)

            elif not context.check_identities:
                pass  # context created from a component
            elif self.name == nm.XSD_IDREF:
                if obj not in context.id_map:
                    context.id_map[obj] = 0
            elif context.level:
                if context.id_list is None:
                    if not context.id_map[obj]:
                        context.id_map[obj] = 1
                    else:
                        reason = _("duplicated xs:ID value {!r}").format(obj)
                        context.validation_error(validation, self, reason, obj)
                elif not context.id_map[obj]:
                    context.id_map[obj] = 1
                    context.id_list.append(obj)
                    if len(context.id_list) > 1 and self.xsd_version == '1.0':
                        reason = _("no more than one attribute of type ID should "
                                   "be present in an element")
                        context.validation_error(validation, self, reason, obj)

                elif obj not in context.id_list or self.xsd_version == '1.0':
                    reason = _("duplicated xs:ID value {!r}").format(obj)
                    context.validation_error(validation, self, reason, obj)

        return result

    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) \
            -> str | None:
        if isinstance(obj, (str, bytes)):
            obj = self.normalize(obj)

        if validation == 'skip':
            try:
                return self.from_python(obj)
            except ValueError:
                return raw_encode_value(obj)

        if isinstance(obj, bool) and self.name != nm.XSD_BOOLEAN:
            msg = _("boolean value {0!r} requires a {1!r} decoder").format(obj, bool)
            context.encode_error(validation, self, obj, self.from_python, msg)

        if isinstance(obj, str):
            try:
                value = self.to_python(obj)
            except (ValueError, TypeError) as err:
                context.encode_error(validation, self, obj, self.to_python, err)
                return None

            text = obj
        else:
            if not isinstance(obj, self.instance_types):
                if not context.untyped_data or not isinstance(obj, UntypedAtomic):
                    msg = _("{0!r} is not an instance of {1!r}").format(obj, self.instance_types)
                    context.encode_error(validation, self, obj, self.to_python, msg)

                try:
                    obj = self.python_type(obj)
                except (ValueError, TypeError) as err:
                    context.encode_error(validation, self, obj, self.to_python, err)
                    return None

            try:
                text = self.from_python(obj)
            except ValueError as err:
                context.encode_error(validation, self, obj, self.from_python, err)
                return None

            value = obj

        for validator in self.validators:
            try:
                validator(value)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err)

        if self.patterns is not None:
            try:
                self.patterns(text)
            except XMLSchemaValidationError as error:
                context.validation_error(validation, self, error)

        return text


class XsdList(XsdSimpleType):
    """
    Class for 'list' definitions. A list definition has an item_type attribute
    that refers to an atomic or union simpleType definition.

    ..  <list
          id = ID
          itemType = QName
          {any attributes with non-schema namespace ...}>
          Content: (annotation?, simpleType?)
        </list>
    """
    item_type: XsdSimpleType
    _ADMITTED_TAGS = nm.XSD_LIST,
    _white_space_elem = ElementTree.Element(
        nm.XSD_WHITE_SPACE, attrib={'value': 'collapse', 'fixed': 'true'}
    )

    __slots__ = ('item_type',)

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: XsdComponent | None,
                 name: str | None = None) -> None:
        facets: dict[str | None, FacetsValueType] | None = {
            nm.XSD_WHITE_SPACE: XsdWhiteSpaceFacet(self._white_space_elem, schema, self, self)
        }
        super().__init__(elem, schema, parent, name, facets)

        if not self.item_type.allow_empty and self.min_length:
            self.allow_empty = False

    def __repr__(self) -> str:
        if self.name is None:
            return '%s(item_type=%r)' % (self.__class__.__name__, self.item_type)
        else:
            return '%s(name=%r)' % (self.__class__.__name__, self.prefixed_name)

    def parse(self, elem: ElementType) -> None:
        if elem.tag != nm.XSD_LIST:
            if elem.tag == nm.XSD_SIMPLE_TYPE:
                for child in elem:
                    if child.tag == nm.XSD_LIST:
                        super().parse(child)
                        return
            raise XMLSchemaValueError(
                f"a {nm.XSD_LIST!r} definition required for {self!r}"
            )
        super().parse(elem)

    def _parse(self) -> None:
        item_type: Any

        child = self._parse_child_component(self.elem)
        if child is not None:
            # Case of a local simpleType declaration inside the list tag
            try:
                item_type = self.builders.simple_type_factory(child, self.schema, self)
            except XMLSchemaParseError as err:
                self.parse_error(err)
                item_type = self.maps.any_atomic_type

            if 'itemType' in self.elem.attrib:
                self.parse_error(_("ambiguous list type declaration"))

        else:
            # List tag with itemType attribute that refers to a global type
            try:
                item_qname = self.schema.resolve_qname(self.elem.attrib['itemType'])
            except (KeyError, ValueError, RuntimeError) as err:
                if 'itemType' not in self.elem.attrib:
                    self.parse_error(_("missing list type declaration"))
                else:
                    self.parse_error(err)
                item_type = self.maps.any_atomic_type
            else:
                try:
                    item_type = self.maps.types[item_qname]
                except KeyError:
                    msg = _("unknown type {!r}")
                    self.parse_error(msg.format(self.elem.attrib['itemType']))
                    item_type = self.maps.any_atomic_type
                except XMLSchemaCircularityError as err:
                    self.parse_error(err, err.elem)
                    item_type = self.maps.any_atomic_type

        if item_type.final == '#all' or 'list' in item_type.final:
            msg = _("'final' value of the itemType %r forbids derivation by list")
            self.parse_error(msg % item_type)

        if item_type.name == nm.XSD_ANY_ATOMIC_TYPE:
            msg = _("cannot use xs:anyAtomicType as base type of a user-defined type")
            self.parse_error(msg)

        if item_type.is_atomic():
            self.item_type = item_type
        else:
            self.parse_error(_("%r: a list must be based on atomic data types") % item_type)
            self.item_type = self.maps.any_atomic_type

    @property
    def variety(self) -> str | None:
        return 'list'

    @property
    def admitted_facets(self) -> frozenset[str]:
        return self.builders.admitted_list_facets

    @property
    def root_type(self) -> BaseXsdType:
        return self.item_type.root_type

    def is_atomic(self) -> bool:
        return False

    def is_list(self) -> bool:
        return True

    @schema_cache
    def is_derived(self, other: BaseXsdType, derivation: str | None = None) -> bool:
        if other.ref is not None:
            other = other.ref
        if derivation and derivation == self.derivation:
            derivation = None  # derivation mode checked

        if derivation and self.derivation and derivation != self.derivation:
            return False
        elif self is other or self.ref is other:
            return True
        elif other.name in self._special_types:
            return derivation != 'extension'
        elif self.item_type is other:
            return True
        else:
            return False

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator[XsdComponent]:
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self
        if self.item_type.parent is not None:
            yield from self.item_type.iter_components(xsd_classes)

    def get_atomic_value(self, value: AtomicValueType,
                         namespaces: NsmapType | None = None,
                         strict: bool = False) -> AtomicValueType:
        return self.item_type.get_atomic_value(value, namespaces=namespaces, strict=strict)

    def raw_decode(self, obj: str | bytes, validation: str, context: ValidationContext) \
            -> list[AtomicValueType | None]:
        items = []
        for chunk in self.normalize(obj).split():
            result = self.item_type.raw_decode(chunk, validation, context)

            if isinstance(result, list):
                reason = _("unexpected nested list item {!r}").format(obj)
                context.validation_error(validation, self, reason, obj)
                items.extend(result)
                continue
            elif not isinstance(context, DecodeContext):
                pass
            elif isinstance(result, context.keep_datatypes) or result is None:
                pass
            elif isinstance(result, str):
                if result[:1] == '{' and self.is_qname():
                    result = chunk
            elif isinstance(result, Decimal):
                if context.decimal_type is not None:
                    result = context.decimal_type(result)
            elif isinstance(result, (AbstractDateTime, Duration)):
                result = chunk.strip()
            else:
                result = str(result)

            items.append(result)
        else:
            return items

    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) -> str | None:
        if not hasattr(obj, '__iter__') or isinstance(obj, (str, bytes)):
            obj = [obj]

        encoded_items: list[Any] = []
        for item in obj:
            encoded_items.append(self.item_type.raw_encode(item, validation, context))

        return ' '.join(item for item in encoded_items if item is not None)


class XsdUnion(XsdSimpleType):
    """
    Class for 'union' definitions. A union definition has a member_types
    attribute that refers to a 'simpleType' definition.

    ..  <union
          id = ID
          memberTypes = list of QName
          {any attributes with non-schema namespace ...}>
          Content: (annotation?, simpleType*)
        </union>
    """
    member_types: list[XsdSimpleType]
    _ADMITTED_TYPES: Any = XsdSimpleType
    _ADMITTED_TAGS = nm.XSD_UNION,

    __slots__ = ('member_types',)

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: XsdComponent | None,
                 name: str | None = None) -> None:
        super().__init__(elem, schema, parent, name, facets=None)

    def __repr__(self) -> str:
        if self.name is None:
            return '%s(member_types=%r)' % (self.__class__.__name__, self.member_types)
        else:
            return '%s(name=%r)' % (self.__class__.__name__, self.prefixed_name)

    def parse(self, elem: ElementType) -> None:
        if elem.tag != nm.XSD_UNION:
            if elem.tag == nm.XSD_SIMPLE_TYPE:
                for child in elem:
                    if child.tag == nm.XSD_UNION:
                        super().parse(child)
                        return
            raise XMLSchemaValueError(
                f"a {nm.XSD_UNION!r} definition required for {self!r}"
            )
        super().parse(elem)

    def _parse(self) -> None:
        mt: Any
        self.member_types = []

        for child in self.elem:
            if child.tag != nm.XSD_ANNOTATION and not callable(child.tag):
                mt = self.builders.simple_type_factory(child, self.schema, self)
                if isinstance(mt, XMLSchemaParseError):
                    self.parse_error(mt)
                else:
                    self.member_types.append(mt)

        if 'memberTypes' in self.elem.attrib:
            for name in self.elem.attrib['memberTypes'].split():
                try:
                    type_qname = self.schema.resolve_qname(name)
                except (KeyError, ValueError, RuntimeError) as err:
                    self.parse_error(err)
                    continue

                try:
                    mt = self.maps.types[type_qname]
                except KeyError:
                    self.parse_error(_("unknown type {!r}").format(type_qname))
                    mt = self.maps.any_atomic_type
                except XMLSchemaParseError as err:
                    self.parse_error(err)
                    mt = self.maps.any_atomic_type
                except XMLSchemaCircularityError as err:
                    self.parse_error(err, err.elem)
                    continue

                if not isinstance(mt, self._ADMITTED_TYPES):
                    msg = _("a {0!r} required, not {1!r}")
                    self.parse_error(msg.format(self._ADMITTED_TYPES, mt))
                    continue
                elif mt.final == '#all' or 'union' in mt.final:
                    msg = _("'final' value of the memberTypes %r forbids derivation by union")
                    self.parse_error(msg % self.member_types)

                self.member_types.append(mt)

        if not self.member_types:
            self.parse_error(_("missing xs:union type declarations"))
            self.member_types = [self.maps.any_atomic_type]
        elif any(mt.name == nm.XSD_ANY_ATOMIC_TYPE for mt in self.member_types):
            msg = _("cannot use xs:anyAtomicType as base type of a user-defined type")
            self.parse_error(msg)
        else:
            if all(not mt.allow_empty for mt in self.member_types):
                self.allow_empty = False

    @property
    def variety(self) -> str | None:
        return 'union'

    @property
    def admitted_facets(self) -> frozenset[str]:
        return self.builders.admitted_union_facets

    def is_atomic(self) -> bool:
        return all(mt.is_atomic() for mt in self.member_types)

    def is_list(self) -> bool:
        return all(mt.is_list() for mt in self.member_types)

    def is_key(self) -> bool:
        return any(mt.is_key() for mt in self.member_types)

    def is_union(self) -> bool:
        return True

    def is_dynamic_consistent(self, other: Any) -> bool:
        return other.name in (nm.XSD_ANY_TYPE, nm.XSD_ANY_SIMPLE_TYPE) or \
            other.is_derived(self) or isinstance(other, self.__class__) and \
            any(mt1.is_derived(mt2) for mt1 in other.member_types for mt2 in self.member_types)

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator[XsdComponent]:
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self
        for mt in filter(lambda x: x.parent is not None, self.member_types):
            yield from mt.iter_components(xsd_classes)

    def get_atomic_value(self, value: AtomicValueType,
                         namespaces: NsmapType | None = None,
                         strict: bool = False) -> AtomicValueType:
        values = []
        for mt in self.member_types:
            try:
                values.append(mt.get_atomic_value(value, namespaces, strict=True))
            except (TypeError, ValueError, DecimalException):
                pass

        if not values:
            if strict:
                msg = f'{self!r} has not compatible types for decoding the given value'
                raise XMLSchemaTypeError(msg)
            return value
        elif any(v == value for v in values):
            return value
        else:
            return values[0]

    def raw_decode(self, obj: str | bytes, validation: str, context: ValidationContext) \
            -> DecodedValueType:
        patterns = context.patterns  # Use and clean pushed patterns
        context.patterns = None

        xsd_type = None
        for mt in self.member_types:
            try:
                result = mt.raw_decode(obj, 'strict', context)
            except XMLSchemaValidationError as err:
                if xsd_type is None and not isinstance(err, XMLSchemaDecodeError):
                    xsd_type = mt
            else:
                if patterns and isinstance(obj, (str, bytes)):
                    try:
                        patterns(mt.normalize(obj))
                    except XMLSchemaValidationError as err:
                        context.validation_error(validation, self, err)
                return result

        if validation == 'skip':
            return raw_encode_value(obj)
        elif validation == 'lax' and xsd_type is not None:
            result = xsd_type.raw_decode(obj, validation, context)
            if patterns and isinstance(obj, (str, bytes)):
                try:
                    patterns(xsd_type.normalize(obj))
                except XMLSchemaValidationError as err:
                    context.validation_error(validation, self, err)
            return result

        msg = _("invalid value {!r}").format(obj)
        context.decode_error(validation, self, obj, self.member_types, msg)
        return None

    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) -> str | None:
        patterns = context.patterns  # Use and clean pushed patterns
        context.patterns = None

        xsd_type = None
        for mt in self.member_types:
            try:
                result = mt.raw_encode(obj, 'strict', context)
            except XMLSchemaValidationError as err:
                if xsd_type is None and not isinstance(err, XMLSchemaEncodeError):
                    xsd_type = mt
            else:
                if patterns and isinstance(result, str):
                    try:
                        patterns(mt.normalize(result))
                    except XMLSchemaValidationError as err:
                        context.validation_error(validation, self, err)
                return result

        if validation == 'skip':
            return raw_encode_value(obj)
        elif validation == 'lax' and xsd_type is not None:
            result = xsd_type.raw_encode(obj, validation, context)
            if patterns and isinstance(result, str):
                try:
                    patterns(result)
                except XMLSchemaValidationError as err:
                    context.validation_error(validation, self, err)
            return result

        msg = _("no type suitable for encoding the object")
        context.encode_error(validation, self, obj, self.member_types, msg)
        return None


class Xsd11Union(XsdUnion):
    _ADMITTED_TYPES = XsdAtomic, XsdList, XsdUnion


class XsdAtomicRestriction(XsdAtomic):
    """
    Class for XSD 1.0 atomic simpleType and complexType's simpleContent restrictions.

    ..  <restriction
          base = QName
          id = ID
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (simpleType?, (minExclusive | minInclusive | maxExclusive |
          maxInclusive | totalDigits | fractionDigits | length | minLength | maxLength |
          enumeration | whiteSpace | pattern)*))
        </restriction>
    """
    parent: 'XsdSimpleType'
    base_type: BaseXsdType
    derivation = 'restriction'
    _CONTENT_TAIL_TAGS = frozenset(
        (nm.XSD_ATTRIBUTE, nm.XSD_ATTRIBUTE_GROUP, nm.XSD_ANY_ATTRIBUTE)
    )

    def parse(self, elem: ElementType) -> None:
        if self.name != nm.XSD_ANY_ATOMIC_TYPE and elem.tag != nm.XSD_RESTRICTION:
            if not (elem.tag == nm.XSD_SIMPLE_TYPE and elem.get('name') is not None):
                raise XMLSchemaValueError(
                    "an xs:restriction definition required for %r." % self
                )
        super().parse(elem)

    def _parse(self) -> None:
        elem = self.elem
        if elem.get('name') == nm.XSD_ANY_ATOMIC_TYPE:
            return  # skip special type xs:anyAtomicType
        elif elem.tag == nm.XSD_SIMPLE_TYPE and elem.get('name') is not None:
            # Global simpleType with internal restriction
            elem = cast(ElementType, self._parse_child_component(elem))

        if self.name is not None and self.parent is not None:
            msg = _("'name' attribute in a local simpleType definition")
            self.parse_error(msg)

        base_type: Any = None
        facets: Any = {}
        has_attributes = False
        has_simple_type_child = False

        if 'base' in elem.attrib:
            try:
                base_qname = self.schema.resolve_qname(elem.attrib['base'])
            except (KeyError, ValueError, RuntimeError) as err:
                self.parse_error(err)
                base_type = self.maps.any_atomic_type
            else:
                if base_qname == self.name:
                    if self.redefine is None:
                        msg = _("wrong definition with self-reference")
                        self.parse_error(msg)
                        base_type = self.maps.any_atomic_type
                    else:
                        base_type = self.base_type
                else:
                    if self.redefine is not None:
                        msg = _("wrong redefinition without self-reference")
                        self.parse_error(msg)

                    try:
                        base_type = self.maps.types[base_qname]
                    except KeyError:
                        self.parse_error(_("unknown type {!r}").format(elem.attrib['base']))

                        base_type = self.maps.any_atomic_type
                    except XMLSchemaParseError as err:
                        self.parse_error(err)
                        base_type = self.maps.any_atomic_type
                    except XMLSchemaCircularityError as err:
                        self.parse_error(err, err.elem)
                        base_type = self.maps.any_atomic_type

            if base_type.is_simple() and base_type.name == nm.XSD_ANY_SIMPLE_TYPE:
                msg = _("wrong base type %r, an atomic type required")
                self.parse_error(msg % nm.XSD_ANY_SIMPLE_TYPE)
            elif base_type.is_complex():
                if base_type.mixed and base_type.is_emptiable():
                    child = self._parse_child_component(elem, strict=False)
                    if child is None:
                        msg = _("an xs:simpleType definition expected")
                        self.parse_error(msg)
                    elif child.tag != nm.XSD_SIMPLE_TYPE:
                        # See: "http://www.w3.org/TR/xmlschema-2/#element-restriction"
                        self.parse_error(_(
                            "when a complexType with simpleContent restricts a complexType "
                            "with mixed and with emptiable content then a simpleType child "
                            "declaration is required"
                        ))
                elif self.parent is None or self.parent.is_simple():
                    msg = _("simpleType restriction of %r is not allowed")
                    self.parse_error(msg % base_type)

        for child in elem:
            if child.tag == nm.XSD_ANNOTATION or callable(child.tag):
                continue
            elif child.tag in self._CONTENT_TAIL_TAGS:
                has_attributes = True  # only if it's a complexType restriction
            elif has_attributes:
                msg = _("unexpected tag after attribute declarations")
                self.parse_error(msg)
            elif child.tag == nm.XSD_SIMPLE_TYPE:
                # Case of simpleType declaration inside a restriction
                if has_simple_type_child:
                    msg = _("duplicated simpleType declaration")
                    self.parse_error(msg)

                if base_type is None:
                    try:
                        base_type = self.builders.simple_type_factory(
                            child, self.schema, self
                        )
                    except XMLSchemaParseError as err:
                        self.parse_error(err, child)
                        base_type = self.maps.any_simple_type
                elif base_type.is_complex():
                    if base_type.admit_simple_restriction():
                        base_type = self.builders.complex_type_class(
                            elem=elem,
                            schema=self.schema,
                            parent=self,
                            content=self.builders.simple_type_factory(
                                child, self.schema, self
                            ),
                            attributes=base_type.attributes,
                            mixed=base_type.mixed,
                            block=base_type.block,
                            final=base_type.final,
                        )
                elif 'base' in elem.attrib:
                    msg = _("restriction with 'base' attribute and simpleType declaration")
                    self.parse_error(msg)

                has_simple_type_child = True
            else:
                try:
                    facet_class = self.builders.facets[child.tag]
                except KeyError:
                    self.parse_error(_("unexpected tag %r in restriction") % child.tag)
                    continue

                if child.tag not in facets:
                    facets[child.tag] = facet_class(child, self.schema, self, base_type)
                elif child.tag not in MULTIPLE_FACETS:
                    msg = _("multiple %r constraint facet")
                    self.parse_error(msg % local_name(child.tag))
                elif child.tag != nm.XSD_ASSERTION:
                    facets[child.tag].append(child)
                else:
                    assertion = facet_class(child, self.schema, self, base_type)
                    try:
                        facets[child.tag].append(assertion)
                    except AttributeError:
                        facets[child.tag] = [facets[child.tag], assertion]

        if base_type is None:
            self.parse_error(_("missing base type in restriction"))
        elif base_type.final == '#all' or 'restriction' in base_type.final:
            msg = _("'final' value of the baseType %r forbids derivation by restriction")
            self.parse_error(msg % base_type)
        if base_type.name == nm.XSD_ANY_ATOMIC_TYPE:
            msg = _("cannot use xs:anyAtomicType as base type of a user-defined type")
            self.parse_error(msg)

        self._set_base_type(base_type)
        self.facets = facets

    @property
    def variety(self) -> str | None:
        return cast(str | None, getattr(self.base_type, 'variety', None))

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator[XsdComponent]:
        if xsd_classes is None:
            yield self
            for facet in self.facets.values():
                if isinstance(facet, list):
                    yield from facet  # XSD 1.1 assertions can be more than one
                elif isinstance(facet, XsdFacet):
                    yield facet  # only XSD facets, skip callables
        else:
            if isinstance(self, xsd_classes):
                yield self
            if issubclass(XsdFacet, xsd_classes):
                for facet in self.facets.values():
                    if isinstance(facet, list):
                        yield from facet
                    elif isinstance(facet, XsdFacet):
                        yield facet

        if self.base_type.parent is not None:
            yield from self.base_type.iter_components(xsd_classes)

    def raw_decode(self, obj: str | bytes, validation: str,
                   context: ValidationContext) -> DecodedValueType:

        if isinstance(obj, (str, bytes)):
            obj = self.normalize(obj)

            if self.patterns:
                if not isinstance(self.primitive_type, XsdUnion):
                    try:
                        self.patterns(obj)
                    except XMLSchemaValidationError as err:
                        context.validation_error(validation, self, err)
                elif context.patterns is None:
                    context.patterns = self.patterns

        if isinstance(self.base_type, XsdSimpleType):
            base_type = self.base_type
        elif isinstance(self.base_type.content, XsdSimpleType):
            base_type = self.base_type.content
        elif self.base_type.mixed:
            return obj
        else:  # pragma: no cover
            msg = _("wrong base type %r: a simpleType or a complexType "
                    "with simple or mixed content required")
            raise XMLSchemaValueError(msg % self.base_type)

        result = base_type.raw_decode(obj, validation, context)
        if result is not None:
            for validator in self.validators:
                try:
                    validator(result)
                except XMLSchemaValidationError as err:
                    context.validation_error(validation, self, err)

        return result

    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) -> str | None:
        base_type: XsdSimpleType
        if isinstance(self.base_type, XsdSimpleType):
            base_type = self.base_type
        elif isinstance(self.base_type.content, XsdSimpleType) and self.max_length != 0:
            base_type = self.base_type.content
        elif self.base_type.mixed:
            return str(obj)
        else:  # pragma: no cover
            msg = _("wrong base type %r: a simpleType or a complexType "
                    "with simple or mixed content required")
            raise XMLSchemaValueError(msg % self.base_type)

        if self.is_list():
            if not hasattr(obj, '__iter__') or isinstance(obj, (str, bytes)):
                obj = [] if obj is None or obj == '' else [obj]
        elif isinstance(obj, (str, bytes)):
            obj = self.normalize(obj)

        if self.patterns:
            if context.patterns is None and isinstance(self.primitive_type, XsdUnion):
                context.patterns = self.patterns

        result = base_type.raw_encode(obj, validation, context)

        if self.validators:
            value: DecodedValueType
            if isinstance(obj, list):
                value = [self.get_atomic_value(x, context.namespaces) for x in obj]
            else:
                value = self.get_atomic_value(obj, context.namespaces)

            for validator in self.validators:
                try:
                    validator(value)
                except XMLSchemaValidationError as err:
                    context.validation_error(validation, self, err)

        if self.patterns and not isinstance(self.primitive_type, XsdUnion) and result is not None:
            try:
                self.patterns(result)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err)

        return result

    def is_list(self) -> bool:
        return self.primitive_type.is_list()

    def is_union(self) -> bool:
        return self.primitive_type.is_union()


class Xsd11AtomicRestriction(XsdAtomicRestriction):
    """
    Class for XSD 1.1 atomic simpleType and complexType's simpleContent restrictions.

    ..  <restriction
          base = QName
          id = ID
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (simpleType?, (minExclusive | minInclusive | maxExclusive |
          maxInclusive | totalDigits | fractionDigits | length | minLength | maxLength |
          enumeration | whiteSpace | pattern | assertion | explicitTimezone |
          {any with namespace: ##other})*))
        </restriction>
    """
    _CONTENT_TAIL_TAGS = nm.CONTENT_TAIL_TAGS

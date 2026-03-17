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
This module contains declarations and classes for XML Schema constraint facets.
"""
import re
import math
import operator
from abc import abstractmethod
from collections.abc import MutableSequence
from typing import TYPE_CHECKING, Any, cast, overload, Optional, Union
from xml.etree.ElementTree import Element

from elementpath import XPathContext, ElementPathError, \
    translate_pattern, RegexError, ElementNode
from elementpath.datatypes import AbstractQName

import xmlschema.names as nm
from xmlschema.aliases import ElementType, SchemaType, AtomicValueType, BaseXsdType
from xmlschema.translation import gettext as _
from xmlschema.utils.decoding import count_digits
from xmlschema.utils.qnames import local_name

from .exceptions import XMLSchemaValidationError, XMLSchemaDecodeError
from .helpers import get_xsd_annotation, parse_xpath_default_namespace
from .xsdbase import XsdComponent, XsdAnnotation

if TYPE_CHECKING:
    from .simple_types import XsdAtomicBuiltin, XsdList, XsdUnion, XsdAtomicRestriction  # noqa

LaxDecodeType = tuple[Any, list[XMLSchemaValidationError]]


class XsdFacet(XsdComponent):
    """
    XML Schema constraining facets base class.
    """
    value: Optional[AtomicValueType]
    base_type: Optional[BaseXsdType]
    base_value: Optional[AtomicValueType] = None
    fixed = False

    __slots__ = ('base_type', 'value', 'validate')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: Union['XsdAtomicBuiltin', 'XsdList', 'XsdUnion', 'XsdAtomicRestriction'],
                 base_type: Optional[BaseXsdType]) -> None:
        self.value = None
        self.base_type = base_type
        self.validate = self.skip_validation
        super().__init__(elem, schema, parent)

    def __repr__(self) -> str:
        return '%s(value=%r, fixed=%r)' % (self.__class__.__name__, self.value, self.fixed)

    def __call__(self, value: Any) -> None:
        self.validate(value)

    def skip_validation(self, value: Any) -> None:
        return

    def _parse(self) -> None:
        if 'fixed' in self.elem.attrib and self.elem.attrib['fixed'] in ('true', '1'):
            self.fixed = True
        base_facet = self.base_facet
        if base_facet is not None:
            self.base_value = base_facet.value

        try:
            self._parse_value(self.elem)
        except (KeyError, TypeError, ValueError) as err:
            self.parse_error(err)
        else:
            if base_facet is not None and base_facet.fixed and \
                    base_facet.value is not None and self.value != base_facet.value:
                msg = _("{0!r} facet value is fixed to {1!r}")
                self.parse_error(msg.format(local_name(self.elem.tag), base_facet.value))

    def _parse_value(self, elem: ElementType) -> Union[None, AtomicValueType, re.Pattern[str]]:
        self.value = elem.attrib['value']  # pragma: no cover
        return None

    def invalid_type_error(self, error: Union[TypeError, AttributeError], value: Any) -> None:
        reason = _("invalid type {!r} provided: {}").format(type(value), str(error))
        raise XMLSchemaValidationError(self, value, reason) from None

    @property
    def base_facet(self) -> Optional['XsdFacet']:
        """
        An object of the same type if the instance has a base facet, `None` otherwise.
        """
        base_type: Optional[BaseXsdType] = self.base_type
        tag = self.elem.tag
        while base_type is not None:
            try:
                base_facet = base_type.facets[tag]  # type: ignore[union-attr]
            except (AttributeError, KeyError):
                base_type = base_type.base_type
            else:
                assert isinstance(base_facet, XsdFacet)
                return base_facet
        else:
            return None


class XsdWhiteSpaceFacet(XsdFacet):
    """
    XSD *whiteSpace* facet.

    ..  <whiteSpace
          fixed = boolean : false
          id = ID
          value = (collapse | preserve | replace)
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </whiteSpace>
    """
    value: str
    _ADMITTED_TAGS = nm.XSD_WHITE_SPACE,

    def _parse_value(self, elem: ElementType) -> None:
        self.value = elem.attrib['value']
        if self.value == 'collapse':
            self.validate = self.collapse_white_space_validator
        elif self.value == 'replace':
            if self.base_value == 'collapse':
                self.parse_error(_("facet value can be only 'collapse'"))
            self.validate = self.replace_white_space_validator
        elif self.base_value == 'collapse':
            self.parse_error(_("facet value can be only 'collapse'"))
        elif self.base_value == 'replace':
            self.parse_error(_("facet value can be only 'replace' or 'collapse'"))

    def replace_white_space_validator(self, value: str) -> None:
        try:
            if '\t' in value or '\n' in value:
                raise XMLSchemaValidationError(
                    self, value, _("value contains tabs or newlines")
                )
        except TypeError as err:
            self.invalid_type_error(err, value)

    def collapse_white_space_validator(self, value: str) -> None:
        try:
            if '\t' in value or '\n' in value or '  ' in value:
                raise XMLSchemaValidationError(
                    self, value, _("value contains non collapsed white spaces")
                )
        except TypeError as err:
            self.invalid_type_error(err, value)


class XsdLengthFacet(XsdFacet):
    """
    XSD *length* facet.

    ..  <length
          fixed = boolean : false
          id = ID
          value = nonNegativeInteger
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </length>
    """
    value: int
    base_type: BaseXsdType
    base_value: int | None
    _ADMITTED_TAGS = nm.XSD_LENGTH,

    def _parse_value(self, elem: ElementType) -> None:
        self.value = int(elem.attrib['value'])
        if self.base_value is not None and self.value != self.base_value:
            msg = _("base facet has a different length ({})")
            self.parse_error(msg.format(self.base_value))

        primitive_type = getattr(self.base_type, 'primitive_type', None)
        if primitive_type is None or primitive_type.name not in nm.QNAME_TAGS:
            # See: https://www.w3.org/Bugs/Public/show_bug.cgi?id=4009
            self.validate = self.length_validator

    def length_validator(self, value: Any) -> None:
        try:
            if len(value) != self.value:
                reason = _("length has to be {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            if not isinstance(value, AbstractQName):
                self.invalid_type_error(err, value)


class XsdMinLengthFacet(XsdFacet):
    """
    XSD *minLength* facet.

    ..  <minLength
          fixed = boolean : false
          id = ID
          value = nonNegativeInteger
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </minLength>
    """
    value: int
    base_type: BaseXsdType
    base_value: int | None
    _ADMITTED_TAGS = nm.XSD_MIN_LENGTH,

    def _parse_value(self, elem: ElementType) -> None:
        self.value = int(elem.attrib['value'])
        if self.base_value is not None and self.value < self.base_value:
            msg = _("base facet has a greater min length ({})")
            self.parse_error(msg.format(self.base_value))

        primitive_type = getattr(self.base_type, 'primitive_type', None)
        if primitive_type is None or primitive_type.name not in nm.QNAME_TAGS:
            # See: https://www.w3.org/Bugs/Public/show_bug.cgi?id=4009
            self.validate = self.min_length_validator

    def min_length_validator(self, value: Any) -> None:
        try:
            if len(value) < self.value:
                reason = _("value length cannot be lesser than {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            if not isinstance(value, AbstractQName):
                self.invalid_type_error(err, value)


class XsdMaxLengthFacet(XsdFacet):
    """
    XSD *maxLength* facet.

    ..  <maxLength
          fixed = boolean : false
          id = ID
          value = nonNegativeInteger
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </maxLength>
    """
    value: int
    base_type: BaseXsdType
    base_value: int | None
    _ADMITTED_TAGS = nm.XSD_MAX_LENGTH,

    def _parse_value(self, elem: ElementType) -> None:
        self.value = int(elem.attrib['value'])
        if self.base_value is not None and self.value > self.base_value:
            msg = _("base type has a lesser max length ({})")
            self.parse_error(msg.format(self.base_value))

        primitive_type = getattr(self.base_type, 'primitive_type', None)
        if primitive_type is None or primitive_type.name not in nm.QNAME_TAGS:
            # See: https://www.w3.org/Bugs/Public/show_bug.cgi?id=4009
            self.validate = self.max_length_validator

    def max_length_validator(self, value: Any) -> None:
        try:
            if len(value) > self.value:
                reason = _("value length cannot be greater than {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            if not isinstance(value, AbstractQName):
                self.invalid_type_error(err, value)


class XsdMinInclusiveFacet(XsdFacet):
    """
    XSD *minInclusive* facet.

    ..  <minInclusive
          fixed = boolean : false
          id = ID
          value = anySimpleType
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </minInclusive>
    """
    base_type: BaseXsdType
    _ADMITTED_TAGS = nm.XSD_MIN_INCLUSIVE,

    def _parse_value(self, elem: ElementType) -> None:
        self.schema.validation_context.clear()
        value = self.base_type.text_decode(
            elem.attrib['value'], 'lax', self.schema.validation_context
        )

        if isinstance(value, list):
            raise TypeError("attribute 'value' must be atomic")
        self.value = value
        self.validate = self.__call__

        for e in self.schema.validation_context.errors:
            self.parse_error(_("invalid restriction: {}").format(e.reason))

        self.value = value

    def __call__(self, value: Any) -> None:
        try:
            if value < self.value:
                reason = _("value has to be greater or equal than {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            self.invalid_type_error(err, value)


class XsdMinExclusiveFacet(XsdFacet):
    """
    XSD *minExclusive* facet.

    ..  <minExclusive
          fixed = boolean : false
          id = ID
          value = anySimpleType
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </minExclusive>
    """
    base_type: BaseXsdType
    _ADMITTED_TAGS = nm.XSD_MIN_EXCLUSIVE,

    def _parse_value(self, elem: ElementType) -> None:
        self.schema.validation_context.clear()
        value = self.base_type.text_decode(
            elem.attrib['value'], 'lax', self.schema.validation_context
        )

        if isinstance(value, list):
            raise TypeError("attribute 'value' must be atomic")
        self.value = value
        self.validate = self.__call__

        for e in self.schema.validation_context.errors:
            if not isinstance(e.validator, self.__class__) or e.validator.value != self.value:
                self.parse_error(_("invalid restriction: {}").format(e.reason))

        facet: Any = self.base_type.get_facet(nm.XSD_MAX_INCLUSIVE)
        if facet is not None and facet.value == self.value:
            msg = _("invalid restriction: {} is also the maximum")
            self.parse_error(msg.format(self.value))

    def __call__(self, value: Any) -> None:
        try:
            if value <= self.value:
                reason = _("value has to be greater than {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            self.invalid_type_error(err, value)


class XsdMaxInclusiveFacet(XsdFacet):
    """
    XSD *maxInclusive* facet.

    ..  <maxInclusive
          fixed = boolean : false
          id = ID
          value = anySimpleType
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </maxInclusive>
    """
    base_type: BaseXsdType
    _ADMITTED_TAGS = nm.XSD_MAX_INCLUSIVE,

    def _parse_value(self, elem: ElementType) -> None:
        self.schema.validation_context.clear()
        value = self.base_type.text_decode(
            elem.attrib['value'], 'lax', self.schema.validation_context
        )

        if isinstance(value, list):
            raise TypeError("attribute 'value' must be atomic")
        self.value = value
        self.validate = self.__call__

        for e in self.schema.validation_context.errors:
            self.parse_error(_("invalid restriction: {}").format(e.reason))

    def __call__(self, value: Any) -> None:
        try:
            if value > self.value:
                reason = _("value has to be less than or equal than {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            self.invalid_type_error(err, value)


class XsdMaxExclusiveFacet(XsdFacet):
    """
    XSD *maxExclusive* facet.

    ..  <maxExclusive
          fixed = boolean : false
          id = ID
          value = anySimpleType
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </maxExclusive>
    """
    base_type: BaseXsdType
    _ADMITTED_TAGS = nm.XSD_MAX_EXCLUSIVE,

    def _parse_value(self, elem: ElementType) -> None:
        self.schema.validation_context.clear()
        value = self.base_type.text_decode(
            elem.attrib['value'], 'lax', self.schema.validation_context
        )

        if isinstance(value, list):
            raise TypeError("attribute 'value' must be atomic")
        self.value = value
        self.validate = self.__call__

        for e in self.schema.validation_context.errors:
            if not isinstance(e.validator, self.__class__) or e.validator.value != self.value:
                self.parse_error(_("invalid restriction: {}").format(e.reason))

        facet: Any = self.base_type.get_facet(nm.XSD_MIN_INCLUSIVE)
        if facet is not None and facet.value == self.value:
            msg = _("invalid restriction: {} is also the minimum")
            self.parse_error(msg.format(self.value))

    def __call__(self, value: Any) -> None:
        try:
            if value >= self.value:
                reason = _("value has to be lesser than {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            self.invalid_type_error(err, value)


class XsdTotalDigitsFacet(XsdFacet):
    """
    XSD *totalDigits* facet.

    ..  <totalDigits
          fixed = boolean : false
          id = ID
          value = positiveInteger
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </totalDigits>
    """
    value: int
    base_type: BaseXsdType
    _ADMITTED_TAGS = nm.XSD_TOTAL_DIGITS,

    def _parse_value(self, elem: ElementType) -> None:
        # Errors are detected by meta-schema validation. For schemas with
        # 'lax' validation mode use 9999 in case of an invalid value.
        self.validate = self.__call__

        try:
            self.value = int(elem.attrib['value'])
        except (ValueError, KeyError):
            self.value = 9999
        else:
            if self.value < 1:
                self.value = 9999

            facet: Any = self.base_type.get_facet(nm.XSD_TOTAL_DIGITS)
            if facet is not None and facet.value < self.value:
                msg = _("invalid restriction: base value is lower ({})")
                self.parse_error(msg.format(facet.value))

    def __call__(self, value: Any) -> None:
        try:
            a, b = count_digits(value)
            if operator.add(a, b) <= self.value:
                return
        except TypeError as err:
            self.invalid_type_error(err, value)
        except (ValueError, ArithmeticError) as err:
            raise XMLSchemaValidationError(self, value, str(err)) from None
        else:
            reason = _("the number of digits has to be lesser or equal "
                       "than {!r}").format(self.value)
            raise XMLSchemaValidationError(self, value, reason)


class XsdFractionDigitsFacet(XsdFacet):
    """
    XSD *fractionDigits* facet.

    ..  <fractionDigits
          fixed = boolean : false
          id = ID
          value = nonNegativeInteger
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </fractionDigits>
    """
    value: int
    base_type: BaseXsdType
    _ADMITTED_TAGS = nm.XSD_FRACTION_DIGITS,

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: 'XsdAtomicRestriction',
                 base_type: BaseXsdType) -> None:

        super().__init__(elem, schema, parent, base_type)
        if not base_type.is_derived(self.maps.types[nm.XSD_DECIMAL]):
            msg = _("fractionDigits facet can be applied only to types derived from xs:decimal")
            self.parse_error(msg)

    def _parse_value(self, elem: ElementType) -> None:
        # Errors are detected by meta-schema validation. For schemas with
        # 'lax' validation mode use 9999 in case of an invalid value.
        self.validate = self.__call__

        try:
            self.value = int(elem.attrib['value'])
        except (ValueError, KeyError):
            self.value = 9999
        else:
            if self.value < 0:
                self.value = 9999
            elif self.value > 0 and self.base_type.is_derived(self.maps.types[nm.XSD_INTEGER]):
                msg = _("fractionDigits facet value must be 0 for types derived from xs:integer")
                raise ValueError(msg)

            facet: Any = self.base_type.get_facet(nm.XSD_FRACTION_DIGITS)
            if facet is not None and facet.value < self.value:
                msg = _("invalid restriction: base value is lower ({})")
                self.parse_error(msg.format(facet.value))

    def __call__(self, value: Any) -> None:
        try:
            if count_digits(value)[1] <= self.value:
                return
        except TypeError as err:
            self.invalid_type_error(err, value)
        except (ValueError, ArithmeticError) as err:
            raise XMLSchemaValidationError(self, value, str(err)) from None
        else:
            reason = _("the number of fraction digits has to be lesser "
                       "or equal than {!r}").format(self.value)
            raise XMLSchemaValidationError(self, value, reason)


class XsdExplicitTimezoneFacet(XsdFacet):
    """
    XSD 1.1 *explicitTimezone* facet.

    ..  <explicitTimezone
          fixed = boolean : false
          id = ID
          value = NCName
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </explicitTimezone>
    """
    value: str
    base_type: BaseXsdType
    _ADMITTED_TAGS = nm.XSD_EXPLICIT_TIMEZONE,

    def _parse_value(self, elem: ElementType) -> None:
        self.value = elem.attrib['value']
        if self.value == 'prohibited':
            self.validate = self.prohibited_timezone_validator
        elif self.value == 'required':
            self.validate = self.required_timezone_validator

        facet: Any = self.base_type.get_facet(nm.XSD_EXPLICIT_TIMEZONE)
        if facet is not None and facet.value != self.value and facet.value != 'optional':
            msg = _("invalid restriction from {!r}")
            self.parse_error(msg.format(facet.value))

    def required_timezone_validator(self, value: Any) -> None:
        try:
            if value.tzinfo is None:
                reason = _("time zone required for value {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except (TypeError, AttributeError) as err:
            self.invalid_type_error(err, value)

    def prohibited_timezone_validator(self, value: Any) -> None:
        try:
            if value.tzinfo is not None:
                reason = _("time zone prohibited for value {!r}").format(self.value)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            self.invalid_type_error(err, value)


class XsdEnumerationFacets(XsdFacet, MutableSequence[ElementType]):
    """
    Sequence of XSD *enumeration* facets. Values are validates if match any of enumeration values.

    ..  <enumeration
          id = ID
          value = anySimpleType
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </enumeration>
    """
    base_type: BaseXsdType
    _ADMITTED_TAGS = nm.XSD_ENUMERATION,

    __slots__ = ('_elements', 'enumeration')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: 'XsdAtomicRestriction',
                 base_type: BaseXsdType) -> None:
        super().__init__(elem, schema, parent, base_type)
        self.validate = self.__call__

    def _parse(self) -> None:
        self._elements = [self.elem]
        self.enumeration = [self._parse_value(self.elem)]

    def _parse_value(self, elem: ElementType) -> Optional[AtomicValueType]:
        self.schema.validation_context.clear()
        try:
            value = self.base_type.text_decode(
                elem.attrib['value'], 'strict', self.schema.validation_context
            )
        except KeyError:
            pass  # pragma: no cover (already detected by meta-schema validation)
        except XMLSchemaValidationError as err:
            self.parse_error(err, elem)
        else:
            if self.base_type.name == nm.XSD_NOTATION_TYPE:
                assert isinstance(value, str)
                try:
                    notation_qname = self.schema.resolve_qname(value)
                except (KeyError, ValueError, RuntimeError) as err:
                    self.parse_error(err, elem)
                else:
                    if notation_qname not in self.maps.notations:
                        msg = _("value {!r} must match a notation declaration")
                        self.parse_error(msg.format(value), elem)
            return cast(AtomicValueType, value)
        return None

    @overload
    @abstractmethod
    def __getitem__(self, i: int) -> ElementType: ...

    @overload
    @abstractmethod
    def __getitem__(self, s: slice) -> MutableSequence[ElementType]: ...

    def __getitem__(self, i: Union[int, slice]) \
            -> Union[ElementType, MutableSequence[ElementType]]:
        return self._elements[i]

    def __setitem__(self, i: Union[int, slice], o: Any) -> None:
        self._elements[i] = o
        if isinstance(i, int):
            self.enumeration[i] = self._parse_value(o)
        else:
            self.enumeration[i] = [self._parse_value(e) for e in o]

    def __delitem__(self, i: Union[int, slice]) -> None:
        del self._elements[i]
        del self.enumeration[i]

    def __len__(self) -> int:
        return len(self._elements)

    def insert(self, i: int, elem: ElementType) -> None:
        self._elements.insert(i, elem)
        self.enumeration.insert(i, self._parse_value(elem))

    def __repr__(self) -> str:
        if len(self.enumeration) > 5:
            return '%s(%s)' % (
                self.__class__.__name__, '[%s, ...]' % ', '.join(map(repr, self.enumeration[:5]))
            )
        else:
            return '%s(%r)' % (self.__class__.__name__, self.enumeration)

    def __call__(self, value: Any) -> None:
        if value in self.enumeration:
            return

        try:
            if math.isnan(value):
                if any(math.isnan(x) for x in self.enumeration):  # type: ignore[arg-type]
                    return
            elif math.isinf(value):
                if any(math.isinf(x) and str(value) == str(x)  # type: ignore[arg-type]
                       for x in self.enumeration):  # pragma: no cover
                    return
        except TypeError:
            pass

        reason = _("value must be one of {!r}").format(self.enumeration)
        raise XMLSchemaValidationError(self, value, reason)

    def get_annotation(self, i: int) -> Optional[XsdAnnotation]:
        """
        Get the XSD annotation of the i-th enumeration facet.

        :param i: an integer index.
        :returns: an XsdAnnotation object or `None`.
        """
        return get_xsd_annotation(self._elements[i], self.schema, self)


class XsdPatternFacets(XsdFacet, MutableSequence[ElementType]):
    """
    Sequence of XSD *pattern* facets. Values are validates if match any of patterns.

    ..  <pattern
          id = ID
          value = string
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </pattern>
    """
    _ADMITTED_TAGS = nm.XSD_PATTERN,
    patterns: list[re.Pattern[str]]

    # XSD pattern translation options
    back_references = False
    lazy_quantifiers = False
    anchors = False

    __slots__ = ('_elements', 'patterns')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: 'XsdAtomicRestriction',
                 base_type: Optional[BaseXsdType]) -> None:
        super().__init__(elem, schema, parent, base_type)
        self.validate = self.__call__

    def _parse(self) -> None:
        self._elements = [self.elem]
        self.patterns = [self._parse_value(self.elem)]

    def _parse_value(self, elem: ElementType) -> re.Pattern[str]:
        try:
            python_pattern = translate_pattern(
                pattern=elem.attrib['value'],
                xsd_version=self.xsd_version,
                back_references=self.back_references,
                lazy_quantifiers=self.lazy_quantifiers,
                anchors=self.anchors,
            )
            return re.compile(python_pattern)
        except KeyError:
            return re.compile(r'^.*$')
        except (RegexError, re.error, XMLSchemaDecodeError) as err:
            self.parse_error(str(err), elem)
            return re.compile(r'^.*$')

    @overload
    @abstractmethod
    def __getitem__(self, i: int) -> ElementType: ...

    @overload
    @abstractmethod
    def __getitem__(self, s: slice) -> MutableSequence[ElementType]: ...

    def __getitem__(self, i: Union[int, slice]) \
            -> Union[ElementType, MutableSequence[ElementType]]:
        return self._elements[i]

    def __setitem__(self, i: Union[int, slice], o: Any) -> None:
        self._elements[i] = o
        if isinstance(i, int):
            self.patterns[i] = self._parse_value(o)
        else:
            self.patterns[i] = [self._parse_value(e) for e in o]

    def __delitem__(self, i: Union[int, slice]) -> None:
        del self._elements[i]
        del self.patterns[i]

    def __len__(self) -> int:
        return len(self._elements)

    def insert(self, i: int, elem: ElementType) -> None:
        self._elements.insert(i, elem)
        self.patterns.insert(i, self._parse_value(elem))

    def __repr__(self) -> str:
        s = repr(self.regexps)
        if len(s) < 70:
            return '%s(%s)' % (self.__class__.__name__, s)
        else:
            return '%s(%s...\'])' % (self.__class__.__name__, s[:70])

    def __call__(self, value: Any) -> None:
        try:
            if all(pattern.match(value) is None for pattern in self.patterns):
                reason = _("value doesn't match any pattern of {!r}").format(self.regexps)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            self.invalid_type_error(err, value)

    def re_match(self, text: str) -> Optional[re.Match[str]]:
        for pattern in self.patterns:
            if match := pattern.match(text):
                return match
        return None

    @property
    def regexps(self) -> list[str]:
        return [e.attrib.get('value', '') for e in self._elements]

    def get_annotation(self, i: int) -> Optional[XsdAnnotation]:
        """
        Get the XSD annotation of the i-th pattern facet.

        :param i: an integer index.
        :returns: an XsdAnnotation object or `None`.
        """
        return get_xsd_annotation(self._elements[i], self.schema, self)


class XsdAssertionFacet(XsdFacet):
    """
    XSD 1.1 *assertion* facet for simpleType definitions.

    ..  <assertion
          id = ID
          test = an XPath expression
          xpathDefaultNamespace = (anyURI | (##defaultNamespace | ##targetNamespace | ##local))
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </assertion>
    """
    _ADMITTED_TAGS = nm.XSD_ASSERTION,
    _root = ElementNode(elem=Element('root'))

    def __repr__(self) -> str:
        return '%s(test=%r)' % (self.__class__.__name__, self.path)

    def _parse(self) -> None:
        self.validate = self.__call__
        try:
            self.path = self.elem.attrib['test']
        except KeyError:
            self.parse_error(_("missing attribute 'test'"))
            self.path = 'true()'

        try:
            value = self.base_type.primitive_type.prefixed_name  # type: ignore[union-attr]
        except AttributeError:
            value = self.maps.any_simple_type.prefixed_name

        if 'xpathDefaultNamespace' in self.elem.attrib:
            self.xpath_default_namespace = parse_xpath_default_namespace(self)
        else:
            self.xpath_default_namespace = self.schema.xpath_default_namespace

        self.parser = self.maps.assertion_parser_class(
            namespaces=self.schema.namespaces,
            strict=False,
            variable_types={'value': value},
            default_namespace=self.xpath_default_namespace
        )

        try:
            self.token = self.parser.parse(self.path)
        except ElementPathError as err:
            self.parse_error(err)
            self.token = self.parser.parse('true()')

    def __call__(self, value: Any) -> None:
        context = XPathContext(self._root, variables={'value': value})
        try:
            if not self.token.evaluate(context):
                reason = _("value is not true with test path {!r}").format(self.path)
                raise XMLSchemaValidationError(self, value, reason)
        except TypeError as err:
            self.invalid_type_error(err, value)
        except ElementPathError as err:
            raise XMLSchemaValidationError(self, value, reason=str(err)) from None


XSD_10_FACETS_CLASSES: dict[str, type[XsdFacet]] = {
    nm.XSD_WHITE_SPACE: XsdWhiteSpaceFacet,
    nm.XSD_LENGTH: XsdLengthFacet,
    nm.XSD_MIN_LENGTH: XsdMinLengthFacet,
    nm.XSD_MAX_LENGTH: XsdMaxLengthFacet,
    nm.XSD_MIN_INCLUSIVE: XsdMinInclusiveFacet,
    nm.XSD_MIN_EXCLUSIVE: XsdMinExclusiveFacet,
    nm.XSD_MAX_INCLUSIVE: XsdMaxInclusiveFacet,
    nm.XSD_MAX_EXCLUSIVE: XsdMaxExclusiveFacet,
    nm.XSD_TOTAL_DIGITS: XsdTotalDigitsFacet,
    nm.XSD_FRACTION_DIGITS: XsdFractionDigitsFacet,
    nm.XSD_ENUMERATION: XsdEnumerationFacets,
    nm.XSD_PATTERN: XsdPatternFacets
}

XSD_11_FACETS_CLASSES: dict[str, type[XsdFacet]] = XSD_10_FACETS_CLASSES.copy()
XSD_11_FACETS_CLASSES.update({
    nm.XSD_ASSERTION: XsdAssertionFacet,
    nm.XSD_EXPLICIT_TIMEZONE: XsdExplicitTimezoneFacet
})

FACETS_CLASSES = {
    '1.0': XSD_10_FACETS_CLASSES,
    '1.1': XSD_11_FACETS_CLASSES,
}

XSD_10_FACETS = frozenset(XSD_10_FACETS_CLASSES)
XSD_11_FACETS = frozenset(XSD_11_FACETS_CLASSES)

XSD_10_LIST_FACETS = frozenset((nm.XSD_LENGTH, nm.XSD_MIN_LENGTH, nm.XSD_MAX_LENGTH,
                                nm.XSD_PATTERN, nm.XSD_ENUMERATION, nm.XSD_WHITE_SPACE))
XSD_11_LIST_FACETS = XSD_10_LIST_FACETS | frozenset((nm.XSD_ASSERTION,))

XSD_10_UNION_FACETS = frozenset((nm.XSD_PATTERN, nm.XSD_ENUMERATION))
XSD_11_UNION_FACETS = frozenset((nm.XSD_PATTERN, nm.XSD_ENUMERATION, nm.XSD_ASSERTION))
MULTIPLE_FACETS = XSD_11_UNION_FACETS

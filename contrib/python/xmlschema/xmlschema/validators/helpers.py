#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from decimal import Decimal
from math import isinf, isnan
from typing import Optional, SupportsInt, SupportsFloat, TYPE_CHECKING, Union
from xml.etree.ElementTree import Element
from elementpath import datatypes

import xmlschema.names as nm
from xmlschema.aliases import ElementType, SchemaType
from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.translation import gettext as _
from .exceptions import XMLSchemaValidationError

if TYPE_CHECKING:
    from xmlschema.validators import XsdAnnotation, XsdComponent  # noqa: F401

XSD_FINAL_ATTRIBUTE_VALUES = {'restriction', 'extension', 'list', 'union'}
XSD_BOOLEAN_MAP = {
    'false': False, '0': False,
    'true': True, '1': True
}


def get_xsd_annotation(elem: ElementType,
                       schema: SchemaType,
                       parent: Optional['XsdComponent'] = None) -> Optional['XsdAnnotation']:
    """
    Returns the XSD annotation from the 1st child element of the provided element,
    `None` if it doesn't exist.
    """
    for child in elem:
        if child.tag == nm.XSD_ANNOTATION:
            return schema.builders.annotation_class(
                child, schema, parent, parent_elem=elem
            )
        elif not callable(child.tag):
            return None
    else:
        return None


def get_schema_annotations(schema: SchemaType) -> list['XsdAnnotation']:
    annotations = []
    annotation_class = schema.builders.annotation_class

    for elem in schema.source.root:
        if elem.tag == nm.XSD_ANNOTATION:
            annotations.append(annotation_class(elem, schema))
        elif elem.tag in nm.SCHEMA_DECLARATION_TAGS or elem.tag == nm.XSD_DEFAULT_OPEN_CONTENT:
            annotation = get_xsd_annotation(elem, schema)
            if annotation is not None:
                annotations.append(annotation)

    return annotations


def parse_xsd_derivation(elem: Element,
                         name: str,
                         choices: Union[None, set[str], tuple[str, ...]] = None,
                         validator: Union[None, SchemaType, 'XsdComponent'] = None) -> str:
    """
    Get a derivation attribute (maybe 'block', 'blockDefault', 'final' or 'finalDefault')
    checking the items with the values arguments. Returns a string.

    :param elem: the Element instance.
    :param name: the attribute name.
    :param choices: a set of admitted values when the attribute value is not '#all'.
    :param validator: optional schema or a component (element or type) to report \
    a parse error instead of raising a `ValueError`.
    """
    try:
        value = elem.attrib[name]
    except KeyError:
        return ''

    if choices is None:
        choices = XSD_FINAL_ATTRIBUTE_VALUES

    items = value.split()
    if len(items) == 1 and items[0] == '#all':
        return ' '.join(choices)
    elif not all(s in choices for s in items):
        msg = _("wrong value {!r} for attribute {!r}").format(value, name)
        if validator is None:
            raise ValueError(msg)
        validator.parse_error(msg)
        return ''
    return value


def parse_xpath_default_namespace(validator: Union[SchemaType, 'XsdComponent']) -> str:
    """
    Parse XSD 1.1 xpathDefaultNamespace attribute for schema, alternative, assert, assertion
    and selector declarations, checking if the value is conforming to the specification. In
    case the attribute is missing or for wrong attribute values defaults to ''.
    """
    try:
        value = validator.elem.attrib['xpathDefaultNamespace']
    except KeyError:
        return ''

    value = value.strip()
    if value == '##local':
        return ''
    elif value == '##defaultNamespace':
        return validator.default_namespace
    elif value == '##targetNamespace':
        return validator.target_namespace
    elif len(value.split()) == 1:
        return value
    else:
        admitted_values = ('##defaultNamespace', '##targetNamespace', '##local')
        msg = _("wrong value {0!r} for 'xpathDefaultNamespace' "
                "attribute, can be (anyURI | {1}).")
        validator.parse_error(msg.format(value, ' | '.join(admitted_values)))
        return ''


def parse_target_namespace(validator: Union[SchemaType, 'XsdComponent']) -> str:
    """
    XSD 1.1 targetNamespace attribute in schema, elements and attributes declarations.
    """
    try:
        target_namespace = validator.elem.attrib['targetNamespace'].strip()
    except KeyError:
        return ''

    if target_namespace == nm.XMLNS_NAMESPACE:
        # https://www.w3.org/TR/xmlschema11-1/#sec-nss-special
        msg = _(f"The namespace {nm.XMLNS_NAMESPACE} cannot be used as 'targetNamespace'")
        raise XMLSchemaValueError(msg)
    elif not target_namespace and validator.elem.tag == nm.XSD_SCHEMA:
        # https://www.w3.org/TR/2004/REC-xmlschema-1-20041028/structures.html#element-schema
        msg = _("the attribute 'targetNamespace' cannot be an empty string")
        validator.parse_error(msg)

    return target_namespace


#
# XSD built-in types validator functions

def decimal_validator(value: Union[Decimal, int, float, str]) -> None:
    try:
        if not isinstance(value, (Decimal, float)):
            datatypes.DecimalProxy.validate(value)
        elif isinf(value) or isnan(value):
            raise ValueError()
    except (ValueError, TypeError):
        raise XMLSchemaValidationError(decimal_validator, value,
                                       _("value is not a valid xs:decimal")) from None


def qname_validator(value: str) -> None:
    if datatypes.QName.pattern.match(value) is None:
        raise XMLSchemaValidationError(qname_validator, value,
                                       _("value is not an xs:QName"))


def byte_validator(value: int) -> None:
    if not (-2**7 <= value < 2 ** 7):
        raise XMLSchemaValidationError(int_validator, value,
                                       _("value must be {:s}").format("-128 <= x < 128"))


def short_validator(value: int) -> None:
    if not (-2**15 <= value < 2 ** 15):
        raise XMLSchemaValidationError(short_validator, value,
                                       _("value must be {:s}").format("-2^15 <= x < 2^15"))


def int_validator(value: int) -> None:
    if not (-2**31 <= value < 2 ** 31):
        raise XMLSchemaValidationError(int_validator, value,
                                       _("value must be {:s}").format("-2^31 <= x < 2^31"))


def long_validator(value: int) -> None:
    if not (-2**63 <= value < 2 ** 63):
        raise XMLSchemaValidationError(long_validator, value,
                                       _("value must be {:s}").format("-2^63 <= x < 2^63"))


def unsigned_byte_validator(value: int) -> None:
    if not (0 <= value < 2 ** 8):
        raise XMLSchemaValidationError(unsigned_byte_validator, value,
                                       _("value must be {:s}").format("0 <= x < 256"))


def unsigned_short_validator(value: int) -> None:
    if not (0 <= value < 2 ** 16):
        raise XMLSchemaValidationError(unsigned_short_validator, value,
                                       _("value must be {:s}").format("0 <= x < 2^16"))


def unsigned_int_validator(value: int) -> None:
    if not (0 <= value < 2 ** 32):
        raise XMLSchemaValidationError(unsigned_int_validator, value,
                                       _("value must be {:s}").format("0 <= x < 2^32"))


def unsigned_long_validator(value: int) -> None:
    if not (0 <= value < 2 ** 64):
        raise XMLSchemaValidationError(unsigned_long_validator, value,
                                       _("value must be {:s}").format("0 <= x < 2^64"))


def negative_int_validator(value: int) -> None:
    if value >= 0:
        raise XMLSchemaValidationError(negative_int_validator, value,
                                       _("value must be negative"))


def positive_int_validator(value: int) -> None:
    if value <= 0:
        raise XMLSchemaValidationError(positive_int_validator, value,
                                       _("value must be positive"))


def non_positive_int_validator(value: int) -> None:
    if value > 0:
        raise XMLSchemaValidationError(non_positive_int_validator, value,
                                       _("value must be non positive"))


def non_negative_int_validator(value: int) -> None:
    if value < 0:
        raise XMLSchemaValidationError(non_negative_int_validator, value,
                                       _("value must be non negative"))


def hex_binary_validator(value: Union[str, datatypes.HexBinary]) -> None:
    if not isinstance(value, datatypes.HexBinary) and \
            datatypes.HexBinary.pattern.match(value) is None:
        raise XMLSchemaValidationError(hex_binary_validator, value,
                                       _("not an hexadecimal number"))


def base64_binary_validator(value: Union[str, datatypes.Base64Binary]) -> None:
    if isinstance(value, datatypes.Base64Binary):
        return
    value = value.replace(' ', '')
    if not value:
        return

    match = datatypes.Base64Binary.pattern.match(value)
    if match is None or match.group(0) != value:
        raise XMLSchemaValidationError(base64_binary_validator, value,
                                       _("not a base64 encoding"))


def error_type_validator(value: object) -> None:
    raise XMLSchemaValidationError(error_type_validator, value,
                                   _("no value is allowed for xs:error type"))


#
# XSD builtin decoding functions

def boolean_to_python(value: str) -> bool:
    try:
        return XSD_BOOLEAN_MAP[value]
    except KeyError:
        raise XMLSchemaValueError(_('{!r} is not a boolean value').format(value))


def python_to_boolean(value: object) -> str:
    if isinstance(value, str):
        if value in XSD_BOOLEAN_MAP:
            return value
        raise XMLSchemaValueError(_('{!r} is not a boolean value').format(value))
    return str(value).lower()


def python_to_float(value: Union[SupportsFloat, str]) -> str:
    if isinstance(value, str):
        if value in ('NaN', 'INF', '-INF'):
            return value
        return str(float(value))
    elif isnan(value):
        return "NaN"
    if value == float("inf"):
        return "INF"
    if value == float("-inf"):
        return "-INF"
    return str(value)


def python_to_int(value: Union[SupportsInt, str]) -> str:
    return str(int(value))

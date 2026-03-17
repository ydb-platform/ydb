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
This module contains definitions and a factory function for XSD builtin datatypes.

Only atomic builtins are created, the list builtins types ('NMTOKENS', 'ENTITIES', 'IDREFS')
are created using the XSD 1.0 meta-schema or with and additional base schema for XSD 1.1.
"""
from decimal import Decimal
from elementpath import datatypes
from typing import Any
from xml.etree.ElementTree import Element

import xmlschema.names as nm

from .helpers import decimal_validator, qname_validator, byte_validator, \
    short_validator, int_validator, long_validator, unsigned_byte_validator, \
    unsigned_short_validator, unsigned_int_validator, unsigned_long_validator, \
    negative_int_validator, positive_int_validator, non_positive_int_validator, \
    non_negative_int_validator, hex_binary_validator, base64_binary_validator, \
    error_type_validator, boolean_to_python, python_to_boolean, python_to_float, \
    python_to_int

#
# Admitted facets sets for XSD atomic types
STRING_FACETS = {
    nm.XSD_LENGTH, nm.XSD_MIN_LENGTH, nm.XSD_MAX_LENGTH, nm.XSD_PATTERN,
    nm.XSD_ENUMERATION, nm.XSD_WHITE_SPACE, nm.XSD_ASSERTION
}

BOOLEAN_FACETS = {nm.XSD_PATTERN, nm.XSD_WHITE_SPACE, nm.XSD_ASSERTION}

FLOAT_FACETS = {
    nm.XSD_PATTERN, nm.XSD_ENUMERATION, nm.XSD_WHITE_SPACE, nm.XSD_MAX_INCLUSIVE,
    nm.XSD_MAX_EXCLUSIVE, nm.XSD_MIN_INCLUSIVE, nm.XSD_MIN_EXCLUSIVE, nm.XSD_ASSERTION
}

DECIMAL_FACETS = {
    nm.XSD_TOTAL_DIGITS, nm.XSD_FRACTION_DIGITS, nm.XSD_PATTERN, nm.XSD_ENUMERATION,
    nm.XSD_WHITE_SPACE, nm.XSD_MAX_INCLUSIVE, nm.XSD_MAX_EXCLUSIVE, nm.XSD_MIN_INCLUSIVE,
    nm.XSD_MIN_EXCLUSIVE, nm.XSD_ASSERTION
}

DATETIME_FACETS = {
    nm.XSD_PATTERN, nm.XSD_ENUMERATION, nm.XSD_WHITE_SPACE,
    nm.XSD_MAX_INCLUSIVE, nm.XSD_MAX_EXCLUSIVE, nm.XSD_MIN_INCLUSIVE,
    nm.XSD_MIN_EXCLUSIVE, nm.XSD_ASSERTION, nm.XSD_EXPLICIT_TIMEZONE
}

#
# Element facets instances for builtin types.
PRESERVE_WHITE_SPACE_ELEMENT = Element(nm.XSD_WHITE_SPACE, value='preserve')
COLLAPSE_WHITE_SPACE_ELEMENT = Element(nm.XSD_WHITE_SPACE, value='collapse')
REPLACE_WHITE_SPACE_ELEMENT = Element(nm.XSD_WHITE_SPACE, value='replace')
XSD10_FLOAT_PATTERN_ELEMENT = Element(
    nm.XSD_PATTERN,
    value=r"(\+|-)?([0-9]+(\.[0-9]*)?|\.[0-9]+)([Ee](\+|-)?[0-9]+)?|INF|-INF|NaN"
)
XSD11_FLOAT_PATTERN_ELEMENT = Element(
    nm.XSD_PATTERN,
    value=r"(\+|-)?([0-9]+(\.[0-9]*)?|\.[0-9]+)([Ee](\+|-)?[0-9]+)?|(\+|-)?INF|NaN"
)


XSD_COMMON_BUILTIN_TYPES: tuple[dict[str, Any], ...] = (
    # ***********************
    # *** Primitive types ***
    # ***********************

    # --- String Types ---
    {
        'name': nm.XSD_STRING,
        'datatype': datatypes.StringProxy,
        'python_type': str,
        'admitted_facets': STRING_FACETS,
        'facets': [PRESERVE_WHITE_SPACE_ELEMENT],
    },  # character string

    # --- Numerical Types ---
    {
        'name': nm.XSD_DECIMAL,
        'datatype': datatypes.DecimalProxy,
        'python_type': (Decimal, int, float),
        'admitted_facets': DECIMAL_FACETS,
        'to_python': datatypes.DecimalProxy,
        'facets': [decimal_validator, COLLAPSE_WHITE_SPACE_ELEMENT],
    },  # decimal number

    # --- Dates and Times (not year related) ---
    {
        'name': nm.XSD_GDAY,
        'datatype': datatypes.GregorianDay,
        'python_type': datatypes.GregorianDay,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.GregorianDay.fromstring,
    },  # DD
    {
        'name': nm.XSD_GMONTH,
        'datatype': datatypes.GregorianMonth,
        'python_type': datatypes.GregorianMonth,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.GregorianMonth.fromstring,
    },  # MM
    {
        'name': nm.XSD_GMONTH_DAY,
        'datatype': datatypes.GregorianMonthDay,
        'python_type': datatypes.GregorianMonthDay,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.GregorianMonthDay.fromstring,
    },  # MM-DD
    {
        'name': nm.XSD_TIME,
        'datatype': datatypes.Time,
        'python_type': datatypes.Time,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.Time.fromstring,
    },  # hh:mm:ss
    {
        'name': nm.XSD_DURATION,
        'datatype': datatypes.Duration,
        'python_type': datatypes.Duration,
        'admitted_facets': FLOAT_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.Duration.fromstring,
    },  # PnYnMnDTnHnMnS

    # Other primitive types
    {
        'name': nm.XSD_QNAME,
        'datatype': datatypes.QName,
        'python_type': (str, datatypes.QName),
        'admitted_facets': STRING_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT, qname_validator],
    },  # prf:name (the prefix needs to be qualified with an in-scope namespace)
    {
        'name': nm.XSD_NOTATION_TYPE,
        'datatype': datatypes.Notation,
        'python_type': (str, datatypes.Notation),
        'admitted_facets': STRING_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
    },  # type for NOTATION attributes: QNames of xs:notation declarations as value space.
    {
        'name': nm.XSD_ANY_URI,
        'datatype': datatypes.AnyURI,
        'python_type': (str, datatypes.AnyURI),
        'admitted_facets': STRING_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
    },  # absolute or relative uri (RFC 2396)
    {
        'name': nm.XSD_BOOLEAN,
        'datatype': datatypes.BooleanProxy,
        'python_type': bool,
        'admitted_facets': BOOLEAN_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': boolean_to_python,
        'from_python': python_to_boolean,
    },  # true/false or 1/0
    {
        'name': nm.XSD_BASE64_BINARY,
        'datatype': datatypes.Base64Binary,
        'python_type': (datatypes.Base64Binary, str, bytes),
        'admitted_facets': STRING_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT, base64_binary_validator],
    },  # base64 encoded binary value
    {
        'name': nm.XSD_HEX_BINARY,
        'datatype': datatypes.HexBinary,
        'python_type': (datatypes.HexBinary, str, bytes),
        'admitted_facets': STRING_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT, hex_binary_validator],
    },   # hexadecimal encoded binary value

    # *********************
    # *** Derived types ***
    # *********************

    # --- String Types ---
    {
        'name': nm.XSD_NORMALIZED_STRING,
        'datatype': datatypes.NormalizedString,
        'python_type': str,
        'base_type': nm.XSD_STRING,
        'facets': [REPLACE_WHITE_SPACE_ELEMENT],
    },  # line breaks are normalized
    {
        'name': nm.XSD_TOKEN,
        'datatype': datatypes.XsdToken,
        'python_type': str,
        'base_type': nm.XSD_NORMALIZED_STRING,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
    },  # whitespaces are normalized
    {
        'name': nm.XSD_LANGUAGE,
        'datatype': datatypes.Language,
        'python_type': str,
        'base_type': nm.XSD_TOKEN,
        'facets': [Element(nm.XSD_PATTERN, value=r"[a-zA-Z]{1,8}(-[a-zA-Z0-9]{1,8})*")]
    },  # language codes
    {
        'name': nm.XSD_NAME,
        'datatype': datatypes.Name,
        'python_type': str,
        'base_type': nm.XSD_TOKEN,
        'facets': [Element(nm.XSD_PATTERN, value=r"\i\c*")]
    },  # not starting with a digit
    {
        'name': nm.XSD_NCNAME,
        'datatype': datatypes.NCName,
        'python_type': str,
        'base_type': nm.XSD_NAME,
        'facets': [Element(nm.XSD_PATTERN, value=r"[\i-[:]][\c-[:]]*")]
    },  # cannot contain colons
    {
        'name': nm.XSD_ID,
        'datatype': datatypes.Id,
        'python_type': str,
        'base_type': nm.XSD_NCNAME
    },  # unique identification in document (attribute only)
    {
        'name': nm.XSD_IDREF,
        'datatype': datatypes.Idref,
        'python_type': str,
        'base_type': nm.XSD_NCNAME
    },  # reference to ID field in document (attribute only)
    {
        'name': nm.XSD_ENTITY,
        'datatype': datatypes.Entity,
        'python_type': str,
        'base_type': nm.XSD_NCNAME
    },  # reference to entity (attribute only)
    {
        'name': nm.XSD_NMTOKEN,
        'datatype': datatypes.NMToken,
        'python_type': str,
        'base_type': nm.XSD_TOKEN,
        'facets': [Element(nm.XSD_PATTERN, value=r"\c+")]
    },  # should not contain whitespace (attribute only)

    # --- Numerical derived types ---
    {
        'name': nm.XSD_INTEGER,
        'datatype': datatypes.Integer,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_DECIMAL
    },  # any integer value
    {
        'name': nm.XSD_LONG,
        'datatype': datatypes.Long,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_INTEGER,
        'facets': [long_validator,
                   Element(nm.XSD_MIN_INCLUSIVE, value='-9223372036854775808'),
                   Element(nm.XSD_MAX_INCLUSIVE, value='9223372036854775807')]
    },  # signed 128 bit value
    {
        'name': nm.XSD_INT,
        'datatype': datatypes.Int,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_LONG,
        'facets': [int_validator,
                   Element(nm.XSD_MIN_INCLUSIVE, value='-2147483648'),
                   Element(nm.XSD_MAX_INCLUSIVE, value='2147483647')]
    },  # signed 64 bit value
    {
        'name': nm.XSD_SHORT,
        'datatype': datatypes.Short,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_INT,
        'facets': [short_validator,
                   Element(nm.XSD_MIN_INCLUSIVE, value='-32768'),
                   Element(nm.XSD_MAX_INCLUSIVE, value='32767')]
    },  # signed 32 bit value
    {
        'name': nm.XSD_BYTE,
        'datatype': datatypes.Byte,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_SHORT,
        'facets': [byte_validator,
                   Element(nm.XSD_MIN_INCLUSIVE, value='-128'),
                   Element(nm.XSD_MAX_INCLUSIVE, value='127')]
    },  # signed 8 bit value
    {
        'name': nm.XSD_NON_NEGATIVE_INTEGER,
        'datatype': datatypes.NonNegativeInteger,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_INTEGER,
        'facets': [non_negative_int_validator, Element(nm.XSD_MIN_INCLUSIVE, value='0')]
    },  # only zero and more value allowed [>= 0]
    {
        'name': nm.XSD_POSITIVE_INTEGER,
        'datatype': datatypes.PositiveInteger,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_NON_NEGATIVE_INTEGER,
        'facets': [positive_int_validator, Element(nm.XSD_MIN_INCLUSIVE, value='1')]
    },  # only positive value allowed [> 0]
    {
        'name': nm.XSD_UNSIGNED_LONG,
        'datatype': datatypes.UnsignedLong,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_NON_NEGATIVE_INTEGER,
        'facets': [unsigned_long_validator,
                   Element(nm.XSD_MAX_INCLUSIVE, value='18446744073709551615')]
    },  # unsigned 128 bit value
    {
        'name': nm.XSD_UNSIGNED_INT,
        'datatype': datatypes.UnsignedInt,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_UNSIGNED_LONG,
        'facets': [unsigned_int_validator, Element(nm.XSD_MAX_INCLUSIVE, value='4294967295')]
    },  # unsigned 64 bit value
    {
        'name': nm.XSD_UNSIGNED_SHORT,
        'datatype': datatypes.UnsignedShort,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_UNSIGNED_INT,
        'facets': [unsigned_short_validator, Element(nm.XSD_MAX_INCLUSIVE, value='65535')]
    },  # unsigned 32 bit value
    {
        'name': nm.XSD_UNSIGNED_BYTE,
        'datatype': datatypes.UnsignedByte,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_UNSIGNED_SHORT,
        'facets': [unsigned_byte_validator, Element(nm.XSD_MAX_INCLUSIVE, value='255')]
    },  # unsigned 8 bit value
    {
        'name': nm.XSD_NON_POSITIVE_INTEGER,
        'datatype': datatypes.NonPositiveInteger,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_INTEGER,
        'facets': [non_positive_int_validator, Element(nm.XSD_MAX_INCLUSIVE, value='0')]
    },  # only zero and smaller value allowed [<= 0]
    {
        'name': nm.XSD_NEGATIVE_INTEGER,
        'datatype': datatypes.NegativeInteger,
        'python_type': int,
        'from_python': python_to_int,
        'base_type': nm.XSD_NON_POSITIVE_INTEGER,
        'facets': [negative_int_validator, Element(nm.XSD_MAX_INCLUSIVE, value='-1')]
    },  # only negative value allowed [< 0]
)

XSD_10_BUILTIN_TYPES: tuple[dict[str, Any], ...] = XSD_COMMON_BUILTIN_TYPES + (
    {
        'name': nm.XSD_DOUBLE,
        'datatype': datatypes.DoubleProxy10,
        'python_type': float,
        'admitted_facets': FLOAT_FACETS,
        'facets': [XSD10_FLOAT_PATTERN_ELEMENT, COLLAPSE_WHITE_SPACE_ELEMENT],
        'from_python': python_to_float,
    },  # 64 bit floating point
    {
        'name': nm.XSD_FLOAT,
        'datatype': datatypes.Float10,
        'python_type': float,
        'admitted_facets': FLOAT_FACETS,
        'facets': [XSD10_FLOAT_PATTERN_ELEMENT, COLLAPSE_WHITE_SPACE_ELEMENT],
        'from_python': python_to_float,
    },  # 32 bit floating point

    # --- Year related primitive types (year 0 not allowed) ---
    {
        'name': nm.XSD_DATETIME,
        'datatype': datatypes.DateTime10,
        'python_type': datatypes.DateTime10,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.DateTime10.fromstring,
    },  # [-][Y*]YYYY-MM-DD[Thh:mm:ss]
    {
        'name': nm.XSD_DATE,
        'datatype': datatypes.Date10,
        'python_type': datatypes.Date10,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.Date10.fromstring,
    },  # [-][Y*]YYYY-MM-DD
    {
        'name': nm.XSD_GYEAR,
        'datatype': datatypes.GregorianYear10,
        'python_type': datatypes.GregorianYear10,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.GregorianYear10.fromstring,
    },  # [-][Y*]YYYY
    {
        'name': nm.XSD_GYEAR_MONTH,
        'datatype': datatypes.GregorianYearMonth10,
        'python_type': datatypes.GregorianYearMonth10,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.GregorianYearMonth10.fromstring,
    },  # [-][Y*]YYYY-MM
)

XSD_11_BUILTIN_TYPES: tuple[dict[str, Any], ...] = XSD_COMMON_BUILTIN_TYPES + (
    {
        'name': nm.XSD_DOUBLE,
        'datatype': datatypes.DoubleProxy,
        'python_type': float,
        'admitted_facets': FLOAT_FACETS,
        'facets': [XSD11_FLOAT_PATTERN_ELEMENT, COLLAPSE_WHITE_SPACE_ELEMENT],
        'from_python': python_to_float,
    },  # 64 bit floating point
    {
        'name': nm.XSD_FLOAT,
        'datatype': datatypes.Float,
        'python_type': float,
        'admitted_facets': FLOAT_FACETS,
        'facets': [XSD11_FLOAT_PATTERN_ELEMENT, COLLAPSE_WHITE_SPACE_ELEMENT],
        'from_python': python_to_float,
    },  # 32 bit floating point

    # --- Year related primitive types (year 0 allowed and mapped to 1 BCE) ---
    {
        'name': nm.XSD_DATETIME,
        'datatype': datatypes.DateTime,
        'python_type': datatypes.DateTime,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.DateTime.fromstring,
    },  # [-][Y*]YYYY-MM-DD[Thh:mm:ss]
    {
        'name': nm.XSD_DATE,
        'datatype': datatypes.Date,
        'python_type': datatypes.Date,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.Date.fromstring,
    },  # [-][Y*]YYYY-MM-DD
    {
        'name': nm.XSD_GYEAR,
        'datatype': datatypes.GregorianYear,
        'python_type': datatypes.GregorianYear,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.GregorianYear.fromstring,
    },  # [-][Y*]YYYY
    {
        'name': nm.XSD_GYEAR_MONTH,
        'datatype': datatypes.GregorianYearMonth,
        'python_type': datatypes.GregorianYearMonth,
        'admitted_facets': DATETIME_FACETS,
        'facets': [COLLAPSE_WHITE_SPACE_ELEMENT],
        'to_python': datatypes.GregorianYearMonth.fromstring,
    },  # [-][Y*]YYYY-MM
    # --- Datetime derived types (XSD 1.1) ---
    {
        'name': nm.XSD_DATE_TIME_STAMP,
        'datatype': datatypes.DateTimeStamp,
        'python_type': datatypes.DateTimeStamp,
        'base_type': nm.XSD_DATETIME,
        'to_python': datatypes.DateTime.fromstring,
        'facets': [Element(nm.XSD_EXPLICIT_TIMEZONE, value='required')],
    },  # [-][Y*]YYYY-MM-DD[Thh:mm:ss] with required timezone
    {
        'name': nm.XSD_DAY_TIME_DURATION,
        'datatype': datatypes.DayTimeDuration,
        'python_type': datatypes.DayTimeDuration,
        'base_type': nm.XSD_DURATION,
        'to_python': datatypes.DayTimeDuration.fromstring,
    },  # PnYnMnDTnHnMnS with month a year equal to 0
    {
        'name': nm.XSD_YEAR_MONTH_DURATION,
        'datatype': datatypes.YearMonthDuration,
        'python_type': datatypes.YearMonthDuration,
        'base_type': nm.XSD_DURATION,
        'to_python': datatypes.YearMonthDuration.fromstring,
    },  # PnYnMnDTnHnMnS with day and time equals to 0
    # --- xs:error primitive type (XSD 1.1) ---
    {
        'name': nm.XSD_ERROR,
        'datatype': type(None),
        'python_type': type(None),
        'admitted_facets': (),
        'facets': [error_type_validator],
    },  # xs:error has no value space and no lexical space
)

BUILTIN_TYPES = {
    '1.0': XSD_10_BUILTIN_TYPES,
    '1.1': XSD_11_BUILTIN_TYPES
}

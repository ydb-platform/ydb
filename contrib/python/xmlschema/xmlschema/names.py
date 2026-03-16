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
This module contains namespaces and name definitions for W3C core standards.
"""

###
# Namespace URIs
XSD_NAMESPACE = 'http://www.w3.org/2001/XMLSchema'
"URI of the XML Schema Definition namespace (xs|xsd)"

XSI_NAMESPACE = 'http://www.w3.org/2001/XMLSchema-instance'
"URI of the XML Schema Instance namespace (xsi)"

XML_NAMESPACE = 'http://www.w3.org/XML/1998/namespace'
"URI of the XML namespace (xml)"

XMLNS_NAMESPACE = 'http://www.w3.org/2000/xmlns/'
"""
Special namespace, reserved for making xmlns declarations with the use of extended
names. Can't be used as a target namespace for a schema or for its components.
"""

XHTML_NAMESPACE = 'http://www.w3.org/1999/xhtml'
XHTML_DATATYPES_NAMESPACE = 'http://www.w3.org/1999/xhtml/datatypes/'
"URIs of the Extensible Hypertext Markup Language namespace (html)"

XLINK_NAMESPACE = 'http://www.w3.org/1999/xlink'
"URI of the XML Linking Language (XLink)"

XSLT_NAMESPACE = "http://www.w3.org/1999/XSL/Transform"
"URI of the XSL Transformations namespace (xslt)"

HFP_NAMESPACE = 'http://www.w3.org/2001/XMLSchema-hasFacetAndProperty'
"URI of the XML Schema has Facet and Property namespace (hfp)"

VC_NAMESPACE = 'http://www.w3.org/2007/XMLSchema-versioning'
"URI of the XML Schema Versioning namespace (vc)"

###
# Namespaces for WSDL documents
WSDL_NAMESPACE = 'http://schemas.xmlsoap.org/wsdl/'
SOAP_NAMESPACE = 'http://schemas.xmlsoap.org/wsdl/soap/'
SOAP_ENVELOPE_NAMESPACE = 'http://schemas.xmlsoap.org/soap/envelope/'
SOAP_ENCODING_NAMESPACE = 'http://schemas.xmlsoap.org/soap/encoding/'

###
# Namespaces for XML Signature Syntax and Processing
DSIG_NAMESPACE = 'http://www.w3.org/2000/09/xmldsig#'
DSIG11_NAMESPACE = 'http://www.w3.org/2009/xmldsig11#'

###
# Namespaces for XML Encryption Syntax and Processing
XENC_NAMESPACE = 'http://www.w3.org/2001/04/xmlenc#'
XENC11_NAMESPACE = 'http://www.w3.org/2009/xmlenc11#'


###
# Elements and attributes names

_VC_TEMPLATE = '{http://www.w3.org/2007/XMLSchema-versioning}%s'
_XML_TEMPLATE = '{http://www.w3.org/XML/1998/namespace}%s'
_XSD_TEMPLATE = '{http://www.w3.org/2001/XMLSchema}%s'
_XSI_TEMPLATE = '{http://www.w3.org/2001/XMLSchema-instance}%s'


#
# Version Control attributes (XSD 1.1)
VC_MIN_VERSION = _VC_TEMPLATE % 'minVersion'
VC_MAX_VERSION = _VC_TEMPLATE % 'maxVersion'
VC_TYPE_AVAILABLE = _VC_TEMPLATE % 'typeAvailable'
VC_TYPE_UNAVAILABLE = _VC_TEMPLATE % 'typeUnavailable'
VC_FACET_AVAILABLE = _VC_TEMPLATE % 'facetAvailable'
VC_FACET_UNAVAILABLE = _VC_TEMPLATE % 'facetUnavailable'


#
# XML attributes
XML_LANG = _XML_TEMPLATE % 'lang'
XML_SPACE = _XML_TEMPLATE % 'space'
XML_BASE = _XML_TEMPLATE % 'base'
XML_ID = _XML_TEMPLATE % 'id'
XML_SPECIAL_ATTRS = _XML_TEMPLATE % 'specialAttrs'


#
# XML Schema Instance attributes
XSI_NIL = _XSI_TEMPLATE % 'nil'
XSI_TYPE = _XSI_TEMPLATE % 'type'
XSI_SCHEMA_LOCATION = _XSI_TEMPLATE % 'schemaLocation'
XSI_NONS_SCHEMA_LOCATION = _XSI_TEMPLATE % 'noNamespaceSchemaLocation'


#
# XML Schema fully qualified names
XSD_SCHEMA = _XSD_TEMPLATE % 'schema'

# Annotations
XSD_ANNOTATION = _XSD_TEMPLATE % 'annotation'
XSD_APPINFO = _XSD_TEMPLATE % 'appinfo'
XSD_DOCUMENTATION = _XSD_TEMPLATE % 'documentation'

# Composing schemas
XSD_INCLUDE = _XSD_TEMPLATE % 'include'
XSD_IMPORT = _XSD_TEMPLATE % 'import'
XSD_REDEFINE = _XSD_TEMPLATE % 'redefine'
XSD_OVERRIDE = _XSD_TEMPLATE % 'override'

# Structures
XSD_SIMPLE_TYPE = _XSD_TEMPLATE % 'simpleType'
XSD_COMPLEX_TYPE = _XSD_TEMPLATE % 'complexType'
XSD_ATTRIBUTE = _XSD_TEMPLATE % 'attribute'
XSD_ELEMENT = _XSD_TEMPLATE % 'element'
XSD_NOTATION = _XSD_TEMPLATE % 'notation'

# Grouping
XSD_GROUP = _XSD_TEMPLATE % 'group'
XSD_ATTRIBUTE_GROUP = _XSD_TEMPLATE % 'attributeGroup'

# simpleType declaration elements
XSD_RESTRICTION = _XSD_TEMPLATE % 'restriction'
XSD_LIST = _XSD_TEMPLATE % 'list'
XSD_UNION = _XSD_TEMPLATE % 'union'

# complexType content
XSD_EXTENSION = _XSD_TEMPLATE % 'extension'
XSD_SEQUENCE = _XSD_TEMPLATE % 'sequence'
XSD_CHOICE = _XSD_TEMPLATE % 'choice'
XSD_ALL = _XSD_TEMPLATE % 'all'
XSD_ANY = _XSD_TEMPLATE % 'any'
XSD_SIMPLE_CONTENT = _XSD_TEMPLATE % 'simpleContent'
XSD_COMPLEX_CONTENT = _XSD_TEMPLATE % 'complexContent'
XSD_ANY_ATTRIBUTE = _XSD_TEMPLATE % 'anyAttribute'

#
#  Facets (lexical, pre-lexical and value-based facets)
XSD_ENUMERATION = _XSD_TEMPLATE % 'enumeration'
XSD_LENGTH = _XSD_TEMPLATE % 'length'
XSD_MIN_LENGTH = _XSD_TEMPLATE % 'minLength'
XSD_MAX_LENGTH = _XSD_TEMPLATE % 'maxLength'
XSD_PATTERN = _XSD_TEMPLATE % 'pattern'              # lexical facet
XSD_WHITE_SPACE = _XSD_TEMPLATE % 'whiteSpace'       # pre-lexical facet
XSD_MAX_INCLUSIVE = _XSD_TEMPLATE % 'maxInclusive'
XSD_MAX_EXCLUSIVE = _XSD_TEMPLATE % 'maxExclusive'
XSD_MIN_INCLUSIVE = _XSD_TEMPLATE % 'minInclusive'
XSD_MIN_EXCLUSIVE = _XSD_TEMPLATE % 'minExclusive'
XSD_TOTAL_DIGITS = _XSD_TEMPLATE % 'totalDigits'
XSD_FRACTION_DIGITS = _XSD_TEMPLATE % 'fractionDigits'

# XSD 1.1 elements
XSD_OPEN_CONTENT = _XSD_TEMPLATE % 'openContent'                 # open content model
XSD_DEFAULT_OPEN_CONTENT = _XSD_TEMPLATE % 'defaultOpenContent'  # default open content model
XSD_ALTERNATIVE = _XSD_TEMPLATE % 'alternative'                  # conditional type assignment
XSD_ASSERT = _XSD_TEMPLATE % 'assert'                            # complex type assertions
XSD_ASSERTION = _XSD_TEMPLATE % 'assertion'                      # facets
XSD_EXPLICIT_TIMEZONE = _XSD_TEMPLATE % 'explicitTimezone'

# Identity constraints
XSD_UNIQUE = _XSD_TEMPLATE % 'unique'
XSD_KEY = _XSD_TEMPLATE % 'key'
XSD_KEYREF = _XSD_TEMPLATE % 'keyref'
XSD_SELECTOR = _XSD_TEMPLATE % 'selector'
XSD_FIELD = _XSD_TEMPLATE % 'field'

#
# XSD Builtin Types

# Special XSD built-in types.
XSD_ANY_TYPE = _XSD_TEMPLATE % 'anyType'
XSD_ANY_SIMPLE_TYPE = _XSD_TEMPLATE % 'anySimpleType'
XSD_ANY_ATOMIC_TYPE = _XSD_TEMPLATE % 'anyAtomicType'

# Other XSD built-in types.
XSD_DECIMAL = _XSD_TEMPLATE % 'decimal'
XSD_STRING = _XSD_TEMPLATE % 'string'
XSD_DOUBLE = _XSD_TEMPLATE % 'double'
XSD_FLOAT = _XSD_TEMPLATE % 'float'

XSD_DATE = _XSD_TEMPLATE % 'date'
XSD_DATETIME = _XSD_TEMPLATE % 'dateTime'
XSD_GDAY = _XSD_TEMPLATE % 'gDay'
XSD_GMONTH = _XSD_TEMPLATE % 'gMonth'
XSD_GMONTH_DAY = _XSD_TEMPLATE % 'gMonthDay'
XSD_GYEAR = _XSD_TEMPLATE % 'gYear'
XSD_GYEAR_MONTH = _XSD_TEMPLATE % 'gYearMonth'
XSD_TIME = _XSD_TEMPLATE % 'time'
XSD_DURATION = _XSD_TEMPLATE % 'duration'

XSD_QNAME = _XSD_TEMPLATE % 'QName'
XSD_NOTATION_TYPE = _XSD_TEMPLATE % 'NOTATION'
XSD_ANY_URI = _XSD_TEMPLATE % 'anyURI'
XSD_BOOLEAN = _XSD_TEMPLATE % 'boolean'
XSD_BASE64_BINARY = _XSD_TEMPLATE % 'base64Binary'
XSD_HEX_BINARY = _XSD_TEMPLATE % 'hexBinary'
XSD_NORMALIZED_STRING = _XSD_TEMPLATE % 'normalizedString'
XSD_TOKEN = _XSD_TEMPLATE % 'token'
XSD_LANGUAGE = _XSD_TEMPLATE % 'language'
XSD_NAME = _XSD_TEMPLATE % 'Name'
XSD_NCNAME = _XSD_TEMPLATE % 'NCName'
XSD_ID = _XSD_TEMPLATE % 'ID'
XSD_IDREF = _XSD_TEMPLATE % 'IDREF'
XSD_ENTITY = _XSD_TEMPLATE % 'ENTITY'
XSD_NMTOKEN = _XSD_TEMPLATE % 'NMTOKEN'

XSD_INTEGER = _XSD_TEMPLATE % 'integer'
XSD_LONG = _XSD_TEMPLATE % 'long'
XSD_INT = _XSD_TEMPLATE % 'int'
XSD_SHORT = _XSD_TEMPLATE % 'short'
XSD_BYTE = _XSD_TEMPLATE % 'byte'
XSD_NON_NEGATIVE_INTEGER = _XSD_TEMPLATE % 'nonNegativeInteger'
XSD_POSITIVE_INTEGER = _XSD_TEMPLATE % 'positiveInteger'
XSD_UNSIGNED_LONG = _XSD_TEMPLATE % 'unsignedLong'
XSD_UNSIGNED_INT = _XSD_TEMPLATE % 'unsignedInt'
XSD_UNSIGNED_SHORT = _XSD_TEMPLATE % 'unsignedShort'
XSD_UNSIGNED_BYTE = _XSD_TEMPLATE % 'unsignedByte'
XSD_NON_POSITIVE_INTEGER = _XSD_TEMPLATE % 'nonPositiveInteger'
XSD_NEGATIVE_INTEGER = _XSD_TEMPLATE % 'negativeInteger'

# Built-in list types
XSD_IDREFS = _XSD_TEMPLATE % 'IDREFS'
XSD_ENTITIES = _XSD_TEMPLATE % 'ENTITIES'
XSD_NMTOKENS = _XSD_TEMPLATE % 'NMTOKENS'

# XSD 1.1 built-in types
XSD_DATE_TIME_STAMP = _XSD_TEMPLATE % 'dateTimeStamp'
XSD_DAY_TIME_DURATION = _XSD_TEMPLATE % 'dayTimeDuration'
XSD_YEAR_MONTH_DURATION = _XSD_TEMPLATE % 'yearMonthDuration'
XSD_ERROR = _XSD_TEMPLATE % 'error'

XSD_UNTYPED_ATOMIC = _XSD_TEMPLATE % 'untypedAtomic'

###
# Aggregations of multiple tags for checking

GLOBAL_TAGS = frozenset((
    XSD_NOTATION, XSD_SIMPLE_TYPE, XSD_COMPLEX_TYPE,
    XSD_ATTRIBUTE, XSD_ATTRIBUTE_GROUP, XSD_GROUP, XSD_ELEMENT
))
SCHEMA_DECLARATION_TAGS = frozenset((XSD_IMPORT, XSD_INCLUDE, XSD_REDEFINE, XSD_OVERRIDE))
MODEL_TAGS = frozenset((XSD_SEQUENCE, XSD_ALL, XSD_CHOICE))
MODEL_GROUP_TAGS = frozenset((XSD_GROUP, XSD_SEQUENCE, XSD_ALL, XSD_CHOICE))
CONTENT_TAIL_TAGS = frozenset((XSD_ATTRIBUTE, XSD_ATTRIBUTE_GROUP, XSD_ANY_ATTRIBUTE, XSD_ASSERT))
IDENTITY_TAGS = frozenset((XSD_KEY, XSD_KEYREF, XSD_UNIQUE))
GLOBAL_TYPES_TAGS = (XSD_COMPLEX_TYPE, XSD_SIMPLE_TYPE)
QNAME_TAGS = (XSD_QNAME, XSD_NOTATION_TYPE)

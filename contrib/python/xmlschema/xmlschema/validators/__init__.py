#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from .exceptions import XMLSchemaValidatorError, XMLSchemaParseError, \
    XMLSchemaModelError, XMLSchemaModelDepthError, XMLSchemaValidationError, \
    XMLSchemaDecodeError, XMLSchemaEncodeError, XMLSchemaNotBuiltError, \
    XMLSchemaChildrenValidationError, XMLSchemaStopValidation, \
    XMLSchemaIncludeWarning, XMLSchemaImportWarning, \
    XMLSchemaTypeTableWarning, XMLSchemaAssertPathWarning

from .validation import ValidationContext, DecodeContext, EncodeContext, ValidationMixin
from .xsdbase import XsdValidator, XsdComponent, XsdAnnotation, XsdType
from .particles import ParticleMixin
from .assertions import XsdAssert
from .notations import XsdNotation
from .identities import XsdSelector, XsdFieldSelector, XsdIdentity, XsdKeyref, XsdKey, \
    XsdUnique, Xsd11Keyref, Xsd11Key, Xsd11Unique
from .facets import XsdFacet, XsdWhiteSpaceFacet, XsdLengthFacet, XsdMinLengthFacet, \
    XsdMaxLengthFacet, XsdMinExclusiveFacet, XsdMinInclusiveFacet, XsdMaxExclusiveFacet, \
    XsdMaxInclusiveFacet, XsdFractionDigitsFacet, XsdTotalDigitsFacet, \
    XsdExplicitTimezoneFacet, XsdPatternFacets, XsdEnumerationFacets, XsdAssertionFacet
from .wildcards import XsdAnyElement, Xsd11AnyElement, XsdAnyAttribute, Xsd11AnyAttribute, \
    XsdOpenContent, XsdDefaultOpenContent
from .attributes import XsdAttribute, Xsd11Attribute, XsdAttributeGroup
from .simple_types import XsdSimpleType, XsdAtomic, XsdAtomicBuiltin, \
    XsdAtomicRestriction, Xsd11AtomicRestriction, XsdList, XsdUnion, Xsd11Union
from .complex_types import XsdComplexType, Xsd11ComplexType
from .models import ModelVisitor
from .groups import XsdGroup, Xsd11Group
from .elements import XsdElement, Xsd11Element, XsdAlternative

from .builders import XsdBuilders, GlobalMaps
from .xsd_globals import XsdGlobals
from .schemas import XMLSchemaMeta, XMLSchemaBase, XMLSchema, XMLSchema10, XMLSchema11


__all__ = [
    'ValidationContext', 'DecodeContext', 'EncodeContext', 'ValidationMixin',
    'XMLSchemaValidatorError', 'XMLSchemaParseError', 'XMLSchemaModelError',
    'XMLSchemaModelDepthError', 'XMLSchemaValidationError', 'XMLSchemaDecodeError',
    'XMLSchemaEncodeError', 'XMLSchemaNotBuiltError', 'XMLSchemaChildrenValidationError',
    'XMLSchemaStopValidation', 'XMLSchemaIncludeWarning', 'XMLSchemaImportWarning',
    'XMLSchemaTypeTableWarning', 'XMLSchemaAssertPathWarning',
    'XsdValidator', 'XsdComponent', 'XsdAnnotation', 'XsdType',
    'ParticleMixin', 'XsdAssert', 'XsdNotation', 'XsdSelector', 'XsdFieldSelector',
    'XsdIdentity', 'XsdKeyref', 'XsdKey', 'XsdUnique', 'Xsd11Keyref', 'Xsd11Key',
    'Xsd11Unique', 'XsdFacet', 'XsdWhiteSpaceFacet', 'XsdLengthFacet', 'XsdMinLengthFacet',
    'XsdMaxLengthFacet', 'XsdMinExclusiveFacet', 'XsdMinInclusiveFacet',
    'XsdMaxExclusiveFacet', 'XsdMaxInclusiveFacet', 'XsdFractionDigitsFacet',
    'XsdTotalDigitsFacet', 'XsdExplicitTimezoneFacet', 'XsdPatternFacets',
    'XsdEnumerationFacets', 'XsdAssertionFacet', 'XsdAnyElement', 'Xsd11AnyElement',
    'XsdAnyAttribute', 'Xsd11AnyAttribute', 'XsdOpenContent', 'XsdDefaultOpenContent',
    'XsdAttribute', 'Xsd11Attribute', 'XsdAttributeGroup', 'XsdSimpleType', 'XsdAtomic',
    'XsdAtomicBuiltin', 'XsdAtomicRestriction', 'Xsd11AtomicRestriction', 'XsdList',
    'XsdUnion', 'Xsd11Union', 'XsdComplexType', 'Xsd11ComplexType', 'ModelVisitor',
    'XsdGroup', 'Xsd11Group', 'XsdElement', 'Xsd11Element', 'XsdAlternative',
    'XsdBuilders', 'GlobalMaps', 'XsdGlobals',
    'XMLSchemaMeta', 'XMLSchemaBase', 'XMLSchema', 'XMLSchema10', 'XMLSchema11',
]

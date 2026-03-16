#
# Copyright (c), 2021-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Type aliases for static typing analysis. In a type checking context the aliases
are defined from effective classes imported from package modules. In a runtime
context the aliases that can't be set from the same bases, due to circular
imports, are set with a common.
"""
from decimal import Decimal
from pathlib import Path
from collections import Counter
from collections.abc import Callable, Iterator, MutableMapping, Sequence
from typing import Any, AnyStr, IO, Optional, TYPE_CHECKING, TypeVar, Union
from xml.etree.ElementTree import Element, ElementTree

from elementpath.datatypes import NormalizedString, QName, Float10, Integer, \
    AnyURI, Duration, AbstractDateTime, AbstractBinary, AnyAtomicType
from elementpath.protocols import ElementProtocol, DocumentProtocol
from elementpath import ElementNode, LazyElementNode, DocumentNode

from xmlschema.utils.protocols import IOProtocol

if TYPE_CHECKING:
    from xmlschema.resources import XMLResource  # noqa: F401
    from xmlschema.locations import NamespaceResourcesMap  # noqa: F401
    from xmlschema.converters import ElementData  # noqa: F401
    from xmlschema.xpath import ElementSelector  # noqa: F401
    from xmlschema.settings import ResourceSettings, SchemaSettings  # noqa: F401

    # noinspection PyUnresolvedReferences
    from xmlschema.validators import XMLSchemaValidationError, XsdComponent, \
        XsdComplexType, XsdSimpleType, XsdElement, XsdAnyElement, XsdAttribute, \
        XsdAnyAttribute, XsdAssert, XsdGroup, XsdAttributeGroup, XsdNotation, \
        ParticleMixin, XMLSchemaBase, XsdGlobals, ValidationContext, \
        DecodeContext  # noqa: F401

##
# Generic and bounded type vars

T = TypeVar('T')

##
# Type aliases for ElementTree
ElementType = Element
ElementTreeType = ElementTree

##
# Type aliases for namespaces
NsmapType = MutableMapping[str, str]
LocationsMapType = dict[str, Union[str, list[str]]]
NormalizedLocationsType = list[tuple[str, str]]
LocationsType = Union[tuple[tuple[str, str], ...], dict[str, str],
                      NormalizedLocationsType, 'NamespaceResourcesMap[str]']
XmlnsType = Optional[list[tuple[str, str]]]

##
# Type aliases for XML resources
SettingsType = Union['ResourceSettings']
IOType = Union[IOProtocol[str], IOProtocol[bytes]]
EtreeType = Union[Element, ElementTree, ElementProtocol, DocumentProtocol]
XMLSourceType = Union[EtreeType, str, bytes, Path, IO[str], IO[bytes]]
SourceArgType = Union[XMLSourceType, 'XMLResource']
SourceDataArgType = Union[SourceArgType, dict[str, str], None, AnyAtomicType, bytes]

ResourceNodeType = Union[ElementNode, LazyElementNode, DocumentNode]
BaseUrlType = Union[str, bytes, Path]
LazyType = Union[bool, int]
BlockType = Union[str, tuple[str, ...], list[str]]
UriMapperType = Union[MutableMapping[str, str], Callable[[str], str]]
IterParseType = Callable[[IOType, Optional[Sequence[str]]], Iterator[tuple[str, Any]]]
SelectorType = Optional[type['ElementSelector']]
ParentMapType = dict[ElementType, Optional[ElementType]]
NsmapsMapType = dict[ElementType, dict[str, str]]
XmlnsMapType = dict[ElementType, list[tuple[str, str]]]
AncestorsType = Optional[list[ElementType]]

LogLevelType = Union[int, str, None]


##
# Type aliases for XSD components
SchemaType = Union['XMLSchemaBase']
GlobalMapsType = Union['XsdGlobals']
BaseXsdType = Union['XsdSimpleType', 'XsdComplexType']
SchemaElementType = Union['XsdElement', 'XsdAnyElement']
SchemaAttributeType = Union['XsdAttribute', 'XsdAnyAttribute']
SchemaGlobalType = Union['XsdNotation', 'BaseXsdType', 'XsdElement',
                         'XsdAttribute', 'XsdAttributeGroup', 'XsdGroup']

ModelGroupType = Union['XsdGroup']
ModelParticleType = Union['XsdElement', 'XsdAnyElement', 'XsdGroup']
OccursCounterType = Counter[
    Union['ParticleMixin', ModelParticleType, tuple[ModelGroupType], None]
]
ComponentClassType = Union[None, type['XsdComponent'], tuple[type['XsdComponent'], ...]]
XPathElementType = Union['XsdElement', 'XsdAnyElement', 'XsdAssert']

C = TypeVar('C')
ClassInfoType = Union[type[C], tuple[type[C], ...]]

LoadedItemType = tuple[ElementType, SchemaType]
StagedItemType = Union[LoadedItemType, list[LoadedItemType], tuple[LoadedItemType]]

##
# Type aliases for datatypes
AtomicValueType = Union[str, bytes, int, float, Decimal, bool, Integer,
                        Float10, NormalizedString, AnyURI, QName, Duration,
                        AbstractDateTime, AbstractBinary]
NumericValueType = Union[str, bytes, int, float, Decimal]

##
# Type aliases for validation/decoding/encoding
DecodeContextType = Union['ValidationContext', 'DecodeContext']
ErrorsType = list['XMLSchemaValidationError']
ExtraValidatorType = Callable[[ElementType, 'XsdElement'],
                              Optional[Iterator['XMLSchemaValidationError']]]
ValidationHookType = Callable[[ElementType, 'XsdElement'], Union[bool, str]]

D = TypeVar('D')
DecodeType = Union[Optional[D], tuple[Optional[D], ErrorsType]]
IterDecodeType = Iterator[Union[D, 'XMLSchemaValidationError']]

E = TypeVar('E')
EncodeType = Union[Optional[E], tuple[Optional[E], ErrorsType]]
IterEncodeType = Iterator[Union[E, 'XMLSchemaValidationError']]

JsonDecodeType = Union[str, None, tuple['XMLSchemaValidationError', ...],
                       tuple[Union[str, None], tuple['XMLSchemaValidationError', ...]]]

DecodedValueType = Union[None, AtomicValueType, list[Optional[AtomicValueType]]]
FillerType = Callable[[Union['XsdElement', 'XsdAttribute']], DecodedValueType]
DepthFillerType = Callable[['XsdElement'], Any]
ValueHookType = Callable[[Optional[AtomicValueType], 'BaseXsdType'], DecodedValueType]
ElementHookType = Callable[
    ['ElementData', Optional['XsdElement'], Optional['BaseXsdType']], 'ElementData'
]
SerializerType = Callable[[Any], IO[AnyStr]]

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
This module contains XMLSchema classes creator for xmlschema package.

Two schema classes are created at the end of this module, XMLSchema10 for XSD 1.0 and
XMLSchema11 for XSD 1.1. The latter class parses also XSD 1.0 schemas, as prescribed by
the standard.
"""
from abc import ABCMeta
import logging
import re
import sys
from collections.abc import Callable, Iterator
from functools import cached_property
from operator import attrgetter
from pathlib import Path
from typing import Any, cast, Optional, Union
from urllib.request import OpenerDirector
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from importlib.resources.abc import Traversable

from elementpath import XPathToken, SchemaElementNode, build_schema_node_tree

import xmlschema.names as nm
from xmlschema.aliases import XMLSourceType, NsmapType, LocationsType, UriMapperType, \
    SchemaType, SourceArgType, ComponentClassType, DecodeType, EncodeType, \
    BaseXsdType, ExtraValidatorType, ValidationHookType, SchemaGlobalType, \
    FillerType, DepthFillerType, ValueHookType, ElementHookType, ElementType, \
    StagedItemType, IterParseType
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaKeyError, \
    XMLSchemaRuntimeError, XMLSchemaValueError, XMLSchemaNamespaceError, \
    XMLSchemaAttributeError
from xmlschema.translation import gettext as _
from xmlschema.utils.decoding import Empty
from xmlschema.utils.etree import prune_etree, is_etree_element, \
    iter_schema_declarations, iter_schema_open_content
from xmlschema.utils.qnames import get_namespace_ext
from xmlschema.resources import XMLResource
from xmlschema.arguments import check_validation_mode
from xmlschema.converters import XMLSchemaConverter, ConverterType
from xmlschema.xpath import XMLSchemaProxy, ElementPathMixin
from xmlschema.namespaces import NamespaceView, NamespaceMapper
from xmlschema.locations import SCHEMAS_DIR
from xmlschema.loaders import SchemaLoader
from xmlschema.exports import export_schema
from xmlschema.settings import SchemaSettings, ResourceSettings
from xmlschema import dataobjects

from .exceptions import XMLSchemaValidationError, XMLSchemaEncodeError, \
    XMLSchemaStopValidation
from .validation import ValidationContext, DecodeContext, EncodeContext
from .helpers import parse_xsd_derivation, get_schema_annotations, qname_validator, \
    parse_xpath_default_namespace, parse_target_namespace
from .xsdbase import XSD_ELEMENT_DERIVATIONS, XsdValidator, XsdComponent, XsdAnnotation
from .notations import XsdNotation
from .identities import XsdIdentity, XsdKeyref, KeyrefCounter
from .simple_types import XsdSimpleType
from .attributes import XsdAttribute, XsdAttributeGroup
from .complex_types import XsdComplexType
from .groups import XsdGroup
from .elements import XsdElement
from .wildcards import XsdAnyElement, XsdDefaultOpenContent
from .builders import XsdBuilders
from .xsd_globals import XsdGlobals

logger = logging.getLogger('xmlschema')

name_attribute = attrgetter('name')

XSD_VERSION_PATTERN = re.compile(r'^\d+\.\d+$')

# Registry for schema instances that are real meta-schema of a schema class
_meta_registry = set()


class XMLSchemaMeta(ABCMeta):
    XSD_VERSION: str
    BASE_SCHEMAS: dict[str, str]
    meta_schema: Optional[SchemaType]
    create_meta_schema: Callable[[SchemaType, Optional[str]], SchemaType]

    def __new__(mcs, name: str, bases: tuple[type[Any]], dict_: dict[str, Any]) \
            -> 'XMLSchemaMeta':
        assert bases, "a base class is mandatory"
        base_class = bases[0]

        meta_schema_file: Optional[str]
        if isinstance(dict_.get('META_SCHEMA'), str):
            meta_schema_file = dict_.get('META_SCHEMA')
            if not isinstance(meta_schema_file, str):
                raise XMLSchemaTypeError("META_SCHEMA must be a string defining the "
                                         "location of the XSD meta-schema file")
        elif isinstance(dict_.get('META_SCHEMA'), Traversable):
            meta_schema_file = dict_.get('META_SCHEMA')
        elif isinstance(dict_.get('meta_schema'), str):
            meta_schema_file = dict_.pop('meta_schema')  # For backward compatibility
        else:
            meta_schema_file = None
        if isinstance(meta_schema_file, (str, Traversable)):
            try:
                base_schemas = dict_.get('BASE_SCHEMAS', {})
            except KeyError as e:
                raise XMLSchemaAttributeError(
                    f"{str(e)} attribute is mandatory for defining a schema class"
                )
            else:
                if not isinstance(base_schemas, dict):
                    raise XMLSchemaTypeError("BASE_SCHEMAS must be a dictionary")  # noqa

            # Build the meta-schema class and register it into module's globals
            meta_schema_class_name = 'Meta' + name

            meta_schema: Optional[SchemaType]
            meta_schema = getattr(base_class, 'meta_schema', None)
            if meta_schema is None:
                meta_bases = bases
            else:
                # Use base's meta_schema class as base for the new meta-schema
                meta_bases = (meta_schema.__class__,)
                if len(bases) > 1:
                    meta_bases += bases[1:]

            meta_schema_class = cast(
                type[SchemaType],
                super().__new__(mcs, meta_schema_class_name, meta_bases, dict_)
            )
            meta_schema_class.__qualname__ = meta_schema_class_name
            module = sys.modules[dict_['__module__']]
            setattr(module, meta_schema_class_name, meta_schema_class)

            meta_schema = meta_schema_class.create_meta_schema(meta_schema_file, base_schemas)
            dict_['meta_schema'] = meta_schema
            _meta_registry.add(meta_schema)

        # Create the class and check some basic attributes
        cls = super().__new__(mcs, name, bases, dict_)
        if cls.XSD_VERSION not in ('1.0', '1.1'):
            raise XMLSchemaValueError(_("XSD_VERSION must be '1.0' or '1.1'"))
        return cls


class XMLSchemaBase(XsdValidator, ElementPathMixin[Union[SchemaType, XsdElement]],
                    metaclass=XMLSchemaMeta):
    """
    Base class for an XML Schema instance.

    :param source: a URI that reference to a resource or a file path or a file-like \
    object or a string containing the schema or an Element or an ElementTree document \
    or an :class:`XMLResource` instance. A multi source initialization is supported \
    providing a not empty list of XSD sources.
    :param namespace: is an optional argument that contains the URI of the namespace \
    that has to used in case the schema has no namespace (chameleon schema). For other \
    cases, when specified, it must be equal to the *targetNamespace* of the schema.
    :param validation: the XSD validation mode to use for build the schema, \
    that can be 'strict' (default), 'lax' or 'skip'.
    :param global_maps: is an optional argument containing an :class:`XsdGlobals` \
    instance, a mediator object for sharing declaration data between dependents \
    schema instances.
    :param parent: optional :class:`XMLSchema` instance to use as parent if a new \
    :class:`XsdGlobals` is created, ignored otherwise.
    :param loader_class: an optional subclass of :class:`SchemaLoader` to use for creating \
    the loader instance.
    :param converter: is an optional argument that can be an :class:`XMLSchemaConverter` \
    subclass or instance, used for defining the default XML data converter for XML Schema instance.
    :param locations: schema extra location hints, that can include custom resource locations \
    (e.g. local XSD file instead of remote resource) or additional namespaces to import after \
    processing schema's import statements. Can be a dictionary or a sequence of couples \
    (namespace URI, resource URL). Extra locations passed using a tuple container are not \
    normalized.
    :param base_url: is an optional base URL, used for the normalization of relative paths \
    when the URL of the schema resource can't be obtained from the source argument.
    :param use_fallback: if `True` the schema processor uses the validator fallback \
    location hints to load well-known namespaces (e.g. xhtml).
    :param use_xpath3: if `True` an XSD 1.1 schema instance uses the XPath 3 processor \
    for assertions. For default a full XPath 2.0 processor is used.
    :param use_meta: if `True` the schema processor uses the validator meta-schema as \
    parent schema. Ignored if either *global_maps* or *parent* argument is provided.
    :param use_cache: if `True` the schema processor enable caching for components on \
    a cache managed by global maps. For default caching is enabled except for predefined \
    meta-schema maps.
    :param loglevel: for setting a different logging level for schema initialization \
    and building. For default is WARNING (30). For INFO level set it with 20, for \
    DEBUG level with 10. The default loglevel is restored after schema building, \
    when exiting the initialization method.
    :param build: defines whether build the schema maps. Default is `True`.
    :param partial: if `True`, the schema is initialized without processing \
    imports/inclusions and the build phase is skipped.
    :param kwargs: additional arguments for overriding default XMLResource settings.

    :cvar XSD_VERSION: store the XSD version (1.0 or 1.1).
    :cvar BASE_SCHEMAS: a dictionary from namespace to schema resource for meta-schema bases.
    :cvar meta_schema: the XSD meta-schema instance.
    :cvar attribute_form_default: the schema's *attributeFormDefault* attribute. \
    Default is 'unqualified'.
    :cvar element_form_default: the schema's *elementFormDefault* attribute. \
    Default is 'unqualified'.
    :cvar block_default: the schema's *blockDefault* attribute. Default is ''.
    :cvar final_default: the schema's *finalDefault* attribute. Default is ''.
    :cvar default_attributes: the XSD 1.1 schema's *defaultAttributes* attribute. \
    Default is ``None``.

    :ivar target_namespace: is the *targetNamespace* of the schema, the namespace to which \
    belong the declarations/definitions of the schema. If it's empty no namespace is associated \
    with the schema. In this case the schema declarations can be reused from other namespaces as \
    *chameleon* definitions.
    :ivar maps: XSD global declarations/definitions maps. This is an instance of \
    :class:`XsdGlobals`, that stores the *global_maps* argument or a new object \
    when this argument is not provided.
    :ivar namespaces: a dictionary that maps from the prefixes used by the schema \
    into namespace URI.
    :ivar imports: a dictionary of namespace imports of the schema, that maps namespace \
    URI to imported schema object, or `None` in case of unsuccessful import.
    :ivar includes: a dictionary of included schemas, that maps a schema location to an \
    included schema. It also comprehends schemas included by "xs:redefine" or \
    "xs:override" statements.
    :ivar warnings: warning messages about failure of import and include elements.
    """
    XSD_VERSION: str = '1.0'
    META_SCHEMA: str
    BASE_SCHEMAS: dict[str, str] = {}

    builders: XsdBuilders
    meta_schema: Optional[SchemaType] = None

    # Instance attributes type annotations
    source: XMLResource
    namespaces: NsmapType
    maps: XsdGlobals

    imported_namespaces: list[str]
    imports: dict[str, Optional[SchemaType]]
    includes: dict[str, SchemaType]
    warnings: list[str]

    # Schema defaults
    attribute_form_default = 'unqualified'
    element_form_default = 'unqualified'
    block_default = ''
    final_default = ''
    redefine: Optional[SchemaType] = None
    partial: bool = False

    # Additional defaults for XSD 1.1
    default_attributes: Optional[Union[str, XsdAttributeGroup]] = None
    default_open_content: Optional[XsdDefaultOpenContent] = None
    override: Optional[SchemaType] = None

    __slots__ = ('validation', 'errors', 'maps', 'target_namespace', 'source', 'namespaces')

    @classmethod
    def from_settings(cls, settings: SchemaSettings,
                      source: Union[SourceArgType, list[SourceArgType]],
                      **kwargs: Any) -> SchemaType:
        """
        Returns a new schema instance from schema settings. Optional keyword arguments
        must be options for schema initialization and can be passed also to override
        some settings. If a `global_map` argument is provided, it will be removed and
        used to provide a `parent` argument.

        :param settings: schema settings.
        :param source: the schema source.
        :param kwargs: additional arguments for schema initialization.
        """
        return settings.get_schema(cls, source, **kwargs)

    def __init__(self, source: Union[SourceArgType, list[SourceArgType]],
                 namespace: Optional[str] = None,
                 validation: str = 'strict',
                 global_maps: Optional[XsdGlobals] = None,
                 parent: Optional[SchemaType] = None,
                 converter: Optional[ConverterType] = None,
                 locations: Optional[LocationsType] = None,
                 base_url: Optional[str] = None,
                 loader_class: Optional[type[SchemaLoader]] = None,
                 use_fallback: bool = True,
                 use_xpath3: bool = False,
                 use_meta: bool = True,
                 use_cache: bool = True,
                 loglevel: Optional[Union[str, int]] = None,
                 build: bool = True,
                 partial: bool = False,
                 **kwargs: Any) -> None:

        super().__init__(validation)

        self.imports = {}
        self.imported_namespaces = []
        self.includes = {}
        self.warnings = []

        if isinstance(global_maps, XsdGlobals):
            if kwargs:
                ResourceSettings(**kwargs)
            settings = global_maps.settings
        else:
            settings = cast(SchemaSettings, SchemaSettings.get_settings(
                validation=validation,
                loader_class=loader_class,
                locations=locations,
                converter=converter,
                base_url=base_url,
                use_fallback=use_fallback,
                use_xpath3=use_xpath3,
                use_cache=use_cache,
                loglevel=loglevel,
                **kwargs,
            ))
            if settings.loglevel is not None:
                logger.setLevel(settings.loglevel)
            elif build:
                logger.setLevel(logging.WARNING)

        if isinstance(source, list):
            other_sources: list[SourceArgType] = source[1:]
            source = source[0]
        else:
            other_sources = []

        logger.debug("Load schema from %r", source)
        self.source = settings.get_schema_resource(source, base_url)

        self.name = self.source.name
        root = self.source.root

        # Initialize schema's namespaces, the XML namespace is implicitly declared.
        self.namespaces = self.source.get_namespaces(
            {'xml': nm.XML_NAMESPACE}, self.meta_schema is None, True
        )
        logger.debug("Schema namespaces: %r", self.namespaces)

        if 'targetNamespace' in root.attrib:
            self.target_namespace = parse_target_namespace(self)
            if namespace is not None and self.target_namespace != namespace:
                msg = _("targetNamespace of XSD resource {} differs from what expected "
                        "(found {!r} instead of {!r})")
                self.parse_error(msg.format(self.url, self.target_namespace, namespace))

        elif namespace is not None:
            self.target_namespace = namespace
            if self.namespaces[''] == '':
                self.namespaces[''] = namespace
        else:
            self.target_namespace = ''

        logger.debug("Schema targetNamespace is %r", self.target_namespace)

        # Parses the schema defaults
        if 'attributeFormDefault' in root.attrib:
            self.attribute_form_default = root.attrib['attributeFormDefault']
        if 'elementFormDefault' in root.attrib:
            self.element_form_default = root.attrib['elementFormDefault']
        if 'finalDefault' in root.attrib:
            self.final_default = parse_xsd_derivation(root, 'finalDefault', validator=self)
        if 'blockDefault' in root.attrib:
            if self.target_namespace != nm.XSD_NAMESPACE or self.name != 'XMLSchema.xsd':
                # Skip for XSD 1.0 meta-schema that has blockDefault="#all"
                # Ref: https://www.w3.org/Bugs/Public/show_bug.cgi?id=6120
                self.block_default = parse_xsd_derivation(
                    root, 'blockDefault', XSD_ELEMENT_DERIVATIONS, self
                )

        # Create or set the XSD global maps instance and the loader
        if isinstance(global_maps, XsdGlobals):
            if parent is not None:
                msg = _("'global_maps' and 'parent' arguments are mutually exclusive")
                raise XMLSchemaValueError(msg)
            self.maps = global_maps
        elif parent is None and use_meta:
            self.maps = XsdGlobals(self, parent=self.meta_schema, settings=settings)
        else:
            self.maps = XsdGlobals(self, parent=parent, settings=settings)

        # Meta-schema maps creation (MetaXMLSchema10/11 classes)
        if self.meta_schema is None:
            return  # Meta-schemas don't need to be checked and don't process imports

        if any(ns == nm.VC_NAMESPACE for ns in self.namespaces.values()):
            # Apply versioning filter to schema tree. See the paragraph
            # 4.2.2 of XSD 1.1 (Part 1: Structures) definition for details.
            # Ref: https://www.w3.org/TR/xmlschema11-1/#cip
            if prune_etree(root, selector=lambda x: not self.version_check(x)):
                for k in list(root.attrib):
                    if k not in ('targetNamespace', nm.VC_MIN_VERSION, nm.VC_MAX_VERSION):
                        del root.attrib[k]

        # Validate the schema document (transforming validation errors to parse errors)
        # Don't check package schemas.
        if validation != 'skip':
            self.meta_schema.build()
            for e in self.meta_schema.iter_errors(root, namespaces=self.namespaces):
                self.parse_error(e.reason or e, elem=e.elem)

        if partial:
            for child in iter_schema_declarations(root):
                self.partial = True
                if child.tag == nm.XSD_IMPORT:
                    namespace = child.get('namespace', '').strip()
                    self.imported_namespaces.append(namespace)
            return

        self.maps.loader.load_declared_schemas(self, other_sources)

        # Parse XSD 1.1 default declarations (defaultAttributes, defaultOpenContent,
        # xpathDefaultNamespace) after all imports/includes.
        if self.XSD_VERSION > '1.0':
            self.xpath_default_namespace = parse_xpath_default_namespace(self)
            if 'defaultAttributes' in root.attrib:
                try:
                    self.default_attributes = self.resolve_qname(root.attrib['defaultAttributes'])
                except (ValueError, KeyError, RuntimeError) as err:
                    self.parse_error(err, root)

            for child in iter_schema_open_content(root):
                self.default_open_content = XsdDefaultOpenContent(child, self)
                break

        try:
            if build:
                self.maps.build()
        finally:
            if loglevel is not None:
                logger.setLevel(logging.WARNING)  # Restore default logging

    def __repr__(self) -> str:
        if (name := self.name) is None:
            return f'{self.__class__.__name__}(namespace={self.target_namespace!r})'
        return f'{self.__class__.__name__}(name={name!r}, namespace={self.target_namespace!r})'

    def __setattr__(self, name: str, value: Any) -> None:
        if name == 'maps':
            if not isinstance(value, XsdGlobals):
                if not hasattr(self, 'maps'):
                    msg = _("'global_maps' argument must be an %r instance")
                else:
                    msg = _("'maps' attribute must be an %r instance")
                raise XMLSchemaTypeError(msg % XsdGlobals)
            elif hasattr(self, 'maps'):
                if value is getattr(self, name):
                    return
                elif self.is_meta():
                    msg = _("can't change the global maps instance of a class meta-schema")
                    raise XMLSchemaAttributeError(msg)
                elif self.maps.validator is self:
                    # can change only if it's the main validator of the new global maps
                    msg = _("can't change the global maps instance of a schema that is "
                            "the main validator of another global maps instance")
                    raise XMLSchemaAttributeError(msg.format(self))

            value.register(self)
            super().__setattr__(name, value)
            return

        if name == 'source':
            if hasattr(self, 'source'):
                raise XMLSchemaAttributeError(_("can't change the schema source"))
            assert isinstance(value, XMLResource)
            if value.is_lazy():
                raise XMLSchemaValueError(_("schema resource can't be lazy"))
            if value.iterparse is not ElementTree.iterparse:
                raise XMLSchemaValueError(_("schema resource must use ElementTree.iterparse"))
        elif name == 'meta_schema':
            msg = _("can't set the meta_schema instance of a schema")
            raise XMLSchemaAttributeError(msg)
        elif name == 'validation':
            check_validation_mode(value)
        elif name == 'default_attributes':
            if isinstance(self.default_attributes, XsdAttributeGroup):
                msg = _("can't change the {!r} attribute of a schema").format(name)
                raise XMLSchemaAttributeError(msg)
        elif name in self.__dict__ and name[:1] != '_' and name != 'partial':
            msg = _("can't change the {!r} attribute of a schema").format(name)
            raise XMLSchemaAttributeError(msg)

        super().__setattr__(name, value)

    def __iter__(self) -> Iterator[XsdElement]:
        yield from sorted(self.elements.values(), key=name_attribute)

    def __reversed__(self) -> Iterator[XsdElement]:
        yield from sorted(self.elements.values(), key=name_attribute, reverse=True)

    def __len__(self) -> int:
        return len(self.elements)

    def __copy__(self) -> SchemaType:
        schema: SchemaType = object.__new__(self.__class__)
        schema.__dict__.update(
            (k, v.copy() if isinstance(v, (list, dict)) else v)
            for k, v in self.__dict__.items()
        )
        for attr in self._mro_slots():
            value = getattr(self, attr)
            if isinstance(value, (list, dict)):
                object.__setattr__(schema, attr, value.copy())
            else:
                object.__setattr__(schema, attr, value)

        return schema

    copy = __copy__

    @property
    def xsd_version(self) -> str:
        """Compatibility property that returns the class attribute XSD_VERSION."""
        return self.XSD_VERSION

    @cached_property
    def types(self) -> NamespaceView[BaseXsdType]:
        """`xsd:simpleType` and `xsd:complexType` global declarations"""
        return NamespaceView(self, 'types')

    @cached_property
    def attributes(self) -> NamespaceView[XsdAttribute]:
        """`xsd:attribute` global declarations"""
        return NamespaceView(self, 'attributes')

    @cached_property
    def attribute_groups(self) -> NamespaceView[XsdAttributeGroup]:
        """`xsd:attributeGroup` definitions"""
        return NamespaceView(self, 'attribute_groups')

    @cached_property
    def groups(self) -> NamespaceView[XsdGroup]:
        """`xsd:group` global definitions"""
        return NamespaceView(self, 'groups')

    @cached_property
    def elements(self) -> NamespaceView[XsdElement]:
        """`xsd:element` global declarations"""
        return NamespaceView(self, 'elements')

    @cached_property
    def notations(self) -> NamespaceView[XsdNotation]:
        """`xsd:notation` declarations"""
        return NamespaceView(self, 'notations')

    @cached_property
    def substitution_groups(self) -> NamespaceView[set[XsdElement]]:
        """`xsd:substitutionGroup` definitions"""
        return NamespaceView(self, 'substitution_groups')

    @cached_property
    def identities(self) -> NamespaceView[XsdIdentity]:
        """`xsd:key`, `xsd:keyref`, `xsd:unique` declarations"""
        return NamespaceView(self, 'identities')

    @property
    def xpath_proxy(self) -> XMLSchemaProxy:
        return XMLSchemaProxy(self)

    @cached_property
    def xpath_node(self) -> SchemaElementNode:
        """Returns an XPath node for processing an XPath expression on the schema instance."""
        # noinspection PyTypeChecker
        return build_schema_node_tree(root=self, uri=self.source.url)

    @property
    def xpath_tokens(self) -> dict[str, type[XPathToken]]:
        """Returns the XPath constructors tokens."""
        return self.maps.xpath_constructors

    @property
    def root(self) -> Element:
        """Root element of the schema."""
        return self.source.root

    @property
    def elem(self) -> Element:
        return self.source.root

    def get_text(self) -> str:
        """Returns the source text of the XSD schema."""
        return self.source.get_text()

    @property
    def url(self) -> Optional[str]:
        """Schema resource URL, is `None` if the schema is built from an Element or a string."""
        return self.source.url

    @property
    def base_url(self) -> Optional[str]:
        """The base URL of the source of the schema."""
        return self.source.base_url

    @property
    def filepath(self) -> Optional[str]:
        """The filepath if the schema is loaded from a local XSD file, `None` otherwise."""
        return self.source.filepath

    @property
    def allow(self) -> str:
        """The resource access security mode: can be 'all', 'remote', 'local' or 'sandbox'."""
        return self.source.allow

    @property
    def defuse(self) -> str:
        """Defines when to defuse XML data: can be 'always', 'remote' or 'never'."""
        return self.source.defuse

    @property
    def timeout(self) -> int:
        """Timeout in seconds for fetching resources."""
        return self.source.timeout

    @property
    def uri_mapper(self) -> Optional[UriMapperType]:
        """The optional URI mapper argument for relocating addressed resources."""
        return self.source.uri_mapper

    @property
    def opener(self) -> Optional[OpenerDirector]:
        """The optional OpenerDirector argument for opening addressed resources."""
        return self.source.opener

    @property
    def converter(self) -> Optional[ConverterType]:
        return self.maps.settings.converter

    @property
    def iterparse(self) -> Optional[IterParseType]:
        """The optional callable argument for creating iterator parsers for XML data."""
        return self.maps.settings.iterparse

    @property
    def locations(self) -> Optional[LocationsType]:
        """Schema extra location hints also provided by document schema location hints."""
        return self.maps.settings.locations

    @property
    def use_fallback(self) -> bool:
        """If the schema processor uses the validator fallback location hints."""
        return self.maps.settings.use_fallback

    @property
    def use_xpath3(self) -> bool:
        """If XSD 1.1 schema instance uses the XPath 3 processor for assertions."""
        return self.maps.settings.use_xpath3

    @property
    def use_meta(self) -> bool:
        """Returns `True` if the class meta-schema is used."""
        return self.is_meta() or self.maps.use_meta

    def is_meta(self) -> bool:
        """Returns `True` if it's a schema of a class meta-schema."""
        return self.meta_schema is None and self in _meta_registry

    # Schema root attributes
    @cached_property
    def tag(self) -> str:
        """Schema root tag. For compatibility with the ElementTree API."""
        return self.source.root.tag

    @cached_property
    def id(self) -> Optional[str]:
        """The schema's *id* attribute, defaults to ``None``."""
        return self.source.root.get('id')

    @cached_property
    def version(self) -> Optional[str]:
        """The schema's *version* attribute, defaults to ``None``."""
        return self.source.root.get('version')

    @cached_property
    def schema_location(self) -> list[tuple[str, str]]:
        """
        A list of location hints extracted from the *xsi:schemaLocation* attribute of the schema.
        """
        return [(k, v) for k, v in self.source.iter_location_hints() if k]

    @cached_property
    def no_namespace_schema_location(self) -> Optional[str]:
        """
        A location hint extracted from the *xsi:noNamespaceSchemaLocation* attribute of the schema.
        """
        for k, v in self.source.iter_location_hints():
            if not k:
                return v
        return None

    @property
    def default_namespace(self) -> str:
        """The namespace associated to the empty prefix ''."""
        return self.namespaces['']

    @cached_property
    def target_prefix(self) -> str:
        """The prefix associated to the *targetNamespace*."""
        for prefix, namespace in self.namespaces.items():
            if namespace == self.target_namespace:
                return prefix
        return ''

    @classmethod
    def builtin_types(cls) -> NamespaceView[BaseXsdType]:
        """Returns the XSD built-in types of the meta-schema."""
        if cls.meta_schema is None:
            raise XMLSchemaRuntimeError(_("meta-schema unavailable for %r") % cls)

        cls.meta_schema.maps.build()
        return cls.meta_schema.types

    @cached_property
    def annotations(self) -> list[XsdAnnotation]:
        """
        Annotations related to schema object. This list includes the annotations
        of xs:include, xs:import, xs:redefine and xs:override elements.
        """
        return get_schema_annotations(self)

    @cached_property
    def components(self) -> dict[ElementType, XsdComponent]:
        """A map from XSD ElementTree elements to their schema components."""
        return {
            c.elem: c for c in cast(Iterator[XsdComponent], self.iter_components(XsdComponent))
        }

    @cached_property
    def root_elements(self) -> list[XsdElement]:
        """
        The list of global elements that are not used by reference in any model of the schema.
        This is implemented as lazy property because it's computationally expensive to build
        when the schema model is complex.
        """
        if not self.elements:
            return []
        elif len(self.elements) == 1:
            return list(self.elements.values())

        names = {e.name for e in self.elements.values()}
        for xsd_element in self.elements.values():
            for e in xsd_element.iter():
                if e is xsd_element or isinstance(e, XsdAnyElement):
                    continue
                elif e.ref or e.parent is None:
                    if e.name in names:
                        names.discard(e.name)
                        if not names:
                            break

        return [e for e in self.elements.values() if e.name in set(names)]

    @cached_property
    def simple_types(self) -> list[XsdSimpleType]:
        """Returns a list containing the global simple types."""
        return [x for x in self.types.values() if isinstance(x, XsdSimpleType)]

    @cached_property
    def complex_types(self) -> list[XsdComplexType]:
        """Returns a list containing the global complex types."""
        return [x for x in self.types.values() if isinstance(x, XsdComplexType)]

    @classmethod
    def create_meta_schema(cls, source: Optional[str] = None,
                           base_schemas: Optional[dict[str, str]] = None,
                           global_maps: Optional[XsdGlobals] = None) -> SchemaType:
        """
        Creates a new meta-schema instance.

        :param source: location of the XSD meta-schema file/resource.
        :param base_schemas: a dictionary that contains namespace URIs and locations \
        of base schemas.
        :param global_maps: an optional XsdGlobals instance where include the meta-schema.
        """
        schema: SchemaType

        if source is None:
            source = cls.META_SCHEMA
        if base_schemas is None:
            base_schemas = cls.BASE_SCHEMAS

        if global_maps is not None and nm.XSD_NAMESPACE in global_maps.namespaces:
            schema = global_maps.namespaces[nm.XSD_NAMESPACE][0]
        else:
            schema = cls(
                source=source,
                namespace=nm.XSD_NAMESPACE,
                global_maps=global_maps,
                defuse='never',
                use_cache=False,
                partial=True,
            )
        for ns, location in base_schemas.items():
            if ns == nm.XSD_NAMESPACE:
                # Process the patch schema for XSD 1.1 meta-schema
                base_url = SCHEMAS_DIR / str(location).split("/")[-2]

                patch_schema = schema.include_schema(location=location, partial=True, base_url=base_url)
                base_url = patch_schema.base_url
                for child in patch_schema.source.root:
                    if child.tag == nm.XSD_OVERRIDE:
                        patch_schema.include_schema(
                            base_url / child.attrib['schemaLocation'],
                            base_url=base_url,
                            partial=True
                        )
                patch_schema.partial = False
            elif ns not in schema.maps.namespaces:
                schema.import_schema(namespace=ns, location=location, partial=True)

        return schema

    def create_any_content_group(self, parent: Union[XsdComplexType, XsdGroup],
                                 any_element: Optional[XsdAnyElement] = None) -> XsdGroup:
        """Helper method for creating an XSD model group based on a wildcard."""
        return self.builders.create_any_content_group(parent, any_element)

    def create_any_attribute_group(self, parent: Union[XsdComplexType, XsdElement]) \
            -> XsdAttributeGroup:
        """Helper method for creating an XSD attribute group based on a wildcard."""
        return self.builders.create_any_attribute_group(parent)

    def create_any_type(self) -> XsdComplexType:
        """Helper method for creating an XSD type that accepts any content."""
        return self.builders.create_any_type(self)

    def create_empty_content_group(self, parent: Union[XsdComplexType, XsdGroup],
                                   model: str = 'sequence', **attrib: Any) -> XsdGroup:
        """Helper method for creating an empty XSD model group."""
        return self.builders.create_empty_content_group(parent, model, **attrib)

    def create_empty_attribute_group(self, parent: Union[XsdComplexType, XsdElement]) \
            -> XsdAttributeGroup:
        """Helper method for creating an empty XSD attribute group."""
        return self.builders.create_empty_attribute_group(parent)

    def create_element(self, name: str, parent: Optional[XsdComponent] = None,
                       text: Optional[str] = None, **attrib: Any) -> XsdElement:
        """Helper method for creating an XSD element."""
        return self.builders.create_element(name, self, parent, text, **attrib)

    def clear(self) -> None:
        """ Clears the schema caches unloading components and schema node tree."""
        for attr in self._cached_properties():
            self.__dict__.pop(attr, None)

    def build(self) -> None:
        """Builds the schema's XSD global maps."""
        self.maps.build()

    @property
    def built(self) -> bool:
        return self.maps.built

    @cached_property
    def validation_attempted(self) -> str:
        if any(isinstance(t, tuple) and t[-1] is self
               for x in self.maps.global_maps.iter_staged() for t in x):
            return 'partial'
        elif any(c.schema is self and not c.built
                 for c in self.maps.global_maps.iter_globals()):
            return 'partial'
        elif any(c.schema is self for c in self.maps.global_maps.iter_globals()):
            return 'full'
        elif any(child.tag in nm.GLOBAL_TAGS for child in self.source.root) or \
                any(e.tag in nm.GLOBAL_TAGS for child in self.source.root for e in child):
            return 'none'
        else:
            return 'full'

    @property
    def validity(self) -> str:
        if self.validation == 'skip':
            return 'notKnown'
        elif any(v.errors for v in self.iter_components()):
            return 'invalid'
        elif self.validation_attempted != 'full':
            return 'notKnown'
        else:
            return 'valid'

    def iter_globals(self) -> Iterator[SchemaGlobalType]:
        """Iterates XSD global definitions/declarations of the schema."""
        def schema_filter(comp: XsdComponent) -> bool:
            return comp.schema is self

        yield from filter(schema_filter, self.maps.iter_globals())

    def iter_staged(self) -> Iterator[StagedItemType]:
        """Iterates the unbuilt XSD global definitions/declarations of the schema."""
        def schema_filter(x: StagedItemType) -> bool:
            return x[1] is self if len(x) == 2 else x[0][1] is self

        yield from filter(schema_filter, self.maps.iter_staged())

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator[Union[XsdComponent, SchemaType]]:
        """
        Iterates yielding the schema and its components. For default
        includes all the relevant components of the schema, excluding
        only facets and empty attribute groups. The first returned
        component is the schema itself.

        :param xsd_classes: provide a class or a tuple of classes to \
        restrict the range of component types yielded.
        """
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self
        for xsd_global in self.iter_globals():
            if not isinstance(xsd_global, tuple):
                yield from xsd_global.iter_components(xsd_classes)

    @cached_property
    def validation_context(self) -> ValidationContext:
        """Returns a validation context instance used for decoding schema simple values."""
        return ValidationContext(
            source=self.source,
            converter=NamespaceMapper(self.namespaces),
        )

    def get_converter(self, converter: Optional[ConverterType] = None,
                      **kwargs: Any) -> XMLSchemaConverter:
        """
        Returns a new converter instance.

        :param converter: can be a converter class or instance. If not provided the \
        converter settings option of the schema instance is used.
        :param kwargs: optional arguments for initialize the converter instance.
        :return: a converter instance.
        """
        return self.maps.settings.get_converter(converter, **kwargs)

    def get_locations(self, namespace: str) -> list[str]:
        """Get a list of location hints for a namespace."""
        return self.maps.loader.get_locations(namespace)

    def get_schema(self, namespace: str) -> SchemaType:
        """
        Returns the first schema loaded for a namespace. Raises a
        `KeyError` if the requested namespace is not loaded.
        """
        try:
            return self.maps.namespaces[namespace][0]
        except KeyError:
            if not namespace:
                return self
            msg = _('the namespace {!r} is not loaded')
            raise XMLSchemaKeyError(msg.format(namespace)) from None

    def get_element(self, tag: str, path: Optional[str] = None,
                    namespaces: Optional[NsmapType] = None) -> Optional[XsdElement]:
        if not path or path == tag or path == f'/{tag}':
            return self.maps.elements.get(tag)
        elif path[-1] == '*':
            xsd_element = self.find(path[:-1] + tag, namespaces)
            if isinstance(xsd_element, XsdElement):
                return xsd_element
            else:
                return self.maps.elements.get(tag)
        else:
            xsd_element = self.find(path, namespaces)
            if not isinstance(xsd_element, XsdElement):
                return None
            elif xsd_element.name != tag:
                return self.maps.elements.get(tag)
            else:
                return xsd_element

    def create_bindings(self, *bases: type, **attrs: Any) -> None:
        """
        Creates data object bindings for XSD elements of the schema.

        :param bases: base classes to use for creating the binding classes.
        :param attrs: attribute and method definitions for the binding classes body.
        """
        for xsd_component in self.iter_components():
            if isinstance(xsd_component, XsdElement):
                xsd_component.get_binding(*bases, replace_existing=True, **attrs)

    def include_schema(self, location: str, base_url: Optional[str] = None,
                       build: bool = False, partial: bool = False) -> SchemaType:
        """
        Includes a schema for the same namespace, from a specific URL.

        :param location: is the URL of the schema.
        :param base_url: is an optional base URL for fetching the schema resource.
        :param build: defines when to build the imported schema, the default is to not build.
        :return: the included :class:`XMLSchema` instance.
        :param partial: if `True`, the included schema is initialized without processing \
        imports/inclusions and the build phase is skipped.
        :return: the included :class:`XMLSchema` instance.
        """
        return self.maps.loader.include_schema(self, location, base_url, build, partial)

    def import_schema(self, namespace: str,
                      location: str,
                      base_url: Optional[str] = None,
                      force: bool = False,
                      build: bool = False,
                      partial: bool = False) -> Optional[SchemaType]:
        """
        Imports a schema for an external namespace from a specific location.

        :param namespace: is the URI of the external namespace.
        :param location: is the URL of the schema.
        :param base_url: is an optional base URL for fetching the schema resource.
        :param force: if set to `True` imports the schema also if the namespace \
        is already imported.
        :param build: defines when to build the imported schema, the default is to not build.
        :param partial: if `True`, the imported schema is initialized without processing \
        imports/inclusions and the build phase is skipped.
        :return: the imported :class:`XMLSchema` instance or `None` if a schema \
        can't be imported from that location.
        """
        if namespace not in self.maps.namespaces:
            return self.maps.loader.import_schema(
                self, namespace, location, base_url, build, partial
            )
        elif not force:
            return self.maps.namespaces[namespace][0]
        else:
            return self.maps.loader.load_schema(location, namespace, base_url, build, partial)

    def add_schema(self, source: SourceArgType,
                   namespace: Optional[str] = None,
                   base_url: Optional[str] = None,
                   build: bool = False,
                   partial: bool = False) -> SchemaType:
        """
        Add another schema source to the maps of the instance without affecting imports or
        includes registrations.

        :param source: a URI that reference to a resource or a file path or a file-like \
        object or a string containing the schema or an Element or an ElementTree document.
        :param namespace: is an optional argument that contains the URI of the namespace \
        that has to used in case the schema has no namespace (chameleon schema). It must \
        be equal to the *targetNamespace* of the schema. If not provided, the resource is \
        examined and if the schema has no namespace it's added as a chameleon schema.
        :param base_url: is an optional base URL for fetching the schema resource.
        :param build: defines when to build the imported schema, the default is to not build.
        :param partial: if `True`, the added schema is initialized without processing \
        imports/inclusions and the build phase is skipped.
        :return: the added :class:`XMLSchema` instance.
        """
        return self.maps.loader.load_schema(source, namespace, base_url, build, partial)

    def load_namespace(self, namespace: str, build: bool = True) -> bool:
        """
        Load namespace from available location hints. Returns `True` if the namespace
        is already loaded or if the namespace can be loaded from one of the locations,
        returns `False` otherwise. Failing locations are inserted into the missing
        locations list.

        :param namespace: the namespace to load.
        :param build: if left with `True` value builds the maps after load. If the \
        build fails the resource URL is added to missing locations.
        """
        return self.maps.loader.load_namespace(namespace, build)

    def export(self, target: Union[str, Path],
               save_remote: bool = False,
               remove_residuals: bool = True,
               exclude_locations: Optional[list[str]] = None,
               loglevel: Optional[Union[str, int]] = None) -> dict[str, str]:
        """
        Exports a schema instance. The schema instance is exported to a
        directory with also the hierarchy of imported/included schemas.

        :param target: a path to a local empty directory.
        :param save_remote: if `True` is provided saves also remote schemas.
        :param remove_residuals: for default removes residual remote schema \
        locations from redundant import statements.
        :param exclude_locations: explicitly exclude schema locations from \
        substitution or removal.
        :param loglevel: for setting a different logging level for schema export.
        :return: a dictionary containing the map of modified locations.
        """
        return export_schema(
            schema=self,
            target=target,
            save_remote=save_remote,
            remove_residuals=remove_residuals,
            exclude_locations=exclude_locations,
            loglevel=loglevel
        )

    def version_check(self, elem: Element) -> bool:
        """
        Checks if the element is compatible with the version of the validator and XSD
        types/facets availability. Invalid vc attributes are not detected in XSD 1.0.

        :param elem: an Element of the schema.
        :return: `True` if the schema element is compatible with the validator, \
        `False` otherwise.
        """
        if nm.VC_MIN_VERSION in elem.attrib:
            vc_min_version = elem.attrib[nm.VC_MIN_VERSION]
            if not XSD_VERSION_PATTERN.match(vc_min_version):
                if self.XSD_VERSION > '1.0':
                    msg = _("invalid attribute vc:minVersion value")
                    self.parse_error(msg, elem)
            elif vc_min_version > self.XSD_VERSION:
                return False

        if nm.VC_MAX_VERSION in elem.attrib:
            vc_max_version = elem.attrib[nm.VC_MAX_VERSION]
            if not XSD_VERSION_PATTERN.match(vc_max_version):
                if self.XSD_VERSION > '1.0':
                    msg = _("invalid attribute vc:maxVersion value")
                    self.parse_error(msg, elem)
            elif vc_max_version <= self.XSD_VERSION:
                return False

        if nm.VC_TYPE_AVAILABLE in elem.attrib:
            for qname in elem.attrib[nm.VC_TYPE_AVAILABLE].split():
                try:
                    if self.resolve_qname(qname) not in self.maps.types:
                        return False
                except XMLSchemaNamespaceError:
                    return False
                except (KeyError, ValueError) as err:
                    self.parse_error(str(err), elem)

        if nm.VC_TYPE_UNAVAILABLE in elem.attrib:
            for qname in elem.attrib[nm.VC_TYPE_UNAVAILABLE].split():
                try:
                    if self.resolve_qname(qname) not in self.maps.types:
                        break
                except XMLSchemaNamespaceError:
                    break
                except (KeyError, ValueError) as err:
                    self.parse_error(err, elem)
            else:
                return False

        if nm.VC_FACET_AVAILABLE in elem.attrib:
            for qname in elem.attrib[nm.VC_FACET_AVAILABLE].split():
                try:
                    facet_name = self.resolve_qname(qname)
                except XMLSchemaNamespaceError:
                    pass
                except (KeyError, ValueError) as err:
                    self.parse_error(str(err), elem)
                else:
                    if facet_name not in self.builders.facets:
                        return False

        if nm.VC_FACET_UNAVAILABLE in elem.attrib:
            for qname in elem.attrib[nm.VC_FACET_UNAVAILABLE].split():
                try:
                    facet_name = self.resolve_qname(qname)
                except XMLSchemaNamespaceError:
                    break
                except (KeyError, ValueError) as err:
                    self.parse_error(err, elem)
                else:
                    if facet_name not in self.builders.facets:
                        break
            else:
                return False

        return True

    def resolve_qname(self, qname: str, namespace_imported: bool = True) -> str:
        """
        QName resolution for a schema instance.

        :param qname: a string in xs:QName format.
        :param namespace_imported: if this argument is `True` raises an \
        `XMLSchemaNamespaceError` if the namespace of the QName is not the \
        *targetNamespace* and the namespace is not imported by the schema.
        :returns: an expanded QName in the format "{*namespace-URI*}*local-name*".
        :raises: `XMLSchemaValueError` for an invalid xs:QName is found, \
        `XMLSchemaKeyError` if the namespace prefix is not declared in the \
        schema instance.
        """
        qname = qname.strip()
        if not qname or ' ' in qname or '\t' in qname or '\n' in qname:
            msg = _("{!r} is not a valid value for xs:QName")
            raise XMLSchemaValueError(msg.format(qname))

        if qname[0] == '{':
            try:
                namespace, local_name = qname[1:].split('}')
            except ValueError:
                msg = _("{!r} is not a valid value for xs:QName")
                raise XMLSchemaValueError(msg.format(qname))
        else:
            qname_validator(qname)
            if ':' in qname:
                prefix, local_name = qname.split(':')
                try:
                    namespace = self.namespaces[prefix]
                except KeyError:
                    msg = _("prefix {!r} not found in namespace map")
                    raise XMLSchemaKeyError(msg.format(prefix))
            else:
                namespace, local_name = self.namespaces.get('', ''), qname

        if not namespace:
            if namespace_imported and self.target_namespace \
                    and '' not in self.imported_namespaces:
                msg = _("the QName {!r} is mapped to no namespace, but this requires "
                        "that there is an xs:import statement in the schema without "
                        "the 'namespace' attribute.")
                raise XMLSchemaNamespaceError(msg.format(qname))
            return local_name
        elif namespace_imported and self.meta_schema is not None and \
                namespace != self.target_namespace and \
                namespace not in (nm.XSD_NAMESPACE, nm.XSI_NAMESPACE) and \
                namespace not in self.imported_namespaces:
            msg = _("the QName {!r} is mapped to the namespace {!r}, but this "
                    "namespace has not an xs:import statement in the schema.")
            raise XMLSchemaNamespaceError(msg.format(qname, namespace))

        return f'{{{namespace}}}{local_name}'

    def validate(self, source: Union[XMLSourceType, XMLResource],
                 path: Optional[str] = None,
                 schema_path: Optional[str] = None,
                 use_defaults: bool = True,
                 namespaces: Optional[NsmapType] = None,
                 max_depth: Optional[int] = None,
                 extra_validator: Optional[ExtraValidatorType] = None,
                 validation_hook: Optional[ValidationHookType] = None,
                 allow_empty: bool = True,
                 use_location_hints: bool = False) -> None:
        """
        Validates an XML data against the XSD schema/component instance.

        :param source: the source of XML data. Can be an :class:`XMLResource` instance, a \
        path to a file or a URI of a resource or an opened file-like object or an Element \
        instance or an ElementTree instance or a string containing the XML data.
        :param path: is an optional XPath expression that matches the elements of the XML \
        data that have to be decoded. If not provided the XML root element is selected.
        :param schema_path: an alternative XPath expression to select the XSD element \
        to use for decoding. Useful if the root of the XML data doesn't match an XSD \
        global element of the schema.
        :param use_defaults: Use schema's default values for filling missing data.
        :param namespaces: is an optional mapping from namespace prefix to URI.
        :param max_depth: maximum level of validation, for default there is no limit. \
        With lazy resources is set to `source.lazy_depth` for managing lazy validation.
        :param extra_validator: an optional function for performing non-standard \
        validations on XML data. The provided function is called for each traversed \
        element, with the XML element as 1st argument and the corresponding XSD \
        element as 2nd argument. It can be also a generator function and has to \
        raise/yield :exc:`XMLSchemaValidationError` exceptions.
        :param validation_hook: an optional function for stopping or changing \
        validation at element level. The provided function must accept two arguments, \
        the XML element and the matching XSD element. If the value returned by this \
        function is evaluated to false then the validation process continues without \
        changes, otherwise the validation process is stopped or changed. If the value \
        returned is a validation mode the validation process continues changing the \
        current validation mode to the returned value, otherwise the element and its \
        content are not processed. The function can also stop validation suddenly \
        raising a `XmlSchemaStopValidation` exception.
        :param allow_empty: for default providing a path argument empty selections \
        of XML data are allowed. Provide `False` to generate a validation error.
        :param use_location_hints: for default schema locations hints provided within \
        XML data are ignored in order to avoid the change of schema instance. Set this \
        option to `True` to activate dynamic schema loading using schema location hints.
        :raises: :exc:`XMLSchemaValidationError` if the XML data instance is invalid.
        """
        for error in self.iter_errors(source, path, schema_path, use_defaults,
                                      namespaces, max_depth, extra_validator,
                                      validation_hook, allow_empty, use_location_hints,
                                      validation='strict'):
            raise error

    def is_valid(self, source: Union[XMLSourceType, XMLResource],
                 path: Optional[str] = None,
                 schema_path: Optional[str] = None,
                 use_defaults: bool = True,
                 namespaces: Optional[NsmapType] = None,
                 max_depth: Optional[int] = None,
                 extra_validator: Optional[ExtraValidatorType] = None,
                 validation_hook: Optional[ValidationHookType] = None,
                 allow_empty: bool = True,
                 use_location_hints: bool = False) -> bool:
        """
        Like :meth:`validate` except that does not raise an exception but returns
        ``True`` if the XML data instance is valid, ``False`` if it is invalid.
        """
        error = next(self.iter_errors(source, path, schema_path, use_defaults,
                                      namespaces, max_depth, extra_validator,
                                      validation_hook, allow_empty, use_location_hints), None)
        return error is None

    def iter_errors(self, source: Union[XMLSourceType, XMLResource],
                    path: Optional[str] = None,
                    schema_path: Optional[str] = None,
                    use_defaults: bool = True,
                    namespaces: Optional[NsmapType] = None,
                    max_depth: Optional[int] = None,
                    extra_validator: Optional[ExtraValidatorType] = None,
                    validation_hook: Optional[ValidationHookType] = None,
                    allow_empty: bool = True,
                    use_location_hints: bool = False,
                    validation: str = 'lax') \
            -> Iterator[XMLSchemaValidationError]:
        """
        Creates an iterator for the errors generated by the validation of an XML data against
        the XSD schema/component instance. Accepts the same arguments of :meth:`validate`.
        """
        self.check_validator(validation='lax')
        resource = self.maps.settings.get_xml_resource(source)
        context = ValidationContext(
            source=resource,
            converter=NamespaceMapper(namespaces, source=resource),
            level=resource.lazy_depth or bool(path),
            check_identities=True,
            use_defaults=use_defaults,
            use_location_hints=use_location_hints,
            max_depth=max_depth,
            extra_validator=extra_validator,
            validation_hook=validation_hook,
        )

        namespaces = context.namespaces
        identities = context.identities
        ancestors: list[Element] = []
        prev_ancestors: list[Element] = []

        namespace = resource.namespace or namespaces.get('', '')
        try:
            schema = self.get_schema(namespace)
        except KeyError:
            schema = self

        if not schema_path:
            schema_path = resource.get_absolute_path(path)

        if path:
            selector = resource.iterfind(path, namespaces, ancestors=ancestors)
        else:
            selector = resource.iter_depth(mode=4, ancestors=ancestors)

        elem: Optional[Element] = None
        for elem in selector:
            if elem is resource.root:
                if resource.lazy_depth:
                    context.level = 0
                    context.identities = {}
                    context.max_depth = resource.lazy_depth
            else:
                if prev_ancestors != ancestors:
                    k = 0
                    for k in range(min(len(ancestors), len(prev_ancestors))):
                        if ancestors[k] is not prev_ancestors[k]:
                            break

                    path_ = f"{'/'.join(e.tag for e in ancestors)}/ancestor-or-self::node()"
                    xsd_ancestors = cast(list[XsdElement],
                                         schema.findall(path_, namespaces)[1:])

                    # Clear identity constraints counters
                    for k, e in enumerate(xsd_ancestors[k:], start=k):
                        for identity in e.identities:
                            if identity in identities:
                                identities[identity].reset(ancestors[k])
                            else:
                                identities[identity] = identity.get_counter(ancestors[k])

                    prev_ancestors = ancestors[:]

            xsd_element = schema.get_element(elem.tag, schema_path, namespaces)
            if xsd_element is None:
                if nm.XSI_TYPE in elem.attrib:
                    xsd_element = self.builders.create_element(elem.tag, self)
                elif elem is not resource.root and ancestors:
                    continue
                else:
                    yield context.missing_element_error(validation, self, elem, path, schema_path)
                    return

            try:
                xsd_element.raw_decode(elem, validation, context)
            except XMLSchemaStopValidation:
                pass

            yield from context.errors
            context.errors.clear()
        else:
            if elem is None and not allow_empty:
                assert path is not None
                reason = _("the provided path selects nothing to validate")
                yield context.validation_error(validation, self, reason)
                return

        if context.identities is not identities:
            for identity, counter in context.identities.items():
                identities[identity].counter.update(counter.counter)
            context.identities = identities

        yield from self._validate_references(validation, context)

    def _validate_references(self, validation: str, context: ValidationContext) \
            -> Iterator[XMLSchemaValidationError]:
        # Check unresolved IDREF values
        for k, v in context.id_map.items():
            if v == 0:
                msg = _("IDREF %r not found in XML document") % k
                yield context.validation_error(validation, self, msg, context.source.root)

        # Check still enabled key references (lazy validation cases)
        for identity, counter in context.identities.items():
            if counter.enabled and isinstance(identity, XsdKeyref):
                for error in cast(KeyrefCounter, counter).iter_errors(context.identities):
                    yield context.validation_error(validation, self, error, context.source.root)

    def raw_decoder(self, source: Union[XMLSourceType, XMLResource],
                    path: Optional[str] = None,
                    schema_path: Optional[str] = None,
                    validation: str = 'lax',
                    **kwargs: Any) -> Iterator[Union[Any, XMLSchemaValidationError]]:
        """Returns a generator for decoding a resource."""
        kwargs['source'] = self.maps.settings.get_xml_resource(source)
        context = DecodeContext(**kwargs)
        if path:
            selector = context.source.iterfind(path, context.namespaces)
        else:
            selector = context.source.iter_depth(mode=2)

        for elem in selector:
            xsd_element = self.get_element(elem.tag, schema_path, context.namespaces)
            if xsd_element is None:
                if nm.XSI_TYPE in elem.attrib:
                    xsd_element = self.builders.create_element(elem.tag, self)
                else:
                    yield context.missing_element_error(validation, self, elem, path, schema_path)
                    continue

            result = xsd_element.raw_decode(elem, validation, context)
            if context.errors:
                yield from context.errors
                context.errors.clear()
            if result is not Empty:
                yield result

        if context.max_depth is None:
            yield from self._validate_references(validation, context)

    def iter_decode(self, source: Union[XMLSourceType, XMLResource],
                    path: Optional[str] = None,
                    schema_path: Optional[str] = None,
                    validation: str = 'lax',
                    process_namespaces: bool = True,
                    namespaces: Optional[NsmapType] = None,
                    use_defaults: bool = True,
                    use_location_hints: bool = False,
                    decimal_type: Optional[type[Any]] = None,
                    datetime_types: bool = False,
                    binary_types: bool = False,
                    converter: Optional[ConverterType] = None,
                    filler: Optional[FillerType] = None,
                    fill_missing: bool = False,
                    keep_empty: bool = False,
                    keep_unknown: bool = False,
                    process_skipped: bool = False,
                    max_depth: Optional[int] = None,
                    depth_filler: Optional[DepthFillerType] = None,
                    extra_validator: Optional[ExtraValidatorType] = None,
                    validation_hook: Optional[ValidationHookType] = None,
                    value_hook: Optional[ValueHookType] = None,
                    element_hook: Optional[ElementHookType] = None,
                    errors: Optional[list[XMLSchemaValidationError]] = None,
                    **kwargs: Any) -> Iterator[Union[Any, XMLSchemaValidationError]]:
        """
        Creates an iterator for decoding an XML source to a data structure.

        :param source: the source of XML data. Can be an :class:`XMLResource` instance, a \
        path to a file or a URI of a resource or an opened file-like object or an Element \
        instance or an ElementTree instance or a string containing the XML data.
        :param path: is an optional XPath expression that matches the elements of the XML \
        data that have to be decoded. If not provided the XML root element is selected.
        :param schema_path: an alternative XPath expression to select the XSD element \
        to use for decoding. Useful if the root of the XML data doesn't match an XSD \
        global element of the schema.
        :param validation: defines the XSD validation mode to use for decode, can be \
        'strict', 'lax' or 'skip'.
        :param process_namespaces: whether to use namespace information in the \
        decoding process, using the map provided with the argument *namespaces* \
        and the namespace declarations extracted from the XML document.
        :param namespaces: is an optional mapping from namespace prefix to URI that \
        integrate/override the root namespace declarations of the XML source. \
        In case of prefix collision an alternate prefix is used for the root \
        XML namespace declaration.
        :param use_defaults: whether to use default values for filling missing data.
        :param use_location_hints: for default schema locations hints provided within \
        XML data are ignored in order to avoid the change of schema instance. Set this \
        option to `True` to activate dynamic schema loading using schema location hints.
        :param decimal_type: conversion type for `Decimal` objects (generated by \
        `xs:decimal` built-in and derived types), useful if you want to generate a \
        JSON-compatible data structure.
        :param datetime_types: if set to `True` the datetime and duration XSD types \
        are kept decoded, otherwise their origin XML string is returned.
        :param binary_types: if set to `True` xs:hexBinary and xs:base64Binary types \
        are kept decoded, otherwise their origin XML string is returned.
        :param converter: an :class:`XMLSchemaConverter` subclass or instance to use \
        for decoding.
        :param filler: an optional callback function to fill undecodable data with a \
        typed value. The callback function must accept one positional argument, that \
        can be an XSD Element or an attribute declaration. If not provided undecodable \
        data is replaced by `None`.
        :param fill_missing: if set to `True` the decoder fills also missing attributes. \
        The filling value is `None` or a typed value if the *filler* callback is provided.
        :param keep_empty: if set to `True` empty elements that are valid are decoded with \
        an empty string value instead of a `None`.
        :param keep_unknown: if set to `True` unknown tags are kept and are decoded with \
        *xs:anyType*. For default unknown tags not decoded by a wildcard are discarded.
        :param process_skipped: process XML data that match a wildcard with \
        `processContents='skip'`.
        :param max_depth: maximum level of decoding, for default there is no limit. \
        With lazy resources is set to `source.lazy_depth` for managing lazy decoding.
        :param depth_filler: an optional callback function to replace data over the \
        *max_depth* level. The callback function must accept one positional argument, that \
        can be an XSD Element. If not provided deeper data are replaced with `None` values.
        :param extra_validator: an optional function for performing non-standard \
        validations on XML data. The provided function is called for each traversed \
        element, with the XML element as 1st argument and the corresponding XSD \
        element as 2nd argument. It can be also a generator function and has to \
        raise/yield :exc:`XMLSchemaValidationError` exceptions.
        :param validation_hook: an optional function for stopping or changing \
        validated decoding at element level. The provided function must accept two \
        arguments, the XML element and the matching XSD element. If the value returned \
        by this function is evaluated to false then the decoding process continues \
        without changes, otherwise the decoding process is stopped or changed. If the \
        value returned is a validation mode the decoding process continues changing the \
        current validation mode to the returned value, otherwise the element and its \
        content are not decoded.
        :param value_hook: an optional function that will be called with any decoded \
        atomic value and the XSD type used for decoding. The return value will be used \
        instead of the original value.
        :param element_hook: an optional function that is called with decoded element \
        data before calling the converter decode method. Takes an `ElementData` \
        instance plus optionally the XSD element and the XSD type, and returns a \
        new `ElementData` instance.
        :param errors: optional internal collector for validation errors.
        :param kwargs: keyword arguments with other options for building converter instances.
        :return: yields a decoded data object, eventually preceded by a sequence of \
        validation or decoding errors.
        """
        self.check_validator(validation)
        resource = self.maps.settings.get_xml_resource(source)
        kwargs.update(
            process_namespaces=process_namespaces,
            namespaces=namespaces,
            check_identities=True,
            use_defaults=use_defaults,
            use_location_hints=use_location_hints,
            decimal_type=decimal_type,
            datetime_types=datetime_types,
            binary_types=binary_types,
            converter=converter,
            filler=filler,
            fill_missing=fill_missing,
            keep_empty=keep_empty,
            keep_unknown=keep_unknown,
            process_skipped=process_skipped,
            max_depth=max_depth,
            depth_filler=depth_filler,
            extra_validator=extra_validator,
            validation_hook=validation_hook,
            value_hook=value_hook,
            element_hook=element_hook,
            errors=errors
        )
        kwargs['converter'] = self.maps.settings.get_converter(source=resource, **kwargs)
        context = DecodeContext(source=resource, **kwargs)
        namespaces = context.namespaces

        namespace = resource.namespace or namespaces.get('', '')
        schema = self.get_schema(namespace)

        if path:
            selector = resource.iterfind(path, namespaces)
            if not schema_path:
                schema_path = resource.get_absolute_path(path)

        elif not resource.is_lazy():
            selector = iter((resource.root,))
        else:
            decoder = self.raw_decoder(
                source=resource,
                schema_path=resource.get_absolute_path(),
                validation=validation,
                **kwargs
            )
            context.depth_filler = lambda x: decoder
            context.max_depth = resource.lazy_depth
            selector = resource.iter_depth(mode=3)

        yielded_errors = 0

        for elem in selector:
            xsd_element = schema.get_element(elem.tag, schema_path, namespaces)
            if xsd_element is None:
                if nm.XSI_TYPE in elem.attrib:
                    xsd_element = self.builders.create_element(elem.tag, self)
                else:
                    yield context.missing_element_error(validation, self, elem, path, schema_path)
                    return

            result = xsd_element.raw_decode(elem, validation, context)

            if errors is not context.errors:
                yield from context.errors
                context.errors.clear()
            elif len(context.errors) > yielded_errors:
                yield from context.errors[yielded_errors:]
                yielded_errors = len(context.errors)

            if result is not Empty:
                yield result

        if context.max_depth is not None:
            yield from self._validate_references(validation, context)

    def decode(self, source: Union[XMLSourceType, XMLResource],
               path: Optional[str] = None,
               schema_path: Optional[str] = None,
               validation: str = 'strict',
               *args: Any, **kwargs: Any) -> DecodeType[Any]:
        """
        Decodes XML data. Takes the same arguments of the method :meth:`iter_decode`.
        """
        data, errors = [], []
        for result in self.iter_decode(source, path, schema_path, validation, *args, **kwargs):
            if not isinstance(result, XMLSchemaValidationError):
                data.append(result)
            elif validation == 'lax':
                errors.append(result)
            elif validation == 'strict':
                raise result

        if not data:
            return (None, errors) if validation == 'lax' else None
        elif len(data) == 1:
            return (data[0], errors) if validation == 'lax' else data[0]
        else:
            return (data, errors) if validation == 'lax' else data

    to_dict = decode

    def to_objects(self, source: Union[XMLSourceType, XMLResource], with_bindings: bool = False,
                   **kwargs: Any) -> DecodeType['dataobjects.DataElement']:
        """
        Decodes XML data to Python data objects.

        :param source: the XML data. Can be a string for an attribute or for a simple \
        type components or a dictionary for an attribute group or an ElementTree's \
        Element for other components.
        :param with_bindings: if `True` is provided the decoding is done using \
        :class:`DataBindingConverter` that used XML data binding classes. For \
        default the objects are instances of :class:`DataElement` and uses the \
        :class:`DataElementConverter`.
        :param kwargs: other optional keyword arguments for the method \
        :func:`iter_decode`, except the argument *converter*.
        """
        if with_bindings:
            return self.decode(source, converter=dataobjects.DataBindingConverter, **kwargs)
        return self.decode(source, converter=dataobjects.DataElementConverter, **kwargs)

    def iter_encode(self, obj: Any,
                    path: Optional[str] = None,
                    validation: str = 'lax',
                    namespaces: Optional[NsmapType] = None,
                    use_defaults: bool = True,
                    converter: Optional[ConverterType] = None,
                    unordered: bool = False,
                    process_skipped: bool = False,
                    max_depth: Optional[int] = None,
                    untyped_data: bool = False,
                    etree_element_class: Optional[type[ElementType]] = None,
                    **kwargs: Any) -> Iterator[Union[Element, XMLSchemaValidationError]]:
        """
        Creates an iterator for encoding a data structure to an ElementTree's Element.

        :param obj: the data that has to be encoded to XML data.
        :param path: is an optional XPath expression for selecting the element of \
        the schema that matches the data that has to be encoded. For default the first \
        global element of the schema is used.
        :param validation: the XSD validation mode. Can be 'strict', 'lax' or 'skip'.
        :param namespaces: is an optional mapping from namespace prefix to URI.
        :param use_defaults: whether to use default values for filling missing data.
        :param converter: an :class:`XMLSchemaConverter` subclass or instance to use for \
        the encoding.
        :param unordered: a flag for explicitly activating unordered encoding mode for \
        content model data. This mode uses content models for a reordered-by-model \
        iteration of the child elements.
        :param process_skipped: process XML decoded data that match a wildcard with \
        `processContents='skip'`.
        :param max_depth: maximum level of encoding, for default there is no limit.
        :param untyped_data: for default xs:untypedAtomic datatype is not accepted as \
        a decoded value, set to true to extend the compatibility of with string and \
        untyped values to all builtin datatypes.
        :param etree_element_class: the class to use for creating new XML elements, \
        if not provided uses the ElementTree's Element class.
        :param kwargs: keyword arguments with other options for building the \
        converter instance.
        :return: yields an Element instance/s or validation/encoding errors.
        """
        self.check_validator(validation)
        if not self.elements:
            msg = _("encoding needs at least one XSD element declaration")
            raise XMLSchemaValueError(msg)

        kwargs.update(
            source=obj,
            namespaces=namespaces,
            check_identities=True,
            use_defaults=use_defaults,
            converter=converter,
            unordered=unordered,
            process_skipped=process_skipped,
            max_depth=max_depth,
            untyped_data=untyped_data,
            etree_element_class=etree_element_class,
        )
        kwargs['converter'] = self.maps.settings.get_converter(**kwargs)
        context = EncodeContext(**kwargs)
        namespaces = context.namespaces

        xsd_element = None
        if path is not None:
            match = re.search(r'[{\w]', path)
            if match:
                namespace = get_namespace_ext(path[match.start():], namespaces)
                schema = self.get_schema(namespace)
                xsd_element = schema.find(path, namespaces)

        elif len(self.elements) == 1:
            xsd_element = list(self.elements.values())[0]
        else:
            root_elements = self.root_elements
            if len(root_elements) == 1:
                xsd_element = root_elements[0]
            elif isinstance(obj, (context.converter.dict_class, dict)) and len(obj) == 1:
                for key in obj:
                    match = re.search(r'[{\w]', key)
                    if match:
                        namespace = get_namespace_ext(key[match.start():], namespaces)
                        schema = self.get_schema(namespace)
                        xsd_element = schema.find(key, namespaces)

        if not isinstance(xsd_element, XsdElement):
            if path is not None:
                reason = _("the path %r doesn't match any element of the schema!") % path
            else:
                reason = _("unable to select an element for encoding data, "
                           "provide a valid 'path' argument.")
            raise XMLSchemaEncodeError(self, obj, self.elements, reason, namespaces=namespaces)
        else:
            result = xsd_element.raw_encode(obj, validation, context)
            if result is None:
                yield from context.errors
            else:
                for e in context.errors:
                    e.root = result
                    yield e
                yield result

            context.errors.clear()

    def encode(self, obj: Any, path: Optional[str] = None, validation: str = 'strict',
               *args: Any, **kwargs: Any) -> EncodeType[Any]:
        """
        Encodes to XML data. Takes the same arguments of the method :meth:`iter_encode`.

        :return: An ElementTree's Element or a list containing a sequence of ElementTree's \
        elements if the argument *path* matches multiple XML data chunks. If *validation* \
        argument is 'lax' a 2-items tuple is returned, where the first item is the encoded \
        object and the second item is a list containing the errors.
        """
        data, errors = [], []
        result: Union[Element, XMLSchemaValidationError]
        for result in self.iter_encode(obj, path, validation, *args, **kwargs):
            if not isinstance(result, XMLSchemaValidationError):
                data.append(result)
            elif validation == 'lax':
                errors.append(result)
            elif validation == 'strict':
                raise result

        if not data:
            return (None, errors) if validation == 'lax' else None
        elif len(data) == 1:
            if errors and is_etree_element(data[0]):
                # Replace decoded data source with an XML resource
                resource = XMLResource(data[0])
                for e in errors:
                    e.source = resource

            return (data[0], errors) if validation == 'lax' else data[0]
        else:
            return (data, errors) if validation == 'lax' else data

    to_etree = encode


class XMLSchema10(XMLSchemaBase):
    """
    XSD 1.0 schema class.

    .. <schema
         attributeFormDefault = (qualified | unqualified) : unqualified
         blockDefault = (#all | List of (extension | restriction | substitution))  : ''
         elementFormDefault = (qualified | unqualified) : unqualified
         finalDefault = (#all | List of (extension | restriction | list | union))  : ''
         id = ID
         targetNamespace = anyURI
         version = token
         xml:lang = language
         {any attributes with non-schema namespace . . .}>
         Content: ((include | import | redefine | annotation)*,  (((simpleType | complexType |
                   group | attributeGroup) | element | attribute | notation), annotation*)*)
       </schema>
    """
    builders = XsdBuilders()

    META_SCHEMA = SCHEMAS_DIR.joinpath('XSD_1.0', 'XMLSchema.xsd')
    BASE_SCHEMAS = {
        nm.XML_NAMESPACE: SCHEMAS_DIR.joinpath('XML', 'xml.xsd'),
        nm.XSI_NAMESPACE: SCHEMAS_DIR.joinpath('XSI', 'XMLSchema-instance.xsd'),
    }


class XMLSchema11(XMLSchemaBase):
    """
    XSD 1.1 schema class.

    .. <schema
         attributeFormDefault = (qualified | unqualified) : unqualified
         blockDefault = (#all | List of (extension | restriction | substitution)) : ''
         defaultAttributes = QName
         xpathDefaultNamespace = (anyURI | (##defaultNamespace | ##targetNamespace|
                                  ##local)) : ##local
         elementFormDefault = (qualified | unqualified) : unqualified
         finalDefault = (#all | List of (extension | restriction | list | union))  : ''
         id = ID
         targetNamespace = anyURI
         version = token
         xml:lang = language
         {any attributes with non-schema namespace . . .}>
         Content: ((include | import | redefine | override | annotation)*,
         (defaultOpenContent, annotation*)?, ((simpleType | complexType |
         group | attributeGroup | element | attribute | notation), annotation*)*)
       </schema>

       <schema
         attributeFormDefault = (qualified | unqualified) : unqualified
         blockDefault = (#all | List of (extension | restriction | substitution))  : ''
         elementFormDefault = (qualified | unqualified) : unqualified
         finalDefault = (#all | List of (extension | restriction | list | union))  : ''
         id = ID
         targetNamespace = anyURI
         version = token
         xml:lang = language
         {any attributes with non-schema namespace . . .}>
         Content: ((include | import | redefine | annotation)*, (((simpleType | complexType |
                   group | attributeGroup) | element | attribute | notation), annotation*)*)
       </schema>
    """
    builders = XsdBuilders()

    XSD_VERSION = '1.1'
    META_SCHEMA = SCHEMAS_DIR.joinpath('XSD_1.1', 'XMLSchema.xsd')
    BASE_SCHEMAS = {
        nm.XML_NAMESPACE: SCHEMAS_DIR.joinpath('XML', 'xml.xsd'),
        nm.XSI_NAMESPACE: SCHEMAS_DIR.joinpath('XSI', 'XMLSchema-instance.xsd'),
        nm.VC_NAMESPACE: SCHEMAS_DIR.joinpath('VC', 'XMLSchema-versioning.xsd'),
        nm.XSD_NAMESPACE: SCHEMAS_DIR.joinpath('XSD_1.1', 'xsd11-extra.xsd'),
    }


XMLSchema = XMLSchema10
"""The default class for schema instances."""

#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import copy
import importlib
import threading
import warnings
from collections.abc import Collection, Iterator, Iterable
from contextlib import contextmanager
from functools import cached_property
from itertools import dropwhile
from typing import Any, cast, Optional
from elementpath import XPathToken, XPath2Parser
from importlib.resources.abc import Traversable

import xmlschema.names as nm
from xmlschema.aliases import SchemaType, BaseXsdType, SchemaGlobalType, \
    SourceArgType, NsmapType, StagedItemType, ComponentClassType
from xmlschema.exceptions import XMLSchemaAttributeError, XMLSchemaTypeError, \
    XMLSchemaValueError, XMLSchemaWarning, XMLSchemaNamespaceError, XMLSchemaException
from xmlschema.translation import gettext as _
from xmlschema.utils.misc import deprecated
from xmlschema.utils.qnames import get_extended_qname
from xmlschema.utils.urls import get_url, normalize_url
from xmlschema.locations import NamespaceResourcesMap
from xmlschema.resources import XMLResource
from xmlschema.xpath import XsdAssertionXPathParser
from xmlschema.settings import SchemaSettings

from .exceptions import XMLSchemaValidatorError, XMLSchemaModelError, \
    XMLSchemaModelDepthError, XMLSchemaParseError
from .xsdbase import XsdValidator, XsdComponent
from .models import check_model
from . import XsdAttribute, XsdSimpleType, XsdComplexType, XsdElement, \
    XsdGroup, XsdIdentity, XsdUnion, XsdAtomicRestriction, \
    XsdAtomic, XsdAtomicBuiltin, XsdNotation, XsdAttributeGroup
from .builders import GLOBAL_MAP_ATTRIBUTE, GlobalMaps, TypesMap, NotationsMap, \
    AttributesMap, AttributeGroupsMap, ElementsMap, GroupsMap
from xmlschema import _limits


# Default placeholder for deprecation of argument 'validation' in XsdGlobals
_strict = type('str', (str,), {})('strict')


class XsdGlobals(XsdValidator, Collection[SchemaType]):
    """
    Mediator collection class for composing XML schema instances and provides lookup maps.
    It stores the global declarations defined in the registered schemas. Register a schema
    to add its declarations to the global maps.

    :param validator: the origin schema class/instance used for creating the global maps.
    :param validation: deprecated argument for the validation mode, now it takes the \
    validation mode of the validator.
    :param parent: an optional parent schema, that is required to be built and with \
    no use of the target namespace of the validator.
    :param loader_class: an optional subclass of :class:`SchemaLoader` to use for creating \
    the loader instance.
    :param locations: schema extra location hints, that can include custom resource locations \
    or additional namespaces to import after processing schema's import statements.
    :param use_fallback: if `True` the schema processor uses the validator fallback \
    location hints to load well-known namespaces (e.g. xhtml).
    :param use_xpath3: if `True` an XSD 1.1 schema instance uses the XPath 3 processor \
    for assertions. For default a full XPath 2.0 processor is used.
    :param kwargs: other keyword arguments passed to :class:`SchemaLoader`.
    """
    _schemas: set[SchemaType]

    settings: SchemaSettings
    namespaces: NamespaceResourcesMap[SchemaType]

    types: TypesMap
    """Global types map"""

    notations: NotationsMap
    """Notations map"""

    attributes: AttributesMap
    """Global attributes map"""

    attribute_groups: AttributeGroupsMap
    """Attribute groups map"""

    elements: ElementsMap
    """Global elements map"""

    groups: GroupsMap
    """Model groups map"""

    substitution_groups: dict[str, set[XsdElement]]
    """Substitution groups map"""

    identities: dict[str, XsdIdentity]
    """Identity constraints map"""

    xpath_parser_class: type[XPath2Parser]
    assertion_parser_class: type[XsdAssertionXPathParser]

    __slots__ = ('_build_lock', '_built', '_schemas', '_parent', 'validation',
                 'errors', 'validator', 'namespaces', 'loader', 'global_maps',
                 'types', 'notations', 'attributes', 'attribute_groups',
                 'elements', 'groups', 'substitution_groups', 'identities',
                 'xpath_parser_class', 'assertion_parser_class', 'settings', 'cache')

    def __init__(self, validator: SchemaType,
                 validation: str = _strict,
                 parent: Optional[SchemaType] = None,
                 settings: Optional[SchemaSettings] = None,
                 **kwargs: Any) -> None:

        if not isinstance(validation, _strict.__class__):
            msg = "argument 'validation' is not used and will be removed in v5.0"
            warnings.warn(msg, DeprecationWarning, stacklevel=1)

        super().__init__(validator.validation)
        self._build_lock = threading.Lock()
        self._built = False
        self._schemas = set()
        self._parent = parent

        self.validator = validator
        self.namespaces = NamespaceResourcesMap()  # Registered schemas by namespace URI

        self.global_maps = GlobalMaps.from_builders(validator.builders)
        (self.types, self.notations, self.attributes,
         self.attribute_groups, self.elements, self.groups) = self.global_maps

        self.substitution_groups = {}
        self.identities = {}

        if isinstance(settings, SchemaSettings):
            self.settings = settings
        else:
            self.settings = SchemaSettings(**kwargs)

        if self.settings.use_xpath3:
            module = importlib.import_module('xmlschema.xpath.xpath3')
            self.xpath_parser_class = module.XPath3Parser
            self.assertion_parser_class = module.XsdAssertionXPath3Parser
        else:
            self.xpath_parser_class = XPath2Parser
            self.assertion_parser_class = XsdAssertionXPathParser

        for ancestor in self.iter_ancestors():
            self._schemas.update(ancestor.maps.schemas)
            self.namespaces.update(ancestor.maps.namespaces)

            ancestor.maps.build()
            self.global_maps.update(ancestor.maps.global_maps)
            self.substitution_groups.update(ancestor.maps.substitution_groups)
            self.identities.update(ancestor.maps.identities)

        self.loader = self.settings.get_loader(self)
        self.cache = self.settings.get_cache()
        self.validator.maps = self

    @property
    def schemas(self) -> set[SchemaType]:
        return self._schemas

    @property
    def owned_schemas(self) -> set[SchemaType]:
        """Returns a set of the registered schemas owned by this instance."""
        return {s for s in self._schemas if s.maps is self}

    @property
    def parent(self) -> Optional[SchemaType]:
        return self._parent

    @cached_property
    def any_type(self) -> 'XsdComplexType':
        return self.validator.create_any_type()

    @cached_property
    def any_simple_type(self) -> 'XsdSimpleType':
        """Property that references to the xs:anySimpleType instance of the global maps."""
        return cast('XsdSimpleType', self.types[nm.XSD_ANY_SIMPLE_TYPE])

    @cached_property
    def any_atomic_type(self) -> 'XsdSimpleType':
        """Property that references to the xs:anyAtomicType instance of the global maps."""
        return cast('XsdSimpleType', self.types[nm.XSD_ANY_ATOMIC_TYPE])

    def __repr__(self) -> str:
        return '%s(validator=%r)' % (self.__class__.__name__, self.validator)

    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(self, name):
            if name == '_built':
                self.__dict__.clear()
            elif name != '_parent' and name != 'settings':
                msg = _("can't change attribute {!r} of a global maps instance")
                raise XMLSchemaAttributeError(msg.format(name))
        super().__setattr__(name, value)

    def __len__(self) -> int:
        return len(self._schemas)

    def __iter__(self) -> Iterator[SchemaType]:
        yield from self._schemas

    def __contains__(self, obj: object) -> bool:
        return obj in self._schemas

    def __getstate__(self) -> dict[str, Any]:
        return {
            a: getattr(self, a) for a in self._mro_slots() if a not in ('_build_lock', 'cache')
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        for attr, value in state.items():
            object.__setattr__(self, attr, value)
        self._build_lock = threading.Lock()
        self.cache = self.settings.get_cache()

    def __copy__(self) -> 'XsdGlobals':
        other = type(self)(
            validator=copy.copy(self.validator),
            parent=self._parent,
            settings=self.settings
        )
        other.loader.__dict__.update(self.loader.__dict__)
        other.loader.locations = self.loader.locations.copy()
        other.loader.missing_locations.update(self.loader.missing_locations)

        other.validator.maps = other
        for schema in self._schemas:
            if schema.maps is self and schema is not self.validator:
                copy.copy(schema).maps = other

        other.clear()
        return other

    copy = __copy__

    def lookup(self, tag: str, qname: str) -> SchemaGlobalType:
        """
        General lookup method for XSD global components.

        :param tag: the expanded QName of the XSD the global declaration/definition \
        (e.g. '{http://www.w3.org/2001/XMLSchema}element'), that is used to select \
        the global map for lookup.
        :param qname: the expanded QName of the component to be looked-up.
        :returns: an XSD global component.
        :raises: an XMLSchemaValueError if the *tag* argument is not appropriate for a global \
        component, an XMLSchemaKeyError if the *qname* argument is not found in the global map.
        """
        try:
            global_map = GLOBAL_MAP_ATTRIBUTE[tag](self)
        except KeyError:
            msg = _("wrong tag {!r} for an XSD global definition/declaration")
            raise XMLSchemaValueError(msg.format(tag)) from None
        else:
            return cast(SchemaGlobalType, global_map[qname])

    @deprecated('5.0', alt='use item lookup on notations instead')
    def lookup_notation(self, qname: str) -> XsdNotation:
        return self.notations[qname]

    @deprecated('5.0', alt='use item lookup on types instead')
    def lookup_type(self, qname: str) -> BaseXsdType:
        return self.types[qname]

    @deprecated('5.0', alt='use item lookup on attributes instead')
    def lookup_attribute(self, qname: str) -> XsdAttribute:
        return self.attributes[qname]

    @deprecated('5.0', alt='use item lookup on attribute_groups instead')
    def lookup_attribute_group(self, qname: str) -> XsdAttributeGroup:
        return self.attribute_groups[qname]

    @deprecated('5.0', alt='use item lookup on groups instead')
    def lookup_group(self, qname: str) -> XsdGroup:
        return self.groups[qname]

    @deprecated('5.0', alt='use item lookup on elements instead')
    def lookup_element(self, qname: str) -> XsdElement:
        return self.elements[qname]

    def get_instance_type(self, type_name: str, base_type: BaseXsdType,
                          namespaces: NsmapType) -> BaseXsdType:
        """
        Returns the instance XSI type from global maps, validating it with the reference base type.

        :param type_name: the XSI type attribute value, a QName in prefixed format.
        :param base_type: the XSD from which the instance type has to be derived.
        :param namespaces: a mapping from prefixes to namespaces.
        """
        if isinstance(base_type, XsdComplexType) and nm.XSI_TYPE in base_type.attributes:
            xsd_attribute = cast(XsdAttribute, base_type.attributes[nm.XSI_TYPE])
            xsd_attribute.validate(type_name)

        extended_name = get_extended_qname(type_name, namespaces)
        xsi_type = self.types[extended_name]
        if xsi_type.is_derived(base_type):
            return xsi_type
        elif isinstance(base_type, XsdSimpleType) and \
                base_type.is_union() and not base_type.facets:
            # Can be valid only if the union doesn't have facets, see:
            #   https://www.w3.org/Bugs/Public/show_bug.cgi?id=4065
            if isinstance(base_type, XsdAtomicRestriction) and \
                    isinstance(base_type.primitive_type, XsdUnion):
                if xsi_type in base_type.primitive_type.member_types:
                    return xsi_type
            elif isinstance(base_type, XsdUnion):
                if xsi_type in base_type.member_types:
                    return xsi_type

        msg = _("{0!r} cannot substitute {1!r}")
        raise XMLSchemaTypeError(msg.format(xsi_type, base_type))

    @property
    def built(self) -> bool:
        return self._built

    @cached_property
    def validation_attempted(self) -> str:
        if not any(m for m in self.global_maps):
            return 'none'
        elif not self._built or any(m.total_staged for m in self.global_maps):
            return 'partial'
        else:
            return 'full'

    @cached_property
    def validity(self) -> str:
        if self.validation == 'skip':
            return 'notKnown'
        elif any(s.errors for s in self._schemas) or \
                any(c.errors for c in self.iter_components()):
            return 'invalid'
        elif not self._built or any(m.total_staged for m in self.global_maps):
            return 'notKnown'
        else:
            return 'valid'

    @cached_property
    def xpath_constructors(self) -> dict[str, type[XPathToken]]:
        if not self._built:
            return {}

        xpath_parser = self.xpath_parser_class()
        xpath_parser.schema = self.validator.xpath_proxy

        constructors: dict[str, type[XPathToken]] = {}
        for name, xsd_type in self.types.items():
            if isinstance(xsd_type, XsdAtomic) and \
                    not isinstance(xsd_type, XsdAtomicBuiltin) and \
                    name not in (nm.XSD_ANY_ATOMIC_TYPE, nm.XSD_NOTATION):
                constructors[name] = xpath_parser.schema_constructor(name)
        return constructors

    @property
    def use_meta(self) -> bool:
        return self.validator.meta_schema is None or \
            self.validator.meta_schema in self._schemas

    @property
    def unbuilt(self) -> list[XsdComponent]:
        """Property that returns a list with unbuilt components."""
        return [c for c in self.iter_components() if not c.built]

    @property
    def xsd_version(self) -> str:
        return self.validator.XSD_VERSION

    @property
    def all_errors(self) -> list[XMLSchemaParseError]:
        return [e for s in self._schemas for e in s.all_errors]

    @property
    def total_errors(self) -> int:
        return sum(s.total_errors for s in self._schemas)

    def create_bindings(self, *bases: type[Any], **attrs: Any) -> None:
        """Creates data object bindings for the XSD elements of built schemas."""
        for xsd_element in self.iter_components(xsd_classes=XsdElement):
            assert isinstance(xsd_element, XsdElement)
            if xsd_element.target_namespace != nm.XSD_NAMESPACE:
                xsd_element.get_binding(*bases, replace_existing=True, **attrs)

    def clear_bindings(self) -> None:
        for xsd_element in self.iter_components(xsd_classes=XsdElement):
            assert isinstance(xsd_element, XsdElement)
            xsd_element.binding = None

    def iter_components(self, xsd_classes: Optional[ComponentClassType] = None) \
            -> Iterator[XsdComponent]:
        """Creates an iterator for the XSD components of built schemas."""
        for xsd_global in self.global_maps.iter_globals():
            yield from xsd_global.iter_components(xsd_classes)

    def iter_globals(self) -> Iterator[SchemaGlobalType]:
        """Creates an iterator for the built XSD global components."""
        return self.global_maps.iter_globals()

    def iter_staged(self) -> Iterator[StagedItemType]:
        """Creates an iterator for the unbuilt XSD global components."""
        return self.global_maps.iter_staged()

    def iter_schemas(self) -> Iterator[SchemaType]:
        """Creates an iterator for the registered schemas."""
        yield from self._schemas

    def iter_ancestors(self) -> Iterator[SchemaType]:
        ancestors: list[SchemaType] = []
        parent = self._parent
        while parent is not None:
            ancestors.append(parent)
            parent = parent.maps.parent

        yield from reversed(ancestors)

    def get_schema(self, namespace: Optional[str] = None,
                   source: Optional[SourceArgType] = None,
                   base_url: Optional[str] = None) -> Optional[SchemaType]:
        schemas: Optional[list[SchemaType]]

        if namespace is not None:
            schemas = self.namespaces.get(namespace)
        elif isinstance(source, XMLResource):
            namespace = source.root.get('targetNamespace', '')
            schemas = self.namespaces.get(namespace)
        elif source is None:
            return None
        else:
            schemas = list(self._schemas)

        if schemas is not None:
            if source is None:
                return schemas[0]
            elif isinstance(source, Traversable):
                for schema in schemas:
                    if schema.source.traversal_source is source:
                        return schema
            elif (url := get_url(source)) is not None:
                url = normalize_url(url, base_url)
                for schema in schemas:
                    if schema.source.match_location(url):
                        return schema

            elif isinstance(source, XMLResource):
                for schema in schemas:
                    if schema.source.source is source.source:
                        return schema
            else:
                for schema in schemas:
                    if schema.source.source is source:
                        return schema

        return None

    def register(self, schema: SchemaType) -> None:
        """Registers an XMLSchema instance."""
        if schema in self._schemas:
            return

        namespace = schema.target_namespace
        source = schema.source.url or schema.source

        ns_schemas = self.namespaces.get(namespace)
        if ns_schemas is None:
            self.namespaces[namespace] = [schema]
            self._schemas.add(schema)
        elif ns_schemas[0].maps is not self:
            self._schemas.add(schema)
            ns_schemas.append(schema)
            schema.maps = self
            self.merge(ancestor=ns_schemas[0].maps.validator)
        elif self.get_schema(namespace, source) is None:
            self._schemas.add(schema)
            ns_schemas.append(schema)
        else:
            source_ref = schema.source.url or type(schema.source)
            raise XMLSchemaValueError(
                f"another schema loaded from {source_ref!r} is already registered"
            )

        if _limits.MAX_SCHEMA_SOURCES < len(self._schemas):
            raise XMLSchemaValidatorError(
                self, f"number of schema sources loaded by {self!r} exceeded"
            )

        self._built = False

    def merge(self, ancestor: SchemaType) -> None:
        """Merge the global maps until to a specific ancestor."""
        self.clear()
        for validator in dropwhile(lambda x: x is not ancestor, self.iter_ancestors()):
            maps = validator.maps
            for schema in maps._schemas:
                if schema.maps is maps:
                    namespace = schema.target_namespace
                    self._schemas.remove(schema)
                    k = self.namespaces[namespace].index(schema)

                    schema = copy.copy(schema)
                    self._schemas.add(schema)
                    self.namespaces[namespace][k] = schema
                    schema.maps = self

        self._parent = ancestor.maps._parent

    def clear(self, remove_schemas: bool = False) -> None:
        """
        Clears the instance maps and schemas.

        :param remove_schemas: removes also the schema instances, keeping only the \
        validator that created the global maps instance and schemas and namespaces \
        inherited from ancestors.
        """
        self.global_maps.clear()
        self.substitution_groups.clear()
        self.identities.clear()

        # Clear maps cache and cached properties of schemas
        self.cache.clear()
        for schema in self._schemas:
            if schema.maps is self:
                schema.clear()

        if remove_schemas:
            self._schemas.clear()
            self.namespaces.clear()

            for ancestor in self.iter_ancestors():
                self._schemas.update(ancestor.maps._schemas)
                self.namespaces.update(ancestor.maps.namespaces)

            self.register(self.validator)
            if self.validator.includes:
                self.validator.includes.clear()

        self._built = False

    def build(self) -> None:
        """
        Build the maps of XSD global definitions/declarations. The global maps are
        updated adding and building the globals of not built registered schemas.
        """
        if self._built:
            return

        with self._build_lock:
            if self._built:
                return

            self.check_loaded_schemas()
            self.clear()

            for ancestor in self.iter_ancestors():
                ancestor.maps.build()
                self.global_maps.update(ancestor.maps.global_maps)
                self.substitution_groups.update(ancestor.maps.substitution_groups)
                self.identities.update(ancestor.maps.identities)

            # Have to respect the insertion order for redefinitions/overrides
            schemas = [s for ns_schemas in self.namespaces.values()
                       for s in ns_schemas if s.maps is self]

            self.global_maps.load(schemas)
            self.types.build_builtins(self.validator)
            self.global_maps.build(schemas)

            # Update substitutes of global elements
            for name in self.substitution_groups:
                xsd_element = self.elements[name]
                assert not isinstance(xsd_element.substitutes, tuple)
                xsd_element.substitutes.update(e.name for e in xsd_element.iter_substitutes())

            self.check(schemas)

            self._built = True
            for s in schemas:
                s.clear()

            self.check_validator()

    @contextmanager
    def protect_status(self, reraise: bool = True) -> Iterator['XsdGlobals']:
        """Context manager for set a restore point in case of error."""
        schemas = self._schemas.copy()
        namespaces = self.namespaces.copy()
        global_maps = self.global_maps.copy()
        substitution_groups = self.substitution_groups.copy()
        identities = self.identities.copy()
        built = self._built

        try:
            yield self
        except XMLSchemaException:
            self.clear()
            self._schemas.clear()
            self.namespaces.clear()
            self._schemas.update(schemas)
            self.namespaces.update(namespaces)
            self.global_maps.update(global_maps)
            self.substitution_groups.update(substitution_groups)
            self.identities.update(identities)
            self._built = built
            if reraise:
                raise

    def check_loaded_schemas(self) -> None:
        """Checks the coherence of schema registrations."""
        if self.validator not in self._schemas:
            raise XMLSchemaValueError(_('global maps main validator is not registered'))

        if self._parent is None:
            self.validator.create_meta_schema(global_maps=self)

        total = 0
        for namespace, schemas in self.namespaces.items():
            total += len(schemas)
            for s in schemas:
                if s.target_namespace != namespace:
                    msg = _('schema {!r} does not belong to namespace {!r}')
                    raise XMLSchemaNamespaceError(msg.format(s, namespace))
                elif s not in self._schemas:
                    msg = _('schema {!r} does not belong to {!r}')
                    raise XMLSchemaNamespaceError(msg.format(s, self))

        if len(self._schemas) != total:
            raise XMLSchemaValueError(
                _('registered schemas do not match namespace mapped schemas')
            )

    def check(self, schemas: Optional[Iterable[SchemaType]] = None) -> None:
        """
        Checks the components of provided schemas. Used after the build of the global maps.
        For default checks all schemas and raises an exception at first error.

        :param schemas: optional argument with the set of the schemas to check.
        :raise: XMLSchemaParseError
        """
        if schemas is None:
            schemas = {s for s in self._schemas if s.maps is self}

        # Checks substitution groups circularity
        for xsd_element in self.elements.values():
            if xsd_element.name in xsd_element.substitutes:
                msg = _("circularity found for substitution group with head element {}")
                xsd_element.parse_error(msg.format(xsd_element))

        # Check redefined global groups restrictions
        for group in self.groups.values():
            assert isinstance(group, XsdGroup), _("global group not built!")
            if group.schema not in schemas or group.redefine is None:
                continue

            while group.redefine is not None:
                if not any(isinstance(e, XsdGroup) and e.name == group.name for e in group) \
                        and not group.is_restriction(group.redefine):
                    msg = _("the redefined group is an illegal restriction")
                    group.parse_error(msg)

                group = group.redefine

        # Check complex content types models restrictions
        for xsd_global in filter(lambda x: x.schema in schemas, self.iter_globals()):
            xsd_type: Any
            for xsd_type in xsd_global.iter_components(XsdComplexType):
                if not isinstance(xsd_type.content, XsdGroup):
                    continue

                if xsd_type.derivation == 'restriction':
                    base_type = xsd_type.base_type
                    if base_type and base_type.name != nm.XSD_ANY_TYPE and base_type.is_complex():
                        if not xsd_type.content.is_restriction(base_type.content):
                            msg = _("the derived group is an illegal restriction")
                            xsd_type.parse_error(msg)

                    if base_type.is_complex() and not base_type.open_content and \
                            xsd_type.open_content and xsd_type.open_content.mode != 'none':
                        _group = xsd_type.schema.builders.create_any_content_group(
                            parent=xsd_type,
                            any_element=xsd_type.open_content.any_element
                        )
                        if not _group.is_restriction(base_type.content):
                            msg = _("restriction has an open content but base type has not")
                            _group.parse_error(msg)

                try:
                    check_model(xsd_type.content)
                except XMLSchemaModelDepthError:
                    msg = _("can't verify the content model of {!r} "
                            "due to exceeding of maximum recursion depth")
                    xsd_type.schema.warnings.append(msg.format(xsd_type))
                    warnings.warn(msg, XMLSchemaWarning, stacklevel=4)
                except XMLSchemaModelError as err:
                    if self.validation == 'strict':
                        raise
                    xsd_type.errors.append(err)

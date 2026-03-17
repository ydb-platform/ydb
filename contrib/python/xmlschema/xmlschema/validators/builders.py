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
from abc import abstractmethod
from collections import Counter
from collections.abc import Callable, ItemsView, Iterator, Mapping, ValuesView, Iterable
from operator import attrgetter
from types import MappingProxyType
from typing import Any, cast, NamedTuple, Optional, Union, TypeVar
from xml.etree.ElementTree import Element

import xmlschema.names as nm
from xmlschema.aliases import BaseXsdType, ElementType, LoadedItemType, \
    SchemaType, StagedItemType, SchemaGlobalType
from xmlschema.exceptions import XMLSchemaAttributeError, XMLSchemaKeyError, \
    XMLSchemaTypeError, XMLSchemaValueError
from xmlschema.translation import gettext as _
from xmlschema.utils.qnames import local_name, get_qname

from .helpers import parse_xsd_derivation
from .exceptions import XMLSchemaCircularityError, XMLSchemaModelDepthError
from .xsdbase import XsdComponent, XsdAnnotation
from .builtins import BUILTIN_TYPES
from .facets import XsdFacet, FACETS_CLASSES, XSD_10_FACETS, XSD_11_FACETS, \
    XSD_11_LIST_FACETS, XSD_10_LIST_FACETS, XSD_11_UNION_FACETS, XSD_10_UNION_FACETS
from .identities import XsdIdentity, XsdUnique, XsdKey, XsdKeyref, Xsd11Unique, \
    Xsd11Key, Xsd11Keyref
from .simple_types import XsdSimpleType, XsdAtomicBuiltin, XsdAtomicRestriction, \
    Xsd11AtomicRestriction, XsdUnion, Xsd11Union, XsdList

from .notations import XsdNotation
from .attributes import XsdAttribute, Xsd11Attribute, XsdAttributeGroup
from .complex_types import XsdComplexType, Xsd11ComplexType
from .wildcards import XsdAnyElement, Xsd11AnyElement, XsdAnyAttribute, Xsd11AnyAttribute
from .groups import XsdGroup, Xsd11Group
from .elements import XsdElement, Xsd11Element
from .assertions import XsdAssert

CT = TypeVar('CT', bound=XsdComponent)

BuilderType = Callable[[ElementType, SchemaType, Optional[XsdComponent]], CT]

# Elements for building dummy groups
ANY_ATTRIBUTE_ATTRIB = {'namespace': '##any', 'processContents': 'lax'}
ANY_ATTRIB = {
    'namespace': '##any',
    'processContents': 'lax',
    'minOccurs': '0',
    'maxOccurs': 'unbounded'
}

GLOBAL_MAP_INDEX = MappingProxyType({
    nm.XSD_SIMPLE_TYPE: 0,
    nm.XSD_COMPLEX_TYPE: 0,
    nm.XSD_NOTATION: 1,
    nm.XSD_ATTRIBUTE: 2,
    nm.XSD_ATTRIBUTE_GROUP: 3,
    nm.XSD_ELEMENT: 4,
    nm.XSD_GROUP: 5,
})

GLOBAL_MAP_ATTRIBUTE = MappingProxyType({
    nm.XSD_SIMPLE_TYPE: attrgetter('types'),
    nm.XSD_COMPLEX_TYPE: attrgetter('types'),
    nm.XSD_ATTRIBUTE: attrgetter('attributes'),
    nm.XSD_ATTRIBUTE_GROUP: attrgetter('attribute_groups'),
    nm.XSD_NOTATION: attrgetter('notations'),
    nm.XSD_ELEMENT: attrgetter('elements'),
    nm.XSD_GROUP: attrgetter('groups'),
})


class XsdBuilders:
    """
    A descriptor that is bound to a schema class for providing versioned builders
    for XSD components.
    """
    components: dict[str, type[XsdComponent]]
    facets: dict[str, type[XsdFacet]]
    identities: dict[str, type[XsdIdentity]]
    simple_types: dict[str, type[XsdSimpleType]]
    local_types: dict[str, Union[type[BaseXsdType], BuilderType[XsdSimpleType]]]
    builtins: tuple[dict[str, Any], ...]

    __slots__ = ('_name', '_xsd_version', 'components', 'facets', 'identities',
                 'simple_types', 'local_types', 'builtins', 'simple_type_class',
                 'notation_class', 'attribute_group_class', 'complex_type_class',
                 'attribute_class', 'group_class', 'element_class', 'any_element_class',
                 'any_attribute_class', 'atomic_restriction_class', 'list_class',
                 'union_class', 'unique_class', 'key_class', 'keyref_class',
                 'annotation_class', 'admitted_facets', 'admitted_union_facets',
                 'admitted_list_facets')

    def __init__(self, xsd_version: Optional[str] = None,
                 *facets_classes: type[XsdFacet],
                 **classes: type[XsdComponent]) -> None:
        if xsd_version is not None:
            self._xsd_version = xsd_version

        self.components = {}
        self.facets = {}

        if facets_classes:
            for cls in facets_classes:
                self.facets[cls.meta_tag()] = self.components[cls.meta_tag()] = cls

        for k, v in classes.items():
            if k.endswith('_class'):
                setattr(self, k, v)

    def __set_name__(self, cls: type[SchemaType], name: str) -> None:
        self._name = name
        self._xsd_version = getattr(cls, 'XSD_VERSION', '1.0')

        if not self.facets:
            self.facets.update(FACETS_CLASSES[self._xsd_version])
        else:
            facets = FACETS_CLASSES[self._xsd_version].copy()
            facets.update(self.facets)
            self.facets = facets

        self.builtins = BUILTIN_TYPES[self._xsd_version]

        self.simple_type_class = XsdSimpleType
        self.notation_class = XsdNotation
        self.attribute_group_class = XsdAttributeGroup
        self.list_class = XsdList
        self.annotation_class = XsdAnnotation

        if self._xsd_version == '1.0':
            self.complex_type_class = XsdComplexType
            self.attribute_class = XsdAttribute
            self.group_class = XsdGroup
            self.element_class = XsdElement
            self.any_element_class = XsdAnyElement
            self.any_attribute_class = XsdAnyAttribute
            self.atomic_restriction_class = XsdAtomicRestriction
            self.union_class = XsdUnion
            self.unique_class = XsdUnique
            self.key_class = XsdKey
            self.keyref_class = XsdKeyref
            self.admitted_facets = XSD_10_FACETS
            self.admitted_union_facets = XSD_10_UNION_FACETS
            self.admitted_list_facets = XSD_10_LIST_FACETS
        else:
            self.complex_type_class = Xsd11ComplexType
            self.attribute_class = Xsd11Attribute
            self.group_class = Xsd11Group
            self.element_class = Xsd11Element
            self.any_element_class = Xsd11AnyElement
            self.any_attribute_class = Xsd11AnyAttribute
            self.atomic_restriction_class = Xsd11AtomicRestriction
            self.union_class = Xsd11Union
            self.unique_class = Xsd11Unique
            self.key_class = Xsd11Key
            self.keyref_class = Xsd11Keyref
            self.admitted_facets = XSD_11_FACETS
            self.admitted_union_facets = XSD_11_UNION_FACETS
            self.admitted_list_facets = XSD_11_LIST_FACETS

        self.identities = {
            nm.XSD_UNIQUE: self.unique_class,
            nm.XSD_KEY: self.key_class,
            nm.XSD_KEYREF: self.keyref_class,
        }
        self.simple_types = {
            nm.XSD_RESTRICTION: self.atomic_restriction_class,
            nm.XSD_LIST: self.list_class,
            nm.XSD_UNION: self.union_class,
        }
        self.local_types = {
            nm.XSD_COMPLEX_TYPE: self.complex_type_class,
            nm.XSD_SIMPLE_TYPE: self.simple_type_factory,
        }

    def __setattr__(self, name: str, value: Any) -> None:
        if name == '_xsd_version':
            if value not in ('1.0', '1.1'):
                raise XMLSchemaValueError(f"wrong or unsupported XSD version {value!r}")
            elif hasattr(self, '_xsd_version') and self._xsd_version != value:
                raise XMLSchemaValueError("XSD version mismatch")

        elif name.endswith('_class'):
            if not isinstance(value, type) or not issubclass(value, XsdComponent):
                raise XMLSchemaTypeError(f"{name} must be a subclass of XsdComponent")
            if hasattr(self, name):
                return  # Skip changing a component class already set at __init__
            self.components[value.meta_tag()] = value

        super().__setattr__(name, value)

    def __get__(self, instance: Optional[Any], cls: type[Any]) -> 'XsdBuilders':
        return self

    def __set__(self, instance: Any, value: Any) -> None:
        raise XMLSchemaAttributeError(_("Can't set attribute {}").format(self._name))

    def __delete__(self, instance: Any) -> None:
        raise XMLSchemaAttributeError(_("Can't delete attribute {}").format(self._name))

    @property
    def xsd_version(self) -> str:
        return self._xsd_version

    def create_any_content_group(self, parent: Union[XsdComplexType, XsdGroup],
                                 any_element: Optional[XsdAnyElement] = None) -> XsdGroup:
        """
        Creates a local child model group for a complex type or a group that accepts any content.

        :param parent: the parent complex type or group for the content group.
        :param any_element: an optional any element to use for the content group. \
        When provided it's copied, linked to the group and the minOccurs/maxOccurs \
        are set to 0 and 'unbounded'.
        """
        schema = parent.schema
        elem = Element(nm.XSD_SEQUENCE)
        if isinstance(any_element, XsdAnyElement):
            attrib = any_element.elem.attrib.copy()
            attrib['minOccurs'] = '0'
            attrib['maxOccurs'] = 'unbounded'
            elem.append(Element(nm.XSD_ANY, attrib))
        else:
            elem.append(Element(nm.XSD_ANY, ANY_ATTRIB))

        elem.text = elem[0].tail = '\n  '
        return self.group_class(elem, schema, parent)

    def create_empty_content_group(self, parent: Union[XsdComplexType, XsdGroup],
                                   model: str = 'sequence', **attrib: Any) -> XsdGroup:
        """
        Creates an empty local child content group for a complex type or a group.
        """
        if model == 'sequence':
            elem = Element(nm.XSD_SEQUENCE, attrib)
        elif model == 'choice':
            elem = Element(nm.XSD_CHOICE, attrib)
        elif model == 'all':
            elem = Element(nm.XSD_ALL, attrib)
        else:
            msg = _("'model' argument must be (sequence | choice | all)")
            raise XMLSchemaValueError(msg)

        elem.text = '\n    '
        return self.group_class(elem, parent.schema, parent)

    def create_any_attribute_group(self, parent: Union[XsdComplexType, XsdElement]) \
            -> XsdAttributeGroup:
        """
        Creates a local child attribute group for a complex type or an element
        that accepts any attribute.
        """
        elem = Element(nm.XSD_ATTRIBUTE_GROUP)
        elem.append(Element(nm.XSD_ANY_ATTRIBUTE, ANY_ATTRIBUTE_ATTRIB))
        elem.text = elem[0].tail = '\n    '

        attribute_group = self.attribute_group_class(elem, parent.schema, parent)
        attribute_group[None] = self.any_attribute_class(
            elem[0], parent.schema, attribute_group
        )
        return attribute_group

    def create_empty_attribute_group(self, parent: Union[XsdComplexType, XsdElement]) \
            -> XsdAttributeGroup:
        """
        Creates an empty local child attribute group for a complex type or an element.
        """
        return self.attribute_group_class(
            Element(nm.XSD_ATTRIBUTE_GROUP), parent.schema, parent
        )

    def create_any_type(self, schema: SchemaType) -> XsdComplexType:
        """
        Creates a xs:anyType equivalent type related with the wildcards
        connected to global maps of the schema instance in order to do a
        correct namespace lookup during wildcards validation.
        """
        maps = schema.maps
        if schema.meta_schema is not None and schema.target_namespace != nm.XSD_NAMESPACE:
            schema = schema.meta_schema

        elem = Element(nm.XSD_COMPLEX_TYPE, name=nm.XSD_ANY_TYPE)
        elem.append(Element(nm.XSD_SEQUENCE))
        elem[0].append(Element(nm.XSD_ANY, ANY_ATTRIB))
        elem.append(Element(nm.XSD_ANY_ATTRIBUTE, ANY_ATTRIBUTE_ATTRIB))
        elem.text = elem[0].tail = '\n  '
        elem[0].text = elem[0][0].tail = '\n    '

        any_type = self.complex_type_class(elem, schema, mixed=True, block='', final='')
        for c in any_type.iter_components():
            c.maps = maps
        return any_type

    def create_element(self, name: str,
                       schema: SchemaType,
                       parent: Optional[XsdComponent] = None,
                       text: Optional[str] = None, **attrib: Any) -> XsdElement:
        """
        Creates a xs:element instance related to schema component.
        Used as dummy element for validation/decoding/encoding
        operations of wildcards and complex types.
        """
        elem = Element(nm.XSD_ELEMENT, name=name, **attrib)
        if text is not None:
            elem.text = text
        return self.element_class(elem, schema, parent)

    def simple_type_factory(self, elem: Element,
                            schema: SchemaType,
                            parent: Optional[XsdComponent] = None) -> XsdSimpleType:
        """
        Factory function for XSD simple types. Parses the xs:simpleType element and its
        child component, that can be a restriction, a list or a union. Annotations are
        linked to simple type instance, omitting the inner annotation if both are given.
        """
        annotation: Optional[XsdAnnotation] = None
        try:
            child = elem[0]
        except IndexError:
            return cast(XsdSimpleType, schema.maps.types[nm.XSD_ANY_SIMPLE_TYPE])
        else:
            if child.tag == nm.XSD_ANNOTATION:
                annotation = XsdAnnotation(child, schema, parent)
                try:
                    child = elem[1]
                except IndexError:
                    msg = _("(restriction | list | union) expected")
                    schema.parse_error(msg, elem)
                    return cast(XsdSimpleType, schema.maps.types[nm.XSD_ANY_SIMPLE_TYPE])

        xsd_type: XsdSimpleType
        try:
            xsd_type = self.simple_types[child.tag](child, schema, parent)
        except KeyError:
            msg = _("(restriction | list | union) expected")
            schema.parse_error(msg, elem)
            return cast(XsdSimpleType, schema.maps.types[nm.XSD_ANY_SIMPLE_TYPE])

        if annotation is not None:
            setattr(xsd_type, 'annotation', annotation)

        try:
            xsd_type.name = get_qname(schema.target_namespace, elem.attrib['name'])
        except KeyError:
            if parent is None:
                msg = _("missing attribute 'name' in a global simpleType")
                schema.parse_error(msg, elem)
                xsd_type.name = 'nameless_%s' % str(id(xsd_type))
        else:
            if parent is not None:
                msg = _("attribute 'name' not allowed for a local simpleType")
                schema.parse_error(msg, elem)
                xsd_type.name = None

        if 'final' in elem.attrib:
            xsd_type._final = parse_xsd_derivation(elem, 'final', validator=xsd_type)

        return xsd_type


class StagedMap(Mapping[str, CT]):
    label = 'component'

    @abstractmethod
    def _factory_or_class(self, elem: ElementType, schema: SchemaType) -> CT:
        """Returns the builder class or method used to build the global map."""

    __slots__ = ('_store', '_staging', '_builders')

    def __init__(self, builders: XsdBuilders):
        self._store: dict[str, CT] = {}
        self._staging: dict[str, StagedItemType] = {}
        self._builders = builders

    def __getitem__(self, qname: str) -> CT:
        try:
            return self._store[qname]
        except KeyError:
            if qname in self._staging:
                return self._build_global(qname)

            msg = _('global {} {!r} not found').format(self.label, qname)
            raise XMLSchemaKeyError(msg) from None

    def __iter__(self) -> Iterator[str]:
        yield from self._store

    def __len__(self) -> int:
        return len(self._store)

    def __repr__(self) -> str:
        return repr(self._store)

    def __copy__(self) -> 'StagedMap[CT]':
        obj = object.__new__(self.__class__)
        obj._builders = self._builders
        obj._staging = self._staging.copy()
        obj._store = self._store.copy()
        return obj

    copy = __copy__

    def clear(self) -> None:
        self._store.clear()
        self._staging.clear()

    def update(self, other: 'StagedMap[CT]') -> None:
        self._store.update(other._store)

    @property
    def total_staged(self) -> int:
        return len(self._staging)

    @property
    def staged(self) -> list[str]:
        return list(self._staging)

    @property
    def staged_items(self) -> ItemsView[str, StagedItemType]:
        return self._staging.items()

    @property
    def staged_values(self) -> ValuesView[StagedItemType]:
        return self._staging.values()

    def load(self, qname: str, elem: ElementType, schema: SchemaType) -> None:
        if qname in self._store:
            comp = self._store[qname]
            if comp.schema is schema:
                msg = _("global xs:{} with name={!r} is already built")
            elif comp.schema.maps is schema.maps or comp.schema.meta_schema is None:
                msg = _("global xs:{} with name={!r} is already defined")
            else:
                # Allows rebuilding of parent maps components for descendant maps
                # but not allows substitution of meta-schema components.
                self._staging[qname] = elem, schema
                return

        elif qname in self._staging:
            obj = self._staging[qname]

            if len(obj) == 2 and isinstance(obj, tuple):
                _elem, _schema = obj  # type: ignore[unused-ignore, misc]
                if _elem is elem and _schema is schema:
                    return  # ignored: it's the same component
                elif schema is _schema.override:
                    return  # ignored: the loaded component is overridden
                elif schema.override is _schema:
                    # replaced: the loaded component is an override
                    self._staging[qname] = (elem, schema)
                    return
                elif schema.meta_schema is None and _schema.meta_schema is not None:
                    return  # ignore merged meta-schema components
                elif _schema.meta_schema is None and schema.meta_schema is not None:
                    # Override merged meta-schema component
                    self._staging[qname] = (elem, schema)
                    return

            msg = _("global xs:{} with name={!r} is already loaded")
        else:
            self._staging[qname] = elem, schema
            return

        schema.parse_error(
            error=msg.format(local_name(elem.tag), qname),
            elem=elem
        )

    def load_redefine(self, qname: str, elem: ElementType, schema: SchemaType) -> None:
        try:
            item = self._staging[qname]
        except KeyError:
            schema.parse_error(_("not a redefinition!"), elem)
        else:
            if isinstance(item, list):
                item.append((elem, schema))
            else:
                self._staging[qname] = [cast(LoadedItemType, item), (elem, schema)]

    def load_override(self, qname: str, elem: ElementType, schema: SchemaType) -> None:
        if qname not in self._staging:
            # Overrides that match nothing in the target schema are ignored. See the
            # period starting with "Source declarations not present in the target set"
            # of the paragraph https://www.w3.org/TR/xmlschema11-1/#override-schema.
            return

        self._staging[qname] = elem, schema

    def build(self) -> None:
        for name in [x for x in self._staging]:
            if name in self._staging:
                self._build_global(name)

    def _build_global(self, qname: str) -> CT:
        obj = self._staging[qname]
        if isinstance(obj, tuple):
            # Not built XSD global component without redefinitions
            try:
                elem, schema = obj  # type: ignore[misc]
            except ValueError:
                raise XMLSchemaCircularityError(qname, *obj[0])

            # Encapsulate into a tuple to catch circular builds
            self._staging[qname] = ((elem, schema),)
            self._store[qname] = self._factory_or_class(elem, schema)
            self._staging.pop(qname)
            return self._store[qname]

        else:
            # Not built XSD global component with redefinitions
            try:
                elem, schema = obj[0]
            except ValueError:
                if not isinstance(obj, tuple):
                    raise
                raise XMLSchemaCircularityError(qname, *obj[0][0])

            self._staging[qname] = obj[0],  # To catch circular builds
            self._store[qname] = component = self._factory_or_class(elem, schema)
            self._staging.pop(qname)

            # Apply redefinitions (changing elem involve reparse of the component)
            for elem, schema in obj[1:]:
                if component.schema.target_namespace != schema.target_namespace:
                    msg = _("redefined schema {!r} has a different targetNamespace")
                    raise XMLSchemaValueError(msg.format(schema))

                component.redefine = copy.copy(component)
                component.redefine.parent = component
                component.schema = schema
                component.parse(elem)

            return self._store[qname]


class TypesMap(StagedMap[BaseXsdType]):

    def _factory_or_class(self, elem: ElementType, schema: SchemaType) -> BaseXsdType:
        if elem.tag == nm.XSD_COMPLEX_TYPE:
            return self._builders.complex_type_class(elem, schema)
        else:
            return self._builders.simple_type_factory(elem, schema)

    def build_builtins(self, schema: SchemaType) -> None:
        if schema.meta_schema is not None and nm.XSD_ANY_TYPE in self._store:
            # builtin types already provided
            return
        #
        # Special builtin types.
        #
        # xs:anyType
        # Ref: https://www.w3.org/TR/xmlschema11-1/#builtin-ctd
        self._store[nm.XSD_ANY_TYPE] = self._builders.create_any_type(schema)

        # xs:anySimpleType
        # Ref: https://www.w3.org/TR/xmlschema11-2/#builtin-stds
        any_simple_type = self._store[nm.XSD_ANY_SIMPLE_TYPE] = XsdSimpleType(
            elem=Element(nm.XSD_SIMPLE_TYPE, name=nm.XSD_ANY_SIMPLE_TYPE),
            schema=schema,
            parent=None,
            name=nm.XSD_ANY_SIMPLE_TYPE
        )

        # xs:anyAtomicType
        # Ref: https://www.w3.org/TR/xmlschema11-2/#builtin-stds
        self._store[nm.XSD_ANY_ATOMIC_TYPE] = \
            self._builders.atomic_restriction_class(
                elem=Element(nm.XSD_SIMPLE_TYPE, name=nm.XSD_ANY_ATOMIC_TYPE),
                schema=schema,
                parent=None,
                name=nm.XSD_ANY_ATOMIC_TYPE,
                base_type=any_simple_type,
            )

        for item in self._builders.builtins:
            item = item.copy()
            name: str = item['name']
            try:
                value = self._staging.pop(name)
            except KeyError:
                # If builtin type element is missing create a dummy element. Necessary for the
                # meta-schema XMLSchema.xsd of XSD 1.1, that not includes builtins declarations.
                elem = Element(nm.XSD_SIMPLE_TYPE, name=name, id=name)
            else:
                if not isinstance(value, tuple) or len(value) != 2:
                    continue
                elem, schema = value

            base_type: Optional[BaseXsdType]
            if 'base_type' in item:
                base_type = item['base_type'] = self._store[item['base_type']]
            else:
                base_type = None

            facets = item.pop('facets', None)
            xsd_type = XsdAtomicBuiltin(elem, schema, **item)
            if isinstance(facets, Iterable):
                built_facets = xsd_type.facets
                for e in facets:
                    try:
                        cls = self._builders.facets[e.tag]
                    except AttributeError:
                        built_facets[None] = e
                    else:
                        built_facets[e.tag] = cls(e, schema, xsd_type, base_type)
                xsd_type.facets = built_facets

            self._store[name] = xsd_type


class NotationsMap(StagedMap[XsdNotation]):
    label = 'notation'

    def _factory_or_class(self, elem: ElementType, schema: SchemaType) -> XsdNotation:
        return self._builders.notation_class(elem, schema)


class AttributesMap(StagedMap[XsdAttribute]):
    label = 'attribute'

    def _factory_or_class(self, elem: ElementType, schema: SchemaType) -> XsdAttribute:
        return self._builders.attribute_class(elem, schema)


class AttributeGroupsMap(StagedMap[XsdAttributeGroup]):
    label = 'attribute group'

    def _factory_or_class(self, elem: ElementType, schema: SchemaType) -> XsdAttributeGroup:
        return self._builders.attribute_group_class(elem, schema)


class ElementsMap(StagedMap[XsdElement]):
    label = 'element'

    def _factory_or_class(self, elem: ElementType, schema: SchemaType) -> XsdElement:
        return self._builders.element_class(elem, schema)


class GroupsMap(StagedMap[XsdGroup]):
    label = 'model group'

    def _factory_or_class(self, elem: ElementType, schema: SchemaType) -> XsdGroup:
        return self._builders.group_class(elem, schema)


class GlobalMaps(NamedTuple):
    types: TypesMap
    notations: NotationsMap
    attributes: AttributesMap
    attribute_groups: AttributeGroupsMap
    elements: ElementsMap
    groups: GroupsMap

    @classmethod
    def from_builders(cls, builders: XsdBuilders) -> 'GlobalMaps':
        return cls(
            TypesMap(builders),
            NotationsMap(builders),
            AttributesMap(builders),
            AttributeGroupsMap(builders),
            ElementsMap(builders),
            GroupsMap(builders)
        )

    def clear(self) -> None:
        for item in self:
            item.clear()

    def update(self, other: 'GlobalMaps') -> None:
        for m1, m2 in zip(self, other):
            m1.update(m2)  # type: ignore[attr-defined]

    def copy(self) -> 'GlobalMaps':
        return GlobalMaps(*[m.copy() for m in self])  # type: ignore[arg-type]

    def iter_globals(self) -> Iterator[SchemaGlobalType]:
        for item in self:
            yield from item.values()

    def iter_staged(self) -> Iterator[StagedItemType]:
        for item in self:
            yield from item.staged_values

    @property
    def total(self) -> int:
        """Total number of global components, fully or partially built."""
        return sum(len(m) for m in self)

    @property
    def total_built(self) -> int:
        """Total number of fully built global components."""
        return sum(1 for c in self.iter_globals() if c.built)

    @property
    def total_unbuilt(self) -> int:
        """Total number of not built or partially built global components."""
        return sum(1 for c in self.iter_globals() if not c.built)

    @property
    def total_staged(self) -> int:
        """Total number of staged global components."""
        return sum(m.total_staged for m in self)

    def load(self, schemas: Iterable[SchemaType]) -> None:
        """Loads global XSD components for the given schemas."""
        redefinitions = []

        for schema in schemas:
            if schema.target_namespace:
                ns_prefix = f'{{{schema.target_namespace}}}'
            else:
                ns_prefix = ''

            for elem in schema.root:
                if (tag := elem.tag) in (nm.XSD_REDEFINE, nm.XSD_OVERRIDE):
                    location = elem.get('schemaLocation')
                    if location is None:
                        continue

                    for child in elem:
                        try:
                            qname = ns_prefix + child.attrib['name']
                        except KeyError:
                            continue

                        try:
                            redefinitions.append(
                                (qname, elem, child, schema, schema.includes[location])
                            )
                        except KeyError:
                            if schema.partial:
                                redefinitions.append((qname, elem, child, schema, schema))

                elif tag in nm.GLOBAL_TAGS:
                    try:
                        qname = ns_prefix + elem.attrib['name']
                    except KeyError:
                        continue  # Invalid global: skip

                    self[GLOBAL_MAP_INDEX[tag]].load(qname, elem, schema)

        redefined_names = Counter(x[0] for x in redefinitions)
        for qname, elem, child, schema, redefined_schema in reversed(redefinitions):

            # Checks multiple redefinitions
            if redefined_names[qname] > 1:
                redefined_names[qname] = 1

                redefined_schemas: Any
                redefined_schemas = [x[-1] for x in redefinitions if x[0] == qname]
                if any(redefined_schemas.count(x) > 1 for x in redefined_schemas):
                    msg = _("multiple redefinition for {} {!r}")
                    schema.parse_error(
                        error=msg.format(local_name(child.tag), qname),
                        elem=child
                    )
                else:
                    redefined_schemas = {x[-1]: x[-2] for x in redefinitions if x[0] == qname}
                    for rs, s in redefined_schemas.items():
                        while True:
                            try:
                                s = redefined_schemas[s]
                            except KeyError:
                                break

                            if s is rs:
                                msg = _("circular redefinition for {} {!r}")
                                schema.parse_error(
                                    error=msg.format(local_name(child.tag), qname),
                                    elem=child
                                )
                                break

            if elem.tag == nm.XSD_REDEFINE:
                self[GLOBAL_MAP_INDEX[child.tag]].load_redefine(qname, child, schema)
            else:
                self[GLOBAL_MAP_INDEX[child.tag]].load_override(qname, child, schema)

    def build(self, schemas: Iterable[SchemaType]) -> None:
        """Builds global XSD components for the given schemas."""
        self.notations.build()
        self.attributes.build()
        self.attribute_groups.build()

        for schema in schemas:
            if not isinstance(schema.default_attributes, str):
                continue

            try:
                attributes = schema.maps.attribute_groups[schema.default_attributes]
            except KeyError:
                schema.default_attributes = None
                msg = _("defaultAttributes={0!r} doesn't match any attribute group of {1!r}")
                schema.parse_error(
                    error=msg.format(schema.root.get('defaultAttributes'), schema),
                    elem=schema.root
                )
            else:
                schema.default_attributes = attributes

        self.types.build()
        self.elements.build()
        self.groups.build()

        # Build element declarations inside model groups.
        for schema in schemas:
            for group in schema.iter_components(XsdGroup):
                try:
                    group.build()
                except XMLSchemaModelDepthError as e:
                    schema.parse_error(error=e, elem=group.elem)

        # Build identity references and XSD 1.1 assertions
        for schema in schemas:
            for obj in schema.iter_components((XsdIdentity, XsdAssert)):
                obj.build()

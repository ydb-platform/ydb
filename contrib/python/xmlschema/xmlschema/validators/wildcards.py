#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import Callable, Iterator, Iterable
from copy import copy
from typing import Any, Optional, Union

from elementpath import SchemaElementNode, build_schema_node_tree

import xmlschema.names as nm
from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.aliases import ElementType, SchemaType, SchemaElementType, SchemaAttributeType, \
    ModelGroupType, ModelParticleType, AtomicValueType, DecodedValueType, OccursCounterType
from xmlschema.translation import gettext as _
from xmlschema.utils.qnames import get_namespace
from xmlschema.utils.decoding import EmptyType, Empty, raw_encode_value
from xmlschema.xpath import XMLSchemaProxy, ElementPathMixin
from xmlschema.caching import schema_cache

from .validation import ValidationContext, EncodeContext, ValidationMixin
from .xsdbase import XsdComponent

from .particles import ParticleMixin
from . import elements


class XsdWildcard(XsdComponent):
    process_contents: str
    namespace: set[str]
    not_namespace: Union[tuple[()], set[str]] = ()
    not_qname: Union[tuple[()], set[str]] = ()

    # For compatibility with protocol of XSD elements/attributes
    type = None
    default = None
    fixed = None

    __slots__ = ('process_contents', 'namespace')

    def __repr__(self) -> str:
        if self.not_namespace:
            return '%s(not_namespace=%r, process_contents=%r)' % (
                self.__class__.__name__, sorted(self.not_namespace), self.process_contents
            )
        else:
            return '%s(namespace=%r, process_contents=%r)' % (
                self.__class__.__name__, sorted(self.namespace), self.process_contents
            )

    def __copy__(self) -> 'XsdWildcard':
        wildcard: XsdWildcard = object.__new__(self.__class__)
        wildcard.__dict__.update(self.__dict__)

        for attr in self._mro_slots():
            object.__setattr__(wildcard, attr, getattr(self, attr))

        wildcard.errors = self.errors.copy()
        wildcard.namespace = self.namespace.copy()
        if isinstance(self.not_namespace, set):
            wildcard.not_namespace = self.not_namespace.copy()
        if isinstance(self.not_qname, set):
            wildcard.not_qname = self.not_qname.copy()
        return wildcard

    def _parse(self) -> None:
        # Parse namespace and processContents
        match namespace := self.elem.attrib.get('namespace', '##any').strip():
            case '##any' | '##other':
                self.namespace = {namespace}
            case '':
                self.namespace = set()  # an empty value means no namespace allowed!
            case '##local':
                self.namespace = {''}
            case '##targetNamespace':
                self.namespace = {self.target_namespace}
            case _:
                self.namespace = set()
                for ns in namespace.split():
                    if ns == '##local':
                        self.namespace.add('')
                    elif ns == '##targetNamespace':
                        self.namespace.add(self.target_namespace)
                    elif ns.startswith('##'):
                        msg = _("wrong value %r in 'namespace' attribute")
                        self.parse_error(msg % ns)
                    else:
                        self.namespace.add(ns)

        self.process_contents = self.elem.attrib.get('processContents', 'strict')
        if self.process_contents not in ('strict', 'lax', 'skip'):
            msg = _("wrong value %r for 'processContents' attribute")
            self.parse_error(msg % self.process_contents)
            self.process_contents = 'strict'

    def _parse_not_constraints(self) -> None:
        if 'notNamespace' not in self.elem.attrib:
            pass
        elif 'namespace' in self.elem.attrib:
            msg = _("'namespace' and 'notNamespace' attributes are mutually exclusive")
            self.parse_error(msg)
        else:
            self.namespace.clear()
            self.not_namespace = set()
            for ns in self.elem.attrib['notNamespace'].strip().split():
                if ns == '##local':
                    self.not_namespace.add('')
                elif ns == '##targetNamespace':
                    self.not_namespace.add(self.target_namespace)
                elif ns.startswith('##'):
                    msg = _("wrong value %r in 'notNamespace' attribute")
                    self.parse_error(msg % ns)
                else:
                    self.not_namespace.add(ns)

        # Parse notQName attribute
        if 'notQName' not in self.elem.attrib:
            return

        not_qname = self.elem.attrib['notQName'].strip().split()

        if isinstance(self, XsdAnyAttribute) and \
                not all(not s.startswith('##') or s == '##defined'
                        for s in not_qname) or \
                not all(not s.startswith('##') or s in {'##defined', '##definedSibling'}
                        for s in not_qname):
            self.parse_error(_("wrong value for 'notQName' attribute"))
            return

        try:
            names = {x if x.startswith('##') else self.schema.resolve_qname(x, False)
                     for x in not_qname}
        except KeyError as err:
            msg = _("unmapped QName in 'notQName' attribute: %s")
            self.parse_error(msg % str(err))
            return
        except ValueError as err:
            msg = _("wrong QName format in 'notQName' attribute: %s")
            self.parse_error(msg % str(err))
            return

        if self.not_namespace:
            if any(not x.startswith('##') for x in names) and \
                    all(get_namespace(x) in self.not_namespace
                        for x in names if not x.startswith('##')):
                msg = _("the namespace of each QName in notQName is allowed by notNamespace")
                self.parse_error(msg)
        elif any(not self.is_namespace_allowed(get_namespace(x))
                 for x in names if not x.startswith('##')):
            msg = _("names in notQName must be in namespaces that are allowed")
            self.parse_error(msg)

        self.not_qname = names

    @property
    def value_constraint(self) -> Optional[str]:
        return None

    def is_matching(self, name: Optional[str],
                    default_namespace: Optional[str] = None,
                    **kwargs: Any) -> bool:
        if name is None:
            return False
        elif not name or name[0] == '{':
            return self.is_namespace_allowed(get_namespace(name))
        elif not default_namespace:
            return self.is_namespace_allowed('')
        else:
            return self.is_namespace_allowed(default_namespace)

    def is_namespace_allowed(self, namespace: str) -> bool:
        if self.not_namespace:
            return namespace not in self.not_namespace
        elif '##any' in self.namespace or namespace == nm.XSI_NAMESPACE:
            return True
        elif '##other' in self.namespace:
            if not namespace:
                return False
            return namespace != self.target_namespace
        else:
            return namespace in self.namespace

    def deny_namespaces(self, namespaces: list[str]) -> bool:
        if self.not_namespace:
            return all(x in self.not_namespace for x in namespaces)
        elif '##any' in self.namespace:
            return False
        elif '##other' in self.namespace:
            return all(x == self.target_namespace for x in namespaces)
        else:
            return all(x not in self.namespace for x in namespaces)

    def deny_qnames(self, names: Iterable[str]) -> bool:
        if self.not_namespace:
            return all(x in self.not_qname or get_namespace(x) in self.not_namespace
                       for x in names)
        elif '##any' in self.namespace:
            return all(x in self.not_qname for x in names)
        elif '##other' in self.namespace:
            return all(x in self.not_qname or get_namespace(x) == self.target_namespace
                       for x in names)
        else:
            return all(x in self.not_qname or get_namespace(x) not in self.namespace
                       for x in names)

    def _has_occurs_restriction(self, other: 'XsdWildcard') -> bool:
        return True

    @schema_cache
    def is_restriction(self, other: Union[ModelParticleType, 'XsdAnyAttribute'],
                       check_occurs: bool = True) -> bool:
        if not isinstance(other, self.__class__):
            return False
        elif check_occurs and not self._has_occurs_restriction(other):
            return False

        assert isinstance(other, XsdWildcard)
        if other.process_contents == 'strict' and self.process_contents != 'strict':
            return False
        elif other.process_contents == 'lax' and self.process_contents == 'skip':
            return False

        if not self.not_qname and not other.not_qname:
            pass
        elif '##defined' in other.not_qname and '##defined' not in self.not_qname:
            return False
        elif '##definedSibling' in other.not_qname and '##definedSibling' not in self.not_qname:
            return False
        elif other.not_qname:
            if not self.deny_qnames(x for x in other.not_qname if not x.startswith('##')):
                return False
        elif any(not other.is_namespace_allowed(get_namespace(x))
                 for x in self.not_qname if not x.startswith('##')):
            return False

        if self.not_namespace:
            if other.not_namespace:
                return all(ns in self.not_namespace for ns in other.not_namespace)
            elif '##any' in other.namespace:
                return True
            elif '##other' in other.namespace:
                return '' in self.not_namespace and other.target_namespace in self.not_namespace
            else:
                return False
        elif other.not_namespace:
            if '##any' in self.namespace:
                return False
            elif '##other' in self.namespace:
                return other.not_namespace.issubset(('', other.target_namespace))
            else:
                return all(ns not in other.not_namespace for ns in self.namespace)

        if self.namespace == other.namespace:
            return True
        elif '##any' in other.namespace:
            return True
        elif '##any' in self.namespace or '##other' in self.namespace:
            return False
        elif '##other' in other.namespace:
            return other.target_namespace not in self.namespace and '' not in self.namespace
        else:
            return all(ns in other.namespace for ns in self.namespace)

    def union(self, other: Union['XsdAnyElement', 'XsdAnyAttribute']) -> None:
        """Update an XSD wildcard with the union of itself and another XSD wildcard."""
        if not self.not_qname:
            self.not_qname = copy(other.not_qname)
        else:
            self.not_qname = {
                x for x in self.not_qname
                if x in other.not_qname or not other.is_namespace_allowed(get_namespace(x))
            }

        if self.not_namespace:
            if other.not_namespace:
                self.not_namespace.intersection_update(other.not_namespace)
            elif '##any' in other.namespace:
                self.not_namespace = set()
                self.namespace.clear()
                self.namespace.add('##any')
                return
            elif '##other' in other.namespace:
                self.not_namespace.intersection_update({'', other.target_namespace})
            else:
                self.not_namespace.difference_update(other.namespace)

            if not self.not_namespace:
                self.namespace.clear()
                self.namespace.add('##any')
            return

        elif other.not_namespace:
            if '##any' in self.namespace:
                return
            elif '##other' in self.namespace:
                self.not_namespace = other.not_namespace & {'', self.target_namespace}
            else:
                self.not_namespace = other.not_namespace - self.namespace

            self.namespace.clear()
            if not self.not_namespace:
                self.namespace.add('##any')
            return

        w1: XsdWildcard
        w2: XsdWildcard
        if not other.namespace or '##any' in self.namespace or self.namespace == other.namespace:
            return
        elif '##any' in other.namespace:
            self.namespace.clear()
            self.namespace.add('##any')
            return
        elif '##other' in other.namespace:
            w1, w2 = other, self
        elif '##other' in self.namespace:
            w1, w2 = self, other
        else:
            self.namespace.update(other.namespace)
            return

        if w1.target_namespace in w2.namespace and '' in w2.namespace:
            self.namespace.clear()
            self.namespace.add('##any')
        elif '' not in w2.namespace and w1.target_namespace == w2.target_namespace:
            self.namespace.clear()
            self.namespace.add('##other')
        elif self.xsd_version == '1.0':
            msg = _("not expressible wildcard namespace union: {0!r} V {1!r}:")
            raise XMLSchemaValueError(msg.format(other.namespace, self.namespace))
        else:
            self.namespace.clear()
            self.not_namespace = {'', w1.target_namespace}

    def intersection(self, other: Union['XsdAnyElement', 'XsdAnyAttribute']) -> None:
        """Update an XSD wildcard with the intersection of itself and another XSD wildcard."""
        if self.not_qname:
            self.not_qname.update(other.not_qname)
        else:
            self.not_qname = copy(other.not_qname)

        if self.not_namespace:
            if other.not_namespace:
                self.not_namespace.update(other.not_namespace)
            elif '##any' in other.namespace:
                pass
            elif '##other' not in other.namespace:
                self.namespace = other.namespace - self.not_namespace
                self.not_namespace.clear()
            else:
                self.not_namespace.add('')
                self.not_namespace.add(other.target_namespace)
            return

        elif other.not_namespace:
            if '##any' in self.namespace:
                self.not_namespace = other.not_namespace.copy()
                self.namespace.clear()
            elif '##other' not in self.namespace:
                self.namespace.difference_update(other.not_namespace)
            else:
                self.not_namespace = other.not_namespace.copy()
                self.not_namespace.add('')
                self.not_namespace.add(other.target_namespace)
                self.namespace.clear()
            return

        if self.namespace == other.namespace:
            return
        elif '##any' in other.namespace:
            return
        elif '##any' in self.namespace:
            self.namespace.clear()
            self.namespace.update(other.namespace)
        elif '##other' in self.namespace:
            self.namespace.clear()
            self.namespace.update(other.namespace)
            self.namespace.discard(other.target_namespace)
            self.namespace.discard('')
        elif '##other' not in other.namespace:
            self.namespace.intersection_update(other.namespace)
        else:
            self.namespace.discard(other.target_namespace)
            self.namespace.discard('')


class XsdAnyElement(XsdWildcard, ParticleMixin,
                    ElementPathMixin[SchemaElementType],
                    ValidationMixin[ElementType, Any]):
    """
    Class for XSD 1.0 *any* wildcards.

    ..  <any
          id = ID
          maxOccurs = (nonNegativeInteger | unbounded) : 1
          minOccurs = nonNegativeInteger : 1
          namespace = ((##any | ##other) | List of (anyURI | (##targetNamespace|##local)) ) : ##any
          processContents = (lax | skip | strict) : strict
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </any>
    """
    _ADMITTED_TAGS = nm.XSD_ANY,
    precedences: dict[ModelGroupType, list[ModelParticleType]]
    copy: Callable[['XsdAnyElement'], 'XsdAnyElement']

    __slots__ = ('min_occurs', 'max_occurs', 'skip', 'precedences')

    def __init__(self, elem: ElementType, schema: SchemaType, parent: XsdComponent) -> None:
        self.min_occurs = self.max_occurs = 1
        self.skip = False
        self.precedences = {}
        super().__init__(elem, schema, parent)

    def __repr__(self) -> str:
        if self.namespace:
            return '%s(namespace=%r, process_contents=%r, occurs=%r)' % (
                self.__class__.__name__, sorted(self.namespace),
                self.process_contents, list(self.occurs)
            )
        else:
            return '%s(not_namespace=%r, process_contents=%r, occurs=%r)' % (
                self.__class__.__name__, sorted(self.not_namespace),
                self.process_contents, list(self.occurs)
            )

    @property
    def xpath_proxy(self) -> XMLSchemaProxy:
        return XMLSchemaProxy(self.schema, self)

    # noinspection PyTypeChecker
    @property
    def xpath_node(self) -> SchemaElementNode:
        schema_node = self.schema.xpath_node
        node = schema_node.get_element_node(self)
        if isinstance(node, SchemaElementNode):
            return node

        return build_schema_node_tree(
            root=self,
            elements=schema_node.elements,
            global_elements=schema_node.children,
        )

    def _parse(self) -> None:
        super()._parse()
        self._parse_particle(self.elem)
        if self.process_contents == 'skip':
            self.skip = True

    def match(self, name: Optional[str], default_namespace: Optional[str] = None,
              resolve: bool = False, **kwargs: Any) -> Optional[SchemaElementType]:
        """
        Returns the element wildcard if name is matching the name provided
        as argument, `None` otherwise.

        :param name: a local or fully-qualified name.
        :param default_namespace: used when it's not `None` and not empty for \
        completing local name arguments.
        :param resolve: when `True` it doesn't return the wildcard but try to \
        resolve and return the element matching the name.
        :param kwargs: additional options used by XSD 1.1 xs:any wildcards.
        """
        if not name or not self.is_matching(name, default_namespace, **kwargs):
            return None
        elif not resolve:
            return self

        try:
            if name[0] != '{' and default_namespace:
                return self.maps.elements[f'{{{default_namespace}}}{name}']
            else:
                return self.maps.elements[name]
        except KeyError:
            return None

    def __iter__(self) -> Iterator[Any]:
        return iter(())

    def _has_occurs_restriction(self, other: XsdWildcard) -> bool:
        return self.max_occurs == 0 or isinstance(other, XsdAnyElement) and \
            self.has_occurs_restriction(other)

    def iter(self, tag: Optional[str] = None) -> Iterator[Any]:
        return iter(())

    def iterchildren(self, tag: Optional[str] = None) -> Iterator[Any]:
        return iter(())

    @staticmethod
    def iter_substitutes() -> Iterator[Any]:
        return iter(())

    def raw_decode(self, obj: ElementType, validation: str, context: ValidationContext) -> Any:

        if not self.is_matching(obj.tag):
            reason = _("element {!r} is not allowed here").format(obj)
            context.validation_error(validation, self, reason, obj)

        if self.process_contents == 'skip' and not context.process_skipped:
            return Empty

        namespace = get_namespace(obj.tag)
        if not self.maps.loader.load_namespace(namespace):
            reason = _("unavailable namespace {!r}").format(namespace)
        else:
            try:
                xsd_element = self.maps.elements[obj.tag]
            except KeyError:
                reason = f"element {obj.tag!r} not found"
            else:
                return xsd_element.raw_decode(obj, validation, context)

        if nm.XSI_TYPE in obj.attrib:
            if self.process_contents == 'strict':
                xsd_element = self.builders.create_element(
                    obj.tag, self.maps.validator, parent=self, form='unqualified'
                )
            else:
                xsd_element = self.builders.create_element(
                    obj.tag, self.maps.validator, self,
                    nillable='true', form='unqualified'
                )
            return xsd_element.raw_decode(obj, validation, context)

        if validation != 'skip' and self.process_contents == 'strict':
            context.validation_error(validation, self, reason, obj)

        xsd_element = self.builders.create_element(
            obj.tag, self.maps.validator, parent=self, form='unqualified'
        )
        return xsd_element.raw_decode(obj, validation, context)

    def raw_encode(self, obj: tuple[str, ElementType], validation: str,
                   context: EncodeContext) -> Any:
        name, value = obj
        namespace = get_namespace(name)

        if not self.is_namespace_allowed(namespace):
            reason = _("element {!r} is not allowed here").format(name)
            context.validation_error(validation, self, reason, value)

        if self.process_contents == 'skip' and not context.process_skipped:
            return Empty

        if not self.maps.loader.load_namespace(namespace):
            reason = _("unavailable namespace {!r}").format(namespace)
        else:
            try:
                xsd_element = self.maps.elements[name]
            except KeyError:
                reason = f"element {name!r} not found"
            else:
                return xsd_element.raw_encode(value, validation, context)

        # Check if there is a xsi:type attribute, but it has to extract
        # attributes using the converter instance.
        if self.process_contents == 'strict':
            xsd_element = self.builders.create_element(
                name, self.maps.validator, parent=self, form='unqualified'
            )
        else:
            xsd_element = self.builders.create_element(
                name, self.maps.validator, parent=self, nillable='true', form='unqualified'
            )

        try:
            element_data = context.converter.element_encode(value, xsd_element)
        except (ValueError, TypeError) as err:
            if validation != 'skip' and self.process_contents == 'strict':
                context.validation_error(validation, self, err, value)
        else:
            if nm.XSI_TYPE in element_data.attributes:
                return xsd_element.raw_encode(value, validation, context)

        if validation != 'skip' and self.process_contents == 'strict':
            context.validation_error(validation, self, reason)

        return self.maps.any_type.raw_encode(obj, validation, context)

    @schema_cache
    def is_overlap(self, other: ModelParticleType) -> bool:
        if not isinstance(other, XsdAnyElement):
            if isinstance(other, elements.XsdElement):
                return other.is_overlap(self)
            return False

        if self.not_namespace:
            if other.not_namespace:
                return True
            elif '##any' in other.namespace:
                return True
            elif '##other' in other.namespace:
                return True
            else:
                return any(ns not in self.not_namespace for ns in other.namespace)
        elif other.not_namespace:
            if '##any' in self.namespace:
                return True
            elif '##other' in self.namespace:
                return True
            else:
                return any(ns not in other.not_namespace for ns in self.namespace)
        elif self.namespace == other.namespace:
            return True
        elif '##any' in self.namespace or '##any' in other.namespace:
            return True
        elif '##other' in self.namespace:
            return any(ns and ns != self.target_namespace for ns in other.namespace)
        elif '##other' in other.namespace:
            return any(ns and ns != other.target_namespace for ns in self.namespace)
        else:
            return any(ns in self.namespace for ns in other.namespace)

    def is_consistent(self, other: SchemaElementType, **kwargs: Any) -> bool:
        return True


class XsdAnyAttribute(XsdWildcard, ValidationMixin[tuple[str, str], DecodedValueType]):
    """
    Class for XSD 1.0 *anyAttribute* wildcards.

    ..  <anyAttribute
          id = ID
          namespace = ((##any | ##other) | List of (anyURI | (##targetNamespace | ##local)) )
          processContents = (lax | skip | strict) : strict
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </anyAttribute>
    """
    copy: Callable[['XsdAnyAttribute'], 'XsdAnyAttribute']
    _ADMITTED_TAGS = nm.XSD_ANY_ATTRIBUTE,

    # Added for compatibility with protocol of XSD attributes
    use = None
    inheritable = False  # XSD 1.1 attributes

    def match(self, name: Optional[str], default_namespace: Optional[str] = None,
              resolve: bool = False, **kwargs: Any) -> Optional[SchemaAttributeType]:
        """
        Returns the attribute wildcard if name is matching the name provided
        as argument, `None` otherwise.

        :param name: a local or fully-qualified name.
        :param default_namespace: used when it's not `None` and not empty for \
        completing local name arguments.
        :param resolve: when `True` it doesn't return the wildcard but try to \
        resolve and return the attribute matching the name.
        :param kwargs: additional options that can be used by certain components.
        """
        if not name or not self.is_matching(name, default_namespace, **kwargs):
            return None
        elif not resolve:
            return self

        try:
            if name[0] != '{' and default_namespace:
                return self.maps.attributes[f'{{{default_namespace}}}{name}']
            else:
                return self.maps.attributes[name]
        except KeyError:
            return None

    def raw_decode(self, obj: tuple[str, str], validation: str,
                   context: ValidationContext) -> Union[DecodedValueType, EmptyType]:
        name, value = obj

        if not self.is_matching(name):
            reason = _("attribute %r not allowed") % name
            context.validation_error(validation, self, reason, obj)

        if self.process_contents == 'skip' and not context.process_skipped:
            return Empty

        namespace = get_namespace(name)
        if self.maps.loader.load_namespace(namespace):
            try:
                xsd_attribute = self.maps.attributes[name]
            except KeyError:
                if validation != 'skip' and self.process_contents == 'strict':
                    reason = _("attribute %r not found") % name
                    context.validation_error(validation, self, reason, obj)
            else:
                return xsd_attribute.raw_decode(value, validation, context)

        elif validation != 'skip' and self.process_contents == 'strict':
            reason = _("unavailable namespace {!r}").format(get_namespace(name))
            context.validation_error(validation, self, reason)

        return value

    def raw_encode(self, obj: tuple[str, AtomicValueType], validation: str,
                   context: EncodeContext) -> Union[str, None, EmptyType]:

        name, value = obj
        namespace = get_namespace(name)

        if not self.is_namespace_allowed(namespace):
            reason = _("attribute %r not allowed") % name
            context.validation_error(validation, self, reason, obj)

        if self.process_contents == 'skip' and not context.process_skipped:
            return Empty

        if self.maps.validator.load_namespace(namespace):
            try:
                xsd_attribute = self.maps.attributes[name]
            except KeyError:
                if validation != 'skip' and self.process_contents == 'strict':
                    reason = _("attribute %r not found") % name
                    context.validation_error(validation, self, reason, obj)
            else:
                return xsd_attribute.raw_encode(value, validation, context)

        elif validation != 'skip' and self.process_contents == 'strict':
            reason = _("unavailable namespace {!r}").format(namespace)
            context.validation_error(validation, self, reason)

        return raw_encode_value(value)


class Xsd11AnyElement(XsdAnyElement):
    """
    Class for XSD 1.1 *any* declarations.

    ..  <any
          id = ID
          maxOccurs = (nonNegativeInteger | unbounded)  : 1
          minOccurs = nonNegativeInteger : 1
          namespace = ((##any | ##other) | List of (anyURI | (##targetNamespace | ##local)) )
          notNamespace = List of (anyURI | (##targetNamespace | ##local))
          notQName = List of (QName | (##defined | ##definedSibling))
          processContents = (lax | skip | strict) : strict
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </any>
    """
    def _parse(self) -> None:
        super()._parse()
        self._parse_not_constraints()

    def is_matching(self, name: Optional[str],
                    default_namespace: Optional[str] = None,
                    group: Optional[ModelGroupType] = None,
                    occurs: Optional[OccursCounterType] = None,
                    **kwargs: Any) -> bool:
        """
        Returns `True` if the component name is matching the name provided as argument,
        `False` otherwise.

        :param name: a local or fully-qualified name.
        :param default_namespace: used by the XPath processor for completing \
        the name argument in case it's a local name.
        :param group: used only by XSD 1.1 any element wildcards to verify siblings in \
        case of ##definedSibling value in notQName attribute.
        :param occurs: a Counter instance for verify model occurrences counting.
        """
        if name is None:
            return False
        elif not name or name[0] == '{':
            if not self.is_namespace_allowed(get_namespace(name)):
                return False
        elif not default_namespace:
            if not self.is_namespace_allowed(''):
                return False
        else:
            name = f'{{{default_namespace}}}{name}'
            if not self.is_namespace_allowed(default_namespace):
                return False

        if group in self.precedences:
            if occurs is None:
                if any(e.is_matching(name) for e in self.precedences[group]):
                    return False
            elif any(e.is_matching(name) and not e.is_over(occurs)
                     for e in self.precedences[group]):
                return False

        if '##defined' in self.not_qname and name in self.maps.elements:
            return False
        if group and '##definedSibling' in self.not_qname:
            if any(e.is_matching(name) for e in group.iter_elements()
                   if not isinstance(e, XsdAnyElement)):
                return False

        return name not in self.not_qname

    def is_consistent(self, other: SchemaElementType, **kwargs: Any) -> bool:
        if isinstance(other, XsdAnyElement) or self.process_contents == 'skip':
            return True
        xsd_element = self.match(other.name, other.default_namespace, resolve=True)
        return xsd_element is None or other.is_consistent(xsd_element, strict=False)

    def add_precedence(self, other: ModelParticleType, group: ModelGroupType) -> None:
        try:
            self.precedences[group].append(other)
        except KeyError:
            self.precedences[group] = [other]


class Xsd11AnyAttribute(XsdAnyAttribute):
    """
    Class for XSD 1.1 *anyAttribute* declarations.

    ..  <anyAttribute
          id = ID
          namespace = ((##any | ##other) | List of (anyURI | (##targetNamespace | ##local)) )
          notNamespace = List of (anyURI | (##targetNamespace | ##local))
          notQName = List of (QName | ##defined)
          processContents = (lax | skip | strict) : strict
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </anyAttribute>
    """
    def _parse(self) -> None:
        super()._parse()
        self._parse_not_constraints()

    def is_matching(self, name: Optional[str],
                    default_namespace: Optional[str] = None,
                    **kwargs: Any) -> bool:
        if name is None:
            return False
        elif not name or name[0] == '{':
            namespace = get_namespace(name)
        elif not default_namespace:
            namespace = ''
        else:
            name = f'{{{default_namespace}}}{name}'
            namespace = default_namespace

        if '##defined' in self.not_qname and name in self.maps.attributes:
            xsd_attribute = self.maps.attributes[name]
            if isinstance(xsd_attribute, tuple):
                if xsd_attribute[1] is self.schema:
                    return False
            elif xsd_attribute.schema is self.schema:
                return False

        return name not in self.not_qname and self.is_namespace_allowed(namespace)


class XsdOpenContent(XsdComponent):
    """
    Class for XSD 1.1 *openContent* model definitions.

    ..  <openContent
          id = ID
          mode = (none | interleave | suffix) : interleave
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?), (any?)
        </openContent>
    """
    _ADMITTED_TAGS = nm.XSD_OPEN_CONTENT,
    mode = 'interleave'
    any_element = None  # type: Xsd11AnyElement

    def __init__(self, elem: ElementType, schema: SchemaType, parent: XsdComponent) -> None:
        super().__init__(elem, schema, parent)

    def __repr__(self) -> str:
        return '%s(mode=%r)' % (self.__class__.__name__, self.mode)

    def _parse(self) -> None:
        try:
            self.mode = self.elem.attrib['mode']
        except KeyError:
            pass
        else:
            if self.mode not in ('none', 'interleave', 'suffix'):
                msg = _("wrong value %r for 'mode' attribute")
                self.parse_error(msg % self.mode)

        child = self._parse_child_component(self.elem)
        if self.mode == 'none':
            if child is not None and child.tag == nm.XSD_ANY:
                msg = _("an openContent with mode='none' cannot "
                        "have an <xs:any> child declaration")
                self.parse_error(msg)
        elif child is None or child.tag != nm.XSD_ANY:
            self.parse_error(_("an <xs:any> child declaration is required"))
        else:
            self.any_element = Xsd11AnyElement(child, self.schema, self)

    @schema_cache
    def is_restriction(self, other: 'XsdOpenContent') -> bool:
        if other is None or other.mode == 'none':
            return self.mode == 'none'
        elif self.mode == 'interleave' and other.mode == 'suffix':
            return False
        else:
            return self.any_element.is_restriction(other.any_element)


class XsdDefaultOpenContent(XsdOpenContent):
    """
    Class for XSD 1.1 *defaultOpenContent* model definitions.

    ..  <defaultOpenContent
          appliesToEmpty = boolean : false
          id = ID
          mode = (interleave | suffix) : interleave
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, any)
        </defaultOpenContent>
    """
    _ADMITTED_TAGS = nm.XSD_DEFAULT_OPEN_CONTENT,
    applies_to_empty = False

    def __init__(self, elem: ElementType, schema: SchemaType) -> None:
        super(XsdOpenContent, self).__init__(elem, schema)

    def _parse(self) -> None:
        super()._parse()
        if self.parent is not None:
            msg = _("defaultOpenContent must be a child of the schema")
            self.parse_error(msg)
        if self.mode == 'none':
            msg = _("the attribute 'mode' of a defaultOpenContent cannot be 'none'")
            self.parse_error(msg)
        if self._parse_child_component(self.elem) is None:
            msg = _("a defaultOpenContent declaration cannot be empty")
            self.parse_error(msg)

        if 'appliesToEmpty' in self.elem.attrib:
            if self.elem.attrib['appliesToEmpty'].strip() in ('true', '1'):
                self.applies_to_empty = True

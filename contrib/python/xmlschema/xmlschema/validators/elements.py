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
This module contains classes for XML Schema elements, complex types and model groups.
"""
import warnings
from copy import copy as _copy
from decimal import Decimal
from types import GeneratorType
from collections.abc import Iterator, MutableSequence
from typing import TYPE_CHECKING, cast, Any, Optional, Union
from xml.etree.ElementTree import Element, ParseError

from elementpath import XPath2Parser, ElementPathError, XPathContext, XPathToken, \
    LazyElementNode, SchemaElementNode, build_schema_node_tree
from elementpath.datatypes import AbstractDateTime, Duration
from elementpath.xpath_nodes import EtreeElementNode

import xmlschema.names as nm
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError, \
    XMLResourceParseError
from xmlschema.aliases import ElementType, BaseXsdType, SchemaElementType, \
    ModelParticleType, ComponentClassType, DecodeType, DecodedValueType
from xmlschema.translation import gettext as _
from xmlschema.utils.etree import iter_schema_location_hints, iter_schema_namespaces
from xmlschema.utils.decoding import Empty, raw_encode_attributes, strictly_equal
from xmlschema.utils.qnames import get_qname
from xmlschema.arguments import XSD_VALIDATION_MODES
from xmlschema import dataobjects
from xmlschema.converters import ElementData
from xmlschema.xpath import XMLSchemaProxy, ElementPathMixin, XPathElement
from xmlschema.caching import schema_cache

from .exceptions import XMLSchemaValidationError, XMLSchemaParseError, \
    XMLSchemaStopValidation, XMLSchemaTypeTableWarning
from .validation import ValidationContext, DecodeContext, EncodeContext, ValidationMixin
from .helpers import parse_xsd_derivation, parse_xpath_default_namespace
from .xsdbase import XSD_TYPE_DERIVATIONS, XSD_ELEMENT_DERIVATIONS, XsdComponent
from .particles import ParticleMixin, OccursCalculator
from .identities import XsdIdentity, XsdKeyref, KeyrefCounter, FieldValueSelector
from .simple_types import XsdSimpleType
from .attributes import XsdAttribute
from .wildcards import XsdAnyElement

if TYPE_CHECKING:
    from .attributes import XsdAttributeGroup  # noqa: F401
    from .groups import XsdGroup  # noqa: F401

DataBindingType = Union[type['dataobjects.DataElement'], 'dataobjects.DataBindingMeta']


class XsdElement(XsdComponent, ParticleMixin,
                 ElementPathMixin[SchemaElementType],
                 ValidationMixin[ElementType, Any]):
    """
    Class for XSD 1.0 *element* declarations.

    ..  <element
          abstract = boolean : false
          block = (#all | List of (extension | restriction | substitution))
          default = string
          final = (#all | List of (extension | restriction))
          fixed = string
          form = (qualified | unqualified)
          id = ID
          maxOccurs = (nonNegativeInteger | unbounded)  : 1
          minOccurs = nonNegativeInteger : 1
          name = NCName
          nillable = boolean : false
          ref = QName
          substitutionGroup = QName
          type = QName
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, ((simpleType | complexType)?, (unique | key | keyref)*))
        </element>
    """
    name: str
    local_name: str
    qualified_name: str
    prefixed_name: str
    target_namespace: str

    parent: Optional['XsdGroup']
    ref: Optional['XsdElement']

    attributes: 'XsdAttributeGroup'
    """The group of the attributes associated with the element."""

    content: Union[tuple[()], 'XsdGroup']

    abstract: bool = False
    """
    Defines whether the element can be used in an instance document. An abstract
    element must be global and can still be the head of a substitution group.
    """

    nillable: bool = False
    """
    Defines whether the element content is nillable using xsi:nil="true" as attribute.
    """

    form: Optional[str] = None
    qualified: bool = False
    """
    The effective form for the element. If `True` the element name is qualified by a
    braced namespace URI as prefix. The name of a global element is always qualified.
    """

    default: Optional[str] = None
    """The default value of the element if its content is a simple type."""

    fixed: Optional[str] = None
    """The fixed value of the element if its content is a simple type."""

    substitution_group: Optional[str] = None

    substitutes: set[str] | tuple[()] = ()
    identities: list[XsdIdentity]
    selected_by: set[XsdIdentity]
    xsi_types: set[BaseXsdType]
    alternatives: Union[tuple[()], list['XsdAlternative']] = ()
    inheritable: Union[tuple[()], dict[str, XsdAttribute]] = ()

    _ADMITTED_TAGS = nm.XSD_ELEMENT,
    _block: Optional[str] = None
    _final: Optional[str] = None
    _built: bool | None
    binding: Optional[DataBindingType] = None

    __slots__ = ('type', 'selected_by', 'xsi_types', 'identities',
                 'content', 'attributes', 'min_occurs', 'max_occurs')

    def __repr__(self) -> str:
        return '%s(%s=%r, occurs=%r)' % (
            self.__class__.__name__,
            'name' if self.ref is None else 'ref',
            self.prefixed_name,
            list(self.occurs)
        )

    def _set_type(self, value: BaseXsdType) -> None:
        self.type: BaseXsdType = value
        """The XSD simpleType or complexType of the element."""

        if isinstance(value, XsdSimpleType):
            self.attributes = self.builders.create_empty_attribute_group(self)
            self.content = ()
        else:
            self.attributes = value.attributes
            if isinstance(value.content, XsdSimpleType):
                self.content = ()
            else:
                self.content = value.content

    def __iter__(self) -> Iterator[SchemaElementType]:
        if self.content:
            yield from self.content.elements

    def build(self) -> None:
        if self._built is False:
            self._built = None
            try:
                self._parse()
                self._built = True
            finally:
                if self._built is None:
                    self._built = False

    def _parse(self) -> None:
        if self._built is not None and isinstance(self.parent, MutableSequence):
            return

        self.min_occurs = self.max_occurs = 1
        self.selected_by = set()
        self.xsi_types = set()

        self._parse_particle(self.elem)
        self._parse_attributes()

        if self.ref is None:
            self._parse_type()
            self._parse_constraints()

            if self.parent is None:
                self.substitutes = set()
                if 'substitutionGroup' in self.elem.attrib:
                    self._parse_substitution_group(self.elem.attrib['substitutionGroup'])

        self._built = True

    def _parse_attributes(self) -> None:
        attrib = self.elem.attrib
        if self._parse_reference():
            try:
                xsd_element: XsdElement = self.maps.elements[self.name]
            except KeyError:
                self._set_type(self.maps.any_type)
                self.parse_error(_('unknown element %r') % self.name)
            else:
                self.ref = xsd_element
                self._set_type(xsd_element.type)
                self.target_namespace = xsd_element.target_namespace
                self.abstract = xsd_element.abstract
                self.nillable = xsd_element.nillable
                self.qualified = xsd_element.qualified
                self.substitutes = xsd_element.substitutes
                self.form = xsd_element.form
                self.default = xsd_element.default
                self.fixed = xsd_element.fixed
                self.substitution_group = xsd_element.substitution_group
                self.identities = xsd_element.identities
                self.alternatives = xsd_element.alternatives
                self.selected_by = xsd_element.selected_by
                self.xsi_types = xsd_element.xsi_types

            for attr_name in ('type', 'nillable', 'default', 'fixed', 'form',
                              'block', 'abstract', 'final', 'substitutionGroup'):
                if attr_name in attrib:
                    msg = _("attribute {!r} is not allowed when element reference is used")
                    self.parse_error(msg.format(attr_name))
            return

        if 'form' in attrib:
            self.form = attrib['form']
            if self.form == 'qualified':
                self.qualified = True
        elif self.schema.element_form_default == 'qualified':
            self.qualified = True

        try:
            if self.parent is None or self.qualified:
                self.name = get_qname(self.target_namespace, attrib['name'])
            else:
                self.name = attrib['name']
        except KeyError:
            pass

        if 'abstract' in attrib:
            if self.parent is not None:
                msg = _("local scope elements cannot have abstract attribute")
                self.parse_error(msg)
            if attrib['abstract'].strip() in ('true', '1'):
                self.abstract = True

        if 'block' in attrib:
            self._block = parse_xsd_derivation(
                self.elem, 'block', XSD_ELEMENT_DERIVATIONS, self
            )

        if 'nillable' in attrib and attrib['nillable'].strip() in ('true', '1'):
            self.nillable = True

        if self.parent is None:
            if 'final' in attrib:
                self._final = parse_xsd_derivation(
                    self.elem, 'final', XSD_TYPE_DERIVATIONS, self
                )

            for attr_name in ('ref', 'form', 'minOccurs', 'maxOccurs'):
                if attr_name in attrib:
                    msg = _("attribute {!r} is not allowed in a global element declaration")
                    self.parse_error(msg.format(attr_name))
        else:
            for attr_name in ('final', 'substitutionGroup'):
                if attr_name in attrib:
                    msg = _("attribute {!r} not allowed in a local element declaration")
                    self.parse_error(msg.format(attr_name))

    def _parse_type(self) -> None:
        type_name = self.elem.get('type')
        if type_name is not None:
            try:
                extended_name = self.schema.resolve_qname(type_name)
            except (KeyError, ValueError, RuntimeError) as err:
                self.parse_error(err)
                self._set_type(self.maps.any_type)
            else:
                if extended_name == nm.XSD_ANY_TYPE:
                    self._set_type(self.maps.any_type)
                else:
                    try:
                        self._set_type(self.maps.types[extended_name])
                    except KeyError:
                        self.parse_error(_('unknown type {!r}').format(type_name))
                        self._set_type(self.maps.any_type)
            finally:
                child = self._parse_child_component(self.elem, strict=False)
                if child is not None and child.tag in nm.GLOBAL_TYPES_TAGS:
                    msg = _("the attribute 'type' and a xs:{} local "
                            "declaration are mutually exclusive")
                    self.parse_error(msg.format(child.tag.split('}')[-1]))
        elif (child := self._parse_child_component(self.elem, strict=False)) is None:
            self._set_type(self.maps.any_type)
        else:
            try:
                self._set_type(
                    self.builders.local_types[child.tag](child, self.schema, self)
                )
            except KeyError:
                self._set_type(self.maps.any_type)

    def _parse_constraints(self) -> None:
        # Value constraints
        if 'default' in self.elem.attrib:
            self.default = self.elem.attrib['default']
            if 'fixed' in self.elem.attrib:
                msg = _("'default' and 'fixed' attributes are mutually exclusive")
                self.parse_error(msg)

            if not self.type.text_is_valid(self.default):
                msg = _("'default' value {!r} is not compatible with element's type")
                self.parse_error(msg.format(self.default))
                self.default = None
            elif self.xsd_version == '1.0' and self.type.is_key():
                msg = _("xs:ID or a type derived from xs:ID cannot have a default value")
                self.parse_error(msg)

        elif 'fixed' in self.elem.attrib:
            self.fixed = self.elem.attrib['fixed']
            if not self.type.text_is_valid(self.fixed):
                msg = _("'fixed' value {!r} is not compatible with element's type")
                self.parse_error(msg.format(self.fixed))
                self.fixed = None
            elif self.xsd_version == '1.0' and self.type.is_key():
                msg = _("xs:ID or a type derived from xs:ID cannot have a fixed value")
                self.parse_error(msg)

        # Identity constraints
        self.identities = []
        for child in self.elem:
            if child.tag in nm.IDENTITY_TAGS:
                identity = self.builders.identities[child.tag](child, self.schema, self)
                if identity.ref:
                    if any(identity.name == x.name for x in self.identities):
                        msg = _("duplicated identity constraint %r:")
                        self.parse_error(msg % identity.name, child)

                    self.identities.append(identity)
                    continue

                try:
                    if child != self.maps.identities[identity.name].elem:
                        msg = _("duplicated identity constraint %r:")
                        self.parse_error(msg % identity.name, child)
                except KeyError:
                    self.maps.identities[identity.name] = identity
                finally:
                    self.identities.append(identity)

    def _parse_substitution_group(self, substitution_group: str) -> None:
        try:
            substitution_group_qname = self.schema.resolve_qname(substitution_group)
        except (KeyError, ValueError, RuntimeError) as err:
            self.parse_error(err)
            return
        else:
            if substitution_group_qname[0] != '{':
                substitution_group_qname = get_qname(
                    self.target_namespace, substitution_group_qname
                )

        try:
            head_element = self.maps.elements[substitution_group_qname]
        except KeyError:
            msg = _("unknown substitutionGroup %r")
            self.parse_error(msg % substitution_group)
            return
        else:
            if isinstance(head_element, tuple):
                msg = _("circularity found for substitutionGroup %r")
                self.parse_error(msg % substitution_group)
                return
            elif 'substitution' in head_element.block:
                return

        final = head_element.final
        if self.type.name == nm.XSD_ANY_TYPE and 'type' not in self.elem.attrib:
            if head_element.type.name != nm.XSD_ANY_TYPE:
                # Set the type with head element's type for validate content
                # ref: https://www.w3.org/TR/xmlschema-1/#cElement_Declarations
                self.type = head_element.type
        elif not self.type.is_derived(head_element.type):
            msg = _("{0!r} type is not of the same or a derivation "
                    "of the head element {1!r} type")
            self.parse_error(msg.format(self, head_element))
        elif final == '#all' or 'extension' in final and 'restriction' in final:
            msg = _("head element %r can't be substituted by an "
                    "element that has a derivation of its type")
            self.parse_error(msg % head_element)
        elif 'extension' in final and self.type.is_derived(head_element.type, 'extension'):
            msg = _("head element %r can't be substituted by an "
                    "element that has an extension of its type")
            self.parse_error(msg % head_element)
        elif 'restriction' in final and self.type.is_derived(head_element.type, 'restriction'):
            msg = _("head element %r can't be substituted by an "
                    "element that has a restriction of its type")
            self.parse_error(msg % head_element)

        try:
            self.maps.substitution_groups[substitution_group_qname].add(self)
        except KeyError:
            self.maps.substitution_groups[substitution_group_qname] = {self}
        finally:
            self.substitution_group = substitution_group_qname

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

    @property
    def scope(self) -> str:
        """The scope of the element declaration that can be 'global' or 'local'."""
        return 'global' if self.parent is None else 'local'

    @property
    def value_constraint(self) -> Optional[str]:
        """The fixed or the default value if either is defined, `None` otherwise."""
        return self.fixed if self.fixed is not None else self.default

    @property
    def final(self) -> str:
        """
        The effective value for prevent the usage of derived elements. Can be empty, '#all'
        or containing a subset of words (extension|restrictions) separated by a space.
        """
        if self.ref is not None:
            return self.ref.final
        elif self._final is not None:
            return self._final
        return self.schema.final_default

    @property
    def block(self) -> str:
        """
        The effective value for blocking the derivation of the element. Can be empty, '#all' or
        containing a subset of words (extension|restrictions|substitution) separated by a space.
        """
        if self.ref is not None:
            return self.ref.block
        elif self._block is not None:
            return self._block
        return self.schema.block_default

    def overall_min_occurs(self, particle: ModelParticleType) -> int:
        """
        Returns the overall minimum for occurrences of a content model particle.
        The content type of the element must be 'element-only' or 'mixed'.
        """
        return self.type.overall_min_occurs(particle)

    def overall_max_occurs(self, particle: ModelParticleType) -> Optional[int]:
        """
        Returns the overall maximum for occurrences of a content model particle.
        The content type of the element must be 'element-only' or 'mixed'.
        """
        return self.type.overall_max_occurs(particle)

    def get_binding(self, *bases: type[Any], replace_existing: bool = False, **attrs: Any) \
            -> DataBindingType:
        """
        Gets data object binding for XSD element, creating a new one if it doesn't exist.

        :param bases: base classes to use for creating the binding class.
        :param replace_existing: provide `True` to replace an existing binding class.
        :param attrs: attribute and method definitions for the binding class body.
        """
        if self.binding is None or replace_existing:
            if not bases:
                bases = (dataobjects.DataElement,)
            attrs['xsd_element'] = self
            class_name = '{}Binding'.format(self.local_name.title().replace('_', ''))
            self.binding = dataobjects.DataBindingMeta(class_name, bases, attrs)

        return self.binding

    def get_alternative_type(self, elem: Union[ElementType, ElementData],
                             inherited: Optional[dict[str, Any]] = None) -> BaseXsdType:
        return self.type

    def get_attributes(self, xsd_type: BaseXsdType) -> 'XsdAttributeGroup':
        if not isinstance(xsd_type, XsdSimpleType):
            return xsd_type.attributes
        elif xsd_type is self.type:
            return self.attributes
        else:
            return self.builders.create_empty_attribute_group(self)

    def get_path(self, ancestor: Optional[XsdComponent] = None,
                 reverse: bool = False) -> Optional[str]:
        """
        Returns the XPath expression of the element. The path is relative to the schema instance
        in which the element is contained or is relative to a specific ancestor passed as argument.
        In the latter case returns `None` if the argument is not an ancestor.

        :param ancestor: optional XSD component of the same schema, that maybe \
        an ancestor of the element.
        :param reverse: if set to `True` returns the reverse path, from the element to ancestor.
        """
        path: list[str] = []
        xsd_component: Optional[XsdComponent] = self
        while xsd_component is not None:
            if xsd_component is ancestor:
                return '/'.join(reversed(path)) or '.'
            elif isinstance(xsd_component, XsdElement):
                path.append('..' if reverse else xsd_component.name)
            xsd_component = xsd_component.parent
        else:
            if ancestor is None:
                return '/'.join(reversed(path)) or '.'
            return None

    def iter_components(self, xsd_classes: Optional[ComponentClassType] = None) \
            -> Iterator[XsdComponent]:

        if xsd_classes is None:
            yield self
            yield from self.identities
        else:
            if isinstance(self, xsd_classes):
                yield self
            if issubclass(XsdIdentity, xsd_classes):
                yield from self.identities

        if self.ref is None and self.type.parent is not None:
            yield from self.type.iter_components(xsd_classes)

    def iter_substitutes(self) -> Iterator['XsdElement']:
        if self.parent is None or self.ref is not None:
            if substitutes := self.maps.substitution_groups.get(self.name):
                for xsd_element in substitutes:
                    if not xsd_element.abstract:
                        yield xsd_element
                    for e in xsd_element.iter_substitutes():
                        if not e.abstract:
                            yield e

    def data_value(self, elem: ElementType) -> DecodedValueType:
        """Returns the decoded data value of the provided element as XPath fn:data()."""
        text = elem.text
        if text is None:
            text = self.fixed if self.fixed is not None else self.default
            if text is None:
                return '' if self.type.text_is_valid('') else None
        return self.type.text_decode(text)

    def check_dynamic_context(self, elem: ElementType, validation: str,
                              context: ValidationContext) -> None:
        for ns, url in iter_schema_location_hints(elem):
            if self.maps.get_schema(ns, url, context.source.base_url) is not None:
                continue

            if ns in iter_schema_namespaces(context.source.root, elem):
                reason = _("schemaLocation declaration after namespace start")
                context.validation_error(validation, self, reason, elem)

            try:
                with self.maps.protect_status():
                    if ns in self.maps.namespaces:
                        schema = self.maps.namespaces[ns][0]
                        schema.include_schema(url, context.source.base_url)
                    else:
                        schema = self.schema
                        schema.import_schema(ns, url, context.source.base_url)
                    schema.clear()
                    schema.build()

            except (XMLSchemaValidationError, XMLResourceParseError) as err:
                context.validation_error(validation, self, err, elem)
            except XMLSchemaParseError as err:
                context.validation_error(validation, self, err.message, elem)
            except OSError:
                continue

    def raw_decode(self, obj: ElementType, validation: str, context: ValidationContext) -> Any:
        """
        Decode an Element instance.

        :param obj: the Element that has to be decoded.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip'.
        :param context: the decoding context.
        :return: a decoded object.
        """
        error: Union[XMLSchemaValueError, XMLSchemaValidationError]
        result: Any

        if self.abstract:
            if self.name == obj.tag:
                reason = _("can't use an abstract element in an instance")
                context.validation_error(validation, self, reason, obj)
            elif self.name not in self.maps.substitution_groups:
                reason = _("can't use an abstract XSD element for validation "
                           "unless it's the head of a substitution group")
                context.validation_error(validation, self, reason, obj)
            else:
                for xsd_element in self.iter_substitutes():
                    if obj.tag == xsd_element.name:
                        return xsd_element.raw_decode(obj, validation, context)
                else:
                    reason = _("can't use an abstract XSD element for validation")
                    context.validation_error(validation, self, reason, obj)

        if context.validation_hook is not None:
            # Control validation on element and its descendants or stop validation
            _validation = context.validation_hook(obj, self)
            if _validation:
                if isinstance(_validation, str) and _validation in XSD_VALIDATION_MODES:
                    context = _copy(context)
                    validation = _validation
                else:
                    return Empty

        context.elem = obj

        for identity in self.identities:
            if identity in context.identities:
                context.identities[identity].reset(obj)
            else:
                context.identities[identity] = identity.get_counter(obj)

        if not context.level:
            # Need to set converter context with the right object (the resource can be lazy)
            context.converter.set_xmlns_context(obj, context.level)
        elif context.use_location_hints:
            # Use location hints for dynamic schema load
            self.check_dynamic_context(obj, validation, context)

        inherited = context.inherited
        value = content = None
        nilled = False

        if not self.alternatives:
            xsd_type = self.type
        else:
            xsd_type = self.get_alternative_type(obj, inherited)

        if nm.XSI_TYPE in obj.attrib and self.schema.meta_schema is not None:
            # Meta-schema elements ignore xsi:type (issue #350)
            type_name = obj.attrib[nm.XSI_TYPE].strip()
            try:
                xsd_type = self.maps.get_instance_type(
                    type_name, xsd_type, context.namespaces
                )
            except (KeyError, TypeError) as err:
                context.validation_error(validation, self, err, obj)
            else:
                if xsd_type.is_blocked(self):
                    reason = _("usage of %r is blocked") % xsd_type
                    context.validation_error(validation, self, reason, obj)
                elif xsd_type not in self.xsi_types:
                    self.xsi_types.add(xsd_type)

                    # For complex contents augments permanently the XSD elements
                    # that collect keys/keyrefs for enabled identities.
                    if xsd_type.has_complex_content():
                        xpath_element = XPathElement(self.name, xsd_type)
                        for counter in context.identities.values():
                            if counter.enabled:
                                try:
                                    counter.identity.update_elements(xpath_element)
                                except TypeError as e:
                                    context.validation_error(validation, self, e, obj)

        if xsd_type.abstract:
            reason = _("%r is abstract") % xsd_type
            context.validation_error(validation, self, reason, obj)

        id_list = context.id_list
        if xsd_type.is_complex() and self.xsd_version == '1.1':
            # Track XSD 1.1 multiple xs:ID attributes/children
            context.id_list = []

        content_decoder = xsd_type if isinstance(xsd_type, XsdSimpleType) else xsd_type.content

        # Decode attributes
        attribute_group = self.get_attributes(xsd_type)
        context.level += 1
        attributes = attribute_group.raw_decode(obj.attrib, validation, context)
        context.level -= 1

        if self.inheritable and any(name in self.inheritable for name in obj.attrib):
            if inherited:
                inherited = inherited.copy()
                inherited.update((k, v) for k, v in obj.attrib.items() if k in self.inheritable)
            else:
                inherited = {k: v for k, v in obj.attrib.items() if k in self.inheritable}
            context = _copy(context)
            context.inherited = inherited

        # Checks the xsi:nil attribute of the instance
        if nm.XSI_NIL in obj.attrib:
            xsi_nil = obj.attrib[nm.XSI_NIL].strip()
            if not self.nillable:
                reason = _("element is not nillable")
                context.validation_error(validation, self, reason, obj)
            elif xsi_nil not in ('0', '1', 'false', 'true'):
                reason = _("xsi:nil attribute must have a boolean value")
                context.validation_error(validation, self, reason, obj)
            elif xsi_nil in ('0', 'false'):
                pass
            elif self.fixed is not None:
                reason = _("xsi:nil='true' but the element has a fixed value")
                context.validation_error(validation, self, reason, obj)
            elif obj.text is not None or len(obj):
                reason = _("xsi:nil='true' but the element is not empty")
                context.validation_error(validation, self, reason, obj)
            else:
                nilled = True

        if xsd_type.is_empty() and obj.text and xsd_type.normalize(obj.text):
            reason = _("character data is not allowed because content is empty")
            context.validation_error(validation, self, reason, obj)

        if nilled:
            pass
        elif not isinstance(content_decoder, XsdSimpleType):
            if not isinstance(xsd_type, XsdSimpleType):
                for assertion in xsd_type.assertions:
                    assertion(obj, validation, context)

            context.level += 1
            content = content_decoder.raw_decode(obj, validation, context)
            context.level -= 1

            if content and len(content) == 1 and content[0][0] == 1:
                value, content = content[0][1], None

            if self.fixed is not None and \
                    (len(obj) > 0 or value is not None and self.fixed != value):
                reason = _("must have the fixed value %r") % self.fixed
                context.validation_error(validation, self, reason, obj)

        else:
            if len(obj):
                reason = _("a simple content element can't have child elements")
                context.validation_error(validation, self, reason, obj)

            text = obj.text
            if self.fixed is not None:
                if not text:
                    text = self.fixed
                elif text == self.fixed:
                    pass
                elif not strictly_equal(xsd_type.text_decode(text, context=context),
                                        xsd_type.text_decode(self.fixed)):
                    reason = _("must have the fixed value %r") % self.fixed
                    context.validation_error(validation, self, reason, obj)

            elif not text and self.default is not None and context.use_defaults:
                text = self.default

            if not isinstance(xsd_type, XsdSimpleType):
                for assertion in xsd_type.assertions:
                    assertion(obj, validation, context, value=text)

                if text and content_decoder.is_list():
                    value = text.split()
                else:
                    value = text

            elif xsd_type.is_notation():
                if xsd_type.name == nm.XSD_NOTATION_TYPE:
                    msg = _("cannot validate against xs:NOTATION directly, "
                            "only against a subtype with an enumeration facet")
                    context.validation_error(validation, self, msg, text)
                elif not xsd_type.enumeration:
                    msg = _("missing enumeration facet in xs:NOTATION subtype")
                    context.validation_error(validation, self, msg, text)

            result = content_decoder.raw_decode(text or '', validation, context)
            if not isinstance(context, DecodeContext):
                value = result
            else:
                if result is None and context.filler is not None:
                    value = context.filler(self)
                elif text or context.keep_empty:
                    value = result

                if context.value_hook is not None:
                    value = context.value_hook(value, xsd_type)
                elif isinstance(value, context.keep_datatypes) or value is None:
                    pass
                elif isinstance(value, str):
                    if value[:1] == '{' and xsd_type.is_qname():
                        value = text
                elif isinstance(value, Decimal):
                    if context.decimal_type is not None:
                        value = context.decimal_type(value)
                elif isinstance(value, (AbstractDateTime, Duration)):
                    value = str(value) if text is None else text.strip()
                else:
                    value = str(value)

        context.id_list = id_list
        xmlns = context.converter.set_xmlns_context(obj, context.level)  # Purge sub-contexts

        if isinstance(context, DecodeContext):
            element_data = ElementData(obj.tag, value, content, attributes, xmlns)
            if context.element_hook is not None:
                element_data = context.element_hook(element_data, self, xsd_type)

            try:
                result = context.converter.element_decode(
                    element_data, self, xsd_type, context.level
                )
            except (ValueError, TypeError) as err:
                context.validation_error(validation, self, err, obj)
                result = None
        elif not context.level:
            result = ElementData(obj.tag, value, None, attributes, None)
        else:
            result = None

        if content is not None:
            del content

        if self.selected_by:
            self.collect_key_fields(obj, xsd_type, validation, nilled, context)

        # Apply non XSD optional validations
        if context.extra_validator is not None:
            try:
                errors = context.extra_validator(obj, self)
            except XMLSchemaValidationError as err:
                context.validation_error(validation, self, err, obj)
            else:
                if isinstance(errors, GeneratorType):
                    for error in errors:
                        context.validation_error(validation, self, error, obj)

        # Disable collect for out of scope identities and check key references
        if context.max_depth is None:
            for identity in self.identities:
                counter = context.identities[identity]
                counter.enabled = False
                if isinstance(identity, XsdKeyref):
                    assert isinstance(counter, KeyrefCounter)
                    for error in counter.iter_errors(context.identities):
                        context.validation_error(validation, self, error, obj)
        elif context.level:
            for identity in self.identities:
                context.identities[identity].enabled = False

        return result

    def collect_key_fields(self, obj: ElementType, xsd_type: BaseXsdType,
                           validation: str, nilled: bool, context: ValidationContext) -> None:

        element_node: Union[EtreeElementNode, LazyElementNode]
        element_node = cast(EtreeElementNode, context.source.get_xpath_node(obj))

        xsd_element = self if self.ref is None else self.ref
        if xsd_element.type is not xsd_type:
            xsd_element = _copy(xsd_element)
            xsd_element._set_type(xsd_type)

        # Collect field values for identities that refer to this XSD element.
        for identity in self.selected_by:
            try:
                counter = context.identities[identity]
            except KeyError:
                continue
            else:
                if not counter.enabled:
                    continue

            if counter.elements is None:
                # Apply selector on Element ancestor for obtain the selected elements
                root_node = context.source.get_xpath_node(counter.elem)
                xpath_context = XPathContext(root_node)
                assert identity.selector is not None
                counter.elements = {
                    x for x in identity.selector.token.select_results(xpath_context)
                }

            if obj not in counter.elements:
                continue

            if xsd_element in identity.elements:
                selectors = identity.elements[xsd_element]
            else:
                # noinspection PyTypeChecker
                selectors = [FieldValueSelector(f, xsd_element) for f in identity.fields]

            try:
                fields = tuple(
                    s.get_value(element_node, context.namespaces) for s in selectors
                )
            except (XMLSchemaValueError, XMLSchemaTypeError) as err:
                context.validation_error(validation, self, err, obj)
            else:
                if any(x is not None for x in fields) or nilled:
                    try:
                        counter.increase(fields)
                    except ValueError as err:
                        context.validation_error(validation, self, err, obj)

    def to_objects(self, obj: ElementType, with_bindings: bool = False, **kwargs: Any) \
            -> DecodeType['dataobjects.DataElement']:
        """
        Decodes XML data to Python data objects.

        :param obj: the XML data source.
        :param with_bindings: if `True` is provided the decoding is done using \
        :class:`DataBindingConverter` that used XML data binding classes. For \
        default the objects are instances of :class:`DataElement` and uses the \
        :class:`DataElementConverter`.
        :param kwargs: other optional keyword arguments for the method \
        :func:`iter_decode`, except the argument *converter*.
        """
        if with_bindings:
            return self.decode(obj, converter=dataobjects.DataBindingConverter, **kwargs)
        return self.decode(obj, converter=dataobjects.DataElementConverter, **kwargs)

    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) \
            -> Optional[ElementType]:
        """
        Encode data to an Element.

        :param obj: the data that has to be encoded.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip'.
        :param context: the encoding context.
        :return: returns an Element.
        """
        errors: list[Union[str, Exception]] = []

        try:
            element_data = context.converter.element_encode(obj, self, context.level)
        except (ValueError, TypeError) as err:
            context.validation_error(validation, self, err, obj)
            return None

        if context.max_depth is not None and context.max_depth == 0 and not context.level:
            return None

        tag, text, content, attributes, xmlns = element_data
        context.elem = elem = context.create_element(tag)

        if self.abstract:
            if self.name == tag and context.converter.losslessly:
                reason = _("can't use an abstract element in an instance")
                context.validation_error(validation, self, reason, obj)
            elif self.name not in self.maps.substitution_groups:
                reason = _("can't use an abstract XSD element for validation "
                           "unless it's the head of a substitution group")
                context.validation_error(validation, self, reason, obj)
            else:
                for xsd_element in self.iter_substitutes():
                    if tag == xsd_element.name:
                        return xsd_element.raw_encode(obj, validation, context)
                else:
                    # In some cases the original tag could be missed, so try each
                    # substitute before generate an error.
                    for xsd_element in self.iter_substitutes():
                        return xsd_element.raw_encode(obj, validation, context)
                    else:
                        reason = _("can't use an abstract XSD element for validation")
                        context.validation_error(validation, self, reason, obj)

        if not self.alternatives:
            xsd_type = self.type
        else:
            xsd_type = self.get_alternative_type(element_data)

        if nm.XSI_TYPE in attributes and self.schema.meta_schema is not None:
            type_name = attributes[nm.XSI_TYPE].strip()
            try:
                xsd_type = self.maps.get_instance_type(
                    type_name, xsd_type, context.namespaces
                )
            except (KeyError, TypeError) as err:
                errors.append(err)
            else:
                default_namespace = context.converter.get('')
                if default_namespace and not isinstance(xsd_type, XsdSimpleType):
                    # Adjust attributes mapped into default namespace

                    ns_part = f'{{{default_namespace}}}'
                    for k in list(attributes):
                        if not k.startswith(ns_part):
                            continue
                        elif k in xsd_type.attributes:
                            continue

                        local_name = k[len(ns_part):]
                        if local_name in xsd_type.attributes:
                            attributes[local_name] = attributes[k]
                            del attributes[k]

        attribute_group = self.get_attributes(xsd_type)
        context.level += 1
        try:
            elem.attrib.update(attribute_group.raw_encode(attributes, validation, context))
        except XMLSchemaValidationError:
            elem.attrib.update(raw_encode_attributes(attributes))
            raise

        context.level -= 1

        if nm.XSI_NIL in attributes:
            xsi_nil = attributes[nm.XSI_NIL].strip()
            if not self.nillable:
                errors.append("element is not nillable.")
            elif xsi_nil not in ('0', '1', 'true', 'false'):
                errors.append("xsi:nil attribute must has a boolean value.")
            elif xsi_nil in ('0', 'false'):
                pass
            elif self.fixed is not None:
                errors.append("xsi:nil='true' but the element has a fixed value.")
            elif text not in (None, '') or content:
                errors.append("xsi:nil='true' but the element is not empty.")
            else:
                for e in errors:
                    context.validation_error(validation, self, e, elem)
                return elem

        if isinstance(xsd_type, XsdSimpleType):
            if content:
                errors.append("a simpleType element can't have child elements.")

            if text is not None:
                try:
                    elem.text = xsd_type.raw_encode(text, validation, context)
                except XMLSchemaValidationError as err:
                    if err.elem is not None:
                        raise
                    errors.append(err)

            elif self.fixed is not None:
                elem.text = self.fixed
            elif self.default is not None and context.use_defaults:
                elem.text = self.default

        elif isinstance(xsd_type.content, XsdSimpleType):
            if xsd_type.content.max_length == 0:
                pass
            elif text is not None:
                try:
                    elem.text = xsd_type.content.raw_encode(text, validation, context)
                except XMLSchemaValidationError as err:
                    if err.elem is not None:
                        raise
                    errors.append(err)

            elif self.fixed is not None:
                elem.text = self.fixed
            elif self.default is not None and context.use_defaults:
                elem.text = self.default

        else:
            context.level += 1
            xsd_type.content.raw_encode(element_data, validation, context)
            context.level -= 1

        if errors:
            for e in errors:
                context.validation_error(validation, self, e, elem)

        del element_data
        return elem

    def is_matching(self, name: Optional[str], default_namespace: Optional[str] = None,
                    group: Optional['XsdGroup'] = None, **kwargs: Any) -> bool:
        if not name:
            return False
        elif default_namespace and name[0] != '{':
            name = f'{{{default_namespace}}}{name}'

            # Workaround for backward compatibility of XPath selectors on schemas.
            if not self.qualified and default_namespace == self.target_namespace:
                return (name == self.qualified_name or
                        any(name == e.qualified_name for e in self.iter_substitutes()))

        return name == self.name or name in self.substitutes

    def match(self, name: Optional[str], default_namespace: Optional[str] = None,
              **kwargs: Any) -> Optional['XsdElement']:
        if not name:
            return None
        elif default_namespace and name[0] != '{':
            name = f'{{{default_namespace}}}{name}'

        if name == self.name:
            return self
        elif name in self.substitutes:
            return self.maps.elements[name]
        return None

    @schema_cache
    def match_child(self, name: str) -> Optional['XsdElement']:
        xsd_group = self.type.model_group
        if xsd_group is None:
            # fallback to xs:anyType encoder for matching extra content
            xsd_group = self.maps.any_type.model_group
            assert xsd_group is not None

        for xsd_child in xsd_group.elements:
            matched_element = xsd_child.match(name, resolve=True)
            if isinstance(matched_element, XsdElement):
                return matched_element
        else:
            if name in self.maps.elements and xsd_group.open_content_mode != 'none':
                return self.maps.elements[name]
            return None

    @schema_cache
    def is_restriction(self, other: ModelParticleType, check_occurs: bool = True) -> bool:
        e: ModelParticleType

        if isinstance(other, XsdAnyElement):
            if self.min_occurs == self.max_occurs == 0:
                return True
            if check_occurs and not self.has_occurs_restriction(other):
                return False
            return other.is_matching(self.name, self.default_namespace)
        elif isinstance(other, XsdElement):
            if self.name != other.name:
                if other.name == self.substitution_group and \
                        other.min_occurs != other.max_occurs and \
                        self.max_occurs != 0 and not other.abstract \
                        and self.xsd_version == '1.0':
                    # A UPA violation case. Base is the head element, it's not
                    # abstract and has non-deterministic occurs: this is less
                    # restrictive than W3C test group (elemZ026), marked as
                    # invalid despite it's based on an abstract declaration.
                    # See also test case invalid_restrictions1.xsd.
                    return False

                for e in other.iter_substitutes():
                    if e.name == self.name:
                        break
                else:
                    return False

            if check_occurs and not self.has_occurs_restriction(other):
                return False
            elif self.max_occurs == 0 and check_occurs:
                return True  # type is not effective if the element can't have occurrences
            elif not self.is_consistent(other) and self.type.elem is not other.type.elem and \
                    not self.type.is_derived(other.type, 'restriction') and not other.type.abstract:
                return False
            elif other.fixed is not None and \
                    (self.fixed is None or self.type.normalize(
                        self.fixed) != other.type.normalize(other.fixed)):
                return False
            elif other.nillable is False and self.nillable:
                return False
            elif any(value not in self.block for value in other.block.split()):
                return False
            elif not all(k in other.identities for k in self.identities):
                return False
            else:
                return True
        elif other.model == 'choice':
            if other.is_empty() and self.max_occurs != 0:
                return False

            check_group_items_occurs = self.xsd_version == '1.0'
            total_occurs = OccursCalculator()
            for e in other.iter_model():
                if not isinstance(e, (XsdElement, XsdAnyElement)):
                    return False
                elif not self.is_restriction(e, check_group_items_occurs):
                    continue
                total_occurs += e
                total_occurs *= other
                if self.has_occurs_restriction(total_occurs):
                    return True
                total_occurs.reset()
            return False
        else:
            match_restriction = False
            for e in other.iter_model():
                if match_restriction:
                    if not e.is_emptiable():
                        return False
                elif self.is_restriction(e):
                    match_restriction = True
                elif not e.is_emptiable():
                    return False
            return True

    @schema_cache
    def is_overlap(self, other: SchemaElementType) -> bool:
        if isinstance(other, XsdElement):
            if self.name == other.name:
                return True
            elif other.substitution_group == self.name or other.name == self.substitution_group:
                return True
        elif isinstance(other, XsdAnyElement):
            if other.is_matching(self.name, self.default_namespace):
                return True
            for e in self.maps.substitution_groups.get(self.name, ()):
                if other.is_matching(e.name, self.default_namespace):
                    return True
        return False

    def is_consistent(self, other: SchemaElementType, strict: bool = True) -> bool:
        """
        Element Declarations Consistent check between two element particles.

        Ref: https://www.w3.org/TR/xmlschema-1/#cos-element-consistent

        :returns: `True` if there is no inconsistency between the particles, `False` otherwise,
        """
        return self.name != other.name or self.type is other.type

    def is_single(self) -> bool:
        if self.parent is None:
            return True
        elif self.max_occurs != 1:
            return False
        elif self.parent.max_occurs == 1:
            return True
        else:
            return self.parent.model != 'choice' and len(self.parent) > 1

    def is_substitute(self, other: ModelParticleType) -> bool:
        return not self.abstract and isinstance(other, XsdElement) \
            and self.substitution_group == other.name


class Xsd11Element(XsdElement):
    """
    Class for XSD 1.1 *element* declarations.

    ..  <element
          abstract = boolean : false
          block = (#all | List of (extension | restriction | substitution))
          default = string
          final = (#all | List of (extension | restriction))
          fixed = string
          form = (qualified | unqualified)
          id = ID
          maxOccurs = (nonNegativeInteger | unbounded)  : 1
          minOccurs = nonNegativeInteger : 1
          name = NCName
          nillable = boolean : false
          ref = QName
          substitutionGroup = List of QName
          targetNamespace = anyURI
          type = QName
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, ((simpleType | complexType)?, alternative*,
          (unique | key | keyref)*))
        </element>
    """
    def _parse(self) -> None:
        if self._built is not None and isinstance(self.parent, MutableSequence):
            return

        self.min_occurs = self.max_occurs = 1
        self.selected_by = set()
        self.xsi_types = set()
        self._parse_particle(self.elem)
        self._parse_attributes()

        if self.ref is None:
            self._parse_type()
            self._parse_alternatives()
            self._parse_constraints()

            if self.parent is None:
                self.substitutes = set()
                if 'substitutionGroup' in self.elem.attrib:
                    for substitution_group in self.elem.attrib['substitutionGroup'].split():
                        self._parse_substitution_group(substitution_group)

        if 'targetNamespace' in self.elem.attrib:
            self._parse_target_namespace()

        if any(v.inheritable for v in self.attributes.values()):
            self.inheritable = {}
            for k, v in self.attributes.items():
                if k is not None and isinstance(v, XsdAttribute):
                    if v.inheritable:
                        self.inheritable[k] = v

        self._built = True

    def _parse_alternatives(self) -> None:
        alternatives = []
        has_test = True
        for child in self.elem:
            if child.tag == nm.XSD_ALTERNATIVE:
                alternatives.append(XsdAlternative(child, self.schema, self))
                if not has_test:
                    msg = _("test attribute missing in non-final alternative")
                    self.parse_error(msg)
                has_test = 'test' in child.attrib

        if alternatives:
            self.alternatives = alternatives

    def iter_components(self, xsd_classes: ComponentClassType = None) -> Iterator[XsdComponent]:
        if xsd_classes is None:
            yield self
            yield from self.identities
        else:
            if isinstance(self, xsd_classes):
                yield self
            if issubclass(XsdIdentity, xsd_classes):
                yield from self.identities

        for alt in self.alternatives:
            yield from alt.iter_components(xsd_classes)

        if self.ref is None and self.type.parent is not None:
            yield from self.type.iter_components(xsd_classes)

    def iter_substitutes(self) -> Iterator[XsdElement]:
        if self.parent is None or self.ref is not None:
            if substitutes := self.maps.substitution_groups.get(self.name):
                for xsd_element in substitutes:
                    yield xsd_element
                    yield from xsd_element.iter_substitutes()

    def get_alternative_type(self, elem: Union[ElementType, ElementData],
                             inherited: Optional[dict[str, Any]] = None) -> BaseXsdType:
        if isinstance(elem, ElementData):
            if elem.attributes:
                attrib = raw_encode_attributes(elem.attributes)
                elem = Element(elem.tag, attrib=attrib)
            else:
                elem = Element(elem.tag)

        if inherited:
            dummy = Element('_dummy_element', attrib=inherited)
            dummy.attrib.update(elem.attrib)

            for alt in self.alternatives:
                if alt.type is not None:
                    if alt.token is None or alt.test(elem) or alt.test(dummy):
                        return alt.type
        else:
            for alt in self.alternatives:
                if alt.type is not None:
                    if alt.token is None or alt.test(elem):
                        return alt.type

        return self.type

    def is_overlap(self, other: SchemaElementType) -> bool:
        if isinstance(other, XsdElement):
            if self.name == other.name:
                return True
            elif any(self.name == x.name for x in other.iter_substitutes()):
                return True

            for e in self.iter_substitutes():
                if other.name == e.name or any(x is e for x in other.iter_substitutes()):
                    return True

        elif isinstance(other, XsdAnyElement):
            if other.is_matching(self.name, self.default_namespace):
                return True
            for e in self.maps.substitution_groups.get(self.name, ()):
                if other.is_matching(e.name, self.default_namespace):
                    return True
        return False

    def is_consistent(self, other: SchemaElementType, strict: bool = True) -> bool:
        if isinstance(other, XsdAnyElement):
            if other.process_contents == 'skip':
                return True
            xsd_element = other.match(self.name, self.default_namespace, resolve=True)
            return xsd_element is None or self.is_consistent(xsd_element, strict=False)

        e1: XsdElement = self
        e2 = other
        if self.name != other.name:
            for e1 in self.iter_substitutes():
                if e1.name == other.name:
                    break
            else:
                for e2 in other.iter_substitutes():
                    if e2.name == self.name:
                        break
                else:
                    return True

        if len(e1.alternatives) != len(e2.alternatives):
            return False
        elif e1.type is not e2.type and strict:
            return False
        elif e1.type is not e2.type or \
                not all(any(a == x for x in e2.alternatives) for a in e1.alternatives) or \
                not all(any(a == x for x in e1.alternatives) for a in e2.alternatives):
            msg = _("Maybe a not equivalent type table between elements {0!r} and {1!r}")
            warnings.warn(msg.format(e1, e2), XMLSchemaTypeTableWarning, stacklevel=3)
        return True

    def check_dynamic_context(self, elem: ElementType, validation: str,
                              context: ValidationContext) -> None:
        for ns, url in iter_schema_location_hints(elem):
            if self.maps.get_schema(ns, url, context.source.base_url) is not None:
                continue

            try:
                with self.maps.protect_status():
                    if ns in self.maps.namespaces:
                        schema = self.maps.namespaces[ns][0]
                        schema.include_schema(url, context.source.base_url)
                    else:
                        schema = self.schema
                        schema.import_schema(ns, url, context.source.base_url)
                    schema.clear()
                    schema.build()

            except (XMLSchemaValidationError, ParseError) as err:
                context.validation_error(validation, self, err, elem)
            except XMLSchemaParseError as err:
                context.validation_error(validation, self, err.message, elem)
            except OSError:
                continue
            else:
                def stop_validation(e: ElementType, _xsd_element: XsdElement) -> bool:
                    if e is elem:
                        raise XMLSchemaStopValidation()
                    return False

                errors = list(schema.iter_errors(context.source, validation_hook=stop_validation))
                if len(context.errors) != len(errors) or \
                        any(e1.elem is not e2.elem for e1, e2 in zip(context.errors, errors)):
                    reason = _(f"adding schema at {url} change the "
                               f"assessment outcome of previous items")
                    context.validation_error(validation, self, reason, elem)


class XsdAlternative(XsdComponent):
    """
    XSD 1.1 type *alternative* definitions.

    ..  <alternative
          id = ID
          test = an XPath expression
          type = QName
          xpathDefaultNamespace = (anyURI | (##defaultNamespace | ##targetNamespace | ##local))
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (simpleType | complexType)?)
        </alternative>
    """
    parent: XsdElement
    type: BaseXsdType
    path: Optional[str]
    token: Optional[XPathToken]
    _ADMITTED_TAGS = nm.XSD_ALTERNATIVE,

    __slots__ = ('xpath_default_namespace', 'path', 'token', 'type')

    def __repr__(self) -> str:
        return '%s(type=%r, test=%r)' % (
            self.__class__.__name__, self.elem.get('type'), self.elem.get('test')
        )

    def __eq__(self, other: object) -> bool:
        return isinstance(other, XsdAlternative) and \
            self.path == other.path and self.type is other.type and \
            self.xpath_default_namespace == other.xpath_default_namespace

    def __ne__(self, other: object) -> bool:
        return not isinstance(other, XsdAlternative) or \
            self.path != other.path or self.type is not other.type or \
            self.xpath_default_namespace != other.xpath_default_namespace

    def _parse(self) -> None:
        attrib = self.elem.attrib

        if 'xpathDefaultNamespace' in attrib:
            self.xpath_default_namespace = parse_xpath_default_namespace(self)
        else:
            self.xpath_default_namespace = self.schema.xpath_default_namespace

        parser = XPath2Parser(
            namespaces=self.schema.namespaces,
            strict=False,
            default_namespace=self.xpath_default_namespace
        )

        try:
            self.path = attrib['test']
        except KeyError:
            # an absent test is not an error, it should be the default type
            self.path = self.token = None
        else:
            try:
                self.token = parser.parse(self.path)
            except ElementPathError as err:
                self.parse_error(err)
                self.token = parser.parse('false()')
                self.path = 'false()'

        try:
            type_qname = self.schema.resolve_qname(attrib['type'])
        except (KeyError, ValueError, RuntimeError) as err:
            if 'type' in attrib:
                self.parse_error(err)
                self.type = self.maps.any_type
            elif (child := self._parse_child_component(self.elem, strict=False)) is None:
                self.parse_error(_("missing 'type' attribute"))
                self.type = self.maps.any_type
            else:
                try:
                    self.type = self.builders.local_types[child.tag](
                        child, self.schema, self
                    )
                except KeyError:
                    self.parse_error(_("missing 'type' attribute"))
                    self.type = self.maps.any_type
                else:
                    if not self.type.is_derived(self.parent.type):
                        msg = _("declared type is not derived from {!r}")
                        self.parse_error(msg.format(self.parent.type))
        else:
            try:
                self.type = self.maps.types[type_qname]
            except KeyError:
                self.parse_error(_("unknown type {!r}").format(attrib['type']))
                self.type = self.maps.any_type
            else:
                if self.type.name != nm.XSD_ERROR and not self.type.is_derived(self.parent.type):
                    msg = _("type {0!r} is not derived from {1!r}")
                    self.parse_error(msg.format(attrib['type'], self.parent.type))

                child = self._parse_child_component(self.elem, strict=False)
                if child is not None and child.tag in nm.GLOBAL_TYPES_TAGS:
                    msg = _("the attribute 'type' and the xs:%s local "
                            "declaration are mutually exclusive")
                    self.parse_error(msg % child.tag.split('}')[-1])

    @property
    def validation_attempted(self) -> str:
        if self._built:
            return 'full'
        elif not hasattr(self, 'type'):
            return 'none'
        else:
            return self.type.validation_attempted

    def iter_components(self, xsd_classes: ComponentClassType = None) -> Iterator[XsdComponent]:
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self
        if self.type is not None and self.type.parent is not None:
            yield from self.type.iter_components(xsd_classes)

    def test(self, elem: ElementType) -> bool:
        if self.token is None:
            return False

        try:
            result = list(self.token.select(context=XPathContext(elem)))
            return self.token.boolean_value(result)
        except (TypeError, ValueError):
            return False

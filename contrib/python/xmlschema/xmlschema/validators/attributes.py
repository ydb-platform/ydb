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
This module contains classes for XML Schema attributes and attribute groups.
"""
from collections.abc import Iterator, MutableMapping
from copy import copy
from decimal import Decimal
from functools import cached_property
from typing import cast, Any, Optional, Union

from elementpath.datatypes import AbstractDateTime, Duration

import xmlschema.names as nm
from xmlschema.aliases import ComponentClassType, ElementType, \
    AtomicValueType, SchemaType, DecodedValueType, NsmapType
from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.translation import gettext as _
from xmlschema.utils.decoding import EmptyType
from xmlschema.utils.qnames import get_namespace, get_qname

from .exceptions import XMLSchemaCircularityError
from .validation import ValidationContext, DecodeContext, EncodeContext, ValidationMixin
from .xsdbase import XsdComponent, XsdAnnotation
from .simple_types import XsdSimpleType
from .wildcards import XsdAnyAttribute

AttributeGroupDecodeType = Optional[list[tuple[str, DecodedValueType]]]


class XsdAttribute(XsdComponent, ValidationMixin[Optional[str], DecodedValueType]):
    """
    Class for XSD 1.0 *attribute* declarations.

    ..  <attribute
          default = string
          fixed = string
          form = (qualified | unqualified)
          id = ID
          name = NCName
          ref = QName
          type = QName
          use = (optional | prohibited | required) : optional
          {any attributes with non-schema namespace ...}>
          Content: (annotation?, simpleType?)
        </attribute>
    """
    _ADMITTED_TAGS = nm.XSD_ATTRIBUTE,

    name: str
    local_name: str
    qualified_name: str
    prefixed_name: str

    form: Optional[str] = None
    qualified: bool = False
    """
    The effective form for the attribute. If `True` the attribute name is qualified by a
    braced namespace URI as prefix. The name of a global attribute is always qualified.
    """

    default: Optional[str] = None
    """The default value of the attribute."""

    fixed: Optional[str] = None
    """The fixed value of the attribute."""

    use: str = 'optional'
    """Defines the use of the attribute. Can be 'optional', 'prohibited' or 'required'."""

    inheritable: bool = False
    """
    Defines whether the attribute can be inherited by descendant elements.
    XSD 1.1 only, it's always `False` for XSD 1.0 attributes.
    """

    __slots__ = ('type',)

    def _parse(self) -> None:
        self.type: XsdSimpleType
        """The XSD simpleType of the attribute."""

        attrib = self.elem.attrib

        if 'use' in attrib and self.parent is not None and \
                attrib['use'] in {'optional', 'prohibited', 'required'}:
            self.use = attrib['use']

        if self._parse_reference():
            try:
                xsd_attribute = self.maps.attributes[self.name]
            except KeyError:
                self.type = self.maps.any_simple_type
                msg = _("unknown attribute {!r}")
                self.parse_error(msg.format(self.name))
            else:
                self.ref = xsd_attribute
                self.target_namespace = xsd_attribute.target_namespace
                self.type = xsd_attribute.type
                self.qualified = xsd_attribute.qualified
                self.form = xsd_attribute.form

                if xsd_attribute.default is not None and 'default' not in attrib:
                    self.default = xsd_attribute.default

                if xsd_attribute.fixed is not None:
                    if 'fixed' not in attrib:
                        self.fixed = xsd_attribute.fixed
                    elif xsd_attribute.fixed != attrib['fixed']:
                        msg = _("referenced attribute has a different fixed value {!r}")
                        self.parse_error(msg.format(xsd_attribute.fixed))

            for attribute in ('form', 'type'):
                if attribute in self.elem.attrib:
                    msg = _("attribute {!r} is not allowed when attribute reference is used")
                    self.parse_error(msg.format(attribute))
        else:
            if 'form' in attrib:
                self.form = attrib['form']
                if self.parent is not None and self.form == 'qualified':
                    self.qualified = True
            elif self.schema.attribute_form_default == 'qualified':
                self.qualified = True

            try:
                name = attrib['name']
            except KeyError:
                pass
            else:
                if name == 'xmlns':
                    msg = _("an attribute name must be different from 'xmlns'")
                    self.parse_error(msg)

                if self.parent is None or self.qualified:
                    if self.target_namespace == nm.XSI_NAMESPACE and \
                            name not in ('nil', 'type', 'schemaLocation',
                                         'noNamespaceSchemaLocation'):
                        msg = _("cannot add attributes in %r namespace")
                        self.parse_error(msg % nm.XSI_NAMESPACE)
                    self.name = get_qname(self.target_namespace, name)
                else:
                    self.name = name

            child = self._parse_child_component(self.elem)
            if 'type' in attrib:
                try:
                    type_qname = self.schema.resolve_qname(attrib['type'])
                except (KeyError, ValueError, RuntimeError) as err:
                    self.type = self.maps.any_simple_type
                    self.parse_error(err)
                else:
                    try:
                        self.type = cast(XsdSimpleType, self.maps.types[type_qname])
                    except KeyError as err:
                        self.type = self.maps.any_simple_type
                        self.parse_error(err)

                    if child is not None and child.tag == nm.XSD_SIMPLE_TYPE:
                        msg = _("ambiguous type definition for XSD attribute")
                        self.parse_error(msg)

            elif child is not None:
                # No 'type' attribute in declaration, parse for child local simpleType
                self.type = self.builders.simple_type_factory(child, self.schema, self)
            else:
                # Empty declaration means xsdAnySimpleType
                self.type = self.maps.any_simple_type

            if not isinstance(self.type, XsdSimpleType):
                self.type = self.maps.any_simple_type
                msg = _("XSD attribute's type must be a simpleType")
                self.parse_error(msg)

        # Check value constraints
        if 'default' in attrib:
            self.default = attrib['default']
            if 'fixed' in attrib:
                msg = _("'default' and 'fixed' attributes are mutually exclusive")
                self.parse_error(msg)

            if self.use != 'optional':
                msg = _("the attribute 'use' must be 'optional' "
                        "if the attribute 'default' is present")
                self.parse_error(msg)

            if not self.type.text_is_valid(self.default):
                msg = _("default value {!r} is not compatible with attribute's type")
                self.parse_error(msg.format(self.default))
            elif self.type.is_key() and self.xsd_version == '1.0':
                msg = _("xs:ID key attributes cannot have a default value")
                self.parse_error(msg)

        elif 'fixed' in attrib:
            self.fixed = attrib['fixed']
            if not self.type.text_is_valid(self.fixed):
                msg = _("fixed value {!r} is not compatible with attribute's type")
                self.parse_error(msg.format(self.fixed))
            elif self.type.is_key() and self.xsd_version == '1.0':
                msg = _("xs:ID key attributes cannot have a fixed value")
                self.parse_error(msg)

    @property
    def scope(self) -> str:
        """The scope of the attribute declaration that can be 'global' or 'local'."""
        return 'global' if self.parent is None else 'local'

    @property
    def value_constraint(self) -> Optional[str]:
        """The fixed or the default value if either is defined, `None` otherwise."""
        return self.fixed if self.fixed is not None else self.default

    def is_optional(self) -> bool:
        return self.use == 'optional'

    def is_required(self) -> bool:
        return self.use == 'required'

    def is_prohibited(self) -> bool:
        return self.use == 'prohibited'

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator[XsdComponent]:
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self
        if self.ref is None and self.type.parent is not None:
            yield from self.type.iter_components(xsd_classes)

    def data_value(self, text: str) -> AtomicValueType:
        """Returns the decoded data value of the provided text as XPath fn:data()."""
        return cast(AtomicValueType, self.decode(text, validation='skip'))

    def raw_decode(self, obj: Optional[str], validation: str, context: ValidationContext) \
            -> DecodedValueType:
        if obj is None and self.default is not None:
            obj = self.default

        if self.type.is_notation():
            if self.type.name == nm.XSD_NOTATION_TYPE:
                msg = _("cannot validate against xs:NOTATION directly, "
                        "only against a subtype with an enumeration facet")
                context.validation_error(validation, self, msg, obj)
            elif not self.type.enumeration:
                msg = _("missing enumeration facet in xs:NOTATION subtype")
                context.validation_error(validation, self, msg, obj)

        if self.fixed is not None:
            if obj is None:
                obj = self.fixed
            elif obj != self.fixed and \
                    self.type.text_decode(obj, context=context) != \
                    self.type.text_decode(self.fixed):
                msg = _("attribute {0!r} has a fixed value {1!r}").format(self.name, self.fixed)
                context.validation_error(validation, self, msg, obj)

        if obj is None:
            msg = _("attribute {0!r} has no value").format(self.name)
            context.validation_error(validation, self, msg, obj)
            return None

        value = self.type.raw_decode(obj, validation, context)
        if not isinstance(context, DecodeContext):
            return value

        if context.value_hook is not None:
            return context.value_hook(value, self.type)  # type:ignore[arg-type]
        elif isinstance(value, context.keep_datatypes):
            return value
        elif value is None:
            if context.filler is not None:
                return context.filler(self)
            return value
        elif isinstance(value, str):
            if value[:1] == '{' and self.type.is_qname():
                return obj
            else:
                return value
        elif isinstance(value, Decimal):
            if context.decimal_type is None:
                return value
            else:
                return context.decimal_type(value)
        elif isinstance(value, (AbstractDateTime, Duration)):
            return obj.strip()
        else:
            return str(value)

    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) -> Optional[str]:
        return self.type.raw_encode(obj, validation, context)


class Xsd11Attribute(XsdAttribute):
    """
    Class for XSD 1.1 *attribute* declarations.

    ..  <attribute
          default = string
          fixed = string
          form = (qualified | unqualified)
          id = ID
          name = NCName
          ref = QName
          targetNamespace = anyURI
          type = QName
          use = (optional | prohibited | required) : optional
          inheritable = boolean
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, simpleType?)
        </attribute>
    """
    def _parse(self) -> None:
        super()._parse()
        if self.use == 'prohibited' and 'fixed' in self.elem.attrib:
            msg = _("attribute 'fixed' with use=prohibited is not allowed in XSD 1.1")
            self.parse_error(msg)
        if 'inheritable' in self.elem.attrib:
            if self.elem.attrib['inheritable'].strip() in ('true', '1'):
                self.inheritable = True
        if 'targetNamespace' in self.elem.attrib:
            self._parse_target_namespace()


class XsdAttributeGroup(
    MutableMapping[Optional[str], Union[XsdAttribute, XsdAnyAttribute]], XsdComponent,
    ValidationMixin[MutableMapping[str, str], AttributeGroupDecodeType]
):
    """
    Class for XSD *attributeGroup* definitions.

    .. <attributeGroup
          id = ID
          name = NCName
          ref = QName
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, ((attribute | attributeGroup)*, anyAttribute?))
        </attributeGroup>
    """
    _ADMITTED_TAGS = (
        nm.XSD_ATTRIBUTE_GROUP, nm.XSD_COMPLEX_TYPE, nm.XSD_RESTRICTION, nm.XSD_EXTENSION
    )

    __slots__ = ('_attribute_group', 'derivation', 'base_attributes')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: Optional[XsdComponent] = None,
                 derivation: Optional[str] = None,
                 base_attributes: Optional['XsdAttributeGroup'] = None) -> None:

        self._attribute_group: dict[Optional[str], Union[XsdAttribute, XsdAnyAttribute]] = {}
        self.derivation = derivation
        self.base_attributes = base_attributes
        XsdComponent.__init__(self, elem, schema, parent)

    def __repr__(self) -> str:
        if self.name is not None:
            return '%s(name=%r)' % (self.__class__.__name__, self.name)
        elif self:
            names = [a if a.name is None else a.name for a in self.values()]
            return '%s(%r)' % (self.__class__.__name__, names)
        else:
            return '%s()' % self.__class__.__name__

    # Implementation of abstract methods
    def __getitem__(self, key: Optional[str]) -> Union[XsdAttribute, XsdAnyAttribute]:
        return self._attribute_group[key]

    def __setitem__(self, key: Optional[str],
                    value: Union[XsdAttribute, XsdAnyAttribute]) -> None:
        if value.name != key:
            msg = "mismatch between %(attr)r name and item key %(key)r"
            raise XMLSchemaValueError(msg % {'attr': value, 'key': key})
        self._attribute_group[key] = value

    def __delitem__(self, key: Optional[str]) -> None:
        del self._attribute_group[key]

    def __iter__(self) -> Iterator[Optional[str]]:
        if None in self._attribute_group:
            # Put AnyAttribute ('None' key) at the end of iteration
            yield from sorted(self._attribute_group, key=lambda x: (x is None, x))
        else:
            yield from self._attribute_group

    def __len__(self) -> int:
        return len(self._attribute_group)

    def _parse(self) -> None:
        if self.elem.tag == nm.XSD_ATTRIBUTE_GROUP:
            if self.parent is not None:
                return  # Skip parsing dummy instances
            try:
                self.name = get_qname(self.target_namespace, self.elem.attrib['name'])
            except KeyError:
                return
            else:
                if self.schema.default_attributes == self.name and self.xsd_version > '1.0':
                    self.schema.default_attributes = self

        any_attribute = None
        attribute_group_refs: list[str] = []
        attributes: dict[Optional[str], Union[XsdAttribute, XsdAnyAttribute]] = {}

        for child in self.elem:
            if child.tag == nm.XSD_ANNOTATION or callable(child.tag):
                continue  # pragma: no cover
            elif any_attribute is not None:
                if child.tag == nm.XSD_ANY_ATTRIBUTE:
                    msg = _("more anyAttribute declarations in the same attribute group")
                    self.parse_error(msg)
                elif child.tag != nm.XSD_ASSERT:
                    msg = _("another declaration after anyAttribute")
                    self.parse_error(msg)

            elif child.tag == nm.XSD_ANY_ATTRIBUTE:
                any_attribute = self.builders.any_attribute_class(child, self.schema, self)
                if None in attributes:
                    attributes[None] = attr = copy(attributes[None])
                    assert isinstance(attr, XsdAnyAttribute)
                    attr.intersection(any_attribute)
                    attr.parent = self
                else:
                    attributes[None] = any_attribute

            elif child.tag == nm.XSD_ATTRIBUTE:
                attribute = self.builders.attribute_class(child, self.schema, self)
                if attribute.name in attributes:
                    msg = _("multiple declaration for attribute {!r}")
                    self.parse_error(msg.format(attribute.name))
                elif attribute.use != 'prohibited' or self.elem.tag != nm.XSD_ATTRIBUTE_GROUP:
                    attributes[attribute.name] = attribute

            elif child.tag == nm.XSD_ATTRIBUTE_GROUP:
                try:
                    ref = child.attrib['ref']
                except KeyError:
                    msg = _("the attribute 'ref' is required in a local attributeGroup")
                    self.parse_error(msg)
                    continue

                try:
                    attribute_group_qname = self.schema.resolve_qname(ref)
                except (KeyError, ValueError, RuntimeError) as err:
                    self.parse_error(err)
                else:
                    if attribute_group_qname in attribute_group_refs:
                        msg = _("duplicated attributeGroup {!r}")
                        self.parse_error(msg.format(ref))

                    elif self.redefine is not None:
                        if attribute_group_qname == self.name:
                            if attribute_group_refs:
                                msg = _("in a redefinition the reference to "
                                        "itself must be the first")
                                self.parse_error(msg)

                            attribute_group_refs.append(attribute_group_qname)
                            attributes.update(self._attribute_group)
                            continue
                        elif not attribute_group_refs:
                            # Maybe an attributeGroup restriction with a ref to another group
                            if not any(e.tag == nm.XSD_ATTRIBUTE_GROUP and ref == e.get('ref')
                                       for e in self.redefine.elem):
                                msg = _("attributeGroup ref={!r} is not in the redefined group")
                                self.parse_error(msg.format(ref))

                    elif attribute_group_qname == self.name and self.xsd_version == '1.0':
                        msg = _("Circular attribute groups not allowed in XSD 1.0")
                        self.parse_error(msg)

                    attribute_group_refs.append(attribute_group_qname)

                    try:
                        ref_attributes = self.maps.attribute_groups[attribute_group_qname]
                    except KeyError:
                        msg = _("unknown attribute group {!r}")
                        self.parse_error(msg.format(child.attrib['ref']))
                    except XMLSchemaCircularityError as err:
                        if self.xsd_version == '1.0':
                            self.parse_error(err, err.elem)
                    else:
                        for name, base_attr in ref_attributes.items():
                            if name not in attributes:
                                attributes[name] = base_attr
                            elif name is not None:
                                if base_attr is not attributes[name]:
                                    msg = _("multiple declaration of attribute {!r}")
                                    self.parse_error(msg.format(name))
                            else:
                                assert isinstance(base_attr, XsdAnyAttribute)
                                attributes[None] = attr = copy(attributes[None])
                                assert isinstance(attr, XsdAnyAttribute)
                                attr.intersection(base_attr)
                                attr.parent = self

            elif self.name is not None:
                msg = _("(attribute | attributeGroup) expected, found {!r}.")
                self.parse_error(msg.format(child))

        # Check and copy base attributes
        if self.base_attributes is not None:
            wildcard = cast(XsdAnyAttribute, self.base_attributes.get(None))
            for name, attr in attributes.items():
                if name not in self.base_attributes:
                    if self.derivation != 'restriction':
                        continue
                    elif wildcard is None or not wildcard.is_matching(name):
                        msg = _("Unexpected attribute {!r} in restriction")
                        self.parse_error(msg.format(name))
                    continue

                base_attr = self.base_attributes[name]

                if isinstance(attr, XsdAnyAttribute):
                    assert name is None, "name key resolves to an xs:anyAttribute"
                    assert isinstance(base_attr, XsdAnyAttribute), "invalid base attribute"

                    if self.derivation == 'extension':
                        try:
                            attr.union(base_attr)
                        except ValueError as err:
                            self.parse_error(err)
                    elif not attr.is_restriction(base_attr):
                        msg = _("Attribute wildcard is not a restriction of the base wildcard")
                        self.parse_error(msg)

                    continue

                assert name is not None, "None key resolves to an xs:attribute"
                assert isinstance(base_attr, XsdAttribute), "invalid base attribute"

                if self.derivation == 'restriction' and \
                        attr.type.name != nm.XSD_ANY_SIMPLE_TYPE and \
                        not attr.type.is_derived(base_attr.type, 'restriction'):
                    msg = _("Attribute type is not a restriction of the base attribute type")
                    self.parse_error(msg)

                if base_attr.use != 'optional' and attr.use == 'optional' or \
                        base_attr.use == 'required' and attr.use != 'required':
                    msg = _("Attribute {!r}: unmatched attribute use in restriction")
                    self.parse_error(msg.format(name))

                if base_attr.fixed is not None:
                    if attr.fixed is None or attr.type.normalize(attr.fixed) != \
                            base_attr.type.normalize(base_attr.fixed):
                        msg = _("Attribute {!r}: derived attribute has a different fixed value")
                        self.parse_error(msg.format(name))

                if base_attr.inheritable is not attr.inheritable:
                    msg = _("Attribute {!r}: 'inheritable' property change in restriction")
                    self.parse_error(msg.format(name))

            if self.redefine is not None:
                pass  # In case of redefinition do not copy base attributes
            else:
                self._attribute_group.update(self.base_attributes.items())

        elif self.redefine is not None and not attribute_group_refs:
            for name, attr in self._attribute_group.items():
                if name is None:
                    continue
                elif name not in attributes:
                    if attr.use == 'required':
                        msg = _("Missing required attribute {!r} in redefinition restriction")
                        self.parse_error(msg.format(name))
                    continue

                if attr.use != 'optional' and attributes[name].use != attr.use:
                    msg = _("Attribute {!r}: unmatched attribute use in redefinition")
                    self.parse_error(msg.format(name))
                if attr.fixed is not None and attributes[name].fixed is None:
                    msg = _("Attribute {!r}: redefinition remove fixed constraint")
                    self.parse_error(msg.format(name))

            pos = 0
            keys = list(self._attribute_group.keys())
            for name in attributes:
                try:
                    next_pos = keys.index(name)
                except ValueError:
                    msg = _("Redefinition restriction contains additional attribute {!r}")
                    self.parse_error(msg.format(name))
                else:
                    if next_pos < pos:
                        msg = _("Wrong attribute order in redefinition restriction")
                        self.parse_error(msg)
                        break
                    pos = next_pos
            self.clear()

        self._attribute_group.update(attributes)
        if None in self._attribute_group and None not in attributes \
                and self.derivation == 'restriction':
            wildcard = copy(cast(XsdAnyAttribute, self._attribute_group[None]))
            wildcard.namespace = set()
            wildcard.not_namespace = wildcard.not_qname = ()
            self._attribute_group[None] = wildcard

        if self.xsd_version == '1.0':
            has_key = False
            for attr in self._attribute_group.values():
                if attr.type is not None and attr.type.is_key():
                    if has_key:
                        msg = _("multiple ID attributes not allowed for XSD 1.0")
                        self.parse_error(msg)
                        break
                    has_key = True

        elif self.parent is None and self.schema.default_attributes == self.name:
            self.schema.default_attributes = self

    @cached_property
    def annotation(self) -> Optional[XsdAnnotation]:
        return super().annotation if self.parent is None else None

    def parse_error(self, error: Union[str, Exception],
                    elem: Optional[ElementType] = None,
                    namespaces: Optional[NsmapType] = None) -> None:
        if self.parent is None:
            super().parse_error(error, elem, namespaces)
        else:
            self.parent.parse_error(error, elem, namespaces)

    def iter_required(self) -> Iterator[str]:
        for k, v in self._attribute_group.items():
            if isinstance(v, XsdAttribute) and k is not None:
                if v.use == 'required':
                    yield k

    def iter_value_constraints(self, use_defaults: bool = True) -> Iterator[tuple[str, str]]:
        if use_defaults:
            for k, v in self._attribute_group.items():
                if v.fixed is not None and k:
                    yield k, v.fixed
                elif v.default is not None and k:
                    yield k, v.default
        else:
            for k, v in self._attribute_group.items():
                if v.fixed is not None and k:
                    yield k, v.fixed

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator[XsdComponent]:
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self

        for attr in self.values():
            if attr.parent is not None:
                yield from attr.iter_components(xsd_classes)

    def raw_decode(self, obj: MutableMapping[str, str], validation: str,
                   context: ValidationContext) -> AttributeGroupDecodeType:

        if not obj and not self:
            return []

        for name in filter(lambda x: x not in obj, self.iter_required()):
            reason = _("missing required attribute {!r}").format(name)
            context.validation_error(validation, self, reason, obj)

        additional_attrs = [
            (k, v) for k, v in self.iter_value_constraints(context.use_defaults)
            if k not in obj
        ]
        if additional_attrs:
            obj = {k: v for k, v in obj.items()}
            obj.update(additional_attrs)

        id_list = context.id_list
        if self.xsd_version == '1.0':
            context.id_list = []

        result: AttributeGroupDecodeType
        value: Any

        result = None if context.validation_only else []
        for name, value in obj.items():
            try:
                xsd_attribute = self._attribute_group[name]
            except KeyError:
                if get_namespace(name) == nm.XSI_NAMESPACE:
                    try:
                        xsd_attribute = self.maps.attributes[name]
                    except KeyError:
                        if None in self._attribute_group:
                            xsd_attribute = self._attribute_group[None]  # None == anyAttribute
                            value = (name, value)
                        else:
                            reason = _("%r is not an attribute of the XSI namespace") % name
                            context.validation_error(validation, self, reason, obj)
                            continue

                elif None in self._attribute_group:
                    xsd_attribute = self._attribute_group[None]  # None == anyAttribute
                    value = (name, value)
                else:
                    reason = _("%r attribute not allowed for element") % name
                    context.validation_error(validation, self, reason, obj)
                    continue
            else:
                if xsd_attribute.use == 'prohibited' and \
                        (None not in self or not self._attribute_group[None].is_matching(name)):
                    reason = _("use of attribute %r is prohibited") % name
                    context.validation_error(validation, self, reason, obj)

            context.attribute = name
            item = xsd_attribute.raw_decode(value, validation, context)
            if result is not None and not isinstance(item, EmptyType):
                result.append((name, item))
            context.attribute = None

        context.id_list = id_list
        if not isinstance(context, DecodeContext):
            return result
        elif result is not None and context.fill_missing:
            if context.filler is None:
                result.extend(
                    (k, None) for k in self._attribute_group
                    if k is not None and k not in obj
                )
            else:
                result.extend(
                    (k, context.filler(v)) for k, v in self._attribute_group.items()
                    if k is not None and k not in obj and isinstance(v, XsdAttribute)
                )
        return result

    def raw_encode(self, obj: MutableMapping[str, Any],
                   validation: str, context: EncodeContext) -> list[tuple[str, str]]:

        if not obj and not self:
            return []

        for name in filter(lambda x: x not in obj, self.iter_required()):
            reason = _("missing required attribute {!r}").format(name)
            context.validation_error(validation, self, reason, obj)

        result: list[tuple[str, str]] = []
        for name, value in obj.items():
            try:
                xsd_attribute = self._attribute_group[name]
            except KeyError:
                namespace = get_namespace(name) or self.target_namespace
                if namespace == nm.XSI_NAMESPACE:
                    try:
                        xsd_attribute = self.maps.attributes[name]
                    except KeyError:
                        if None in self._attribute_group:
                            xsd_attribute = self._attribute_group[None]  # None == anyAttribute
                            value = (name, value)
                        else:
                            reason = _("%r is not an attribute of the XSI namespace") % name
                            context.validation_error(validation, self, reason, obj)
                            continue

                elif None in self._attribute_group:
                    xsd_attribute = self._attribute_group[None]  # None == anyAttribute
                    value = (name, value)
                else:
                    reason = _("%r attribute not allowed for element") % name
                    context.validation_error(validation, self, reason, obj)
                    continue

            item = xsd_attribute.raw_encode(value, validation, context)
            if result is not None and item is not None and not isinstance(item, EmptyType):
                result.append((name, item))

        if result is not None:
            result.extend(
                (k, v) for k, v in self.iter_value_constraints(context.use_defaults)
                if k not in obj
            )
        return result

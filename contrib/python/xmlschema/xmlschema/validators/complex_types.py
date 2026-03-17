#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import Iterator
from functools import cached_property
from typing import cast, Any, Optional, Union

from elementpath.datatypes import AnyAtomicType

import xmlschema.names as nm
from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.aliases import ElementType, NsmapType, SchemaType, ComponentClassType, \
    DecodeType, BaseXsdType, DecodedValueType, ExtraValidatorType, ValidationHookType
from xmlschema.translation import gettext as _
from xmlschema.utils.qnames import get_qname, local_name
from xmlschema.caching import schema_cache

from .exceptions import XMLSchemaCircularityError, XMLSchemaDecodeError
from .validation import ValidationContext, EncodeContext, ValidationMixin
from .helpers import parse_xsd_derivation
from .xsdbase import XSD_TYPE_DERIVATIONS, XsdComponent, XsdType
from .attributes import XsdAttributeGroup
from .assertions import XsdAssert
from .simple_types import FacetsValueType, XsdSimpleType, XsdUnion, XsdAtomic
from .groups import XsdGroup
from .wildcards import XsdAnyElement, XsdOpenContent, XsdDefaultOpenContent


class XsdComplexType(XsdType, ValidationMixin[Union[ElementType, str, bytes], Any]):
    """
    Class for XSD 1.0 *complexType* definitions.

    :var attributes: the attribute group related with the complexType.
    :var content: the content of the complexType can be a model group or a simple type.
    :var mixed: if `True` the complex type has mixed content.

    ..  <complexType
          abstract = boolean : false
          block = (#all | List of (extension | restriction))
          final = (#all | List of (extension | restriction))
          id = ID
          mixed = boolean : false
          name = NCName
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (simpleContent | complexContent |
          ((group | all | choice | sequence)?, ((attribute | attributeGroup)*, anyAttribute?))))
        </complexType>
    """
    attributes: XsdAttributeGroup
    redefine: Optional[BaseXsdType]
    content: Union[XsdGroup, XsdSimpleType]

    abstract: bool = False
    mixed: bool = False
    assertions: Union[tuple[()], list[XsdAssert]] = ()
    open_content: Optional[XsdOpenContent] = None
    _block: Optional[str] = None

    _ADMITTED_TAGS = (nm.XSD_COMPLEX_TYPE, nm.XSD_RESTRICTION)
    _CONTENT_TAIL_TAGS = frozenset(
        (nm.XSD_ATTRIBUTE, nm.XSD_ATTRIBUTE_GROUP, nm.XSD_ANY_ATTRIBUTE)
    )

    @staticmethod
    def normalize(text: Union[str, bytes]) -> str:
        return text.decode('utf-8') if isinstance(text, bytes) else text

    __slots__ = ('content', 'attributes')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: Optional[XsdComponent] = None,
                 name: Optional[str] = None,
                 **kwargs: Any) -> None:

        if kwargs:
            if 'content' in kwargs:
                self.content = kwargs['content']
            if 'attributes' in kwargs:
                self.attributes = kwargs['attributes']
            if 'mixed' in kwargs:
                self.mixed = kwargs['mixed']
            if 'block' in kwargs:
                self._block = kwargs['block']
            if 'final' in kwargs:
                self._final = kwargs['final']
        super().__init__(elem, schema, parent, name)

    def __repr__(self) -> str:
        if self.name is not None:
            return '%s(name=%r)' % (self.__class__.__name__, self.prefixed_name)
        elif not hasattr(self, 'content') or not hasattr(self, 'attributes'):
            return '%s(id=%r)' % (self.__class__.__name__, id(self))
        else:
            return '%s(content=%r, attributes=%r)' % (
                self.__class__.__name__, self.content_type_label,
                [a if a.name is None else a.prefixed_name for a in self.attributes.values()]
            )

    def _parse(self) -> None:
        if self.elem.tag == nm.XSD_RESTRICTION:
            return  # a local restriction is already parsed by the caller

        if 'abstract' in self.elem.attrib:
            if self.elem.attrib['abstract'].strip() in ('true', '1'):
                self.abstract = True

        if 'block' in self.elem.attrib:
            self._block = parse_xsd_derivation(self.elem, 'block', XSD_TYPE_DERIVATIONS, self)

        if 'final' in self.elem.attrib:
            self._final = parse_xsd_derivation(self.elem, 'final', XSD_TYPE_DERIVATIONS, self)

        if 'mixed' in self.elem.attrib:
            if self.elem.attrib['mixed'].strip() in ('true', '1'):
                self.mixed = True

        try:
            self.name = get_qname(self.target_namespace, self.elem.attrib['name'])
        except KeyError:
            self.name = None
            if self.parent is None:
                msg = _("missing attribute 'name' in a global complexType")
                self.parse_error(msg)
                self.name = 'nameless_%s' % str(id(self))
        else:
            if self.parent is not None:
                msg = _("attribute 'name' not allowed in a local complexType")
                self.parse_error(msg)
                self.name = None

        content_elem = self._parse_child_component(self.elem, strict=False)
        if content_elem is None or content_elem.tag in self._CONTENT_TAIL_TAGS:
            self.content = self.builders.create_empty_content_group(self)
            self._parse_content_tail(self.elem)
            default_open_content = self.default_open_content
            if default_open_content is not None and \
                    (self.mixed or self.content or default_open_content.applies_to_empty):
                self.open_content = default_open_content

        elif content_elem.tag in nm.MODEL_GROUP_TAGS:
            self.content = self.builders.group_class(content_elem, self.schema, self)
            default_open_content = self.default_open_content
            if default_open_content is not None and \
                    (self.mixed or self.content or default_open_content.applies_to_empty):
                self.open_content = default_open_content
            self._parse_content_tail(self.elem)

        elif content_elem.tag == nm.XSD_SIMPLE_CONTENT:
            if 'mixed' in content_elem.attrib:
                msg = _("'mixed' attribute not allowed with simpleContent")
                self.parse_error(msg, content_elem)

            derivation_elem = self._parse_derivation_elem(content_elem)
            if derivation_elem is None:
                return

            self.base_type = base_type = self._parse_base_type(derivation_elem)
            if derivation_elem.tag == nm.XSD_RESTRICTION:
                self._parse_simple_content_restriction(derivation_elem, base_type)
            else:
                self._parse_simple_content_extension(derivation_elem, base_type)

            if content_elem is not self.elem[-1]:
                k = 2 if content_elem is not self.elem[0] else 1
                msg = _("unexpected tag %r after simpleContent declaration:")
                self.parse_error(msg % self.elem[k].tag)

        elif content_elem.tag == nm.XSD_COMPLEX_CONTENT:
            #
            # complexType with complexContent restriction/extension
            if 'mixed' in content_elem.attrib:
                mixed = content_elem.attrib['mixed'] in ('true', '1')
                if mixed is not self.mixed:
                    self.mixed = mixed
                    if 'mixed' in self.elem.attrib and self.xsd_version == '1.1':
                        msg = _("value of 'mixed' attribute in complexType "
                                "and complexContent must be the same")
                        self.parse_error(msg)

            derivation_elem = self._parse_derivation_elem(content_elem)
            if derivation_elem is None:
                return

            self.base_type = self._parse_base_type(derivation_elem, complex_content=True)
            if self.base_type is self and self.redefine is not None:
                self.base_type = self.redefine
                self.open_content = None

            if derivation_elem.tag == nm.XSD_RESTRICTION:
                self._parse_complex_content_restriction(derivation_elem, self.base_type)
            else:
                self._parse_complex_content_extension(derivation_elem, self.base_type)

            if content_elem is not self.elem[-1]:
                k = 2 if content_elem is not self.elem[0] else 1
                msg = _("unexpected tag %r after complexContent declaration")
                self.parse_error(msg % self.elem[k].tag)

        elif content_elem.tag == nm.XSD_OPEN_CONTENT and self.xsd_version > '1.0':
            self.open_content = XsdOpenContent(content_elem, self.schema, self)

            if content_elem is self.elem[-1]:
                self.content = self.builders.create_empty_content_group(self)
            else:
                for index, child in enumerate(self.elem):
                    if content_elem is not child:
                        continue
                    elif self.elem[index + 1].tag in nm.MODEL_GROUP_TAGS:
                        self.content = self.builders.group_class(
                            self.elem[index + 1], self.schema, self
                        )
                    else:
                        self.content = self.builders.create_empty_content_group(self)
                    break
            self._parse_content_tail(self.elem)

        else:
            if self.schema.validation == 'skip':
                # Also generated by meta-schema validation for 'lax' and 'strict' modes
                msg = _("unexpected tag %r for complexType content")
                self.parse_error(msg % content_elem.tag)

            self.content = self.builders.create_any_content_group(self)
            self.attributes = self.builders.create_any_attribute_group(self)

        if self.redefine is None:
            if self.base_type is not None and self.base_type.name == self.name:
                msg = _("wrong definition with self-reference")
                self.parse_error(msg)
        elif self.base_type is None or self.base_type.name != self.name:
            msg = _("wrong redefinition without self-reference")
            self.parse_error(msg)

    def _parse_content_tail(self, elem: ElementType, **kwargs: Any) -> None:
        self.attributes = self.builders.attribute_group_class(
            elem, self.schema, self, **kwargs
        )

    def _parse_derivation_elem(self, elem: ElementType) -> Optional[ElementType]:
        derivation_elem = self._parse_child_component(elem)
        if derivation_elem is None or \
                derivation_elem.tag not in (nm.XSD_RESTRICTION, nm.XSD_EXTENSION):
            msg = _("restriction or extension tag expected")
            self.parse_error(msg, derivation_elem)
            self.content = self.builders.create_any_content_group(self)
            self.attributes = self.builders.create_any_attribute_group(self)
            return None

        if self.derivation is not None and self.redefine is None:
            msg = _("{!r} is expected to have a redefined/overridden component")
            raise XMLSchemaValueError(msg.format(self))
        self.derivation = local_name(derivation_elem.tag)

        if self.base_type is not None and self.derivation in self.base_type.final:
            msg = _("{0!r} derivation not allowed for {1!r}")
            self.parse_error(msg.format(self.derivation, self))
        return derivation_elem

    def _parse_base_type(self, elem: ElementType, complex_content: bool = False) \
            -> Union[XsdSimpleType, 'XsdComplexType']:
        try:
            base_qname = self.schema.resolve_qname(elem.attrib['base'])
        except (KeyError, ValueError, RuntimeError) as err:
            if 'base' not in elem.attrib:
                msg = _("'base' attribute required")
                self.parse_error(msg, elem)
            else:
                self.parse_error(err, elem)
            return self.maps.any_type

        try:
            base_type = self.maps.types[base_qname]
        except KeyError:
            msg = _("missing base type %r")
            self.parse_error(msg % base_qname, elem)
            if complex_content:
                return self.maps.any_type
            else:
                return self.maps.any_simple_type
        except XMLSchemaCircularityError as err:
            self.parse_error(err, err.elem)
            return self.maps.any_type
        else:
            if complex_content and base_type.is_simple():
                msg = _("a complexType ancestor required: {!r}")
                self.parse_error(msg.format(base_type), elem)
                return self.maps.any_type

            if base_type.final and elem.tag.rsplit('}', 1)[-1] in base_type.final:
                msg = _("derivation by %r blocked by attribute 'final' in base type")
                self.parse_error(msg % elem.tag.rsplit('}', 1)[-1])

            return base_type

    def _parse_simple_content_restriction(self, elem: ElementType, base_type: Any) -> None:
        # simpleContent restriction: the base type must be a complexType with a simple
        # content or a complex content with a mixed and emptiable content.
        if base_type.is_simple():
            msg = _("a complexType ancestor required: {!r}")
            self.parse_error(msg.format(base_type), elem)
            self.content = self.builders.create_any_content_group(self)
            self._parse_content_tail(elem)
        else:
            if base_type.is_empty():
                self.content = self.builders.atomic_restriction_class(
                    elem, self.schema, self
                )
                if not self.is_empty():
                    msg = _("a not empty simpleContent cannot restrict an empty content type")
                    self.parse_error(msg, elem)
                    self.content = self.builders.create_any_content_group(self)

            elif base_type.has_simple_content():
                self.content = self.builders.atomic_restriction_class(
                    elem, self.schema, self
                )
                if not self.content.is_derived(base_type.content, 'restriction'):
                    msg = _("content type is not a restriction of base content")
                    self.parse_error(msg, elem)

            elif base_type.mixed and base_type.is_emptiable():
                self.content = self.builders.atomic_restriction_class(
                    elem, self.schema, self
                )
            else:
                msg = _("with simpleContent cannot restrict an element-only content type")
                self.parse_error(msg, elem)
                self.content = self.builders.create_any_content_group(self)

            self._parse_content_tail(elem, derivation='restriction',
                                     base_attributes=base_type.attributes)

    def _parse_simple_content_extension(self, elem: ElementType, base_type: Any) -> None:
        # simpleContent extension: the base type must be a simpleType or a complexType
        # with simple content.
        child = self._parse_child_component(elem, strict=False)
        if child is not None and child.tag not in self._CONTENT_TAIL_TAGS:
            msg = _('unexpected tag %r')
            self.parse_error(msg % child.tag, child)

        if base_type.is_simple():
            self.content = base_type
            self._parse_content_tail(elem)
        else:
            if base_type.has_simple_content():
                self.content = base_type.content
            else:
                self.parse_error(_("base type %r has no simple content") % base_type, elem)
                self.content = self.builders.create_any_content_group(self)

            self._parse_content_tail(elem, derivation='extension',
                                     base_attributes=base_type.attributes)

    def _parse_complex_content_restriction(self, elem: ElementType, base_type: Any) -> None:
        if 'restriction' in base_type.final:
            msg = _("the base type is not derivable by restriction")
            self.parse_error(msg)
        if base_type.is_simple() or base_type.has_simple_content():
            msg = _("base %r is simple or has a simple content")
            self.parse_error(msg % base_type, elem)
            base_type = self.maps.any_type

        # complexContent restriction: the base type must be a complexType with a complex content.
        for child in elem:
            if child.tag == nm.XSD_OPEN_CONTENT and self.xsd_version > '1.0':
                self.open_content = XsdOpenContent(child, self.schema, self)
                continue
            elif child.tag in nm.MODEL_GROUP_TAGS:
                content = self.builders.group_class(child, self.schema, self)
                if not base_type.content.admits_restriction(content.model):
                    msg = _("restriction of an xs:{0} with more than "
                            "one particle with xs:{1} is forbidden")
                    self.parse_error(msg.format(base_type.content.model, content.model))
                break
        else:
            content = self.builders.create_empty_content_group(
                self, base_type.content.model
            )

        content.restriction = base_type.content

        if base_type.is_element_only() and content.mixed:
            msg = _("derived a mixed content from a base type that has element-only content")
            self.parse_error(msg, elem)
        elif base_type.is_empty() and not content.is_empty():
            msg = _("an empty content derivation from base type that has not empty content")
            self.parse_error(msg, elem)

        if self.open_content is None:
            default_open_content = self.default_open_content
            if default_open_content is not None and \
                    (self.mixed or content or default_open_content.applies_to_empty):
                self.open_content = default_open_content

        if self.open_content and content and \
                not self.open_content.is_restriction(base_type.open_content):
            msg = _("{0!r} is not a restriction of the base type {1!r}")
            self.parse_error(msg.format(self.open_content, base_type.open_content))

        self.content = content
        self._parse_content_tail(elem, derivation='restriction',
                                 base_attributes=base_type.attributes)

    def _parse_complex_content_extension(self, elem: ElementType, base_type: Any) -> None:
        if 'extension' in base_type.final:
            msg = _("the base type is not derivable by extension")
            self.parse_error(msg)

        group_elem: Optional[ElementType]
        for group_elem in elem:
            if group_elem.tag != nm.XSD_ANNOTATION and not callable(group_elem.tag):
                break
        else:
            group_elem = None

        if base_type.is_empty():
            if not base_type.mixed:
                # Empty element-only model extension: don't create a nested group.
                if group_elem is not None and group_elem.tag in nm.MODEL_GROUP_TAGS:
                    self.content = self.builders.group_class(
                        group_elem, self.schema, self
                    )
                elif base_type.is_simple() or base_type.has_simple_content():
                    self.content = self.builders.create_empty_content_group(self)
                else:
                    self.content = self.builders.create_empty_content_group(
                        parent=self, elem=base_type.content.elem
                    )
            else:
                # Empty mixed model extension
                self.content = self.builders.create_empty_content_group(self)
                self.content.append(self.builders.create_empty_content_group(self.content))

                if group_elem is not None and group_elem.tag in nm.MODEL_GROUP_TAGS:
                    group = self.builders.group_class(
                        group_elem, self.schema, self.content
                    )
                    if not self.mixed:
                        msg = _("base has a different content type (mixed=%r) "
                                "and the extension group is not empty.")
                        self.parse_error(msg % base_type.mixed, elem)
                else:
                    group = self.builders.create_empty_content_group(self)

                self.content.append(group)
                self.content.elem.append(base_type.content.elem)
                self.content.elem.append(group.elem)

        elif group_elem is not None and group_elem.tag in nm.MODEL_GROUP_TAGS:
            # Derivation from a simple content is forbidden if base type is not empty.
            if base_type.is_simple() or base_type.has_simple_content():
                msg = _("base %r is simple or has a simple content")
                self.parse_error(msg % base_type, elem)
                base_type = self.maps.any_type

            group = self.builders.group_class(group_elem, self.schema, self)

            if group.model == 'all':
                msg = _("cannot extend a complex content with xs:all")
                self.parse_error(msg)
            if base_type.content.model == 'all' and group.model == 'sequence':
                msg = _("xs:sequence cannot extend xs:all")
                self.parse_error(msg)

            content = self.builders.create_empty_content_group(self)
            content.append(base_type.content)
            content.append(group)
            content.elem.append(base_type.content.elem)
            content.elem.append(group.elem)

            if base_type.content.model == 'all' and base_type.content and group:
                msg = _("XSD 1.0 does not allow extension of a not empty 'all' model group")
                self.parse_error(msg)
            if base_type.mixed is not self.mixed:
                msg = _("base has a different content type (mixed=%r) "
                        "and the extension group is not empty")
                self.parse_error(msg % base_type.mixed, elem)
            self.content = content

        elif base_type.is_simple():
            self.content = base_type
        elif base_type.has_simple_content():
            self.content = base_type.content
        else:
            # Derived type has an empty content
            if self.mixed is not base_type.mixed:
                if self.mixed:
                    msg = _("extended type has a mixed content but the base is element-only")
                    self.parse_error(msg, elem)
                self.mixed = base_type.mixed  # not an error if mixed='false'

            self.content = self.builders.create_empty_content_group(self)
            self.content.append(base_type.content)
            self.content.elem.append(base_type.content.elem)

        self._parse_content_tail(elem, derivation='extension', base_attributes=base_type.attributes)

    @property
    def default_open_content(self) -> Optional[XsdDefaultOpenContent]:
        return None

    @property
    def block(self) -> str:
        return self.schema.block_default if self._block is None else self._block

    @property
    def simple_type(self) -> Optional[XsdSimpleType]:
        return self.content if isinstance(self.content, XsdSimpleType) else None

    @property
    def model_group(self) -> Optional[XsdGroup]:
        return self.content if isinstance(self.content, XsdGroup) else None

    @property
    def content_type_label(self) -> str:
        if self.is_empty():
            return 'empty'
        elif isinstance(self.content, XsdSimpleType):
            return 'simple'
        elif self.mixed:
            return 'mixed'
        else:
            return 'element-only'

    @cached_property
    def root_type(self) -> BaseXsdType:
        if self.attributes or self.base_type is None:
            return cast('XsdComplexType', self.maps.types[nm.XSD_ANY_TYPE])
        else:
            return self.base_type.root_type

    @cached_property
    def sequence_type(self) -> str:
        if self.is_empty():
            return 'empty-sequence()'
        elif isinstance(self.content, XsdAtomic):
            name = self.content.primitive_type.local_name
            st = 'item()' if name is None else f'xs:{name}'
        else:
            st = 'xs:untypedAtomic'

        return f"{st}{'*' if self.is_emptiable() else '+'}"

    @staticmethod
    def is_simple() -> bool:
        return False

    @staticmethod
    def is_complex() -> bool:
        return True

    def is_empty(self) -> bool:
        if self.open_content and self.open_content.mode != 'none':
            return False
        return self.content.is_empty()

    def is_emptiable(self) -> bool:
        return self.content.is_emptiable()

    def has_simple_content(self) -> bool:
        if not isinstance(self.content, XsdGroup):
            return not self.content.is_empty()
        elif self.content or self.content.mixed or self.base_type is None:
            return False
        else:
            return self.base_type.is_simple() or self.base_type.has_simple_content()

    def has_complex_content(self) -> bool:
        if not isinstance(self.content, XsdGroup):
            return False
        elif self.open_content and self.open_content.mode != 'none':
            return True
        return not self.content.is_empty()

    def has_mixed_content(self) -> bool:
        if not isinstance(self.content, XsdGroup):
            return False
        elif self.content.is_empty():
            return False
        else:
            return self.content.mixed

    def is_element_only(self) -> bool:
        if not isinstance(self.content, XsdGroup):
            return False
        elif self.content.is_empty():
            return False
        else:
            return not self.content.mixed

    def is_list(self) -> bool:
        return isinstance(self.content, XsdSimpleType) and self.content.is_list()

    def is_dynamic_consistent(self, other: Any) -> bool:
        return other.name == nm.XSD_ANY_TYPE or self.is_derived(other) or \
            isinstance(other, XsdUnion) and any(self.is_derived(mt) for mt in other.member_types)

    def validate(self, obj: Union[ElementType, str, bytes],
                 use_defaults: bool = True,
                 namespaces: Optional[NsmapType] = None,
                 max_depth: Optional[int] = None,
                 extra_validator: Optional[ExtraValidatorType] = None,
                 validation_hook: Optional[ValidationHookType] = None) -> None:
        kwargs: Any = {
            'use_defaults': use_defaults,
            'namespaces': namespaces,
            'max_depth': max_depth,
            'extra_validator': extra_validator,
            'validation_hook': validation_hook,
        }
        if not isinstance(obj, (str, bytes)):
            super().validate(obj, **kwargs)
        elif isinstance(self.content, XsdSimpleType):
            self.content.validate(obj, **kwargs)
        elif not self.mixed and self.base_type is not None:
            self.base_type.validate(obj, **kwargs)

    def is_valid(self, obj: Union[ElementType, str, bytes],
                 use_defaults: bool = True,
                 namespaces: Optional[NsmapType] = None,
                 max_depth: Optional[int] = None,
                 extra_validator: Optional[ExtraValidatorType] = None,
                 validation_hook: Optional[ValidationHookType] = None) -> bool:
        kwargs: Any = {
            'use_defaults': use_defaults,
            'namespaces': namespaces,
            'max_depth': max_depth,
            'extra_validator': extra_validator,
            'validation_hook': validation_hook,
        }
        if not isinstance(obj, (str, bytes)):
            return super().is_valid(obj, **kwargs)
        elif isinstance(self.content, XsdSimpleType):
            return self.content.is_valid(obj, **kwargs)
        else:
            return self.mixed or self.base_type is not None and \
                self.base_type.is_valid(obj, **kwargs)

    @schema_cache
    def is_derived(self, other: BaseXsdType, derivation: Optional[str] = None) -> bool:
        if derivation and derivation == self.derivation:
            derivation = None  # derivation mode checked

        if other.ref is not None:
            other = other.ref

        if self is other or self.ref is other:
            return True
        elif other.name == nm.XSD_ANY_TYPE:
            return derivation != 'extension'
        elif self.base_type is other:
            return derivation is None
        elif isinstance(other, XsdUnion):
            return any(self.is_derived(m, derivation) for m in other.member_types)
        elif self.base_type is None:
            if not self.has_simple_content():
                return False
            return isinstance(self.content, XsdSimpleType) and \
                self.content.is_derived(other, derivation)
        elif self.has_simple_content():
            return isinstance(self.content, XsdSimpleType) and \
                self.content.is_derived(other, derivation) or \
                self.base_type.is_derived(other, derivation)
        else:
            return self.base_type.is_derived(other, derivation)

    def iter_components(self, xsd_classes: ComponentClassType = None) \
            -> Iterator[XsdComponent]:
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self
        if self.attributes and self.attributes.parent is not None:
            yield from self.attributes.iter_components(xsd_classes)
        if self.content.parent is not None:
            yield from self.content.iter_components(xsd_classes)
        if self.base_type is not None and self.base_type.parent is not None:
            yield from self.base_type.iter_components(xsd_classes)

        for obj in filter(lambda x: x.base_type is self, self.assertions):
            if xsd_classes is None or isinstance(obj, xsd_classes):
                yield obj

    def get_facet(self, tag: str) -> Optional[FacetsValueType]:
        if isinstance(self.content, XsdSimpleType):
            return self.content.get_facet(tag)
        return None

    def admit_simple_restriction(self) -> bool:
        if 'restriction' in self.final:
            return False
        else:
            return self.has_simple_content() or self.mixed and self.is_emptiable()

    def has_restriction(self) -> bool:
        return self.derivation == 'restriction'

    def has_extension(self) -> bool:
        return self.derivation == 'extension'

    def text_decode(self, text: str, validation: str = 'skip',
                    context: Optional[ValidationContext] = None) -> DecodedValueType:
        if isinstance(self.content, XsdSimpleType):
            return self.content.text_decode(text, validation, context)
        else:
            return text

    def text_is_valid(self, text: str, context: Optional[ValidationContext] = None) -> bool:
        if isinstance(self.content, XsdSimpleType):
            return self.content.text_is_valid(text, context)
        elif self.mixed or not text.strip():
            return True
        else:
            return len(self.content) == 1 and isinstance(self.content[0], XsdAnyElement)

    def decode(self, obj: Union[ElementType, str, bytes], *args: Any, **kwargs: Any) \
            -> DecodeType[Any]:
        if not isinstance(obj, (str, bytes)):
            return super().decode(obj, *args, **kwargs)
        elif isinstance(self.content, XsdSimpleType):
            return self.content.decode(obj, *args, **kwargs)
        else:
            msg = _("cannot decode %(obj)r data with %(decoder)r")
            raise XMLSchemaDecodeError(
                self, obj, str, msg % {'obj': obj, 'decoder': self}
            )

    def raw_decode(self, obj: Union[ElementType, str, bytes],
                   validation: str, context: ValidationContext) -> Any:
        """
        Decodes an Element instance using a dummy XSD element. Typically used
        for decoding with xs:anyType when an XSD element is not available.
        Also decodes strings if the type has a simple content.

        :param obj: the XML data that has to be decoded.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip.
        :param context: the decoding context.
        :return: a decoded object.
        """
        if not isinstance(obj, (str, bytes)):
            xsd_element = self.builders.create_element(
                obj.tag, self.schema, self, form='unqualified'
            )
            xsd_element.type = self
            return xsd_element.raw_decode(obj, validation, context)
        elif isinstance(self.content, XsdSimpleType):
            return self.content.raw_decode(obj, validation, context)
        else:
            msg = _("cannot decode %(obj)r data with %(decoder)r")
            raise XMLSchemaDecodeError(
                self, obj, str, msg % {'obj': obj, 'decoder': self}
            )

    def raw_encode(self, obj: Any, validation: str, context: EncodeContext) \
            -> Optional[ElementType]:
        """
        Encode XML data. A dummy element is created for the type, and it's used for
        encode data. Typically used for encoding with xs:anyType when an XSD element
        is not available.

        :param obj: decoded XML data.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip.
        :param context: the encoding context.
        :return: returns an Element.
        """
        try:
            name, value = obj
        except ValueError:
            name = obj.name
            value = obj

        xsd_type: BaseXsdType
        if isinstance(value, AnyAtomicType):
            xsd_type = self.maps.any_atomic_type
        else:
            xsd_type = self.maps.any_type

        xsd_element = self.builders.create_element(
            name, self.schema, xsd_type, form='unqualified'
        )
        xsd_element.type = xsd_type

        return xsd_element.raw_encode(value, validation, context)


class Xsd11ComplexType(XsdComplexType):
    """
    Class for XSD 1.1 *complexType* definitions.

    ..  <complexType
          abstract = boolean : false
          block = (#all | List of (extension | restriction))
          final = (#all | List of (extension | restriction))
          id = ID
          mixed = boolean
          name = NCName
          defaultAttributesApply = boolean : true
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (simpleContent | complexContent | (openContent?,
          (group | all | choice | sequence)?,
          ((attribute | attributeGroup)*, anyAttribute?), assert*)))
        </complexType>
    """
    default_attributes_apply = True

    _CONTENT_TAIL_TAGS = nm.CONTENT_TAIL_TAGS

    @property
    def default_attributes(self) -> Optional[XsdAttributeGroup]:
        if self.redefine is not None:
            default_attributes = self.schema.default_attributes
        else:
            for child in self.schema.root:
                if child.tag == nm.XSD_OVERRIDE and self.elem in child:
                    schema = self.schema.includes[child.attrib['schemaLocation']]
                    if schema.override is self.schema:
                        default_attributes = schema.default_attributes
                        break
            else:
                default_attributes = self.schema.default_attributes

        if isinstance(default_attributes, str):
            return None
        return default_attributes

    @property
    def default_open_content(self) -> Optional[XsdDefaultOpenContent]:
        if self.parent is not None:
            return self.schema.default_open_content

        for child in self.schema.root:
            if child.tag == nm.XSD_OVERRIDE and self.elem in child:
                schema = self.schema.includes[child.attrib['schemaLocation']]
                if schema.override is self.schema:
                    return schema.default_open_content
        else:
            return self.schema.default_open_content

    def _parse(self) -> None:
        super()._parse()

        if self.base_type and self.base_type.base_type is self.maps.any_simple_type and \
                self.base_type.derivation == 'extension' and not self.attributes:
            # Derivation from xs:anySimpleType with missing variety.
            # See: http://www.w3.org/TR/xmlschema11-1/#Simple_Type_Definition_details
            msg = _("the simple content of {!r} is not a valid simple type in XSD 1.1")
            self.parse_error(msg.format(self.base_type))

        # Add open content to a complex content type
        if isinstance(self.content, XsdGroup):
            if self.open_content is None:
                if self.content.open_content is not None:
                    msg = _("openContent mismatch between type and model group")
                    self.parse_error(msg)
            elif self.open_content:
                self.content.open_content = self.open_content

        # Add inheritable attributes
        if isinstance(self.base_type, XsdComplexType):
            for name, attr in self.base_type.attributes.items():
                if attr.inheritable:
                    if name not in self.attributes:
                        self.attributes[name] = attr
                    elif not self.attributes[name].inheritable:
                        msg = _("attribute %r must be inheritable")
                        self.parse_error(msg % name)

        if 'defaultAttributesApply' not in self.elem.attrib:
            self.default_attributes_apply = True
        elif self.elem.attrib['defaultAttributesApply'].strip() in ('false', '0'):
            self.default_attributes_apply = False
        else:
            self.default_attributes_apply = True

        # Add default attributes
        if self.default_attributes_apply and \
                isinstance(self.default_attributes, XsdAttributeGroup):
            if self.redefine is None:
                for k in self.default_attributes:
                    if k in self.attributes:
                        msg = _("default attribute {!r} is already "
                                "declared in the complex type")
                        self.parse_error(msg.format(k))

            self.attributes.update((k, v) for k, v in self.default_attributes.items())

    def _parse_complex_content_extension(self, elem: ElementType, base_type: Any) -> None:
        # Complex content extension with simple base is forbidden XSD 1.1.
        # For the detailed rule refer to XSD 1.1 documentation:
        #   https://www.w3.org/TR/2012/REC-xmlschema11-1-20120405/#sec-cos-ct-extends
        if base_type.is_simple() or base_type.has_simple_content():
            msg = _("base %r is simple or has a simple content")
            self.parse_error(msg % base_type, elem)
            base_type = self.maps.any_type

        if 'extension' in base_type.final:
            msg = _("the base type is not derivable by extension")
            self.parse_error(msg)

        # Parse openContent
        group_elem: Any
        for group_elem in elem:
            if group_elem.tag == nm.XSD_ANNOTATION or callable(group_elem.tag):
                continue
            elif group_elem.tag != nm.XSD_OPEN_CONTENT:
                break
            self.open_content = XsdOpenContent(group_elem, self.schema, self)
            try:
                any_element = base_type.open_content.any_element
                self.open_content.any_element.union(any_element)
            except AttributeError:
                pass
        else:
            group_elem = None

        if not base_type.content:
            if not base_type.mixed:
                # Empty element-only model extension: don't create a nested sequence group.
                if group_elem is not None and group_elem.tag in nm.MODEL_GROUP_TAGS:
                    self.content = self.builders.group_class(
                        group_elem, self.schema, self
                    )
                else:
                    max_occurs = base_type.content.max_occurs
                    self.content = self.builders.create_empty_content_group(
                        parent=self,
                        model=base_type.content.model,
                        minOccurs=str(base_type.content.min_occurs),
                        maxOccurs='unbounded' if max_occurs is None else str(max_occurs),
                    )

            else:
                # Empty mixed model extension
                self.content = self.builders.create_empty_content_group(self)
                self.content.append(self.builders.create_empty_content_group(self.content))

                if group_elem is not None and group_elem.tag in nm.MODEL_GROUP_TAGS:
                    group = self.builders.group_class(
                        group_elem, self.schema, self.content
                    )
                    if not self.mixed:
                        msg = _("base has a different content type (mixed=%r) "
                                "and the extension group is not empty.")
                        self.parse_error(msg % base_type.mixed, elem)
                    if group.model == 'all':
                        msg = _("cannot extend an empty mixed content with an xs:all")
                        self.parse_error(msg)
                else:
                    group = self.builders.create_empty_content_group(self)

                self.content.append(group)
                self.content.elem.append(base_type.content.elem)
                self.content.elem.append(group.elem)

        elif group_elem is not None and group_elem.tag in nm.MODEL_GROUP_TAGS:
            group = self.builders.group_class(group_elem, self.schema, self)

            if base_type.content.model != 'all':
                content = self.builders.create_empty_content_group(self)
                content.append(base_type.content)
                content.elem.append(base_type.content.elem)

                if group.model == 'all':
                    msg = _("xs:all cannot extend a not empty xs:%s")
                    self.parse_error(msg % base_type.content.model)
                else:
                    content.append(group)
                    content.elem.append(group.elem)
            else:
                content = self.builders.create_empty_content_group(
                    self, model='all', minOccurs=str(base_type.content.min_occurs)
                )
                content.extend(base_type.content)
                content.elem.extend(base_type.content.elem)

                if not group:
                    pass
                elif group.model != 'all':
                    msg = _("cannot extend a not empty 'all' model group with a different model")
                    self.parse_error(msg)
                elif base_type.content.min_occurs != group.min_occurs:
                    msg = _("when extend an xs:all group minOccurs must be the same")
                    self.parse_error(msg)
                elif base_type.mixed and not base_type.content:
                    msg = _("cannot extend an xs:all group with mixed empty content")
                    self.parse_error(msg)
                else:
                    content.extend(group)
                    content.elem.extend(group.elem)

            if base_type.mixed is not self.mixed:
                msg = _("base has a different content type (mixed=%r) "
                        "and the extension group is not empty.")
                self.parse_error(msg % base_type.mixed, elem)

            self.content = content

        elif base_type.is_simple():
            self.content = base_type
        elif base_type.has_simple_content():
            self.content = base_type.content
        else:
            # Derived type has an empty content
            if self.mixed is not base_type.mixed:
                if self.mixed:
                    msg = _("extended type has a mixed content but the base is element-only")
                    self.parse_error(msg, elem)
                self.mixed = base_type.mixed  # not an error if mixed='false'

            self.content = self.builders.create_empty_content_group(self)
            self.content.append(base_type.content)
            self.content.elem.append(base_type.content.elem)

        if self.open_content is None:
            default_open_content = self.default_open_content
            if default_open_content is not None and \
                    (self.mixed or self.content or default_open_content.applies_to_empty):
                self.open_content = default_open_content
            elif base_type.open_content is not None:
                self.open_content = base_type.open_content

        if base_type.open_content is not None and \
                self.open_content is not None and \
                self.open_content is not base_type.open_content:

            if self.open_content.mode == 'none':
                self.open_content = base_type.open_content
            elif not base_type.open_content.is_restriction(self.open_content):
                msg = _("{0!r} is not an extension of the base type {1!r}")
                self.parse_error(msg.format(self.open_content, base_type.open_content))

        self._parse_content_tail(elem, derivation='extension',
                                 base_attributes=base_type.attributes)

    def _parse_content_tail(self, elem: ElementType, **kwargs: Any) -> None:
        self.attributes = self.builders.attribute_group_class(
            elem, self.schema, self, **kwargs
        )

        self.assertions = [XsdAssert(e, self.schema, self, self)
                           for e in elem if e.tag == nm.XSD_ASSERT]
        if isinstance(self.base_type, XsdComplexType):
            self.assertions.extend(
                XsdAssert(assertion.elem, self.schema, self, self)
                for assertion in self.base_type.assertions
            )

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
This module contains classes for XML Schema model groups.
"""
import warnings
from collections.abc import Iterable, Iterator, MutableMapping, MutableSequence
from copy import copy as _copy
from operator import attrgetter
from typing import TYPE_CHECKING, cast, overload, Any, Optional, Union
from xml.etree import ElementTree

import xmlschema.names as nm
from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.aliases import ElementType, NsmapType, SchemaType, ModelParticleType, \
    SchemaElementType, ComponentClassType, OccursCounterType
from xmlschema.converters import ElementData
from xmlschema.translation import gettext as _
from xmlschema.utils.decoding import Empty, raw_encode_value, raw_encode_attributes
from xmlschema.utils.qnames import get_qname, local_name
from xmlschema import _limits
from xmlschema.caching import schema_cache, schema_cached_property

from .exceptions import XMLSchemaModelError, XMLSchemaModelDepthError, \
    XMLSchemaValidationError, XMLSchemaTypeTableWarning, XMLSchemaCircularityError, \
    XMLSchemaValidatorError
from .validation import ValidationContext, DecodeContext, EncodeContext, ValidationMixin
from .xsdbase import XsdComponent, XsdType
from .particles import ParticleMixin
from .elements import XsdElement, XsdAlternative
from .wildcards import XsdAnyElement, XsdOpenContent
from .models import ModelVisitor, InterleavedModelVisitor, SuffixedModelVisitor, \
    iter_unordered_content, iter_collapsed_content

if TYPE_CHECKING:
    from .complex_types import XsdComplexType  # noqa: F401
    from .particles import OccursCalculator  # noqa: F401

get_occurs = attrgetter('min_occurs', 'max_occurs')

ANY_ELEMENT = ElementTree.Element(
    nm.XSD_ANY,
    attrib={
        'namespace': '##any',
        'processContents': 'lax',
        'minOccurs': '0',
        'maxOccurs': 'unbounded'
    })

GroupDecodeType = Optional[list[tuple[Union[str, int], Any, Optional[SchemaElementType]]]]
GroupEncodeType = ElementType


class XsdGroup(XsdComponent, MutableSequence[ModelParticleType],
               ParticleMixin, ValidationMixin[ElementType, GroupDecodeType]):
    """
    Class for XSD 1.0 *model group* definitions.

    ..  <group
          id = ID
          maxOccurs = (nonNegativeInteger | unbounded) : 1
          minOccurs = nonNegativeInteger : 1
          name = NCName
          ref = QName
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (all | choice | sequence)?)
        </group>

    ..  <all
          id = ID
          maxOccurs = 1 : 1
          minOccurs = (0 | 1) : 1
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, element*)
        </all>

    ..  <choice
          id = ID
          maxOccurs = (nonNegativeInteger | unbounded)  : 1
          minOccurs = nonNegativeInteger : 1
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (element | group | choice | sequence | any)*)
        </choice>

    ..  <sequence
          id = ID
          maxOccurs = (nonNegativeInteger | unbounded)  : 1
          minOccurs = nonNegativeInteger : 1
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (element | group | choice | sequence | any)*)
        </sequence>
    """
    parent: Optional[Union['XsdComplexType', 'XsdGroup']]
    model: str
    mixed: bool = False
    ref: Optional['XsdGroup']
    content: list[ModelParticleType]  # The effective content model for validate
    restriction: Optional['XsdGroup'] = None
    redefine: Optional['XsdGroup']

    # For XSD 1.1 openContent processing
    open_content: Optional[XsdOpenContent] = None
    _ADMITTED_TAGS = (nm.XSD_GROUP, nm.XSD_SEQUENCE, nm.XSD_ALL, nm.XSD_CHOICE)

    __slots__ = ('_group', 'content', 'oid', 'model', 'min_occurs', 'max_occurs')

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: Optional[Union['XsdComplexType', 'XsdGroup']] = None) -> None:

        self._group: list[ModelParticleType] = []
        self.content = self._group
        self.oid = (self,)
        self.min_occurs = self.max_occurs = 1
        super().__init__(elem, schema, parent)

    def __repr__(self) -> str:
        model = getattr(self, 'model', None)
        if self.name is None:
            return '%s(model=%r, occurs=%r)' % (
                self.__class__.__name__, model, list(self.occurs)
            )
        elif self.ref is None:
            return '%s(name=%r, model=%r, occurs=%r)' % (
                self.__class__.__name__, self.prefixed_name, model, list(self.occurs)
            )
        else:
            return '%s(ref=%r, model=%r, occurs=%r)' % (
                self.__class__.__name__, self.prefixed_name, model, list(self.occurs)
            )

    @overload
    def __getitem__(self, i: int) -> ModelParticleType: ...

    @overload
    def __getitem__(self, s: slice) -> MutableSequence[ModelParticleType]: ...

    def __getitem__(self, i: Union[int, slice]) \
            -> Union[ModelParticleType, MutableSequence[ModelParticleType]]:
        return self._group[i]

    def __setitem__(self, i: Union[int, slice], o: Any) -> None:
        if self._built:
            raise XMLSchemaValidatorError(self, 'cannot modify an already built group')
        self._group[i] = o

    def __delitem__(self, i: Union[int, slice]) -> None:
        if self._built:
            raise XMLSchemaValidatorError(self, 'cannot modify an already built group')
        del self._group[i]

    def __len__(self) -> int:
        return len(self._group)

    def insert(self, i: int, item: ModelParticleType) -> None:
        if self._built:
            raise XMLSchemaValidatorError(self, 'cannot modify an already built group')
        self._group.insert(i, item)

    def is_emptiable(self) -> bool:
        if self.model == 'choice':
            return self.min_occurs == 0 or not self or any(item.is_emptiable() for item in self)
        else:
            return self.min_occurs == 0 or not self or all(item.is_emptiable() for item in self)

    def is_single(self) -> bool:
        if self.max_occurs != 1 or not self:
            return False
        elif len(self) > 1 or not isinstance(self[0], XsdGroup):
            return True
        else:
            return self[0].is_single()

    def is_pointless(self, parent: 'XsdGroup') -> bool:
        """
        Returns `True` if the group may be eliminated without affecting the model,
        `False` otherwise. A group is pointless if one of those conditions is verified:

         - the group is empty
         - minOccurs == maxOccurs == 1 and the group has one child
         - minOccurs == maxOccurs == 1 and the group and its parent have a sequence model
         - minOccurs == maxOccurs == 1 and the group and its parent have a choice model

        Ref: https://www.w3.org/TR/2004/REC-xmlschema-1-20041028/#coss-particle

        :param parent: effective parent of the model group.
        """
        if not self:
            return True
        elif self.min_occurs != 1 or self.max_occurs != 1:
            return False
        elif len(self) == 1:
            return True
        elif self.model == 'sequence' and parent.model != 'sequence':
            return False
        elif self.model == 'choice' and parent.model != 'choice':
            return False
        else:
            return True

    @property
    def open_content_mode(self) -> str:
        return 'none' if self.open_content is None else self.open_content.mode

    @property
    def effective_min_occurs(self) -> int:
        if not self.min_occurs or not self:
            return 0

        effective_items: list[Any]
        min_occurs: int
        effective_items = [e for e in self.iter_model() if e.effective_max_occurs != 0]
        if not effective_items:
            return 0
        elif self.model == 'choice':
            min_occurs = min(e.effective_min_occurs for e in effective_items)
            return self.min_occurs * min_occurs
        elif self.model == 'all':
            min_occurs = max(e.effective_min_occurs for e in effective_items)
            return min_occurs

        not_emptiable_items = [e for e in effective_items if e.effective_min_occurs]
        if not not_emptiable_items:
            return 0
        elif len(not_emptiable_items) > 1:
            return self.min_occurs

        min_occurs = not_emptiable_items[0].effective_min_occurs
        return self.min_occurs * min_occurs

    @property
    def effective_max_occurs(self) -> Optional[int]:
        if self.max_occurs == 0 or not self:
            return 0

        effective_items: list[Any]
        max_occurs: int

        model_items = [(e, e.effective_max_occurs) for e in self.iter_model()]
        effective_items = [x for x in model_items if x[1] != 0]
        if not effective_items:
            return 0
        elif self.max_occurs is None:
            return None
        elif self.model == 'choice':
            if any(x[1] is None for x in effective_items):
                return None
            else:
                max_occurs = max(x[1] for x in effective_items)
                return self.max_occurs * max_occurs

        not_emptiable_items = [x for x in effective_items if x[0].effective_min_occurs]
        if not not_emptiable_items:
            if any(x[1] is None for x in effective_items):
                return None
            else:
                max_occurs = max(x[1] for x in effective_items)
                return self.max_occurs * max_occurs

        elif len(not_emptiable_items) > 1:
            if self.model == 'sequence':
                return self.max_occurs
            elif all(x[1] is None for x in not_emptiable_items):
                return None
            else:
                max_occurs = min(x[1] for x in not_emptiable_items if x[1] is not None)
                return max_occurs
        elif not_emptiable_items[0][1] is None:
            return None
        else:
            return self.max_occurs * cast(int, not_emptiable_items[0][1])

    def has_occurs_restriction(
            self, other: Union[ModelParticleType, ParticleMixin, 'OccursCalculator']) -> bool:

        if not self:
            return True
        elif isinstance(other, XsdGroup):
            return super().has_occurs_restriction(other)

        # Group particle compared to element particle
        if self.max_occurs is None or any(e.max_occurs is None for e in self):
            if other.max_occurs is not None:
                return False
            elif self.model == 'choice':
                return self.min_occurs * min(e.min_occurs for e in self) >= other.min_occurs
            else:
                return self.min_occurs * sum(e.min_occurs for e in self) >= other.min_occurs

        elif self.model == 'choice':
            if self.min_occurs * min(e.min_occurs for e in self) < other.min_occurs:
                return False
            elif other.max_occurs is None:
                return True
            else:
                value: int
                try:
                    value = max(e.max_occurs for e in self)  # type: ignore[type-var, assignment]
                except TypeError:
                    return False
                else:
                    return self.max_occurs * value <= other.max_occurs

        else:
            if self.min_occurs * sum(e.min_occurs for e in self) < other.min_occurs:
                return False
            elif other.max_occurs is None:
                return True
            else:
                try:
                    value = sum(e.max_occurs for e in self)  # type: ignore[misc]
                except TypeError:
                    return False
                else:
                    return self.max_occurs * value <= other.max_occurs

    def iter_model(self) -> Iterator[ModelParticleType]:
        """
        A generator function iterating elements and groups of a model group.
        Skips pointless groups, iterating deeper through them.
        Raises `XMLSchemaModelDepthError` if the *depth* of the model is over
        `limits.MAX_MODEL_DEPTH` value.
        """
        iterators: list[Iterator[ModelParticleType]] = []
        particles = iter(self)

        while True:
            for item in particles:
                if isinstance(item, XsdGroup) and item.is_pointless(parent=self):
                    iterators.append(particles)
                    particles = iter(item)
                    if len(iterators) > _limits.MAX_MODEL_DEPTH:
                        raise XMLSchemaModelDepthError(self)
                    break
                else:
                    yield item
            else:
                try:
                    particles = iterators.pop()
                except IndexError:
                    return

    def iter_elements(self) -> Iterator[SchemaElementType]:
        """
        A generator function iterating model's elements. Raises `XMLSchemaModelDepthError`
        if the overall depth of the model groups is over `limits.MAX_MODEL_DEPTH`.
        """
        if self.max_occurs == 0:
            return

        iterators: list[Iterator[ModelParticleType]] = []
        particles = iter(self)

        while True:
            for item in particles:
                if isinstance(item, XsdGroup):
                    if item.max_occurs == 0:
                        continue

                    iterators.append(particles)
                    particles = iter(item.content)
                    if len(iterators) > _limits.MAX_MODEL_DEPTH:
                        raise XMLSchemaModelDepthError(self)
                    break
                else:
                    yield item
            else:
                try:
                    particles = iterators.pop()
                except IndexError:
                    return

    @schema_cached_property
    def elements(self) -> tuple[SchemaElementType, ...]:
        return tuple(self.iter_elements())

    def get_subgroups(self, particle: ModelParticleType) -> list['XsdGroup']:
        """
        Returns a list of the groups that represent the first path to the enclosed particle.
        Raises an `XMLSchemaModelError` if the argument is not a particle of the model group.
        """
        for subgroups in self.iter_subgroups(particle):
            return subgroups
        else:
            return []

    def iter_subgroups(self, particle: ModelParticleType) -> Iterator[list['XsdGroup']]:
        """
        Iterates all subgroups where the provided particle is present.
        Raises an `XMLSchemaModelError` if the argument is not a particle of the model group.
        """
        subgroups: list[tuple[XsdGroup, Iterator[ModelParticleType]]] = []
        group, children = self, iter(self if self.ref is None else self.ref)
        found = False

        while True:
            for child in children:
                if child is particle:
                    found = True
                    _subgroups = [x[0] for x in subgroups]
                    _subgroups.append(group)
                    yield _subgroups
                elif isinstance(child, XsdGroup):
                    if len(subgroups) > _limits.MAX_MODEL_DEPTH:
                        raise XMLSchemaModelDepthError(self)
                    subgroups.append((group, children))
                    group, children = child, iter(child if child.ref is None else child.ref)
                    break
            else:
                try:
                    group, children = subgroups.pop()
                except IndexError:
                    if not found:
                        msg = _('{!r} is not a particle of the model group')
                        raise XMLSchemaModelError(self, msg.format(particle)) from None
                    return

    def get_model_visitor(self) -> ModelVisitor:
        if self.open_content is None or self.open_content.mode == 'none':
            return ModelVisitor(self)
        elif self.open_content.mode == 'interleave':
            return InterleavedModelVisitor(self, self.open_content.any_element)
        else:
            return SuffixedModelVisitor(self, self.open_content.any_element)

    def overall_min_occurs(self, particle: ModelParticleType) -> int:
        """
        Returns the overall min occurs of a particle in the model group.
        """
        model = self.get_model_visitor()
        return model.overall_min_occurs(particle)

    def overall_max_occurs(self, particle: ModelParticleType) -> Optional[int]:
        """
        Returns the overall max occurs of a particle in the model group.
        """
        model = self.get_model_visitor()
        return model.overall_max_occurs(particle)

    def is_optional(self, particle: ModelParticleType) -> bool:
        """
        Returns `True` if the provided particle can be optional in the model group.
        """
        return self.overall_min_occurs(particle) == 0

    def is_missing(self, occurs: OccursCounterType) -> bool:
        value = occurs[self.oid] or occurs[self]
        return not self.is_emptiable() if value == 0 else self.min_occurs > value

    def get_expected(self, occurs: OccursCounterType) -> list[SchemaElementType]:
        """
        Returns the expected elements of the current and descendant groups
        given a counter of occurrences. Returns an empty list if the group
        reached the maximum number of occurrences.
        """
        expected: list[SchemaElementType] = []

        if self.is_over(occurs):
            return expected

        for e in self.elements:
            if e not in expected and isinstance(e, XsdElement) and e.min_occurs > occurs[e]:
                expected.append(e)
                expected.extend(s for s in e.iter_substitutes())

        return expected

    def __copy__(self) -> 'XsdGroup':
        group: XsdGroup = object.__new__(self.__class__)
        group.__dict__.update(self.__dict__)

        for attr in self._mro_slots():
            object.__setattr__(group, attr, getattr(self, attr))

        group.errors = self.errors.copy()
        group._group = self._group.copy()
        if self.ref is None:
            group.content = group._group
        return group

    def _any_content_group_fallback(self) -> None:
        self.model = 'sequence'
        self.mixed = True
        xsd_element = self.builders.any_element_class(ANY_ELEMENT, self.schema, self)
        self._group.clear()
        self._group.append(xsd_element)
        self.__dict__.pop('elements', None)

    def _parse(self) -> None:
        self._group.clear()
        self.__dict__.pop('elements', None)
        self._parse_particle(self.elem)

        if self.parent is not None and self.parent.mixed:
            self.mixed = self.parent.mixed

        if self.elem.tag != nm.XSD_GROUP:
            # Local group (sequence|all|choice)
            if 'name' in self.elem.attrib:
                msg = _("attribute 'name' not allowed in a local group")
                self.parse_error(msg)
            self._parse_content_model(self.elem)

        elif self._parse_reference():
            assert self.name is not None
            try:
                xsd_group = self.maps.groups[self.name]
            except KeyError:
                self.parse_error(_("missing group %r") % self.prefixed_name)
                self._any_content_group_fallback()
            except XMLSchemaCircularityError as err:
                # Circular definition, substituted with any content group.
                self.parse_error(err, err.elem)
                self._any_content_group_fallback()
            else:
                self.model = xsd_group.model
                if self.model == 'all':
                    if self.max_occurs != 1:
                        msg = _("maxOccurs must be 1 for 'all' model groups")
                        self.parse_error(msg)
                    if self.min_occurs not in (0, 1):
                        msg = _("minOccurs must be (0 | 1) for 'all' model groups")
                        self.parse_error(msg)
                    if self.xsd_version == '1.0' and isinstance(self.parent, XsdGroup):
                        msg = _("in XSD 1.0 an 'all' model group cannot be nested")
                        self.parse_error(msg)
                self._group.append(xsd_group)
                self.ref = xsd_group
                self.target_namespace = xsd_group.target_namespace
                self.content = xsd_group.content

        else:
            attrib = self.elem.attrib
            try:
                self.name = get_qname(self.target_namespace, attrib['name'])
            except KeyError:
                pass
            else:
                if self.parent is not None:
                    msg = _("attribute 'name' not allowed in a local group")
                    self.parse_error(msg)
                else:
                    if 'minOccurs' in attrib:
                        msg = _("attribute 'minOccurs' not allowed in a global group")
                        self.parse_error(msg)
                    if 'maxOccurs' in attrib:
                        msg = _("attribute 'maxOccurs' not allowed in a global group")
                        self.parse_error(msg)

                content_model = self._parse_child_component(self.elem, strict=True)
                if content_model is not None:
                    if self.parent is None:
                        if 'minOccurs' in content_model.attrib:
                            msg = _("attribute 'minOccurs' not allowed in a global group")
                            self.parse_error(msg, content_model)
                        if 'maxOccurs' in content_model.attrib:
                            msg = _("attribute 'maxOccurs' not allowed in a global group")
                            self.parse_error(msg, content_model)

                    if content_model.tag in nm.MODEL_TAGS:
                        self._parse_content_model(content_model)
                    else:
                        msg = _('unexpected tag %r')
                        self.parse_error(msg % content_model.tag, content_model)
                        self._any_content_group_fallback()

    def _parse_content_model(self, content_model: ElementType) -> None:
        self.model = local_name(content_model.tag)
        if self.model == 'all':
            if self.max_occurs != 1:
                msg = _("maxOccurs must be 1 for 'all' model groups")
                self.parse_error(msg)
            if self.min_occurs not in (0, 1):
                msg = _("minOccurs must be (0 | 1) for 'all' model groups")
                self.parse_error(msg)

        child: ElementType
        for child in content_model:
            match child.tag:
                case x if callable(x):
                    continue
                case nm.XSD_ANNOTATION:
                    continue
                case nm.XSD_ELEMENT:
                    self._group.append(self.builders.element_class(child, self.schema, self))
                case nm.XSD_ALL:
                    self.parse_error(_("'all' model can contain only elements"))
                case nm.XSD_ANY:
                    self._group.append(
                        self.builders.any_element_class(child, self.schema, self)
                    )
                case nm.XSD_SEQUENCE | nm.XSD_CHOICE:
                    self._group.append(XsdGroup(child, self.schema, self))
                case nm.XSD_GROUP:
                    try:
                        ref = self.schema.resolve_qname(child.attrib['ref'])
                    except (KeyError, ValueError, RuntimeError) as err:
                        if 'ref' not in child.attrib:
                            msg = _("missing attribute 'ref' in local group")
                            self.parse_error(msg, child)
                        else:
                            self.parse_error(err, child)
                        continue

                    if ref != self.name:
                        xsd_group = XsdGroup(child, self.schema, self)
                        if xsd_group.model == 'all':
                            msg = _("'all' model can appears only at 1st level of a model group")
                            self.parse_error(msg)
                        else:
                            self._group.append(xsd_group)
                    elif self.redefine is not None:
                        self._group.append(self.redefine)
                        if child.get('minOccurs', '1') != '1' \
                                or child.get('maxOccurs', '1') != '1':
                            msg = _("Redefined group reference can't have "
                                    "minOccurs/maxOccurs other than 1")
                            self.parse_error(msg)
                    else:
                        msg = _("Circular definition detected for group %r")
                        self.parse_error(msg % self.name)

    def build(self) -> None:
        if self._built is False:
            self._built = None
            try:
                for item in self._group:
                    if isinstance(item, XsdElement):
                        item.build()

                if self.redefine is not None:
                    for group in self.redefine.iter_components(XsdGroup):
                        group.build()
                self._built = True
            finally:
                if self._built is None:
                    self._built = False

    @property
    def schema_elem(self) -> ElementType:
        return self.parent.elem if self.parent is not None else self.elem

    def iter_components(self, xsd_classes: Optional[ComponentClassType] = None) \
            -> Iterator[XsdComponent]:
        if xsd_classes is None or isinstance(self, xsd_classes):
            yield self
        for item in self:
            if item.parent is None:
                continue
            elif item.parent is not self.parent and isinstance(item.parent, XsdType) \
                    and item.parent.parent is None:
                continue
            yield from item.iter_components(xsd_classes)

        if self.redefine is not None and self.redefine not in self:
            yield from self.redefine.iter_components(xsd_classes)

    def admits_restriction(self, model: str) -> bool:
        if self.model == model:
            return True
        elif self.model == 'all':
            return model == 'sequence'
        elif self.model == 'choice':
            return model == 'sequence' or len(self.ref or self) <= 1
        else:
            return model == 'choice' or len(self.ref or self) <= 1

    def is_empty(self) -> bool:
        return not self.mixed and (not self._group or self.max_occurs == 0)

    @schema_cache
    def is_restriction(self, other: ModelParticleType, check_occurs: bool = True) -> bool:
        if not self._group:
            return True
        elif not isinstance(other, ParticleMixin):
            raise XMLSchemaValueError("the argument 'other' must be an XSD particle")
        elif not isinstance(other, XsdGroup):
            return self.is_element_restriction(other)
        elif not other:
            return False
        elif len(other) == other.min_occurs == other.max_occurs == 1:
            if len(self) > 1:
                return self.is_restriction(other[0], check_occurs)
            elif self.ref is None and isinstance(self[0], XsdGroup) \
                    and self[0].is_pointless(parent=self):
                return self[0].is_restriction(other[0], check_occurs)

        # Compare model with model
        if self.model != other.model and self.model != 'sequence' and \
                (len(self) > 1 or self.ref is not None and len(self[0]) > 1):
            return False
        elif self.model == other.model or other.model == 'sequence':
            return self.is_sequence_restriction(other)
        elif other.model == 'all':
            return self.is_all_restriction(other)
        else:  # other.model == 'choice':
            return self.is_choice_restriction(other)

    @schema_cache
    def is_element_restriction(self, other: ModelParticleType) -> bool:
        if self.xsd_version == '1.0' and isinstance(other, XsdElement) and \
                not other.ref and other.name not in self.maps.substitution_groups:
            return False
        elif not self.has_occurs_restriction(other):
            return False
        elif self.model == 'choice':
            if all(e.is_substitute(other) for e in self):
                return True
            return any(e.is_restriction(other, False) for e in self)
        else:
            min_occurs = 0
            max_occurs: Optional[int] = 0
            for item in self.iter_model():
                if isinstance(item, XsdGroup):
                    return False
                elif item.min_occurs == 0 or item.is_restriction(other, False):
                    min_occurs += item.min_occurs
                    if max_occurs is not None:
                        if item.max_occurs is None:
                            max_occurs = None
                        else:
                            max_occurs += item.max_occurs
                    continue
                return False

            if min_occurs < other.min_occurs:
                return False
            elif max_occurs is None:
                return other.max_occurs is None
            elif other.max_occurs is None:
                return True
            else:
                return max_occurs <= other.max_occurs

    @schema_cache
    def is_sequence_restriction(self, other: 'XsdGroup') -> bool:
        if not self.has_occurs_restriction(other):
            return False

        check_occurs = other.max_occurs != 0

        # Same model: declarations must simply preserve order
        other_iterator = iter(other.iter_model())
        for item in self.iter_model():
            for other_item in other_iterator:
                if other_item is item or item.is_restriction(other_item, check_occurs):
                    break
                elif other.model == 'choice':
                    if item.max_occurs != 0:
                        continue
                    elif not other_item.is_matching(item.name):
                        continue
                    elif all(e.max_occurs == 0 for e in self.iter_model()):
                        return False
                    else:
                        break
                elif not other_item.is_emptiable():
                    return False
            else:
                return False

        if other.model != 'choice':
            for other_item in other_iterator:
                if not other_item.is_emptiable():
                    return False
        return True

    @schema_cache
    def is_all_restriction(self, other: 'XsdGroup') -> bool:
        if not self.has_occurs_restriction(other):
            return False

        check_occurs = other.max_occurs != 0
        if self.ref is None:
            restriction_items = [x for x in self]
        else:
            restriction_items = [x for x in self[0]]

        for other_item in other.iter_model():
            for item in restriction_items:
                if other_item is item or item.is_restriction(other_item, check_occurs):
                    break
            else:
                if not other_item.is_emptiable():
                    return False
                continue
            restriction_items.remove(item)

        return not bool(restriction_items)

    def is_choice_restriction(self, other: 'XsdGroup') -> bool:
        if self.ref is None:
            if self.parent is None and other.parent is not None:
                return False  # not allowed restriction in XSD 1.0
            restriction_items = [x for x in self]
        elif other.parent is None:
            restriction_items = [x for x in self[0]]
        else:
            return False  # not allowed restriction in XSD 1.0

        check_occurs = other.max_occurs != 0
        max_occurs: Optional[int] = 0
        other_max_occurs: Optional[int] = 0

        for other_item in other.iter_model():
            for item in restriction_items:
                if other_item is item or item.is_restriction(other_item, check_occurs):
                    if max_occurs is not None:
                        if item.max_occurs is None:
                            max_occurs = None
                        else:
                            max_occurs += item.max_occurs

                    if other_max_occurs is not None:
                        if other_item.max_occurs is None:
                            other_max_occurs = None
                        else:
                            other_max_occurs = max(other_max_occurs, other_item.max_occurs)
                    break
            else:
                continue
            restriction_items.remove(item)

        if restriction_items:
            return False
        elif other_max_occurs is None:
            if other.max_occurs != 0:
                return True
            other_max_occurs = 0
        elif other.max_occurs is None:
            if other_max_occurs != 0:
                return True
            other_max_occurs = 0
        else:
            other_max_occurs *= other.max_occurs

        if max_occurs is None:
            return self.max_occurs == 0
        elif self.max_occurs is None:
            return max_occurs == 0
        else:
            return other_max_occurs >= max_occurs * self.max_occurs

    def check_dynamic_context(self, elem: ElementType,
                              xsd_element: SchemaElementType,
                              model_element: SchemaElementType,
                              namespaces: NsmapType) -> None:

        if model_element is not xsd_element and isinstance(model_element, XsdElement):
            if 'substitution' in model_element.block \
                    or xsd_element.type and xsd_element.type.is_blocked(model_element):
                reason = _("substitution of %r is blocked") % model_element
                raise XMLSchemaValidationError(model_element, elem, reason)

        alternatives: Union[tuple[()], list[XsdAlternative]] = []
        if isinstance(xsd_element, XsdAnyElement):
            if xsd_element.process_contents == 'skip':
                return

            try:
                xsd_element = self.maps.elements[elem.tag]
            except KeyError:
                if self.schema.meta_schema is None:
                    # Meta-schema groups ignore xsi:type (issue #350)
                    return

                try:
                    type_name = elem.attrib[nm.XSI_TYPE].strip()
                except KeyError:
                    return
                else:
                    xsd_type = self.maps.get_instance_type(
                        type_name, self.maps.any_type, namespaces
                    )
            else:
                alternatives = xsd_element.alternatives
                try:
                    type_name = elem.attrib[nm.XSI_TYPE].strip()
                except KeyError:
                    xsd_type = xsd_element.type
                else:
                    xsd_type = self.maps.get_instance_type(
                        type_name, xsd_element.type, namespaces
                    )

        else:
            if nm.XSI_TYPE not in elem.attrib or self.schema.meta_schema is None:
                xsd_type = xsd_element.type
            else:
                alternatives = xsd_element.alternatives
                try:
                    type_name = elem.attrib[nm.XSI_TYPE].strip()
                except KeyError:
                    xsd_type = xsd_element.type
                else:
                    xsd_type = self.maps.get_instance_type(
                        type_name, xsd_element.type, namespaces
                    )

            if model_element is not xsd_element and \
                    isinstance(model_element, XsdElement) and model_element.block:
                for derivation in model_element.block.split():
                    if xsd_type is not model_element.type and \
                            xsd_type.is_derived(model_element.type, derivation):
                        reason = _("usage of {0!r} with type {1} is blocked by "
                                   "head element").format(xsd_element, derivation)
                        raise XMLSchemaValidationError(self, elem, reason)

            if nm.XSI_TYPE not in elem.attrib or self.schema.meta_schema is None:
                return

        # If it's a restriction the context is the base_type's group
        group = self.restriction if self.restriction is not None else self

        # Dynamic EDC check of matched element
        for e in group.elements:
            if not isinstance(e, XsdElement):
                continue
            elif (other := e.match(elem.tag)) is None:
                continue

            if len(other.alternatives) != len(alternatives) or \
                    not xsd_type.is_dynamic_consistent(other.type):
                reason = _("{0!r} that matches {1!r} is not consistent with local "
                           "declaration {2!r}").format(elem, xsd_element, other)
                raise XMLSchemaValidationError(self, reason)

            if not all(any(a == x for x in alternatives) for a in other.alternatives) or \
                    not all(any(a == x for x in other.alternatives) for a in alternatives):
                msg = _("Maybe a not equivalent type table between elements "
                        "{0!r} and {1!r}.").format(self, xsd_element)
                warnings.warn(msg, XMLSchemaTypeTableWarning, stacklevel=3)

    @schema_cache
    def match_element(self, name: str) -> Optional[SchemaElementType]:
        """
        Try a model-less match of a child element. Returns the
        matched element, or `None` if there is no match.
        """
        for xsd_element in self.elements:
            if xsd_element.is_matching(name, group=self):
                return xsd_element
        return None

    def raw_decode(self, obj: ElementType, validation: str, context: ValidationContext) \
            -> GroupDecodeType:
        """
        Decoding an Element content.

        :param obj: an Element.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip.
        :param context: the encoding context.
        :return: a list of 3-tuples (key, decoded data, decoder).
        """
        result: GroupDecodeType = None if context.validation_only else []
        cdata_index = 1  # keys for CDATA sections are positive integers
        index = 0

        if not self._group and self.model == 'choice' and self.min_occurs:
            reason = _("an empty 'choice' group with minOccurs > 0 cannot validate any content")
            context.validation_error(validation, self, reason, obj)
            return result

        if not self.mixed:
            # Check element CDATA
            if obj.text and obj.text.strip() or \
                    any(child.tail and child.tail.strip() for child in obj):
                if len(self) == 1 and isinstance(self[0], XsdAnyElement):
                    pass  # [XsdAnyElement()] equals to an empty complexType declaration
                else:
                    reason = _("character data between child elements not allowed")
                    context.validation_error(validation, self, reason, obj)
                    cdata_index = 0  # Do not decode CDATA

        if cdata_index and obj.text is not None:
            if self.mixed and context.preserve_mixed:
                text = obj.text
            else:
                text = str(obj.text.strip())
            if text:
                if result is not None:
                    result.append((cdata_index, text, None))
                cdata_index += 1

        over_max_depth = context.max_depth is not None and context.max_depth <= context.level

        errors: list[tuple[int, ModelParticleType, int, Optional[list[SchemaElementType]]]]
        xsd_element: Optional[SchemaElementType]
        expected: Optional[list[SchemaElementType]]

        errors = []
        broken_model = False
        namespaces = context.namespaces
        model = self.get_model_visitor()

        for index, child in enumerate(obj):
            if callable(child.tag):
                continue  # child is a comment or PI

            context.converter.set_xmlns_context(child, context.level)
            name = context.converter.map_qname(child.tag)

            while model.element is not None:
                xsd_element = model.match_element(child.tag)
                if xsd_element is None:
                    for particle, occurs, expected in model.advance(False):
                        errors.append((index, particle, occurs, expected))
                        model.clear()
                        broken_model = True  # the model is broken, continues with raw decoding.
                        xsd_element = self.match_element(child.tag)
                        break
                    else:
                        continue
                    break

                try:
                    self.check_dynamic_context(child, xsd_element, model.element, namespaces)
                except (XMLSchemaValidationError, TypeError) as err:
                    context.validation_error(validation, self, err, obj)

                for particle, occurs, expected in model.advance(True):
                    errors.append((index, particle, occurs, expected))
                break
            else:
                xsd_element = self.match_element(child.tag)
                if xsd_element is None:
                    errors.append((index, self, 0, None))
                    broken_model = True
                elif not broken_model:
                    errors.append((index, xsd_element, 0, []))
                    broken_model = True

            # Optional checks on matched XSD child
            if not isinstance(context, DecodeContext):
                if xsd_element is None or over_max_depth:
                    continue
            elif xsd_element is None:
                if context.keep_unknown:
                    result_item = self.maps.any_type.raw_decode(child, validation, context)
                    if result is not None:
                        result.append((name, result_item, None))
                continue
            elif over_max_depth:
                if context.depth_filler is not None and isinstance(xsd_element, XsdElement):
                    func = context.depth_filler
                    if result is not None:
                        result.append((name, func(xsd_element), xsd_element))
                continue

            result_item = xsd_element.raw_decode(child, validation, context)
            if result_item is Empty:
                continue
            elif result is not None:
                result.append((name, result_item, xsd_element))

                if cdata_index and child.tail is not None:
                    if self.mixed and context.preserve_mixed:
                        tail = child.tail
                    else:
                        tail = str(child.tail.strip())
                    if tail:
                        if result and isinstance(result[-1][0], int):
                            tail = result[-1][1] + ' ' + tail
                            result[-1] = result[-1][0], tail, None
                        else:
                            result.append((cdata_index, tail, None))
                            cdata_index += 1

        if model.element is not None:
            index = len(obj)
            for particle, occurs, expected in model.stop():
                errors.append((index, particle, occurs, expected))
                break

        if errors:
            for index, particle, occurs, expected in errors:
                context.children_validation_error(
                    validation, self, obj, index, particle, occurs, expected
                )

        if index or result is not None or obj.text is None:
            return result
        elif self.mixed and context.preserve_mixed:
            return [(1, obj.text, None)]
        else:
            return [(1, str(obj.text.strip()), None)]

    def raw_encode(self, obj: ElementData, validation: str, context: EncodeContext) \
            -> GroupEncodeType:
        """
        Encode data to a list containing Element children.

        :param obj: an ElementData instance.
        :param validation: the validation mode. Can be 'lax', 'strict' or 'skip'.
        :param context: the encoding context.
        :return: returns a couple with the text of the Element and a list of child \
        elements.
        """
        errors = []
        text = raw_encode_value(obj.text)
        children: list[ElementType] = []
        default_namespace = context.converter.get('')

        if (elem := context.elem) is None:
            context.elem = elem = context.create_element(tag=obj.tag)
            elem.attrib.update(raw_encode_attributes(obj.attributes))

        index = cdata_index = 0
        wrong_content_type = False
        over_max_depth = context.max_depth is not None and context.max_depth <= context.level
        model = self.get_model_visitor()

        content: Iterable[Any]
        if not obj.content:
            content = []
        elif isinstance(obj.content, MutableMapping) or context.unordered:
            content = iter_unordered_content(obj.content, self)
        elif not isinstance(obj.content, MutableSequence):
            wrong_content_type = True
            content = []
        elif not isinstance(obj.content[0], tuple):
            if len(obj.content) > 1 or text is not None:
                wrong_content_type = True
            else:
                text = raw_encode_value(obj.content[0])
            content = []
        elif context.converter.losslessly:
            content = obj.content
        else:
            content = iter_collapsed_content(obj.content, self)

        for index, (name, value) in enumerate(content):
            if isinstance(name, int):
                if not children:
                    text = text + value if text is not None else value
                else:
                    children[-1].tail = value
                cdata_index += 1
                continue

            xsd_element: Optional[SchemaElementType]
            while model.element is not None:
                xsd_element = model.match_element(name)
                if xsd_element is None:
                    for particle, occurs, expected in model.advance():
                        errors.append((index - cdata_index, particle, occurs, expected))
                    continue
                elif isinstance(xsd_element, XsdAnyElement):
                    value = get_qname(default_namespace, name), value

                for particle, occurs, expected in model.advance(True):
                    errors.append((index - cdata_index, particle, occurs, expected))
                break
            else:
                errors.append((index - cdata_index, self, 0, []))
                xsd_element = self.match_element(name)
                if isinstance(xsd_element, XsdAnyElement):
                    value = get_qname(default_namespace, name), value
                elif xsd_element is None:
                    if name.startswith('{') or ':' not in name:
                        reason = _('{!r} does not match any declared element '
                                   'of the model group').format(name)
                    else:
                        reason = _('{0} has an unknown prefix {1!r}').format(
                            name, name.split(':')[0]
                        )
                    context.validation_error(validation, self, reason, value)
                    continue

            if xsd_element.skip and not context.process_skipped:
                continue
            if over_max_depth:
                continue

            child = xsd_element.raw_encode(value, validation, context)
            if children is not None and child is not None:
                children.append(child)

        if model.element is not None:
            for particle, occurs, expected in model.stop():
                errors.append((index - cdata_index + 1, particle, occurs, expected))
                break

        context.set_element_content(
            elem=elem,
            text=text,
            children=children,
            level=context.level,
            mixed=self.mixed and context.preserve_mixed
        )

        if wrong_content_type:
            reason = _("wrong content type {!r}").format(type(obj.content))
            context.validation_error(validation, self, reason, elem)

        if not self.mixed and text and text.strip() and self and \
                (len(self) > 1 or not isinstance(self[0], XsdAnyElement)):
            reason = _("character data between child elements not allowed")
            context.validation_error(validation, self, reason, elem)

        for index, particle, occurs, expected in errors:
            context.children_validation_error(
                validation, self, elem, index, particle, occurs, expected
            )

        return elem


class Xsd11Group(XsdGroup):
    """
    Class for XSD 1.1 *model group* definitions.

    .. The XSD 1.1 model groups differ from XSD 1.0 groups for the 'all' model,
       that can contains also other groups.
    ..  <all
          id = ID
          maxOccurs = (0 | 1) : 1
          minOccurs = (0 | 1) : 1
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?, (element | any | group)*)
        </all>
    """
    def _parse_content_model(self, content_model: ElementType) -> None:
        self.model = local_name(content_model.tag)
        if self.model == 'all':
            if self.max_occurs not in (0, 1):
                msg = _("maxOccurs must be (0 | 1) for 'all' model groups")
                self.parse_error(msg)
            if self.min_occurs not in (0, 1):
                msg = _("minOccurs must be (0 | 1) for 'all' model groups")
                self.parse_error(msg)

        for child in content_model:
            match child.tag:
                case nm.XSD_ELEMENT:
                    self._group.append(self.builders.element_class(child, self.schema, self))
                case nm.XSD_ANY:
                    self._group.append(self.builders.any_element_class(child, self.schema, self))
                case x if x in nm.MODEL_TAGS:
                    self._group.append(Xsd11Group(child, self.schema, self))
                case nm.XSD_GROUP:
                    try:
                        ref = self.schema.resolve_qname(child.attrib['ref'])
                    except (KeyError, ValueError, RuntimeError) as err:
                        if 'ref' not in child.attrib:
                            msg = _("missing attribute 'ref' in local group")
                            self.parse_error(msg, child)
                        else:
                            self.parse_error(err, child)
                        continue

                    if ref != self.name:
                        xsd_group = Xsd11Group(child, self.schema, self)
                        self._group.append(xsd_group)
                        if (self.model != 'all') ^ (xsd_group.model != 'all'):
                            msg = _("an xs:{0} group cannot include a reference to an "
                                    "xs:{1} group").format(self.model, xsd_group.model)
                            self.parse_error(msg)
                            self._group.pop()

                    elif self.redefine is not None:
                        if child.get('minOccurs', '1') != '1' or child.get('maxOccurs', '1') != '1':
                            msg = _("Redefined group reference cannot have "
                                    "minOccurs/maxOccurs other than 1")
                            self.parse_error(msg)
                        self._group.append(self.redefine)
                    else:
                        msg = _("Circular definition detected for group %r")
                        self.parse_error(msg % self.name)

    def admits_restriction(self, model: str) -> bool:
        if self.model == model or self.model == 'all':
            return True
        elif self.model == 'choice':
            return model == 'sequence' or len(self.ref or self) <= 1
        else:
            return model == 'choice' or len(self.ref or self) <= 1

    def is_restriction(self, other: ModelParticleType, check_occurs: bool = True) -> bool:
        if not self._group:
            return True
        elif not isinstance(other, ParticleMixin):
            raise XMLSchemaValueError("the argument 'base' must be a %r instance" % ParticleMixin)
        elif not isinstance(other, XsdGroup):
            return self.is_element_restriction(other)
        elif not other:
            return False
        elif len(other) == other.min_occurs == other.max_occurs == 1:
            if len(self) > 1:
                return self.is_restriction(other[0], check_occurs)
            elif self.ref is None and isinstance(self[0], XsdGroup) \
                    and self[0].is_pointless(parent=self):
                return self[0].is_restriction(other[0], check_occurs)

        if other.model == 'sequence':
            return self.is_sequence_restriction(other)
        elif other.model == 'all':
            return self.is_all_restriction(other)
        else:  # other.model == 'choice':
            return self.is_choice_restriction(other)

    def has_occurs_restriction(
            self, other: Union[ModelParticleType, ParticleMixin, 'OccursCalculator']) -> bool:
        if not isinstance(other, XsdGroup):
            return super().has_occurs_restriction(other)
        elif not self:
            return True
        elif self.effective_min_occurs < other.effective_min_occurs:
            return False

        effective_max_occurs = self.effective_max_occurs
        if effective_max_occurs == 0:
            return True
        elif effective_max_occurs is None:
            return other.effective_max_occurs is None

        try:
            return effective_max_occurs <= other.effective_max_occurs  # type: ignore[operator]
        except TypeError:
            return True

    def is_sequence_restriction(self, other: XsdGroup) -> bool:
        if not self.has_occurs_restriction(other):
            return False

        check_occurs = other.max_occurs != 0

        item_iterator = iter(self.iter_model())
        item = next(item_iterator, None)

        for other_item in other.iter_model():
            if item is not None and item.is_restriction(other_item, check_occurs):
                item = next(item_iterator, None)
            elif not other_item.is_emptiable():
                break
        else:
            if item is None:
                return True

        # Restriction check failed: try another check without removing pointless groups
        item_iterator = iter(self)
        item = next(item_iterator, None)

        for other_item in other.iter_model():
            if item is not None and item.is_restriction(other_item, check_occurs):
                item = next(item_iterator, None)
            elif not other_item.is_emptiable():
                break
        else:
            if item is None:
                return True

        # Restriction check failed again: try checking other items against self
        other_items = other.iter_model()
        for other_item in other_items:
            if self.is_restriction(other_item, check_occurs):
                return all(x.is_emptiable() for x in other_items)
            elif not other_item.is_emptiable():
                return False
        else:
            return False

    def is_all_restriction(self, other: XsdGroup) -> bool:
        restriction_items = [x for x in self.iter_model()]

        base_items = [x for x in other.iter_model()]

        # If the base includes more wildcard, calculates and appends a
        # wildcard union for validating wildcard unions in restriction
        wildcards: list[XsdAnyElement] = []
        extended: list[XsdAnyElement] = []
        for w1 in base_items:
            if isinstance(w1, XsdAnyElement):
                for w2 in wildcards:
                    if w1.process_contents == w2.process_contents and \
                            get_occurs(w1) == get_occurs(w2):
                        w2.union(w1)
                        extended.append(w2)
                        break
                else:
                    wildcards.append(_copy(w1))

        base_items.extend(extended)

        if self.model != 'choice':
            restriction_wildcards = [e for e in restriction_items if isinstance(e, XsdAnyElement)]

            for other_item in base_items:
                min_occurs, max_occurs = 0, other_item.max_occurs
                for k in range(len(restriction_items) - 1, -1, -1):
                    item = restriction_items[k]

                    if item.is_restriction(other_item, check_occurs=False):
                        if max_occurs is None:
                            min_occurs += item.min_occurs
                        elif item.max_occurs is None or max_occurs < item.max_occurs or \
                                min_occurs + item.min_occurs > max_occurs:
                            continue
                        else:
                            min_occurs += item.min_occurs
                            max_occurs -= item.max_occurs

                        restriction_items.remove(item)
                        if not min_occurs or max_occurs == 0:
                            break
                else:
                    if self.model == 'all' and restriction_wildcards:
                        if not isinstance(other_item, XsdGroup) and other_item.type \
                                and other_item.type.name != nm.XSD_ANY_TYPE:

                            for w in restriction_wildcards:
                                if w.is_matching(other_item.name, self.target_namespace):
                                    return False

                if min_occurs < other_item.min_occurs:
                    break
            else:
                if not restriction_items:
                    return True
            return False

        # Restriction with a choice model: this a more complex case
        # because the not emptiable elements of the base group have
        # to be included in each item of the choice group.
        not_emptiable_items = {x for x in base_items if x.min_occurs}

        for other_item in base_items:
            min_occurs, max_occurs = 0, other_item.max_occurs
            for k in range(len(restriction_items) - 1, -1, -1):
                item = restriction_items[k]

                if item.is_restriction(other_item, check_occurs=False):
                    if max_occurs is None:
                        min_occurs += item.min_occurs
                    elif item.max_occurs is None or max_occurs < item.max_occurs or \
                            min_occurs + item.min_occurs > max_occurs:
                        continue
                    else:
                        min_occurs += item.min_occurs
                        max_occurs -= item.max_occurs

                    if not_emptiable_items:
                        if len(not_emptiable_items) > 1:
                            continue
                        if other_item not in not_emptiable_items:
                            continue

                    restriction_items.remove(item)
                    if not min_occurs or max_occurs == 0:
                        break

            if min_occurs < other_item.min_occurs:
                break
        else:
            if not restriction_items:
                return True

        if any(not isinstance(x, XsdGroup) for x in restriction_items):
            return False

        # If the remaining items are groups try to verify if they are all
        # restrictions of the 'all' group and if each group contains all
        # not emptiable elements.
        for group in restriction_items:
            if not group.is_restriction(other):
                return False

            for item in not_emptiable_items:
                for e in group:
                    if e.name == item.name:
                        break
                else:
                    return False
        else:
            return True

    def is_choice_restriction(self, other: XsdGroup) -> bool:
        restriction_items = [x for x in self.iter_model()]
        has_not_empty_item = any(e.max_occurs != 0 for e in restriction_items)

        check_occurs = other.max_occurs != 0
        max_occurs: Optional[int] = 0
        other_max_occurs: Optional[int] = 0

        for other_item in other.iter_model():
            for item in restriction_items:
                if other_item is item or item.is_restriction(other_item, check_occurs):
                    if max_occurs is not None:
                        effective_max_occurs = item.effective_max_occurs
                        if effective_max_occurs is None:
                            max_occurs = None
                        elif self.model == 'choice':
                            max_occurs = max(max_occurs, effective_max_occurs)
                        else:
                            max_occurs += effective_max_occurs

                    if other_max_occurs is not None:
                        effective_max_occurs = other_item.effective_max_occurs
                        if effective_max_occurs is None:
                            other_max_occurs = None
                        else:
                            other_max_occurs = max(other_max_occurs, effective_max_occurs)
                    break
                elif item.max_occurs != 0:
                    continue
                elif not other_item.is_matching(item.name):
                    continue
                elif has_not_empty_item:
                    break
                else:
                    return False
            else:
                continue
            restriction_items.remove(item)

        if restriction_items:
            return False
        elif other_max_occurs is None:
            if other.max_occurs != 0:
                return True
            other_max_occurs = 0
        elif other.max_occurs is None:
            if other_max_occurs != 0:
                return True
            other_max_occurs = 0
        else:
            other_max_occurs *= other.max_occurs

        if max_occurs is None:
            return self.max_occurs == 0
        elif self.max_occurs is None:
            return max_occurs == 0
        else:
            return other_max_occurs >= max_occurs * self.max_occurs

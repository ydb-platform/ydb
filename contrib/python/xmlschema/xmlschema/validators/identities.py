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
This module contains classes for other XML Schema identity constraints.
"""
import copy
import re
import math
from collections import Counter
from collections.abc import Iterator
from typing import TYPE_CHECKING, cast, Any, Optional, Union

from elementpath import ElementPathError, XPathContext, \
    ElementNode, translate_pattern, AttributeNode
from elementpath.datatypes import UntypedAtomic
from elementpath.xpath_nodes import EtreeElementNode

import xmlschema.names as nm
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError
from xmlschema.translation import gettext as _
from xmlschema.utils.qnames import get_qname, get_extended_qname
from xmlschema.aliases import ElementType, SchemaType, NsmapType, AtomicValueType, \
    BaseXsdType, SchemaElementType, SchemaAttributeType
from .helpers import parse_xpath_default_namespace
from ..xpath import IdentityXPathParser, XPathElement, XMLSchemaProxy

from .exceptions import XMLSchemaNotBuiltError
from .xsdbase import XsdComponent
from .attributes import XsdAttribute
from .wildcards import XsdAnyElement, XsdWildcard
from . import elements as elements_module

if TYPE_CHECKING:
    from .elements import XsdElement

IdentityFieldItemType = Union[AtomicValueType, XsdAttribute, tuple[Any, ...], None]
IdentityCounterType = tuple[IdentityFieldItemType, ...]


# XSD identities use a restricted XPath 2.0 parser. The XMLSchemaProxy is
# not used for the specific selection of fields and elements and the XSD
# fields are collected at first validation run.

IdentityMapType = dict[Union['XsdKey', 'XsdKeyref', str, None],
                       Union['IdentityCounter', 'KeyrefCounter']]
IdentityNodeType = Union[ElementNode, AttributeNode]
FieldDecoderType = Union[SchemaElementType, SchemaAttributeType]


class XsdSelector(XsdComponent):
    """Class for defining an XPath selector for an XSD identity constraint."""
    _ADMITTED_TAGS = nm.XSD_SELECTOR,
    _REGEXP = (
        r"(\.//)?(((child::)?((\i\c*:)?(\i\c*|\*)))|\.)(/(((child::)?"
        r"((\i\c*:)?(\i\c*|\*)))|\.))*(\|(\.//)?(((child::)?((\i\c*:)?"
        r"(\i\c*|\*)))|\.)(/(((child::)?((\i\c*:)?(\i\c*|\*)))|\.))*)*"
    )
    pattern: Optional[re.Pattern[str]] = None
    xpath_default_namespace = ''

    def __init__(self, elem: ElementType, schema: SchemaType,
                 parent: Optional['XsdIdentity']) -> None:
        super().__init__(elem, schema, parent)

    def _parse(self) -> None:
        try:
            self.path = self.elem.attrib['xpath']
        except KeyError:
            self.parse_error(_("'xpath' attribute required"))
            self.path = '*'
        else:
            path = self.path.replace(' ', '')
            if self.pattern is None:
                regexp = translate_pattern(
                    self._REGEXP,
                    back_references=False,
                    lazy_quantifiers=False,
                    anchors=False
                )
                self.__class__.pattern = re.compile(regexp)
                assert self.pattern is not None

            if not self.pattern.match(path):
                msg = _("invalid XPath expression for an {}")
                self.parse_error(msg.format(self.__class__.__name__))

        # XSD 1.1 xpathDefaultNamespace attribute
        if self.schema.XSD_VERSION > '1.0':
            if 'xpathDefaultNamespace' in self.elem.attrib:
                self.xpath_default_namespace = parse_xpath_default_namespace(self)
            else:
                self.xpath_default_namespace = self.schema.xpath_default_namespace

        self.parser = IdentityXPathParser(
            namespaces=self.schema.namespaces,
            strict=False,
            compatibility_mode=True,
            default_namespace=self.xpath_default_namespace,
        )

        try:
            self.token = self.parser.parse(self.path)
        except ElementPathError as err:
            self.token = self.parser.parse('*')
            self.parse_error(err)

    def __repr__(self) -> str:
        return '%s(path=%r)' % (self.__class__.__name__, self.path)


class XsdFieldSelector(XsdSelector):
    """Class for defining an XPath field selector for an XSD identity constraint."""
    _ADMITTED_TAGS = nm.XSD_FIELD,
    _REGEXP = (
        r"(\.//)?((((child::)?((\i\c*:)?(\i\c*|\*)))|\.)/)*((((child::)?"
        r"((\i\c*:)?(\i\c*|\*)))|\.)|((attribute::|@)((\i\c*:)?(\i\c*|\*))))"
        r"(\|(\.//)?((((child::)?((\i\c*:)?(\i\c*|\*)))|\.)/)*"
        r"((((child::)?((\i\c*:)?(\i\c*|\*)))|\.)|"
        r"((attribute::|@)((\i\c*:)?(\i\c*|\*)))))*"
    )
    pattern = None


class XsdIdentity(XsdComponent):
    """
    Common class for XSD identity constraints.

    :ivar selector: the XPath selector of the identity constraint.
    :ivar fields: a list containing the XPath field selectors of the identity constraint.
    """
    name: str
    local_name: str
    prefixed_name: str
    parent: 'XsdElement'
    ref: Optional['XsdIdentity']

    selector: Optional[XsdSelector]
    fields: list[XsdFieldSelector]

    # XSD elements bound by selector (for speed-up and for lazy mode)
    elements: dict['XsdElement', list['FieldValueSelector']]

    __slots__ = ('selector', 'fields', 'elements')

    def __init__(self, elem: ElementType, schema: SchemaType,
                 parent: Optional['XsdElement']) -> None:
        super().__init__(elem, schema, parent)

    def _parse(self) -> None:
        try:
            self.name = get_qname(self.target_namespace, self.elem.attrib['name'])
        except KeyError:
            self.parse_error(_("missing required attribute 'name'"))
            self.name = ''

        for child in self.elem:
            if child.tag == nm.XSD_SELECTOR:
                self.selector = XsdSelector(child, self.schema, self)
                break
        else:
            self.parse_error(_("missing 'selector' declaration"))
            self.selector = None

        self.fields = []
        for child in self.elem:
            if child.tag == nm.XSD_FIELD:
                self.fields.append(XsdFieldSelector(child, self.schema, self))

        self.elements = {}

    def build(self) -> None:
        if self._built is not False:
            return
        self._built = None

        try:
            if self.ref is self:
                try:
                    ref = self.maps.identities[self.name]
                except KeyError:
                    self.fields = []
                    self.elements = {}
                    msg = _("unknown identity constraint {!r}")
                    self.parse_error(msg.format(self.name))
                    self.ref = None
                    return
                else:
                    if not isinstance(ref, self.__class__):
                        msg = _("attribute 'ref' points to a different kind constraint")
                        self.parse_error(msg)
                    self.selector = ref.selector
                    self.fields = ref.fields
                    self.elements = {}
                    self.ref = ref

            try:
                self.update_elements(base_element=self.parent)
            except TypeError as err:
                self.parse_error(err)

            self._built = True
        finally:
            if self._built is None:
                self._built = False

    def update_elements(self, base_element: Union['XsdElement', XPathElement]) -> None:
        if self.selector is None:
            return

        context = XPathContext(self.schema.xpath_node, item=base_element.xpath_node)
        e: Any
        for e in self.selector.token.select_results(context):
            if isinstance(e, elements_module.XsdElement):
                if e.name is not None:
                    if e.ref is not None:
                        e = e.ref
                    if e not in self.elements:
                        self.elements[e] = [FieldValueSelector(f, e) for f in self.fields]
                        e.selected_by.add(self)

            elif not isinstance(e, (XsdAnyElement, XPathElement)):
                msg = _("selector xpath expression can only select elements")
                raise XMLSchemaTypeError(msg)

        # Try to detect other target XSD elements extracting QNames of
        # the leaf elements from the XPath expression and use them to
        # match from global elements. Anyway identity counters created
        # by identity are not enabled if the data is outside the scope.
        qname: Any
        for qname in self.selector.token.iter_leaf_elements():
            e = self.maps.elements.get(
                get_extended_qname(qname, self.schema.namespaces)
            )
            if isinstance(e, elements_module.XsdElement):
                if e.ref is not None:
                    e = e.ref
                if e not in self.elements:
                    self.elements[e] = [FieldValueSelector(f, e) for f in self.fields]
                    e.selected_by.add(self)

    def get_counter(self, elem: ElementType) -> 'IdentityCounter':
        return IdentityCounter(self, elem)


class XsdUnique(XsdIdentity):
    _ADMITTED_TAGS = nm.XSD_UNIQUE,


class XsdKey(XsdIdentity):
    _ADMITTED_TAGS = nm.XSD_KEY,


class XsdKeyref(XsdIdentity):
    """
    Implementation of xs:keyref.

    :ivar refer: reference to a *xs:key* declaration that must be in the same element \
    or in a descendant element.
    """
    _ADMITTED_TAGS = nm.XSD_KEYREF,
    refer: str | XsdKey | None = None
    refer_path = '.'

    def _parse(self) -> None:
        super()._parse()
        try:
            self.refer = self.schema.resolve_qname(self.elem.attrib['refer'])
        except (KeyError, ValueError, RuntimeError) as err:
            if 'refer' not in self.elem.attrib:
                self.parse_error(_("missing required attribute 'refer'"))
            else:
                self.parse_error(err)

    def build(self) -> None:
        if self._built:
            return
        super().build()

        if isinstance(self.refer, (XsdKey, XsdUnique)):
            return  # referenced key/unique identity constraint already set
        elif isinstance(self.ref, XsdKeyref):
            self.refer = self.ref.refer

        if self.refer is None:
            return  # attribute or key/unique identity constraint missing
        elif isinstance(self.refer, str):
            refer: Optional[XsdIdentity]
            for refer in self.parent.identities:
                if refer.name == self.refer:
                    break
            else:
                refer = None

            if refer is not None and refer.ref is None:
                self.refer = refer  # type: ignore[assignment]
            else:
                try:
                    self.refer = self.maps.identities[self.refer]  # type: ignore[assignment]
                except KeyError:
                    msg = _("key/unique identity constraint %r is missing")
                    self.parse_error(msg % self.refer)
                    return

        if not isinstance(self.refer, (XsdKey, XsdUnique)):
            msg = _("reference to a non key/unique identity constraint %r")
            self.parse_error(msg % self.refer)
        elif len(self.refer.fields) != len(self.fields):
            msg = _("field cardinality mismatch between {0!r} and {1!r}")
            self.parse_error(msg.format(self, self.refer))
        elif self.parent is not self.refer.parent:
            refer_path = self.refer.parent.get_path(ancestor=self.parent)
            if refer_path is None:
                # From a note in par. 3.11.5 Part 1 of XSD 1.0 spec: "keyref
                # identity-constraints may be defined on domains distinct from
                # the embedded domain of the identity-constraint they reference,
                # or the domains may be the same but self-embedding at some depth.
                # In either case the node table for the referenced identity-constraint
                # needs to propagate upwards, with conflict resolution."
                refer_path = self.parent.get_path(ancestor=self.refer.parent, reverse=True)
                if refer_path is None:
                    path1 = self.parent.get_path(reverse=True)
                    path2 = self.refer.parent.get_path()
                    assert path1 is not None
                    assert path2 is not None
                    refer_path = f'{path1}/{path2}'

            self.refer_path = refer_path

    def get_counter(self, elem: ElementType) -> 'KeyrefCounter':
        return KeyrefCounter(self, elem)


class Xsd11Unique(XsdUnique):
    def _parse(self) -> None:
        if self._parse_reference():
            self.ref = self
        else:
            super()._parse()


class Xsd11Key(XsdKey):
    def _parse(self) -> None:
        if self._parse_reference():
            self.ref = self
        else:
            super()._parse()


class Xsd11Keyref(XsdKeyref):
    def _parse(self) -> None:
        if self._parse_reference():
            self.ref = self
        else:
            super()._parse()


class IdentityCounter:
    elements: Optional[set[Any]]  # don't need to check, should be only etree elements anyway

    __slots__ = ('elements', 'counter', 'identity', 'elem', 'enabled')

    def __init__(self, identity: XsdIdentity, elem: ElementType) -> None:
        self.counter: Counter[IdentityCounterType] = Counter[IdentityCounterType]()
        self.identity = identity
        self.elem = elem
        self.enabled = True
        self.elements = None

    def __repr__(self) -> str:
        return "%s%r" % (self.__class__.__name__[:-7], self.counter)

    def reset(self, elem: ElementType) -> None:
        self.counter.clear()
        self.elem = elem
        self.enabled = True
        self.elements = None

    def increase(self, fields: IdentityCounterType) -> None:
        self.counter[fields] += 1
        if self.counter[fields] == 2:
            msg = _("duplicated value {0!r} for {1!r}")
            raise XMLSchemaValueError(msg.format(fields, self.identity))


class KeyrefCounter(IdentityCounter):
    identity: XsdKeyref

    def __init__(self, identity: XsdIdentity, elem: ElementType) -> None:
        super().__init__(identity, elem)
        if isinstance(self.identity.refer, (XsdKey, XsdUnique)):
            self.refer = self.identity.refer

    def increase(self, fields: IdentityCounterType) -> None:
        self.counter[fields] += 1

    def iter_errors(self, identities: dict[XsdIdentity, IdentityCounter]) \
            -> Iterator[XMLSchemaValueError]:
        if self.refer is None:
            return  # don't validate with an unbuilt keyref

        refer_values = identities[self.refer].counter

        for v in filter(lambda x: x not in refer_values, self.counter):
            if len(v) == 1 and v[0] in refer_values:
                continue
            elif self.counter[v] > 1:
                msg = "value {} not found for {!r} ({} times)"
                yield XMLSchemaValueError(msg.format(v, self.refer, self.counter[v]))
            else:
                msg = "value {} not found for {!r}"
                yield XMLSchemaValueError(msg.format(v, self.identity.refer))


class FieldValueSelector:

    __slots__ = ('field', 'xsd_element', 'xpath_proxy', 'value_constraints',
                 'token', 'decoders', 'skip_wildcard')

    def __init__(self, field: XsdFieldSelector, xsd_element: 'XsdElement') -> None:
        if field.token is None:
            msg = f"identity field {field} is not built"
            raise XMLSchemaNotBuiltError(field, msg)

        self.skip_wildcard = False
        self.field = field
        self.xsd_element = xsd_element
        self.value_constraints = {}

        self.xpath_proxy = XMLSchemaProxy(xsd_element.schema, xsd_element)
        self.token = copy.deepcopy(field.token)
        self.decoders = []

        for node in self.token.select(self.xpath_proxy.get_context()):
            if not isinstance(node, (AttributeNode, ElementNode)):
                raise XMLSchemaTypeError(
                    "xs:field path must select only attributes and elements"
                )

            comp = cast(FieldDecoderType, node.obj)
            self.decoders.append(comp)
            if isinstance(comp, XsdWildcard):
                if comp.process_contents == 'skip':
                    self.skip_wildcard = True
            else:
                value_constraint = comp.value_constraint
                if value_constraint is not None:
                    self.value_constraints[node.name] = comp.type.text_decode(value_constraint)
                    if isinstance(comp, XsdAttribute):
                        self.value_constraints[None] = self.value_constraints[node.name]

        if len(self.decoders) > 1 and None in self.value_constraints:
            self.value_constraints.pop(None)

    def get_value(self, element_node: EtreeElementNode,
                  namespaces: Optional[NsmapType] = None) -> IdentityFieldItemType:
        """
        Get field value from an element node for a schema or instance context element.

        :param element_node: a no Element
        :param namespaces: is an optional mapping from namespace prefix to URI.
        """
        value: Union[AtomicValueType, list[Optional[AtomicValueType]], None] = None
        element_node.schema = None  # type: ignore[assignment]
        context = XPathContext(
            element_node,
            namespaces=namespaces,
            schema=self.xpath_proxy,
        )

        empty = True
        for node in cast(Iterator[IdentityNodeType], self.token.select(context)):
            if empty:
                empty = False
            else:
                msg = _("%r field selects multiple values!")
                raise XMLSchemaValueError(msg % self.field)

            try:
                xsd_type = cast(Optional[BaseXsdType], node.xsd_type)
            except AttributeError:
                msg = _("%r field selects a %r!")
                raise XMLSchemaTypeError(msg % (self.field, type(node)))

            if xsd_type is None:
                if self.skip_wildcard:
                    value = None
                else:
                    value = node.string_value
            elif xsd_type.content_type_label not in ('simple', 'mixed'):
                msg = _("%r field doesn't have a simple type!")
                raise XMLSchemaTypeError(msg % self.field)
            elif xsd_type.is_qname():
                value = get_extended_qname(node.string_value.strip(), namespaces)
            elif xsd_type.is_boolean():
                # Workarounds for discovered issues with XPath processors
                value = xsd_type.text_decode(node.string_value.strip())
            else:
                try:
                    value = node.typed_value  # type: ignore[assignment,unused-ignore]
                except (KeyError, ValueError):
                    for decoder in self.decoders:
                        if not isinstance(decoder, XsdWildcard):
                            if decoder.is_matching(node.name):
                                value = decoder.type.text_decode(node.string_value)
                                break
                    else:
                        value = node.string_value

            if value is None:
                value = self.value_constraints.get(node.name)
        else:
            if empty:
                value = self.value_constraints.get(None)

        match value:
            case None:
                if not isinstance(self.field.parent, XsdKey) or \
                        'ref' in element_node.obj.attrib and \
                        self.field.schema.meta_schema is None and \
                        self.field.schema.XSD_VERSION != '1.0':
                    return None
                else:
                    msg = _("missing key field {0!r} for {1!r}")
                    raise XMLSchemaValueError(msg.format(self.field.path, self))
            case list():
                return tuple(value)
            case UntypedAtomic():
                return str(value)
            case bool():
                return value, bool
            case float():
                if math.isnan(value):
                    return 'nan', float
                else:
                    return value, float
            case _:
                return value

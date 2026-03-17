#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import warnings
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Union

from elementpath import ElementPathError, XPathContext, XPathToken, \
    SchemaElementNode, build_schema_node_tree

from xmlschema.names import XSD_ASSERT
from xmlschema.aliases import ElementType, SchemaType, SchemaElementType
from xmlschema.translation import gettext as _
from xmlschema.xpath import ElementPathMixin, XMLSchemaProxy

from .exceptions import XMLSchemaNotBuiltError, XMLSchemaAssertPathWarning
from .helpers import parse_xpath_default_namespace
from .validation import ValidationContext
from .xsdbase import XsdComponent
from .groups import XsdGroup

if TYPE_CHECKING:
    from elementpath import XPath2Parser  # noqa
    from elementpath.xpath3 import XPath3Parser  # noqa
    from . import XsdAttributeGroup, XsdComplexType, XsdElement, XsdAnyElement  # noqa

warnings.filterwarnings(action="always", category=XMLSchemaAssertPathWarning)


class XsdAssert(XsdComponent, ElementPathMixin[Union['XsdAssert', SchemaElementType]]):
    """
    Class for XSD *assert* constraint definitions.

    ..  <assert
          id = ID
          test = an XPath expression
          xpathDefaultNamespace = (anyURI | (##defaultNamespace | ##targetNamespace | ##local))
          {any attributes with non-schema namespace . . .}>
          Content: (annotation?)
        </assert>
    """
    parent: 'XsdComplexType'
    token: XPathToken
    parser: Union['XPath2Parser', 'XPath3Parser']

    _ADMITTED_TAGS = XSD_ASSERT,

    __slots__ = (
        'token', 'parser', 'path', 'base_type', 'xpath_default_namespace',
    )

    def __init__(self, elem: ElementType,
                 schema: SchemaType,
                 parent: 'XsdComplexType',
                 base_type: 'XsdComplexType') -> None:

        self.base_type = base_type
        super().__init__(elem, schema, parent)

    def __repr__(self) -> str:
        if len(self.path) < 40:
            return '%s(test=%r)' % (self.__class__.__name__, self.path)
        else:
            return '%s(test=%r)' % (self.__class__.__name__, self.path[:37] + '...')

    def _parse(self) -> None:
        try:
            self.path = self.elem.attrib['test'].strip()
        except KeyError:
            self.path = 'true()'
            self.parse_error(_("missing required attribute 'test'"))

        if 'xpathDefaultNamespace' in self.elem.attrib:
            self.xpath_default_namespace = parse_xpath_default_namespace(self)
        else:
            self.xpath_default_namespace = self.schema.xpath_default_namespace

    def build(self) -> None:
        # Assert requires a schema bound parser because select
        # is on XML elements and with XSD type decoded values
        if self._built is not False:
            return

        self._built = None
        self.parser = self.maps.xpath_parser_class(
            namespaces=self.schema.namespaces,
            variable_types={'value': self.base_type.sequence_type},
            strict=False,
            default_namespace=self.xpath_default_namespace,
            schema=self.xpath_proxy,
        )

        try:
            self.token = self.parser.parse(self.path)
        except ElementPathError as err:
            self.token = self.parser.parse('true()')
            self.parse_error(err)
        else:
            if any(len(tk) < 2 for tk in self.token.iter('/', '//')):
                msg = (
                    f"The XPath expression of {self} contains absolute location paths "
                    f"/ or //, but an assert XPath tree is rooted at a parentless elem"
                    f"ent so these operators will return empty sequences."
                )
                warnings.warn(msg, category=XMLSchemaAssertPathWarning, stacklevel=4)
            self._built = True
        finally:
            if self.parser.variable_types:
                self.parser.variable_types.clear()
            if self._built is None:
                self._built = False

    def __call__(self,
                 obj: ElementType,
                 validation: str,
                 context: ValidationContext,
                 value: Any = None) -> None:

        if not hasattr(self, 'parser') or not hasattr(self, 'token'):
            raise XMLSchemaNotBuiltError(self, 'schema bound parser not set')

        if not self.parser.is_schema_bound() and self.parser.schema:
            self.parser.schema.bind_parser(self.parser)

        if value is not None:
            value = self.base_type.text_decode(value, context=context)

        xpath_context = XPathContext(
            root=context.source.get_xpath_node(obj),
            namespaces=context.namespaces,
            uri=context.source.url,
            fragment=True,
            variables={'value': value},
            schema=self.parser.schema,
        )

        try:
            if not self.token.evaluate(xpath_context):
                context.validation_error(validation, self, "assertion test is false", obj)
        except ElementPathError as err:
            context.validation_error(validation, self, err, obj)

    # For implementing ElementPathMixin
    def __iter__(self) -> Iterator[Union['XsdElement', 'XsdAnyElement']]:
        if isinstance(self.parent.content, XsdGroup):
            yield from self.parent.content.elements

    @property
    def attrib(self) -> 'XsdAttributeGroup':
        return self.parent.attributes

    @property
    def type(self) -> 'XsdComplexType':
        return self.parent

    @property
    def xpath_proxy(self) -> 'XMLSchemaProxy':
        return XMLSchemaProxy(self.schema, self)

    # noinspection PyTypeChecker
    @property
    def xpath_node(self) -> SchemaElementNode:
        schema_node = self.schema.xpath_node
        node = schema_node.get_element_node(self)
        if isinstance(node, SchemaElementNode):
            return node

        return build_schema_node_tree(
            root=self,  # type: ignore[arg-type, unused-ignore] # FIXME: update protocols
            uri=schema_node.uri,
            elements=schema_node.elements,
            global_elements=schema_node.children,
        )

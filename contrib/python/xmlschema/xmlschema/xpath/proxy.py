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
from typing import cast, Any, Optional, Union, TYPE_CHECKING

from elementpath import AbstractSchemaProxy, XPath2Parser, XPathSchemaContext
from elementpath.protocols import XsdTypeProtocol

from xmlschema.exceptions import XMLSchemaValueError, XMLSchemaTypeError
from xmlschema.aliases import SchemaType
from xmlschema.names import XSD_NAMESPACE

if TYPE_CHECKING:
    from xmlschema.validators import XsdElement, XsdAnyElement, XsdAssert  # noqa:F401
    from .mixin import XPathElement  # noqa:F401

BaseElementType = Union['XsdElement', 'XsdAnyElement', 'XsdAssert', 'XPathElement']


class XMLSchemaProxy(AbstractSchemaProxy):
    """XPath schema proxy for the *xmlschema* library."""
    _schema: SchemaType
    _base_element: BaseElementType   # type: ignore[assignment, unused-ignore]

    def __init__(self, schema: Optional[SchemaType] = None,
                 base_element: Optional[BaseElementType] = None) -> None:

        if schema is None:
            from xmlschema import XMLSchema10
            schema = getattr(XMLSchema10, 'meta_schema', None)
            assert schema is not None

        super().__init__(schema, base_element)  # type: ignore[arg-type, unused-ignore]

        if base_element is not None:
            try:
                if base_element.schema is not schema:
                    msg = "{} is not an element of {}"
                    raise XMLSchemaValueError(msg.format(base_element, schema))
            except AttributeError:
                raise XMLSchemaTypeError("%r is not an XsdElement" % base_element)

    def bind_parser(self, parser: XPath2Parser) -> None:
        parser.schema = self
        parser.symbol_table = {k: v for k, v in parser.__class__.symbol_table.items()}
        parser.symbol_table.update(self._schema.maps.xpath_constructors)

    def get_context(self) -> XPathSchemaContext:
        if self._base_element is not None:
            return XPathSchemaContext(
                root=self._schema.xpath_node,
                namespaces=self._schema.namespaces,
                item=self._base_element.xpath_node,
            )
        return XPathSchemaContext(self._schema.xpath_node, self._schema.namespaces)

    def is_instance(self, obj: Any, type_qname: str) -> bool:
        xsd_type = self._schema.maps.types[type_qname]
        try:
            xsd_type.encode(obj)
        except ValueError:
            return False
        else:
            return True

    def cast_as(self, obj: Any, type_qname: str) -> Any:
        xsd_type = self._schema.maps.types[type_qname]
        return xsd_type.decode(obj)

    def iter_atomic_types(self) -> Iterator[XsdTypeProtocol]:
        for xsd_type in self._schema.maps.types.values():
            if getattr(xsd_type, 'target_namespace', None) != XSD_NAMESPACE and \
                    hasattr(xsd_type, 'primitive_type'):
                yield cast(XsdTypeProtocol, xsd_type)

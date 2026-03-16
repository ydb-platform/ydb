#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from abc import ABCMeta, abstractmethod
from collections.abc import Iterator
from functools import lru_cache
from typing import Any, Optional, Union

from elementpath.exceptions import ElementPathTypeError
from elementpath.protocols import XsdTypeProtocol, XsdAttributeProtocol, \
    XsdElementProtocol, XsdSchemaProtocol
from elementpath.aliases import AtomicType, XPath2ParserType
from elementpath.etree import is_etree_element
from elementpath.xpath_context import XPathSchemaContext

PathResult = Union[XsdSchemaProtocol, XsdElementProtocol, XsdAttributeProtocol]
FindAttrType = Optional[XsdAttributeProtocol]
FindElemType = Optional[XsdElementProtocol]


class AbstractSchemaProxy(metaclass=ABCMeta):
    """
    Abstract base class for defining schema proxies. An implementation can override
    initialization type annotations

    :param schema: a schema instance compatible with the XsdSchemaProtocol.
    :param base_element: the schema element used as base item for static analysis.
    """
    __slots__ = ('_schema', '_base_element', '_is_fully_valid')

    def __init__(self, schema: XsdSchemaProtocol,
                 base_element: Optional[XsdElementProtocol] = None) -> None:
        if not is_etree_element(schema):
            raise ElementPathTypeError(
                "argument {!r} is not a compatible schema instance".format(schema)
            )
        if base_element is not None and not is_etree_element(base_element):
            raise ElementPathTypeError(
                "argument 'base_element' is not a compatible element instance"
            )

        self._schema = schema
        self._base_element: Optional[XsdElementProtocol] = base_element
        self._is_fully_valid = False

    @property
    def schema(self) -> XsdSchemaProtocol:
        return self._schema

    @property
    def base_element(self) -> Optional[XsdElementProtocol]:
        return self._base_element

    @property
    def validity(self) -> str:
        validity = self._schema.validity
        if validity != 'valid':
            self._is_fully_valid = False
        return validity

    @property
    def validation_attempted(self) -> str:
        validation_attempted = self._schema.validation_attempted
        if validation_attempted != 'full':
            self._is_fully_valid = False
        return validation_attempted

    def is_fully_valid(self) -> bool:
        if self._is_fully_valid:
            return True
        self._is_fully_valid = self.validity == 'valid' and self.validation_attempted == 'full'
        return self._is_fully_valid

    def bind_parser(self, parser: XPath2ParserType) -> None:
        """
        Binds a parser instance with schema proxy adding the schema's atomic types constructors.
        This method can be redefined in a concrete proxy to optimize schema bindings.

        :param parser: a parser instance.
        """
        if parser.schema is not self:
            parser.schema = self

        for xsd_type in self.iter_atomic_types():
            if xsd_type.name is not None:  # pragma: no cover
                parser.schema_constructor(xsd_type.name)

    def get_context(self) -> XPathSchemaContext:
        """
        Get a context instance for static analysis phase.

        :returns: an `XPathSchemaContext` instance.
        """
        return XPathSchemaContext(root=self._schema, item=self._base_element, schema=self)

    def find(self, path: str, namespaces: Optional[dict[str, str]] = None) \
            -> Optional[PathResult]:
        """
        Find a schema element or attribute using an XPath expression. Currently unused
        in the code, it may be deprecated in the future.

        :param path: an XPath expression that selects an element or an attribute node.
        :param namespaces: an optional mapping from namespace prefix to namespace URI.
        :return: The first matching schema component, or ``None`` if there is no match.
        """
        if self._base_element is not None:
            return self._base_element.find(path, namespaces=namespaces)
        return self._schema.find(path, namespaces)

    @lru_cache(maxsize=None)
    def cached_find(self, expanded_path: str) -> Optional[PathResult]:
        """
        Find a schema element or attribute using an expanded XPath expression.
        Currently unused in the code, it may be deprecated in the future.
        """
        if self._base_element is not None:
            return self._base_element.find(expanded_path)
        return self._schema.find(expanded_path)

    @property
    def xsd_version(self) -> str:
        """The XSD version, returns '1.0' or '1.1'."""
        return self._schema.xsd_version

    def is_assertion_based(self) -> bool:
        return self._base_element is not None and \
            self._base_element.parent is self._base_element.type

    def get_type(self, qname: str) -> Optional[XsdTypeProtocol]:
        """
        Get the XSD global type from the schema's scope.

        :param qname: the fully qualified name of the type to retrieve.
        :returns: an object that represents an XSD type or `None`.
        """
        return self._schema.maps.types.get(qname)

    def get_attribute(self, qname: str) -> Optional[XsdAttributeProtocol]:
        """
        Get the XSD global attribute from the schema's scope.

        :param qname: the fully qualified name of the attribute to retrieve.
        """
        return self._schema.maps.attributes.get(qname)

    def get_element(self, qname: str,) -> Optional[XsdElementProtocol]:
        """
        Get the XSD global element from the schema's scope.

        :param qname: the fully qualified name of the attribute to retrieve.
        """
        return self._schema.maps.elements.get(qname)

    def get_substitution_group(self, qname: str) -> Optional[set[XsdElementProtocol]]:
        """
        Get a substitution group. Currently unused in the code, it may be deprecated
        in the future.

        :param qname: the fully qualified name of the substitution group to retrieve.
        :returns: a list containing substitution elements or `None`.
        """
        return self._schema.maps.substitution_groups.get(qname)

    @abstractmethod
    def is_instance(self, obj: Any, type_qname: str) -> bool:
        """
        Returns `True` if *obj* is an instance of the XSD global type, `False` if not.

        :param obj: the instance to be tested.
        :param type_qname: the fully qualified name of the type used to test the instance.
        """

    @abstractmethod
    def cast_as(self, obj: Any, type_qname: str) -> AtomicType:
        """
        Converts *obj* to the Python type associated with an XSD global type. A concrete
        implementation must raises a `ValueError` or `TypeError` in case of a decoding
        error or a `KeyError` if the type is not bound to the schema's scope.

        :param obj: the instance to be cast.
        :param type_qname: the fully qualified name of the type used to convert the instance.
        """

    @abstractmethod
    def iter_atomic_types(self) -> Iterator[XsdTypeProtocol]:
        """
        Returns an iterator for not builtin atomic types defined in the schema's scope. A concrete
        implementation must yield objects that implement the protocol `XsdTypeProtocol`.
        """


__all__ = ['PathResult', 'AbstractSchemaProxy']

#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from abc import abstractmethod
from collections.abc import Iterator, Sequence
from typing import cast, overload, Any, Optional, TypeVar, Union, TYPE_CHECKING

from elementpath import XPathSchemaContext, LazyElementNode, SchemaElementNode
from elementpath.protocols import XsdElementProtocol

from xmlschema.aliases import NsmapType, SchemaType, BaseXsdType
from xmlschema.utils.qnames import get_qname, local_name, get_prefixed_qname

from .proxy import XMLSchemaProxy
from .find_parser import SchemaFindParser

if TYPE_CHECKING:
    from ..validators import XsdGlobals

E_co = TypeVar('E_co', covariant=True, bound='ElementPathMixin[Any]')


class ElementPathMixin(Sequence[E_co]):
    """
    Mixin abstract class for enabling ElementTree and XPath 2.0 API on XSD components.

    :cvar text: the Element text, for compatibility with the ElementTree API.
    :cvar tail: the Element tail, for compatibility with the ElementTree API.
    """
    text: Optional[str] = None
    tail: Optional[str] = None
    name: Optional[str] = None
    attributes: Any = {}
    namespaces: Any = {}
    xpath_default_namespace = ''
    _xpath_node: Optional[Union[SchemaElementNode, LazyElementNode]] = None

    @abstractmethod
    def __iter__(self) -> Iterator[E_co]:
        raise NotImplementedError

    @overload
    def __getitem__(self, i: int) -> E_co: ...  # pragma: no cover

    @overload
    def __getitem__(self, s: slice) -> Sequence[E_co]: ...  # pragma: no cover

    def __getitem__(self, i: Union[int, slice]) -> Union[E_co, Sequence[E_co]]:
        try:
            return [e for e in self][i]
        except IndexError:
            raise IndexError('child index out of range')

    def __reversed__(self) -> Iterator[E_co]:
        return reversed([e for e in self])

    def __len__(self) -> int:
        return len([e for e in self])

    @property
    def tag(self) -> str:
        """Alias of the *name* attribute. For compatibility with the ElementTree API."""
        return self.name or ''

    @property
    def attrib(self) -> Any:
        """Returns the Element attributes. For compatibility with the ElementTree API."""
        return self.attributes

    def get(self, key: str, default: Any = None) -> Any:
        """Gets an Element attribute. For compatibility with the ElementTree API."""
        return self.attributes.get(key, default)

    @property
    def xpath_proxy(self) -> XMLSchemaProxy:
        """Returns an XPath proxy instance bound with the schema."""
        raise NotImplementedError

    @property
    def xpath_node(self) -> Union[SchemaElementNode, LazyElementNode]:
        """Returns an XPath node for applying selectors on XSD schema/component."""
        raise NotImplementedError

    def is_matching(self, name: Optional[str], default_namespace: Optional[str] = None) -> bool:
        if not name or name[0] == '{' or not default_namespace:
            return self.name == name
        else:
            return self.name == f'{{{default_namespace}}}{name}'

    def find(self, path: str, namespaces: Optional[NsmapType] = None) -> Optional[E_co]:
        """
        Finds the first XSD subelement matching the path.

        :param path: an XPath expression that considers the XSD component as the root element.
        :param namespaces: an optional mapping from namespace prefix to namespace URI.
        :return: the first matching XSD subelement or ``None`` if there is no match.
        """
        if namespaces is None:
            namespaces = self.namespaces
        parser = SchemaFindParser(namespaces, strict=False)
        context = XPathSchemaContext(self.xpath_node)
        return cast(Optional[E_co], next(parser.parse(path).select_results(context), None))

    def findall(self, path: str, namespaces: Optional[NsmapType] = None) -> list[E_co]:
        """
        Finds all XSD subelements matching the path.

        :param path: an XPath expression that considers the XSD component as the root element.
        :param namespaces: an optional mapping from namespace prefix to full name.
        :return: a list containing all matching XSD subelements in document order, an empty \
        list is returned if there is no match.
        """
        if namespaces is None:
            namespaces = self.namespaces
        parser = SchemaFindParser(namespaces, strict=False)
        context = XPathSchemaContext(self.xpath_node)
        return cast(list[E_co], parser.parse(path).get_results(context))

    def iterfind(self, path: str, namespaces: Optional[NsmapType] = None) -> Iterator[E_co]:
        """
        Creates and iterator for all XSD subelements matching the path.

        :param path: an XPath expression that considers the XSD component as the root element.
        :param namespaces: is an optional mapping from namespace prefix to full name.
        :return: an iterable yielding all matching XSD subelements in document order.
        """
        if namespaces is None:
            namespaces = self.namespaces
        parser = SchemaFindParser(namespaces, strict=False)
        context = XPathSchemaContext(self.xpath_node)
        return cast(Iterator[E_co], parser.parse(path).select_results(context))

    def iter(self, tag: Optional[str] = None) -> Iterator[E_co]:
        """
        Creates an iterator for the XSD element and its subelements. If tag is not `None` or '*',
        only XSD elements whose matches tag are returned from the iterator. Local elements are
        expanded without repetitions. Element references are not expanded because the global
        elements are not descendants of other elements.
        """
        def safe_iter(elem: Any) -> Iterator[E_co]:
            if tag is None or elem.is_matching(tag):
                yield elem
            for child in elem:
                if child.parent is None:
                    yield from safe_iter(child)
                elif getattr(child, 'ref', None) is not None:
                    if tag is None or child.is_matching(tag):
                        yield child
                elif child not in local_elements:
                    local_elements.add(child)
                    yield from safe_iter(child)

        if tag == '*':
            tag = None
        local_elements: set[E_co] = set()
        return safe_iter(self)

    def iterchildren(self, tag: Optional[str] = None) -> Iterator[E_co]:
        """
        Creates an iterator for the child elements of the XSD component. If *tag* is not `None`
        or '*', only XSD elements whose name matches tag are returned from the iterator.
        """
        if tag == '*':
            tag = None
        for child in self:
            if tag is None or child.is_matching(tag):
                yield child


class XPathElement(ElementPathMixin['XPathElement']):
    """An element node for making XPath operations on schema types."""
    name: str
    ref = None
    parent = None
    _xpath_node: Optional[LazyElementNode]

    def __init__(self, name: str, xsd_type: BaseXsdType) -> None:
        self.name = name
        self.type = xsd_type
        self.attributes = getattr(xsd_type, 'attributes', {})

    def __repr__(self) -> str:
        return '%s(%r, %r)' % (self.__class__.__name__, self.name, self.type)

    def __iter__(self) -> Iterator['XPathElement']:
        if not self.type.has_simple_content():
            yield from self.type.content.iter_elements()  # type: ignore[union-attr,misc]

    @property
    def xsd_version(self) -> str:
        return self.type.xsd_version

    @property
    def maps(self) -> 'XsdGlobals':
        return self.schema.maps

    @property
    def xpath_proxy(self) -> XMLSchemaProxy:
        return XMLSchemaProxy(self.schema, self)

    @property
    def xpath_node(self) -> LazyElementNode:
        if self._xpath_node is None:
            self._xpath_node = LazyElementNode(cast(XsdElementProtocol, self))
        return self._xpath_node

    @property
    def schema(self) -> SchemaType:
        return self.type.schema

    @property
    def target_namespace(self) -> str:
        return self.type.schema.target_namespace

    @property
    def namespaces(self) -> NsmapType:
        return self.type.schema.namespaces

    @property
    def local_name(self) -> str:
        return local_name(self.name)

    @property
    def qualified_name(self) -> str:
        return get_qname(self.target_namespace, self.name)

    @property
    def prefixed_name(self) -> str:
        return get_prefixed_qname(self.name, self.namespaces)

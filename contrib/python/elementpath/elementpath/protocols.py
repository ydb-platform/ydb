#
# Copyright (c), 2021-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Define protocols for type annotation of XPath related objects.
"""
from collections.abc import Callable, Hashable, Iterator, Iterable, \
    ItemsView, Mapping, Sequence, Sized
from typing import overload, Any, Optional, Protocol, Union, TypeVar
from xml.etree.ElementTree import Element, ElementTree

from collections.abc import MutableMapping
from elementpath.aliases import NamespacesType, NsmapType

_T = TypeVar("_T")
_AnyStr = Union[str, bytes]


class LxmlQNameProtocol(Protocol):
    localname: _AnyStr
    namespace: _AnyStr
    text: _AnyStr


LxmlKeyType = Union[str, bytes, LxmlQNameProtocol]


class LxmlAttribProtocol(Protocol):
    """A minimal protocol for attributes of lxml Element objects."""
    def get(self, *args: Any, **kwargs: Any) -> Optional[str]: ...

    def items(self) -> Sequence[tuple[Any, Any]]: ...

    def __contains__(self, key: Any) -> bool: ...

    def __getitem__(self, key: Any) -> Any: ...

    def __iter__(self) -> Iterator[Any]: ...

    def __len__(self) -> int: ...


class ElementProtocol(Sized, Hashable, Protocol):
    """A protocol for generic ElementTree elements."""

    def __iter__(self) -> Iterator['ElementProtocol']: ...

    def find(
            self, path: str, namespaces: Optional[dict[str, str]] = ...
    ) -> Optional['ElementProtocol']: ...
    def iter(self, tag: Optional[str] = ...) -> Iterator['ElementProtocol']: ...

    @overload
    def get(self, key: str) -> Optional[str]: ...

    @overload
    def get(self, key: str, default: _T) -> Union[str, _T]: ...

    def get(self, key: str, default: Optional[_T] = None) -> Union[str, _T, None]: ...

    @property
    def tag(self) -> Union[str, Callable[[], 'ElementProtocol']]: ...

    @property
    def text(self) -> Optional[str]: ...

    @property
    def tail(self) -> Optional[str]: ...

    @property
    def attrib(self) -> 'AttribType': ...


class EtreeElementProtocol(ElementProtocol, Protocol):
    """A protocol for xml.etree.ElementTree elements."""
    def __iter__(self) -> Iterator['EtreeElementProtocol']: ...

    def find(
            self, path: str, namespaces: Optional[dict[str, str]] = ...
    ) -> Optional['EtreeElementProtocol']: ...
    def iter(self, tag: Optional[str] = ...) -> Iterator['EtreeElementProtocol']: ...

    @property
    def attrib(self) -> dict[str, str]: ...


class LxmlElementProtocol(ElementProtocol, Protocol):
    """A protocol for lxml.etree elements."""
    def __iter__(self) -> Iterator['LxmlElementProtocol']: ...

    def find(
            self, path: str, namespaces: Optional[MutableMapping[str, str]] = ...
    ) -> Optional['LxmlElementProtocol']: ...
    def iter(self, tag: Optional[str] = ...) -> Iterator['LxmlElementProtocol']: ...

    def getroottree(self) -> 'LxmlDocumentProtocol': ...
    def getnext(self) -> Optional['LxmlElementProtocol']: ...
    def getparent(self) -> Optional['LxmlElementProtocol']: ...
    def getprevious(self) -> Optional['LxmlElementProtocol']: ...
    def itersiblings(self, tag: Optional[str] = ..., *tags: str,
                     preceding: bool = False) -> Iterable['LxmlElementProtocol']: ...

    @property
    def nsmap(self) -> NsmapType: ...

    @property
    def attrib(self) -> LxmlAttribProtocol: ...


class DocumentProtocol(Hashable, Protocol):
    def getroot(self) -> Optional[ElementProtocol]: ...
    def parse(self, source: Any, *args: Any, **kwargs: Any) -> ElementProtocol: ...
    def iter(self, tag: Optional[str] = ...) -> Iterator[ElementProtocol]: ...


class LxmlDocumentProtocol(Hashable, Protocol):
    def getroot(self) -> Optional[LxmlElementProtocol]: ...
    def parse(self, source: Any, *args: Any, **kwargs: Any) -> LxmlElementProtocol: ...
    def iter(self, tag: Optional[str] = ...) -> Iterator[LxmlElementProtocol]: ...


class XsdValidatorProtocol(Hashable, Protocol):
    def is_matching(self, name: Optional[str],
                    default_namespace: Optional[str] = None) -> bool: ...

    @property
    def name(self) -> Optional[str]: ...

    @property
    def xsd_version(self) -> str: ...

    @property
    def maps(self) -> 'GlobalMapsProtocol': ...


class XsdComponentProtocol(XsdValidatorProtocol, Protocol):

    @property
    def parent(self) -> Optional['XsdComponentProtocol']: ...

    @property
    def ref(self) -> Optional['XsdComponentProtocol']: ...


class XsdTypeProtocol(XsdComponentProtocol, Protocol):

    def is_simple(self) -> bool:
        """Returns `True` if it's a simpleType instance, `False` if it's a complexType."""
        ...

    def is_empty(self) -> bool:
        """
        Returns `True` if it's a simpleType instance or a complexType with empty content,
        `False` otherwise.
        """
        ...

    def has_simple_content(self) -> bool:
        """
        Returns `True` if it's a simpleType instance or a complexType with simple content,
        `False` otherwise.
        """
        ...

    def has_mixed_content(self) -> bool:
        """
        Returns `True` if it's a complexType with mixed content, `False` otherwise.
        """
        ...

    def is_element_only(self) -> bool:
        """
        Returns `True` if it's a complexType with element-only content, `False` otherwise.
        """
        ...

    def is_atomic(self) -> bool:
        """Returns `True` if the instance is an atomic simpleType, `False` otherwise."""
        ...

    def is_list(self) -> bool:
        """Returns `True` if the instance is a list simpleType, `False` otherwise."""
        ...

    def is_union(self) -> bool:
        """Returns `True` if the instance is a union simpleType, `False` otherwise."""
        ...

    def is_key(self) -> bool:
        """Returns `True` if it's a simpleType derived from xs:ID, `False` otherwise."""
        ...

    def is_qname(self) -> bool:
        """Returns `True` if it's a simpleType derived from xs:QName, `False` otherwise."""
        ...

    def is_notation(self) -> bool:
        """Returns `True` if it's a simpleType derived from xs:NOTATION, `False` otherwise."""
        ...

    @overload
    def is_valid(self, obj: Any, use_defaults: bool = True,
                 namespaces: Optional[NamespacesType] = None,
                 *args: Any, **kwargs: Any) -> bool: ...

    @overload
    def is_valid(self, obj: Any, *args: Any, **kwargs: Any) -> bool: ...

    def is_valid(self, obj: Any, *args: Any, **kwargs: Any) -> bool:
        """
        Validates an XML object node using the XSD type. The argument *obj* is an element
        for complex type nodes or a text value for simple type nodes. Returns `True` if
        the argument is valid, `False` otherwise.
        """
        ...

    @overload
    def validate(self, obj: Any, use_defaults: bool = True,
                 namespaces: Optional[NamespacesType] = None,
                 *args: Any, **kwargs: Any) -> None: ...

    @overload
    def validate(self, obj: Any, *args: Any, **kwargs: Any) -> None: ...

    def validate(self, obj: Any, *args: Any, **kwargs: Any) -> None:
        """
        Validates an XML object node using the XSD type. The argument *obj* is an element
        for complex type nodes or a text value for simple type nodes. Raises a `ValueError`
        compatible exception (a `ValueError` or a subclass of it) if the argument is not valid.
        """
        ...

    def decode(self, obj: Any, *args: Any, **kwargs: Any) -> Any:
        """
        Decodes an XML object node using the XSD type. The argument *obj* is an element
        for complex type nodes or a text value for simple type nodes. Raises a `ValueError`
        or a `TypeError` compatible exception if the argument it's not valid.
        """
        ...

    @property
    def root_type(self) -> 'XsdTypeProtocol':
        """
        The type at base of the definition of the XSD type. For a special type is the type
        itself. For an atomic type is the primitive type. For a list is the primitive type
        of the item. For a union is the base union type. For a complex type is xs:anyType.
        """
        ...

    @property
    def simple_type(self) -> Optional['XsdTypeProtocol']:
        """
        The instance if it's a simpleType instance or the simpleType instance used for
        deriving a complexType with simple content, `None` otherwise.
        """
        ...

    @property
    def model_group(self) -> Optional['XsdGroupProtocol']:
        """
        A model group if it's a complexType with mixed or element-only content, `None` otherwise.
        """
        ...


class XsdComplexTypeProtocol(XsdComponentProtocol, Protocol):

    @property
    def attributes(self) -> 'XsdAttributeGroupProtocol': ...


class XsdGroupProtocol(XsdComponentProtocol, Protocol):

    def iter_elements(self) -> Iterator['XsdElementProtocol']: ...


class XsdAttributeProtocol(XsdComponentProtocol, Protocol):

    @property
    def type(self) -> XsdTypeProtocol: ...


class XsdAnyAttributeProtocol(XsdComponentProtocol, Protocol):

    @property
    def type(self) -> None: ...


XsdAttributeProtocolType = Union[XsdAnyAttributeProtocol, XsdAttributeProtocol]


class XsdAttributeGroupProtocol(XsdComponentProtocol, Protocol):

    def get(self, key: Optional[str], /) -> Optional[XsdAttributeProtocolType]: ...

    def items(self) -> ItemsView[Optional[str], XsdAttributeProtocol]: ...

    def __contains__(self, key: Optional[str]) -> bool: ...

    def __getitem__(self, key: Optional[str]) -> XsdAttributeProtocol: ...

    def __iter__(self) -> Iterator[Optional[str]]: ...

    def __len__(self) -> int: ...

    def __hash__(self) -> int: ...


class XsdElementProtocol(XsdComponentProtocol, ElementProtocol, Protocol):

    @property
    def name(self) -> Optional[str]: ...

    @property
    def type(self) -> Optional[XsdTypeProtocol]: ...

    @property
    def attrib(self) -> XsdAttributeGroupProtocol: ...

    def __iter__(self) -> Iterator['XsdElementProtocol']: ...

    def find(self, path: str, namespaces: Optional[NamespacesType] = ...) \
        -> Union['XsdElementProtocol', None]: ...


class GlobalMapsProtocol(Protocol):

    @property
    def types(self) -> Mapping[str, XsdTypeProtocol]: ...

    @property
    def attributes(self) -> Mapping[str, XsdAttributeProtocol]: ...

    @property
    def elements(self) -> Mapping[str, XsdElementProtocol]: ...

    @property
    def substitution_groups(self) -> Mapping[str, set[Any]]: ...


class XsdSchemaProtocol(XsdValidatorProtocol, Protocol):

    @property
    def validity(self) -> str: ...

    @property
    def validation_attempted(self) -> str: ...

    @property
    def tag(self) -> str: ...

    @property
    def attrib(self) -> MutableMapping[Optional[str], 'XsdAttributeProtocol']: ...

    def __iter__(self) -> Iterator['XsdXPathNodeType']: ...

    def find(self, path: str, namespaces: Optional[NamespacesType] = ...) \
        -> Union['XsdSchemaProtocol', 'XsdElementProtocol', None]: ...


XsdXPathNodeType = Union[XsdSchemaProtocol, XsdElementProtocol]
DocumentType = Union[ElementTree, DocumentProtocol]
ElementType = Union[Element, ElementProtocol]
SchemaElemType = Union[XsdSchemaProtocol, XsdElementProtocol]
CommentType = Union[Element, ElementProtocol]
ProcessingInstructionType = Union[Element, ElementProtocol]
AttribType = Union[
    MutableMapping[str, Any],
    MutableMapping[Optional[str], Any],
    LxmlAttribProtocol,
    XsdAttributeGroupProtocol
]

__all__ = ['ElementProtocol', 'EtreeElementProtocol', 'LxmlAttribProtocol',
           'LxmlElementProtocol', 'DocumentProtocol', 'LxmlDocumentProtocol',
           'XsdValidatorProtocol', 'XsdComponentProtocol', 'XsdTypeProtocol',
           'XsdAttributeProtocol', 'XsdAttributeGroupProtocol', 'XsdElementProtocol',
           'GlobalMapsProtocol', 'XsdSchemaProtocol', 'DocumentType', 'ElementType',
           'SchemaElemType', 'CommentType', 'ProcessingInstructionType',
           'AttribType', 'XsdXPathNodeType']

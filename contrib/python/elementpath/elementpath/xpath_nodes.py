#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import importlib
from collections import defaultdict, deque
from collections.abc import Iterator
from urllib.parse import urljoin
from typing import cast, Any, Optional
from xml.etree import ElementTree

import elementpath.aliases as ta

from elementpath.exceptions import ElementPathRuntimeError, \
    ElementPathValueError, ElementPathKeyError
from elementpath.datatypes import UntypedAtomic, AnyURI, QName
from elementpath.namespaces import XML_NAMESPACE, XML_BASE, XSI_NIL, \
    XSD_ANY_TYPE, XSD_ANY_SIMPLE_TYPE, XSD_ANY_ATOMIC_TYPE, XSI_TYPE, \
    XML_ID, XSD_IDREF, XSD_IDREFS, XSD_UNTYPED, XSD_UNTYPED_ATOMIC, \
    XPATH_FUNCTIONS_NAMESPACE, get_expanded_name
from elementpath.protocols import ElementProtocol, XsdElementProtocol, \
    XsdAttributeProtocol, XsdTypeProtocol, DocumentType, ElementType, \
    SchemaElemType, CommentType, ProcessingInstructionType
from elementpath.helpers import match_wildcard, is_absolute_uri
from elementpath.decoder import get_atomic_sequence
from elementpath.etree import etree_iter_strings, is_etree_element_instance

__all__ = ['XPathNodeTree', 'XPathNode', 'NamespaceNode', 'AttributeNode', 'TextAttributeNode',
           'SchemaAttributeNode', 'TextNode', 'CommentNode', 'ProcessingInstructionNode',
           'ElementNode', 'EtreeElementNode', 'LazyElementNode', 'SchemaElementNode',
           'DocumentNode', 'EtreeDocumentNode']

_EMPTY_NAME_PATH = f'*[Q{{{XPATH_FUNCTIONS_NAMESPACE}}}local-name()=""]'

_XSD_SPECIAL_TYPES = frozenset((XSD_ANY_TYPE, XSD_ANY_SIMPLE_TYPE, XSD_ANY_ATOMIC_TYPE))


class XPathNodeTree:
    """
    Status of the node tree structure, shared between nodes.
    """
    root: ta.ParentNodeType
    elements: ta.ElementMapType
    namespaces: ta.NamespacesType
    schema: Optional[ta.SchemaProxyType]
    uri: str | None

    __slots__ = ('root_node', 'uri', 'namespaces', 'ns_offset', 'elements', 'schema', 'position')

    def __init__(self, root: ta.ParentNodeType,
                 uri: str | None = None,
                 namespaces: Optional[ta.NamespacesType] = None,
                 elements: Optional[ta.ElementMapType] = None,
                 schema: Optional[ta.SchemaProxyType] = None) -> None:

        self.root_node = root
        self.namespaces = {} if namespaces is None else namespaces
        self.uri = uri
        self.elements = {} if elements is None else elements
        self.schema = schema


###
# XQuery and XPath Data Model: https://www.w3.org/TR/xpath-datamodel/
#
# Note: in this implementation empty sequence return value is replaced by None.
#
# XPath has seven kinds of nodes:
#
#  element, attribute, text, namespace, processing-instruction, comment, document
###
class XPathNode:
    """
    The base class of all XPath nodes. In the base class and in other intermediate
    derivation string and typed values are not implemented. Use these classes only
    for type checking and for wrapping other types in a custom XPath node types.
    """
    __slots__ = ('name', 'value', 'parent', 'position')

    ###
    # XDM accessors

    @property
    def attributes(self) -> list['AttributeNode'] | None:
        return None

    @property
    def base_uri(self) -> str | None:
        return self.parent.base_uri if self.parent is not None else None

    children: list[ta.ChildNodeType] | None

    @property
    def document_uri(self) -> str | None:
        return None

    @property
    def is_id(self) -> bool | None:
        return None

    @property
    def is_idrefs(self) -> bool | None:
        return None

    @property
    def namespace_nodes(self) -> list['NamespaceNode'] | None:
        return None

    @property
    def nilled(self) -> bool | None:
        return None

    @property
    def node_kind(self) -> str:
        raise NotImplementedError()

    @property
    def node_name(self) -> QName | None:
        name: str | None = getattr(self, 'name', None)
        if name is None:
            return None
        elif not name.startswith('{'):
            return QName(None, name)

        try:
            namespace, local = name[1:].split('}')
        except ValueError:
            raise ElementPathValueError(f'invalid name format for {self!r}')
        else:
            if namespace == XML_NAMESPACE:
                return QName(namespace, f'xml:{local}')

        if isinstance(self, ElementNode):
            nsmap = self.nsmap
        elif isinstance(self.parent, ElementNode):
            nsmap = self.parent.nsmap
        else:
            nsmap = {}

        for prefix, ns in nsmap.items():
            if namespace == ns:
                if not prefix:
                    return QName(namespace, local)
                return QName(namespace, f"{prefix}:{local}")
        raise ElementPathKeyError(f'missing namespace prefix mapping in {self!r}')

    parent: ta.ParentNodeType | None

    @property
    def type_name(self) -> str | None:
        return None

    @property
    def string_value(self) -> str:
        raise NotImplementedError()

    @property
    def typed_value(self) -> list[ta.AtomicType] | ta.AtomicType:
        raise NotImplementedError()

    @staticmethod
    def unparsed_entity_public_id(name: str) -> str | None:
        return None

    @staticmethod
    def unparsed_entity_system_id(name: str) -> AnyURI | None:
        return None

    ###
    # Other properties and methods

    name: str | None  # node name
    value: object        # the object wrapped in the node
    position: int        # position of the node, for document total order

    @property
    def obj(self) -> Any:
        """Access to wrapped object using the old API."""
        return self.value

    @property
    def compat_string_value(self) -> str:
        return self.string_value

    @property
    def root_node(self) -> 'XPathNode':
        return self if self.parent is None else self.parent.tree.root_node

    @property
    def path(self) -> str:
        """Returns the node path in XPath 3.0+ format."""
        return ''

    @property
    def extended_path(self) -> str:
        """Returns the node path in extended format."""
        return self.path.replace('Q{}', '').replace('Q{', '{')

    @property
    def qname_path(self) -> str:
        """Returns the node path with names in prefixed QName format."""
        path = self.path

        if isinstance(self, ElementNode):
            for prefix, namespace in self.nsmap.items():
                path = path.replace(f'Q{{{namespace}}}', f'{prefix}:')
        elif isinstance(self.parent, ElementNode):
            for prefix, namespace in self.parent.nsmap.items():
                path = path.replace(f'Q{{{namespace}}}', f'{prefix}:')

        path = path.replace('Q{}', '')
        if 'Q{' not in path:
            return path
        raise ElementPathKeyError(f'missing namespace prefix mapping in {path}')

    @property
    def iter_typed_values(self) -> Iterator[ta.AtomicType]:
        raise NotImplementedError()

    @property
    def is_schema_node(self) -> bool | None:
        return None

    @property
    def is_typed(self) -> bool | None:
        return None

    @property
    def is_extended(self) -> bool | None:
        return None

    @property
    def is_list(self) -> bool | None:
        return None

    def apply_schema(self, schema: ta.SchemaProxyType) -> None:
        """Set XSD types for elements and attribute nodes from schema proxy instance."""
        if self.parent is not None:
            self.parent.apply_schema(schema)

    def clear_types(self) -> None:
        """Clear XSD types for elements and attribute nodes."""
        if self.parent is not None:
            self.parent.clear_types()

    def match_name(self, name: str, default_namespace: str | None = None) -> bool:
        """
        Returns `True` if the argument is matching the name of the node, `False` otherwise.
        Raises a ValueError if the argument is used, but it's in a wrong format.

        :param name: a fully qualified name, a local name or a wildcard. The accepted \
        wildcard formats are '*', '*:*', '*:local-name' and '{namespace}*'.
        :param default_namespace: the default namespace for matching element names. \
        The default is no-namespace.
        """
        return False

    def get_child_position(self, child: ta.ChildNodeType) -> int:
        pos = 0
        if self.children:
            for c in self.children:
                if isinstance(child, ElementNode):
                    if c.name == child.name:
                        pos += 1
                elif isinstance(c, child.__class__):
                    pos += 1
                if c is child:
                    break
        return pos


###
# NAMESPACE NODES

class NamespaceNode(XPathNode):
    """
    A class for processing XPath namespace nodes.

    :param prefix: the namespace prefix.
    :param uri: the namespace URI.
    :param parent: the parent element node.
    :param position: the position of the node in the document.
    """
    value: str
    parent: 'ElementNode | None'

    __slots__ = ()

    def __init__(self,
                 prefix: str | None, uri: str,
                 parent: 'ElementNode | None' = None,
                 position: int = 1) -> None:
        self.name = prefix
        self.value = uri
        self.parent = parent
        self.position = position

    def __repr__(self) -> str:
        return '%s(prefix=%r, uri=%r)' % (self.__class__.__name__, self.name, self.value)

    @property
    def prefix(self) -> str | None:
        return self.name

    @property
    def uri(self) -> str:
        return self.value

    @property
    def name_path(self) -> str:
        return self.prefix or _EMPTY_NAME_PATH

    @property
    def path(self) -> str:
        if self.parent is None:
            return '/namespace::{name_path}'
        elif isinstance(self.parent, ElementNode):
            return f"{self.parent.path}/namespace::{self.name_path}"
        return f"/namespace::{self.name_path}"

    @property
    def node_kind(self) -> str:
        return 'namespace'

    @property
    def node_name(self) -> QName | None:
        return None if not self.name else QName(None, self.name)

    @property
    def string_value(self) -> str:
        return self.value

    @property
    def iter_typed_values(self) -> Iterator[str]:
        yield self.value


###
# ATTRIBUTE NODES

class AttributeNode(XPathNode):
    """
    Base class for XPath attribute nodes, used only for type checking.
    """
    name: str | None
    parent: Optional['ElementNode']
    xsd_type: XsdTypeProtocol | None

    __slots__ = ('xsd_type',)

    def __new__(cls, *args: Any, **kwargs: Any) -> 'AttributeNode':
        if cls is AttributeNode:
            return object.__new__(TextAttributeNode)
        return object.__new__(cls)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name={self.name!r}, value={self.value!r})'

    @property
    def uri_qualified_name(self) -> str | None:
        """The URI qualified name of the attribute."""
        if not self.name:
            return self.name
        elif self.name[0] == '{':
            return f'Q{self.name}'
        else:
            return self.name

    @property
    def name_path(self) -> str:
        return self.uri_qualified_name or _EMPTY_NAME_PATH

    @property
    def path(self) -> str:
        if self.parent is None:
            return f'/@{self.name_path}'
        elif isinstance(self.parent, ElementNode):
            return f"{self.parent.path}/@{self.name_path}"
        return f"/@{self.name_path}"

    @property
    def is_typed(self) -> bool:
        return self.xsd_type is not None

    @property
    def is_list(self) -> bool:
        return self.xsd_type is not None and self.xsd_type.is_list()

    def match_name(self, name: str, default_namespace: str | None = None) -> bool:
        return self.name == name or '*' in name and match_wildcard(self.name, name)

    @property
    def base_uri(self) -> str | None:
        return self.parent.base_uri if self.parent is not None else None

    @property
    def is_id(self) -> bool:
        return self.name == XML_ID or self.xsd_type is not None and self.xsd_type.is_key()

    @property
    def is_idrefs(self) -> bool:
        if self.xsd_type is None:
            return False
        root_type = self.xsd_type.root_type
        return root_type.name == XSD_IDREF or root_type.name == XSD_IDREFS

    @property
    def node_kind(self) -> str:
        return 'attribute'

    @property
    def string_value(self) -> str:
        raise NotImplementedError()

    @property
    def type_name(self) -> str | None:
        return XSD_UNTYPED_ATOMIC if self.xsd_type is None else self.xsd_type.name

    @property
    def typed_value(self) -> list[ta.AtomicType] | ta.AtomicType:
        value = [x for x in self.iter_typed_values]
        return value[0] if len(value) == 1 else value

    @property
    def iter_typed_values(self) -> Iterator[ta.AtomicType]:
        raise NotImplementedError()


class TextAttributeNode(AttributeNode):
    """
    Class for processing XPath attribute nodes.

    :param name: the attribute name.
    :param value: the string value of the attribute.
    :param parent: the parent element node.
    :param position: the position of the node in the document.
    """
    name: str
    value: str
    parent: 'EtreeElementNode | None'

    __slots__ = ()

    def __init__(self,
                 name: str,
                 value: str,
                 parent: 'EtreeElementNode | None' = None,
                 position: int = 1) -> None:

        self.name = name
        self.value = value
        self.parent = parent
        self.position = position
        self.xsd_type = None

    def __repr__(self) -> str:
        return '%s(name=%r, value=%r)' % (self.__class__.__name__, self.name, self.value)

    @property
    def string_value(self) -> str:
        return self.value

    @property
    def iter_typed_values(self) -> Iterator[ta.AtomicType]:
        if self.parent is not None:
            yield from get_atomic_sequence(self.xsd_type, self.value, self.parent.nsmap)
        else:
            yield from get_atomic_sequence(self.xsd_type, self.value)

    def apply_schema(self, schema: ta.SchemaProxyType) -> None:
        if self.parent is not None:
            self.parent.apply_schema(schema)
        elif (xsd_attribute := schema.get_attribute(self.name)) is not None:
            self.xsd_type = xsd_attribute.type
        else:
            self.xsd_type = None


class SchemaAttributeNode(AttributeNode):
    """A class for processing XML Schema attribute nodes."""
    name: str | None
    value: XsdAttributeProtocol
    parent: 'ElementNode | None'

    __slots__ = ()

    def __init__(self,
                 attr: XsdAttributeProtocol,
                 parent: 'ElementNode | None' = None,
                 position: int = 1) -> None:

        self.name = attr.name
        self.value = attr
        self.parent = parent
        self.position = position
        self.xsd_type = attr.type

    def __repr__(self) -> str:
        return '%s(attr=%r)' % (self.__class__.__name__, self.value)

    @property
    def string_value(self) -> str:
        return str(get_atomic_sequence(self.xsd_type))

    @property
    def iter_typed_values(self) -> Iterator[ta.AtomicType]:
        yield from get_atomic_sequence(self.xsd_type)

    def match_name(self, name: str, default_namespace: str | None = None) -> bool:
        if not self.name:
            return self.value.is_matching(name, default_namespace)
        elif '*' in name:
            return match_wildcard(self.name, name)
        else:
            return self.name == name

    @property
    def is_schema_node(self) -> bool:
        return True


###
# TEXT NODES

class TextNode(XPathNode):
    """
    A class for processing XPath text nodes. An Element's property
    (elem.text or elem.tail) with a `None` value is not a text node.

    :param content: a string value.
    :param parent: the parent element node.
    :param position: the position of the node in the document.
    """
    name: None
    value: str
    parent: 'ElementNode | None'
    children: None = None

    __slots__ = ()

    def __init__(self,
                 content: str,
                 parent: 'ElementNode | None' = None,
                 position: int = 1) -> None:
        self.name = None
        self.value = content
        self.parent = parent
        self.position = position

        if parent is not None:
            parent.children.append(self)

    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, self.value)

    @property
    def content(self) -> str:
        return self.value

    @property
    def path(self) -> str:
        if self.parent is None:
            return '/text()[1]'

        pos = self.parent.get_child_position(self)
        if isinstance(self.parent, ElementNode):
            return f"{self.parent.path}/text()[{pos}]"
        return f"/text()[{pos}]"

    ###
    # Text node accessors

    @property
    def base_uri(self) -> str | None:
        return self.parent.base_uri if self.parent is not None else None

    @property
    def node_kind(self) -> str:
        return 'text'

    @property
    def string_value(self) -> str:
        return self.value

    @property
    def type_name(self) -> str | None:
        return XSD_UNTYPED_ATOMIC

    @property
    def typed_value(self) -> UntypedAtomic:
        return UntypedAtomic(self.value)

    @property
    def iter_typed_values(self) -> Iterator[UntypedAtomic]:
        yield UntypedAtomic(self.value)


###
# COMMENT NODES

class CommentNode(XPathNode):
    """
    A class for processing XPath comment nodes.

    :param content: the wrapped Comment Element or a string.
    :param parent: the parent node.
    :param position: the position of the node in the document.
    """
    name: None
    value: CommentType

    __slots__ = ()

    def __init__(self,
                 content: CommentType | str,
                 parent: ta.ParentNodeType | None = None,
                 position: int = 1) -> None:

        self.name = None
        if isinstance(content, str):
            self.value = ElementTree.Comment(content)
        else:
            self.value = content

        self.parent = parent
        self.position = position

        if parent is not None:
            parent.children.append(self)
            parent.tree.elements[self.value] = self

    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, self.value.text or '')

    @property
    def content(self) -> CommentType:
        return self.value

    elem = content

    @property
    def path(self) -> str:
        if self.parent is None:
            return '/comment()[1]'

        pos = self.parent.get_child_position(self)
        if isinstance(self.parent, ElementNode):
            return f"{self.parent.path}/comment()[{pos}]"
        return f"/comment()[{pos}]"

    @property
    def base_uri(self) -> str | None:
        return self.parent.base_uri if self.parent is not None else None

    @property
    def node_kind(self) -> str:
        return 'comment'

    @property
    def string_value(self) -> str:
        return self.value.text or ''

    @property
    def typed_value(self) -> str:
        return self.string_value

    @property
    def iter_typed_values(self) -> Iterator[str]:
        yield self.string_value


###
# PROCESSING INSTRUCTION NODES

class ProcessingInstructionNode(XPathNode):
    """
    A class for XPath processing instructions nodes.

    :param target: the wrapped Processing Instruction object or a string.
    :param content: an optional string, used if *target* is a string.
    :param parent: the parent element node.
    :param position: the position of the node in the document.
    """
    name: str
    value: ProcessingInstructionType

    __slots__ = ()

    def __init__(self,
                 target: str | ProcessingInstructionType,
                 content: str | None = None,
                 parent: ta.ParentNodeType | None = None,
                 position: int = 1) -> None:

        if isinstance(target, str):
            self.name = target
            self.value = ElementTree.ProcessingInstruction(self.name, content)
        else:
            if hasattr(target, 'target'):
                self.name = cast(str, target.target)  # lxml PI
            else:
                self.name = (target.text or '').partition(' ')[0]
            self.value = target

        self.parent = parent
        self.position = position

        if parent is not None:
            parent.children.append(self)
            parent.tree.elements[self.value] = self

    def __repr__(self) -> str:
        return '%s(target=%r, content=%r)' % (self.__class__.__name__, self.name, self.content)

    @property
    def target(self) -> str:
        return self.name

    @property
    def content(self) -> str:
        if hasattr(self.value, 'target'):
            return self.value.text or ''
        else:
            return (self.value.text or '').partition(' ')[-1]

    @property
    def elem(self) -> ProcessingInstructionType:
        return self.value

    @property
    def path(self) -> str:
        if self.parent is None:
            return '/processing-instruction({self.name})[1]'

        pos = self.parent.get_child_position(self)
        if isinstance(self.parent, ElementNode):
            return f"{self.parent.path}/processing-instruction({self.name})[{pos}]"
        return f"/processing-instruction({self.name})[{pos}]"

    @property
    def base_uri(self) -> str | None:
        return self.parent.base_uri if self.parent is not None else None

    @property
    def node_kind(self) -> str:
        return 'processing-instruction'

    @property
    def node_name(self) -> QName:
        return QName(None, self.name)

    @property
    def string_value(self) -> str:
        return self.content

    @property
    def typed_value(self) -> str:
        return self.content

    @property
    def iter_typed_values(self) -> Iterator[str]:
        yield self.content

    text = string_value


###
# ELEMENT NODES

class ElementNode(XPathNode):
    """
    Base class for XPath element nodes, used only for type checking. Element nodes
    use lazy properties to diminish the average load for a tree processing.
    """
    name: str | None
    value: object
    tree: XPathNodeTree
    children: list[ta.ChildNodeType]
    parent: ta.ParentNodeType | None
    xsd_type: XsdTypeProtocol | None
    xsd_element: XsdElementProtocol | None
    _nsmap: ta.NsmapType | ta.NamespacesType | None

    # Lazy protected attributes
    _namespace_nodes: list[NamespaceNode]
    _attributes: list[AttributeNode]

    __slots__ = ('children', 'tree', 'xsd_type', 'xsd_element',
                 '_nsmap', '_namespace_nodes', '_attributes')

    def __new__(cls, *args: Any, **kwargs: Any) -> 'ElementNode':
        if cls is ElementNode:
            return object.__new__(EtreeElementNode)
        return object.__new__(cls)

    def __repr__(self) -> str:
        return '%s(elem=%r)' % (self.__class__.__name__, self.value)

    def __getitem__(self, i: int | slice) -> ta.ChildNodeType | list[ta.ChildNodeType]:
        return self.children[i]

    def __len__(self) -> int:
        return len(self.children)

    def __iter__(self) -> Iterator[ta.ChildNodeType]:
        yield from self.children

    @property
    def nsmap(self) -> ta.NsmapType | ta.NamespacesType:
        if hasattr(self.value, 'nsmap'):
            return cast(ta.NsmapType, self.value.nsmap)
        elif self._nsmap is not None:
            return self._nsmap
        else:
            return self.tree.namespaces

    @nsmap.setter
    def nsmap(self, nsmap: ta.NsmapType | ta.NamespacesType) -> None:
        self._nsmap = nsmap

    @property
    def uri_qualified_name(self) -> str | None:
        """The URI qualified name of the element."""
        if not self.name:
            return self.name
        elif self.name[0] == '{':
            return f'Q{self.name}'
        else:
            return f'Q{{}}{self.name}'

    @property
    def attributes(self) -> list[AttributeNode]:
        return []

    @property
    def base_uri(self) -> str | None:
        base_uri: str | None = self._uri.strip() if hasattr(self, '_uri') else None
        if self.parent is None:
            return base_uri
        elif base_uri is None:
            return self.parent.base_uri
        else:
            return urljoin(self.parent.base_uri or '', base_uri)

    @property
    def is_id(self) -> bool:
        return self.name == XML_ID or self.xsd_type is not None and self.xsd_type.is_key()

    @property
    def is_idrefs(self) -> bool:
        if self.xsd_type is None:
            return False
        root_type = self.xsd_type.root_type
        return root_type.name == XSD_IDREF or root_type.name == XSD_IDREFS

    @property
    def namespace_nodes(self) -> list[NamespaceNode]:
        if not hasattr(self, '_namespace_nodes'):
            # Lazy generation of namespace nodes of the element
            position = self.position + 1
            self._namespace_nodes = [NamespaceNode('xml', XML_NAMESPACE, self, position)]
            position += 1
            if self.nsmap:
                for pfx, uri in self.nsmap.items():
                    if pfx != 'xml':
                        self._namespace_nodes.append(NamespaceNode(pfx, uri, self, position))
                        position += 1

        return self._namespace_nodes

    @property
    def nilled(self) -> bool:
        return False

    @property
    def node_kind(self) -> str:
        return 'element'

    @property
    def string_value(self) -> str:
        raise NotImplementedError()

    @property
    def type_name(self) -> str | None:
        return XSD_UNTYPED if self.xsd_type is None else self.xsd_type.name

    @property
    def typed_value(self) -> list[ta.AtomicType] | ta.AtomicType:
        value = [x for x in self.iter_typed_values]
        return value[0] if len(value) == 1 else value

    @property
    def iter_typed_values(self) -> Iterator[ta.AtomicType]:
        raise NotImplementedError()

    @property
    def is_list(self) -> bool:
        return self.xsd_type is not None and self.xsd_type.is_list()

    @property
    def uri(self) -> str | None:
        return self.tree.uri

    @uri.setter
    def uri(self, uri: str) -> None:
        self.tree.uri = uri

    @property
    def schema(self) -> Optional[ta.SchemaProxyType]:
        return self.tree.schema

    @schema.setter
    def schema(self, schema: ta.SchemaProxyType) -> None:
        self.tree.schema = schema

    @property
    def elements(self) -> Optional[ta.ElementMapType]:
        return self.tree.elements

    @elements.setter
    def elements(self, elements: ta.ElementMapType) -> None:
        self.tree.elements = elements

    @property
    def name_path(self) -> str:
        return self.uri_qualified_name or _EMPTY_NAME_PATH

    @property
    def path(self) -> str:
        if self.parent is None:
            return f'/{self.name_path}[1]'

        pos = self.parent.get_child_position(self)
        if isinstance(self.parent, ElementNode):
            return f"{self.parent.path}/{self.name_path}[{pos}]"
        return f"/{self.name_path}[{pos}]"

    @property
    def default_namespace(self) -> str | None:
        if None in self.nsmap:
            return self.nsmap[None]  # type: ignore
        else:
            return self.nsmap.get('')

    @property
    def is_typed(self) -> bool:
        return self.xsd_type is not None

    def apply_schema(self, schema: ta.SchemaProxyType) -> None:
        return

    def clear_types(self) -> None:
        return

    def match_name(self, name: str, default_namespace: str | None = None) -> bool:
        if self.name is None:
            return False
        elif '*' in name:
            return match_wildcard(self.name, name)
        elif not name:
            return not self.name
        elif name[0] == '{' or not default_namespace:
            return self.name == name
        else:
            return self.name == f'{{{default_namespace}}}{name}'

    def get_element_node(self, elem: ElementType | SchemaElemType) -> ta.TaggedNodeType | None:
        return self.tree.elements.get(elem)

    def get_document_node(self, replace: bool = False, as_parent: bool = True) -> 'DocumentNode':
        """
        Returns a `DocumentNode` for the element node. If the element belongs to a tree that
        already has a document root, returns the document, otherwise creates a dummy document.

        :param replace: if `True` the root element of the tree is replaced by the \
        document node. This is usually useful for extended data models (more element \
        children, text nodes). Default is `False`.
        :param as_parent: if `True` the root node/s of parent attribute is set with \
        the dummy document node, otherwise is set to `None`.
        """
        raise NotImplementedError()

    def iter(self) -> Iterator[XPathNode]:
        """Iterates the tree building lazy components."""
        yield self
        yield from self.namespace_nodes
        yield from self.attributes

        for child in self:
            if isinstance(child, ElementNode):
                yield from child.iter()
            else:
                yield child

    iter_document = iter  # For backward compatibility

    def iter_lazy(self) -> Iterator[XPathNode]:
        """Iterates the tree not including the not built lazy components."""
        yield self

        iterators: deque[Any] = deque()  # slightly faster than list()
        children: Iterator[Any] = iter(self.children)

        if hasattr(self, '_namespace_nodes'):
            yield from self._namespace_nodes
        if hasattr(self, '_attributes'):
            yield from self._attributes

        while True:
            for child in children:
                yield child

                if isinstance(child, ElementNode):
                    if hasattr(child, '_namespace_nodes'):
                        yield from child._namespace_nodes
                    if hasattr(child, '_attributes'):
                        yield from child._attributes

                    if child.children:
                        iterators.append(children)
                        children = iter(child.children)
                        break
            else:
                try:
                    children = iterators.pop()
                except IndexError:
                    return

    def iter_descendants(self, with_self: bool = True) -> Iterator[ta.ChildNodeType]:
        if with_self:
            yield self

        iterators: deque[Any] = deque()
        children: Iterator[Any] = iter(self.children)

        while True:
            for child in children:
                yield child

                if isinstance(child, ElementNode) and child.children:
                    iterators.append(children)
                    children = iter(child.children)
                    break
            else:
                try:
                    children = iterators.pop()
                except IndexError:
                    return


class EtreeElementNode(ElementNode):
    """
    XPath element nodes for wrapping ElementTree elements.

    :param elem: the wrapped Element.
    :param parent: the parent document node or element node.
    :param position: the position of the node in the document.
    """
    name: str
    value: ElementType

    __slots__ = ('xsd_type', 'xsd_element')

    def __init__(self,
                 elem: ElementType,
                 parent: ta.ParentNodeType | None = None,
                 position: int = 1,
                 nsmap: ta.NsmapType | ta.NamespacesType | None = None) -> None:

        assert not callable(elem.tag)
        self.name = elem.tag
        self.value = elem
        self.parent = parent
        self.position = position
        self.children = []
        self.xsd_type = self.xsd_element = None
        self._nsmap = nsmap

        if parent is not None:
            self.tree = parent.tree
            parent.children.append(self)
        else:
            self.tree = XPathNodeTree(self)

        self.tree.elements[elem] = self

    @property
    def content(self) -> ElementType:
        return self.value

    elem = content

    @property
    def attributes(self) -> list[AttributeNode]:
        if not hasattr(self, '_attributes'):
            nsmap = self.nsmap
            position = self.position + len(nsmap) + int('xml' not in nsmap) + 1
            self._attributes = [
                TextAttributeNode(name, value, self, pos)
                for pos, (name, value) in enumerate(self.value.attrib.items(), position)
            ]

            if self.xsd_type is None or (schema := self.tree.schema) is None:
                return self._attributes
            elif not schema.is_fully_valid():
                any_simple_type = schema.get_type(XSD_ANY_SIMPLE_TYPE)
                for attr in self._attributes:
                    attr.xsd_type = any_simple_type
                return self._attributes
            elif self.xsd_element is None or XSI_TYPE in self.value.attrib:
                xsd_type = self.xsd_type
            else:
                xsd_type = cast(XsdTypeProtocol, self.xsd_element.type)

            if hasattr(xsd_type, 'attributes'):
                attributes = xsd_type.attributes
                for attr in self._attributes:
                    assert attr.name is not None
                    if attr.name.startswith('{http://www.w3.org/2001/XMLSchema-instance}'):
                        attr.xsd_type = schema.get_type(XSD_ANY_ATOMIC_TYPE)
                    if attr.name in attributes:
                        attr.xsd_type = attributes[attr.name].type
                    elif None in attributes and attributes[None].is_matching(attr.name):
                        xsd_attribute = schema.get_attribute(attr.name)
                        if xsd_attribute is not None:
                            attr.xsd_type = xsd_attribute.type

                # Add missing attributes with a default value, at the same
                # position of the last attribute.
                position = position + len(self.value.attrib)
                for name, xsd_attribute in attributes.items():
                    if name is not None and name not in self.value.attrib:
                        value = getattr(xsd_attribute, 'value_constraint', None)
                        if value is not None:
                            attr = TextAttributeNode(name, value, self, position)
                            attr.xsd_type = xsd_attribute.type
                            self._attributes.append(attr)

        return self._attributes

    @property
    def base_uri(self) -> str | None:
        base_uri = self.value.get(XML_BASE)
        if isinstance(base_uri, str):
            base_uri = base_uri.strip()
        elif base_uri is not None:
            base_uri = ''
        elif hasattr(self, '_uri'):
            base_uri = self._uri.strip()

        if self.parent is None:
            return base_uri
        elif base_uri is None:
            return self.parent.base_uri
        else:
            return urljoin(self.parent.base_uri or '', base_uri)

    @property
    def nilled(self) -> bool:
        return self.value.get(XSI_NIL) in ('true', '1')

    @property
    def string_value(self) -> str:
        if self.xsd_type is not None and self.xsd_type.is_element_only():
            # Element-only text content is normalized
            return ''.join(etree_iter_strings(self.value, normalize=True))

        return ''.join(etree_iter_strings(self.value)) or \
            getattr(self.xsd_element, 'value_constraint', None) or ''

    @property
    def iter_typed_values(self) -> Iterator[ta.AtomicType]:
        if self.xsd_type is None or \
                self.xsd_type.name in _XSD_SPECIAL_TYPES or \
                self.xsd_type.has_mixed_content():
            yield UntypedAtomic(''.join(etree_iter_strings(self.value)))
        elif self.xsd_type.is_element_only():
            return
        elif self.value.get(XSI_NIL) and getattr(self.xsd_type.parent, 'nillable', None):
            return
        elif self.value.text is not None:
            yield from get_atomic_sequence(self.xsd_type, self.value.text, self.nsmap)
        elif self.value.get(XSI_NIL) in ('1', 'true'):
            yield ''
        else:
            value = getattr(self.xsd_element, 'value_constraint', None)
            yield from get_atomic_sequence(self.xsd_type, value or '')

    @property
    def compat_string_value(self) -> str:
        return ''.join(etree_iter_strings(self.value))

    def apply_schema(self, schema: ta.SchemaProxyType) -> None:
        if self.tree.schema is schema and not schema.is_assertion_based():
            return
        self.tree.schema = schema

        if not schema.is_fully_valid():
            element_type = schema.get_type(XSD_ANY_TYPE)
            for node in self.iter_descendants(with_self=True):
                if isinstance(node, EtreeElementNode):
                    node.xsd_type = element_type
                    if hasattr(node, '_attributes'):
                        delattr(node, '_attributes')
            return

        if schema.base_element is not None:
            self.xsd_element = schema.base_element
            xsd_types: list[XsdTypeProtocol | None] = [schema.base_element.type]
            children: Iterator[Any] = iter(self)
            if schema.is_assertion_based():
                self.xsd_type = schema.get_type(XSD_ANY_TYPE)
            else:
                self.xsd_type = schema.base_element.type

            if hasattr(self, '_attributes'):
                delattr(self, '_attributes')
        else:
            root_node: ta.ParentNodeType = self
            while isinstance(root_node.parent, EtreeElementNode):
                root_node = root_node.parent

            xsd_types = [None]
            children = iter((root_node,))

            if hasattr(root_node, '_attributes'):
                delattr(root_node, '_attributes')

        iterators: deque[Any] = deque()
        element_match_cache: defaultdict[
            int, dict[str | None, XsdElementProtocol | None]
        ] = defaultdict(dict)
        while True:
            for node in children:
                if not isinstance(node, EtreeElementNode):
                    continue
                elif XSI_TYPE in node.value.attrib:
                    xsi_type = cast(str, node.value.attrib[XSI_TYPE])
                    try:
                        type_name = get_expanded_name(xsi_type, node.nsmap)
                    except (KeyError, TypeError):
                        node.clear_types()
                        continue
                    else:
                        xsd_type = schema.get_type(type_name)
                else:
                    if xsd_types[-1] is None:
                        xsd_element = schema.get_element(node.name)
                    elif (content := xsd_types[-1].model_group) is None:
                        xsd_element = None
                    elif node.name in (sub_cache := element_match_cache[id(content)]):
                        xsd_element = sub_cache[node.name]
                    else:
                        for xsd_element in content.iter_elements():
                            if xsd_element.is_matching(node.name):
                                if xsd_element.name != node.name:
                                    # a wildcard or a substitute
                                    xsd_element = schema.get_element(node.name)
                                sub_cache[node.name] = xsd_element
                                break
                        else:
                            xsd_element = None

                    xsd_type = getattr(xsd_element, 'type', None)
                    node.xsd_element = xsd_element

                if xsd_type is None:
                    node.clear_types()
                    continue

                node.xsd_type = xsd_type
                if hasattr(node, '_attributes'):
                    delattr(node, '_attributes')

                if len(node.value):
                    xsd_types.append(xsd_type)
                    iterators.append(children)
                    children = iter(node)
                    break
            else:
                try:
                    children = iterators.pop()
                    xsd_types.pop()
                except IndexError:
                    return

    def clear_types(self) -> None:
        """Clear XSD types for element node subtree."""
        for node in self.iter_descendants(with_self=True):
            if isinstance(node, EtreeElementNode):
                node.xsd_type = None
                node.xsd_element = None
                if hasattr(node, '_attributes'):
                    delattr(node, '_attributes')

    @property
    def is_typed(self) -> bool:
        return self.xsd_type is not None

    def match_name(self, name: str, default_namespace: str | None = None) -> bool:
        if '*' in name:
            return match_wildcard(self.name, name)
        elif not name:
            return not self.name
        elif hasattr(self.value, 'type') and hasattr(self.value, 'is_matching'):
            return cast(XsdElementProtocol, self.value).is_matching(name, default_namespace)
        elif name[0] == '{' or not default_namespace:
            return self.name == name
        else:
            return self.name == f'{{{default_namespace}}}{name}'

    def get_document_node(self, replace: bool = False, as_parent: bool = True) -> 'DocumentNode':
        """
        Returns a `DocumentNode` for the element node. If the element belongs to a tree that
        already has a document root, returns the document, otherwise creates a dummy document
        if the element node wraps an Element of an ElementTree structure or return `None`.

        :param replace: if `True` the root element of the tree is replaced by the \
        document node. This is usually useful for extended data models (more element \
        children, text nodes). Default is `False`.
        :param as_parent: if `True` the root node/s of parent attribute is set with \
        the dummy document node, otherwise is set to `None`.
        """
        if isinstance(self.tree.root_node, DocumentNode):
            return self.tree.root_node

        root_node = self.tree.root_node
        assert isinstance(root_node, EtreeElementNode)

        if root_node.value.__class__.__module__ not in ('lxml.etree', 'lxml.html'):
            etree = ElementTree
        else:
            etree = importlib.import_module('lxml.etree')

        document_node = object.__new__(EtreeDocumentNode)
        document_node.parent = None
        document_node.tree = root_node.tree
        if as_parent:
            document_node.tree.root_node = document_node

        if replace:
            document = etree.ElementTree()
            if sum(isinstance(x, ElementNode) for x in root_node.children) == 1:
                for child in root_node.children:
                    if isinstance(child, ElementNode):
                        document = etree.ElementTree(cast(ElementTree.Element, child.value))
                        break

            document_node.value = document
            document_node.position = root_node.position
            document_node.children = root_node.children

            for child in document_node.children:
                child.parent = document_node if as_parent else None

            root_node.tree.elements.pop(root_node.value)
            del root_node

        else:
            document_node.value = etree.ElementTree(cast(ElementTree.Element, root_node.value))
            document_node.position = root_node.position - 1
            document_node.children = [root_node]

            if as_parent:
                root_node.parent = document_node

        return document_node


###
# Specialized element nodes

class LazyElementNode(EtreeElementNode):
    """
    A fully lazy element node, slower but better if the node has not
    to be used in a document context. The node extends descendants but
    does not record positions and a map of elements.
    """
    __slots__ = ()

    def __iter__(self) -> Iterator[ta.ChildNodeType]:
        if not self.children:
            if self.value.text is not None:
                TextNode(self.value.text, self)
            if len(self.value):
                for elem in self.value:
                    if not callable(elem.tag):
                        LazyElementNode(elem, self)
                    elif elem.tag.__name__ == 'Comment':
                        CommentNode(elem, self)
                    else:
                        ProcessingInstructionNode(elem, parent=self)

                    if elem.tail is not None:
                        TextNode(elem.tail, self)

        yield from self.children

    def iter_descendants(self, with_self: bool = True) -> Iterator[ta.ChildNodeType]:
        if with_self:
            yield self

        for child in self:
            if isinstance(child, ElementNode):
                yield from child.iter_descendants()
            else:
                yield child


class SchemaElementNode(ElementNode):
    """
    An element node class for wrapping the XSD schema and its elements.
    The resulting structure can be a tree or a set of disjoint trees.
    With more roots only one of them is the schema node.
    """
    value: SchemaElemType
    ref: 'SchemaElementNode | None'

    __slots__ = ('ref',)

    def __init__(self,
                 elem: SchemaElemType,
                 parent: ta.ParentNodeType | None = None,
                 position: int = 1,
                 nsmap: ta.NsmapType | ta.NamespacesType | None = None):

        self.name = elem.name
        self.value = elem
        self.parent = parent
        self.position = position
        self.children = []
        self.ref = None

        if hasattr(elem, 'type'):
            self.xsd_element = cast(XsdElementProtocol, elem)
            self.xsd_type = self.xsd_element.type
        else:
            self.xsd_element = self.xsd_type = None

        self._nsmap = nsmap

        try:
            self.tree = parent.tree  # type: ignore[union-attr, unused-ignore]
        except AttributeError:
            self.tree = XPathNodeTree(self)
        else:
            parent.children.append(self)  # type: ignore[union-attr, unused-ignore]

        self.tree.elements[elem] = self

    def __iter__(self) -> Iterator[ta.ChildNodeType]:
        if self.ref is None:
            yield from self.children
        else:
            yield from self.ref.children

    @property
    def content(self) -> SchemaElemType:
        return self.value

    elem = content

    @property
    def path(self) -> str:
        if not hasattr(self, 'type'):
            return '/'
        return super().path

    @property
    def is_schema_node(self) -> bool:
        return True

    def match_name(self, name: str, default_namespace: str | None = None) -> bool:
        if '*' in name:
            return match_wildcard(self.name, name)
        elif not name:
            return not self.name
        elif hasattr(self.value, 'type'):
            return self.value.is_matching(name, default_namespace)
        else:
            return self.value.tag == name  # a schema

    @property
    def attributes(self) -> list[AttributeNode]:
        if not hasattr(self, '_attributes'):
            nsmap = self.tree.namespaces
            position = self.position + len(nsmap) + int('xml' not in nsmap) + 1
            self._attributes = [
                SchemaAttributeNode(attr, self, pos)
                for pos, (_, attr) in enumerate(self.value.attrib.items(), position)
            ]
        return self._attributes

    @property
    def base_uri(self) -> str | None:
        base_uri: str | None = self._uri.strip() if hasattr(self, '_uri') else None
        if self.parent is None:
            return base_uri
        elif base_uri is None:
            return self.parent.base_uri
        else:
            return urljoin(self.parent.base_uri or '', base_uri)

    @property
    def type_name(self) -> str | None:
        if (xsd_type := getattr(self.value, 'type', None)) is not None:
            return cast(str | None, xsd_type.name)
        return None

    @property
    def string_value(self) -> str:
        if not hasattr(self.value, 'type'):
            return ''
        for item in get_atomic_sequence(self.xsd_type):
            return str(item)
        return ''

    @property
    def iter_typed_values(self) -> Iterator[ta.AtomicType]:
        yield from get_atomic_sequence(self.xsd_type)

    def iter(self) -> Iterator[XPathNode]:
        yield self

        iterators: list[Any] = []
        children: Iterator[Any] = iter(self.children)

        if hasattr(self, '_namespace_nodes'):
            yield from self._namespace_nodes
        if hasattr(self, '_attributes'):
            yield from self._attributes

        elements = {self}
        while True:
            for child in children:
                if child in elements:
                    continue
                yield child
                elements.add(child)

                if isinstance(child, ElementNode):
                    if hasattr(child, '_namespace_nodes'):
                        yield from child._namespace_nodes
                    if hasattr(child, '_attributes'):
                        yield from child._attributes

                    if child.children:
                        iterators.append(children)
                        children = iter(child.children)
                        break
            else:
                try:
                    children = iterators.pop()
                except IndexError:
                    return

    def iter_descendants(self, with_self: bool = True) -> Iterator[ta.ChildNodeType]:
        if with_self:
            yield self

        iterators: list[Any] = []
        children: Iterator[Any] = iter(self.children)

        elements = {self}
        while True:
            for child in children:
                if child.ref is not None:
                    child = child.ref

                if child in elements:
                    continue
                yield child
                elements.add(child)

                if child.children:
                    iterators.append(children)
                    children = iter(child.children)
                    break
            else:
                try:
                    children = iterators.pop()
                except IndexError:
                    return


###
# DOCUMENT NODES

class DocumentNode(XPathNode):
    """
    Base class for all XPath document nodes.
    """
    name: None
    value: object
    parent: None
    children: list[ta.ChildNodeType]
    tree: XPathNodeTree

    __slots__ = ('children', 'tree')

    def __new__(cls, *args: Any, **kwargs: Any) -> 'DocumentNode':
        if cls is DocumentNode:
            return object.__new__(EtreeDocumentNode)
        return object.__new__(cls)

    def __repr__(self) -> str:
        return '%s(document=%r)' % (self.__class__.__name__, self.value)

    def __getitem__(self, i: int | slice) -> ta.ChildNodeType | list[ta.ChildNodeType]:
        return self.children[i]

    def __len__(self) -> int:
        return len(self.children)

    def __iter__(self) -> Iterator[ta.ChildNodeType]:
        yield from self.children

    @property
    def document(self) -> object:
        return self.value

    @property
    def uri(self) -> str | None:
        return self.tree.uri

    @property
    def elements(self) -> dict[object, ta.TaggedNodeType]:
        return self.tree.elements

    @property
    def path(self) -> str:
        return '/'

    def getroot(self) -> ElementNode:
        for child in self.children:
            if isinstance(child, ElementNode):
                return child
        raise ElementPathRuntimeError("Missing document root")

    def get_element_node(self, elem: ElementProtocol) -> ta.TaggedNodeType | None:
        return self.elements.get(elem)

    def iter(self) -> Iterator[XPathNode]:
        yield self

        for e in self.children:
            if isinstance(e, ElementNode):
                yield from e.iter()
            else:
                yield e

    iter_document = iter

    def iter_lazy(self) -> Iterator[XPathNode]:
        yield self

        for e in self.children:
            if isinstance(e, ElementNode):
                yield from e.iter_lazy()
            else:
                yield e

    def iter_descendants(self, with_self: bool = True) \
            -> Iterator['DocumentNode | ta.ChildNodeType']:
        if with_self:
            yield self

        for e in self.children:
            if isinstance(e, ElementNode):
                yield from e.iter_descendants()
            else:
                yield e

    @property
    def is_typed(self) -> bool:
        for child in self.children:
            if isinstance(child, ElementNode):
                return child.is_typed
        else:
            return False

    def apply_schema(self, schema: ta.SchemaProxyType) -> None:
        for child in self.children:
            if isinstance(child, ElementNode):
                child.apply_schema(schema)

    def clear_types(self) -> None:
        for child in self.children:
            if isinstance(child, ElementNode):
                child.clear_types()

    @property
    def is_extended(self) -> bool:
        """
        Returns `True` if the document node can't be represented with an
        ElementTree structure, `False` otherwise.
        """
        if not self.children:
            raise ElementPathRuntimeError("Missing document root")
        return len(self.children) > 1 or not isinstance(self.children[0], ElementNode)

    @property
    def base_uri(self) -> str | None:
        return self.uri.strip() if self.uri is not None else None

    @property
    def document_uri(self) -> str | None:
        if self.uri is not None and is_absolute_uri(self.uri):
            return self.uri.strip()
        else:
            return None

    @property
    def node_kind(self) -> str:
        return 'document'

    @property
    def string_value(self) -> str:
        raise NotImplementedError()

    @property
    def typed_value(self) -> ta.AtomicType:
        return UntypedAtomic(self.string_value)

    @property
    def iter_typed_values(self) -> Iterator[UntypedAtomic]:
        yield UntypedAtomic(self.string_value)


class EtreeDocumentNode(DocumentNode):
    """
    A class for ElementTree document nodes.

    :param document: the wrapped ElementTree instance.
    :param uri: the document URI.
    :param position: the position of the node in the document, usually 1, \
    or 0 for lxml standalone root elements with siblings.
    """
    value: DocumentType

    __slots__ = ()

    def __init__(self, document: DocumentType,
                 uri: str | None = None,
                 position: int = 1) -> None:

        self.value = document
        self.name = None
        self.parent = None
        self.position = position
        self.children = []
        self.tree = XPathNodeTree(self, uri=uri)

    @property
    def document(self) -> DocumentType:
        return self.value

    @property
    def string_value(self) -> str:
        if not self.children:
            # Fallback for not built documents
            root = self.value.getroot()
            if root is None:
                return ''
            return ''.join(etree_iter_strings(root))
        return ''.join(child.string_value for child in self.children)

    @property
    def compat_string_value(self) -> str:
        if not self.children:
            return self.string_value
        return ''.join(child.compat_string_value for child in self.children)

    @property
    def is_extended(self) -> bool:
        """
        Returns `True` if the document node can't be represented with an
        ElementTree structure, `False` otherwise.
        """
        root = self.value.getroot()
        if root is None or not is_etree_element_instance(root):
            return True
        elif not self.children:
            raise ElementPathRuntimeError("Missing document root")
        elif len(self.children) == 1:
            return not isinstance(self.children[0], ElementNode)
        elif not hasattr(root, 'itersiblings'):
            return True  # an extended xml.etree.ElementTree structure
        elif any(isinstance(x, TextNode) for x in root):
            return True
        else:
            return sum(isinstance(x, ElementNode) for x in root) != 1

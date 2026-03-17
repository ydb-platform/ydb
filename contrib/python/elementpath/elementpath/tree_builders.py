#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections import deque
from collections.abc import Iterator
from typing import cast, Any, Optional, Union

import elementpath.aliases as ta
from elementpath.exceptions import ElementPathTypeError
from elementpath.protocols import LxmlElementProtocol, DocumentProtocol, \
    LxmlDocumentProtocol, XsdElementProtocol, DocumentType, ElementType, \
    SchemaElemType
from elementpath.etree import is_etree_document, is_etree_element, is_etree_element_instance
from elementpath.xpath_nodes import TextNode, ElementNode, SchemaElementNode, DocumentNode, \
    EtreeElementNode, EtreeDocumentNode, CommentNode, ProcessingInstructionNode

__all__ = ['get_node_tree', 'build_node_tree', 'build_lxml_node_tree', 'build_schema_node_tree']

ElementTreeRootType = Union[DocumentType, ElementType]
LxmlRootType = Union[LxmlDocumentProtocol, LxmlElementProtocol]


def is_schema(obj: Any) -> bool:
    return hasattr(obj, 'xsd_version') and hasattr(obj, 'maps') and not hasattr(obj, 'parent')


def get_node_tree(root: ta.RootArgType,
                  namespaces: Optional[ta.NamespacesType] = None,
                  uri: Optional[str] = None,
                  fragment: Optional[bool] = None) -> ta.RootNodeType:
    """
    Returns a tree of XPath nodes that wrap the provided root tree.

    :param root: an Element or an ElementTree or a schema or a schema element.
    :param namespaces: an optional mapping from prefixes to namespace URIs, \
    Ignored if root is a lxml etree or a schema structure.
    :param uri: an optional URI associated with the root element or the document.
    :param fragment: if `True` is provided the root is considered a fragment. In this \
    case if `root` is an ElementTree instance skips it and use the root Element. If \
    `False` is provided creates a dummy document when the root is an Element instance. \
    For default the root node kind is preserved.
    """
    if isinstance(root, (DocumentNode, ElementNode)):
        if uri is not None and root.uri is None:
            root.tree.uri = uri

        if fragment:
            if isinstance(root, DocumentNode):
                return root.getroot()
        elif fragment is False and \
                isinstance(root, ElementNode) and \
                is_etree_element_instance(root.value):
            return root.get_document_node()

        return root

    if not is_etree_document(root) and \
            (not is_etree_element(root) or callable(cast(ElementType, root).tag)):
        msg = "invalid root {!r}, an Element or an ElementTree or a schema node required"
        raise ElementPathTypeError(msg.format(root))
    elif hasattr(root, 'xpath'):
        # a lxml element tree data
        return build_lxml_node_tree(
            cast(LxmlRootType, root), uri, fragment
        )
    elif hasattr(root, 'xsd_version') and hasattr(root, 'maps'):
        # a schema or a schema node
        return build_schema_node_tree(
            cast(SchemaElemType, root), uri
        )
    else:
        return build_node_tree(root, namespaces, uri, fragment)


def build_node_tree(root: ElementTreeRootType,
                    namespaces: Optional[ta.NamespacesType] = None,
                    uri: Optional[str] = None,
                    fragment: Optional[bool] = None) -> ta.RootNodeType:
    """
    Returns a tree of XPath nodes that wrap the provided root tree.

    :param root: an Element or an ElementTree.
    :param namespaces: an optional mapping from prefixes to namespace URIs.
    :param uri: an optional URI associated with the document or the root element.
    :param fragment: if `True` is provided the root is considered a fragment. In this \
    case if `root` is an ElementTree instance skips it and use the root Element. If \
    `False` is provided creates a dummy document when the root is an Element instance. \
    For default the root node kind is preserved.
    """
    elem: ElementType
    parent: Any
    child: ta.ChildNodeType
    children: Iterator[Any]
    document: Optional[DocumentProtocol]

    position = 1
    if namespaces is None:
        namespaces = {}
    ns_pos_offset = len(namespaces) + int('xml' not in namespaces) + 1

    if hasattr(root, 'parse'):
        document = cast(DocumentProtocol, root)
        root_elem = document.getroot()
    else:
        document = None
        root_elem = root

    if fragment and root_elem is not None:
        document = None  # Explicitly requested a fragment: don't create a document node

    if document is not None:
        document_node = EtreeDocumentNode(document, uri, position)
        position += 1

        if root_elem is None:
            return document_node

        elem = root_elem
        root_node = EtreeElementNode(elem, document_node, position)
    else:
        assert root_elem is not None
        document_node = None
        elem = root_elem

        root_node = EtreeElementNode(elem, None, position)
        if uri is not None:
            root_node.uri = uri

    root_node.tree.namespaces = namespaces
    position += ns_pos_offset + len(elem.attrib)
    if elem.text is not None:
        TextNode(elem.text, root_node, position)
        position += 1

    children = iter(elem)
    iterators: deque[Any] = deque()
    ancestors: deque[Any] = deque()
    parent = root_node

    while True:
        for elem in children:
            if not callable(elem.tag):
                child = EtreeElementNode(elem, parent, position)
                position += ns_pos_offset + len(elem.attrib)

                if elem.text is not None:
                    TextNode(elem.text, child, position)
                    position += 1

            elif elem.tag.__name__ == 'Comment':
                child = CommentNode(elem, parent, position)
                position += 1
            else:
                child = ProcessingInstructionNode(elem, None, parent, position)
                position += 1

            if len(elem):
                ancestors.append(parent)
                parent = child
                iterators.append(children)
                children = iter(elem)
                break

            if elem.tail is not None:
                TextNode(elem.tail, parent, position)
                position += 1
        else:
            try:
                children, parent = iterators.pop(), ancestors.pop()
            except IndexError:
                if document_node is not None:
                    return document_node
                elif fragment is False and \
                        isinstance(root_node, ElementNode) and \
                        is_etree_element_instance(root_node.elem):
                    return root_node.get_document_node()
                else:
                    return root_node
            else:
                if (tail := parent.children[-1].elem.tail) is not None:
                    TextNode(tail, parent, position)
                    position += 1


def build_lxml_node_tree(root: LxmlRootType,
                         uri: Optional[str] = None,
                         fragment: Optional[bool] = None) -> ta.RootNodeType:
    """
    Returns a tree of XPath nodes that wrap the provided lxml root tree.

    :param root: a lxml Element or a lxml ElementTree.
    :param uri: an optional URI associated with the document or the root element.
    :param fragment: if `True` is provided the root is considered a fragment. In this \
    case if `root` is an ElementTree instance skips it and use the root Element. If \
    `False` is provided creates a dummy document when the root is an Element instance. \
    For default the root node kind is preserved.
    """
    root_node: ta.RootNodeType
    document: Optional[LxmlDocumentProtocol]
    parent: Any
    child: ta.ChildNodeType
    children: Iterator[Any]

    position = 1

    if fragment:
        document = None  # Explicitly requested a fragment: don't create a document node
    elif hasattr(root, 'parse'):
        document = cast(LxmlDocumentProtocol, root)
    elif fragment is False or root.getparent() is None and (
            any(True for _sibling in root.itersiblings(preceding=True)) or
            any(True for _sibling in root.itersiblings())):
        # Despite a document is not explicitly requested create a dummy document
        # because the root element has siblings
        document = root.getroottree()
    else:
        document = None

    if document is not None:
        document_node = EtreeDocumentNode(document, uri, position)
        position += 1

        root_elem = document.getroot()
        if root_elem is None:
            return document_node

        # Add root siblings (comments and processing instructions)
        for elem in reversed([x for x in root_elem.itersiblings(preceding=True)]):
            assert callable(elem.tag)
            if elem.tag.__name__ == 'Comment':
                CommentNode(elem, document_node, position)
                position += 1
            else:
                ProcessingInstructionNode(elem, None, document_node, position)
                position += 1

        root_node = EtreeElementNode(root_elem, document_node, position)
    else:
        if hasattr(root, 'parse'):
            root_elem = cast(LxmlDocumentProtocol, root).getroot()
        else:
            root_elem = root

        if root_elem is None:
            if fragment:
                msg = "requested a fragment of an empty ElementTree document"
            else:
                msg = "root argument is neither an lxml ElementTree nor a lxml Element"
            raise ElementPathTypeError(msg)

        document_node = None
        root_node = EtreeElementNode(root_elem)
        if uri is not None:
            root_node.uri = uri

    # Complete the root element node build
    if 'xml' in root_elem.nsmap:
        position += len(root_elem.nsmap) + len(root_elem.attrib) + 1
    else:
        position += len(root_elem.nsmap) + len(root_elem.attrib) + 2

    if root_elem.text is not None:
        TextNode(root_elem.text, root_node, position)
        position += 1

    children = iter(root_elem)
    iterators: deque[Any] = deque()
    ancestors: deque[Any] = deque()
    parent = root_node

    while True:
        for elem in children:
            if not callable(elem.tag):
                child = EtreeElementNode(elem, parent, position)
                if 'xml' in elem.nsmap:
                    position += len(elem.nsmap) + len(elem.attrib) + 1
                else:
                    position += len(elem.nsmap) + len(elem.attrib) + 2

                if elem.text is not None:
                    TextNode(elem.text, child, position)
                    position += 1

            elif elem.tag.__name__ == 'Comment':
                child = CommentNode(elem, parent, position)
                position += 1
            else:
                child = ProcessingInstructionNode(elem, None, parent, position)
                position += 1

            if len(elem):
                ancestors.append(parent)
                parent = child
                iterators.append(children)
                children = iter(elem)
                break

            if elem.tail is not None:
                TextNode(elem.tail, parent, position)
                position += 1
        else:
            try:
                children, parent = iterators.pop(), ancestors.pop()
            except IndexError:
                if document_node is None:
                    return root_node

                # Add root following siblings (comments and processing instructions)
                for elem in root_elem.itersiblings():
                    assert callable(elem.tag)
                    if elem.tag.__name__ == 'Comment':
                        CommentNode(elem, document_node, position)
                        position += 1
                    else:
                        ProcessingInstructionNode(elem, None, document_node, position)
                        position += 1

                return document_node
            else:
                if (tail := parent.children[-1].elem.tail) is not None:
                    TextNode(tail, parent, position)
                    position += 1


def build_schema_node_tree(root: SchemaElemType,
                           uri: Optional[str] = None,
                           elements: Optional[ta.ElementMapType] = None,
                           global_elements: Optional[list[ta.ChildNodeType]] = None) \
        -> SchemaElementNode:
    """
    Returns a graph of XPath nodes that wrap the provided XSD schema structure.
    The elements dictionary is shared between all nodes to keep all of them,
    globals and local, linked in a single structure.

    :param root: a schema or a schema element.
    :param uri: an optional URI associated with the root element.
    :param elements: a shared map from XSD elements to tree nodes. Provided for \
    linking together parts of the same schema or other schemas.
    :param global_elements: a list for schema global elements, used for linking \
    the elements declared by reference.
    """
    parent: SchemaElementNode
    elem: XsdElementProtocol
    child: SchemaElementNode
    children: Iterator[Any]

    position = 1
    if hasattr(root, 'namespaces'):
        namespaces: ta.NamespacesType = root.namespaces
        ns_pos_offset = len(namespaces) + int('xml' not in namespaces) + 1
    else:
        namespaces = {}
        ns_pos_offset = 2

    root_node = SchemaElementNode(root, None, position)
    if elements is not None:
        elements.update(root_node.tree.elements)
        root_node.tree.elements = elements
    root_node.tree.namespaces = namespaces
    position += ns_pos_offset + len(root.attrib)

    if global_elements is not None:
        global_elements.append(root_node)
    elif is_schema(root):
        global_elements = root_node.children
    else:
        # Track global elements even if the initial root is not a schema to avoid circularity
        global_elements = []

    local_nodes = {root: root_node}  # Irrelevant even if it's the schema
    ref_nodes: list[SchemaElementNode] = []

    if is_schema(root):
        children = iter(e for e in root.maps.elements.values() if e.maps is root.maps)
    else:
        children = iter(root)

    iterators: list[Any] = []
    ancestors: list[Any] = []
    parent = root_node

    while True:
        for elem in children:
            child = SchemaElementNode(elem, parent, position)
            position += ns_pos_offset + len(elem.attrib)

            if elem in local_nodes:
                if elem.ref is None:
                    child.children = local_nodes[elem].children
                else:
                    ref_nodes.append(child)
            else:
                local_nodes[elem] = child
                if elem.ref is None:
                    ancestors.append(parent)
                    parent = child
                    iterators.append(children)
                    children = iter(elem)
                    break
                else:
                    ref_nodes.append(child)
        else:
            try:
                children, parent = iterators.pop(), ancestors.pop()
            except IndexError:
                # connect references to proper nodes
                for element_node in ref_nodes:
                    elem = cast(XsdElementProtocol, element_node.elem)
                    ref = cast(XsdElementProtocol, elem.ref)

                    other: Any
                    for other in global_elements:
                        if other.elem is ref:
                            element_node.ref = other
                            break
                    else:
                        # Extend node tree with other globals
                        element_node.ref = build_schema_node_tree(
                            ref, elements=root_node.elements, global_elements=global_elements
                        )

                return root_node

#
# Copyright (c), 2024-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import platform
from itertools import zip_longest
from collections.abc import Iterator
from threading import Lock, RLock
from typing import cast, Any, Optional, Union
from xml.etree import ElementTree

from elementpath import ElementNode, LazyElementNode, DocumentNode, \
    build_lxml_node_tree, build_node_tree
from elementpath.protocols import LxmlElementProtocol

from xmlschema.aliases import ElementType, ElementTreeType, \
    EtreeType, IOType, IterParseType, ParentMapType
from xmlschema.exceptions import XMLResourceError, XMLResourceParseError, XMLResourceExceeded
from xmlschema.utils.misc import iter_class_slots
from xmlschema.utils.qnames import get_namespace
from xmlschema.arguments import BooleanOption, LazyOption, IterParseOption
from xmlschema import _limits

LazyLockType = RLock if platform.python_implementation() == 'PyPy' else Lock


class XMLResourceLoader:
    """
    A proxy for XML data loading that can handle full or lazy loads of XML trees.
    """
    # Descriptor-based attributes for arguments
    lazy = LazyOption(default=False)
    thin_lazy = BooleanOption(default=True)
    iterparse = IterParseOption(default=ElementTree.iterparse)

    # Private attributes for arguments
    _lazy: Union[bool, int]
    _thin_lazy: bool
    _iterparse: IterParseType

    # Protected attributes for XML data
    _xpath_root: Union[None, ElementNode, DocumentNode]
    _nsmaps: dict[ElementType, dict[str, str]]
    _xmlns: dict[ElementType, list[tuple[str, str]]]
    _parent_map: Optional[ParentMapType]

    root: ElementType
    """The XML tree root Element."""

    __slots__ = ('root', '_nsmaps', '_xmlns', '_lazy', '_thin_lazy',
                 '_iterparse', '_xpath_root', '_parent_map', '__dict__')

    def __init__(self, source: Union[IOType, EtreeType],
                 lazy: Union[bool, int] = False,
                 thin_lazy: bool = True,
                 iterparse: Optional[IterParseType] = None) -> None:

        self.lazy = lazy
        self.thin_lazy = thin_lazy
        self.iterparse = iterparse
        self._nsmaps = {}
        self._xmlns = {}
        self._xpath_root = None
        self._parent_map = None
        self._lazy_lock = LazyLockType()

        if hasattr(source, 'read'):
            fp = cast(IOType, source)
            if self._lazy:
                for _ in self._lazy_iterparse(fp):
                    break
            else:
                self._parse(fp)
        else:
            if hasattr(source, 'tag'):
                self.root = cast(ElementType, source)
            else:
                self.root = cast(ElementType, cast(ElementTreeType, source).getroot())

            if self._lazy:
                msg = f"a {self.__class__.__name__} created from an ElementTree can't be lazy"
                raise XMLResourceError(msg)
            if hasattr(self.root, 'nsmap') and hasattr(self.root, 'xpath'):
                self._parse_namespace_declarations()

    def __repr__(self) -> str:
        if not hasattr(self, 'root'):
            return '<%s object at %#x>' % (self.__class__.__name__, id(self))
        return '%s(root=%r)' % (self.__class__.__name__, self.root)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        for attr in iter_class_slots(self):
            if attr not in state and attr != '__dict__':
                state[attr] = getattr(self, attr)

        state.pop('_lazy_lock', None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        for attr in iter_class_slots(self):
            if attr in state and attr != '__dict__':
                object.__setattr__(self, attr, state.pop(attr))

        self.__dict__.update(state)
        self._lazy_lock = LazyLockType()

    def __copy__(self) -> 'XMLResourceLoader':
        obj: 'XMLResourceLoader' = object.__new__(self.__class__)
        obj.__dict__.update(self.__dict__)

        for attr in iter_class_slots(self):
            if attr != '__dict__':
                object.__setattr__(obj, attr, getattr(self, attr))

        obj._nsmaps = self._nsmaps.copy()
        obj._xmlns = self._xmlns.copy()
        obj._xpath_root = None
        obj._parent_map = None
        obj._lazy_lock = LazyLockType()
        return obj

    @property
    def namespace(self) -> str:
        """The namespace of the XML resource."""
        return get_namespace(self.root.tag)

    @property
    def parent_map(self) -> dict[ElementType, Optional[ElementType]]:
        if self._lazy:
            raise XMLResourceError("can't create the parent map of a lazy XML resource")
        if self._parent_map is None:
            self._parent_map = {child: elem for elem in self.root.iter() for child in elem}
            self._parent_map[self.root] = None
        return self._parent_map

    @property
    def xpath_root(self) -> Union[ElementNode, DocumentNode]:
        """The XPath root node."""
        if self._xpath_root is None:
            if self._lazy:
                self._xpath_root = LazyElementNode(self.root, nsmap=self._nsmaps[self.root])
            elif hasattr(self.root, 'xpath'):
                self._xpath_root = build_lxml_node_tree(cast(LxmlElementProtocol, self.root))
            else:
                try:
                    _nsmap = self._nsmaps[self.root]
                except KeyError:
                    # A resource based on an ElementTree structure (no namespace maps)
                    self._xpath_root = build_node_tree(self.root)
                else:
                    node_tree = build_node_tree(self.root, _nsmap)

                    # Update namespace maps
                    for node in node_tree.iter_descendants(with_self=False):
                        if isinstance(node, ElementNode):
                            nsmap = self._nsmaps[cast(ElementType, node.obj)]
                            node.nsmap = {k or '': v for k, v in nsmap.items()}

                    self._xpath_root = node_tree

        return self._xpath_root

    def clear(self, elem: ElementType) -> None:
        if elem not in self._nsmaps:
            del elem[:]
        else:
            self._clear(elem)

    def get_nsmap(self, elem: ElementType) -> Optional[dict[str, str]]:
        """
        Returns the namespace map (nsmap) of the element. Returns `None` if no nsmap is
        found for the element. Lazy resources have only the nsmap for the root element.
        """
        try:
            return self._nsmaps[elem]
        except KeyError:
            return getattr(elem, 'nsmap', None)  # an lxml element

    def get_xmlns(self, elem: ElementType) -> Optional[list[tuple[str, str]]]:
        """
        Returns the list of namespaces declarations (xmlns and xmlns:<prefix> attributes)
        of the element. Returns `None` if the element doesn't have namespace declarations.
        Lazy resources have only the namespace declarations for the root element.
        """
        return self._xmlns.get(elem)

    def get_xpath_node(self, elem: ElementType) -> ElementNode:
        """
        Returns an XPath node for the element, fetching it from the XPath root node.
        Returns a new lazy element node if the matching element node is not found.
        """
        xpath_node = self.xpath_root.get_element_node(elem)
        if isinstance(xpath_node, ElementNode):
            return xpath_node

        try:
            return LazyElementNode(elem, nsmap=self._nsmaps[elem])
        except KeyError:
            return LazyElementNode(elem)

    def get_absolute_path(self, path: Optional[str] = None) -> str:
        if path is None:
            if self._lazy:
                return f"/{self.root.tag}/{'/'.join('*' * int(self._lazy))}"
            return f'/{self.root.tag}'
        elif path.startswith('/'):
            return path
        else:
            return f'/{self.root.tag}/{path}'

    ##
    # Protected parsing and clearing methods

    def _lazy_iterparse(self, fp: IOType) -> Iterator[tuple[str, ElementType]]:
        events: tuple[str, ...]
        events = 'start-ns', 'end-ns', 'start', 'end'

        root_started = False
        start_ns: list[tuple[str, str]] = []
        end_ns = False
        nsmap_stack: list[dict[str, str]] = [{}]
        remaining_levels = _limits.MAX_XML_DEPTH

        self._nsmaps.clear()
        self._xmlns.clear()

        acquired = self._lazy_lock.acquire(blocking=False)
        if not acquired:
            raise XMLResourceError(f"lazy resource {self!r} is already under iteration")

        try:
            for event, node in self._iterparse(fp, events):
                if event == 'start':
                    remaining_levels -= 1
                    if not remaining_levels:
                        msg = "maximum XML depth reached (MAX_XML_DEPTH={}) for {}"
                        raise XMLResourceExceeded(msg.format(_limits.MAX_XML_DEPTH, self))

                    if end_ns:
                        nsmap_stack.pop()
                        end_ns = False

                    if start_ns:
                        nsmap_stack.append(nsmap_stack[-1].copy())
                        nsmap_stack[-1].update(start_ns)
                        self._xmlns[node] = start_ns
                        start_ns = []

                    self._nsmaps[node] = nsmap_stack[-1]
                    if not root_started:
                        self.root = node
                        self._xpath_root = LazyElementNode(
                            self.root, nsmap=self._nsmaps[node]
                        )
                        root_started = True

                    yield event, node

                elif event == 'end':
                    remaining_levels += 1
                    if end_ns:
                        nsmap_stack.pop()
                        end_ns = False

                    yield event, node

                elif event == 'start-ns':
                    start_ns.append(node)
                elif event == 'end-ns':
                    end_ns = True
                else:
                    yield event, node  # comment or pi node

        except SyntaxError as err:
            raise XMLResourceParseError("invalid XML syntax: {}".format(err)) from err
        finally:
            self._lazy_lock.release()

    def _parse(self, fp: IOType) -> None:
        root_started = False
        start_ns: list[tuple[str, str]] = []
        end_ns = False
        nsmaps = self._nsmaps
        xmlns = self._xmlns
        events = 'start-ns', 'end-ns', 'start', 'comment', 'pi', 'end'
        nsmap_stack: list[dict[str, str]] = [{}]
        remaining_levels = _limits.MAX_XML_DEPTH
        remaining_elements = _limits.MAX_XML_ELEMENTS

        try:
            for event, node in self._iterparse(fp, events):
                if event == 'start':
                    remaining_levels -= 1
                    remaining_elements -= 1
                    if not remaining_levels:
                        msg = "maximum XML depth reached (MAX_XML_DEPTH={}) for {!r}"
                        raise XMLResourceExceeded(msg.format(_limits.MAX_XML_DEPTH, self))
                    if not remaining_elements:
                        msg = ("maximum XML elements reached (MAX_XML_ELEMENTS={} for {!r}). "
                               "Try to increase the limit or process the data using a lazy "
                               "XMLResource, that has no limit.")
                        raise XMLResourceExceeded(msg.format(_limits.MAX_XML_ELEMENTS, self))

                    if not root_started:
                        self.root = node
                        root_started = True
                    if end_ns:
                        nsmap_stack.pop()
                        end_ns = False
                    if start_ns:
                        nsmap_stack.append(nsmap_stack[-1].copy())
                        nsmap_stack[-1].update(start_ns)
                        xmlns[node] = start_ns
                        start_ns = []
                    nsmaps[node] = nsmap_stack[-1]
                elif event == 'start-ns':
                    start_ns.append(node)
                elif event == 'end-ns':
                    end_ns = True
                elif event == 'end':
                    remaining_levels += 1
        except SyntaxError as err:
            raise XMLResourceParseError("invalid XML syntax: {}".format(err)) from err

    def _clear(self, elem: ElementType,
               ancestors: Optional[list[ElementType]] = None) -> None:

        if ancestors and self._thin_lazy:
            # Delete preceding elements
            for parent, child in zip_longest(ancestors, ancestors[1:]):
                if child is None:
                    child = elem

                for k, e in enumerate(parent):
                    if child is not e:
                        if e in self._xmlns:
                            del self._xmlns[e]
                        del self._nsmaps[e]
                    else:
                        if k:
                            del parent[:k]
                        break

        for e in elem.iter():
            if elem is not e:
                if e in self._xmlns:
                    del self._xmlns[e]
                del self._nsmaps[e]

        del elem[:]  # delete children, keep attributes, text and tail.

        # reset the whole XPath tree to let it still usable if other
        # children are added to the root by ElementTree.iterparse().
        if self._xpath_root is not None:
            self._xpath_root.children.clear()

    def _parse_namespace_declarations(self) -> None:
        nsmap = {}
        lxml_nsmap = None
        for elem in cast(Any, self.root.iter()):
            if callable(elem.tag):
                self._nsmaps[elem] = {}
                continue

            if lxml_nsmap != elem.nsmap:
                nsmap = {k or '': v for k, v in elem.nsmap.items()}
                lxml_nsmap = elem.nsmap

            parent = elem.getparent()
            if parent is None:
                xmlns = [(k or '', v) for k, v in nsmap.items()]
            elif parent.nsmap != elem.nsmap:
                xmlns = [(k or '', v) for k, v in elem.nsmap.items()
                         if k not in parent.nsmap or v != parent.nsmap[k]]
            else:
                xmlns = None

            self._nsmaps[elem] = nsmap
            if xmlns:
                self._xmlns[elem] = xmlns

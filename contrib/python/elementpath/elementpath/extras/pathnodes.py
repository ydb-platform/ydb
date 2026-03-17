#
# Copyright (c), 2025-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""Experimental use of XDM on pathlib.Path objects."""

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Optional

from elementpath.aliases import AtomicType, ParentNodeType, ChildNodeType
from elementpath.protocols import XsdElementProtocol
from elementpath.xpath_nodes import XPathNodeTree, AttributeNode, DocumentNode, ElementNode
from elementpath.helpers import match_wildcard
from elementpath.namespaces import XSD_NAMESPACE
from elementpath.datatypes import DateTime


class PathElementNode(ElementNode):
    name: str
    value: Path
    xsd_element: Optional[XsdElementProtocol]

    __slots__ = ('stat',)

    def __init__(self,
                 path: Path,
                 parent: Optional[ParentNodeType] = None,
                 fragment: bool = False) -> None:

        if parent is not None:
            self.parent = parent
            self.tree = parent.tree
        elif fragment:
            self.parent = parent
            path = path.resolve(strict=True)
            self.tree = XPathNodeTree(self, uri=path.as_uri())
        else:
            path = path.resolve(strict=True)

            parents = [p for p in reversed(path.parents)]
            parent = PathDocumentNode(parents[0], uri=path.as_uri())
            self.tree = parent.tree

            for p in parents:
                parent = PathElementNode(p, parent)
            self.parent = parent

        self.name = path.name
        self.value = path
        self.stat = path.stat()
        self.position = self.stat.st_ino
        self.children = []
        self.xsd_type = None
        self._nsmap = None
        self.tree.elements[path] = self

    @property
    def content(self) -> Path:
        return self.value

    elem = content

    @property
    def attributes(self) -> list[AttributeNode]:
        """Stat slots as attributes."""
        if not hasattr(self, '_attributes'):
            self._attributes = []
            for name in dir(self.stat):
                if name[:3] != 'st_':
                    continue
                elif name in ('st_atime_ns', 'st_ctime_ns', 'st_mtime_ns', 'st_birthtime_ns'):
                    dt = datetime.fromtimestamp(getattr(self.stat, name) / 1e9)
                    self._attributes.append(DatetimeAttributeNode(name[3:-3], dt, self))
                elif name[-4:] != 'time':
                    value: int = getattr(self.stat, name)
                    if name == 'st_mode':
                        self._attributes.append(ModeAttributeNode(name[3:], value, self))
                    else:
                        self._attributes.append(IntAttributeNode(name[3:], value, self))
        return self._attributes

    @property
    def name_path(self) -> str:
        return self.name

    @property
    def path(self) -> str:
        if self.parent is None:
            return f'/{self.name_path}'

        if isinstance(self.parent, ElementNode):
            return f"{self.parent.path}/{self.name_path}"
        return f"/{self.name_path}"

    @property
    def string_value(self) -> str:
        if self.value.is_dir():
            return ''
        if self.value.is_file():
            return self.value.read_text()
        else:
            return ''

    @property
    def iter_typed_values(self) -> Iterator[AtomicType]:
        yield from ()

    @property
    def is_typed(self) -> bool:
        return False

    def match_name(self, name: str, default_namespace: Optional[str] = None) -> bool:
        if '*' in name:
            return match_wildcard(self.name, name)
        elif name[0] == '{' or not default_namespace:
            return self.name == name
        else:
            return self.name == f'{{{default_namespace}}}{name}'

    def get_document_node(self, replace: bool = True, as_parent: bool = True) -> 'DocumentNode':
        return PathDocumentNode(Path(self.value.absolute().root))

    def __iter__(self) -> Iterator[ChildNodeType]:
        if not self.children:
            if self.value.is_dir():
                for path in self.value.iterdir():
                    if path in self.tree.elements:
                        self.children.append(self.tree.elements[path])
                    else:
                        self.children.append(PathElementNode(path, self))

        yield from self.children

    def iter_descendants(self, with_self: bool = True) -> Iterator[ChildNodeType]:
        if with_self:
            yield self

        for child in self:
            if isinstance(child, PathElementNode):
                yield from child.iter_descendants()
            else:
                yield child


class PathDocumentNode(DocumentNode):
    value: Path

    __slots__ = ('stat',)

    def __init__(self, document: Path, uri: Optional[str] = None) -> None:

        if not document.is_dir() or len(document.parts) > 1 or not document.is_absolute():
            raise ValueError(f'{document} must be a root directory')

        self.value = document
        self.name = None
        self.parent = None
        self.position = document.stat().st_ino
        self.children = []
        self.tree = XPathNodeTree(self, uri=uri)

    @property
    def document(self) -> Path:
        return self.value

    @property
    def string_value(self) -> str:
        return self.value.read_text()

    def __iter__(self) -> Iterator[ChildNodeType]:
        if not self.children:
            for path in self.value.iterdir():
                if path in self.tree.elements:
                    self.children.append(self.tree.elements[path])
                else:
                    self.children.append(PathElementNode(path, self))

        yield from self.children


class IntAttributeNode(AttributeNode):
    name: str
    value: int
    parent: Optional['PathElementNode']

    __slots__ = ()

    def __init__(self,
                 name: str,
                 value: int,
                 parent: Optional['PathElementNode'] = None) -> None:

        self.name = name
        self.value = value
        self.parent = parent
        self.position = parent.position if parent is not None else 1
        self.xsd_type = None

    @property
    def string_value(self) -> str:
        return str(self.value)

    @property
    def iter_typed_values(self) -> Iterator[AtomicType]:
        yield self.value

    @property
    def type_name(self) -> Optional[str]:
        return f'{{{XSD_NAMESPACE}}}int'


class ModeAttributeNode(IntAttributeNode):

    @property
    def string_value(self) -> str:
        return oct(self.value)


class DatetimeAttributeNode(AttributeNode):
    name: str
    value: datetime
    parent: Optional['PathElementNode']

    __slots__ = ()

    def __init__(self,
                 name: str,
                 value: datetime,
                 parent: Optional['PathElementNode'] = None) -> None:

        self.name = name
        self.value = value
        self.parent = parent
        self.position = parent.position if parent is not None else 1
        self.xsd_type = None

    @property
    def string_value(self) -> str:
        return str(self.value)

    @property
    def iter_typed_values(self) -> Iterator[AtomicType]:
        yield DateTime.fromdatetime(self.value)

    @property
    def type_name(self) -> Optional[str]:
        return f'{{{XSD_NAMESPACE}}}dateTime'

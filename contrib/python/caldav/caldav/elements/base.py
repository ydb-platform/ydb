#!/usr/bin/env python
import sys
from typing import ClassVar
from typing import List
from typing import Optional
from typing import Union

from lxml import etree
from lxml.etree import _Element

from caldav.lib.namespace import nsmap
from caldav.lib.python_utilities import to_unicode

if sys.version_info < (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


class BaseElement:
    children: Optional[List[Self]] = None
    tag: ClassVar[Optional[str]] = None
    value: Optional[str] = None
    attributes: Optional[dict] = None
    caldav_class = None

    def __init__(
        self, name: Optional[str] = None, value: Union[str, bytes, None] = None
    ) -> None:
        self.children = []
        self.attributes = {}
        value = to_unicode(value)
        self.value = None
        if name is not None:
            self.attributes["name"] = name
        if value is not None:
            self.value = value

    def __add__(
        self, other: Union["BaseElement", Iterable["BaseElement"]]
    ) -> "BaseElement":
        return self.append(other)

    def __str__(self) -> str:
        utf8 = etree.tostring(
            self.xmlelement(), encoding="utf-8", xml_declaration=True, pretty_print=True
        )
        return str(utf8, "utf-8")

    def xmlelement(self) -> _Element:
        if self.tag is None:
            ## We can do better than this.  tag may be a property that expands
            ## from the class name.  Another layer of base class to indicate
            ## if it's the D-namespace, C-namespace or another namespace.
            raise ValueError("Unexpected value None for self.tag")

        if self.attributes is None:
            raise ValueError("Unexpected value None for self.attributes")

        root = etree.Element(self.tag, nsmap=nsmap)
        if self.value is not None:
            root.text = self.value

        for k in self.attributes:
            root.set(k, self.attributes[k])

        self.xmlchildren(root)
        return root

    def xmlchildren(self, root: _Element) -> None:
        if self.children is None:
            raise ValueError("Unexpected value None for self.children")

        for c in self.children:
            root.append(c.xmlelement())

    def append(self, element: Union[Self, Iterable[Self]]) -> Self:
        if self.children is None:
            raise ValueError("Unexpected value None for self.children")

        if isinstance(element, Iterable):
            self.children.extend(element)
        else:
            self.children.append(element)

        return self


class NamedBaseElement(BaseElement):
    def __init__(self, name: Optional[str] = None) -> None:
        super(NamedBaseElement, self).__init__(name=name)

    def xmlelement(self):
        if self.attributes.get("name") is None:
            raise Exception("name attribute must be defined")
        return super(NamedBaseElement, self).xmlelement()


class ValuedBaseElement(BaseElement):
    def __init__(self, value: Union[str, bytes, None] = None) -> None:
        super(ValuedBaseElement, self).__init__(value=value)

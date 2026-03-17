"""Objects shared by pptx modules."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pptx.opc.package import XmlPart
    from pptx.oxml.xmlchemy import BaseOxmlElement
    from pptx.types import ProvidesPart


class ElementProxy(object):
    """Base class for lxml element proxy classes.

    An element proxy class is one whose primary responsibilities are fulfilled by manipulating the
    attributes and child elements of an XML element. They are the most common type of class in
    python-pptx other than custom element (oxml) classes.
    """

    def __init__(self, element: BaseOxmlElement):
        self._element = element

    def __eq__(self, other: object) -> bool:
        """Return |True| if this proxy object refers to the same oxml element as does *other*.

        ElementProxy objects are value objects and should maintain no mutable local state.
        Equality for proxy objects is defined as referring to the same XML element, whether or not
        they are the same proxy object instance.
        """
        if not isinstance(other, ElementProxy):
            return False
        return self._element is other._element

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, ElementProxy):
            return True
        return self._element is not other._element

    @property
    def element(self):
        """The lxml element proxied by this object."""
        return self._element


class ParentedElementProxy(ElementProxy):
    """Provides access to ancestor objects and part.

    An ancestor may occasionally be required to provide a service, such as add or drop a
    relationship. Provides the :attr:`_parent` attribute to subclasses and the public
    :attr:`parent` read-only property.
    """

    def __init__(self, element: BaseOxmlElement, parent: ProvidesPart):
        super(ParentedElementProxy, self).__init__(element)
        self._parent = parent

    @property
    def parent(self):
        """The ancestor proxy object to this one.

        For example, the parent of a shape is generally the |SlideShapes| object that contains it.
        """
        return self._parent

    @property
    def part(self) -> XmlPart:
        """The package part containing this object."""
        return self._parent.part


class PartElementProxy(ElementProxy):
    """Provides common members for proxy-objects that wrap a part's root element, e.g. `p:sld`."""

    def __init__(self, element: BaseOxmlElement, part: XmlPart):
        super(PartElementProxy, self).__init__(element)
        self._part = part

    @property
    def part(self) -> XmlPart:
        """The package part containing this object."""
        return self._part
